"""raw-recorder command line."""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import aiohttp

from . import RECORDER_VERSION, kalshi, mlb
from .archive import (ArchiveWriter, build_manifest, find_parts, iter_archive,
                      iter_file, read_journal, recover_orphans, verify)
from .config import Config, load_config, load_games
from .novig import public as novig_public
from .novig.stream import CHANNEL_WEIGHT
from .rest import RestRecorder
from .runtime import Recorder, RecorderBusy, load_signer, signal_running_recorder
from .session import ArchiveLock, new_session_id

ET = ZoneInfo("America/New_York")


# --------------------------------------------------------------------------
# Short commands share the archive lock with `run`.
# --------------------------------------------------------------------------

@asynccontextmanager
async def short_session(cfg: Config, command: str):
    lock = ArchiveLock(cfg.archive_root)
    if not lock.acquire():
        raise RecorderBusy(f"the recorder (pid {lock.holder_pid()}) is running; "
                           f"`{command}` writes to the archive and must not run beside it")
    session_id = new_session_id()
    archive = ArchiveWriter(cfg.archive_root, session_id=session_id,
                            segment_max_s=cfg.params.segment_max_s,
                            fsync_interval_s=cfg.params.fsync_interval_s)
    rec = archive.stream("recorder", f"{command}-{session_id[:12]}")
    rec.event(rec.stream_id, "command_start", command=command, recorder_version=RECORDER_VERSION,
              config_sha256=cfg.sha256)
    try:
        async with aiohttp.ClientSession() as http:
            yield archive, http, session_id[:12]
        rec.event(rec.stream_id, "command_stop", command=command)
    finally:
        archive.close()
        lock.release()


def _rest(archive: ArchiveWriter, http, source: str, name: str, sid: str, cfg: Config) -> RestRecorder:
    return RestRecorder(http, archive.stream(source, f"{name}-{sid}"), timeout_s=cfg.params.rest_timeout_s)


# --------------------------------------------------------------------------
# Commands
# --------------------------------------------------------------------------

def cmd_run(args) -> int:
    cfg = load_config(args.config)
    games = load_games(args.games)
    if not games:
        print("warning: no games in games.toml; only housekeeping will run", file=sys.stderr)
    asyncio.run(Recorder(cfg, games).run())
    return 0


async def _echo(cfg: Config) -> int:
    signer = load_signer(cfg)
    if signer is None:
        print(f"set ${cfg.novig['key_id_env']} and ${cfg.novig['key_path_env']} first", file=sys.stderr)
        return 2
    host = cfg.novig["host"]
    async with short_session(cfg, "echo") as (archive, http, sid):
        rest = _rest(archive, http, "novig", "echo", sid, cfg)
        body = json.dumps({"r0": "echo", "recorder": RECORDER_VERSION}, separators=(",", ":")).encode()
        echo = await rest.request("POST", host, "/v3/echo", body=body, signer=signer, purpose="echo")
        print(f"POST /v3/echo -> {echo.status or echo.failure}"
              f"{' (body echoed byte for byte)' if echo.body == body else ''}")
        limits = await rest.request("GET", host, "/v3/limits", signer=signer, purpose="limits")
        print(f"GET /v3/limits -> {limits.status or limits.failure}: "
              f"{(limits.body or b'').decode(errors='replace')[:300]}")
        key = await rest.request("GET", host, f"/v3/keys/{signer.key_id}", signer=signer,
                                 purpose="scope_check")
        verdict = ("MANAGEMENT KEY: do not use on the recorder host" if key.status == 200 else
                   "not a management key (expected for trading::read)" if key.status == 403 else
                   "inconclusive")
        print(f"GET /v3/keys/{{id}} -> {key.status or key.failure}: {verdict}")
        return 0 if echo.ok and key.status != 200 else 1


def cmd_echo(args) -> int:
    return asyncio.run(_echo(load_config(args.config)))


def _kalshi_start_utc(event_ticker: str) -> datetime | None:
    """KXMLBGAME-26OCT071800LADATL -> 2026-10-07 18:00 ET, as UTC."""
    try:
        code = event_ticker.split("-")[1][:11]
        return datetime.strptime(code, "%y%b%d%H%M").replace(tzinfo=ET).astimezone(timezone.utc)
    except (IndexError, ValueError):
        return None


async def _catalog(cfg: Config, days: int) -> int:
    today = datetime.now(timezone.utc).astimezone(ET).date()
    async with short_session(cfg, "catalog") as (archive, http, sid):
        m_rest = _rest(archive, http, "mlb_statsapi", "catalog", sid, cfg)
        res = await m_rest.request("GET", cfg.mlb["base_url"], "/api/v1/schedule",
                                   params={"sportId": "1", "startDate": today.isoformat(),
                                           "endDate": (today + timedelta(days=days)).isoformat(),
                                           "hydrate": "team"},
                                   subjects=["mlb:schedule"], purpose="catalog")
        games = [g for d in json.loads(res.body or b"{}").get("dates", []) for g in d.get("games", [])]
        _, n_markets = await novig_public.fetch_catalog(
            _rest(archive, http, "novig", "catalog", sid, cfg), cfg.novig["host"])
        k_markets = await kalshi.fetch_series(_rest(archive, http, "kalshi", "catalog", sid, cfg),
                                              cfg.kalshi["base_url"], cfg.kalshi["series_ticker"])

    print("# Suggested games.toml entries. The responses behind them are archived;")
    print("# check every match by hand before recording.\n")
    for g in games:
        if g.get("status", {}).get("startTimeTBD"):
            continue
        start = datetime.fromisoformat(g["gameDate"].replace("Z", "+00:00"))
        away, home = g["teams"]["away"]["team"], g["teams"]["home"]["team"]
        abbrs = {away.get("abbreviation"), home.get("abbreviation")}
        start_ms = int(start.timestamp() * 1000)
        novig = [m for m in n_markets if m.get("startsTs") == start_ms]
        if len(novig) > 1:
            novig = [m for m in novig if {o["name"] for o in m.get("outcomes", [])} == abbrs] or novig
        k_events = defaultdict(list)
        for m in k_markets:
            if _kalshi_start_utc(m.get("event_ticker", "")) == start:
                k_events[m["event_ticker"]].append(m["ticker"])
        status = g.get("status", {}).get("detailedState", "")
        print("[[game]]")
        print(f"game_pk = {g['gamePk']}")
        print(f'label = "{away["name"]} @ {home["name"]}, {g.get("seriesDescription", "")}, '
              f'{start.astimezone(ET):%a %b %d %H:%M} ET ({status})"')
        print(f'scheduled_start_utc = "{start:%Y-%m-%dT%H:%M:%SZ}"')
        print(f"novig_markets = {json.dumps([m['marketId'] for m in novig])}"
              + ("" if len(novig) == 1 else f"  # CHECK: {len(novig)} candidates"))
        tickers = [t for ts in k_events.values() for t in sorted(ts)]
        print(f"kalshi_tickers = {json.dumps(tickers)}"
              + ("" if len(k_events) == 1 else f"  # CHECK: {len(k_events)} candidate events"))
        print()
    return 0


def cmd_catalog(args) -> int:
    return asyncio.run(_catalog(load_config(args.config), args.days))


async def _probe_subscriptions(cfg: Config, market: str, event: str | None, wait_s: float) -> int:
    """Experiment for the R0 gate: can one connection carry book and trades?"""
    signer = load_signer(cfg)
    if signer is None:
        print("a read key is required", file=sys.stderr)
        return 2
    url = cfg.novig["ws_url"]
    async with short_session(cfg, "probe-subscriptions") as (archive, http, sid):
        conn_id = f"probe-subscriptions-{sid}"
        stream = archive.stream("novig", conn_id)
        ws = await http.ws_connect(url, headers=signer("GET", urlsplit(url).path, "", b""),
                                   autoping=False, autoclose=False, max_msg_size=0, compress=0)
        stream.event(conn_id, "ws_connected", experiment="probe-subscriptions", market=market, event=event)

        async def receive():
            while True:
                msg = await ws.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    stream.write("in", conn_id, msg.data)
                    print(f"<- {msg.data[:400]}")
                elif msg.type == aiohttp.WSMsgType.PING:
                    stream.event(conn_id, "ws_control", op="ping", direction="in", payload_hex=msg.data.hex())
                    await ws.pong(msg.data)
                    stream.event(conn_id, "ws_control", op="pong", direction="out", payload_hex=msg.data.hex())
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    stream.event(conn_id, "ws_closed", code=ws.close_code)
                    return

        rx = asyncio.create_task(receive())
        steps = [{"subscribe": {"markets": {market: "book"}}},
                 {"subscribe": {"markets": {market: "trades"}}},
                 {"status": {}}]
        if event:
            steps += [{"subscribe": {"markets": {market: "book"}, "events": {event: "trades"}}},
                      {"status": {}}]
        steps.append({"snapshot": {"markets": {market: "book"}}})
        for nonce, step in enumerate(steps, start=1):
            text = json.dumps({"nonce": nonce, **step}, separators=(",", ":"))
            await ws.send_str(text)
            stream.write("out", conn_id, text)
            print(f"-> {text}")
            await asyncio.sleep(wait_s)
        await ws.close()
        rx.cancel()
        await asyncio.gather(rx, return_exceptions=True)
    return 0


def cmd_probe_subscriptions(args) -> int:
    return asyncio.run(_probe_subscriptions(load_config(args.config), args.market, args.event, args.wait))


async def _mlb_feed(cfg: Config, game_pks: list[int], date: str | None) -> int:
    async with short_session(cfg, "mlb-feed") as (archive, http, sid):
        rest = _rest(archive, http, "mlb_statsapi", "mlb-feed", sid, cfg)
        if date:
            game_pks = game_pks + [g["gamePk"] for g in await mlb.fetch_schedule(rest, cfg.mlb["base_url"], date, date)]
        for pk in game_pks:
            await mlb.fetch_game(rest, cfg.mlb["base_url"], pk)
            print(f"archived feed and play-by-play for gamePk {pk}")
    return 0


def cmd_mlb_feed(args) -> int:
    if not args.game_pk and not args.date:
        print("give gamePk values or --date", file=sys.stderr)
        return 2
    return asyncio.run(_mlb_feed(load_config(args.config), args.game_pk, args.date))


def cmd_seal(args) -> int:
    cfg = load_config(args.config)
    root = cfg.archive_root
    pid = signal_running_recorder(root)
    if pid is not None:
        print(f"recorder pid {pid} is running: sent SIGHUP; it rotates and seals its active segments")
        return 0
    lock = ArchiveLock(root)
    if not lock.acquire():
        print("archive lock is busy; try again", file=sys.stderr)
        return 1
    try:
        for e in recover_orphans(root, session_id=None):
            print(f"sealed orphan {e.file} (unclean_close, dropped {e.dropped_tail_bytes} tail bytes)")
        for journal in sorted(root.glob("*/*/manifest.jsonl")):
            build_manifest(journal.parent)
    finally:
        lock.release()
    print("no active segments; manifest.json views rebuilt from journals")
    return 0


def cmd_verify(args) -> int:
    cfg = load_config(args.config)
    root = Path(args.path) if args.path else cfg.archive_root
    problems = verify(root)
    for p in problems:
        print(p)
    n = sum(len(read_journal(j.parent)) for j in root.rglob("manifest.jsonl"))
    print(f"{n} sealed segments checked; {len(problems)} problem(s)")
    return 1 if problems else 0


def cmd_report(args) -> int:
    cfg = load_config(args.config)
    root = cfg.archive_root
    print(f"{'source':<14}{'day':<12}{'files':>6}{'frames':>10}{'gz MB':>9}{'raw B/s':>10}")
    for journal in sorted(root.glob("*/*/manifest.jsonl")):
        entries = read_journal(journal.parent)
        if args.day and journal.parent.name != args.day:
            continue
        frames = sum(e["frame_count"] for e in entries)
        gz = sum(e["bytes"] for e in entries)
        raw = 0
        for e in entries:
            raw += sum(len(f["frame"]) for f in iter_file(journal.parent / e["file"]))
        ts = [t for e in entries for t in (e["first_recv_ts_ms"], e["last_recv_ts_ms"]) if t]
        span = (max(ts) - min(ts)) / 1000 if len(ts) > 1 else 0
        rate = f"{raw / span:,.0f}" if span else "-"
        print(f"{journal.parent.parent.name:<14}{journal.parent.name:<12}{len(entries):>6}"
              f"{frames:>10}{gz / 1e6:>9.2f}{rate:>10}")
    parts = find_parts(root)
    if parts:
        print(f"\n{len(parts)} active segment(s) not yet sealed (included below only if sealed)")

    spent, remaining, probes, skipped, pings = 0.0, None, 0, 0, 0
    for f in iter_archive(root):
        if f["dir"] != "event":
            continue
        meta = json.loads(f["frame"])
        kind = meta.get("type")
        if kind == "rest_complete":
            hdr = {k.lower(): v for k, v in meta.get("response_headers", [])}
            if "x-requests-last" in hdr:
                spent += float(hdr["x-requests-last"])
                remaining = hdr.get("x-requests-remaining", remaining)
        elif kind == "probe_sent":
            probes += 1
        elif kind == "probe_skipped":
            skipped += 1
        elif kind == "ws_control" and meta.get("op") == "ping" and meta.get("direction") == "in":
            pings += 1
    print(f"\nThe Odds API: {spent:g} credits spent in archived calls; last remaining: {remaining}")
    print(f"Novig: {pings} transport pings, {probes} snapshot probes sent, {skipped} skipped for budget "
          f"(weights: {CHANNEL_WEIGHT})")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="raw-recorder", description=__doc__)
    ap.add_argument("--config", type=Path, help="config.toml (default: tools/raw_recorder/config.toml)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="record every source for the games in games.toml")
    p.add_argument("--games", type=Path, help="games.toml (default: tools/raw_recorder/games.toml)")
    p.set_defaults(fn=cmd_run)
    sub.add_parser("echo", help="test the Novig read key: signed POST /v3/echo, limits, scope").set_defaults(fn=cmd_echo)
    p = sub.add_parser("catalog", help="archive MLB/Novig/Kalshi catalogs and print games.toml suggestions")
    p.add_argument("--days", type=int, default=2)
    p.set_defaults(fn=cmd_catalog)
    p = sub.add_parser("probe-subscriptions", help="experiment: book and trades on one Novig connection")
    p.add_argument("--market", required=True)
    p.add_argument("--event")
    p.add_argument("--wait", type=float, default=3.0, help="seconds between steps")
    p.set_defaults(fn=cmd_probe_subscriptions)
    p = sub.add_parser("mlb-feed", help="archive StatsAPI feed and play-by-play")
    p.add_argument("game_pk", type=int, nargs="*")
    p.add_argument("--date", help="every game on YYYY-MM-DD")
    p.set_defaults(fn=cmd_mlb_feed)
    sub.add_parser("seal", help="seal active segments (SIGHUPs a running recorder)").set_defaults(fn=cmd_seal)
    p = sub.add_parser("verify", help="recheck sealed segments against their journals")
    p.add_argument("path", nargs="?")
    p.set_defaults(fn=cmd_verify)
    p = sub.add_parser("report", help="volume per source/day, Odds credit spend, Novig probe counts")
    p.add_argument("--day")
    p.set_defaults(fn=cmd_report)

    args = ap.parse_args(argv)
    try:
        return args.fn(args)
    except RecorderBusy as exc:
        print(f"raw-recorder: {exc}", file=sys.stderr)
        return 3


if __name__ == "__main__":
    raise SystemExit(main())
