"""raw-recorder command line."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import aiohttp

from . import RECORDER_VERSION, backfill, discovery, leagues, mlb
from .archive import (ArchiveWriter, build_manifest, find_parts, iter_archive,
                      iter_file, now_ms, read_journal, recover_orphans, verify)
from .config import Config, LeagueEvent, discovered_games, load_all_games, load_config, load_events, load_games, shadowed
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
    games_path = Path(args.games or os.environ.get("RAW_RECORDER_GAMES") or cfg.games_path)
    games = load_games(games_path)
    if not games and not cfg.discovery.get("enabled", False) and not cfg.enabled_leagues():
        print(f"warning: no games in {games_path}, and discovery and every league are off; only housekeeping"
              " will run", file=sys.stderr)
    asyncio.run(Recorder(cfg, games, games_path=games_path).run())
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


async def _catalog(cfg: Config) -> int:
    """One discovery round by hand, for when no recorder is running (a running recorder
    discovers on its own schedule): archived, merged into the discovered list, and printed."""
    async with short_session(cfg, "catalog") as (archive, http, sid):
        rests = (_rest(archive, http, "mlb_statsapi", "catalog", sid, cfg),
                 _rest(archive, http, "novig", "catalog", sid, cfg),
                 _rest(archive, http, "kalshi", "catalog", sid, cfg) if cfg.enabled(cfg.kalshi) else None)
        merged, found = await discovery.discover(cfg, *rests, now_ms())
        league_round = (await leagues.discover(cfg, rests[1], now_ms())
                        if cfg.enabled_leagues() and cfg.enabled(cfg.novig) else None)
    print(f"# Matched this round; merged into {cfg.discovered_path} ({len(merged)} games in all).")
    print("# The responses behind them are archived. Copy an entry into games.toml to override it.\n")
    print(discovery.render(found.games, now_ms(), found.warnings + found.notes))
    if league_round is not None:
        lg_merged, lg_found = league_round
        print(f"\n# League events this round; merged into {cfg.events_discovered_path} ({len(lg_merged)} in all).\n")
        print(leagues.render(lg_found.events, now_ms(), lg_found.warnings + lg_found.notes))
    return 0


def cmd_catalog(args) -> int:
    return asyncio.run(_catalog(load_config(args.config)))


TICK_S = 1.0


class SealFailed(RuntimeError):
    """A segment could not be sealed: its completed requests are not in the sealed archive."""

    def __init__(self, errors: list[BaseException]):
        super().__init__("; ".join(str(e) for e in errors))
        self.errors = errors


async def _tick(archive: ArchiveWriter) -> None:
    """The archive's housekeeping, as the recorder runs it: fsync on cadence and seal a
    segment when it is due, even while its source is idle or finished. A failed seal stops
    the run: what it held is missing from the sealed archive, and when the disk is full,
    everything after it would fail too."""
    while True:
        archive.tick()
        if archive.seal_errors:
            raise SealFailed(list(archive.seal_errors))
        await asyncio.sleep(TICK_S)


def _close(archive: ArchiveWriter) -> list[BaseException]:
    """Close the archive; every seal that failed, during the run or at close, once each."""
    errors = list(archive.seal_errors)
    try:
        archive.close()
    except Exception as exc:            # drain() re-raises a seal pending at close
        errors.append(exc)
    for exc in archive.seal_errors + errors:
        if not any(exc is e for e in errors):
            errors.append(exc)
    return errors


async def _backfill(cfg: Config, sources: list[str], ranges) -> int:
    root = cfg.backfill_root
    lock = ArchiveLock(root)
    if not lock.acquire():
        raise RecorderBusy(f"a backfill (pid {lock.holder_pid()}) is already writing to {root}")
    try:
        free_mb = shutil.disk_usage(root).free // 2**20
        if free_mb < cfg.params.min_free_disk_mb:
            print(f"backfill: {free_mb} MB free under {root}, below r0.min_free_disk_mb", file=sys.stderr)
            return 2
        for e in recover_orphans(root, session_id=None):
            print(f"sealed orphan {e.file} from an interrupted run")
        done = backfill.load_done(root)
        signer = load_signer(cfg) if "novig" in sources else None
        if "novig" in sources and signer is None:
            print(f"no ${cfg.novig['key_id_env']}: Novig history routes are skipped", file=sys.stderr)
        session_id = new_session_id()
        archive = ArchiveWriter(root, session_id=session_id, segment_max_s=cfg.backfill["segment_max_s"],
                                fsync_interval_s=cfg.params.fsync_interval_s, clock=backfill.clock)
        housekeeping = asyncio.create_task(_tick(archive))
        fetch = None
        try:
            async with aiohttp.ClientSession() as http:
                rests = {s: RestRecorder(http, archive.stream(s, f"backfill-{session_id[:12]}"),
                                         timeout_s=cfg.backfill["rest_timeout_s"]) for s in backfill.SOURCES.values()}
                fetch = asyncio.create_task(backfill.run(rests, done, cfg, sources, ranges, signer))
                await asyncio.wait({fetch, housekeeping}, return_when=asyncio.FIRST_COMPLETED)
                if housekeeping.done():         # it ends only when a seal failed
                    housekeeping.exception()    # reported below, from archive.seal_errors
                    fetch.cancel()
                    await asyncio.gather(fetch, return_exceptions=True)
        finally:
            housekeeping.cancel()
            if fetch is not None and not fetch.done():
                fetch.cancel()
            seal_errors = _close(archive)
    finally:
        lock.release()
    if seal_errors:
        for exc in seal_errors:
            print(f"sealing failed: {exc!r}", file=sys.stderr)
        print("the unsealed segments stay as .jsonl.part; free space or fix the cause, then rerun: the next"
              " run seals them (recover_orphans) and resumes", file=sys.stderr)
        return 2
    stats, seen = fetch.result()
    for source in sources:
        name = backfill.SOURCES[source]
        print(f"{source}: fetched {stats.fetched[name]}, already archived {stats.skipped[name]}, "
              f"failed {sum(1 for x in stats.failed if x[0] == name)}; {dict(seen.get(source, {}))}")
    for source, path, query, why in stats.failed[:20]:
        print(f"  failed {source} {path}{'?' + query if query else ''}: {why}")
    if len(stats.failed) > 20:
        print(f"  ... and {len(stats.failed) - 20} more; rerun to retry them")
    return 1 if stats.failed else 0


def cmd_backfill(args) -> int:
    cfg = load_config(args.config)
    return asyncio.run(_backfill(cfg, args.sources or list(backfill.SOURCES), backfill.parse_ranges(args.dates)))


def cmd_games(args) -> int:
    """Read-only: the games the recorder records over the next days, and how fresh discovery is."""
    from .scheduler import capture_windows
    cfg = load_config(args.config)
    t = now_ms()
    games = load_all_games(cfg)
    events = load_events(cfg.events_discovered_path)
    code = 0
    if cfg.discovery.get("enabled", False):
        at, notes = discovery.last_run(cfg.discovered_path)
        stale_ms = 2 * float(cfg.discovery.get("interval_s", 3600)) * 1000
        if at is None:
            print(f"discovery: has not run yet ({cfg.discovered_path} missing)")
            code = 1
        else:
            age = (t - at) / 60000
            print(f"discovery: last run {_et_time(at)} ({age:,.0f} min ago){'  STALE' if t - at > stale_ms else ''}")
            code = 1 if t - at > stale_ms else 0
        for n in notes:
            print(f"  note: {n}")
    else:
        print("discovery: off in this config; only games.toml is recorded")
    if cfg.enabled_leagues():
        names = ", ".join(lg["name"] for lg in cfg.enabled_leagues())
        at, notes = discovery.last_run(cfg.events_discovered_path)
        stale_ms = 2 * float(cfg.discovery.get("interval_s", 3600)) * 1000
        if at is None:
            print(f"league discovery ({names}): has not run yet ({cfg.events_discovered_path} missing)")
            code = max(code, 1)
        else:
            stale = t - at > stale_ms
            print(f"league discovery ({names}): last run {_et_time(at)} ({(t - at) / 60000:,.0f} min ago)"
                  f"{'  STALE' if stale else ''}")
            code = max(code, 1 if stale else 0)
        for n in notes:
            print(f"  note: {n}")
    for message in shadowed(load_games(cfg.games_path), discovered_games(cfg)):
        print(f"WARNING: {message}")
        code = max(code, 1)
    lo, hi = t - args.past_hours * 3_600_000, t + args.days * 86_400_000
    shown = [w for w in capture_windows([*games, *events], cfg.params) if w.end_ms >= lo and w.start_ms <= hi]
    print(f"\n{len(shown)} game(s) with a capture window from {args.past_hours} h ago to {args.days} day(s) ahead:")
    for w in shown:
        g = w.game
        phase = "open" if w.contains(t) else ("ended" if w.end_ms <= t else "upcoming")
        if isinstance(g, LeagueEvent):
            print(f"\n- {g.league} event {g.event_id} {g.label} [{g.source}, {phase}]")
            print(f"    start {_et_time(g.scheduled_start_ms)}; capture {_et_time(w.start_ms)} to {_et_time(w.end_ms)}")
            print(f"    Novig {', '.join(g.novig_markets) or 'none'}")
            for n in g.notes:
                print(f"    note: {n}")
            continue
        print(f"\n- {g.game_pk} {g.label} [{g.source}, {phase}]")
        print(f"    first pitch {_et_time(g.scheduled_start_ms)}; capture {_et_time(w.start_ms)} to {_et_time(w.end_ms)}; "
              f"odds from {_et_time(g.scheduled_start_ms - cfg.params.odds_window_lead_s * 1000)}")
        print(f"    Novig {', '.join(g.novig_markets) or 'none'}")
        print(f"    Kalshi {', '.join(g.kalshi_tickers) or 'none'}")
        for n in g.notes:
            print(f"    note: {n}")
    return code


def _utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _et_time(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).astimezone(ET).strftime("%a %b %d %H:%M ET")


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


def cmd_relay(args) -> int:
    from . import faults
    cfg = load_config(args.config)
    listen = args.listen or cfg.faults.get("relay_listen", "127.0.0.1:8765")
    upstream = args.upstream or cfg.faults.get("relay_upstream")
    if not upstream:
        print(f"no upstream: set [faults] relay_upstream in {cfg.path} or pass --upstream", file=sys.stderr)
        return 2
    print(f"raw-recorder relay: ws://{listen} -> {upstream}; faults by POST {faults.FAULT_PATH}", file=sys.stderr)
    try:
        asyncio.run(faults.run_relay(listen, upstream))
    except KeyboardInterrupt:
        pass
    return 0


def cmd_fault(args) -> int:
    from . import faults
    cfg = load_config(args.config)
    kind = args.kind.replace("-", "_")
    need = {"channel_stall": ("market", "channel", "seconds"), "sequence_skip": ("market", "channel"),
            "resubscribe": ("market",)}.get(kind, ())
    missing = [f"--{n}" for n in need if getattr(args, n) is None]
    if missing:
        print(f"{args.kind} needs {', '.join(missing)}", file=sys.stderr)
        return 2
    target = {k: v for k, v in (("market", args.market), ("channel", args.channel), ("seconds", args.seconds),
                                ("timeout_s", args.timeout if kind == "sequence_skip" else None)) if v is not None}
    try:
        rep = asyncio.run(faults.inject(kind, target, control_path=cfg.control_socket_path, relay_url=cfg.relay_url,
                                        restart_wait_s=args.restart_wait,
                                        log=lambda s: print(f"raw-recorder fault: {s}", file=sys.stderr)))
    except (faults.FaultError, OSError) as exc:
        print(f"raw-recorder fault: {exc} (is the recorder running with [faults] enabled?)", file=sys.stderr)
        return 2
    if rep.get("touched_markets"):
        # The gate needs every touched market ingested, which waits for its window to close.
        from .scheduler import capture_windows, last_window_end
        windows = capture_windows([*load_all_games(cfg), *load_events(cfg.events_discovered_path)], cfg.params)
        end, unknown = last_window_end(windows, rep["touched_markets"])
        if end is not None:
            rep["keep_recording_until_utc"] = _utc_iso(end)
            print(f"raw-recorder fault: touched {len(rep['touched_markets'])} market(s); the last of their windows"
                  f" closes {_utc_iso(end)} ({_et_time(end)}). Keep recording until then, or clv gate a1 fails"
                  " with `market not ingested`", file=sys.stderr)
        if unknown:
            print(f"raw-recorder fault: touched market(s) in no capture window: {', '.join(unknown)}", file=sys.stderr)
    print(json.dumps(rep, indent=2))
    return 0 if rep.get("ok") else 1


def cmd_status(args) -> int:
    from . import status
    from .archive import now_ms
    from .session import running_recorder
    cfg = load_config(args.config)
    games = load_all_games(cfg)
    t = now_ms()
    rep = status.status(cfg, games, t, running_recorder(cfg.archive_root), args.game_pk,
                        events=load_events(cfg.events_discovered_path), event_id=args.event)
    print(status.render(rep, t))
    return rep.exit_code()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="raw-recorder", description=__doc__)
    config_help = "config.toml (default: tools/raw_recorder/config.toml)"
    ap.add_argument("--config", type=Path, help=config_help)
    # --config is also accepted after the subcommand; SUPPRESS keeps a value
    # given before the subcommand from being reset by the subparser default.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--config", type=Path, default=argparse.SUPPRESS, help=config_help)
    sub = ap.add_subparsers(dest="cmd", required=True)

    def add(name: str, **kw) -> argparse.ArgumentParser:
        return sub.add_parser(name, parents=[common], **kw)

    p = add("run", help="record every source for the games in games.toml")
    p.add_argument("--games", type=Path, help="games file (default: the config's games_file)")
    p.set_defaults(fn=cmd_run)
    add("echo", help="test the Novig read key: signed POST /v3/echo, limits, scope").set_defaults(fn=cmd_echo)
    add("catalog", help="run one game discovery round (when no recorder is running) and print the matches"
        ).set_defaults(fn=cmd_catalog)
    p = add("games", help="read-only: the games being recorded over the next days, and discovery's freshness")
    p.add_argument("--days", type=float, default=2, help="how far ahead to list (default 2 days)")
    p.add_argument("--past-hours", type=float, default=12, help="also list windows that ended this recently")
    p.set_defaults(fn=cmd_games)
    p = add("probe-subscriptions", help="experiment: book and trades on one Novig connection")
    p.add_argument("--market", required=True)
    p.add_argument("--event")
    p.add_argument("--wait", type=float, default=3.0, help="seconds between steps")
    p.set_defaults(fn=cmd_probe_subscriptions)
    p = add("mlb-feed", help="archive StatsAPI feed and play-by-play")
    p.add_argument("game_pk", type=int, nargs="*")
    p.add_argument("--date", help="every game on YYYY-MM-DD")
    p.set_defaults(fn=cmd_mlb_feed)
    add("seal", help="seal active segments (SIGHUPs a running recorder)").set_defaults(fn=cmd_seal)
    p = add("verify", help="recheck sealed segments against their journals")
    p.add_argument("path", nargs="?")
    p.set_defaults(fn=cmd_verify)
    p = add("status", help="is the recorder capturing the current (or a given) game? read-only; "
                           "exit 0 OK, 1 WARN, 2 FAIL")
    which = p.add_mutually_exclusive_group()
    which.add_argument("--game-pk", type=int, help="the game to check (default: the current or next capture window)")
    which.add_argument("--event", help="a league event to check, by its Novig event ID (A1)")
    p.set_defaults(fn=cmd_status)
    p = add("backfill", help="B1: archive the historical reference inputs into the backfill root; resumable")
    p.add_argument("sources", nargs="*", choices=list(backfill.SOURCES), help="default: all three")
    p.add_argument("--dates", action="append", metavar="FROM[:TO]",
                   help="only games and files on these dates, YYYY-MM-DD (repeatable; default: everything)")
    p.set_defaults(fn=cmd_backfill)
    p = add("relay", help="Gate A1: the fault relay between the recorder and Novig (raw_recorder.faults)")
    p.add_argument("--listen", help="host:port on loopback (default: [faults] relay_listen)")
    p.add_argument("--upstream", help="the venue's WebSocket origin (default: [faults] relay_upstream)")
    p.set_defaults(fn=cmd_relay)
    p = add("fault", help="Gate A1: label, cause and record one fault in the running recorder")
    p.add_argument("kind", choices=["socket-kill", "process-kill", "channel-stall", "sequence-skip", "resubscribe"])
    p.add_argument("--market", help="the Novig market (channel-stall, sequence-skip, resubscribe)")
    p.add_argument("--channel", choices=["book", "trades"], help="the channel (socket-kill: only its connection)")
    p.add_argument("--seconds", type=float, help="how long a channel-stall lasts")
    p.add_argument("--timeout", type=float, default=300, help="sequence-skip: how long to wait for a delta")
    p.add_argument("--restart-wait", type=float, default=180,
                   help="process-kill: how long to wait for the restarted recorder to take the result")
    p.set_defaults(fn=cmd_fault)
    p = add("report", help="volume per source/day, Odds credit spend, Novig probe counts")
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
