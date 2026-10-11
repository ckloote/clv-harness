"""`raw-recorder status`: is the recorder capturing a game right now, source by source?

Read-only: it reads the archive (sealed and active segments) and the lock file,
and never writes either, so it is safe beside a running recorder. Each check is
OK, WARN or FAIL; the exit code is 0, 1 or 2 for the worst of them.

Checks, for one game's capture window (the current one by default):

- the recorder process holds the archive lock, and its session started with no
  source disabled (a missing key, a disabled section);
- Novig: every configured channel's connection is up, its transport is alive
  (control Pings within stream.transport_liveness_max_s, each answered), every
  snapshot probe was answered, and book/trades sequences are contiguous;
- Kalshi: every ticker is polled at its interval, without failures;
- The Odds API: polled through its window, without failures, with credits left;
- StatsAPI: the feed is fetched once the window has ended;
- free disk above r0.min_free_disk_mb;
- game discovery (when on) ran within two of its intervals, and the game's
  entry is reported with its source and any notes (an ambiguous match).

Sequence contiguity here is an operational signal only; `clv inspect` and
`clv ingest` own the interpretation and record gaps (DESIGN.md §7.2).
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from .archive import PART_SUFFIX, SEALED_SUFFIX, iter_file, utc_day
from .config import Config, Game
from .replay import rest_exchanges, ws_evidence
from .scheduler import Window, capture_windows, odds_windows
from .session import free_disk_mb

OK, WARN, FAIL = "OK", "WARN", "FAIL"
LEVELS = (OK, WARN, FAIL)
DISABLED_EVENTS = {"odds_disabled", "novig_stream_disabled", "sources_disabled", "novig_refused"}
PROBE_GRACE_MS = 10_000     # a probe sent this recently may still be awaiting its reply


@dataclass
class Report:
    game: Game | None = None
    window: Window | None = None
    phase: str = ""
    checks: list[tuple[str, str, str]] = field(default_factory=list)    # (level, area, message)

    def add(self, level: str, area: str, message: str) -> None:
        self.checks.append((level, area, message))

    @property
    def worst(self) -> str:
        return max((c[0] for c in self.checks), key=LEVELS.index, default=OK)

    def exit_code(self) -> int:
        return LEVELS.index(self.worst)


def pick_window(cfg: Config, games: list[Game], now_ms: int, game_pk: int | None) -> Window | None:
    windows = capture_windows(games, cfg.params)
    if game_pk is not None:
        return next((w for w in windows if w.game.game_pk == game_pk), None)
    current = [w for w in windows if w.contains(now_ms)]
    if current:
        return current[0]
    upcoming = [w for w in windows if w.start_ms > now_ms]
    return upcoming[0] if upcoming else (windows[-1] if windows else None)


def frames_between(root: Path, source: str, lo_ms: int, hi_ms: int) -> list[dict]:
    """Frames of one source received in [lo, hi], from sealed and active segments."""
    days = {utc_day(t) for t in range(lo_ms, hi_ms + 1, 3_600_000)} | {utc_day(hi_ms)}
    out = []
    for day in sorted(days):
        d = Path(root) / source / day
        for path in sorted(d.glob(f"*{SEALED_SUFFIX}")) + sorted(d.glob(f"*{PART_SUFFIX}")):
            out += [f for f in iter_file(path) if lo_ms <= f["recv_ts_ms"] <= hi_ms]
    return sorted(out, key=lambda f: f["recv_ts_ms"])


def _event(f: dict) -> dict:
    return json.loads(f["frame"]) if f["dir"] == "event" else {}


def _age(now_ms: int, t_ms: int | None) -> str:
    return "never" if t_ms is None else f"{(now_ms - t_ms) / 1000:,.1f} s ago"


def status(cfg: Config, games: list[Game], now_ms: int, running_pid: int | None,
           game_pk: int | None = None) -> Report:
    root, p = cfg.archive_root, cfg.params
    rep = Report()
    w = pick_window(cfg, games, now_ms, game_pk)
    if w is None:
        rep.add(FAIL, "games", f"no game{f' {game_pk}' if game_pk else ''} in {cfg.games_path} "
                               f"or {cfg.discovered_path}")
        return rep
    rep.game, rep.window = w.game, w
    in_window = w.contains(now_ms)
    rep.phase = "in window" if in_window else ("before window" if now_ms < w.start_ms else "after window")

    # -- the process and its session ------------------------------------------------------
    if running_pid is not None:
        rep.add(OK, "recorder", f"running (pid {running_pid if running_pid > 0 else 'unknown'})")
    else:
        rep.add(FAIL if in_window else WARN, "recorder", "not running: nothing holds the archive lock")
    session = _latest_session(root, now_ms)
    if session is None:
        rep.add(WARN, "recorder", "no session start found in the last 2 days of the archive")
    else:
        start, events = session
        rep.add(OK, "recorder", f"session started {_age(now_ms, start)}")
        for e in events:
            if e["type"] in DISABLED_EVENTS:
                level = WARN if e["type"] == "novig_stream_disabled" else FAIL
                rep.add(level, "recorder", f"{e['type']}: {e.get('reason') or e.get('sources')}")
        errors = [e for e in events if e["type"] in ("task_error", "seal_error")]
        if errors:
            rep.add(WARN, "recorder", f"{len(errors)} task/seal error(s) this session; last: "
                                      f"{errors[-1].get('task')}: {errors[-1].get('error_type')}")
        if any(e["type"] == "disk_low" for e in events):
            rep.add(FAIL, "recorder", "disk_low reported this session")
    free = free_disk_mb(root)
    rep.add(OK if free >= p.min_free_disk_mb else FAIL, "disk", f"{free:,} MB free (minimum {p.min_free_disk_mb:,})")
    _discovery(rep, cfg, w.game, now_ms)

    if now_ms < w.start_ms:
        rep.add(OK, "window", "not open yet: nothing to check per source")
        return rep
    hi = min(now_ms, w.end_ms)
    _novig(rep, cfg, w, frames_between(root, "novig", w.start_ms, hi), now_ms, in_window, session)
    _kalshi(rep, cfg, w, frames_between(root, "kalshi", w.start_ms, hi), now_ms, in_window)
    odds_w = next(o for o in odds_windows([w.game], p))
    _odds(rep, cfg, odds_w, frames_between(root, "odds_api", odds_w.start_ms, min(now_ms, odds_w.end_ms) + 60_000),
          now_ms)
    _mlb(rep, w, frames_between(root, "mlb_statsapi", w.end_ms, now_ms) if now_ms > w.end_ms else [], now_ms)
    return rep


def _latest_session(root: Path, now_ms: int) -> tuple[int, list[dict]] | None:
    frames = frames_between(root, "recorder", now_ms - 2 * 86_400_000, now_ms)
    starts = [f for f in frames if _event(f).get("type") == "process_start"]
    if not starts:
        return None
    last = starts[-1]
    events = [_event(f) for f in frames if f["conn_id"] == last["conn_id"]]
    return last["recv_ts_ms"], events


def _novig(rep: Report, cfg: Config, w: Window, frames: list[dict], now_ms: int, in_window: bool, session) -> None:
    markets = set(w.game.novig_markets)
    stream_off = session is not None and any(e["type"] == "novig_stream_disabled" for e in session[1])
    if stream_off:
        polls = [x for x in rest_exchanges(frames).values() if (x.start or {}).get("purpose") == "public_book"]
        bad = [x for x in polls if x.failure or not 200 <= ((x.complete or {}).get("status") or 0) < 300]
        rep.add(WARN if polls and not bad else FAIL, "novig", f"stream off; public book polled {len(polls)} times, "
                                                             f"{len(bad)} failed")
        return
    conns = {}
    for f in frames:
        e = _event(f)
        if e.get("type") == "ws_connect_attempt" and markets & set(e.get("markets") or []):
            conns[f["conn_id"]] = e.get("channel")
    for channel in cfg.novig.get("channels", []):
        ids = [c for c, ch in conns.items() if ch == channel]
        if not ids:
            rep.add(FAIL if in_window else WARN, f"novig/{channel}", "no connection for this game's market")
            continue
        conn_frames = [f for f in frames if f["conn_id"] in ids]
        ev = ws_evidence(conn_frames)
        events = [e for c in ids for e in ev[c].events]
        drops = Counter(e.get("reason") for e in events if e["type"] == "ws_disconnected")
        last_in = max((f["recv_ts_ms"] for f in conn_frames if f["dir"] == "in"), default=None)
        pings = [t for c in ids for t in ev[c].transport if t.get("op") == "ping" and t.get("direction") == "in"]
        pongs = [t for c in ids for t in ev[c].transport if t.get("op") == "pong" and t.get("direction") == "out"]
        last_ping = max((t["recv_ts_ms"] for t in pings), default=None)
        probes = [pr for c in ids for pr in ev[c].probes.values()]
        unanswered = [pr for pr in probes if pr.reply is None and now_ms - pr.sent_ts_ms > PROBE_GRACE_MS]
        skipped = sum(e["type"] == "probe_skipped" for e in events)
        gaps = _sequence_gaps(conn_frames, markets, channel)
        # Coverage lost between connections: from one connection's last frame to the next's first.
        spans = sorted((min(ts), max(ts)) for ts in (
            [f["recv_ts_ms"] for f in conn_frames if f["conn_id"] == c and f["dir"] == "in"] for c in ids) if ts)
        hole = max((b[0] - a[1] for a, b in zip(spans, spans[1:]) if b[0] > a[1]), default=0)
        live = last_ping is not None and now_ms - last_ping <= cfg.params.transport_liveness_max_s * 1000
        level = OK
        if in_window and (not live or len(ids) > 1 and drops):
            level = FAIL if not live else WARN
        if unanswered or gaps or abs(len(pings) - len(pongs)) > 1 or skipped:
            level = max(level, WARN, key=LEVELS.index)
        rep.add(level, f"novig/{channel}",
                f"{len(ids)} connection(s); last frame {_age(now_ms, last_in)}; last ping {_age(now_ms, last_ping)}; "
                f"pings {len(pings)} / pongs {len(pongs)}; probes {len(probes)} sent, {len(unanswered)} unanswered, "
                f"{skipped} skipped; sequence gaps {gaps}"
                + (f"; disconnects {dict(drops)}, longest uncovered {hole / 1000:.1f} s" if drops else ""))


def _sequence_gaps(frames: list[dict], markets: set[str], channel: str) -> int:
    """Deltas whose sequence is not the previous + 1, per connection and market. A snapshot
    (subscribe or probe reply) resets the baseline only when it is not behind it."""
    last: dict[tuple[str, str], int] = {}
    gaps = 0
    for f in frames:
        if f["dir"] != "in":
            continue
        try:
            msg = json.loads(f["frame"])
        except ValueError:
            continue
        for kind in ("snapshot", "delta"):
            for market, body in (msg.get(kind) or {}).items() if isinstance(msg, dict) else ():
                if market not in markets or channel not in (body or {}):
                    continue
                seq, key = body[channel].get("seq"), (f["conn_id"], market)
                if seq is None:
                    continue
                if kind == "snapshot":
                    if key not in last or seq >= last[key]:
                        last[key] = seq
                elif key in last:
                    if seq > last[key] + 1:
                        gaps += 1
                    last[key] = max(last[key], seq)
    return gaps


def _kalshi(rep: Report, cfg: Config, w: Window, frames: list[dict], now_ms: int, in_window: bool) -> None:
    by_ticker: dict[str, list] = defaultdict(list)
    for x in rest_exchanges(frames).values():
        path = (x.start or {}).get("path", "")
        for t in w.game.kalshi_tickers:
            if t in path and path.endswith("/orderbook"):
                by_ticker[t].append(x)
    stale_ms = 3 * cfg.params.kalshi_poll_interval_s * 1000
    for t in w.game.kalshi_tickers:
        xs = by_ticker.get(t, [])
        bad = [x for x in xs if x.failure or not 200 <= ((x.complete or {}).get("status") or 0) < 300]
        starts = sorted(x.start["start_ts_ms"] for x in xs if x.start)
        last = starts[-1] if starts else None
        longest = max((b - a for a, b in zip(starts, starts[1:])), default=0)
        stale = last is None or now_ms - last > stale_ms
        level = FAIL if in_window and stale else (WARN if bad or longest > stale_ms else OK)
        rep.add(level, f"kalshi/{t.rsplit('-', 1)[-1]}",
                f"{len(xs)} polls, {len(bad)} failed; last {_age(now_ms, last)}; "
                f"longest interval {longest / 1000:.1f} s")


def _odds(rep: Report, cfg: Config, w: Window, frames: list[dict], now_ms: int) -> None:
    if now_ms < w.start_ms:
        rep.add(OK, "odds_api", "window not open yet")
        return
    xs = [x for x in rest_exchanges(frames).values() if (x.start or {}).get("purpose") == "odds"]
    bad = [x for x in xs if x.failure or not 200 <= ((x.complete or {}).get("status") or 0) < 300]
    remaining = None
    for x in xs:
        hdr = {k.lower(): v for k, v in (x.complete or {}).get("response_headers", [])}
        remaining = hdr.get("x-requests-remaining", remaining)
    interval_ms = cfg.params.odds_poll_interval_s * 1000
    expected = (min(now_ms, w.end_ms) - w.start_ms) // interval_ms + 1
    last = max((x.start["start_ts_ms"] for x in xs if x.start), default=None)
    level = FAIL if not xs else (WARN if bad or len(xs) < expected - 1 else OK)
    if remaining is not None and float(remaining) < cfg.params.odds_quota_floor:
        level = FAIL
    span = "window open" if now_ms < w.end_ms else "window closed"
    rep.add(level, "odds_api", f"{span}: {len(xs)} of ~{expected} polls, {len(bad)} failed; last {_age(now_ms, last)}; "
                               f"credits remaining {remaining}")


def _mlb(rep: Report, w: Window, frames: list[dict], now_ms: int) -> None:
    if now_ms <= w.end_ms:
        rep.add(OK, "mlb_statsapi", "feed is fetched when the window ends")
        return
    xs = [x for x in rest_exchanges(frames).values() if f"/{w.game.game_pk}/" in (x.start or {}).get("path", "")]
    ok = [x for x in xs if not x.failure and 200 <= ((x.complete or {}).get("status") or 0) < 300]
    rep.add(OK if ok else WARN, "mlb_statsapi",
            f"{len(ok)} of {len(xs)} feed requests succeeded after the window" if xs else
            "feed not fetched yet (fetch it with `raw-recorder mlb-feed`)")


def render(rep: Report, now_ms: int) -> str:
    from datetime import datetime, timezone

    def iso(ms):
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    lines = []
    if rep.game is not None:
        lines.append(f"{rep.game.label} (gamePk {rep.game.game_pk})")
        lines.append(f"capture window {iso(rep.window.start_ms)} .. {iso(rep.window.end_ms)}: {rep.phase} "
                     f"(now {iso(now_ms)})")
    width = max((len(c[1]) for c in rep.checks), default=0)
    for level, area, message in rep.checks:
        lines.append(f"[{level:>4}] {area:<{width}}  {message}")
    lines.append(f"overall: {rep.worst}")
    return "\n".join(lines)


def _discovery(rep: Report, cfg: Config, game: Game, now_ms: int) -> None:
    from .discovery import last_run
    rep.add(OK, "games", f"{game.game_pk} is {'hand-checked (games.toml)' if game.source == 'manual' else 'discovered'}"
            + "".join(f"; {n}" for n in game.notes))
    if not cfg.discovery.get("enabled", False):
        return
    at, _ = last_run(cfg.discovered_path)
    limit_ms = 2 * float(cfg.discovery.get("interval_s", 3600)) * 1000
    if at is None:
        rep.add(WARN, "games", "discovery has not run yet")
    else:
        rep.add(OK if now_ms - at <= limit_ms else WARN, "games", f"discovery last ran {_age(now_ms, at)}")
