"""`raw-recorder run`: the long-running capture process."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
import traceback
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiohttp

from . import RECORDER_VERSION, discovery, faults, kalshi, leagues, mlb, redact
from .archive import ArchiveWriter, Stream, find_parts, iter_file, now_ms, recover_orphans
from .config import Config, Game, LeagueEvent, combine, load_events, load_games, shadowed
from .novig import public as novig_public
from .novig.signing import NovigSigner
from .novig.stream import NovigStream, TokenBudget, alert_books
from .odds_api import OddsPoller
from .rest import RestRecorder
from .scheduler import Schedule
from .session import (ArchiveLock, SleepInhibitor, clock_status, clock_synced, free_disk_mb, host_info,
                      new_session_id)


class RecorderBusy(RuntimeError):
    pass


def load_signer(cfg: Config) -> NovigSigner | None:
    key_id = cfg.env(cfg.novig, "key_id_env")
    key_path = cfg.env(cfg.novig, "key_path_env")
    if not key_id or not key_path:
        return None
    return NovigSigner.from_pem_file(key_id, Path(key_path))


def orphan_sessions(root: Path) -> list[str]:
    """Session IDs named in the event frames of orphaned `.part` files."""
    sessions = []
    for part in find_parts(root):
        try:
            for frame in iter_file(part):
                if frame["dir"] == "event":
                    sid = json.loads(frame["frame"]).get("session_id")
                    if sid and sid not in sessions:
                        sessions.append(sid)
                    break
        except (ValueError, OSError):
            continue
    return sessions


class Recorder:
    """Records every source for the games to record. Those are the hand-checked list
    (`games`, reread from `games_path` when that file changes) over the discovered list
    (`cfg.discovered_path`, written by the discovery task and reread when it changes), so
    new games never need a restart. League events (`cfg.events_discovered_path`, written by
    the league discovery task, A1) are recorded the same way, on Novig only.

    No capture starts before the host clock is synchronized: a host without a battery-backed
    clock boots with the wrong time, and every recv_ts_ms would be wrong
    (docs/decisions/2026-10-16-a1-host.md)."""

    GAME_FILES_CHECK_S = 5      # how often the game lists are checked for changes
    MLB_CHECK_S = 30            # how often ended windows are checked for feeds to fetch
    WAKE_CHECK_S = 5            # the longest a periodic task sleeps before looking at the wall clock
    ODDS_CHECK_S = 15           # how often the odds poller looks for an odds window that opened
    CATALOG_CHECK_S = 30        # how often newly active Novig markets get their catalog evidence
    CLOCK_CHECK_S = 5           # how often an unsynchronized clock is checked again at startup

    def __init__(self, cfg: Config, games: list[Game], inhibitor: SleepInhibitor | None = None,
                 games_path: Path | None = None):
        self.cfg = cfg
        self.p = cfg.params
        self.manual, self.games_path = games, games_path
        self.games = combine(games, load_games(cfg.discovered_path, "discovered"))
        self.events = load_events(cfg.events_discovered_path)
        self.schedule = Schedule(self.games, cfg.params, self.events)
        self.game_files_stamp = self._game_files_stamp()
        self.session_id = new_session_id()
        self.sid = self.session_id[:12]
        self.lock = ArchiveLock(cfg.archive_root)
        self.inhibitor = inhibitor if inhibitor is not None else SleepInhibitor()
        self.stop = asyncio.Event()
        self.archive: ArchiveWriter | None = None
        self.rec: Stream | None = None
        # Known secret values, scrubbed from any free text we archive or print.
        self.secrets = {v for v in (cfg.env(cfg.odds_api, "key_env"),) if v}
        self.config_warnings: list[str] = []
        self.novig: list[NovigStream] = []      # the live Novig streams, for the fault harness

    # ------------------------------------------------------------- helpers

    def note(self, type_: str, **fields) -> None:
        assert self.rec is not None
        self.rec.event(self.rec.stream_id, type_, **fields)

    def warn_config(self, message: str) -> None:
        """A configuration problem that silently loses evidence (a missing key, a disabled
        source): archived by the caller, and printed now and whenever a capture window opens,
        so it reaches the journal near game time and `raw-recorder status`."""
        self.config_warnings.append(message)
        warn(message)

    def scrub(self, text: str) -> str:
        return redact.scrub(text, self.secrets)

    def note_exception(self, type_: str, name: str, exc: BaseException) -> None:
        tb = "".join(traceback.format_exception(exc))
        self.note(type_, task=name, error_type=type(exc).__name__,
                  error=self.scrub(str(exc)), traceback=self.scrub(tb))
        print(f"[{name}] {type_}: {type(exc).__name__}: {self.scrub(str(exc))}", file=sys.stderr)

    async def every(self, name: str, interval_s: float, fn: Callable[[], Awaitable[None]]) -> None:
        """Run fn every interval_s by the wall clock. The event loop's clock stops while the host
        sleeps, so a sleep is cut into WAKE_CHECK_S pieces and the wall clock read after each:
        a task overdue after a suspend runs within seconds of the wake, not its interval later."""
        next_t = time.time()
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep capturing; make the failure visible
                self.note_exception("task_error", name, exc)
            next_t += interval_s
            if next_t < time.time():
                next_t = time.time()                # behind (a slow call, or a suspend): now, then on schedule
            while (left := next_t - time.time()) > 0:
                await asyncio.sleep(min(left, self.WAKE_CHECK_S))

    @staticmethod
    def _games_summary(games: list[Game]) -> list[dict]:
        return [{"game_pk": g.game_pk, "label": g.label, "source": g.source,
                 "scheduled_start_ms": g.scheduled_start_ms, "novig_markets": list(g.novig_markets),
                 "kalshi_tickers": list(g.kalshi_tickers), "capture_lead_s": g.capture_lead_s,
                 "capture_tail_s": g.capture_tail_s, "notes": list(g.notes)} for g in games]

    @staticmethod
    def _events_summary(events: list[LeagueEvent]) -> list[dict]:
        return [{"league": e.league, "event_id": e.event_id, "label": e.label,
                 "scheduled_start_ms": e.scheduled_start_ms, "novig_markets": list(e.novig_markets),
                 "capture_lead_s": e.capture_lead_s, "capture_tail_s": e.capture_tail_s,
                 "notes": list(e.notes)} for e in events]

    def _game_files_stamp(self) -> tuple:
        out = []
        for path in (self.games_path, self.cfg.discovered_path, self.cfg.events_discovered_path):
            try:
                st = path.stat() if path else None
                out.append((st.st_mtime_ns, st.st_size) if st else None)
            except FileNotFoundError:
                out.append(None)
        return tuple(out)

    def reload_games(self, reason: str) -> None:
        """Reread both game lists and the league events; archive and announce what changed.
        A file that does not parse leaves the current list in place, loudly."""
        self.game_files_stamp = self._game_files_stamp()
        try:
            manual = load_games(self.games_path) if self.games_path else self.manual
            found = load_games(self.cfg.discovered_path, "discovered")
            games = combine(manual, found)
            events = load_events(self.cfg.events_discovered_path)
        except (ValueError, KeyError, OSError) as exc:
            self.note("games_load_failed", reason=reason, error=repr(exc))
            warn(f"game list not reloaded ({reason}): {exc!r}; still recording the previous list")
            return
        if games == self.games and events == self.events:
            return
        if games != self.games:
            for message in shadowed(manual, found):
                self.note("games_shadowed", message=message)
                warn(message)
            before = {g.game_pk for g in self.games}
            self.note("games_loaded", reason=reason, games=self._games_summary(games))
            added = [g for g in games if g.game_pk not in before]
            print(f"raw-recorder: game list reloaded ({reason}): {len(games)} games"
                  + "".join(f"\n  + {g.game_pk} {g.label} [{g.source}]" for g in added), file=sys.stderr, flush=True)
        if events != self.events:
            before_ev = {e.event_id for e in self.events}
            self.note("events_loaded", reason=reason, events=self._events_summary(events))
            added_ev = [e for e in events if e.event_id not in before_ev]
            print(f"raw-recorder: league events reloaded ({reason}): {len(events)} events"
                  + "".join(f"\n  + {e.league} {e.event_id} {e.label}" for e in added_ev), file=sys.stderr, flush=True)
        self.games, self.events = games, events
        self.schedule = Schedule(games, self.p, events)

    def rest(self, http: aiohttp.ClientSession, source: str, name: str) -> RestRecorder:
        stream = self.archive.stream(source, f"{name}-{self.sid}")
        return RestRecorder(http, stream, timeout_s=self.p.rest_timeout_s)

    # ---------------------------------------------------------------- run

    async def run(self) -> None:
        root = self.cfg.archive_root
        if not self.lock.acquire():
            raise RecorderBusy(f"another recorder (pid {self.lock.holder_pid()}) holds {self.lock.path}")
        try:
            unclean_sessions = orphan_sessions(root)
            orphans = recover_orphans(root, session_id=self.session_id)
            self.archive = ArchiveWriter(root, session_id=self.session_id,
                                         segment_max_s=self.p.segment_max_s,
                                         fsync_interval_s=self.p.fsync_interval_s)
            self.rec = self.archive.stream("recorder", f"session-{self.sid}")
            self.note("process_start", recorder_version=RECORDER_VERSION,
                      config_path=str(self.cfg.path), config_sha256=self.cfg.sha256,
                      games=self._games_summary(self.games), events=self._events_summary(self.events),
                      host=host_info(), clock=clock_status())
            if orphans:
                self.note("previous_session_unclean", sessions=unclean_sessions,
                          sealed_files=[e.file for e in orphans])
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop.set)
            loop.add_signal_handler(signal.SIGHUP, self.archive.rotate_all)
            print(f"raw-recorder {RECORDER_VERSION}: session {self.session_id}, "
                  f"{len(self.games)} games, {len(self.events)} league events, archive {root}", file=sys.stderr)
            if await self.wait_for_clock():
                await self._serve()
            self.note("process_stop", reason="signal")
        finally:
            self.inhibitor.release()
            if self.archive is not None:
                self.archive.close()
            self.lock.release()

    async def wait_for_clock(self) -> bool:
        """Hold every source until the host says its clock is synchronized, archiving
        `clock_unsynced` once and `clock_synced` when it is. A host that cannot say (no
        timedatectl or chronyc) is not held: its clock_status is in process_start. Returns
        False if the recorder was stopped while waiting."""
        if clock_synced() is not False:
            return True
        self.note("clock_unsynced", clock=clock_status())
        warn("the host clock is not synchronized: capture waits for it, so no timestamp is wrong")
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), self.CLOCK_CHECK_S)
            except TimeoutError:
                pass
            if clock_synced() is not False:
                self.note("clock_synced", clock=clock_status())
                return True
        return False

    async def _serve(self) -> None:
        async with aiohttp.ClientSession() as http:
            specs = [("housekeeping", self.housekeeping)] + self.source_specs(http)
            if self.cfg.faults.get("enabled", False):        # off unless said: never by default
                specs.append(("fault_control", self.fault_control))
            await self.supervise(specs)

    async def fault_control(self) -> None:
        """Gate A1's control socket (raw_recorder.faults). Announced loudly: it belongs to the
        gate run and its rehearsals, never to an ordinary capture."""
        path = self.cfg.control_socket_path
        self.note("fault_control", socket=str(path), ws_url=self.cfg.novig.get("ws_url"))
        warn(f"fault injection is on ([faults] in {self.cfg.path}): control socket {path}")
        await faults.Control(self.rec, lambda: self.novig).serve(path)

    async def supervise(self, specs: list[tuple[str, Callable[[], Awaitable[None]]]]) -> None:
        """Run every task until stop. A task that ends early (a bug, an
        unexpected exception) is recorded as task_died and restarted with the
        stream.reconnect_backoff_s schedule, so no source stays silently dead
        while the others keep recording."""
        p = self.p
        loop = asyncio.get_running_loop()
        state = {name: p.reconnect_backoff_initial_s for name, _ in specs}

        async def delayed(delay: float, factory) -> None:
            await asyncio.sleep(delay)
            await factory()

        tasks = {asyncio.create_task(factory(), name=name): (name, factory, loop.time())
                 for name, factory in specs}
        stopper = asyncio.create_task(self.stop.wait())
        try:
            while True:
                done, _ = await asyncio.wait(set(tasks) | {stopper},
                                             return_when=asyncio.FIRST_COMPLETED)
                if self.stop.is_set():
                    break
                for t in done:
                    name, factory, started = tasks.pop(t)
                    if loop.time() - started >= p.reconnect_backoff_max_s:
                        state[name] = p.reconnect_backoff_initial_s
                    delay = state[name]
                    state[name] = min(delay * 2, p.reconnect_backoff_max_s)
                    exc = None if t.cancelled() else t.exception()
                    if exc is not None:
                        self.note_exception("task_died", name, exc)
                    else:
                        self.note("task_died", task=name, error_type=None, error="returned early")
                    self.note("task_restart", task=name, delay_s=delay)
                    tasks[asyncio.create_task(delayed(delay, factory), name=name)] = (
                        name, factory, loop.time())
        finally:
            for t in [*tasks, stopper]:
                t.cancel()
            await asyncio.gather(*tasks, stopper, return_exceptions=True)

    def source_specs(self, http: aiohttp.ClientSession) -> list[tuple[str, Callable[[], Awaitable[None]]]]:
        """(name, coroutine factory) for every enabled source; supervise() runs them."""
        cfg = self.cfg
        specs = []
        if cfg.discovery.get("enabled", False):
            specs += self._discovery_specs(http)
        if cfg.enabled_leagues() and cfg.enabled(cfg.novig):
            specs += self._league_specs(http)
        elif cfg.enabled_leagues():
            # League events are found from Novig's catalog and recorded on Novig only: with
            # Novig disabled there is nothing to discover or record for them.
            names = [lg["name"] for lg in cfg.enabled_leagues()]
            self.note("league_discovery_disabled", reason="novig disabled", leagues=names)
            self.warn_config(f"league discovery off ({', '.join(names)}): [novig] is disabled in {cfg.path}")
        if cfg.enabled(cfg.kalshi):
            specs += self._kalshi_specs(http)
        if cfg.enabled(cfg.odds_api):
            specs += self._odds_specs(http)
        if cfg.enabled(cfg.mlb):
            specs += self._mlb_specs(http)
        if cfg.enabled(cfg.novig):
            specs += self._novig_specs(http)
        disabled = [n for n, s in (("kalshi", cfg.kalshi), ("odds_api", cfg.odds_api),
                                   ("mlb", cfg.mlb), ("novig", cfg.novig)) if not cfg.enabled(s)]
        if disabled:
            self.note("sources_disabled", sources=disabled, config_path=str(cfg.path))
            self.warn_config(f"sources disabled in {cfg.path}: {', '.join(disabled)}")
        return specs

    def _discovery_specs(self, http: aiohttp.ClientSession) -> list:
        # Which games to record: the schedule and venue catalogs, archived, matched and merged
        # into the discovered list (raw_recorder.discovery), then reloaded at once.
        cfg = self.cfg
        rests = (self.rest(http, "mlb_statsapi", "discovery"), self.rest(http, "novig", "discovery"),
                 self.rest(http, "kalshi", "discovery") if cfg.enabled(cfg.kalshi) else None)

        warned: set[str] = set()                    # each ambiguity is printed once, archived every round

        async def discovery_round() -> None:
            merged, found = await discovery.discover(cfg, *rests, now_ms())
            self.note("games_discovered", matched=self._games_summary(found.games), notes=found.notes,
                      warnings=found.warnings, discovered_count=len(merged))
            for w in found.warnings:
                if w not in warned:
                    warned.add(w)
                    warn(f"discovery: {w}")
            self.reload_games("discovery")
        return [("discovery", lambda: self.every("discovery", float(cfg.discovery.get("interval_s", 3600)),
                                                 discovery_round))]

    def _league_specs(self, http: aiohttp.ClientSession) -> list:
        # League events (A1): each enabled league's Novig catalog, archived, matched and merged
        # into the discovered events (raw_recorder.leagues), then reloaded at once. The stream
        # name starts "discovery-", so clv.ingest reads it as REST, not a WebSocket.
        cfg = self.cfg
        rest = self.rest(http, "novig", "discovery-leagues")
        warned: set[str] = set()

        async def league_round() -> None:
            merged, found = await leagues.discover(cfg, rest, now_ms())
            self.note("events_discovered", matched=self._events_summary(found.events), notes=found.notes,
                      warnings=found.warnings, discovered_count=len(merged))
            for w in found.warnings:
                if w not in warned:
                    warned.add(w)
                    warn(f"league discovery: {w}")
            self.reload_games("league_discovery")
        return [("league_discovery", lambda: self.every(
            "league_discovery", float(cfg.discovery.get("interval_s", 3600)), league_round))]

    def _kalshi_specs(self, http: aiohttp.ClientSession) -> list:
        # Kalshi order books, with catalog evidence when a ticker becomes active.
        cfg, p, specs = self.cfg, self.p, []
        k_rest = self.rest(http, "kalshi", "kalshi")
        seen_tickers: set[str] = set()

        async def kalshi_round() -> None:
            tickers = self.schedule.kalshi_tickers(now_ms())
            new = tuple(t for t in tickers if t not in seen_tickers)
            if new and await kalshi.fetch_markets(k_rest, cfg.kalshi["base_url"], new):
                seen_tickers.update(new)            # a failed fetch (no network after a wake) is retried
            await kalshi.poll_orderbooks(k_rest, cfg.kalshi["base_url"], tickers)
        specs.append(("kalshi", lambda: self.every("kalshi", p.kalshi_poll_interval_s, kalshi_round)))
        return specs

    def _odds_specs(self, http: aiohttp.ClientSession) -> list:
        # The Odds API, only inside odds windows, stopping at the credit floor.
        cfg, p, specs = self.cfg, self.p, []
        o_rest = self.rest(http, "odds_api", "odds")
        odds = OddsPoller(o_rest, o_rest.stream, cfg.odds_api, cfg.env(cfg.odds_api, "key_env"),
                          p.odds_quota_floor)
        if odds.api_key is None:
            self.warn_config(f"The Odds API disabled: ${cfg.odds_api.get('key_env')} is not set, so no sportsbook "
                             f"quotes are recorded. Set it in the service's environment file and restart.")

        async def odds_loop() -> None:
            await odds.check_quota()
            last_poll: list[int | None] = [None]

            async def odds_round() -> None:
                # Poll as an odds window opens, then every odds_poll_interval_s while any is open.
                # A request that got no response at all (the network not back after a wake) is
                # retried at the next check; one the API answered counts, whatever its status.
                t = now_ms()
                if self.schedule.odds_active(t) and (
                        last_poll[0] is None or t - last_poll[0] >= p.odds_poll_interval_s * 1000):
                    res = await odds.poll()
                    if res is not None and res.status is not None:
                        last_poll[0] = t
            await self.every("odds_api", self.ODDS_CHECK_S, odds_round)
        specs.append(("odds_api", odds_loop))
        return specs

    def _mlb_specs(self, http: aiohttp.ClientSession) -> list:
        # MLB feeds when each capture window ends, and again `mlb.refetch_after_s` later for
        # the final data and scoring corrections (`mlb-feed` re-fetches by hand).
        cfg, specs = self.cfg, []
        m_rest = self.rest(http, "mlb_statsapi", "mlb")
        last_check = [now_ms()]
        delays_ms = [0] + [int(d) * 1000 for d in cfg.mlb.get("refetch_after_s", [])]

        async def mlb_round() -> None:
            t = now_ms()
            due = {w.game.game_pk for d in delays_ms for w in self.schedule.ended_between(last_check[0] - d, t - d)}
            for pk in sorted(due):
                await mlb.fetch_game(m_rest, cfg.mlb["base_url"], pk)
            last_check[0] = t
        specs.append(("mlb", lambda: self.every("mlb", self.MLB_CHECK_S, mlb_round)))
        return specs

    def _novig_specs(self, http: aiohttp.ClientSession) -> list:
        # Novig: stream with a read key; otherwise (or additionally) the public book.
        cfg, p, specs = self.cfg, self.p, []
        signer = load_signer(cfg)
        n_rest = self.rest(http, "novig", "rest")
        mode = cfg.novig.get("public_book_poll", "auto")
        seen_markets: set[str] = set()

        async def novig_catalog_round() -> None:
            new = self.schedule.novig_markets(now_ms()) - seen_markets
            if new:                                 # a market whose fetch failed is retried next round
                seen_markets.update(await novig_public.fetch_markets(n_rest, cfg.novig["host"], frozenset(new)))
        specs.append(("novig_catalog", lambda: self.every("novig_catalog", self.CATALOG_CHECK_S, novig_catalog_round)))

        if signer is None:
            self.note("novig_stream_disabled", reason="no read key configured "
                      f"(${cfg.novig['key_id_env']}, ${cfg.novig['key_path_env']})")
            self.warn_config(f"Novig stream disabled: no read key (${cfg.novig['key_id_env']}, "
                             f"${cfg.novig['key_path_env']}); only the unsigned public book is polled.")
        else:
            specs.append(("novig_streams", lambda: self.novig_streams(http, signer, n_rest)))
        if mode == "always" or (mode == "auto" and signer is None):
            async def public_round() -> None:
                markets = self.schedule.novig_markets(now_ms())
                if markets:
                    await novig_public.poll_books(n_rest, cfg.novig["host"], markets,
                                                  int(cfg.novig.get("public_book_depth", 20)))
            specs.append(("novig_public_book", lambda: self.every(
                "novig_public_book", p.novig_public_book_poll_interval_s, public_round)))
        return specs

    async def novig_streams(self, http: aiohttp.ClientSession, signer: NovigSigner,
                            n_rest: RestRecorder) -> None:
        cfg = self.cfg
        host = cfg.novig["host"]
        # A trading::read key cannot read key metadata (management scopes only),
        # so a 200 here means a management key was loaded: refuse to run.
        key = await n_rest.request("GET", host, f"/v3/keys/{signer.key_id}", signer=signer,
                                   purpose="scope_check")
        if key.status == 200:
            self.note("novig_refused", reason="key can read /v3/keys: a management key, never allowed")
            print("refusing to stream: the configured Novig key has management scope", file=sys.stderr)
            self.stop.set()
            return
        budget = TokenBudget()
        limits = await n_rest.request("GET", host, "/v3/limits", signer=signer, purpose="limits")
        if limits.ok and limits.body:
            try:
                s = json.loads(limits.body)["stream"]
                budget.configure(s["capacity"], s["refillPerSec"])
            except (ValueError, KeyError, TypeError) as exc:
                self.note("limits_unparsed", error=repr(exc), request_id=limits.request_id)

        async def diagnose() -> None:
            await n_rest.request("GET", host, "/v3/limits", signer=signer, purpose="ws_refusal_diagnostic")

        streams = self.novig = [NovigStream(http=http, archive=self.archive, signer=signer,
                                            ws_url=cfg.novig["ws_url"], channel=ch,
                                            markets_fn=lambda: self.schedule.novig_markets(now_ms()),
                                            params=self.p, budget=budget, on_handshake_failure=diagnose,
                                            dense_fn=lambda: self.schedule.dense_markets(now_ms()),
                                            on_lifecycle=lambda m: alert_books(self.novig, m))
                                for ch in cfg.novig["channels"]]
        await asyncio.gather(*(s.run() for s in streams))

    async def housekeeping(self) -> None:
        hourly_at = 0.0
        loop = asyncio.get_running_loop()
        reported_errors = 0
        files_checked_at = 0.0
        while True:
            self.archive.tick()
            if loop.time() >= files_checked_at:         # a hand edit of games.toml, or discovery
                files_checked_at = loop.time() + self.GAME_FILES_CHECK_S
                if self._game_files_stamp() != self.game_files_stamp:
                    self.reload_games("file_changed")
            errors = self.archive.seal_errors
            for exc in errors[reported_errors:]:
                self.note_exception("seal_error", "sealer", exc)
            reported_errors = len(errors)

            active = self.schedule.any_active(now_ms())
            if active and not self.inhibitor.held:
                err = self.inhibitor.acquire()
                self.note("sleep_inhibit", held=err is None, error=err)
                for message in self.config_warnings:      # again as a capture window opens
                    warn(message)
            elif not active and self.inhibitor.held:
                self.inhibitor.release()
                self.note("sleep_inhibit", held=False, error=None)

            if loop.time() >= hourly_at:
                hourly_at = loop.time() + 3600
                free = free_disk_mb(self.cfg.archive_root)
                self.note("host_status", clock=clock_status(), free_disk_mb=free)
                if free < self.p.min_free_disk_mb:
                    self.note("disk_low", free_disk_mb=free, min_free_disk_mb=self.p.min_free_disk_mb)
                    warn(f"{free} MB free under {self.cfg.archive_root}")
            await asyncio.sleep(1)


def warn(message: str) -> None:
    """A warning on stderr. Under systemd (JOURNAL_STREAM is set) the `<4>` prefix gives it
    warning priority, so `journalctl -p warning` and `systemctl status` show it."""
    prefix = "<4>" if os.environ.get("JOURNAL_STREAM") else ""
    print(f"{prefix}raw-recorder: WARNING: {message}", file=sys.stderr, flush=True)


def signal_running_recorder(root: Path, sig: int = signal.SIGHUP) -> int | None:
    """If a recorder holds the archive lock, send it `sig` and return its PID."""
    lock = ArchiveLock(root)
    if lock.acquire():
        lock.release()
        return None
    pid = lock.holder_pid()
    if pid:
        os.kill(pid, sig)
    return pid
