"""`raw-recorder run`: the long-running capture process."""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import traceback
from collections.abc import Awaitable, Callable
from pathlib import Path

import aiohttp

from . import RECORDER_VERSION, kalshi, mlb, redact
from .archive import ArchiveWriter, Stream, find_parts, iter_file, now_ms, recover_orphans
from .config import Config, Game
from .novig import public as novig_public
from .novig.signing import NovigSigner
from .novig.stream import NovigStream, TokenBudget
from .odds_api import OddsPoller
from .rest import RestRecorder
from .scheduler import Schedule
from .session import (ArchiveLock, SleepInhibitor, clock_status, free_disk_mb, host_info,
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
    def __init__(self, cfg: Config, games: list[Game], inhibitor: SleepInhibitor | None = None):
        self.cfg = cfg
        self.p = cfg.params
        self.schedule = Schedule(games, cfg.params)
        self.games = games
        self.session_id = new_session_id()
        self.sid = self.session_id[:12]
        self.lock = ArchiveLock(cfg.archive_root)
        self.inhibitor = inhibitor if inhibitor is not None else SleepInhibitor()
        self.stop = asyncio.Event()
        self.archive: ArchiveWriter | None = None
        self.rec: Stream | None = None
        # Known secret values, scrubbed from any free text we archive or print.
        self.secrets = {v for v in (cfg.env(cfg.odds_api, "key_env"),) if v}

    # ------------------------------------------------------------- helpers

    def note(self, type_: str, **fields) -> None:
        assert self.rec is not None
        self.rec.event(self.rec.stream_id, type_, **fields)

    def scrub(self, text: str) -> str:
        return redact.scrub(text, self.secrets)

    def note_exception(self, type_: str, name: str, exc: BaseException) -> None:
        tb = "".join(traceback.format_exception(exc))
        self.note(type_, task=name, error_type=type(exc).__name__,
                  error=self.scrub(str(exc)), traceback=self.scrub(tb))
        print(f"[{name}] {type_}: {type(exc).__name__}: {self.scrub(str(exc))}", file=sys.stderr)

    async def every(self, name: str, interval_s: float, fn: Callable[[], Awaitable[None]]) -> None:
        loop = asyncio.get_running_loop()
        next_t = loop.time()
        while True:
            try:
                await fn()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # keep capturing; make the failure visible
                self.note_exception("task_error", name, exc)
            next_t += interval_s
            delay = next_t - loop.time()
            if delay < 0:
                next_t, delay = loop.time(), 0
            await asyncio.sleep(delay)

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
                      games=[{"game_pk": g.game_pk, "label": g.label,
                              "scheduled_start_ms": g.scheduled_start_ms,
                              "novig_markets": list(g.novig_markets),
                              "kalshi_tickers": list(g.kalshi_tickers)} for g in self.games],
                      host=host_info(), clock=clock_status())
            if orphans:
                self.note("previous_session_unclean", sessions=unclean_sessions,
                          sealed_files=[e.file for e in orphans])
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(sig, self.stop.set)
            loop.add_signal_handler(signal.SIGHUP, self.archive.rotate_all)
            print(f"raw-recorder {RECORDER_VERSION}: session {self.session_id}, "
                  f"{len(self.games)} games, archive {root}", file=sys.stderr)
            await self._serve()
            self.note("process_stop", reason="signal")
        finally:
            self.inhibitor.release()
            if self.archive is not None:
                self.archive.close()
            self.lock.release()

    async def _serve(self) -> None:
        async with aiohttp.ClientSession() as http:
            specs = [("housekeeping", self.housekeeping)] + self.source_specs(http)
            await self.supervise(specs)

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
        """(name, coroutine factory) for every source; supervise() runs them."""
        cfg, p = self.cfg, self.p
        specs = []

        # Kalshi order books, with catalog evidence when a ticker becomes active.
        k_rest = self.rest(http, "kalshi", "kalshi")
        seen_tickers: set[str] = set()

        async def kalshi_round() -> None:
            tickers = self.schedule.kalshi_tickers(now_ms())
            new = tuple(t for t in tickers if t not in seen_tickers)
            if new:
                await kalshi.fetch_markets(k_rest, cfg.kalshi["base_url"], new)
                seen_tickers.update(new)
            await kalshi.poll_orderbooks(k_rest, cfg.kalshi["base_url"], tickers)
        specs.append(("kalshi", lambda: self.every("kalshi", p.kalshi_poll_interval_s, kalshi_round)))

        # The Odds API, only inside odds windows, stopping at the credit floor.
        o_rest = self.rest(http, "odds_api", "odds")
        odds = OddsPoller(o_rest, o_rest.stream, cfg.odds_api, cfg.env(cfg.odds_api, "key_env"),
                          p.odds_quota_floor)

        async def odds_loop() -> None:
            await odds.check_quota()

            async def odds_round() -> None:
                if self.schedule.odds_active(now_ms()):
                    await odds.poll()
            await self.every("odds_api", p.odds_poll_interval_s, odds_round)
        specs.append(("odds_api", odds_loop))

        # MLB feeds when each capture window ends (re-fetch later with `mlb-feed`).
        m_rest = self.rest(http, "mlb_statsapi", "mlb")
        last_check = [now_ms()]

        async def mlb_round() -> None:
            t = now_ms()
            for w in self.schedule.ended_between(last_check[0], t):
                await mlb.fetch_game(m_rest, cfg.mlb["base_url"], w.game.game_pk)
            last_check[0] = t
        specs.append(("mlb", lambda: self.every("mlb", 30, mlb_round)))

        # Novig: stream with a read key; otherwise (or additionally) the public book.
        signer = load_signer(cfg)
        n_rest = self.rest(http, "novig", "rest")
        mode = cfg.novig.get("public_book_poll", "auto")
        seen_markets: set[str] = set()

        async def novig_catalog_round() -> None:
            new = self.schedule.novig_markets(now_ms()) - seen_markets
            if new:
                await novig_public.fetch_markets(n_rest, cfg.novig["host"], frozenset(new))
                seen_markets.update(new)
        specs.append(("novig_catalog", lambda: self.every("novig_catalog", 30, novig_catalog_round)))

        if signer is None:
            self.note("novig_stream_disabled", reason="no read key configured "
                      f"(${cfg.novig['key_id_env']}, ${cfg.novig['key_path_env']})")
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

        streams = [NovigStream(http=http, archive=self.archive, signer=signer,
                               ws_url=cfg.novig["ws_url"], channel=ch,
                               markets_fn=lambda: self.schedule.novig_markets(now_ms()),
                               params=self.p, budget=budget, on_handshake_failure=diagnose)
                   for ch in cfg.novig["channels"]]
        await asyncio.gather(*(s.run() for s in streams))

    async def housekeeping(self) -> None:
        hourly_at = 0.0
        loop = asyncio.get_running_loop()
        reported_errors = 0
        while True:
            self.archive.tick()
            errors = self.archive.seal_errors
            for exc in errors[reported_errors:]:
                self.note_exception("seal_error", "sealer", exc)
            reported_errors = len(errors)

            active = self.schedule.any_active(now_ms())
            if active and not self.inhibitor.held:
                err = self.inhibitor.acquire()
                self.note("sleep_inhibit", held=err is None, error=err)
            elif not active and self.inhibitor.held:
                self.inhibitor.release()
                self.note("sleep_inhibit", held=False, error=None)

            if loop.time() >= hourly_at:
                hourly_at = loop.time() + 3600
                free = free_disk_mb(self.cfg.archive_root)
                self.note("host_status", clock=clock_status(), free_disk_mb=free)
                if free < self.p.min_free_disk_mb:
                    self.note("disk_low", free_disk_mb=free, min_free_disk_mb=self.p.min_free_disk_mb)
                    print(f"WARNING: {free} MB free under {self.cfg.archive_root}", file=sys.stderr)
            await asyncio.sleep(1)


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
