"""Ingest one recorded game from the raw archive into the V0 fact tables.

Every row cites the sealed segment and line it came from (raw_artifact,
raw_line). The ingest is deterministic for a given archive, game list,
parser version and `now_ms`, so a clean database reproduces it exactly.

A league event (A1, `ingest_league_event`) is ingested from the venue side only:
its Novig instruments, books, liveness evidence and collection gaps, with no
event, outcome, mapping or off observation, since which league game it is is
decided in A2. A `venue_ingest` row (migrations/0006_a1.sql) records it, and it
is ingested only once its window has settled: `ingest.window_settle_s` after
it ends, with no active segment overlapping it.

What goes where (migrations/0001_v0.sql):

- identity: event (gamePk), outcomes (the two teams), event_alias per provider,
  venue_instrument per Novig market, Kalshi ticker and sportsbook line, and
  instrument_mapping_observation per native side.
- books: Novig stream and public-poll states, and Kalshi polls, become a tick
  when the top `book.tick_levels` or the venue status change, and on the first
  observation after a subscription, resync, failed poll or recorder restart (so a
  tick never spans a gap), and liveness_evidence when they don't; nothing vouches
  for a stored state once a later one was quarantined or lost to a sequence gap;
  book_snapshot holds complete ladders at subscription, after a resync, on a
  recorder session's first poll and every `book.full_snapshot_interval_s`.
- continuity and time: collection_gap open/close pairs, including a quarantine gap when a
  quarantined state replaces the stored one (from its last trustworthy observation to the
  next stored state, so a close never prices a replaced book); off_observation per sighting.

Sportsbook quotes are not stored as facts here: an entry cites the quote it
uses (entry_quote_observation, clv.entries).
"""
from __future__ import annotations

import dataclasses
import json
import sqlite3
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from clv import archive, gaps, mlb
from clv.config import param
from clv.games import (
    GAMES,
    capture_window,
    league_events,
    league_window,
    load_game,
    load_league_event,
)
from clv.identity import (
    IdentityConflict,
    ensure_event,
    ensure_instrument,
    ensure_mapping,
    ensure_outcome,
    novig_abbr,
)
from clv.off import sources
from clv.scoring import odds_api
from clv.timeutil import iso_ms, ms_iso
from clv.venues import kalshi
from clv.venues.novig import parser as novig
from clv.venues.protocol import BinaryBook, Level

PARSER_VERSION = "v0.1"
# The recorder's Novig REST streams (raw_recorder.runtime and cli); any other stream_id is a WebSocket.
WS_PREFIXES = ("rest-", "catalog-", "echo-", "discovery-")


class IngestError(Exception):
    pass


@dataclass
class IngestReport:
    game_pk: int | None                     # an MLB game; None for a league event
    event_id: int | None                    # its event row; a league event has none until A2
    rows: Counter = field(default_factory=Counter)
    quarantined: Counter = field(default_factory=Counter)        # reason -> count
    quarantine_span: dict = field(default_factory=dict)         # reason -> (first ref, last ref, first ms, last ms)
    unmatched: dict = field(default_factory=dict)               # vendor event id -> why it is not this game

    native_event_id: str | None = None      # a league event's Novig event

    def quarantine(self, reason: str, ref: str, ts_ms: int) -> None:
        """Never stored as a fact; the raw archive keeps the frame (DESIGN.md §2.4)."""
        self.quarantined[reason] += 1
        first = self.quarantine_span.get(reason, (ref, ref, ts_ms, ts_ms))
        self.quarantine_span[reason] = (first[0], ref, first[2], ts_ms)


class Writer:
    """Inserts rows and resolves frame citations to (raw_artifact_id, raw_line)."""

    def __init__(self, conn: sqlite3.Connection, now_ms: int):
        self.conn, self.now = conn, now_ms
        self.segments: dict[str, archive.Segment] = {}
        self.artifact_ids: dict[str, int] = {}
        self.rows: Counter = Counter()

    def know(self, segs):
        for s in segs:
            self.segments[s.relpath] = s

    def cite(self, ref: str) -> tuple[int, int]:
        relpath, _, line = ref.rpartition("#")
        if relpath not in self.artifact_ids:
            s = self.segments[relpath]
            row = self.conn.execute("SELECT raw_artifact_id FROM raw_artifact WHERE relpath = ? AND parser_version = ?",
                                    (relpath, PARSER_VERSION)).fetchone()
            self.artifact_ids[relpath] = row[0] if row else self.insert("raw_artifact", dict(
                source=s.source, relpath=relpath, sha256=s.sha256, bytes=s.bytes, frame_count=s.frame_count,
                first_recv_ts_ms=s.first_recv_ts_ms, last_recv_ts_ms=s.last_recv_ts_ms, session_id=s.session_id,
                recorder_version=s.recorder_version, redaction_policy=s.redaction_policy,
                unclean_close=int(s.unclean_close), parser_version=PARSER_VERSION, ingested_ts_ms=self.now))
        return self.artifact_ids[relpath], int(line)

    def insert(self, table: str, row: dict) -> int:
        cols = ", ".join(row)
        cur = self.conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({', '.join('?' * len(row))})", tuple(row.values()))
        self.rows[table] += 1
        return cur.lastrowid

    def cited(self, ref: str, prefix: str = "") -> dict:
        aid, line = self.cite(ref)
        return {f"{prefix}raw_artifact_id": aid, f"{prefix}raw_line": line}


def payout_cents(qty: Decimal, cents_per_contract: int, ref: str) -> int:
    cents = qty * cents_per_contract
    if cents != cents.to_integral_value():
        raise IngestError(f"{ref}: {qty} contracts at {cents_per_contract} cents is not a whole number of cents")
    return int(cents)


class _Books:
    """Writes ticks, liveness evidence and complete-ladder snapshots for one instrument."""

    def __init__(self, w: Writer, instrument_id: int, subject: str, sides: tuple[str, str], cents_per_contract: int):
        self.w, self.iid, self.subject, self.sides, self.cents = w, instrument_id, subject, sides, cents_per_contract
        self.n = param("book.tick_levels")
        self.snapshot_interval_ms = param("book.full_snapshot_interval_s") * 1000
        self.last_state: dict[str, tuple | None] = {}  # source -> top-N state of the last tick
        self.last_snapshot_ms: dict[str, int] = {}
        # source -> (book, snapshot ref) of the last stored state, while it is still the
        # current one; None once a later state was quarantined or became untrusted.
        self.current: dict[str, tuple | None] = {}
        self.last_good: dict[str, tuple[int, str]] = {}    # source -> last tick or evidence (ms, ref)
        self.quarantine_gap: dict[str, tuple[int, str]] = {}   # source -> open collection_gap (id, reason)

    def scope(self, source: str) -> str:
        return f"{self.subject}/book" if source == "stream" else self.subject

    def observe(self, book: BinaryBook, status: str, ref: str, snapshot_ref: str | None,
                evidence_kind: str, snapshot_reason: str | None, report: IngestReport) -> None:
        ladders = [book.bids[s] for s in self.sides]
        best = [lv[0].price_e4 if lv else None for lv in ladders]
        if None not in best and best[0] + best[1] > 10_000:
            report.quarantine("crossed_book", ref, book.recv_ts_ms)
            self.quarantined(book.source, "crossed_book")       # one of gaps.QUARANTINE_REASONS
            return
        self.current[book.source] = (book, snapshot_ref)
        top = tuple(tuple((lv.price, lv.qty) for lv in ladder[: self.n]) for ladder in ladders)
        state = (status, book.complete, top)
        # After a subscription, resync or recorder restart the state is re-established, so it
        # is a new tick even if unchanged: a tick never spans a gap.
        if self.last_state.get(book.source) == state and snapshot_reason is None:
            self.w.insert("liveness_evidence", dict(venue_instrument_id=self.iid, source=book.source, kind=evidence_kind,
                                                    observed_ts_ms=book.recv_ts_ms, **self.w.cited(ref)))
            self.last_good[book.source] = (book.recv_ts_ms, ref)
        else:
            self.last_state[book.source] = state
            tick = self.w.insert("tick", dict(
                venue_instrument_id=self.iid, source=book.source, seq=book.seq, venue_status=status,
                venue_ts_ms=book.venue_ts_ms, observed_ts_ms=book.recv_ts_ms, bid0_e4=best[0], bid1_e4=best[1],
                levels0=len(top[0]), levels1=len(top[1]), truncated=int(any(len(lv) > self.n for lv in ladders)),
                ladder_complete=int(book.complete), **self.w.cited(ref),
                **(self.w.cited(snapshot_ref, "snapshot_") if snapshot_ref else
                   {"snapshot_raw_artifact_id": None, "snapshot_raw_line": None})))
            self._levels("tick_level", "tick_id", tick, [ladder[: self.n] for ladder in ladders], ref)
            self.last_good[book.source] = (book.recv_ts_ms, ref)
            if (opened := self.quarantine_gap.pop(book.source, None)) is not None:
                self._gap_row("close", opened[0], book.source, book.recv_ts_ms, ref, opened[1])
        due = book.recv_ts_ms - self.last_snapshot_ms.get(book.source, -10**15) >= self.snapshot_interval_ms
        if snapshot_reason or due:
            self.last_snapshot_ms[book.source] = book.recv_ts_ms
            snap = self.w.insert("book_snapshot", dict(
                venue_instrument_id=self.iid, reason=snapshot_reason or "periodic", source=book.source, seq=book.seq,
                venue_status=status, venue_ts_ms=book.venue_ts_ms, observed_ts_ms=book.recv_ts_ms,
                ladder_complete=int(book.complete), **self.w.cited(ref)))
            self._levels("book_snapshot_level", "book_snapshot_id", snap, ladders, ref)

    def _levels(self, table: str, key: str, owner: int, ladders: list[tuple[Level, ...]], ref: str) -> None:
        rows = [(owner, side, rank, lv.price, lv.price_e4, str(lv.qty), payout_cents(lv.qty, self.cents, ref))
                for side, ladder in enumerate(ladders) for rank, lv in enumerate(ladder)]
        self.w.conn.executemany(f"INSERT INTO {table} ({key}, side, rank, price_native, price_e4, qty_native, payout_cents)"
                                " VALUES (?, ?, ?, ?, ?, ?, ?)", rows)
        self.w.rows[table] += len(rows)

    def evidence(self, source: str, kind: str, observed_ms: int, ref: str) -> None:
        """Evidence that the last stored state is still current; nothing while it isn't."""
        if self.current.get(source) is None:
            return
        self.w.insert("liveness_evidence", dict(venue_instrument_id=self.iid, source=source, kind=kind,
                                                observed_ts_ms=observed_ms, **self.w.cited(ref)))
        self.last_good[source] = (observed_ms, ref)

    def quarantined(self, source: str, reason: str) -> None:
        """A state that cannot be stored replaced the stored one. If that state was still current,
        a collection gap opens at its last trustworthy observation, on the book's scope, and the
        next stored state closes it: so the close function, reading only facts, never prices the
        replaced book after this point."""
        if self.current.get(source) is not None and source not in self.quarantine_gap:
            ms, ref = self.last_good[source]
            self.quarantine_gap[source] = (self._gap_row("open", None, source, ms, ref, reason), reason)
        self.untrusted(source)

    def _gap_row(self, kind: str, opens: int | None, source: str, ms: int, ref: str, reason: str) -> int:
        return self.w.insert("collection_gap", dict(
            kind=kind, opens_gap_id=opens, source=self.subject.split(":", 1)[0], scope=self.scope(source),
            reason=reason, boundary_ts_ms=ms, observed_ts_ms=self.w.now, **self.w.cited(ref)))

    def untrusted(self, source: str) -> None:
        """The source's current state is not stored (quarantined, or a sequence gap): no
        evidence may vouch for the last tick, and the next stored state is a new tick."""
        self.current[source] = None
        self.last_state[source] = None

    def status_change(self, source: str, status: str, ref: str, recv_ts_ms: int, venue_ts_ms: int | None,
                      report: IngestReport) -> None:
        """A venue status change is a new tick even when the book is unchanged, so a stored
        state never carries a status the venue has already left."""
        if self.current.get(source) is None:
            return                  # no current stored state to restate; the next one carries the status
        book, snapshot_ref = self.current[source]
        restated = dataclasses.replace(book, recv_ts_ms=recv_ts_ms, venue_ts_ms=venue_ts_ms)
        self.observe(restated, status, ref, snapshot_ref, "delta_unchanged", None, report)


def _stream_frame(report: IngestReport, books: _Books, status: dict[str, str], market: str,
                  outs: list, resynced: set[str], off: Callable[[novig.Lifecycle], None] | None) -> None:
    """One stream frame's outputs for one market. `off` records a lifecycle transition as an
    off observation; a league event (A1) has no event to attach one to yet, so it passes None.

    Order: a sequence gap first makes the stored state untrusted; then the frame's status
    is taken (the replay has already dropped a status behind the lifecycle sequence, which
    is independent of the book's); then the frame's own book, if any, is observed with that
    status. Only a frame with no book of its own restates the stored book with a new status:
    the probe confirmed the book, or was behind it and proves nothing about it.
    """
    probes = [o for o in outs if isinstance(o, novig.ProbeCheck) and o.channel == "book"]
    new_books = [o for o in outs if isinstance(o, BinaryBook)]
    for o in outs:
        if isinstance(o, novig.SequenceGap) and o.channel == "book":
            books.untrusted("stream")
        elif isinstance(o, novig.Resync) and o.channel == "book":
            resynced.add(market)
    changed = None
    for o in outs:
        if not isinstance(o, novig.Lifecycle):
            continue
        if off is not None:
            off(o)
        if o.status != status[market]:
            status[market] = o.status
            changed = o
    for o in new_books:
        snapshot_ref, ref = o.refs[0], o.refs[-1]
        reason = None
        if ref == snapshot_ref:     # this frame is itself the snapshot
            reason = "resync" if market in resynced else "subscribe"
            resynced.discard(market)
        books.observe(o, status[market], ref, snapshot_ref, "delta_unchanged", reason, report)
    if changed is not None and not new_books:
        books.status_change("stream", changed.status, changed.ref, changed.recv_ts_ms, changed.venue_ts_ms, report)
    for p in probes:
        if p.status == "confirmed":
            books.evidence("stream", "probe_confirmed", p.recv_ts_ms, p.ref)


def _novig_catalog(w: Writer, root: Path, lo: int, hi: int, markets) -> tuple[list, list, dict[str, tuple]]:
    """Novig's REST exchanges in the window, its WebSocket segments overlapping it, and each
    wanted market's first catalog response (its own, or a league listing)."""
    novig_segs = archive.overlapping(archive.segments(root, "novig"), lo, hi)
    w.know(novig_segs)
    ws = [s for s in novig_segs if not s.stream_id.startswith(WS_PREFIXES)]
    novig_x = list(archive.rest_exchanges(f for f in archive.iter_frames(root, [s for s in novig_segs if s not in ws])
                                          if lo <= f.recv_ts_ms <= hi))
    catalog: dict[str, tuple] = {}
    for x in novig_x:
        for m in novig.catalog_markets(x):          # the market's own response, or a league listing
            if m.market in markets and m.market not in catalog:
                catalog[m.market] = (x, m)
    return novig_x, ws, catalog


def _novig_instrument(w: Writer, x, m) -> tuple[int, tuple[str, str]]:
    (s0, n0, _), (s1, n1, _) = m.outcomes
    return ensure_instrument(w, dict(
        venue="novig", native_id=m.market, native_event_id=m.event, operator=None, kind="exchange_book",
        side0_id=s0, side0_label=n0, side1_id=s1, side1_label=n1, payout_currency="USD",
        payout_cents_per_contract=int(novig.PAYOUT_USD_PER_CONTRACT * 100), quantity_increment="1",
        first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))


def _novig_record(w: Writer, report: IngestReport, root: Path, novig_x: list, ws: list, books: dict[str, _Books],
                  status: dict[str, str], catalog_outcomes: dict, off) -> tuple[list, novig.StreamReplay, list]:
    """Novig's public book polls, then its stream replay, into the books' facts. Returns the
    stream frames, the fed replay and the poll gaps, for the caller's collection gaps."""
    novig_poll_gaps = gaps.poll_gaps("novig", novig_x, {"public_book"})
    poll_reason = _poll_reasons(novig_poll_gaps)
    last_session: dict[str, str] = {}
    for x in novig_x:
        if not x.ok:
            continue
        if x.request.get("purpose") == "catalog":
            for m in novig.catalog_markets(x):
                if m.market in books and m.status != status[m.market]:
                    status[m.market] = m.status
                    books[m.market].status_change("poll", m.status, x.body.ref, m.recv_ts_ms, None, report)
            continue
        if x.request.get("purpose") != "public_book":
            continue
        book = novig.public_book(x, catalog_outcomes.get(x.path.split("/")[-2], ()))   # .../markets/{id}/book
        if book.market not in books:
            continue
        first_of_session = last_session.get(book.market) != x.request["session_id"]
        last_session[book.market] = x.request["session_id"]
        books[book.market].observe(book, status[book.market], x.body.ref, None, "poll_unchanged",
                                   poll_reason(x, first_of_session), report)

    # One connection carries many markets. Only frames naming one of ours can change their
    # books (the replay keeps each market's state apart), so the rest are not replayed; every
    # command and recorder event is kept, since gaps.stream_gaps reads them.
    ours = tuple(books)
    stream_frames = [f for f in archive.iter_frames(root, ws) if f.dir != "in" or any(m in f.frame for m in ours)]
    replay = novig.StreamReplay(catalog_outcomes)
    resynced: set[str] = set()
    for f in stream_frames:
        # A frame is handled as a whole: a snapshot carries its lifecycle status, sequence check
        # and book together, and the status may restate the stored book only once the frame's
        # own book result is known (it may be a gap, a replacement or a quarantined state).
        by_market: dict[str, list] = {}
        for o in replay.feed(f):
            if getattr(o, "market", None) in books:
                by_market.setdefault(o.market, []).append(o)
        for market, outs in by_market.items():
            _stream_frame(report, books[market], status, market, outs, resynced, off)
    return stream_frames, replay, novig_poll_gaps


def _write_gaps(w: Writer, found: list[gaps.Gap], subjects: set[str], keep=lambda subject: False) -> None:
    """Collection gaps whose subject is one of ours (or that `keep` admits), each as an open
    row and, once it ended, a close row."""
    for gp in found:
        subject = gp.scope.split("/", 1)[0]
        if subject not in subjects and not keep(subject):
            continue
        opened = w.insert("collection_gap", dict(
            kind="open", opens_gap_id=None, source=gp.source, scope=gp.scope, reason=gp.reason,
            boundary_ts_ms=gp.start_ms, observed_ts_ms=w.now,
            **(w.cited(gp.start_ref) if gp.start_ref else {"raw_artifact_id": None, "raw_line": None})))
        if gp.end_ms is not None:
            w.insert("collection_gap", dict(
                kind="close", opens_gap_id=opened, source=gp.source, scope=gp.scope, reason=gp.reason,
                boundary_ts_ms=gp.end_ms, observed_ts_ms=w.now,
                **(w.cited(gp.end_ref) if gp.end_ref else {"raw_artifact_id": None, "raw_line": None})))

def _poll_reasons(poll_gaps: list[gaps.Gap]):
    """A poll that ends a collection gap re-establishes its subject's state: a resync, so a
    new tick and a complete ladder. Using gaps.poll_gaps itself keeps ticks and gaps in step."""
    recovery = {g.end_ref for g in poll_gaps if g.end_ref}

    def reason(x, first_of_session: bool) -> str | None:
        if first_of_session:
            return "first_poll"
        return "resync" if x.body.ref in recovery else None
    return reason


def ingest_game(conn: sqlite3.Connection, root: Path, game_pk: int, games_path: Path = GAMES,
                now_ms: int | None = None) -> IngestReport:
    """Ingest one game in a single transaction. Refuses a game whose recording is already ingested.

    The game's identity rows may already exist, from the historical import (clv.backfill):
    they are shared, not duplicated (clv.identity.ensure_*)."""
    now = int(time.time() * 1000) if now_ms is None else now_ms
    g = load_game(game_pk, games_path, root)
    lo, hi = capture_window(g)
    if conn.execute("""SELECT 1 FROM event e JOIN event_alias a USING (event_id) JOIN raw_artifact r USING (raw_artifact_id)
                       WHERE e.league = 'MLB' AND e.league_game_id = ? AND r.source = 'mlb_statsapi'""",
                    (str(game_pk),)).fetchone():
        raise IngestError(f"gamePk {game_pk} is already ingested; ingest into a clean database")
    w = Writer(conn, now)
    conn.execute("BEGIN")
    try:
        try:
            report = _ingest(w, Path(root), g, lo, hi)
        except IdentityConflict as exc:
            raise IngestError(str(exc)) from exc
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    report.rows = w.rows
    return report


def _ingest(w: Writer, root: Path, g: dict, lo: int, hi: int) -> IngestReport:
    game_pk = g["game_pk"]

    # -- StatsAPI: identity, outcomes and off observations ------------------------------------
    mlb_segs = archive.segments(root, "mlb_statsapi")
    w.know(mlb_segs)
    feeds, play_events = [], []
    for x in archive.rest_exchanges(archive.iter_frames(root, mlb_segs)):
        if x.ok and mlb.game_pk_of(x) == game_pk:
            if "/feed/live" in x.path:
                feeds.append((x, mlb.game(x)))
            play_events.append((x, mlb.first_play_events(x)))
    if not feeds:
        raise IngestError(f"no StatsAPI feed for gamePk {game_pk} in the archive")
    first_x, first = feeds[0]
    event_id = ensure_event(w, game_pk)
    report = IngestReport(game_pk, event_id)
    outcome_by_team: dict[int, int] = {}
    abbr_to_outcome: dict[str, int] = {}
    name_to_outcome: dict[str, int] = {}
    for team in (first.away, first.home):
        oid = ensure_outcome(w, event_id, game_pk, team.id, team.abbreviation, team.name)
        outcome_by_team[team.id] = oid
        abbr_to_outcome[team.abbreviation] = oid
        name_to_outcome[team.name] = oid
    seen = set()
    for x, gm in feeds:
        detail = dict(double_header=gm.double_header, game_number=gm.game_number, official_date=gm.official_date,
                      original_date=gm.original_date, away=gm.away.abbreviation, home=gm.home.abbreviation)
        key = (gm.scheduled_start_ms, json.dumps(detail, sort_keys=True))
        if key in seen:
            continue                # an unchanged repeat is not a new observation
        seen.add(key)
        w.insert("event_alias", dict(
            event_id=event_id, provider="mlb_statsapi", provider_event_id=str(game_pk),
            title=f"{gm.away.name} @ {gm.home.name}", scheduled_start_ms=gm.scheduled_start_ms,
            detail_json=json.dumps(detail, sort_keys=True), status="exact", method="gamePk",
            evidence="StatsAPI feed/live for this gamePk", observed_ts_ms=gm.recv_ts_ms, **w.cited(x.body.ref)))
    for x, events in play_events:
        for o in sources.from_play_events(events):
            w.insert("off_observation", dict(event_id=event_id, source=o.source, kind=o.kind, subject=o.subject,
                                             detected_off_ts_ms=o.detected_off_ts_ms, observed_ts_ms=o.observed_ts_ms,
                                             **w.cited(x.body.ref)))

    # A hand-checked entry makes its links manual_verified; a discovered one is a rule match,
    # fuzzy_candidate until a correction verifies it (clv.games, measurement contract §4).
    if g["source"] == "manual":
        link_status, link_method = "manual_verified", "games.toml"
        link_evidence = f"listed for gamePk {game_pk} in tools/raw_recorder/games.toml, checked by hand ({g['label']})"
    else:
        link_status, link_method = "fuzzy_candidate", "recorder discovery"
        link_evidence = (f"matched to gamePk {game_pk} by the recorder's discovery rules, not checked "
                        f"({g['label']}{''.join('; ' + n for n in g.get('notes', []))})")

    # -- Novig: catalog, instruments, mappings -------------------------------------------------
    novig_x, ws, catalog = _novig_catalog(w, root, lo, hi, g["novig_markets"])
    # A book that leaves out an outcome with no orders still has both: the catalog names them.
    catalog_outcomes = {mk: tuple(o[0] for o in m.outcomes) for mk, (_, m) in catalog.items()}
    novig_books: dict[str, _Books] = {}
    novig_status: dict[str, str] = {}
    for market in g["novig_markets"]:
        if market not in catalog:
            raise IngestError(f"no Novig catalog response for market {market} in the capture window")
        x, m = catalog[market]
        iid, (s0, s1) = _novig_instrument(w, x, m)
        w.insert("event_alias", dict(
            event_id=event_id, provider="novig", provider_event_id=m.event, title=None,
            scheduled_start_ms=m.starts_ts_ms,
            detail_json=json.dumps({"market": market, "market_description": m.description, "voids": m.voids}),
            status=link_status, method=link_method, evidence=link_evidence, observed_ts_ms=m.recv_ts_ms,
            **w.cited(x.body.ref)))
        by_abbr = {novig_abbr(a): oid for a, oid in abbr_to_outcome.items()}
        name_of = {oid: name for oid, name, _ in m.outcomes}
        for side, outcome in enumerate((s0, s1)):
            name = name_of[outcome]
            if name not in by_abbr:
                raise IngestError(f"Novig outcome {name!r} names neither team of gamePk {game_pk}")
            ensure_mapping(w, dict(
                venue_instrument_id=iid, side=side, outcome_id=by_abbr[name], polarity="direct",
                mapping_status=link_status, settlement_equivalence="pending",
                method="outcome name = team abbreviation", evidence=f"Novig outcome {name!r}; market {link_evidence}",
                rules_native=None, effective_from_ms=None, observed_ts_ms=w.now, **w.cited(x.body.ref)))
        novig_books[market] = _Books(w, iid, f"novig:market:{market}", (s0, s1),
                                     int(novig.PAYOUT_USD_PER_CONTRACT * 100))
        novig_status[market] = m.status

    def off(o: novig.Lifecycle) -> None:
        if (obs := sources.from_lifecycle(o, game_pk)) is not None:
            w.insert("off_observation", dict(event_id=event_id, source=obs.source, kind=obs.kind, subject=obs.subject,
                                             detected_off_ts_ms=obs.detected_off_ts_ms,
                                             observed_ts_ms=obs.observed_ts_ms, **w.cited(o.ref)))
    stream_frames, replay, novig_poll_gaps = _novig_record(w, report, root, novig_x, ws, novig_books, novig_status,
                                                           catalog_outcomes, off)

    # -- Kalshi: listings, instruments, mappings, order-book polls ---------------------------------
    kalshi_segs = archive.overlapping(archive.segments(root, "kalshi"), lo, hi)
    w.know(kalshi_segs)
    kx = list(archive.rest_exchanges(f for f in archive.iter_frames(root, kalshi_segs) if lo <= f.recv_ts_ms <= hi))
    listing: dict[str, tuple] = {}
    kstatus: dict[str, str] = {}
    for x in kx:
        if x.ok and x.request.get("purpose") == "catalog":
            for m in kalshi.markets(x):
                listing.setdefault(m.ticker, (x, m))
    kbooks: dict[str, _Books] = {}
    for ticker in g["kalshi_tickers"]:
        if ticker not in listing:
            raise IngestError(f"no Kalshi market listing for {ticker} in the capture window")
        x, m = listing[ticker]
        cents = m.notional_value_dollars * 100
        if cents != cents.to_integral_value():
            raise IngestError(f"{ticker}: notional {m.notional_value_dollars} is not a whole number of cents")
        iid, sides = ensure_instrument(w, dict(
            venue="kalshi", native_id=ticker, native_event_id=m.event_ticker, operator=None, kind="exchange_book",
            side0_id="yes", side0_label=f"YES: {m.title}", side1_id="no", side1_label=f"NO: {m.title}",
            payout_currency="USD", payout_cents_per_contract=int(cents), quantity_increment="0.01",
            first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))
        if sides != kalshi.SIDES:
            raise IngestError(f"Kalshi {ticker} is stored with sides {sides}, not {kalshi.SIDES}")
        if not w.conn.execute("SELECT 1 FROM event_alias WHERE event_id = ? AND provider = 'kalshi' AND provider_event_id = ?",
                              (event_id, m.event_ticker)).fetchone():
            w.insert("event_alias", dict(
                event_id=event_id, provider="kalshi", provider_event_id=m.event_ticker, title=m.event_ticker,
                scheduled_start_ms=None, detail_json=json.dumps({"occurrence_datetime": m.occurrence_datetime,
                                                                 "close_time": m.close_time}),
                status=link_status, method=link_method, evidence=link_evidence, observed_ts_ms=m.recv_ts_ms,
                **w.cited(x.body.ref)))
        team = ticker.rsplit("-", 1)[1]
        if team not in abbr_to_outcome:
            raise IngestError(f"Kalshi ticker {ticker} names neither team of gamePk {game_pk}")
        rules = "\n\n".join(r for r in (m.rules_primary, m.rules_secondary) if r) or None
        for side, polarity in ((0, "direct"), (1, "complement")):
            ensure_mapping(w, dict(
                venue_instrument_id=iid, side=side, outcome_id=abbr_to_outcome[team], polarity=polarity,
                mapping_status=link_status, settlement_equivalence="pending",
                method="ticker team suffix = team abbreviation; YES direct, NO its complement",
                evidence=f"Kalshi {ticker} ({m.title!r}); ticker {link_evidence}", rules_native=rules,
                effective_from_ms=None, observed_ts_ms=w.now, **w.cited(x.body.ref)))
        kbooks[ticker] = _Books(w, iid, f"kalshi:market:{ticker}", kalshi.SIDES, int(cents))
        kstatus[ticker] = m.status
    kalshi_gaps = gaps.poll_gaps("kalshi", kx, {"orderbook"})
    poll_reason = _poll_reasons(kalshi_gaps)
    last_session = {}
    for x in kx:
        if x.ok and x.request.get("purpose") == "catalog":
            for m in kalshi.markets(x):
                if m.ticker in kbooks and m.status != kstatus[m.ticker]:
                    kstatus[m.ticker] = m.status
                    kbooks[m.ticker].status_change("poll", m.status, x.body.ref, m.recv_ts_ms, None, report)
        if not (x.ok and x.request.get("purpose") == "orderbook" and kalshi.ticker_of(x) in kbooks):
            continue
        ticker = kalshi.ticker_of(x)
        book = kalshi.orderbook(x, listing[ticker][1].notional_value_dollars)
        first_of_session = last_session.get(ticker) != x.request["session_id"]
        last_session[ticker] = x.request["session_id"]
        kbooks[ticker].observe(book, kstatus[ticker], x.body.ref, None, "poll_unchanged",
                               poll_reason(x, first_of_session), report)

    # -- The Odds API: vendor event alias, sportsbook instruments and mappings ------------------
    odds_segs = archive.overlapping(archive.segments(root, "odds_api"), lo, hi)
    w.know(odds_segs)
    ox = [x for x in archive.rest_exchanges(f for f in archive.iter_frames(root, odds_segs) if lo <= f.recv_ts_ms <= hi)
          if x.request.get("purpose") == "odds"]
    scheduled = {gm.scheduled_start_ms for _, gm in feeds}
    lines: dict[tuple[str, str], int] = {}
    aliased: set[str] = set()
    for x in ox:
        if not x.ok:
            continue
        for q in odds_api.quotes(x):
            if q.market != "h2h" or {q.home_team, q.away_team} != {first.home.name, first.away.name}:
                continue
            # The same teams can meet again within the window (a series, a doubleheader). A vendor
            # event is this game only with the same home and away and commence_time on this game's
            # StatsAPI schedule; anything else is reported, never guessed.
            if (q.home_team, q.away_team) != (first.home.name, first.away.name):
                report.unmatched[q.vendor_event_id] = "home and away reversed"
                continue
            if iso_ms(q.commence_time) not in scheduled:
                report.unmatched[q.vendor_event_id] = f"commence_time {q.commence_time} is not the StatsAPI scheduled start"
                continue
            evidence = (f"home {q.home_team!r} and away {q.away_team!r} are this game's StatsAPI home and away; "
                        f"commence_time {q.commence_time} is its StatsAPI scheduled start; not yet verified by hand")
            if q.vendor_event_id not in aliased:
                aliased.add(q.vendor_event_id)
                w.insert("event_alias", dict(
                    event_id=event_id, provider="odds_api", provider_event_id=q.vendor_event_id,
                    title=f"{q.away_team} @ {q.home_team}", scheduled_start_ms=iso_ms(q.commence_time),
                    detail_json="{}", status="fuzzy_candidate", method="teams_and_commence_time",
                    evidence=evidence, observed_ts_ms=q.recv_ts_ms, **w.cited(x.body.ref)))
            key = (q.vendor_event_id, q.bookmaker)
            if key in lines:
                continue
            iid = w.insert("venue_instrument", dict(
                venue="odds_api", native_id=f"{q.vendor_event_id}:{q.bookmaker}:h2h", native_event_id=q.vendor_event_id,
                operator=q.bookmaker, kind="sportsbook_line", side0_id=q.home_team, side0_label=q.home_team,
                side1_id=q.away_team, side1_label=q.away_team, payout_currency="USD", payout_cents_per_contract=None,
                quantity_increment=None, first_seen_ts_ms=q.recv_ts_ms, **w.cited(x.body.ref)))
            lines[key] = iid
            for side, name in enumerate((q.home_team, q.away_team)):
                w.insert("instrument_mapping_observation", dict(
                    venue_instrument_id=iid, side=side, outcome_id=name_to_outcome[name], polarity="direct",
                    mapping_status="fuzzy_candidate", settlement_equivalence="pending",
                    method="outcome name = team name; vendor event by teams and commence_time",
                    evidence=evidence, rules_native=None, effective_from_ms=None, observed_ts_ms=w.now,
                    supersedes_id=None, **w.cited(x.body.ref)))

    # -- Collection gaps for this game's subjects ---------------------------------------------------
    subjects = {f"novig:market:{m}" for m in g["novig_markets"]} | {f"kalshi:market:{t}" for t in g["kalshi_tickers"]}
    found = (gaps.stream_gaps("novig", stream_frames, replay) + novig_poll_gaps + kalshi_gaps
             + gaps.poll_gaps("odds_api", ox, {"odds"}))
    _write_gaps(w, found, subjects, keep=lambda subject: subject.startswith("odds_api:"))
    return report


# -- league events (A1): the venue side only ----------------------------------------------------

def unsettled(root: Path, e: dict, now_ms: int) -> str | None:
    """Why a league event cannot be ingested yet, or None once its window has settled: ended
    `ingest.window_settle_s` ago (so its last segment is sealed and mirrored), with no active
    Novig segment that began by its end."""
    _, hi = league_window(e)
    ready = hi + param("ingest.window_settle_s") * 1000
    if now_ms < ready:
        return f"its window ends {ms_iso(hi)}; it can be ingested from {ms_iso(ready)}"
    for part in sorted(Path(root, "novig").glob("*/*.part")):
        try:
            with open(part, "rb") as f:
                first = json.loads(f.readline())
        except (OSError, ValueError):
            continue
        if first.get("recv_ts_ms", hi + 1) <= hi:
            return f"{part.relative_to(root)} overlaps its window and is still being written"
    return None


def loaded(conn: sqlite3.Connection, event_id: str) -> bool:
    return conn.execute("SELECT 1 FROM venue_ingest WHERE venue = 'novig' AND native_event_id = ? AND parser_version = ?",
                        (event_id, PARSER_VERSION)).fetchone() is not None


def ingest_league_event(conn: sqlite3.Connection, root: Path, event_id: str, now_ms: int | None = None,
                        event: dict | None = None) -> IngestReport:
    """Ingest one league event's venue side in a single transaction. Refuses one already
    ingested, or whose window has not settled."""
    now = int(time.time() * 1000) if now_ms is None else now_ms
    e = event if event is not None else load_league_event(event_id, root)
    if loaded(conn, event_id):
        raise IngestError(f"league event {event_id} is already ingested; ingest into a clean database")
    if (why := unsettled(Path(root), e, now)) is not None:
        raise IngestError(f"league event {event_id} is not settled: {why}")
    lo, hi = league_window(e)
    markets = list(e["novig_markets"])
    w = Writer(conn, now)
    conn.execute("BEGIN")
    try:
        report = IngestReport(None, None, native_event_id=event_id)
        novig_x, ws, catalog = _novig_catalog(w, Path(root), lo, hi, markets)
        catalog_outcomes = {mk: tuple(o[0] for o in m.outcomes) for mk, (_, m) in catalog.items()}
        books: dict[str, _Books] = {}
        status: dict[str, str] = {}
        for market in markets:
            if market not in catalog:
                raise IngestError(f"no Novig catalog response for market {market} in the capture window")
            x, m = catalog[market]
            if m.event != event_id:
                raise IngestError(f"Novig market {market} belongs to event {m.event}, not {event_id}")
            iid, sides = _novig_instrument(w, x, m)
            books[market] = _Books(w, iid, f"novig:market:{market}", sides, int(novig.PAYOUT_USD_PER_CONTRACT * 100))
            status[market] = m.status
        stream_frames, replay, poll_gaps = _novig_record(w, report, Path(root), novig_x, ws, books, status,
                                                         catalog_outcomes, None)
        _write_gaps(w, gaps.stream_gaps("novig", stream_frames, replay) + poll_gaps,
                    {f"novig:market:{m}" for m in markets})
        w.insert("venue_ingest", dict(
            venue="novig", league=e["league"], native_event_id=event_id, label=e.get("label", ""),
            scheduled_start_ms=iso_ms(e["scheduled_start_utc"]), window_start_ms=lo, window_end_ms=hi,
            markets_json=json.dumps(markets), parser_version=PARSER_VERSION, ingested_ts_ms=now))
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    report.rows = w.rows
    return report


def ingest_league_events(conn: sqlite3.Connection, root: Path, now_ms: int | None = None) -> list[tuple[dict, object]]:
    """Every discovered league event not yet ingested whose window has settled, each in its
    own transaction. Returns (event, IngestReport or the IngestError that stopped it) for each
    one attempted; events still waiting are left for a later run."""
    now = int(time.time() * 1000) if now_ms is None else now_ms
    out = []
    for e in league_events(root):
        if loaded(conn, e["event_id"]) or unsettled(Path(root), e, now) is not None:
            continue
        try:
            out.append((e, ingest_league_event(conn, root, e["event_id"], now, event=e)))
        except IngestError as exc:
            out.append((e, exc))
    return out
