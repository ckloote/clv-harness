"""Ingest one recorded game from the raw archive into the V0 fact tables.

Every row cites the sealed segment and line it came from (raw_artifact,
raw_line). The ingest is deterministic for a given archive, game list,
parser version and `now_ms`, so a clean database reproduces it exactly.

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
- continuity and time: collection_gap open/close pairs; off_observation per sighting.

Sportsbook quotes are not stored as facts here: an entry cites the quote it
uses (entry_quote_observation, clv.entries).
"""
from __future__ import annotations

import dataclasses
import json
import sqlite3
import time
from collections import Counter
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from clv import archive, gaps, mlb
from clv.config import param
from clv.games import GAMES, capture_window, load_game
from clv.identity import novig_abbr
from clv.off import sources
from clv.scoring import odds_api
from clv.timeutil import iso_ms
from clv.venues import kalshi
from clv.venues.novig import parser as novig
from clv.venues.protocol import BinaryBook, Level

PARSER_VERSION = "v0.1"
WS_PREFIXES = ("rest-", "catalog-", "echo-")       # Novig poller streams; any other stream_id is a WebSocket


class IngestError(Exception):
    pass


@dataclass
class IngestReport:
    game_pk: int
    event_id: int
    rows: Counter = field(default_factory=Counter)
    quarantined: Counter = field(default_factory=Counter)        # reason -> count
    quarantine_span: dict = field(default_factory=dict)         # reason -> (first ref, last ref, first ms, last ms)
    unmatched: dict = field(default_factory=dict)               # vendor event id -> why it is not this game

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

    def __init__(self, w: Writer, instrument_id: int, sides: tuple[str, str], cents_per_contract: int):
        self.w, self.iid, self.sides, self.cents = w, instrument_id, sides, cents_per_contract
        self.n = param("book.tick_levels")
        self.snapshot_interval_ms = param("book.full_snapshot_interval_s") * 1000
        self.last_state: dict[str, tuple | None] = {}  # source -> top-N state of the last tick
        self.last_snapshot_ms: dict[str, int] = {}
        # source -> (book, snapshot ref) of the last stored state, while it is still the
        # current one; None once a later state was quarantined or became untrusted.
        self.current: dict[str, tuple | None] = {}

    def observe(self, book: BinaryBook, status: str, ref: str, snapshot_ref: str | None,
                evidence_kind: str, snapshot_reason: str | None, report: IngestReport) -> None:
        ladders = [book.bids[s] for s in self.sides]
        best = [lv[0].price_e4 if lv else None for lv in ladders]
        if None not in best and best[0] + best[1] > 10_000:
            report.quarantine("crossed_book", ref, book.recv_ts_ms)
            self.untrusted(book.source)
            return
        self.current[book.source] = (book, snapshot_ref)
        top = tuple(tuple((lv.price, lv.qty) for lv in ladder[: self.n]) for ladder in ladders)
        state = (status, book.complete, top)
        # After a subscription, resync or recorder restart the state is re-established, so it
        # is a new tick even if unchanged: a tick never spans a gap.
        if self.last_state.get(book.source) == state and snapshot_reason is None:
            self.w.insert("liveness_evidence", dict(venue_instrument_id=self.iid, source=book.source, kind=evidence_kind,
                                                    observed_ts_ms=book.recv_ts_ms, **self.w.cited(ref)))
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


def _stream_frame(w: Writer, report: IngestReport, books: _Books, status: dict[str, str], market: str,
                  outs: list, resynced: set[str], event_id: int, game_pk: int) -> None:
    """One stream frame's outputs for one market.

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
        if (obs := sources.from_lifecycle(o, game_pk)) is not None:
            w.insert("off_observation", dict(event_id=event_id, source=obs.source, kind=obs.kind, subject=obs.subject,
                                             detected_off_ts_ms=obs.detected_off_ts_ms,
                                             observed_ts_ms=obs.observed_ts_ms, **w.cited(o.ref)))
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
    """Ingest one game in a single transaction. Refuses a game already in the database."""
    now = int(time.time() * 1000) if now_ms is None else now_ms
    g = load_game(game_pk, games_path)
    lo, hi = capture_window(g)
    if conn.execute("SELECT 1 FROM event WHERE league = 'MLB' AND league_game_id = ?", (str(game_pk),)).fetchone():
        raise IngestError(f"gamePk {game_pk} is already ingested; ingest into a clean database")
    w = Writer(conn, now)
    conn.execute("BEGIN")
    try:
        report = _ingest(w, Path(root), g, lo, hi)
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
    event_id = w.insert("event", dict(league="MLB", league_game_id=str(game_pk), created_ts_ms=w.now))
    report = IngestReport(game_pk, event_id)
    outcome_by_team: dict[int, int] = {}
    abbr_to_outcome: dict[str, int] = {}
    name_to_outcome: dict[str, int] = {}
    for team in (first.away, first.home):
        oid = w.insert("outcome", dict(event_id=event_id, kind="team_wins", team_id=team.id,
                                       team_abbreviation=team.abbreviation,
                                       description=f"{team.name} win MLB game {game_pk}"))
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

    hand_checked = f"listed for gamePk {game_pk} in tools/raw_recorder/games.toml, checked by hand ({g['label']})"

    # -- Novig: catalog, instruments, mappings -------------------------------------------------
    novig_segs = archive.overlapping(archive.segments(root, "novig"), lo, hi)
    w.know(novig_segs)
    ws = [s for s in novig_segs if not s.stream_id.startswith(WS_PREFIXES)]
    novig_x = list(archive.rest_exchanges(f for f in archive.iter_frames(root, [s for s in novig_segs if s not in ws])
                                          if lo <= f.recv_ts_ms <= hi))
    catalog: dict[str, tuple] = {}
    for x in novig_x:
        if x.ok and x.request.get("purpose") == "catalog" and x.path.count("/") == 5:
            m = novig.catalog_market(x)
            if m.market in g["novig_markets"] and m.market not in catalog:
                catalog[m.market] = (x, m)
    novig_books: dict[str, _Books] = {}
    novig_status: dict[str, str] = {}
    for market in g["novig_markets"]:
        if market not in catalog:
            raise IngestError(f"no Novig catalog response for market {market} in the capture window")
        x, m = catalog[market]
        (s0, n0, _), (s1, n1, _) = m.outcomes
        iid = w.insert("venue_instrument", dict(
            venue="novig", native_id=market, native_event_id=m.event, operator=None, kind="exchange_book",
            side0_id=s0, side0_label=n0, side1_id=s1, side1_label=n1, payout_currency="USD",
            payout_cents_per_contract=int(novig.PAYOUT_USD_PER_CONTRACT * 100), quantity_increment="1",
            first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))
        w.insert("event_alias", dict(
            event_id=event_id, provider="novig", provider_event_id=m.event, title=None,
            scheduled_start_ms=m.starts_ts_ms,
            detail_json=json.dumps({"market": market, "market_description": m.description, "voids": m.voids}),
            status="manual_verified", method="games.toml", evidence=hand_checked, observed_ts_ms=m.recv_ts_ms,
            **w.cited(x.body.ref)))
        by_abbr = {novig_abbr(a): oid for a, oid in abbr_to_outcome.items()}
        for side, (_, name, _) in enumerate(m.outcomes):
            if name not in by_abbr:
                raise IngestError(f"Novig outcome {name!r} names neither team of gamePk {game_pk}")
            w.insert("instrument_mapping_observation", dict(
                venue_instrument_id=iid, side=side, outcome_id=by_abbr[name], polarity="direct",
                mapping_status="manual_verified", settlement_equivalence="pending",
                method="outcome name = team abbreviation", evidence=f"Novig outcome {name!r}; market {hand_checked}",
                rules_native=None, effective_from_ms=None, observed_ts_ms=w.now, supersedes_id=None,
                **w.cited(x.body.ref)))
        novig_books[market] = _Books(w, iid, (s0, s1), int(novig.PAYOUT_USD_PER_CONTRACT * 100))
        novig_status[market] = m.status

    # -- Novig: public book polls (before the stream) -------------------------------------------
    novig_poll_gaps = gaps.poll_gaps("novig", novig_x, {"public_book"})
    poll_reason = _poll_reasons(novig_poll_gaps)
    last_session: dict[str, str] = {}
    for x in novig_x:
        if not x.ok:
            continue
        if x.request.get("purpose") == "catalog" and x.path.count("/") == 5:
            m = novig.catalog_market(x)
            if m.market in novig_books and m.status != novig_status[m.market]:
                novig_status[m.market] = m.status
                novig_books[m.market].status_change("poll", m.status, x.body.ref, m.recv_ts_ms, None, report)
            continue
        if x.request.get("purpose") != "public_book":
            continue
        book = novig.public_book(x)
        if book.market not in novig_books:
            continue
        first_of_session = last_session.get(book.market) != x.request["session_id"]
        last_session[book.market] = x.request["session_id"]
        novig_books[book.market].observe(book, novig_status[book.market], x.body.ref, None, "poll_unchanged",
                                         poll_reason(x, first_of_session), report)

    # -- Novig: stream replay -------------------------------------------------------------------
    stream_frames = list(archive.iter_frames(root, ws))
    replay = novig.StreamReplay()
    resynced: set[str] = set()
    for f in stream_frames:
        # A frame is handled as a whole: a snapshot carries its lifecycle status, sequence check
        # and book together, and the status may restate the stored book only once the frame's
        # own book result is known (it may be a gap, a replacement or a quarantined state).
        by_market: dict[str, list] = {}
        for o in replay.feed(f):
            if getattr(o, "market", None) in novig_books:
                by_market.setdefault(o.market, []).append(o)
        for market, outs in by_market.items():
            _stream_frame(w, report, novig_books[market], novig_status, market, outs, resynced, event_id, game_pk)

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
        iid = w.insert("venue_instrument", dict(
            venue="kalshi", native_id=ticker, native_event_id=m.event_ticker, operator=None, kind="exchange_book",
            side0_id="yes", side0_label=f"YES: {m.title}", side1_id="no", side1_label=f"NO: {m.title}",
            payout_currency="USD", payout_cents_per_contract=int(cents), quantity_increment="0.01",
            first_seen_ts_ms=m.recv_ts_ms, **w.cited(x.body.ref)))
        if not w.conn.execute("SELECT 1 FROM event_alias WHERE event_id = ? AND provider = 'kalshi' AND provider_event_id = ?",
                              (event_id, m.event_ticker)).fetchone():
            w.insert("event_alias", dict(
                event_id=event_id, provider="kalshi", provider_event_id=m.event_ticker, title=m.event_ticker,
                scheduled_start_ms=None, detail_json=json.dumps({"occurrence_datetime": m.occurrence_datetime,
                                                                 "close_time": m.close_time}),
                status="manual_verified", method="games.toml", evidence=hand_checked, observed_ts_ms=m.recv_ts_ms,
                **w.cited(x.body.ref)))
        team = ticker.rsplit("-", 1)[1]
        if team not in abbr_to_outcome:
            raise IngestError(f"Kalshi ticker {ticker} names neither team of gamePk {game_pk}")
        rules = "\n\n".join(r for r in (m.rules_primary, m.rules_secondary) if r) or None
        for side, polarity in ((0, "direct"), (1, "complement")):
            w.insert("instrument_mapping_observation", dict(
                venue_instrument_id=iid, side=side, outcome_id=abbr_to_outcome[team], polarity=polarity,
                mapping_status="manual_verified", settlement_equivalence="pending",
                method="ticker team suffix = team abbreviation; YES direct, NO its complement",
                evidence=f"Kalshi {ticker} ({m.title!r}); ticker {hand_checked}", rules_native=rules,
                effective_from_ms=None, observed_ts_ms=w.now, supersedes_id=None, **w.cited(x.body.ref)))
        kbooks[ticker] = _Books(w, iid, kalshi.SIDES, int(cents))
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
    for gp in found:
        subject = gp.scope.split("/", 1)[0]
        if subject not in subjects and not subject.startswith("odds_api:"):
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
    return report
