"""Ingest a recorded game from the raw archive into the V0 facts.

The first tests build a small archive with the recorder's own writer, in the
real wire shapes of all four sources, and check that every fact lands, cites
the exact frame it came from, and is reproduced identically from a clean
database. The last test ingests the real 849832 archive when it is present.
"""
import gzip
import json
from pathlib import Path

import pytest
from raw_recorder.archive import ArchiveWriter

from clv import db
from clv.identity import (
    IdentityConflict,
    correct_mapping,
    ensure_event,
    ensure_instrument,
    ensure_mapping,
    ensure_outcome,
    side_head,
)
from clv.ingest import IngestError, Writer, ingest_game
from clv.timeutil import iso_ms

T = 1_791_504_000_000            # scheduled start 2026-10-09T00:00:00Z
PK = 849832
M, CWS, CLE = "mkt", "out-cws", "out-cle"
TICKERS = ("KX-CLE", "KX-CWS")
NOW = 1_791_600_000_000


class Clock:
    t = T - 3_600_000

    def __call__(self):
        return self.t


def rest(clock, stream, rid, t, path, purpose, body, subjects=(), query="", status=200):
    clock.t = t
    stream.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x", path=path, query=query,
                 subjects=list(subjects), purpose=purpose, start_ts_ms=t)
    stream.write("in", rid, json.dumps(body) if status == 200 else "Service Unavailable", recv_ts_ms=t + 5)
    clock.t = t + 6
    stream.event(rid, "rest_complete", request_id=rid, status=status, failure=None, response_headers=[])


def book(seq, cws_bids, cle_bids, ts):
    orders = {CWS: [{"orderId": f"w{i}", "price": p, "qty": q} for i, (p, q) in enumerate(cws_bids)],
              CLE: [{"orderId": f"l{i}", "price": p, "qty": q} for i, (p, q) in enumerate(cle_bids)]}
    return {"seq": seq, "orders": orders}


def feed(coded):
    play = {"about": {}, "playEvents": [
        {"type": "action", "isPitch": False, "startTime": "2026-10-09T00:08:04.996Z",
         "details": {"description": "Status Change - In Progress"}},
        {"type": "pitch", "isPitch": True, "startTime": "2026-10-09T00:08:51.917Z", "playId": "p1",
         "details": {"description": "Ball"}}]}
    return {"gameData": {"game": {"pk": PK, "doubleHeader": "N", "gameNumber": 1},
                         "datetime": {"dateTime": "2026-10-09T00:00:00Z", "officialDate": "2026-10-08",
                                      "originalDate": "2026-10-08"},
                         "status": {"detailedState": "Final" if coded == "F" else "In Progress", "codedGameState": coded},
                         "teams": {"away": {"id": 114, "name": "Cleveland Guardians", "abbreviation": "CLE"},
                                   "home": {"id": 145, "name": "Chicago White Sox", "abbreviation": "CWS"}}},
            "liveData": {"linescore": {"teams": {"away": {"runs": 9}, "home": {"runs": 5}}},
                         "plays": {"allPlays": [play]}}}


def kalshi_book(yes, no):
    return {"orderbook_fp": {"yes_dollars": [list(x) for x in yes], "no_dollars": [list(x) for x in no]}}


def write_archive(root: Path, games: Path, crossed_resync: str | None = None, late_snapshot: bool = False,
                  catalog_listing: bool = False) -> None:
    """crossed_resync ("gap" or "mismatch"): after the halt, instead of GOLIVE and in-play
    trading, a probe reply reports OPEN and replaces the book with a crossed one, either
    jumping the sequence (a gap) or at the same sequence (a mismatch).

    late_snapshot: after book delta 12 the connection ends with two delayed snapshot replies
    whose book is at seq 11: the first carries a newer lifecycle status (CLOSED, lifecycle
    seq 1), the second an older one (OPEN, lifecycle seq 0)."""
    clock = Clock()
    w = ArchiveWriter(root, session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    # StatsAPI: an in-game feed, then the final feed.
    mlb = w.stream("mlb_statsapi", "mlb-s1")
    rest(clock, mlb, "m1", T + 600_000, f"/api/v1.1/game/{PK}/feed/live", "game_feed", feed("I"), [f"mlb:game:{PK}"])
    rest(clock, mlb, "m2", T + 1_200_000, f"/api/v1.1/game/{PK}/feed/live", "game_feed", feed("F"), [f"mlb:game:{PK}"])
    # Novig: catalog, two public polls (the second unchanged), then the stream.
    nv = w.stream("novig", "rest-s1")
    market = {"marketId": M, "description": "CWS", "eventId": "ev1", "marketType": "MONEY", "status": "OPEN",
              "voids": "FMV", "startsTs": T, "fee": {}, "outcomes": [{"outcomeId": CWS, "name": "CWS", "status": "TBD"},
                                                                     {"outcomeId": CLE, "name": "CLE", "status": "TBD"}]}
    if catalog_listing:     # the market's own fetch failed: only the discovery's league listing has it
        disc = w.stream("novig", "discovery-s1")
        rest(clock, disc, "d1", T - 3_000_000, "/v3/public/catalog/markets", "catalog",
             {"items": [market], "next": None}, ["novig:league:MLB"], query="league=MLB&marketType=MONEY&limit=100")
        clock.t = T - 2_999_000
        nv.event("n1", "rest_request", request_id="n1", method="GET", origin="https://x",
                 path=f"/v3/public/catalog/markets/{M}", query="", subjects=[f"novig:market:{M}"],
                 purpose="catalog", start_ts_ms=clock.t)
        nv.event("n1", "rest_complete", request_id="n1", status=None, failure="connect_error: DNS", response_headers=[])
    else:
        rest(clock, nv, "n1", T - 3_000_000, f"/v3/public/catalog/markets/{M}", "catalog", market, [f"novig:market:{M}"])
    pub = {"marketId": M, "seq": 5, "orders": book(5, [("0.48", 1000)], [("0.51", 500)], 0)["orders"]}
    for i, t in enumerate((T - 2_900_000, T - 2_890_000)):
        rest(clock, nv, f"p{i}", t, f"/v3/public/catalog/markets/{M}/book", "public_book", pub,
             [f"novig:market:{M}"], query="depth=20")
    conn = "c1"
    ws = w.stream("novig", conn)

    def ev(t, type_, **f):
        clock.t = t
        ws.event(conn, type_, **f)

    def send(t, d, obj):
        clock.t = t
        ws.write(d, conn, json.dumps(obj), recv_ts_ms=t)

    ev(T - 2_000_000, "ws_connect_attempt", markets=[M], channel="book")
    send(T - 2_000_000, "out", {"nonce": 1, "subscribe": {"markets": {M: "book"}}})
    send(T - 1_999_990, "in", {"ts": T - 1_999_991, "nonce": 1, "subscribed": {"markets": {M: "book"}}, "snapshot": {
        M: {"eventId": "ev1", "book": book(10, [("0.48", 1000)], [("0.51", 500)], 0),
            "lifecycle": {"seq": 0, "status": "OPEN"}}}})
    send(T - 1_000_000, "in", {"ts": T - 1_000_001, "delta": {M: {"eventId": "ev1", "book": {"seq": 11, "deltas": [
        {"kind": "add", "orderId": "w9", "outcomeId": CWS, "price": "0.485", "qty": 200}]}}}})
    send(T - 999_000, "in", {"ts": T - 999_001, "delta": {M: {"eventId": "ev1", "book": {"seq": 12, "deltas": [
        {"kind": "add", "orderId": "w8", "outcomeId": CWS, "price": "0.40", "qty": 1}]}}}})   # beyond nothing: changes level 2
    if late_snapshot:
        # Review finding: a snapshot taken between book deltas 11 and 12 and received 50 ms after
        # delta 12. Its book is behind the replay, but lifecycle has its own sequence: the CLOSED
        # is new and must be recorded. The second reply's OPEN is behind lifecycle seq 1.
        after_11 = {CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000}],
                    CLE: [{"orderId": "l0", "price": "0.51", "qty": 500}]}
        for nonce, t, lc in ((6, T - 998_950, {"seq": 1, "status": "CLOSED"}), (7, T - 998_900, {"seq": 0, "status": "OPEN"})):
            send(t, "in", {"ts": T - 999_020, "nonce": nonce, "snapshot": {
                M: {"eventId": "ev1", "book": {"seq": 11, "orders": after_11}, "lifecycle": lc}}})
        ev(T - 990_000, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
    else:
        send(T - 500_000, "out", {"nonce": 2, "snapshot": {"markets": {M: "book"}}})
        send(T - 499_950, "in", {"ts": T - 499_951, "nonce": 2, "snapshot": {
            M: {"eventId": "ev1", "book": {"seq": 12, "orders": {
                CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000},
                      {"orderId": "w8", "price": "0.40", "qty": 1}],
                CLE: [{"orderId": "l0", "price": "0.51", "qty": 500}]}}, "lifecycle": {"seq": 0, "status": "OPEN"}}}})
        # A halt seen only in a probe: CLOSED with the book unchanged, at a lifecycle seq no delta brought
        # (review finding 2). Under the probe policy that is a missed transition: the book is untrusted
        # from its last observation and the reply re-establishes it, now CLOSED. GOLIVE reopens.
        send(T - 400_000, "in", {"ts": T - 400_001, "nonce": 3, "snapshot": {
            M: {"eventId": "ev1", "book": {"seq": 12, "orders": {
                CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000},
                      {"orderId": "w8", "price": "0.40", "qty": 1}],
                CLE: [{"orderId": "l0", "price": "0.51", "qty": 500}]}}, "lifecycle": {"seq": 1, "status": "CLOSED"}}}})
        if crossed_resync:
            # Review regression: one probe reply reports OPEN and replaces the book with a crossed one.
            # The stored CLOSED book must not be restated as OPEN.
            send(T - 300_000, "in", {"ts": T - 300_001, "nonce": 5, "snapshot": {
                M: {"eventId": "ev1", "book": {"seq": 14 if crossed_resync == "gap" else 12, "orders": {
                    CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}],
                    CLE: [{"orderId": "l9", "price": "0.53", "qty": 50}]}}, "lifecycle": {"seq": 2, "status": "OPEN"}}}})
            ev(T - 200_000, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
        else:
            # GOLIVE: the venue drops the pregame orders without book deltas, so the book is untrusted
            # until the snapshot the recorder requests next. The in-play adds before it are not applied.
            send(T + 503_244, "in", {"ts": T + 503_244, "delta": {M: {"eventId": "ev1", "lifecycle": {
                "seq": 2, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}})
            live = {CWS: [{"orderId": "w1", "price": "0.48", "qty": 300}], CLE: [{"orderId": "l1", "price": "0.50", "qty": 400}]}
            send(T + 503_600, "in", {"ts": T + 503_600, "delta": {M: {"eventId": "ev1", "book": {"seq": 13, "deltas": [
                {"kind": "add", "orderId": o["orderId"], "outcomeId": side, **o} for side, lst in live.items() for o in lst]}}}})
            send(T + 504_000, "out", {"nonce": 4, "snapshot": {"markets": {M: "book"}}})
            send(T + 504_050, "in", {"ts": T + 504_049, "nonce": 4, "snapshot": {
                M: {"eventId": "ev1", "book": {"seq": 13, "orders": live}, "lifecycle": {"seq": 2, "status": "OPEN"}}}})
            send(T + 560_000, "in", {"ts": T + 560_000, "delta": {M: {"eventId": "ev1", "book": {"seq": 14, "deltas": [
                {"kind": "add", "orderId": "l9", "outcomeId": CLE, "price": "0.53", "qty": 50}]}}}})      # crossed: a fault
            # A probe confirming the crossed (quarantined) state must not vouch for the last stored tick.
            send(T + 580_000, "in", {"ts": T + 579_999, "nonce": 5, "snapshot": {
                M: {"eventId": "ev1", "book": {"seq": 14, "orders": {
                    CWS: live[CWS], CLE: [{"orderId": "l9", "price": "0.53", "qty": 50}, *live[CLE]]}},
                    "lifecycle": {"seq": 2, "status": "OPEN"}}}})
            ev(T + 600_000, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
    # Kalshi: listing, then three polls per ticker; the third is unchanged but follows a failed poll
    # (review finding 3); a restart before the fourth.
    k = w.stream("kalshi", "kalshi-s1")
    listing = {"cursor": "", "markets": [
        {"ticker": t, "event_ticker": "KX", "title": f"{t[-3:]} wins", "yes_sub_title": t[-3:], "status": "active",
         "result": "", "notional_value_dollars": "1.0000", "occurrence_datetime": "2026-10-09T00:00:00Z",
         "close_time": "2026-10-11T21:00:00Z", "rules_primary": f"If {t[-3:]} wins...", "rules_secondary": "",
         "price_level_structure": "linear_cent", "can_close_early": True} for t in TICKERS]}
    rest(clock, k, "k0", T - 3_000_000, "/trade-api/v2/markets", "catalog", listing,
         [f"kalshi:market:{t}" for t in TICKERS])
    for i, (t, yes) in enumerate(((T - 2_900_000, "0.5100"), (T - 2_000_000, "0.5200"), (T - 1_995_000, None),
                                  (T - 1_990_000, "0.5200"))):
        for ticker in TICKERS:
            rest(clock, k, f"k{i}{ticker}", t, f"/trade-api/v2/markets/{ticker}/orderbook", "orderbook",
                 kalshi_book([(yes, "100.50")], [("0.4700", "20.00")]), [f"kalshi:market:{ticker}"],
                 status=503 if yes is None else 200)
    # The Odds API: one pregame poll. The same teams also meet two days later, once with home and
    # away reversed and once not (review finding 1); neither is this game.
    def odds_event(vid, commence, home, away):
        return {"id": vid, "sport_key": "baseball_mlb", "commence_time": commence, "home_team": home,
                "away_team": away, "bookmakers": [
                    {"key": "draftkings", "title": "DraftKings", "last_update": "2026-10-08T23:52:11Z", "markets": [
                        {"key": "h2h", "last_update": "2026-10-08T23:52:11Z", "outcomes": [
                            {"name": home, "price": -102}, {"name": away, "price": -118}]}]}]}
    o = w.stream("odds_api", "odds-s1")
    rest(clock, o, "o1", T - 480_000, "/v4/sports/baseball_mlb/odds", "odds", [
        odds_event("vend1", "2026-10-09T00:00:00Z", "Chicago White Sox", "Cleveland Guardians"),
        odds_event("vend2", "2026-10-11T00:00:00Z", "Cleveland Guardians", "Chicago White Sox"),
        odds_event("vend3", "2026-10-11T00:00:00Z", "Chicago White Sox", "Cleveland Guardians")],
         ["odds_api:sport:baseball_mlb"])
    w.close()
    # A second recorder session for the restart gap: one more Kalshi poll per ticker.
    clock2 = Clock()
    w2 = ArchiveWriter(root, session_id="s2", segment_max_s=86_400, fsync_interval_s=5, clock=clock2)
    k2 = w2.stream("kalshi", "kalshi-s2")
    for ticker in TICKERS:
        rest(clock2, k2, f"k9{ticker}", T - 1_000_000, f"/trade-api/v2/markets/{ticker}/orderbook", "orderbook",
             kalshi_book([("0.5200", "100.50")], [("0.4700", "20.00")]), [f"kalshi:market:{ticker}"])
    w2.close()
    games.write_text(f'''[[game]]
game_pk = {PK}
label = "CLE @ CWS (fixture)"
scheduled_start_utc = "2026-10-09T00:00:00Z"
capture_lead_s = 3600
capture_tail_s = 3600
novig_markets = ["{M}"]
kalshi_tickers = ["{TICKERS[0]}", "{TICKERS[1]}"]
''')


@pytest.fixture(scope="module")
def recorded(tmp_path_factory):
    root = tmp_path_factory.mktemp("archive")
    games = root / "games.toml"
    write_archive(root, games)
    return root, games


@pytest.fixture(scope="module")
def recorded_late_snapshot(tmp_path_factory):
    root = tmp_path_factory.mktemp("archive")
    games = root / "games.toml"
    write_archive(root, games, late_snapshot=True)
    return root, games


@pytest.fixture(scope="module", params=["gap", "mismatch"])
def recorded_crossed_resync(request, tmp_path_factory):
    root = tmp_path_factory.mktemp(f"archive_crossed_{request.param}")
    games = root / "games.toml"
    write_archive(root, games, crossed_resync=request.param)
    return root, games, request.param


def ingested(recorded, now_ms=NOW):
    root, games = recorded
    c = db.connect(":memory:")
    db.migrate(c)
    return c, ingest_game(c, root, PK, games, now_ms=now_ms)


def q(c, sql, *args):
    return [tuple(r) for r in c.execute(sql, args)]


def test_identity_rows(recorded):
    c, rep = ingested(recorded)
    assert q(c, "SELECT league_game_id FROM event") == [(str(PK),)]
    assert q(c, "SELECT team_abbreviation FROM outcome ORDER BY outcome_id") == [("CLE",), ("CWS",)]
    assert q(c, "SELECT provider, status FROM event_alias ORDER BY event_alias_id") == [
        ("mlb_statsapi", "exact"), ("novig", "manual_verified"), ("kalshi", "manual_verified"),
        ("odds_api", "fuzzy_candidate")]
    maps = q(c, """SELECT v.venue, v.native_id, m.side, o.team_abbreviation, m.polarity, m.mapping_status
                   FROM instrument_mapping_observation m JOIN venue_instrument v USING (venue_instrument_id)
                   JOIN outcome o USING (outcome_id) ORDER BY m.mapping_obs_id""")
    assert maps == [("novig", M, 0, "CWS", "direct", "manual_verified"), ("novig", M, 1, "CLE", "direct", "manual_verified"),
                    ("kalshi", "KX-CLE", 0, "CLE", "direct", "manual_verified"),
                    ("kalshi", "KX-CLE", 1, "CLE", "complement", "manual_verified"),
                    ("kalshi", "KX-CWS", 0, "CWS", "direct", "manual_verified"),
                    ("kalshi", "KX-CWS", 1, "CWS", "complement", "manual_verified"),
                    ("odds_api", "vend1:draftkings:h2h", 0, "CWS", "direct", "fuzzy_candidate"),
                    ("odds_api", "vend1:draftkings:h2h", 1, "CLE", "direct", "fuzzy_candidate")]
    assert q(c, "SELECT DISTINCT settlement_equivalence FROM instrument_mapping_observation") == [("pending",)]


def test_books_ticks_evidence_and_snapshots(recorded):
    c, rep = ingested(recorded)
    by = q(c, """SELECT v.venue, t.source, count(*) FROM tick t JOIN venue_instrument v USING (venue_instrument_id)
                 GROUP BY 1, 2 ORDER BY 1, 2""")
    # Kalshi: 4 good polls per ticker, each a tick: changed, or re-established after a failed poll
    # or a restart even when unchanged. Novig poll: 2, the second unchanged. Novig stream: the
    # snapshot, two deltas, the resync at a missed transition (CLOSED, book unchanged), and the resync after
    # GOLIVE (no tick at GOLIVE itself: the book is untrusted there); the crossed state is quarantined.
    assert by == [("kalshi", "poll", 8), ("novig", "poll", 1), ("novig", "stream", 5)]
    # Probes: the first only. The CLOSED one showed a missed transition, so it resynced rather than
    # confirmed; the probe confirming the quarantined crossed state adds none.
    assert q(c, "SELECT source, kind, count(*) FROM liveness_evidence GROUP BY 1, 2 ORDER BY 1, 2") == [
        ("poll", "poll_unchanged", 1), ("stream", "probe_confirmed", 1)]
    assert rep.quarantined == {"crossed_book": 1}
    # Complete ladders: each recorder session's first poll, the recovery after the failed polls, the
    # subscription, and every book.full_snapshot_interval_s (the fixture's events are minutes apart).
    assert q(c, "SELECT reason, source, count(*) FROM book_snapshot GROUP BY 1, 2 ORDER BY 1, 2") == [
        ("first_poll", "poll", 5), ("periodic", "poll", 2), ("periodic", "stream", 1), ("resync", "poll", 2),
        ("resync", "stream", 2), ("subscribe", "stream", 1)]
    snap = q(c, """SELECT bid0_e4, bid1_e4, seq, venue_status, levels0, levels1 FROM tick
                   WHERE source = 'stream' ORDER BY observed_ts_ms""")
    assert snap == [(4800, 5100, 10, "OPEN", 1, 1), (4850, 5100, 11, "OPEN", 2, 1), (4850, 5100, 12, "OPEN", 3, 1),
                    (4850, 5100, 12, "CLOSED", 3, 1), (4800, 5000, 13, "OPEN", 1, 1)]
    status_ticks = q(c, "SELECT observed_ts_ms, venue_ts_ms FROM tick WHERE source = 'stream' AND seq = 12 ORDER BY 1")
    assert status_ticks[1:] == [(T - 400_000, T - 400_001)]          # at the status change
    resync = q(c, "SELECT observed_ts_ms, ladder_complete FROM tick WHERE source = 'stream' AND seq = 13")
    assert resync == [(T + 504_050, 1)]                                # the snapshot after GOLIVE
    levels = q(c, """SELECT side, rank, price_native, price_e4, qty_native, payout_cents FROM tick_level
                     WHERE tick_id = (SELECT min(tick_id) FROM tick WHERE seq = 12) ORDER BY side, rank""")
    assert levels == [(0, 0, "0.485", 4850, "200", 200), (0, 1, "0.48", 4800, "1000", 1000),
                      (0, 2, "0.40", 4000, "1", 1), (1, 0, "0.51", 5100, "500", 500)]
    kalshi = q(c, "SELECT payout_cents, qty_native FROM tick_level l JOIN tick t USING (tick_id) "
                  "WHERE t.source = 'poll' AND l.price_native = '0.5100' LIMIT 1")
    assert kalshi == [(10050, "100.50")]               # 100.50 contracts at $1


def test_off_observations_and_gaps(recorded):
    c, _ = ingested(recorded)
    assert q(c, "SELECT source, kind, detected_off_ts_ms, count(*) FROM off_observation GROUP BY 1, 2, 3 ORDER BY 3") == [
        ("mlb_statsapi", "status_in_progress", T + 484_996, 2), ("novig_stream", "venue_golive", T + 503_244, 1),
        ("mlb_statsapi", "first_pitch", T + 531_917, 2)]
    gaps = q(c, "SELECT kind, scope, reason, boundary_ts_ms FROM collection_gap ORDER BY collection_gap_id")
    # GOLIVE makes the book untrusted until the snapshot after it. The crossed delta then replaces
    # that stored book: a quarantine gap opens there and never closes, since the book stays crossed
    # (review of PR #8).
    # The halt seen only in a probe is a missed transition: from the last trustworthy observation
    # (the previous probe) to that reply.
    expected = [("open", f"novig:market:{M}/book", "crossed_book", T + 504_050),
                ("open", f"novig:market:{M}/book", "lifecycle_missed", T - 499_950),
                ("close", f"novig:market:{M}/book", "lifecycle_missed", T - 400_000),
                ("open", f"novig:market:{M}/book", "lifecycle_reset", T + 503_244),
                ("close", f"novig:market:{M}/book", "lifecycle_reset", T + 504_050)]
    for ticker in TICKERS:
        expected += [("open", f"kalshi:market:{ticker}", "request_failed", T - 1_999_995),
                     ("close", f"kalshi:market:{ticker}", "request_failed", T - 1_989_995),
                     ("open", f"kalshi:market:{ticker}", "recorder_restart", T - 1_989_995),
                     ("close", f"kalshi:market:{ticker}", "recorder_restart", T - 999_995)]
    assert gaps == expected


def test_no_tick_spans_a_gap(recorded):
    # Review finding 3: for every closed gap, the instrument's first tick at or after the gap's
    # end is at its end, so the latest tick before any later cutoff is never from before the gap.
    c, _ = ingested(recorded)
    rows = q(c, """SELECT g.scope, close.boundary_ts_ms,
                          (SELECT min(t.observed_ts_ms) FROM tick t JOIN venue_instrument v USING (venue_instrument_id)
                           WHERE g.scope LIKE v.venue || ':market:' || v.native_id || '%'
                             AND t.observed_ts_ms >= close.boundary_ts_ms)
                   FROM collection_gap g JOIN collection_gap close ON close.opens_gap_id = g.collection_gap_id""")
    assert rows and all(end == first_tick for _, end, first_tick in rows)


def test_rejected_snapshot_cannot_restate_the_old_book_as_open(recorded_crossed_resync):
    # Review regression: CLOSED at seq 12, then a snapshot reporting OPEN with a crossed book,
    # at seq 14 (a gap) or at seq 12 (a mismatch). The crossed book is quarantined, and the old
    # seq 12 book is never restated as OPEN: no tick, and no evidence, after the halt.
    root, games, _ = recorded_crossed_resync
    c, rep = ingested((root, games))
    stream = q(c, "SELECT seq, venue_status, observed_ts_ms FROM tick WHERE source = 'stream' ORDER BY observed_ts_ms")
    assert stream[-1] == (12, "CLOSED", T - 400_000)
    assert q(c, "SELECT count(*) FROM tick WHERE source = 'stream' AND observed_ts_ms > ?", T - 400_000) == [(0,)]
    assert q(c, "SELECT count(*) FROM liveness_evidence WHERE source = 'stream' AND observed_ts_ms > ?",
             T - 400_000) == [(0,)]
    assert rep.quarantined == {"crossed_book": 1}
    # Both replies show a lifecycle seq no delta brought, so each is a missed transition: the first
    # resyncs the book as CLOSED, the second untrusts it from there and resyncs to the crossed
    # (quarantined) book, whether at seq 14 or seq 12. The close function finds only the CLOSED
    # seq 12 tick before the second gap, which it rejects.
    novig_gaps = q(c, "SELECT kind, reason, boundary_ts_ms FROM collection_gap WHERE scope LIKE 'novig:%' "
                      "ORDER BY collection_gap_id")
    assert novig_gaps == [("open", "lifecycle_missed", T - 499_950), ("close", "lifecycle_missed", T - 400_000),
                          ("open", "lifecycle_missed", T - 400_000), ("close", "lifecycle_missed", T - 300_000)]


def test_lifecycle_freshness_does_not_follow_the_book_sequence(recorded_late_snapshot):
    # Review finding: a delayed snapshot whose book (seq 11) is behind the replay (seq 12) carries
    # a newer lifecycle status (CLOSED, lifecycle seq 1). No delta brought that transition, so under
    # the probe policy it was missed: the seq 12 book is untrusted from its last observation, and
    # the reply, being behind it, cannot re-establish it. The CLOSED is still never priced as OPEN:
    # the gap stays open, so no close reads the seq 12 book after it. A second delayed reply whose
    # OPEN is behind lifecycle seq 1 changes nothing.
    c, _ = ingested(recorded_late_snapshot)
    stream = q(c, "SELECT seq, venue_status, observed_ts_ms FROM tick WHERE source = 'stream' ORDER BY observed_ts_ms")
    assert stream == [(10, "OPEN", T - 1_999_990), (11, "OPEN", T - 1_000_000), (12, "OPEN", T - 999_000)]
    assert q(c, "SELECT kind, reason, boundary_ts_ms FROM collection_gap WHERE scope LIKE 'novig:%'") == [
        ("open", "lifecycle_missed", T - 999_000)]
    assert q(c, "SELECT count(*) FROM liveness_evidence WHERE source = 'stream'") == [(0,)]


def test_other_games_between_the_same_teams_are_not_this_game(recorded):
    # Review finding 1.
    c, rep = ingested(recorded)
    assert rep.unmatched == {"vend2": "home and away reversed",
                             "vend3": "commence_time 2026-10-11T00:00:00Z is not the StatsAPI scheduled start"}
    assert q(c, "SELECT DISTINCT native_event_id FROM venue_instrument WHERE venue = 'odds_api'") == [("vend1",)]
    assert q(c, "SELECT provider_event_id FROM event_alias WHERE provider = 'odds_api'") == [("vend1",)]


def test_every_citation_is_the_exact_raw_frame(recorded):
    root, _ = recorded
    c, _ = ingested(recorded)
    arts = {r[0]: (r[1], r[2]) for r in c.execute("SELECT raw_artifact_id, relpath, sha256 FROM raw_artifact")}
    for aid, (relpath, sha) in arts.items():
        import hashlib
        assert hashlib.sha256((root / relpath).read_bytes()).hexdigest() == sha
    lines = {aid: gzip.decompress((root / rel).read_bytes()).split(b"\n") for aid, (rel, _) in arts.items()}
    # Each stream tick cites the frame that produced it: its book sequence number, or, for a
    # status change with the book unchanged, the status itself.
    for aid, line, seq, status in c.execute(
            "SELECT raw_artifact_id, raw_line, seq, venue_status FROM tick WHERE source = 'stream'"):
        frame = json.loads(json.loads(lines[aid][line])["frame"])
        body = (frame.get("snapshot") or frame.get("delta"))[M]
        if "book" in body and body["book"]["seq"] == seq:
            continue
        lifecycle = body["lifecycle"]
        assert status in [lifecycle.get("status")] + [d["status"] for d in lifecycle.get("deltas", [])]
    # Each off observation cites a StatsAPI body or the GOLIVE delta.
    for aid, line, kind in c.execute("SELECT raw_artifact_id, raw_line, kind FROM off_observation"):
        text = json.loads(lines[aid][line])["frame"]
        assert ("GOLIVE" in text) == (kind == "venue_golive") and ('"gameData"' in text or "GOLIVE" in text)
    assert q(c, "PRAGMA foreign_key_check") == []


def test_clean_database_reproduces_the_ingest_exactly(recorded):
    a, _ = ingested(recorded)
    b, _ = ingested(recorded)
    tables = [r[0] for r in a.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name <> 'schema_migration'")]
    for t in tables:
        assert q(a, f"SELECT * FROM {t} ORDER BY 1") == q(b, f"SELECT * FROM {t} ORDER BY 1"), t


def test_ingesting_twice_is_refused(recorded):
    root, games = recorded
    c, _ = ingested(recorded)
    with pytest.raises(IngestError, match="already ingested"):
        ingest_game(c, root, PK, games, now_ms=NOW)


# -- Identity shared with the historical import (clv.backfill) --------------------------------------

def imported_first(c, kalshi_cents=100):
    """The rows a historical import makes for this game before it is recorded live: the event,
    its outcomes, the Novig market with its outcomes in the other order, and unverified links."""
    w = Writer(c, NOW - 1)
    art = w.insert("raw_artifact", dict(
        source="novig_hist", relpath="novig_hist/2026-10-14/h.0001.jsonl.gz", sha256="b" * 64, bytes=1,
        frame_count=1, recorder_version="0.1.0", redaction_policy="r0-v1", unclean_close=0, parser_version="v0.1",
        ingested_ts_ms=NOW - 1))
    cite = dict(raw_artifact_id=art, raw_line=0)
    event = ensure_event(w, PK)
    cle = ensure_outcome(w, event, PK, 114, "CLE", "Cleveland Guardians")
    cws = ensure_outcome(w, event, PK, 145, "CWS", "Chicago White Sox")
    novig, _ = ensure_instrument(w, dict(
        venue="novig", native_id=M, native_event_id="ev1", operator=None, kind="exchange_book", side0_id=CLE,
        side0_label="CLE", side1_id=CWS, side1_label="CWS", payout_currency="USD", payout_cents_per_contract=1,
        quantity_increment="1", first_seen_ts_ms=T - 86_400_000, **cite))
    kalshi, _ = ensure_instrument(w, dict(
        venue="kalshi", native_id="KX-CLE", native_event_id="KX", operator=None, kind="exchange_book",
        side0_id="yes", side0_label="YES", side1_id="no", side1_label="NO", payout_currency="USD",
        payout_cents_per_contract=kalshi_cents, quantity_increment="0.01", first_seen_ts_ms=T - 86_400_000, **cite))
    for iid, side, outcome, polarity in ((novig, 0, cle, "direct"), (novig, 1, cws, "direct"),
                                         (kalshi, 0, cle, "direct"), (kalshi, 1, cle, "complement")):
        ensure_mapping(w, dict(venue_instrument_id=iid, side=side, outcome_id=outcome, polarity=polarity,
                               mapping_status="fuzzy_candidate", settlement_equivalence="pending", method="backfill",
                               evidence="rule", rules_native=None, effective_from_ms=None, observed_ts_ms=NOW - 1,
                               **cite))
    return novig


def test_a_live_ingest_shares_the_identity_a_historical_import_made(recorded):
    root, games = recorded
    c = db.connect(":memory:")
    db.migrate(c)
    novig = imported_first(c)
    ingest_game(c, root, PK, games, now_ms=NOW)
    assert q(c, "SELECT count(*) FROM event") == [(1,)]
    assert q(c, "SELECT count(*) FROM outcome") == [(2,)]
    assert q(c, "SELECT side0_id, side1_id FROM venue_instrument WHERE venue_instrument_id = ?", novig) == [(CLE, CWS)]
    # The stored side order holds: side 0 is CLE, so the snapshot's CLE bid (0.51) is bid0.
    assert q(c, "SELECT bid0_e4, bid1_e4 FROM tick WHERE source = 'stream' ORDER BY observed_ts_ms LIMIT 1") == \
        [(5100, 4800)]
    # The hand-checked games.toml link supersedes each unverified one; nothing else is added.
    heads = q(c, """SELECT v.native_id, m.side, o.team_abbreviation, m.polarity, m.mapping_status, p.mapping_status
                    FROM instrument_mapping_observation m JOIN venue_instrument v USING (venue_instrument_id)
                    JOIN outcome o USING (outcome_id)
                    JOIN instrument_mapping_observation p ON p.mapping_obs_id = m.supersedes_id
                    WHERE v.venue = ? OR v.native_id = ? ORDER BY 1, 2""", "novig", "KX-CLE")
    assert heads == [("KX-CLE", 0, "CLE", "direct", "manual_verified", "fuzzy_candidate"),
                     ("KX-CLE", 1, "CLE", "complement", "manual_verified", "fuzzy_candidate"),
                     (M, 0, "CLE", "direct", "manual_verified", "fuzzy_candidate"),
                     (M, 1, "CWS", "direct", "manual_verified", "fuzzy_candidate")]
    with pytest.raises(IngestError, match="already ingested"):
        ingest_game(c, root, PK, games, now_ms=NOW)


def test_an_instrument_that_differs_from_the_stored_one_is_refused(recorded):
    root, games = recorded
    c = db.connect(":memory:")
    db.migrate(c)
    imported_first(c, kalshi_cents=1)
    with pytest.raises(IngestError, match="KX-CLE: payout_cents_per_contract differ"):
        ingest_game(c, root, PK, games, now_ms=NOW)
    assert q(c, "SELECT count(*) FROM tick") == [(0,)]         # nothing of the game was written


def test_a_link_that_names_another_outcome_is_a_conflict_not_a_choice():
    c = db.connect(":memory:")
    db.migrate(c)
    imported_first(c)
    w = Writer(c, NOW - 1)
    kalshi = q(c, "SELECT venue_instrument_id FROM venue_instrument WHERE native_id = 'KX-CLE'")[0][0]
    cws = q(c, "SELECT outcome_id FROM outcome WHERE team_abbreviation = 'CWS'")[0][0]
    with pytest.raises(IdentityConflict, match="Settle it with a correction"):
        ensure_mapping(w, dict(venue_instrument_id=kalshi, side=0, outcome_id=cws, polarity="direct",
                               mapping_status="manual_verified", settlement_equivalence="pending", method="m",
                               evidence="e", rules_native=None, effective_from_ms=None, observed_ts_ms=NOW))


def test_a_status_upgrade_keeps_the_settlement_verdict_already_checked(recorded):
    # The historical link's settlement rules were checked before the live ingest verified its
    # identity: the stronger status supersedes it, and the verdict carries over (PR #15 review).
    root, games = recorded
    c = db.connect(":memory:")
    db.migrate(c)
    novig = imported_first(c)
    for side in (0, 1):
        correct_mapping(c, "novig", M, side, evidence="rules read", method="manual", now_ms=NOW - 1,
                        settlement_equivalence="identical")
    ingest_game(c, root, PK, games, now_ms=NOW)
    for side in (0, 1):
        head = side_head(c, novig, side)
        assert (head.status, head.equivalence, head.ineligible_reason) == ("manual_verified", "identical", None)
        (evidence,) = q(c, "SELECT evidence FROM instrument_mapping_observation WHERE mapping_obs_id = ?",
                        head.mapping_obs_id)
        assert "settlement identical carried from mapping observation" in evidence[0]


def upgrade(c, rules, equivalence="pending"):
    w = Writer(c, NOW)
    kalshi = q(c, "SELECT venue_instrument_id FROM venue_instrument WHERE native_id = 'KX-CLE'")[0][0]
    cle = q(c, "SELECT outcome_id FROM outcome WHERE team_abbreviation = 'CLE'")[0][0]
    ensure_mapping(w, dict(venue_instrument_id=kalshi, side=0, outcome_id=cle, polarity="direct",
                           mapping_status="manual_verified", settlement_equivalence=equivalence, method="m",
                           evidence="e", rules_native=rules, effective_from_ms=None, observed_ts_ms=NOW))
    return side_head(c, kalshi, 0)


def test_a_verdict_checked_against_other_rules_text_is_checked_again():
    c = db.connect(":memory:")
    db.migrate(c)
    imported_first(c)
    correct_mapping(c, "kalshi", "KX-CLE", 0, evidence="rules read", method="manual", now_ms=NOW - 1,
                    settlement_equivalence="identical")
    head = upgrade(c, "rules text the check never saw")
    assert (head.status, head.equivalence) == ("manual_verified", "pending")


def test_two_checked_verdicts_that_disagree_are_a_conflict():
    c = db.connect(":memory:")
    db.migrate(c)
    imported_first(c)
    correct_mapping(c, "kalshi", "KX-CLE", 0, evidence="rules read", method="manual", now_ms=NOW - 1,
                    settlement_equivalence="identical")
    with pytest.raises(IdentityConflict, match="settlement checked identical"):
        upgrade(c, None, "not_equivalent")


def test_one_mapping_lookup_reads_only_its_own_side():
    # ensure_mapping resolves one side's head through index mapping_instrument, never by reading
    # every head (PR #15 review): no query it runs scans the mapping table.
    c = db.connect(":memory:")
    db.migrate(c)
    imported_first(c)
    ran = []
    c.set_trace_callback(ran.append)
    upgrade(c, None)
    c.set_trace_callback(None)
    reads = [s for s in ran if s.lstrip().upper().startswith("SELECT") and "instrument_mapping_observation" in s]
    assert reads
    for sql in reads:
        plan = [r[-1] for r in q(c, f"EXPLAIN QUERY PLAN {sql}")]
        assert not any(p.startswith("SCAN") and "mapping" in p for p in plan), (sql, plan)


# -- The recorded golden-game candidate (local only) ---------------------------------------------------

ARCHIVE = Path(__file__).resolve().parents[1] / "archive"


@pytest.mark.skipif(not (ARCHIVE / "novig" / "2026-10-08").exists(), reason="raw archive not present")
def test_recorded_849832_ingests():
    c = db.connect(":memory:")
    db.migrate(c)
    rep = ingest_game(c, ARCHIVE, PK, now_ms=NOW)
    assert q(c, "PRAGMA foreign_key_check") == []
    assert rep.rows["venue_instrument"] == 12 and rep.rows["instrument_mapping_observation"] == 24
    assert rep.rows["off_observation"] == 14
    # 11 restart gaps, all closed, and the book untrusted from GOLIVE to the end: no snapshot followed
    # it. (Before the lifecycle reset, the stale pregame orders crossed the in-play book 61,621 times.)
    golive_recv = iso_ms("2026-10-09T00:08:23.299Z")
    assert q(c, "SELECT reason, kind, count(*), min(boundary_ts_ms) FROM collection_gap GROUP BY 1, 2 ORDER BY 1, 2")[0] == (
        "lifecycle_reset", "open", 1, golive_recv)
    assert q(c, "SELECT reason, kind, count(*) FROM collection_gap GROUP BY 1, 2 ORDER BY 1, 2")[1:] == [
        ("recorder_restart", "close", 11), ("recorder_restart", "open", 11)]
    # Every stream state is pregame, uncrossed and complete.
    assert not rep.quarantined
    assert q(c, "SELECT count(*), max(observed_ts_ms) < ?, sum(truncated), sum(1 - ladder_complete) FROM tick "
                "WHERE source = 'stream'", golive_recv) == [(1633, 1, 0, 0)]


def test_a_discovered_game_ingests_as_fuzzy_candidates(tmp_path):
    # The recorder's discovery matched this game by rule: nothing was checked by hand, so its
    # instruments enter quarantine until a correction verifies them (measurement contract §4).
    root = tmp_path / "archive"
    write_archive(root, tmp_path / "found.toml")
    (root / "games.discovered.toml").write_text(
        (tmp_path / "found.toml").read_text() + 'notes = ["2 Kalshi events, all recorded: check which is this game"]\n')
    c = db.connect(":memory:")
    db.migrate(c)
    ingest_game(c, root, PK, tmp_path / "no-games.toml", now_ms=NOW)
    exchange_maps = ("SELECT DISTINCT m.mapping_status FROM instrument_mapping_observation m "
                     "JOIN venue_instrument v USING (venue_instrument_id) WHERE v.venue IN ('kalshi', 'novig')")
    maps = q(c, exchange_maps)
    aliases = q(c, "SELECT DISTINCT provider, status, method FROM event_alias WHERE provider IN ('kalshi', 'novig') ORDER BY 1")
    assert maps == [("fuzzy_candidate",)]
    assert aliases == [("kalshi", "fuzzy_candidate", "recorder discovery"), ("novig", "fuzzy_candidate", "recorder discovery")]
    (evidence,) = q(c, "SELECT DISTINCT evidence FROM event_alias WHERE provider = 'kalshi'")
    assert "not checked" in evidence[0] and "check which is this game" in evidence[0]
    # A hand-checked games.toml entry for the same gamePk wins: manual_verified.
    c2 = db.connect(":memory:")
    db.migrate(c2)
    ingest_game(c2, root, PK, tmp_path / "found.toml", now_ms=NOW)
    assert q(c2, exchange_maps) == [("manual_verified",)]


def test_the_recorder_and_harness_agree_on_novig_abbreviations():
    from raw_recorder import discovery
    from raw_recorder.config import DISCOVERED_FILE

    from clv import games, identity
    assert discovery.NOVIG_ABBR == identity.NOVIG_ABBR
    assert DISCOVERED_FILE == games.DISCOVERED


def test_the_league_listing_is_catalog_evidence_when_the_market_fetch_failed(tmp_path):
    # 849820: the market's own catalog request failed as the host woke (DNS); discovery's hourly
    # listing of every MLB MONEY market carries the same market object, and is cited instead.
    write_archive(tmp_path, tmp_path / "games.toml", catalog_listing=True)
    c, rep = ingested((tmp_path, tmp_path / "games.toml"))
    (cite,) = q(c, "SELECT a.relpath FROM venue_instrument v JOIN raw_artifact a USING (raw_artifact_id) "
                   "WHERE v.venue = 'novig'")
    assert "discovery-s1" in cite[0]
    assert rep.rows["tick"] == ingested(recorded_like(tmp_path / "plain"))[1].rows["tick"]   # nothing else differs


def recorded_like(root):
    write_archive(root, root / "games.toml")
    return root, root / "games.toml"
