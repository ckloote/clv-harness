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
from clv.ingest import IngestError, ingest_game

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


def write_archive(root: Path, games: Path, crossed_resync: str | None = None) -> None:
    """crossed_resync ("gap" or "mismatch"): after the halt, instead of GOLIVE and in-play
    trading, a probe reply reports OPEN and replaces the book with a crossed one, either
    jumping the sequence (a gap) or at the same sequence (a mismatch)."""
    clock = Clock()
    w = ArchiveWriter(root, session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    # StatsAPI: an in-game feed, then the final feed.
    mlb = w.stream("mlb_statsapi", "mlb-s1")
    rest(clock, mlb, "m1", T + 600_000, f"/api/v1.1/game/{PK}/feed/live", "game_feed", feed("I"), [f"mlb:game:{PK}"])
    rest(clock, mlb, "m2", T + 1_200_000, f"/api/v1.1/game/{PK}/feed/live", "game_feed", feed("F"), [f"mlb:game:{PK}"])
    # Novig: catalog, two public polls (the second unchanged), then the stream.
    nv = w.stream("novig", "rest-s1")
    rest(clock, nv, "n1", T - 3_000_000, f"/v3/public/catalog/markets/{M}", "catalog",
         {"marketId": M, "description": "CWS", "eventId": "ev1", "marketType": "MONEY", "status": "OPEN",
          "voids": "FMV", "startsTs": T, "fee": {}, "outcomes": [{"outcomeId": CWS, "name": "CWS", "status": "TBD"},
                                                                 {"outcomeId": CLE, "name": "CLE", "status": "TBD"}]},
         [f"novig:market:{M}"])
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
    send(T - 500_000, "out", {"nonce": 2, "snapshot": {"markets": {M: "book"}}})
    send(T - 499_950, "in", {"ts": T - 499_951, "nonce": 2, "snapshot": {
        M: {"eventId": "ev1", "book": {"seq": 12, "orders": {
            CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000},
                  {"orderId": "w8", "price": "0.40", "qty": 1}],
            CLE: [{"orderId": "l0", "price": "0.51", "qty": 500}]}}, "lifecycle": {"seq": 0, "status": "OPEN"}}}})
    # A halt: a confirmed probe reports CLOSED with the book unchanged (review finding 2); GOLIVE reopens.
    send(T - 400_000, "in", {"ts": T - 400_001, "nonce": 3, "snapshot": {
        M: {"eventId": "ev1", "book": {"seq": 12, "orders": {
            CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000},
                  {"orderId": "w8", "price": "0.40", "qty": 1}],
            CLE: [{"orderId": "l0", "price": "0.51", "qty": 500}]}}, "lifecycle": {"seq": 0, "status": "CLOSED"}}}})
    if crossed_resync:
        # Review regression: one probe reply reports OPEN and replaces the book with a crossed one.
        # The stored CLOSED book must not be restated as OPEN.
        send(T - 300_000, "in", {"ts": T - 300_001, "nonce": 5, "snapshot": {
            M: {"eventId": "ev1", "book": {"seq": 14 if crossed_resync == "gap" else 12, "orders": {
                CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}],
                CLE: [{"orderId": "l9", "price": "0.53", "qty": 50}]}}, "lifecycle": {"seq": 0, "status": "OPEN"}}}})
        ev(T - 200_000, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
    else:
        send(T + 503_244, "in", {"ts": T + 503_244, "delta": {M: {"eventId": "ev1", "lifecycle": {
            "seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}})
        send(T + 560_000, "in", {"ts": T + 560_000, "delta": {M: {"eventId": "ev1", "book": {"seq": 13, "deltas": [
            {"kind": "add", "orderId": "l9", "outcomeId": CLE, "price": "0.53", "qty": 50}]}}}})      # crossed: in-play
        # A probe confirming the crossed (quarantined) state must not vouch for the last stored tick.
        send(T + 580_000, "in", {"ts": T + 579_999, "nonce": 4, "snapshot": {
            M: {"eventId": "ev1", "book": {"seq": 13, "orders": {
                CWS: [{"orderId": "w9", "price": "0.485", "qty": 200}, {"orderId": "w0", "price": "0.48", "qty": 1000},
                      {"orderId": "w8", "price": "0.40", "qty": 1}],
                CLE: [{"orderId": "l9", "price": "0.53", "qty": 50}, {"orderId": "l0", "price": "0.51", "qty": 500}]}},
                "lifecycle": {"seq": 1, "status": "OPEN"}}}})
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
    # snapshot, two deltas, and two status changes with the book unchanged (CLOSED, then OPEN at
    # GOLIVE); the crossed in-play state is quarantined.
    assert by == [("kalshi", "poll", 8), ("novig", "poll", 1), ("novig", "stream", 5)]
    # Probes: the first, and the CLOSED one. The probe confirming the quarantined crossed state adds none.
    assert q(c, "SELECT source, kind, count(*) FROM liveness_evidence GROUP BY 1, 2 ORDER BY 1, 2") == [
        ("poll", "poll_unchanged", 1), ("stream", "probe_confirmed", 2)]
    assert rep.quarantined == {"crossed_book": 1}
    # Complete ladders: each recorder session's first poll, the recovery after the failed polls, the
    # subscription, and every book.full_snapshot_interval_s (the fixture's events are minutes apart).
    assert q(c, "SELECT reason, source, count(*) FROM book_snapshot GROUP BY 1, 2 ORDER BY 1, 2") == [
        ("first_poll", "poll", 5), ("periodic", "poll", 2), ("periodic", "stream", 3), ("resync", "poll", 2),
        ("subscribe", "stream", 1)]
    snap = q(c, """SELECT bid0_e4, bid1_e4, seq, venue_status, levels0, levels1 FROM tick
                   WHERE source = 'stream' ORDER BY observed_ts_ms""")
    assert snap == [(4800, 5100, 10, "OPEN", 1, 1), (4850, 5100, 11, "OPEN", 2, 1), (4850, 5100, 12, "OPEN", 3, 1),
                    (4850, 5100, 12, "CLOSED", 3, 1), (4850, 5100, 12, "OPEN", 3, 1)]
    status_ticks = q(c, "SELECT observed_ts_ms, venue_ts_ms FROM tick WHERE source = 'stream' AND seq = 12 ORDER BY 1")
    assert status_ticks[1:] == [(T - 400_000, T - 400_001), (T + 503_244, T + 503_244)]   # at each status change
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
    expected = []
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
    root, games, kind = recorded_crossed_resync
    c, rep = ingested((root, games))
    stream = q(c, "SELECT seq, venue_status, observed_ts_ms FROM tick WHERE source = 'stream' ORDER BY observed_ts_ms")
    assert stream[-1] == (12, "CLOSED", T - 400_000)
    assert q(c, "SELECT count(*) FROM tick WHERE source = 'stream' AND observed_ts_ms > ?", T - 400_000) == [(0,)]
    assert q(c, "SELECT count(*) FROM liveness_evidence WHERE source = 'stream' AND observed_ts_ms > ?",
             T - 400_000) == [(0,)]
    assert rep.quarantined == {"crossed_book": 1}
    # On a gap the feed itself resynced, so the sequence gap closes there; the close function
    # (PR 3) then finds only the CLOSED seq 12 tick before the gap, which it must reject.
    novig_gaps = q(c, "SELECT kind, reason, boundary_ts_ms FROM collection_gap WHERE scope LIKE 'novig:%' "
                      "ORDER BY collection_gap_id")
    assert novig_gaps == ([("open", "sequence_gap", T - 400_000), ("close", "sequence_gap", T - 300_000)]
                          if kind == "gap" else [])


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
    assert rep.rows["collection_gap"] == 22               # 11 open/close pairs, all closed
    assert q(c, "SELECT count(*) FROM collection_gap WHERE kind = 'open'") == [(11,)]
    # Every stream state before GOLIVE is uncrossed and complete; the crossing is all in-play.
    golive = q(c, "SELECT detected_off_ts_ms FROM off_observation WHERE kind = 'venue_golive'")[0][0]
    span = rep.quarantine_span["crossed_book"]
    assert span[2] > golive and rep.quarantined["crossed_book"] == 61_621
    assert q(c, """SELECT count(*) FROM tick WHERE source = 'stream' AND observed_ts_ms < ? AND truncated = 1""",
             golive) == [(0,)]
