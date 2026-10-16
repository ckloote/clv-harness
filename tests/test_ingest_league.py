"""League events (A1) ingested from the venue side: Novig instruments, books, liveness and
collection gaps from a recorded window, with no event, outcome, mapping or off observation;
only once the window has settled; once; and reproducibly.

The synthetic archive has two NBA events whose markets share one WebSocket connection, as
they do live. The connection is lost and replaced in the middle of the first event's window,
and its market goes live before the window ends."""
import json
import sqlite3
from pathlib import Path

import pytest
from raw_recorder.archive import ArchiveWriter

from clv import db
from clv.config import param
from clv.ingest import IngestError, ingest_league_event, ingest_league_events, unsettled

S = 1_792_202_400_000            # e1's scheduled start
H = 3_600_000
E1, E2 = "nev1", "nev2"
M1, M2 = "nm1", "nm2"
LAL, DEN, BOS, NYK = "o-lal", "o-den", "o-bos", "o-nyk"
SETTLED = S + 2 * H + param("ingest.window_settle_s") * 1000


class Clock:
    t = S - 3 * H

    def __call__(self):
        return self.t


def rest(clock, stream, rid, t, path, purpose, body, subjects=(), query=""):
    clock.t = t
    stream.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x", path=path, query=query,
                 subjects=list(subjects), purpose=purpose, start_ts_ms=t)
    stream.write("in", rid, json.dumps(body), recv_ts_ms=t + 5)
    clock.t = t + 6
    stream.event(rid, "rest_complete", request_id=rid, status=200, failure=None, response_headers=[])


def market(mid, event, start, outcomes):
    return {"marketId": mid, "description": outcomes[0][1], "eventId": event, "marketType": "MONEY", "status": "OPEN",
            "voids": "FMV", "startsTs": start, "fee": {},
            "outcomes": [{"outcomeId": o, "name": n, "status": "TBD"} for o, n in outcomes]}


def orders(a, b, pa, pb):
    return {a: [{"orderId": f"{a}-1", "price": pa, "qty": 1000}], b: [{"orderId": f"{b}-1", "price": pb, "qty": 800}]}


def write_archive(root: Path) -> None:
    clock = Clock()
    w = ArchiveWriter(root, session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    # League discovery's listing (a discovery- stream, so REST), then each market's own catalog fetch
    # as its window opens, before the stream subscribes.
    disc = w.stream("novig", "discovery-leagues-s1")
    m1 = market(M1, E1, S, [(LAL, "LAL"), (DEN, "DEN")])
    m2 = market(M2, E2, S + H, [(BOS, "BOS"), (NYK, "NYK")])
    rest(clock, disc, "d1", S - 2 * H + 100, "/v3/public/catalog/markets", "catalog", {"items": [m1, m2], "next": None},
         ["novig:league:NBA"], query="league=NBA&marketType=MONEY&limit=100")
    nv = w.stream("novig", "rest-s1")
    for i, m in enumerate((m1, m2)):
        rest(clock, nv, f"c{i}", S - 2 * H + 200 + 20 * i, f"/v3/public/catalog/markets/{m['marketId']}", "catalog", m,
             [f"novig:market:{m['marketId']}"])

    def connection(conn, t0, seq0, lose_at=None, end_at=None, golive_at=None):
        ws = w.stream("novig", conn)

        def send(t, d, obj):
            clock.t = t
            ws.write(d, conn, json.dumps(obj), recv_ts_ms=t)

        def event(t, type_, **f):
            clock.t = t
            ws.event(conn, type_, **f)

        event(t0, "ws_connect_attempt", markets=[M1, M2], channel="book")
        send(t0, "out", {"nonce": 1, "subscribe": {"markets": {M1: "book", M2: "book"}}})
        send(t0 + 10, "in", {"ts": t0 + 9, "nonce": 1, "subscribed": {"markets": {M1: "book", M2: "book"}}, "snapshot": {
            M1: {"eventId": E1, "book": {"seq": seq0, "orders": orders(LAL, DEN, "0.55", "0.43")},
                 "lifecycle": {"seq": 0, "status": "OPEN"}},
            M2: {"eventId": E2, "book": {"seq": seq0, "orders": orders(BOS, NYK, "0.60", "0.38")},
                 "lifecycle": {"seq": 0, "status": "OPEN"}}}})
        send(t0 + 60_000, "in", {"ts": t0 + 60_000, "delta": {M1: {"eventId": E1, "book": {"seq": seq0 + 1, "deltas": [
            {"kind": "add", "orderId": "x1", "outcomeId": LAL, "price": "0.56", "qty": 100}]}}}})
        send(t0 + 61_000, "in", {"ts": t0 + 61_000, "delta": {M2: {"eventId": E2, "book": {"seq": seq0 + 1, "deltas": [
            {"kind": "add", "orderId": "y1", "outcomeId": BOS, "price": "0.61", "qty": 100}]}}}})
        if lose_at is not None:
            event(lose_at, "ws_disconnected", reason="io_error", close_code=None, duration_s=1.0)
            return
        if golive_at is not None:
            send(golive_at, "in", {"ts": golive_at, "delta": {M1: {"eventId": E1, "lifecycle": {
                "seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}})
        event(end_at, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)

    connection("c1", S - 2 * H + 1_000, 10, lose_at=S - H)
    connection("c2", S - H + 30_000, 50, golive_at=S + 10 * 60_000, end_at=S + 2 * H - 1_000)
    w.close()
    (root / "events.discovered.toml").write_text(f'''discovered_at_utc = "2026-10-17T00:00:00Z"
last_notes = []

[[event]]
league = "NBA"
event_id = "{E1}"
label = "Denver Nuggets @ Los Angeles Lakers, NBA (fixture)"
scheduled_start_utc = "2026-10-17T02:00:00Z"
novig_markets = ["{M1}"]
capture_lead_s = 7200
capture_tail_s = 7200

[[event]]
league = "NBA"
event_id = "{E2}"
label = "New York Knicks @ Boston Celtics, NBA (fixture)"
scheduled_start_utc = "2026-10-17T03:00:00Z"
novig_markets = ["{M2}"]
capture_lead_s = 10800
capture_tail_s = 7200
''')


@pytest.fixture(scope="module")
def recorded(tmp_path_factory):
    root = tmp_path_factory.mktemp("league") / "archive"
    write_archive(root)
    return root


def fresh():
    c = db.connect(":memory:")
    db.migrate(c)
    return c


def q(c, sql, *args):
    return [tuple(r) for r in c.execute(sql, args)]


def test_the_fixture_start_is_the_listed_one():
    assert S == 1_792_202_400_000       # 2026-10-17T02:00:00Z, as events.discovered.toml says


def test_a_league_event_ingests_its_venue_side_only(recorded):
    c = fresh()
    rep = ingest_league_event(c, recorded, E1, now_ms=SETTLED)
    assert rep.game_pk is None and rep.event_id is None and rep.native_event_id == E1
    # Instruments for this event's market only; no identity, mapping or off observation (A2's).
    assert q(c, "SELECT venue, native_id, native_event_id, side0_label, side1_label FROM venue_instrument") == [
        ("novig", M1, E1, "LAL", "DEN")]
    for table in ("event", "event_alias", "outcome", "instrument_mapping_observation", "off_observation"):
        assert q(c, f"SELECT count(*) FROM {table}") == [(0,)], table
    # Books: both connections' subscribe snapshots and deltas, of M1 only; the GOLIVE makes the book
    # untrusted (no probe followed it here), so nothing after it is stored.
    ticks = q(c, "SELECT seq, bid0_e4, bid1_e4, observed_ts_ms FROM tick ORDER BY observed_ts_ms")
    assert [t[0] for t in ticks] == [10, 11, 50, 51]
    assert ticks[0][1:3] == (5500, 4300) and ticks[1][1] == 5600
    assert q(c, "SELECT reason FROM book_snapshot ORDER BY observed_ts_ms") == [("subscribe",), ("subscribe",)]
    # Gaps on M1's book: the lost connection until its replacement resubscribed, and the GOLIVE
    # lifecycle reset, still open when the window ends. Nothing about M2.
    gaps = q(c, "SELECT kind, scope, reason, boundary_ts_ms FROM collection_gap ORDER BY collection_gap_id")
    assert {g[1] for g in gaps} == {f"novig:market:{M1}/book"}
    opens = [(g[2], g[3]) for g in gaps if g[0] == "open"]
    assert ("reconnect", S - 2 * H + 1_000 + 60_000) in opens
    assert ("lifecycle_reset", S + 10 * 60_000) in opens
    (row,) = q(c, "SELECT venue, league, native_event_id, scheduled_start_ms, window_start_ms, window_end_ms,"
                  " markets_json, ingested_ts_ms FROM venue_ingest")
    assert row == ("novig", "NBA", E1, S, S - 2 * H, S + 2 * H, json.dumps([M1]), SETTLED)


def test_a_window_that_has_not_settled_is_refused(recorded, tmp_path):
    c = fresh()
    with pytest.raises(IngestError, match="not settled: its window ends"):
        ingest_league_event(c, recorded, E1, now_ms=SETTLED - 1)
    # A segment still being written that began by the window's end: not settled either.
    root = tmp_path / "archive"
    write_archive(root)
    day = next((root / "novig").iterdir())
    (day / "c3.0001.jsonl.part").write_text(json.dumps({"recv_ts_ms": S + H, "conn_id": "c3", "dir": "event",
                                                         "frame": "{}"}) + "\n")
    with pytest.raises(IngestError, match="still being written"):
        ingest_league_event(c, root, E1, now_ms=SETTLED)
    assert q(c, "SELECT count(*) FROM venue_ingest") == [(0,)]


def test_a_league_event_is_ingested_once(recorded):
    c = fresh()
    ingest_league_event(c, recorded, E1, now_ms=SETTLED)
    with pytest.raises(IngestError, match="already ingested"):
        ingest_league_event(c, recorded, E1, now_ms=SETTLED)


def test_every_settled_event_is_ingested_and_a_rerun_adds_nothing(recorded):
    c = fresh()
    e2_settled = S + 3 * H + param("ingest.window_settle_s") * 1000
    done = ingest_league_events(c, recorded, now_ms=SETTLED)
    assert [e["event_id"] for e, _ in done] == [E1]          # E2's window has not settled yet
    done = ingest_league_events(c, recorded, now_ms=e2_settled)
    assert [e["event_id"] for e, _ in done] == [E2]
    assert ingest_league_events(c, recorded, now_ms=e2_settled) == []
    assert q(c, "SELECT native_id, native_event_id FROM venue_instrument ORDER BY 1") == [(M1, E1), (M2, E2)]
    # Each event's gaps name its own market only; the shared connection's loss is a gap for both.
    scopes = {s for (s,) in q(c, "SELECT DISTINCT scope FROM collection_gap")}
    assert scopes == {f"novig:market:{M1}/book", f"novig:market:{M2}/book"}
    assert unsettled(recorded, {"scheduled_start_utc": "2026-10-17T03:00:00Z", "capture_lead_s": 10800,
                                "capture_tail_s": 7200}, e2_settled) is None


def test_a_market_of_another_event_is_refused(recorded, tmp_path):
    root = tmp_path / "archive"
    write_archive(root)
    path = root / "events.discovered.toml"
    path.write_text(path.read_text().replace(f'novig_markets = ["{M1}"]', f'novig_markets = ["{M2}"]', 1))
    c = fresh()
    with pytest.raises(IngestError, match=f"belongs to event {E2}"):
        ingest_league_event(c, root, E1, now_ms=SETTLED)
    assert q(c, "SELECT count(*) FROM venue_instrument") == [(0,)]          # rolled back whole


def dump(c: sqlite3.Connection) -> dict:
    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
                                      " AND name <> 'schema_migration'")]   # its apply times differ
    return {t: q(c, f"SELECT * FROM {t} ORDER BY 1") for t in tables}


def test_a_clean_database_reproduces_the_ingest_exactly(recorded):
    a, b = fresh(), fresh()
    for c in (a, b):
        ingest_league_events(c, recorded, now_ms=S + 3 * H + param("ingest.window_settle_s") * 1000)
    assert dump(a) == dump(b) and dump(a)["tick"]
