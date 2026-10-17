"""Liveness by bracketing (docs/decisions/2026-10-17-a1-probe-policy.md; DESIGN.md §7.2, §9.4
item 5): raw stream frames through the replay and the ingest to the close function.

A quiet Novig book is subscribed at T0, changed once at T0 + 1 s, and then probed only at
T0 + 600 s, the slow cadence. A close at T0 + 300 s has no evidence within
stream.liveness_max_s, so it is live only if a later observation brackets it:

- an unchanged probe brackets it, and so does a contiguous delta;
- a lifecycle transition before the probe does not (the venue may drop orders without deltas);
- a sequence jump at the probe is a gap;
- a resubscription after a deliberate unsubscribe does not (its snapshot starts new state);
- a probe showing a lifecycle sequence ahead (a missed transition) is a gap.
"""
import json

import pytest
from raw_recorder.archive import ArchiveWriter
from test_ingest_league import Clock, market, rest

from clv import db, lineage
from clv.close.definitions import Reference, live_defs, price_as_of
from clv.ingest import ingest_league_event
from clv.timeutil import ms_iso

T0 = 1_792_202_400_000
M, E, A, B = "nm-q", "ne-q", "o-home", "o-away"
S = 1_000
ORDERS = {A: [{"orderId": "a1", "price": "0.55", "qty": 1000}], B: [{"orderId": "b1", "price": "0.43", "qty": 800}]}
# The book after the change at T0 + 1 s (seq 11), as any later snapshot lists it.
ORDERS_11 = {A: ORDERS[A] + [{"orderId": "x1", "price": "0.50", "qty": 10}], B: ORDERS[B]}
EVENT = {"league": "NBA", "event_id": E, "label": "fixture", "novig_markets": [M],
         "scheduled_start_utc": ms_iso(T0 + 3_600_000), "capture_lead_s": 3_700, "capture_tail_s": 300}


def book_msg(t, seq, lc_seq=0, nonce=None, subscribed=False):
    msg = {"ts": t, **({"nonce": nonce} if nonce else {}),
           "snapshot": {M: {"eventId": E, "book": {"seq": seq, "orders": ORDERS if seq == 10 else ORDERS_11},
                            "lifecycle": {"seq": lc_seq, "status": "OPEN"}}}}
    if subscribed:
        msg = {"ts": t, "nonce": nonce, "subscribed": {"markets": {M: "book"}, "events": {}, "private": []},
               "snapshot": msg["snapshot"]}
    return msg


def add(t, seq, oid, price):
    return {"ts": t, "delta": {M: {"eventId": E, "book": {"seq": seq, "deltas": [
        {"kind": "add", "orderId": oid, "outcomeId": A, "price": price, "qty": 10}]}}}}


def golive(t):
    return {"ts": t, "delta": {M: {"eventId": E, "lifecycle": {"seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}}


def record(root, after):
    """The subscription and the change at T0 + 1 s, then `after`: (seconds after T0, dir, message)."""
    clock = Clock()
    clock.t = T0 - 60_000
    w = ArchiveWriter(root, session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    nv = w.stream("novig", "rest-s1")
    rest(clock, nv, "c0", T0 - 30_000, f"/v3/public/catalog/markets/{M}", "catalog",
         market(M, E, T0 + 3_600_000, [(A, "HOME"), (B, "AWAY")]), [f"novig:market:{M}"])
    ws = w.stream("novig", "c1")

    def put(t, d, obj):
        clock.t = t
        ws.write(d, "c1", json.dumps(obj), recv_ts_ms=t)

    clock.t = T0 - 1
    ws.event("c1", "ws_connect_attempt", markets=[M], channel="book")
    put(T0, "out", {"nonce": 1, "subscribe": {"markets": {M: "book"}}})
    put(T0 + 5, "in", book_msg(T0 + 5, 10, nonce=1, subscribed=True))
    put(T0 + S, "in", add(T0 + S, 11, "x1", "0.50"))
    for s, d, obj in after:
        put(int(T0 + s * S), d, obj)
    clock.t = T0 + 3_500 * S
    ws.event("c1", "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
    clock.t = T0 + 4_000 * S                        # idle on: everything before this is sealed
    w.close()


def close_at(tmp_path, after, as_of_s=300):
    root = tmp_path / "archive"
    record(root, after)
    c = db.connect(":memory:")
    db.migrate(c)
    ingest_league_event(c, root, E, now_ms=T0 + 5_000 * S, event=EVENT)
    lineage.use(c, lineage.take(c, T0 + 5_000 * S))
    (iid,) = c.execute("SELECT venue_instrument_id FROM venue_instrument WHERE native_id = ?", (M,)).fetchone()
    return price_as_of(c, Reference("novig", M, iid, 0, None), T0 + as_of_s * S, live_defs()[0])


def probe(s, seq, lc_seq=0, nonce=2):
    return (s, "in", book_msg(T0 + s * S, seq, lc_seq, nonce=nonce))


def test_recent_evidence_is_live_as_before(tmp_path):
    p = close_at(tmp_path, [probe(600, 11)], as_of_s=20)
    assert p.reason is None and p.bracket_ms is None and p.liveness_age_ms == 19 * S


def test_an_unchanged_probe_brackets_the_quiet_interval_before_it(tmp_path):
    p = close_at(tmp_path, [probe(600, 11)])
    assert p.reason is None and p.p is not None
    assert p.bracket_ms == T0 + 600 * S and p.liveness_age_ms == 299 * S


def test_a_contiguous_delta_brackets_the_interval_before_it(tmp_path):
    p = close_at(tmp_path, [(600, "in", add(T0 + 600 * S, 12, "x2", "0.51"))])
    assert p.reason is None and p.bracket_ms == T0 + 600 * S


def test_without_a_later_observation_a_quiet_book_is_stalled(tmp_path):
    assert close_at(tmp_path, []).reason == "feed_stalled"


def test_a_lifecycle_transition_before_the_probe_breaks_the_bracket(tmp_path):
    p = close_at(tmp_path, [(400, "in", golive(T0 + 400 * S)), probe(600, 11, lc_seq=1)])
    assert p.reason == "feed_stalled" and p.bracket_ms is None


def test_a_sequence_jump_at_the_probe_is_a_gap(tmp_path):
    assert close_at(tmp_path, [probe(600, 13)]).reason == "collection_gap"


def test_a_resubscription_after_a_deliberate_end_never_brackets(tmp_path):
    p = close_at(tmp_path, [(200, "out", {"nonce": 2, "unsubscribe": [f"market:{M}"]}),
                            (500, "out", {"nonce": 3, "subscribe": {"markets": {M: "book"}}}),
                            (500.005, "in", book_msg(T0 + 500_005, 11, nonce=3, subscribed=True)),
                            probe(600, 11, nonce=4)])
    assert p.reason == "feed_stalled" and p.bracket_ms is None


def test_a_probe_showing_a_missed_transition_is_a_gap(tmp_path):
    # Without the lifecycle check the unchanged book seq would vouch for the interval.
    assert close_at(tmp_path, [probe(600, 11, lc_seq=1)]).reason == "collection_gap"


@pytest.mark.parametrize("as_of_s", [700, 1_000])
def test_after_the_probe_recency_and_brackets_judge_as_usual(tmp_path, as_of_s):
    # After the first probe, too old for recency, the next probe brackets the interval as before.
    p = close_at(tmp_path, [probe(600, 11), probe(1_200, 11, nonce=3)], as_of_s=as_of_s)
    assert p.reason is None and p.bracket_ms == T0 + 1_200 * S
