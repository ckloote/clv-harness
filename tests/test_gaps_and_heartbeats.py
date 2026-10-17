"""Collection gaps from archived polls and streams (DESIGN.md §7.2, §7.3).

A gap starts at the last trustworthy observation and ends at the next one; a
gap with no later observation stays open; a planned end of collection is not
a gap. Stream cases run real-shaped frames through StreamReplay, which is the
one judge of channel evidence.
"""
import json

from clv import gaps
from clv.archive import Frame, RestExchange, Segment
from clv.venues.novig.parser import StreamReplay

SEG = Segment("novig", "2026-10-08", "c.0001.jsonl.gz", "c", "0001", "0" * 64, 0, 0, None, None, "s1",
              "0.1.0", "r0-v1", False)


def frame(recv, conn, dir="in", obj=None):
    return Frame(recv, conn, dir, json.dumps(obj or {}), SEG, recv)


def poll(t, ok=True, session="s1", subject="kalshi:market:A", purpose="orderbook"):
    req = {"request_id": f"r{t}", "path": "/p", "subjects": [subject], "purpose": purpose,
           "start_ts_ms": t, "session_id": session}
    done = {"status": 200 if ok else None, "failure": None if ok else "timeout"}
    body = frame(t + 5, f"r{t}") if ok else None
    return RestExchange(f"r{t}", req, body, done, (f"req{t}",))


def test_failed_poll_opens_a_gap_from_the_last_success_to_the_next():
    (g,) = gaps.poll_gaps("kalshi", [poll(0), poll(10, ok=False), poll(20)], {"orderbook"})
    assert (g.reason, g.start_ms, g.end_ms, g.scope) == ("request_failed", 5, 25, "kalshi:market:A")


def test_restart_between_polls_is_a_gap():
    (g,) = gaps.poll_gaps("kalshi", [poll(0), poll(10, session="s2")], {"orderbook"})
    assert (g.reason, g.start_ms, g.end_ms) == ("recorder_restart", 5, 15)


def test_trailing_failure_leaves_the_gap_open():
    (g,) = gaps.poll_gaps("kalshi", [poll(0), poll(10, ok=False)], {"orderbook"})
    assert g.end_ms is None and g.start_ms == 5


def test_steady_polls_and_other_purposes_make_no_gap():
    xs = [poll(t) for t in range(0, 100, 10)] + [poll(5, ok=False, purpose="catalog")]
    assert gaps.poll_gaps("kalshi", xs, {"orderbook"}) == []


def test_no_gap_before_the_first_observation():
    assert gaps.poll_gaps("kalshi", [poll(10, ok=False), poll(20)], {"orderbook"}) == []


# -- streams: frames in the recorder's real shapes, through a StreamReplay -------------------

_line = iter(range(10**6))
ORDERS = {"x": [{"orderId": "o1", "price": "0.48", "qty": 1}], "y": [{"orderId": "o2", "price": "0.51", "qty": 1}]}


def fr(t, conn, dir, obj):
    return Frame(t, conn, dir, json.dumps(obj), SEG, next(_line))


def connect(t, conn, markets, channel="book"):
    return fr(t, conn, "event", {"type": "ws_connect_attempt", "markets": markets, "channel": channel})


def subscribe(t, conn, market, seq=1, channel="book"):
    body = {"seq": seq, "orders": ORDERS} if channel == "book" else {"seq": seq, "trades": []}
    return [fr(t, conn, "out", {"nonce": 1, "subscribe": {"markets": {market: channel}}}),
            fr(t, conn, "in", {"ts": t, "nonce": 1, "subscribed": {"markets": {market: channel}},
                               "snapshot": {market: {"eventId": "ev", channel: body}}})]


def delta(t, conn, market, seq, channel="book"):
    return fr(t, conn, "in", {"ts": t, "delta": {market: {"eventId": "ev", channel: {"seq": seq, "deltas": []}}}})


def unsubscribe(t, conn, market):
    return fr(t, conn, "out", {"nonce": 9, "unsubscribe": [f"market:{market}"]})


def disconnected(t, conn, reason):
    return fr(t, conn, "event", {"type": "ws_disconnected", "reason": reason})


def stream_gaps(*frames):
    flat = sorted((f for x in frames for f in (x if isinstance(x, list) else [x])), key=lambda f: (f.recv_ts_ms, f.line))
    replay = StreamReplay()
    for f in flat:
        replay.feed(f)
    return [(g.reason, g.scope.removeprefix("novig:market:"), g.start_ms, g.end_ms)
            for g in gaps.stream_gaps("novig", flat, replay)]


def one_connection(conn, t0, t1, reason, market="m", channel="book"):
    out = [connect(t0 - 1, conn, [market], channel), *subscribe(t0, conn, market, channel=channel),
           delta(t1, conn, market, 2, channel)]
    return out + ([disconnected(t1 + 2, conn, reason)] if reason else [])


def subscription_lost(t, conn, market, channel="book"):
    return fr(t, conn, "event", {"type": "subscription_lost", "channel": channel, "markets": [market]})


def test_subscription_lost_and_resubscribed_is_a_gap_on_the_same_connection():
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscription_lost(200, "c1", "a"), subscribe(220, "c1", "a", seq=5),
                      delta(250, "c1", "a", 6), disconnected(400, "c1", "recorder_stop"))
    assert got == [("subscription_lost", "a/book", 150, 220)]


def test_subscription_lost_stays_open_though_the_connection_closed_on_purpose():
    # The venue dropped it: a planned close later on the connection proves nothing about it.
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscription_lost(200, "c1", "a"), disconnected(400, "c1", "recorder_stop"))
    assert got == [("subscription_lost", "a/book", 150, None)]


def probe_reply(t, conn, market, seq):
    return fr(t, conn, "in", {"ts": t, "nonce": 2, "snapshot": {market: {"eventId": "ev", "book": {
        "seq": seq, "orders": ORDERS}}}})


def test_probe_after_a_subscription_loss_cannot_erase_it():
    # PR #24 review: status and a probe are sent in one maintenance tick, so the probe's reply
    # (answered for an unsubscribed channel) can land after the loss and extend the span.
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscription_lost(200, "c1", "a"), probe_reply(201, "c1", "a", 2),
                      disconnected(400, "c1", "recorder_stop"))
    # The confirmed probe proves the book unchanged to 201, so the gap opens there and stays open.
    assert got == [("subscription_lost", "a/book", 201, None)]


def test_probe_after_a_subscription_loss_keeps_the_resubscribe_gap_labelled():
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscription_lost(200, "c1", "a"), probe_reply(201, "c1", "a", 2),
                      subscribe(220, "c1", "a", seq=5), delta(250, "c1", "a", 6),
                      disconnected(400, "c1", "recorder_stop"))
    assert got == [("subscription_lost", "a/book", 201, 220)]


def test_loss_on_an_earlier_subscription_does_not_follow_the_resubscribe():
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscription_lost(200, "c1", "a"), subscribe(220, "c1", "a", seq=5),
                      delta(250, "c1", "a", 6), unsubscribe(300, "c1", "a"),
                      disconnected(400, "c1", "recorder_stop"))
    assert got == [("subscription_lost", "a/book", 150, 220)]


def test_planned_close_is_not_a_gap():
    for reason in ("recorder_stop", "no_active_markets"):
        assert stream_gaps(one_connection("c1", 100, 200, reason)) == []


def test_reconnect_is_a_gap_and_a_lost_connection_stays_open():
    got = stream_gaps(one_connection("c1", 100, 200, "transport_silence"), one_connection("c2", 300, 400, "io_error"))
    assert got == [("reconnect", "m/book", 200, 300), ("connection_lost", "m/book", 400, None)]


def test_connection_with_no_recorded_end_stays_open():
    assert stream_gaps(one_connection("c1", 100, 200, None)) == [("connection_end_unrecorded", "m/book", 200, None)]


def test_channels_are_separate_scopes():
    assert stream_gaps(one_connection("c1", 100, 200, "recorder_stop", channel="book"),
                       one_connection("c2", 300, 400, "recorder_stop", channel="trades")) == []


def test_market_subscribed_after_connect_gets_its_own_gap():
    # Review finding 1: subjects come from the data, not only from ws_connect_attempt.
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), delta(150, "c1", "a", 2),
                      subscribe(200, "c1", "b"), delta(250, "c1", "b", 2), delta(300, "c1", "a", 3),
                      disconnected(400, "c1", "io_error"))
    assert got == [("connection_lost", "a/book", 300, None), ("connection_lost", "b/book", 250, None)]


def test_unsubscribed_market_ends_without_a_gap():
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a"), subscribe(200, "c1", "b"),
                      unsubscribe(260, "c1", "b"), delta(300, "c1", "a", 2), disconnected(400, "c1", "io_error"))
    assert got == [("connection_lost", "a/book", 300, None)]


def test_sequence_gap_is_closed_by_the_replacement_connection():
    # Review finding 2: a gap on c1 closes at trusted state on c2.
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a", seq=10), delta(150, "c1", "a", 11),
                      delta(200, "c1", "a", 13), disconnected(250, "c1", "transport_silence"),
                      connect(299, "c2", ["a"]), subscribe(300, "c2", "a", seq=50), disconnected(400, "c2", "recorder_stop"))
    assert got == [("reconnect", "a/book", 150, 300), ("sequence_gap", "a/book", 150, 300)]


def test_lifecycle_transition_is_a_gap_until_a_snapshot_on_any_connection():
    golive = {"ts": 200, "delta": {"a": {"eventId": "ev", "lifecycle": {"seq": 1, "deltas": [
        {"kind": "GOLIVE", "status": "OPEN"}]}}}}
    probe = {"ts": 210, "nonce": 2, "snapshot": {"a": {"eventId": "ev", "book": {"seq": 11, "orders": ORDERS}}}}
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a", seq=10), delta(150, "c1", "a", 11),
                      fr(200, "c1", "in", golive), delta(205, "c1", "a", 12), fr(210, "c1", "in", probe),
                      disconnected(400, "c1", "recorder_stop"))
    assert got == [("lifecycle_reset", "a/book", 200, 210)]
    # The same transition on a second connection's book is the same gap, not another.
    got = stream_gaps(connect(99, "c1", ["a"]), subscribe(100, "c1", "a", seq=10),
                      connect(99, "c2", ["a"]), subscribe(101, "c2", "a", seq=10), delta(150, "c1", "a", 11),
                      delta(151, "c2", "a", 11), fr(200, "c1", "in", golive),
                      fr(201, "c2", "in", golive), disconnected(400, "c1", "recorder_stop"),
                      disconnected(400, "c2", "recorder_stop"))
    assert got == [("lifecycle_reset", "a/book", 200, None)]


def test_overlapping_connections_leave_no_gap():
    assert stream_gaps(one_connection("c1", 100, 400, "recorder_stop"), one_connection("c2", 200, 500, "no_active_markets")) == []


def test_in_flight_frame_after_unsubscribe_keeps_the_end_deliberate():
    # Follow-up review: a delta already in flight arrives after the unsubscribe and extends B's span.
    got = stream_gaps(connect(99, "c1", ["a", "b"]), subscribe(100, "c1", "a"), subscribe(100, "c1", "b"),
                      unsubscribe(200, "c1", "b"), delta(201, "c1", "b", 2), delta(300, "c1", "a", 2),
                      disconnected(400, "c1", "io_error"))
    assert got == [("connection_lost", "a/book", 300, None)]


def test_snapshot_arriving_after_unsubscribe_is_still_a_deliberate_end():
    snapshot_in = subscribe(160, "c1", "b")[1]                      # sent at 100, unsubscribed at 150
    got = stream_gaps(connect(99, "c1", ["b"]), subscribe(100, "c1", "b")[0], unsubscribe(150, "c1", "b"),
                      snapshot_in, disconnected(400, "c1", "io_error"))
    assert got == []


def test_a_resubscribe_in_one_millisecond_ends_the_old_subscription_and_not_the_new():
    # Gate A1's resubscribe fault sends both commands at once, so they share a stamp: the order
    # they were sent in, not their stamps, says which subscription the unsubscribe ended.
    unsub, resub = unsubscribe(200, "c1", "b"), fr(200, "c1", "out", {"nonce": 2, "subscribe": {"markets": {"b": "book"}}})
    got = stream_gaps(connect(99, "c1", ["b"]), subscribe(100, "c1", "b"), unsub, resub,
                      subscribe(205, "c1", "b", seq=5)[1], delta(250, "c1", "b", 6), disconnected(400, "c1", "io_error"))
    assert got == [("connection_lost", "b/book", 250, None)]


def test_resubscription_after_an_unsubscribe_is_not_covered_by_it():
    got = stream_gaps(connect(99, "c1", ["b"]), subscribe(100, "c1", "b"), unsubscribe(150, "c1", "b"),
                      subscribe(200, "c1", "b", seq=5), delta(250, "c1", "b", 6), disconnected(400, "c1", "io_error"))
    assert got == [("connection_lost", "b/book", 250, None)]
