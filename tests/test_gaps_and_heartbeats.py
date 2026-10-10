"""Collection gaps from archived polls and streams (DESIGN.md §7.2, §7.3).

A gap starts at the last trustworthy observation and ends at the next one; a
gap with no later observation stays open; a planned end of collection is not
a gap.
"""
import json

from clv import gaps
from clv.archive import Frame, RestExchange, Segment
from clv.venues.novig.parser import Resync, SequenceGap

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


def ws(conn, t0, t1, reason, market="m", channel="book"):
    out = [frame(t0 - 1, conn, "event", {"type": "ws_connect_attempt", "markets": [market], "channel": channel}),
           frame(t0, conn), frame(t1, conn)]
    if reason:
        out.append(frame(t1 + 1, conn, "event", {"type": "ws_closed"}))
        out.append(frame(t1 + 2, conn, "event", {"type": "ws_disconnected", "reason": reason}))
    return out


def test_planned_close_is_not_a_gap():
    for reason in ("recorder_stop", "no_active_markets"):
        assert gaps.stream_gaps("novig", ws("c1", 100, 200, reason)) == []


def test_reconnect_is_a_gap_and_a_lost_connection_stays_open():
    frames = ws("c1", 100, 200, "transport_silence") + ws("c2", 300, 400, "io_error")
    g1, g2 = gaps.stream_gaps("novig", frames)
    assert (g1.reason, g1.scope, g1.start_ms, g1.end_ms) == ("reconnect", "novig:market:m/book", 200, 300)
    assert (g2.reason, g2.start_ms, g2.end_ms) == ("connection_lost", 400, None)


def test_connection_with_no_recorded_end_stays_open():
    (g,) = gaps.stream_gaps("novig", ws("c1", 100, 200, None))
    assert (g.reason, g.start_ms, g.end_ms) == ("connection_end_unrecorded", 200, None)


def test_channels_are_separate_scopes():
    frames = ws("c1", 100, 200, "recorder_stop", channel="book") + ws("c2", 300, 400, "recorder_stop", channel="trades")
    assert gaps.stream_gaps("novig", frames) == []


def test_sequence_gap_runs_from_the_last_contiguous_frame_to_the_resync():
    sg = SequenceGap("c1", "m", "book", 11, 13, "f#11", 1100, "f#13", 1200)
    rs = [Resync("c1", "m", "book", 20, 900, "f#9"), Resync("c1", "m", "book", 20, 1400, "f#20")]
    (g,) = [g for g in gaps.stream_gaps("novig", ws("c1", 100, 2000, "recorder_stop"), [sg], rs)]
    assert (g.reason, g.start_ms, g.end_ms, g.start_ref, g.end_ref) == ("sequence_gap", 1100, 1400, "f#11", "f#20")
