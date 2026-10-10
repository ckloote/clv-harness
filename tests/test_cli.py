"""`clv inspect` keeps only the requested game's outputs from a shared Novig replay."""
import json

from clv.archive import Frame, Segment
from clv.cli import game_subject, novig_summary

SEG = Segment("novig", "2026-10-08", "c.0001.jsonl.gz", "c", "0001", "0" * 64, 0, 0, None, None, "s1",
              "0.1.0", "r0-v1", False)
OURS, OTHER = "mkt-ours", "mkt-other"
ORDERS = {"x": [{"orderId": "o1", "price": "0.48", "qty": 1}], "y": [{"orderId": "o2", "price": "0.51", "qty": 1}]}
_line = iter(range(10**6))


def fr(t, obj):
    return Frame(t, "c1", "in", json.dumps(obj), SEG, next(_line))


def market_body(seq):
    return {"eventId": "ev", "book": {"seq": seq, "orders": ORDERS}, "lifecycle": {"seq": 0, "status": "OPEN"}}


def golive(t, market):
    return fr(t, {"ts": t, "delta": {market: {"eventId": "ev", "lifecycle": {
        "seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}})


def test_other_games_lifecycle_probes_and_trades_are_not_this_games():
    # Review finding 4: one connection carries both markets.
    frames = [
        fr(100, {"ts": 100, "nonce": 1, "subscribed": {}, "snapshot": {OURS: market_body(10), OTHER: market_body(20)}}),
        fr(200, {"ts": 200, "nonce": 2, "snapshot": {OURS: market_body(10), OTHER: market_body(20)}}),
        fr(250, {"ts": 250, "delta": {OTHER: {"eventId": "ev", "trades": {"seq": 1, "deltas": [
            {"tradeId": "t1", "outcomeId": "x", "price": "0.48", "qty": 5, "ts": 250}]}}}}),
        golive(300, OTHER),
        golive(400, OURS),
    ]
    s = novig_summary(frames, {OURS}, 849832)
    assert [(o.subject, o.detected_off_ts_ms, o.game_pk) for o in s.off_obs] == [(f"novig:market:{OURS}", 400, 849832)]
    assert [lc.market for lc in s.lifecycle] == [OURS]
    assert dict(s.probes) == {("book", "confirmed"): 1}
    assert s.trades == 0 and {b.market for b in s.books} == {OURS}
    assert len(s.frames) == 5                     # every frame is still replayed


def test_gap_scopes_are_kept_only_for_this_game():
    g = {"novig_markets": [OURS], "kalshi_tickers": ["KX-CLE"]}
    assert game_subject(f"novig:market:{OURS}/book", g) and not game_subject(f"novig:market:{OTHER}/book", g)
    assert game_subject("kalshi:market:KX-CLE", g) and not game_subject("kalshi:market:KX-NYY", g)
    assert game_subject("odds_api:sport:baseball_mlb", g)
