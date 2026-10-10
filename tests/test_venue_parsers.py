"""Venue parsers over the wire shapes recorded in R0 (docs/vendor-capabilities.md).

Frames here are small hand-made copies of real Production shapes for gamePk
849832. Each parser must keep native strings, refuse what it does not
understand, and never infer state it was not shown.
"""
import json
from decimal import Decimal
from fractions import Fraction

import pytest

from clv import mlb
from clv.archive import Frame, RestExchange, Segment
from clv.off import sources
from clv.scoring import odds_api
from clv.venues import kalshi
from clv.venues.novig import parser as novig
from clv.venues.protocol import Level, aggregate, price_e4

SEG = Segment("novig", "2026-10-08", "conn-1.0001.jsonl.gz", "conn-1", "0001", "0" * 64, 0, 0, None, None,
              "s1", "0.1.0", "r0-v1", False)
M, A, B = "mkt", "out-cws", "out-cle"
_line = iter(range(10**6))


def frame(obj, recv=1000, conn="conn-1", dir="in"):
    return Frame(recv, conn, dir, json.dumps(obj) if not isinstance(obj, str) else obj, SEG, next(_line))


def exchange(path, body, query="", subjects=(), purpose="orderbook", recv=1000, status=200, headers=()):
    req = {"type": "rest_request", "request_id": "r", "path": path, "query": query, "subjects": list(subjects),
           "purpose": purpose, "start_ts_ms": recv - 10, "session_id": "s1"}
    done = {"type": "rest_complete", "request_id": "r", "status": status, "failure": None,
            "response_headers": [list(h) for h in headers]}
    return RestExchange("r", req, frame(body, recv, conn="r"), done, ("req#0", "body#1", "done#2"))


def snapshot(seq, orders, nonce=1, subscribed=True, ts=999):
    snap = {"ts": ts, "nonce": nonce, "snapshot": {M: {"eventId": "ev", "book": {"seq": seq, "orders": orders},
                                                      "lifecycle": {"seq": 0, "status": "OPEN"}}}}
    if subscribed:
        snap["subscribed"] = {"markets": {M: "book"}, "events": {}, "private": []}
    return snap


def delta(seq, *deltas, channel="book", ts=1001):
    return {"ts": ts, "delta": {M: {"eventId": "ev", channel: {"seq": seq, "deltas": list(deltas)}}}}


ORDERS = {A: [{"orderId": "o1", "price": "0.485", "qty": 1000}, {"orderId": "o2", "price": "0.48", "qty": 500}],
          B: [{"orderId": "o3", "price": "0.51", "qty": 300}]}


def books(outs):
    return [o for o in outs if isinstance(o, novig.BinaryBook)]


# -- prices and levels ---------------------------------------------------------------

@pytest.mark.parametrize("native, e4", [("0.485", 4850), ("0.4900", 4900), ("0.001", 10), ("1", 10000), ("0", 0)])
def test_price_e4_is_exact(native, e4):
    assert price_e4(native) == e4


@pytest.mark.parametrize("native", ["0.00015", "1.01", "-0.1"])
def test_price_e4_refuses_to_round(native):
    with pytest.raises(ValueError):
        price_e4(native)


def test_aggregate_groups_by_numeric_price_highest_first():
    lv = aggregate([("0.5", Decimal(1)), ("0.50", Decimal(2)), ("0.505", Decimal(3)), ("0.49", Decimal(0))])
    assert lv == (Level("0.505", Decimal(3)), Level("0.5", Decimal(3)))


# -- Novig stream replay ---------------------------------------------------------------

def test_snapshot_then_deltas_rebuild_the_book():
    r = novig.StreamReplay()
    (b0,) = books(r.feed(frame(snapshot(10, ORDERS))))
    assert b0.seq == 10 and b0.complete and b0.source == "stream"
    assert b0.bids[A] == (Level("0.485", Decimal(1000)), Level("0.48", Decimal(500)))
    assert b0.best_ask(A) == Level("0.49", Decimal(300))          # CLE bid 0.51 is a CWS ask at 0.49
    assert b0.best_ask(B) == Level("0.515", Decimal(1000))
    (b1,) = books(r.feed(frame(delta(11, {"kind": "update", "orderId": "o1", "remaining": 400}))))
    assert b1.best_bid(A) == Level("0.485", Decimal(400))
    (b2,) = books(r.feed(frame(delta(12, {"kind": "remove", "orderId": "o1", "reason": "fill"},
                                     {"kind": "add", "orderId": "o4", "outcomeId": B, "price": "0.515", "qty": 7}))))
    assert b2.best_bid(A) == Level("0.48", Decimal(500)) and b2.best_bid(B) == Level("0.515", Decimal(7))
    assert b2.refs[0] == b0.refs[0] and len(b2.refs) == 2      # the snapshot it rests on, and this frame
    assert b2.payout_usd_per_contract == Decimal("0.01")


def test_probe_reply_confirms_or_exposes_mismatch():
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS)))
    (check,) = r.feed(frame(snapshot(10, ORDERS, nonce=2, subscribed=False)))[1:]
    assert check.status == "confirmed" and check.channel == "book"
    other = {A: ORDERS[A][:1], B: ORDERS[B]}
    outs = r.feed(frame(snapshot(10, other, nonce=3, subscribed=False)))
    assert [o.status for o in outs if isinstance(o, novig.ProbeCheck)] == ["mismatch"]


def test_late_probe_reply_is_superseded_and_does_not_roll_back():
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS)))
    r.feed(frame(delta(11, {"kind": "remove", "orderId": "o3", "reason": "cancel"})))
    outs = r.feed(frame(snapshot(10, ORDERS, nonce=2, subscribed=False)))
    assert [o.status for o in outs if isinstance(o, novig.ProbeCheck)] == ["superseded"]
    assert not books(outs)
    (b,) = books(r.feed(frame(delta(12, {"kind": "add", "orderId": "o5", "outcomeId": B, "price": "0.5", "qty": 1}))))
    assert b.bids[B] == (Level("0.5", Decimal(1)),)            # o3 stays removed


def test_sequence_gap_drops_state_until_a_snapshot():
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS), recv=1000))
    r.feed(frame(delta(11, {"kind": "remove", "orderId": "o2", "reason": "cancel"}), recv=1100))
    (gap,) = r.feed(frame(delta(13, {"kind": "remove", "orderId": "o1", "reason": "cancel"}), recv=1200))
    assert isinstance(gap, novig.SequenceGap) and (gap.last_seq, gap.got_seq) == (11, 13)
    assert gap.last_recv_ts_ms == 1100
    assert r.feed(frame(delta(14, {"kind": "remove", "orderId": "o3", "reason": "cancel"}), recv=1300)) == []
    outs = r.feed(frame(snapshot(20, ORDERS, nonce=2, subscribed=False), recv=1400))
    assert [type(o).__name__ for o in outs] == ["Lifecycle", "Resync", "BinaryBook"]
    assert books(outs)[0].seq == 20


def test_probe_ahead_of_replay_is_a_gap_and_resyncs():
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS)))
    outs = r.feed(frame(snapshot(15, ORDERS, nonce=2, subscribed=False)))
    kinds = [type(o).__name__ for o in outs]
    assert kinds == ["Lifecycle", "ProbeCheck", "SequenceGap", "Resync", "BinaryBook"]
    assert outs[1].status == "gap"


@pytest.mark.parametrize("d, match", [
    ({"kind": "update", "orderId": "o1", "remaining": 1000}, "update"),
    ({"kind": "add", "orderId": "o1", "outcomeId": A, "price": "0.4", "qty": 1}, "add of resting"),
    ({"kind": "remove", "orderId": "o1", "reason": "expired"}, "reason"),
    ({"kind": "amend", "orderId": "o1"}, "unknown book delta kind"),
])
def test_unexpected_book_deltas_are_refused(d, match):
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS)))
    with pytest.raises(ValueError, match=match):
        r.feed(frame(delta(11, d)))


def test_trades_deduplicate_across_snapshot_and_deltas():
    t = {"tradeId": "t1", "outcomeId": A, "price": "0.485", "qty": 1941, "ts": 990}
    snap = {"ts": 999, "nonce": 1, "subscribed": {}, "snapshot": {M: {"eventId": "ev", "trades": {
        "seq": 5, "trades": [{"seq": 5, "deltas": [t]}]}, "lifecycle": {"seq": 0, "status": "OPEN"}}}}
    r = novig.StreamReplay()
    first = [o for o in r.feed(frame(snap)) if isinstance(o, novig.Trade)]
    t2 = dict(t, tradeId="t2", ts=1001)
    later = [o for o in r.feed(frame(delta(6, t, t2, channel="trades"))) if isinstance(o, novig.Trade)]
    assert [x.trade_id for x in first] == ["t1"] and [x.trade_id for x in later] == ["t2"]
    probe = dict(snap, nonce=2)
    del probe["subscribed"]
    checks = [o for o in r.feed(frame(probe)) if isinstance(o, novig.ProbeCheck)]
    assert [(c.channel, c.status) for c in checks] == [("trades", "superseded")]


def test_golive_is_a_venue_off_observation():
    r = novig.StreamReplay()
    r.feed(frame(snapshot(10, ORDERS)))
    (lc,) = r.feed(frame({"ts": 1791504503244, "delta": {M: {"eventId": "ev", "lifecycle": {
        "seq": 1, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}}, recv=1791504503299))
    obs = sources.from_lifecycle(lc, 849832)
    assert (obs.source, obs.kind, obs.detected_off_ts_ms, obs.observed_ts_ms) == \
        ("novig_stream", "venue_golive", 1791504503244, 1791504503299)


def public_poll(levels_a, levels_b, depth=20):
    orders = {A: [{"orderId": f"a{i}{j}", "price": str(0.40 - i * 0.005)[:5], "qty": 1}
                  for i in range(levels_a) for j in range(3)],
              B: [{"orderId": f"b{i}", "price": str(0.51 + i * 0.005)[:5], "qty": 5} for i in range(levels_b)]}
    return exchange(f"/v3/public/catalog/markets/{M}/book", {"marketId": M, "seq": 7, "orders": orders},
                    query=f"depth={depth}", purpose="public_book")


def test_public_book_depth_limits_levels_not_orders():
    b = novig.public_book(public_poll(9, 5))      # 27 orders on one side, 14 levels in all: complete
    assert b.complete and b.source == "poll" and b.seq == 7
    assert b.bids[A][0] == Level("0.4", Decimal(3))


@pytest.mark.parametrize("levels_a, levels_b", [(20, 1), (12, 8)])
def test_public_book_is_incomplete_when_either_reading_of_depth_could_cut_it(levels_a, levels_b):
    assert not novig.public_book(public_poll(levels_a, levels_b)).complete


def test_catalog_market():
    m = {"marketId": M, "description": "CWS", "eventId": "ev", "marketType": "MONEY", "strike": "0",
         "status": "OPEN", "voids": "FMV", "startsTs": 1791504000000,
         "fee": {"coefficient": "0.06", "makerCredit": "0.5", "charged": "WHEN_LIVE"},
         "outcomes": [{"outcomeId": A, "name": "CWS", "status": "TBD"}, {"outcomeId": B, "name": "CLE", "status": "TBD"}]}
    c = novig.catalog_market(exchange(f"/v3/public/catalog/markets/{M}", m, purpose="catalog"))
    assert c.outcomes == ((A, "CWS", "TBD"), (B, "CLE", "TBD")) and c.voids == "FMV"
    assert c.fee["coefficient"] == "0.06"


# -- Kalshi ------------------------------------------------------------------------------

def test_kalshi_orderbook_takes_its_ticker_from_the_request():
    t = "KXMLBGAME-26OCT081700CLECWS-CLE"
    body = {"orderbook_fp": {"yes_dollars": [["0.5000", "99555.88"], ["0.5100", "859012.73"]],
                             "no_dollars": [["0.4700", "974702.98"], ["0.4800", "4806891.01"]]}}
    b = kalshi.orderbook(exchange(f"/trade-api/v2/markets/{t}/orderbook", body, subjects=[f"kalshi:market:{t}"]),
                         Decimal("1.0000"))
    assert b.market == t and b.complete and b.payout_usd_per_contract == Decimal("1.0000")
    assert b.best_bid("yes") == Level("0.5100", Decimal("859012.73"))
    assert b.best_ask("yes") == Level("0.5200", Decimal("4806891.01"))


def test_kalshi_orderbook_with_undeclared_ticker_is_refused():
    with pytest.raises(ValueError, match="declared ticker"):
        kalshi.orderbook(exchange("/trade-api/v2/markets/X/orderbook", {"orderbook_fp": {}}, subjects=["kalshi:market:Y"]),
                         Decimal(1))


def test_kalshi_depth_limited_orderbook_is_incomplete():
    t = "T"
    x = exchange(f"/trade-api/v2/markets/{t}/orderbook", {"orderbook_fp": {"yes_dollars": [], "no_dollars": []}},
                 query="depth=5", subjects=[f"kalshi:market:{t}"])
    assert not kalshi.orderbook(x, Decimal(1)).complete


# -- The Odds API -----------------------------------------------------------------------------

@pytest.mark.parametrize("american, decimal", [(-118, Fraction(109, 59)), (101, Fraction(201, 100)),
                                               (100, Fraction(2)), (-100, Fraction(2)), (-110, Fraction(21, 11))])
def test_american_to_decimal_is_exact(american, decimal):
    assert odds_api.american_to_decimal(american) == decimal


@pytest.mark.parametrize("bad", [99, -99, 0, 1.5, True])
def test_american_odds_must_be_integers_of_at_least_100(bad):
    with pytest.raises(ValueError):
        odds_api.american_to_decimal(bad)


def test_odds_quotes_and_credits():
    body = [{"id": "b57e", "sport_key": "baseball_mlb", "commence_time": "2026-10-09T00:00:00Z",
             "home_team": "Chicago White Sox", "away_team": "Cleveland Guardians",
             "bookmakers": [{"key": "draftkings", "title": "DraftKings", "last_update": "2026-10-08T23:52:11Z",
                             "markets": [{"key": "h2h", "last_update": "2026-10-08T23:52:10Z", "outcomes": [
                                 {"name": "Chicago White Sox", "price": -102},
                                 {"name": "Cleveland Guardians", "price": -118}]}]}]}]
    x = exchange("/v4/sports/baseball_mlb/odds", body, purpose="odds",
                 headers=[("x-requests-remaining", "468"), ("x-requests-used", "32"), ("x-requests-last", "1")])
    qs = odds_api.quotes(x)
    assert [(q.bookmaker, q.outcome_name, q.price_american) for q in qs] == [
        ("draftkings", "Chicago White Sox", -102), ("draftkings", "Cleveland Guardians", -118)]
    assert qs[0].book_last_update_ms == 1791503531000 and qs[0].market_last_update_ms == 1791503530000
    assert odds_api.credits(x) == {"remaining": 468, "used": 32, "last": 1}


# -- MLB StatsAPI -----------------------------------------------------------------------------

PLAY0 = {"about": {"startTime": "2026-10-09T00:08:49.866Z"}, "playEvents": [
    {"type": "action", "isPitch": False, "startTime": "2026-10-08T20:32:20.982Z",
     "endTime": "2026-10-08T23:43:15.182Z", "details": {"description": "Status Change - Pre-Game"}},
    {"type": "action", "isPitch": False, "startTime": "2026-10-08T23:43:15.182Z",
     "endTime": "2026-10-09T00:08:04.996Z", "details": {"description": "Status Change - Warmup"}},
    {"type": "action", "isPitch": False, "startTime": "2026-10-09T00:08:04.996Z",
     "endTime": "2026-10-09T00:08:51.917Z", "details": {"description": "Status Change - In Progress"}},
    {"type": "pitch", "isPitch": True, "startTime": "2026-10-09T00:08:51.917Z",
     "endTime": "2026-10-09T00:08:56.023Z", "playId": "78205f58", "details": {"description": "Ball"}},
    {"type": "pitch", "isPitch": True, "startTime": "2026-10-09T00:09:10.000Z", "details": {"description": "Strike"}}]}


def feed(coded="F", away_runs=9, home_runs=5):
    return {"gameData": {"game": {"pk": 849832, "doubleHeader": "N", "gameNumber": 1},
                         "datetime": {"dateTime": "2026-10-09T00:00:00Z", "officialDate": "2026-10-08",
                                      "originalDate": "2026-10-08"},
                         "status": {"detailedState": "Final" if coded == "F" else "In Progress", "codedGameState": coded},
                         "teams": {"away": {"id": 114, "name": "Cleveland Guardians", "abbreviation": "CLE"},
                                   "home": {"id": 145, "name": "Chicago White Sox", "abbreviation": "CWS"}}},
            "liveData": {"linescore": {"teams": {"away": {"runs": away_runs}, "home": {"runs": home_runs}}},
                         "plays": {"allPlays": [PLAY0]}}}


def test_mlb_game_identity_and_winner():
    x = exchange("/api/v1.1/game/849832/feed/live", feed(), subjects=["mlb:game:849832"], purpose="game_feed")
    g = mlb.game(x)
    assert (g.game_pk, g.official_date, g.double_header, g.game_number) == (849832, "2026-10-08", "N", 1)
    assert g.scheduled_start_ms == 1791504000000 and g.final and g.winner.abbreviation == "CLE"
    live = mlb.game(exchange("/api/v1.1/game/849832/feed/live", feed("I", 0, 2), subjects=["mlb:game:849832"]))
    assert live.winner is None


def test_first_pitch_and_status_changes_from_play_by_play():
    x = exchange("/api/v1/game/849832/playByPlay", {"allPlays": [PLAY0]}, subjects=["mlb:game:849832"],
                 purpose="play_by_play", recv=1791547436267)
    obs = sources.from_play_events(mlb.first_play_events(x))
    assert [(o.kind, o.detected_off_ts_ms) for o in obs] == [
        ("status_warmup", 1791502995182), ("status_in_progress", 1791504484996), ("first_pitch", 1791504531917)]
    assert all(o.observed_ts_ms == 1791547436267 and o.game_pk == 849832 for o in obs)


def test_no_plays_means_no_off_observation():
    x = exchange("/api/v1/game/1/playByPlay", {"allPlays": []}, subjects=["mlb:game:1"])
    assert mlb.first_play_events(x) == []
