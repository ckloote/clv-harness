"""B1 import (clv.backfill) on a synthetic backfill archive written by the recorder's writer, in
the vendors' real shapes. It covers B0's edge cases (docs/feasibility.md §4), Novig's
dual-side rows, and the reconciliation of a streamed trade with its MAKER row."""
import json
import sqlite3
from collections import Counter
from fractions import Fraction
from pathlib import Path

import pytest
from raw_recorder.archive import ArchiveWriter

from clv import backfill, db, identity
from clv.backfill import links, novig_public
from clv.close.vwap import trade_vwap
from clv.ingest import Writer
from clv.timeutil import iso_ms

NOW = 1_800_000_000_000
TEAMS = {"NYY": (147, "New York Yankees"), "BOS": (111, "Boston Red Sox"), "TOR": (141, "Toronto Blue Jays"),
         "BAL": (110, "Baltimore Orioles"), "CHC": (112, "Chicago Cubs"), "MIL": (158, "Milwaukee Brewers"),
         "LAD": (119, "Los Angeles Dodgers"), "SD": (135, "San Diego Padres"), "SF": (137, "San Francisco Giants"),
         "AZ": (109, "Arizona Diamondbacks")}
FP_901 = iso_ms("2026-08-04T23:12:00Z")          # 901's first pitch
N1, NYY_O, BOS_O = "0001-n1", "o-nyy", "o-bos"


class Clock:
    t = iso_ms("2026-10-14T00:00:00Z")

    def __call__(self):
        return self.t


def rest(clock, stream, rid, path, purpose, body, subjects=(), query="", text=None):
    clock.t += 1000
    stream.event(rid, "rest_request", request_id=rid, method="GET", origin="https://x", path=path, query=query,
                 subjects=list(subjects), purpose=purpose, start_ts_ms=clock.t)
    stream.write("in", rid, text if text is not None else json.dumps(body), recv_ts_ms=clock.t + 5)
    clock.t += 6
    stream.event(rid, "rest_complete", request_id=rid, status=200, failure=None, response_headers=[])


def row(pk, date, start, away, home, status="Final", winner=None, dh="N", gn=1, **kw):
    side = lambda abbr, s: {"team": {"id": TEAMS[abbr][0], "name": TEAMS[abbr][1]},
                            **({"isWinner": winner == abbr} if winner else {})}
    return {"gamePk": pk, "season": "2026", "officialDate": date, "gameDate": start, "doubleHeader": dh,
            "gameNumber": gn, "status": {"detailedState": status},
            "teams": {"away": side(away, "away"), "home": side(home, "home")}, **kw}


SCHEDULE = [
    row(901, "2026-08-04", "2026-08-04T23:10:00Z", "NYY", "BOS", winner="BOS"),
    row(902, "2026-08-05", "2026-08-05T23:10:00Z", "NYY", "BOS", winner="NYY"),
    row(903, "2026-08-04", "2026-08-04T22:35:00Z", "TOR", "BAL", status="Postponed"),
    row(903, "2026-08-05", "2026-08-05T22:35:00Z", "TOR", "BAL", winner="TOR", rescheduledFrom="2026-08-04"),
    row(904, "2026-08-04", "2026-08-05T00:10:00Z", "CHC", "MIL", status="Cancelled"),
    row(905, "2026-08-04", "2026-08-04T20:10:00Z", "LAD", "SD", winner="SD", dh="Y", gn=1),
    row(906, "2026-08-04", "2026-08-04T20:15:00Z", "LAD", "SD", winner="LAD", dh="Y", gn=2),
    row(907, "2026-08-04", "2026-08-05T01:40:00Z", "SF", "AZ", winner="AZ"),
]


def market(ticker, result, close, title="t"):
    return {"ticker": ticker, "event_ticker": ticker.rsplit("-", 1)[0], "title": title, "status": "finalized",
            "result": result, "notional_value_dollars": "1.0000", "close_time": close, "settlement_ts": close,
            "rules_primary": "If the team wins...", "rules_secondary": "postponed: within two days"}


KALSHI = [
    market("KXMLBGAME-26AUG041910NYYBOS-NYY", "no", "2026-08-05T02:30:00Z"),        # 901: clean
    market("KXMLBGAME-26AUG041910NYYBOS-BOS", "yes", "2026-08-05T02:30:00Z"),
    market("KXMLBGAME-26AUG061910NYYBOS-NYY", "yes", "2026-08-06T02:40:00Z"),        # 902, dated a day late
    market("KXMLBGAME-26AUG061910NYYBOS-BOS", "no", "2026-08-06T02:40:00Z"),
    market("KXMLBGAME-26AUG041835TORBAL-TOR", "yes", "2026-08-06T01:30:00Z"),        # 903, postponed
    market("KXMLBGAME-26AUG041835TORBAL-BAL", "no", "2026-08-06T01:30:00Z"),
    market("KXMLBGAME-26AUG042010CHCMIL-CHC", "scalar", "2026-08-05T04:00:00Z"),     # 904, cancelled
    market("KXMLBGAME-26AUG042010CHCMIL-MIL", "scalar", "2026-08-05T04:00:00Z"),
    market("KXMLBGAME-26AUG041610LADSDG1-LAD", "no", "2026-08-04T23:00:00Z"),        # 905/906: two fit
    market("KXMLBGAME-26AUG041610LADSDG1-SD", "yes", "2026-08-04T23:00:00Z"),
    market("KXMLBGAME-26AUG042140SFARI-SF", "no", "2026-08-05T04:40:00Z"),           # 907: Kalshi's ARI is AZ
    market("KXMLBGAME-26AUG042140SFARI-ARI", "yes", "2026-08-05T04:40:00Z"),
]
BOS_T = "KXMLBGAME-26AUG041910NYYBOS-BOS"


def candle(end, bid, ask, price=None):
    p = {"close": price, "high": price, "low": price, "open": price} if price else {}
    return {"end_period_ts": iso_ms(end) // 1000, "open_interest": "10.00", "volume": "1.00", "price": p,
            "yes_bid": {"close": bid, "high": bid, "low": bid, "open": bid},
            "yes_ask": {"close": ask, "high": ask, "low": ask, "open": ask}}


def tape(tid, when, price, count):
    return {"trade_id": tid, "ticker": BOS_T, "created_time": when, "yes_price_dollars": price,
            "no_price_dollars": "x", "count_fp": count, "taker_side": "yes"}


CSV = "\n".join([
    "timestamp,outcomeId,marketId,contractSeries,league,marketType,tradeType,legs,cost,qty,side",
    # A: one taker, one maker on the other outcome: balanced.
    f"2026-08-04T21:00:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,50,156.25,TAKER",
    f"2026-08-04T21:00:00.000Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,106.25,156.25,MAKER",
    # B: one taker filled by two makers: balanced.
    f"2026-08-04T21:05:00.000Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,30,100,TAKER",
    f"2026-08-04T21:05:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,42,60,MAKER",
    f"2026-08-04T21:05:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,28,40,MAKER",
    # C: two identical maker rows, and a taker that does not balance them: ambiguous.
    f"2026-08-04T21:10:00.000Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,50,100,MAKER",
    f"2026-08-04T21:10:00.000Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,50,100,MAKER",
    f"2026-08-04T21:10:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,75,150,TAKER",
    # D: no milliseconds in the timestamp.
    f"2026-08-04T21:15:00Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,20,40,MAKER",
    f"2026-08-04T21:15:00Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,20,40,TAKER",
    # E: payouts balance, but the taker bought the makers' own outcome: ambiguous.
    f"2026-08-04T21:25:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,10,20,TAKER",
    f"2026-08-04T21:25:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,10,20,MAKER",
    # A parlay leg, another league, and a trade after the first pitch.
    f"2026-08-04T21:20:00.000Z,{BOS_O},{N1},Parlay,MLB,MONEY,COMBO,2,10,30,MAKER",
    "2026-08-04T21:20:00.000Z,o-x,nba-1,Basketball,NBA,MONEY,STRAIGHT,1,5,10,MAKER",
    f"2026-08-04T23:30:00.000Z,{BOS_O},{N1},Baseball Moneyline,MLB,MONEY,STRAIGHT,1,60,100,MAKER",
]) + "\n"


def write_hist(root: Path, csv_text: str = CSV, sid: str = "h1") -> None:
    clock = Clock()
    w = ArchiveWriter(root, session_id=sid, segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    m = w.stream("mlb_hist", f"backfill-{sid}")
    rest(clock, m, "t", "/api/v1/teams", "teams", {"teams": [
        {"id": i, "abbreviation": a, "name": n} for a, (i, n) in TEAMS.items()]}, query="sportId=1&season=2026")
    rest(clock, m, "s", "/api/v1/schedule", "schedule", {"dates": [{"games": SCHEDULE}]},
         query="sportId=1&startDate=2026-08-01&endDate=2026-08-31&gameType=R%2CF%2CD%2CL%2CW")
    for pk, fp in ((901, "2026-08-04T23:12:00Z"), (902, "2026-08-05T23:12:00Z"), (903, "2026-08-05T22:37:00Z"),
                   (905, "2026-08-04T20:11:00Z"), (906, "2026-08-04T23:40:00Z"), (907, "2026-08-05T01:41:00Z")):
        rest(clock, m, f"p{pk}", f"/api/v1/game/{pk}/playByPlay", "play_by_play", {"allPlays": [{"playEvents": [
            {"isPitch": True, "startTime": fp, "type": "pitch", "details": {"description": "Ball"}}]}]},
             [f"mlb:game:{pk}"])
    k = w.stream("kalshi_hist", f"backfill-{sid}")
    rest(clock, k, "c", "/trade-api/v2/historical/cutoff", "cutoff",
         {"market_settled_ts": "2026-08-14T00:00:00Z", "trades_created_ts": "2026-08-14T00:00:00Z"})
    rest(clock, k, "e", "/trade-api/v2/events", "events", {"cursor": "", "events": [
        {"event_ticker": KALSHI[0]["event_ticker"], "markets": []}]})              # the historical tier lists none
    rest(clock, k, "h", "/trade-api/v2/historical/markets", "markets", {"cursor": "", "markets": KALSHI})
    rest(clock, k, "k1", f"/trade-api/v2/historical/markets/{BOS_T}/candlesticks", "candles", {"ticker": BOS_T,
         "candlesticks": [candle("2026-08-04T22:00:00Z", "0.6100", "0.6300", "0.6200"),
                          candle("2026-08-04T22:01:00Z", "0.6400", "0.6300"),               # crossed
                          candle("2026-08-04T23:12:00Z", "0.6000", "0.6200"),               # straddles the pitch
                          candle("2026-08-04T23:20:00Z", "0.5000", "0.5200")]},             # in play
         [f"kalshi:market:{BOS_T}"], query="start_ts=1&end_ts=2&period_interval=1")
    rest(clock, k, "k2", "/trade-api/v2/historical/trades", "trades", {"cursor": "", "trades": [
        tape("ta", "2026-08-04T22:00:00.123456Z", "0.6200", "100.00"),
        tape("tb", "2026-08-04T23:30:00Z", "0.5000", "5.00")]}, [f"kalshi:market:{BOS_T}"], query=f"ticker={BOS_T}")
    rest(clock, k, "k3", "/trade-api/v2/markets/trades", "trades", {"cursor": "", "trades": [
        tape("ta", "2026-08-04T22:00:00.123456Z", "0.6200", "100.00")]}, [f"kalshi:market:{BOS_T}"],
         query=f"ticker={BOS_T}")                                                    # the same trade, other tier
    n = w.stream("novig_hist", f"backfill-{sid}")
    rest(clock, n, "i", "/reporting/trade-data/index.json", "index", {"dates": ["2026-08-04"]})
    rest(clock, n, "d", "/reporting/trade-data/2026-08-04/trades.csv", "trades_csv", None, text=csv_text)
    for mid, eid, desc, start, outs in (
            (N1, "e1", "New York Yankees @ Boston Red Sox", "2026-08-04T23:10:00Z",
             [(NYY_O, "NYY", "LOSE"), (BOS_O, "BOS", "WIN")]),
            ("0005-n5", "e5", "Los Angeles Dodgers @ San Diego Padres", "2026-08-04T20:10:00Z",
             [("o-lad", "LAD", "LOSE"), ("o-sd", "SD", "WIN")])):
        rest(clock, n, f"m{mid}", f"/v3/history/markets/{mid}", "history_market", {
            "marketId": mid, "eventId": eid, "description": "x", "marketType": "MONEY", "status": "SETTLED",
            "outcomes": [{"outcomeId": o, "name": nm, "status": s} for o, nm, s in outs]}, [f"novig:market:{mid}"])
        rest(clock, n, f"v{mid}", f"/v3/history/events/{eid}", "history_event", {
            "eventId": eid, "description": desc, "league": "MLB", "status": "FINAL", "startsTs": iso_ms(start)},
             [f"novig:event:{eid}"])
    w.close()


def write_live(root: Path) -> None:
    """The R0 stream for N1: t1 is the CSV's 21:00 MAKER fill, 5 s earlier; t2 has no row; t3 has
    D's outcome, size and time but not its price; a later snapshot repeats t1 and t2."""
    clock = Clock()
    clock.t = iso_ms("2026-08-04T20:00:00Z")
    w = ArchiveWriter(root, session_id="r1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    s = w.stream("novig", "c1")
    t1 = {"tradeId": "t1", "outcomeId": NYY_O, "price": "0.68", "qty": "15625", "ts": iso_ms("2026-08-04T20:59:55Z")}
    t2 = {"tradeId": "t2", "outcomeId": BOS_O, "price": "0.55", "qty": "300", "ts": iso_ms("2026-08-04T21:30:00Z")}
    s.write("in", "c1", json.dumps({"delta": {N1: {"trades": {"seq": 5, "deltas": [t1]}}}, "ts": 1}))
    s.write("in", "c1", json.dumps({"delta": {N1: {"trades": {"seq": 6, "deltas": [t2]}}}, "ts": 2}))
    t3 = {"tradeId": "t3", "outcomeId": BOS_O, "price": "0.52", "qty": "4000", "ts": iso_ms("2026-08-04T21:14:58Z")}
    s.write("in", "c1", json.dumps({"delta": {N1: {"trades": {"seq": 7, "deltas": [t3]}}}, "ts": 2}))
    s.write("in", "c1", json.dumps({"snapshot": {N1: {"trades": {"seq": 6, "trades": [{"deltas": [t1, t2]}]}}},
                                    "ts": 3}))
    w.close()


@pytest.fixture
def archives(tmp_path):
    write_hist(tmp_path / "hist")
    write_live(tmp_path / "live")
    return tmp_path / "hist", tmp_path / "live"


def q(c, sql, *args):
    return [tuple(r) for r in c.execute(sql, args)]


def counts(c):
    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type = 'table'"
                                      " AND name NOT IN ('schema_migration')")]
    return {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables}


def loaded(archives, tmp_path):
    hist, live = archives
    c = db.connect(":memory:")
    db.migrate(c)
    backfill.import_identity(c, hist, NOW)
    found, _ = backfill.link(c, hist, tmp_path / "b1", NOW)
    rep = backfill.import_data(c, hist, live, NOW)
    return c, {(k.venue, k.native_event): k for k in found}, rep


def test_identity_needs_no_game_and_loads_no_data_before_links(archives):
    hist, live = archives
    c = db.connect(":memory:")
    db.migrate(c)
    rep = backfill.import_identity(c, hist, NOW)
    assert rep["mlb_games"] == 7 and rep["kalshi_markets_listed"] == 12 and rep["novig_markets_listed"] == 2
    assert q(c, "SELECT count(*) FROM event") == [(7,)] and q(c, "SELECT count(*) FROM outcome") == [(14,)]
    assert q(c, "SELECT venue, count(*) FROM venue_instrument GROUP BY 1 ORDER BY 1") == [("kalshi", 12), ("novig", 2)]
    assert q(c, "SELECT count(*) FROM instrument_mapping_observation") == [(0,)]
    # The postponed game's two schedule rows are two observations of one event.
    assert q(c, """SELECT count(*) FROM event_alias a JOIN event e USING (event_id)
                   WHERE e.league_game_id = '903'""") == [(2,)]
    assert q(c, "SELECT count(*) FROM off_observation WHERE kind = 'first_pitch'") == [(6,)]
    rep = backfill.import_data(c, hist, live, NOW)
    assert rep["kalshi_linked"] == rep["novig_linked"] == 0
    assert q(c, "SELECT count(*) FROM historical_candle") == [(0,)]


def test_the_rules_propose_clean_links_and_list_every_edge_case(archives, tmp_path):
    c, found, _ = loaded(archives, tmp_path)
    k = lambda ev: found[("kalshi", ev)]
    assert (k("KXMLBGAME-26AUG041910NYYBOS").status, k("KXMLBGAME-26AUG041910NYYBOS").game.game_pk) == ("clean", 901)
    assert (k("KXMLBGAME-26AUG042140SFARI").status, k("KXMLBGAME-26AUG042140SFARI").game.game_pk) == ("clean", 907)
    late = k("KXMLBGAME-26AUG061910NYYBOS")              # 823491: the ticker names the next day
    assert (late.status, late.game.game_pk) == ("edge", 902) and "ticker dated 2026-08-06" in late.reasons[0]
    moved = k("KXMLBGAME-26AUG041835TORBAL")             # 824785: settled on the rescheduled game
    assert (moved.status, moved.game.game_pk) == ("edge", 903) and "postponed or rescheduled" in moved.reasons
    cancelled = k("KXMLBGAME-26AUG042010CHCMIL")         # 823490: a fair-price settlement
    assert cancelled.status == "excluded" and "scalar" in cancelled.reasons[0] and not cancelled.sides
    dh = k("KXMLBGAME-26AUG041610LADSDG1")               # both games of the doubleheader fit
    assert dh.status == "edge" and dh.game is None and [g.game_pk for g in dh.candidates] == [905, 906]
    assert found[("novig", N1)].status == "clean" and found[("novig", N1)].game.game_pk == 901
    assert found[("novig", "0005-n5")].status == "edge"  # two games of the doubleheader start within 6 h
    # Unverified mappings for clean links and single-candidate edges; none for the rest.
    mapped = dict(q(c, """SELECT v.native_id, count(*) FROM instrument_mapping_observation m
                          JOIN venue_instrument v USING (venue_instrument_id) GROUP BY 1"""))
    assert set(mapped) == {*(m["ticker"] for m in KALSHI[:6] + KALSHI[10:]), N1}
    assert set(mapped.values()) == {2} and q(c, "SELECT DISTINCT mapping_status FROM instrument_mapping_observation"
                                             ) == [("fuzzy_candidate",)]
    # Kalshi YES on BOS is BOS direct, and NO its complement; Novig's sides follow its outcomes.
    assert q(c, """SELECT m.side, o.team_abbreviation, m.polarity FROM instrument_mapping_observation m
                   JOIN venue_instrument v USING (venue_instrument_id) JOIN outcome o USING (outcome_id)
                   WHERE v.native_id = ? ORDER BY 1""", BOS_T) == [(0, "BOS", "direct"), (1, "BOS", "complement")]
    proposed = (tmp_path / "b1" / "links.proposed.toml").read_text()
    assert proposed.count("[[correction]]") == 2 * 4 + 2          # two clean Kalshi games, one Novig market
    review_text = (tmp_path / "b1" / "links.review.md").read_text()
    assert "KXMLBGAME-26AUG041610LADSDG1" in review_text and "candidate: gamePk 906" in review_text


def test_a_novig_market_links_from_its_latest_history_not_the_last_one_read(archives, tmp_path):
    # An earlier copy, from before the market settled and with the event's first estimate of
    # its start, journaled after the settled one (an interrupted run's segments are sealed by
    # the next run): the latest response decides (PR #16 review).
    hist, _ = archives
    clock = Clock()
    clock.t = iso_ms("2026-10-14T00:00:00Z")
    w = ArchiveWriter(hist, session_id="h9", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    n = w.stream("novig_hist", "backfill-h9")
    rest(clock, n, "m-old", f"/v3/history/markets/{N1}", "history_market", {
        "marketId": N1, "eventId": "e1", "description": "x", "marketType": "MONEY", "status": "OPEN",
        "outcomes": [{"outcomeId": NYY_O, "name": "NYY", "status": "OPEN"},
                     {"outcomeId": BOS_O, "name": "BOS", "status": "OPEN"}]}, [f"novig:market:{N1}"])
    rest(clock, n, "v-old", "/v3/history/events/e1", "history_event", {
        "eventId": "e1", "description": "New York Yankees @ Boston Red Sox", "league": "MLB", "status": "OPEN",
        "startsTs": iso_ms("2026-08-05T17:10:00Z")}, ["novig:event:e1"])
    w.close()
    found, events = novig_public.markets(hist)
    assert found[N1][1].status == "SETTLED" and events["e1"].status == "FINAL"
    _, links, _ = loaded(archives, tmp_path)
    assert links[("novig", N1)].status == "clean" and links[("novig", N1)].game.game_pk == 901


def test_approving_the_proposals_verifies_exactly_the_clean_links(archives, tmp_path):
    c, _, _ = loaded(archives, tmp_path)
    approved = tmp_path / "b1" / "links.proposed.toml"
    identity.apply_corrections(c, approved, NOW + 1)
    heads = identity.current_mappings(c, "instrument_mapping_observation")
    verified = {q(c, "SELECT native_id FROM venue_instrument WHERE venue_instrument_id = ?", iid)[0][0]
                for (iid, _), h in heads.items() if h.status == "manual_verified"}
    assert verified == {*(m["ticker"] for m in KALSHI[:2] + KALSHI[10:]), N1}


def test_kalshi_data_stops_at_the_first_pitch_and_dedupes_by_trade_id(archives, tmp_path):
    c, _, rep = loaded(archives, tmp_path)
    assert q(c, "SELECT interval_start_ms, interval_end_ms, bid_close_e4, ask_close_e4, price_close_e4, tier"
                " FROM historical_candle ORDER BY 1") == [
        (iso_ms("2026-08-04T21:59:00Z"), iso_ms("2026-08-04T22:00:00Z"), 6100, 6300, 6200, "historical"),
        (FP_901 - 60_000, FP_901, 6000, 6200, None, "historical")]
    assert rep["kalshi_candles_crossed"] == 1 and rep["kalshi_candles_after_first_pitch"] == 1
    assert q(c, "SELECT execution_key, price_num, price_den, payout_cents FROM trade_execution"
                " WHERE execution_key = 'ta'") == [("ta", 31, 50, 10000)]
    assert q(c, "SELECT source FROM trade_source_observation WHERE native_trade_id = 'ta' ORDER BY 1") == [
        ("kalshi_hist",), ("kalshi_live",)]                     # one execution, two sightings
    assert rep["kalshi_trades_after_first_pitch"] == 1


def test_novig_rows_reconcile_across_sides_and_with_the_stream(archives, tmp_path):
    c, _, rep = loaded(archives, tmp_path)
    iid = q(c, "SELECT venue_instrument_id FROM venue_instrument WHERE native_id = ?", N1)[0][0]
    # Executions: one per MAKER row before the pitch (A, B x2, C x2, D, E), and the unmatched
    # stream t2 and t3 (D's outcome, size and time, but 0.52 against 0.50).
    keys = [r[0] for r in q(c, "SELECT execution_key FROM trade_execution WHERE venue_instrument_id = ? ORDER BY 1",
                            iid)]
    assert len(keys) == 9 and {"novig:trade:t2", "novig:trade:t3"} <= set(keys)
    assert sum(k.startswith(f"novig:maker:{NYY_O}:{iso_ms('2026-08-04T21:10:00Z')}:50:100:") for k in keys) == 2
    assert rep["novig_rows_combo"] == 1 and rep["novig_rows_after_first_pitch"] == 1
    groups = dict(q(c, "SELECT dual_side_group, reconcile FROM trade_source_observation WHERE role = 'taker'"))
    assert groups == {f"{N1}@2026-08-04T21:00:00.000Z": "balanced", f"{N1}@2026-08-04T21:05:00.000Z": "balanced",
                      f"{N1}@2026-08-04T21:10:00.000Z": "ambiguous", f"{N1}@2026-08-04T21:15:00Z": "balanced",
                      f"{N1}@2026-08-04T21:25:00.000Z": "ambiguous"}
    # One economic trade, three records: the stream's t1, its MAKER row and the TAKER row of its group.
    (t1,) = q(c, "SELECT execution_id, reconcile FROM trade_source_observation WHERE native_trade_id = 't1'")
    assert t1[1] == "rule_matched"
    assert q(c, "SELECT role FROM trade_source_observation WHERE execution_id = ? ORDER BY 1", t1[0]) == [
        ("maker",), ("stream",)]
    # The stream's time decides the execution's: 5 s before the file's.
    assert trade_vwap(c, iid, 0, iso_ms("2026-08-04T20:59:55Z"), iso_ms("2026-08-04T21:00:00Z")).executions == 1


def test_every_step_is_idempotent(archives, tmp_path):
    hist, live = archives
    c, _, _ = loaded(archives, tmp_path)
    before = counts(c)
    backfill.import_identity(c, hist, NOW + 5)
    backfill.link(c, hist, tmp_path / "b1", NOW + 5)
    backfill.import_data(c, hist, live, NOW + 5)
    assert counts(c) == before


def test_a_streamed_trade_waits_for_its_days_file(archives, tmp_path):
    # Stream first would make t1's execution carry the stream's 3-place price; so it waits.
    _, live = archives
    write_hist(tmp_path / "no-file", "timestamp,outcomeId,marketId,contractSeries,league,marketType,tradeType,legs,"
                                     "cost,qty,side\n", "h3")
    c = db.connect(":memory:")
    db.migrate(c)
    backfill.import_identity(c, tmp_path / "no-file", NOW)
    backfill.link(c, tmp_path / "no-file", tmp_path / "b1", NOW)
    _, novig, _ = backfill.bounds(c)
    w, rep = Writer(c, NOW), Counter()
    novig_public.import_stream(w, live, novig, set(), rep)
    assert rep["novig_stream_trades_waiting_for_their_file"] == 5 and q(c, "SELECT count(*) FROM trade_execution") == [(0,)]
    novig_public.import_stream(w, live, novig, {"2026-08-04"}, rep)     # the file is archived: it no longer waits
    assert q(c, "SELECT count(*) FROM trade_execution") == [(3,)]


def test_a_republished_file_adds_sightings_not_executions(archives, tmp_path):
    c, _, _ = loaded(archives, tmp_path)
    executions = q(c, "SELECT count(*) FROM trade_execution")
    write_hist(tmp_path / "hist2", CSV, "h2")            # the same rows, fetched again by another run
    backfill.import_identity(c, tmp_path / "hist2", NOW)
    backfill.import_data(c, tmp_path / "hist2", None, NOW)
    assert q(c, "SELECT count(*) FROM trade_execution") == executions
    assert q(c, "SELECT count(DISTINCT raw_artifact_id) FROM trade_source_observation WHERE role = 'maker'") == [(2,)]


def test_a_trade_vwap_counts_each_execution_once(archives, tmp_path):
    c, _, _ = loaded(archives, tmp_path)
    iid = q(c, "SELECT venue_instrument_id FROM venue_instrument WHERE native_id = ?", BOS_T)[0][0]
    v = trade_vwap(c, iid, 0, 0, FP_901)
    assert (v.p, v.executions, v.payout_cents) == (Fraction(31, 50), 1, 10000)


def test_links_need_two_settled_markets():
    games = {}
    one = {"KX-A": (None, type("M", (), dict(event_ticker="KXMLBGAME-26AUG04X", status="finalized", ticker="KX-A"))())}
    (link,) = links.kalshi_links(one, games)
    assert link.status == "excluded"


def test_a_crossed_candle_would_be_refused_by_the_schema_too(archives, tmp_path):
    c, _, _ = loaded(archives, tmp_path)
    (cid,) = q(c, "SELECT candle_id FROM historical_candle LIMIT 1")[0]
    with pytest.raises(sqlite3.IntegrityError):
        c.execute("INSERT INTO historical_candle SELECT NULL, venue_instrument_id, source, tier, resolution_s,"
                  " interval_start_ms, interval_end_ms, side, bid_open_e4, bid_high_e4, bid_low_e4, 9000, ask_open_e4,"
                  " ask_high_e4, ask_low_e4, ask_close_e4, price_open_e4, price_high_e4, price_low_e4, price_close_e4,"
                  " volume_native, open_interest_native, native_json, observed_ts_ms, raw_artifact_id, raw_line, 99"
                  " FROM historical_candle WHERE candle_id = ?", (cid,))


def test_one_trade_traces_to_every_record_in_both_archives(archives, tmp_path):
    from clv import trace
    hist, live = archives
    c, _, _ = loaded(archives, tmp_path)
    (eid,) = q(c, "SELECT execution_id FROM trade_source_observation WHERE native_trade_id = 't1'")[0]
    doc = trace.trace_execution(c, eid, live, hist)
    assert doc["execution"]["price"]["exact"] == "17/25" and doc["execution"]["payout_cents"] == 15625
    by_role = {s["role"]: s for s in doc["sightings"]}
    assert by_role["maker"]["record"] == (f"2026-08-04T21:00:00.000Z,{NYY_O},{N1},Baseball Moneyline,MLB,MONEY,"
                                          "STRAIGHT,1,106.25,156.25,MAKER")
    assert by_role["maker"]["frame"]["ref"].startswith("novig_hist/")
    assert json.loads(by_role["stream"]["record"])["tradeId"] == "t1"
    assert by_role["stream"]["frame"]["ref"].startswith("novig/")
    (taker,) = doc["dual_side_takers"]
    assert taker["record"].endswith(",50,156.25,TAKER") and taker["reconcile"] == "balanced"
    with pytest.raises(trace.TraceError, match="backfill archive root"):
        trace.trace_execution(c, eid, live, None)


def test_the_coverage_table_rebuilds_identically_and_holds_no_prices(archives, tmp_path):
    from clv.backfill import coverage
    c, _, _ = loaded(archives, tmp_path)
    identity.apply_corrections(c, tmp_path / "b1" / "links.proposed.toml", NOW + 1)
    _, rows, extra = coverage.table(c, NOW + 2)
    by = {r["game_pk"]: r for r in rows}
    g = by[901]
    # 901: verified on both venues; the start is the first pitch, the cutoff 60 s before it.
    assert (g["off"], g["kalshi_link"], g["novig_link"]) == ("trusted", "manual_verified", "manual_verified")
    assert g["cutoff"] == "2026-08-04T23:11:00.000Z"
    # The candle ending at 22:00 is the last wholly before the cutoff: 71 min old, past the
    # 30 min max quote age, so there is no candle close; the one straddling the pitch is excluded.
    assert g["kalshi_candle_age_s"] is None and g["kalshi_reason"] == "no_candle_before_cutoff"
    assert g["kalshi_candle_age_s_at_3600"] == 720                # 22:12, an hour before the start, is 12 min after 22:00
    # Novig: no MAKER fill in the 10 min before the cutoff.
    assert (g["novig_executions_in_window"], g["novig_reason"]) == (0, "vwap_insufficient")
    assert by[904]["off"] == "no_trusted_off" and by[904]["kalshi_reason"] == "no_trusted_off"
    assert by[902]["kalshi_reason"] == "mapping_unverified"    # an edge case: linked, not approved
    assert by[906]["kalshi_reason"] == "no_link"                # the doubleheader: no candidate chosen
    assert extra == {"duplicate_candle_conflicts": 0}
    # Counts, ages and flags only.
    assert not [k for k in coverage.COLUMNS if "price" in k or "vwap" in k or "bid" in k or "ask" in k]
    _, again, _ = coverage.table(c, NOW + 3)
    assert again == rows
    assert [s for s, _, _ in coverage.waterfall(rows, "kalshi")][-1] == "no_candle_before_cutoff"
