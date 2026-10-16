"""Depth-walk benchmarks (DESIGN.md §2.4), and the historical candle and VWAP closes (§5.6).

The V0 worked fixture: canonical bids (0.40, $300), (0.35, $400) and asks
(0.60, $200), (0.65, $500), quantities in payout dollars. At N = $500 the bid
VWAP is 0.38, the ask VWAP 0.63 and the midpoint 0.505; the complement's is
0.495. At N = $1,000 both ladders are short. $500 of payout is 50,000 Novig
contracts at $0.01, and the cash proceeds and cost are $190 and $315.
"""
import sqlite3
from decimal import Decimal
from fractions import Fraction

import pytest

from clv import db, lineage
from clv.close import definitions as close
from clv.close import depth
from clv.close.candles import candle_close
from clv.close.vwap import Executions
from clv.identity import Mapping, reverse
from clv.ingest import payout_cents
from clv.venues.protocol import Level


def usd(price, dollars):
    return Level(price, Decimal(dollars * 100))         # quantity in payout cents


BIDS = (usd("0.40", 300), usd("0.35", 400))
ASKS = (usd("0.60", 200), usd("0.65", 500))


def test_worked_fixture_at_500():
    b = depth.mid_depth(BIDS, ASKS, 500, truncated=False)
    assert (b.bid, b.ask, b.p) == (Fraction("0.38"), Fraction("0.63"), Fraction("0.505"))
    assert b.reason is None


def test_complement_midpoint_is_one_minus():
    bids, asks = reverse(BIDS, ASKS)
    assert depth.mid_depth(bids, asks, 500, truncated=False).p == Fraction("0.495")
    assert [lv.qty for lv in bids] == [lv.qty for lv in ASKS]        # depth follows the side


def test_both_ladders_are_short_at_1000_and_never_renormalized():
    b = depth.mid_depth(BIDS, ASKS, 1000, truncated=False)
    assert (b.p, b.reason) == (None, "insufficient_depth")
    assert depth.side_vwap(BIDS, 100_000) is None and depth.side_vwap(ASKS, 100_000) is None


def test_a_short_stored_ladder_fails_closed():
    assert depth.mid_depth(BIDS, ASKS, 1000, truncated=True).reason == "truncated_ladder"
    # A truncated ladder that still covers N is fine: the walk never reached the cut.
    assert depth.mid_depth(BIDS, ASKS, 500, truncated=True).p == Fraction("0.505")


def test_unit_conversion_and_cash_figures():
    assert payout_cents(Decimal(50_000), 1, "novig") == 500_00        # 50,000 contracts at $0.01
    assert payout_cents(Decimal("100.50"), 100, "kalshi") == 100_50   # Kalshi pays $1, two-decimal quantities
    n = 500_00
    assert depth.side_vwap(BIDS, n) * n / 100 == 190 and depth.side_vwap(ASKS, n) * n / 100 == 315


def test_partial_final_level_is_proportional_and_exact():
    # $250 of bids: all $300 is not needed; 250 at 0.40.
    assert depth.side_vwap(BIDS, 250_00) == Fraction("0.40")
    # $350 of asks: $200 at 0.60 and $150 of the $500 at 0.65.
    assert depth.side_vwap(ASKS, 350_00) == (Fraction("0.60") * 200 + Fraction("0.65") * 150) / 350
    # Native strings of any precision walk exactly (Novig quotes "0.415").
    assert depth.side_vwap((usd("0.415", 1),), 100) == Fraction(83, 200)


def test_mid_top_and_empty_sides():
    assert depth.mid_top(BIDS, ASKS).p == Fraction(1, 2)
    for bids, asks in (((), ASKS), (BIDS, ())):
        assert depth.mid_top(bids, asks).reason == "no_quotes"
        assert depth.mid_depth(bids, asks, 100, truncated=False).reason == "no_quotes"


@pytest.mark.parametrize("n", [100, 500])
def test_complement_identity_holds_at_each_notional(n):
    p = depth.mid_depth(BIDS, ASKS, n, truncated=False).p
    assert depth.mid_depth(*reverse(BIDS, ASKS), n, truncated=False).p == 1 - p


# -- B2: the historical closes (DESIGN.md §5.6) --------------------------------------------------------

START = 1_791_504_000_000                 # the earliest plausible start
CUT = START - 60_000                      # the cutoff at close.buffer_s
MIN = 60_000
H = "a" * 64


def ins(c, table, **row):
    return c.execute(f"INSERT INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                     tuple(row.values())).lastrowid


@pytest.fixture
def facts():
    """A game with a Kalshi market (YES = the home team) and a Novig market (side 1 = home)."""
    c = db.connect(":memory:")
    db.migrate(c)
    art = ins(c, "raw_artifact", source="kalshi_hist", relpath="kalshi_hist/x.jsonl.gz", sha256=H, bytes=1,
              frame_count=1, recorder_version="0", redaction_policy="r", unclean_close=0, parser_version="v",
              ingested_ts_ms=START)
    event = ins(c, "event", league="MLB", league_game_id="1", created_ts_ms=START)
    home = ins(c, "outcome", event_id=event, kind="team_wins", team_id=1, team_abbreviation="HOM", description="h")
    ids = {"art": art, "home": home, "item": 0}
    for venue, native, cents in (("kalshi", "KX-HOM", 100), ("novig", "nv", 1)):
        ids[venue] = ins(c, "venue_instrument", venue=venue, native_id=native, native_event_id="e",
                         kind="exchange_book", side0_id="a", side0_label="A", side1_id="b", side1_label="B",
                         payout_currency="USD", payout_cents_per_contract=cents, first_seen_ts_ms=START,
                         raw_artifact_id=art, raw_line=0)
    return c, ids


def add_candle(c, ids, end, bid, ask, minutes=1, tier="historical"):
    ids["item"] += 1
    return ins(c, "historical_candle", venue_instrument_id=ids["kalshi"], source="kalshi_hist", tier=tier,
               resolution_s=60 * minutes, interval_start_ms=end - minutes * MIN, interval_end_ms=end, side=0,
               bid_close_e4=bid, ask_close_e4=ask, native_json="{}", observed_ts_ms=START, raw_artifact_id=ids["art"],
               raw_line=1, raw_item=ids["item"])


def at(c, ids, as_of=CUT, side=0, age_s=1800):
    snap = lineage.take(c, START)
    lineage.use(c, snap)
    return candle_close(c, ids["kalshi"], side, as_of, age_s)


def test_the_latest_candle_wholly_before_the_cutoff_is_the_close(facts):
    c, ids = facts
    add_candle(c, ids, CUT - 5 * MIN, 4000, 4200)
    last = add_candle(c, ids, CUT - 2 * MIN, 5100, 5300)
    r = at(c, ids)
    assert (r.p, r.reason, r.candle_id, r.cutoff_candle_active) == (Fraction(52, 100), None, last, False)
    assert (r.quote_age_ms, r.spread_e4) == (2 * MIN, 200)


def test_a_candle_ending_exactly_at_the_cutoff_is_wholly_before_it(facts):
    c, ids = facts
    last = add_candle(c, ids, CUT, 5100, 5300)
    assert (at(c, ids).candle_id, at(c, ids).quote_age_ms) == (last, 0)


def test_a_candle_containing_the_cutoff_is_skipped_and_flagged(facts):
    # Its OHLC may include activity after the cutoff, so the earlier candle is the close, as of
    # its own end, and the flag records that the book was active in the minute that was cut.
    c, ids = facts
    before = add_candle(c, ids, CUT - 30_000, 5100, 5300)
    add_candle(c, ids, CUT + 30_000, 6000, 6200)
    r = at(c, ids)
    assert (r.p, r.candle_id, r.cutoff_candle_active, r.quote_age_ms) == (Fraction(52, 100), before, True, 30_000)


def test_a_five_minute_candle_overlapping_the_start_leaves_no_close(facts):
    # DESIGN.md §9.4 item 6: the only candle near the cutoff overlaps the first pitch.
    c, ids = facts
    add_candle(c, ids, START + 2 * MIN, 5000, 5200, minutes=5)
    assert at(c, ids).reason == "ambiguous_candle"
    # An earlier candle older than close.max_quote_age_s does not rescue it.
    add_candle(c, ids, CUT - 3 * 3_600_000, 5000, 5200)
    assert at(c, ids).reason == "ambiguous_candle"


def test_no_candle_one_sided_and_stale(facts):
    c, ids = facts
    assert at(c, ids).reason == "no_quotes"
    add_candle(c, ids, CUT - 3 * 3_600_000, 5000, 5200)
    stale = at(c, ids)
    assert (stale.reason, stale.quote_age_ms) == ("stale_book", 3 * 3_600_000)
    # The latest candle is one-sided: the book had no ask, and an earlier two-sided candle is
    # not the book as of the cutoff.
    add_candle(c, ids, CUT - 4 * MIN, 5100, 5300)
    one_sided = add_candle(c, ids, CUT - MIN, 5100, None)
    r = at(c, ids)
    assert (r.p, r.reason, r.candle_id) == (None, "no_quotes", one_sided)


def test_tier_duplicates_must_agree(facts):
    c, ids = facts
    first = add_candle(c, ids, CUT - MIN, 5100, 5300, tier="live")
    add_candle(c, ids, CUT - MIN, 5100, 5300, tier="historical")
    assert (at(c, ids).p, at(c, ids).candle_id) == (Fraction(52, 100), first)
    add_candle(c, ids, CUT - MIN, 5000, 5300, tier="historical")
    assert at(c, ids).reason == "ambiguous_candle"


def test_the_complement_side_swaps_bid_and_ask(facts):
    c, ids = facts
    add_candle(c, ids, CUT - MIN, 5100, 5400)
    yes, no = at(c, ids, side=0), at(c, ids, side=1)
    assert no.p == 1 - yes.p == Fraction(475, 1000)
    assert no.spread_e4 == yes.spread_e4 == 300


def test_a_later_candle_never_leaks_into_an_earlier_snapshot(facts):
    c, ids = facts
    add_candle(c, ids, CUT - 2 * MIN, 5100, 5300)
    snap = lineage.take(c, START)
    add_candle(c, ids, CUT - MIN, 6100, 6300)
    lineage.use(c, snap)
    assert candle_close(c, ids["kalshi"], 0, CUT, 1800).p == Fraction(52, 100)


# -- the VWAP close ------------------------------------------------------------------------------------

def add_fill(c, ids, t, price, payout_cents, side=1, stream_t=None, key=None):
    """A Novig MAKER execution and its CSV sighting, and optionally its stream sighting."""
    ids["item"] += 1
    p = Fraction(price)
    x = ins(c, "trade_execution", venue_instrument_id=ids["novig"], execution_key=key or f"k{ids['item']}",
            side=side, price_native=price, price_num=p.numerator, price_den=p.denominator,
            qty_native=str(payout_cents), payout_cents=payout_cents, method="m", created_ts_ms=START,
            raw_artifact_id=ids["art"], raw_line=2, raw_item=ids["item"])
    common = dict(venue_instrument_id=ids["novig"], side=side, price_native=price, qty_native="1", method="m",
                  native_json="{}", observed_ts_ms=START, raw_artifact_id=ids["art"])
    ins(c, "trade_source_observation", execution_id=x, source="novig_csv", role="maker", dual_side_group=f"g{t}",
        venue_ts_ms=t, reconcile="key", raw_line=2, raw_item=ids["item"], **common)
    if stream_t is not None:
        ins(c, "trade_source_observation", execution_id=x, source="novig_stream", role="stream",
            venue_ts_ms=stream_t, reconcile="rule_matched", raw_line=3, raw_item=ids["item"], **common)
    return x


def vwap_at(c, ids, as_of=CUT, side=1, buffer_s=60):
    snap = lineage.take(c, START)
    lineage.use(c, snap)
    (d,) = [d for d in close.hist_defs([buffer_s]) if d.method == "trade_vwap"]
    ref = close.Reference("novig", "nv", ids["novig"], side,
                          Mapping(ids["novig"], side, 0, ids["home"], "direct", "manual_verified", "pending",
                                  "equivalence_pending"))
    return close.hist_price_as_of(c, ref, as_of, d, Executions(c, "f_"))


def test_vwap_window_is_closed_below_open_above_and_prefers_stream_time(facts):
    c, ids = facts
    lo = CUT - 600_000
    for t in (lo, lo + 1, CUT - 2, CUT - 1):
        add_fill(c, ids, t, "0.60", 10_000)
    add_fill(c, ids, CUT, "0.10", 10_000)                         # at the cutoff: excluded
    add_fill(c, ids, lo - 1, "0.10", 10_000)                      # before the window: excluded
    add_fill(c, ids, CUT + 20_000, "0.10", 10_000, stream_t=CUT - 5_000)   # the file is late; the stream is in
    r = vwap_at(c, ids)
    assert (r.p, r.reason, r.vwap_executions, r.vwap_payout_cents) == (Fraction(1, 2), None, 5, 50_000)
    assert (r.vwap_lo_ms, r.vwap_hi_ms, r.quote_age_ms) == (lo, CUT, 1)
    assert r.tick_id is None and r.candle_id is None


def test_a_taker_row_never_enters_the_vwap(facts):
    c, ids = facts
    for i in range(5):
        add_fill(c, ids, CUT - 10_000 - i, "0.60", 10_000)
    before = vwap_at(c, ids)
    ins(c, "trade_source_observation", venue_instrument_id=ids["novig"], execution_id=None, source="novig_csv",
        role="taker", dual_side_group=f"g{CUT - 10_000}", side=0, price_native="0.01", qty_native="1",
        venue_ts_ms=CUT - 10_000, observed_ts_ms=START, reconcile="balanced", method="m", native_json="{}",
        raw_artifact_id=ids["art"], raw_line=4, raw_item=0)
    assert vwap_at(c, ids) == before


def test_few_trades_keeps_its_counts(facts):
    c, ids = facts
    for i in range(4):
        add_fill(c, ids, CUT - 10_000 - i, "0.60", 10_000)
    r = vwap_at(c, ids)
    assert (r.p, r.reason, r.vwap_executions, r.vwap_payout_cents) == (None, "few_trades", 4, 40_000)
    add_fill(c, ids, CUT - 20_000, "0.60", 100)                   # a fifth: five fills, $401 of payout
    assert vwap_at(c, ids).reason is None


def test_few_trades_below_the_minimum_notional(facts):
    c, ids = facts
    for i in range(6):
        add_fill(c, ids, CUT - 10_000 - i, "0.60", 4_000)          # six fills, $240 of payout
    r = vwap_at(c, ids)
    assert (r.reason, r.vwap_executions, r.vwap_payout_cents) == ("few_trades", 6, 24_000)


def test_vwap_of_the_other_side_is_the_complement(facts):
    c, ids = facts
    for i, price in enumerate(("0.55", "0.60", "0.62", "0.58", "0.65")):
        add_fill(c, ids, CUT - 10_000 - i, price, 10_000 * (i + 1), side=i % 2)
    assert vwap_at(c, ids, side=0).p == 1 - vwap_at(c, ids, side=1).p


def test_one_trade_from_stream_and_csv_imported_twice_is_one_execution_in_the_vwap(facts):
    # DESIGN.md §9.4 item 7 at the close: the same economic trade via both sources, twice.
    c, ids = facts
    for i in range(5):
        add_fill(c, ids, CUT - 10_000 - i, "0.60", 10_000, stream_t=CUT - 10_005 - i, key=f"same{i}")
    once = vwap_at(c, ids)
    for i in range(5):
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            add_fill(c, ids, CUT - 10_000 - i, "0.60", 10_000, key=f"same{i}")
    assert vwap_at(c, ids) == once and once.vwap_executions == 5


def test_historical_definitions_carry_only_their_own_values():
    candle, vwap = close.hist_defs([60])
    assert (candle.method, candle.family, vwap.method) == ("candle_mid_top", "close_hist", "trade_vwap")
    assert candle.vwap_window_s is None and candle.candle_rule == "interval_ends_before_cutoff"
    assert vwap.candle_rule is None and (vwap.vwap_window_s, vwap.vwap_min_executions,
                                         vwap.vwap_min_notional_usd) == (600, 5, 250)
    sweep = close.hist_defs([0, 30, 60, 120, 600])
    assert len({d.params_json() for d in sweep}) == 10           # every buffer is its own definition
    c = db.connect(":memory:")
    db.migrate(c)
    assert close.ensure_def(c, candle, START) == close.ensure_def(c, candle, START)
    assert c.execute("SELECT family FROM close_def").fetchall()[0][0] == "close_hist"
