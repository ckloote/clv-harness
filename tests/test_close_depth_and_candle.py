"""Depth-walk benchmarks (DESIGN.md §2.4). Historical candles arrive with B1.

The V0 worked fixture: canonical bids (0.40, $300), (0.35, $400) and asks
(0.60, $200), (0.65, $500), quantities in payout dollars. At N = $500 the bid
VWAP is 0.38, the ask VWAP 0.63 and the midpoint 0.505; the complement's is
0.495. At N = $1,000 both ladders are short. $500 of payout is 50,000 Novig
contracts at $0.01, and the cash proceeds and cost are $190 and $315.
"""
from decimal import Decimal
from fractions import Fraction

import pytest

from clv.close import depth
from clv.identity import reverse
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
