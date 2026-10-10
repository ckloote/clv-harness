"""Book benchmarks for a canonical outcome (DESIGN.md §2.4), in exact arithmetic.

A ladder here is canonical: bids highest first and asks lowest first, for
`P(outcome)`, after polarity normalization (clv.identity). Each level's
quantity is its payout in USD cents if the contract wins, so venues with
different contract sizes walk alike.

    side_vwap(N)   = sum(price_j * consumed_payout_j) / N, consuming exactly N
                     of payout, with a proportional fraction of the last level
    mid_depth_N    = (bid_vwap_N + ask_vwap_N) / 2
    mid_top        = (best bid + best ask) / 2

A side short of N is never renormalized: the benchmark is unavailable, with
`insufficient_depth`, or `truncated_ladder` when the stored ladder may have
cut off depth beyond what it shows (fail closed, DESIGN.md §7.2). An empty
side is `no_quotes`.
"""
from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

from clv.venues.protocol import Level


@dataclass(frozen=True)
class Benchmark:
    p: Fraction | None
    reason: str | None
    bid: Fraction | None = None         # the bid-side figure: best bid or bid VWAP
    ask: Fraction | None = None


def price(lv: Level) -> Fraction:
    return Fraction(lv.price)


def side_vwap(levels: tuple[Level, ...], notional_cents: int) -> Fraction | None:
    """VWAP over exactly `notional_cents` of payout, or None if the ladder is short."""
    left, total = Fraction(notional_cents), Fraction(0)
    for lv in levels:
        take = min(left, Fraction(lv.qty))
        total += price(lv) * take
        left -= take
        if left == 0:
            return total / notional_cents
    return None


def mid_top(bids: tuple[Level, ...], asks: tuple[Level, ...]) -> Benchmark:
    if not bids or not asks:
        return Benchmark(None, "no_quotes")
    b, a = price(bids[0]), price(asks[0])
    return Benchmark((b + a) / 2, None, b, a)


def mid_depth(bids: tuple[Level, ...], asks: tuple[Level, ...], notional_usd: int, truncated: bool) -> Benchmark:
    if not bids or not asks:
        return Benchmark(None, "no_quotes")
    n = notional_usd * 100
    b, a = side_vwap(bids, n), side_vwap(asks, n)
    if b is None or a is None:
        return Benchmark(None, "truncated_ladder" if truncated else "insufficient_depth", b, a)
    return Benchmark((b + a) / 2, None, b, a)
