"""Normalized shapes shared by the venue parsers.

A binary exchange market is two sides, each with resting bids. Novig quotes
both outcomes of a market; Kalshi quotes YES and NO of each ticker. Buying a
side at p is the same trade as selling the other side at 1 - p, so one side's
asks are the other side's bids, complemented (DESIGN.md §2.4, §4.3). Which
side is which canonical outcome is a mapping decision (identity), not a parser
decision, so books here are keyed by the venue's own side IDs.

Prices are kept as native strings and as integers scaled by 10,000 (§8.3); a
native price that is not a whole number of 1/10,000 is rejected rather than
rounded. Quantities are native contracts; payout USD is quantity times the
venue's USD payout per contract, kept per book.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

PRICE_SCALE = 10_000


def price_e4(native: str) -> int:
    """Native probability string -> integer scaled by 10,000. Exact or an error."""
    scaled = Decimal(native) * PRICE_SCALE
    if scaled != scaled.to_integral_value() or not 0 <= scaled <= PRICE_SCALE:
        raise ValueError(f"price {native!r} is not a probability on a 1/10,000 grid")
    return int(scaled)


@dataclass(frozen=True)
class Level:
    """One price level: total resting quantity at one price."""
    price: str                  # native string, as received (the first spelling seen at this price)
    qty: Decimal                # native contracts

    @property
    def price_e4(self) -> int:
        return price_e4(self.price)


@dataclass(frozen=True)
class BinaryBook:
    """Both sides' resting bids at one moment, best first.

    `complete` is False when the source may have truncated the ladder (a
    depth-limited poll); a depth walk must then fail closed beyond what is shown.
    """
    venue: str                              # "novig" | "kalshi"
    market: str                             # Novig marketId | Kalshi ticker
    bids: dict[str, tuple[Level, ...]]      # side ID -> bids, highest price first
    payout_usd_per_contract: Decimal
    complete: bool
    source: str                             # "stream" | "poll"
    seq: int | None                         # venue sequence, where the venue has one
    venue_ts_ms: int | None                 # venue's own time for this state
    recv_ts_ms: int                         # when the frame establishing this state arrived
    refs: tuple[str, ...] = field(default=())   # archive frames this state rests on (see clv.archive.Frame.ref)

    def sides(self) -> tuple[str, ...]:
        return tuple(self.bids)

    def other(self, side: str) -> str:
        (o,) = [s for s in self.bids if s != side]
        return o

    def asks(self, side: str) -> tuple[Level, ...]:
        """Asks for `side`: the other side's bids at 1 - p, lowest first."""
        return tuple(Level(str(1 - Decimal(lv.price)), lv.qty) for lv in self.bids[self.other(side)])

    def best_bid(self, side: str) -> Level | None:
        return self.bids[side][0] if self.bids[side] else None

    def best_ask(self, side: str) -> Level | None:
        asks = self.asks(side)
        return asks[0] if asks else None


def aggregate(orders: list[tuple[str, Decimal]]) -> tuple[Level, ...]:
    """(price, qty) pairs -> levels summed by price, highest price first.
    Prices are grouped by numeric value, so "0.50" and "0.5" are one level."""
    by_price: dict[Decimal, tuple[str, Decimal]] = {}
    for price, qty in orders:
        p = Decimal(price)
        native, total = by_price.get(p, (price, Decimal(0)))
        by_price[p] = (native, total + qty)
    return tuple(Level(native, total) for _, (native, total) in sorted(by_price.items(), reverse=True)
                 if total > 0)
