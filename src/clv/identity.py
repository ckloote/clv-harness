"""Identity: venue team naming, and polarity normalization (DESIGN.md §4.3).

A mapping says that one native side of an instrument is a canonical outcome
(`direct`) or its exact complement (`complement`). For a complement side the
canonical ladder is the native one reflected through 1 - p: bids and asks swap,
and each level keeps its own depth. Applying it twice returns every native
price string and quantity exactly.
"""
from __future__ import annotations

from decimal import Decimal

from clv.venues.protocol import BinaryBook, Level

POLARITIES = ("direct", "complement")

# Novig names a MONEY outcome by its own team abbreviation; these differ from
# StatsAPI's (docs/vendor-capabilities.md, Public exchange data). StatsAPI -> Novig.
NOVIG_ABBR = {"KC": "KAN", "AZ": "ARI", "WSH": "WAS", "ATH": "OAK"}


def novig_abbr(statsapi_abbr: str) -> str:
    return NOVIG_ABBR.get(statsapi_abbr, statsapi_abbr)


def complement(levels: tuple[Level, ...]) -> tuple[Level, ...]:
    """Reflect a ladder through 1 - p, keeping each level's depth and order."""
    return tuple(Level(str(1 - Decimal(lv.price)), lv.qty) for lv in levels)


def canonical_ladder(book: BinaryBook, side: str, polarity: str) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """(bids, asks) for the canonical outcome that native `side` maps to.

    direct:     bids = side's bids;              asks = other side's bids at 1 - p
    complement: bids = other side's bids as-is;  asks = side's bids at 1 - p
    (the outcome's bid is 1 - the side's ask, and its ask is 1 - the side's bid)
    """
    if polarity == "direct":
        return book.bids[side], book.asks(side)
    if polarity == "complement":
        return book.bids[book.other(side)], complement(book.bids[side])
    raise ValueError(f"polarity {polarity!r} not in {POLARITIES}")


def reverse(bids: tuple[Level, ...], asks: tuple[Level, ...]) -> tuple[tuple[Level, ...], tuple[Level, ...]]:
    """The complement outcome's (bids, asks) from one outcome's (bids, asks)."""
    return complement(asks), complement(bids)
