"""Polarity normalization (DESIGN.md §4.3, §8.4).

Reversing a complement-quoted ladder with asymmetric depth twice reproduces
every native price string and depth exactly; a complement mapping swaps bid and
ask; direct and complement mappings of the two sides of one book agree.
"""
from decimal import Decimal

import pytest

from clv.identity import canonical_ladder, complement, reverse
from clv.venues.protocol import BinaryBook, Level

L = lambda p, q: Level(p, Decimal(q))

# A Kalshi-shaped book: YES bids and NO bids, asymmetric depth and spread, a partly filled level.
BOOK = BinaryBook("kalshi", "KX-CLE", {
    "yes": (L("0.5100", "859012.73"), L("0.5000", "99555.88"), L("0.4900", "60846.94")),
    "no": (L("0.4800", "4806891.01"), L("0.4700", "974702.98")),
}, Decimal("1.00"), complete=True, source="poll", seq=None, venue_ts_ms=None, recv_ts_ms=0)


def test_complement_twice_is_exact():
    for side in BOOK.sides():
        assert complement(complement(BOOK.bids[side])) == BOOK.bids[side]
    bids, asks = canonical_ladder(BOOK, "yes", "direct")
    assert reverse(*reverse(bids, asks)) == (bids, asks)


def test_complement_mapping_swaps_bid_and_ask():
    # NO on the CLE ticker, mapped to CLE as its complement: CLE's bid is 1 - NO's ask, CLE's ask is 1 - NO's bid.
    bids, asks = canonical_ladder(BOOK, "no", "complement")
    assert bids[0] == L("0.5100", "859012.73") and asks[0] == L("0.5200", "4806891.01")
    assert [lv.qty for lv in asks] == [lv.qty for lv in BOOK.bids["no"]]     # depth follows its side


def test_direct_and_complement_views_of_one_book_agree():
    assert canonical_ladder(BOOK, "yes", "direct") == canonical_ladder(BOOK, "no", "complement")
    assert canonical_ladder(BOOK, "no", "direct") == canonical_ladder(BOOK, "yes", "complement")


def test_reverse_is_the_other_outcome():
    cle = canonical_ladder(BOOK, "yes", "direct")
    cws = canonical_ladder(BOOK, "no", "direct")
    assert reverse(*cle) == cws and reverse(*cws) == cle


@pytest.mark.parametrize("native", ["0.485", "0.4900", "0.50", "0.001", "1", "0"])
def test_native_strings_survive_the_round_trip(native):
    (lv,) = complement(complement((L(native, "7"),)))
    assert lv.price == native


def test_unknown_polarity_is_refused():
    with pytest.raises(ValueError, match="polarity"):
        canonical_ladder(BOOK, "yes", "inverse")
