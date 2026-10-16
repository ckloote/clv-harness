"""The historical candle close (DESIGN.md §5.6; close.hist_candle_rule): the book as the last
candle wholly before the as-of time left it.

    candle = the latest candle whose whole interval ends at or before as_of
    price  = mid_top of its bid and ask close, on the reference side's canonical ladder

A candle summarizes an interval, so one that contains the as-of time could include later
activity: it is never used. With 1-minute candles and a cutoff at an arbitrary second, a
market active near the start nearly always has one, so its presence does not make the close
unscoreable. The close is the earlier candle's, as of that candle's end, with its true age
recorded and `cutoff_candle_active` set: the cutoff is in effect rounded down to the candle
grid, and the flag stratifies what that cost.

Unscoreable, in order:

    no_quotes          no candle ends by as_of, and none contains it
    ambiguous_candle   none ends by as_of but one contains it, or the latest one is too old and
                       one contains as_of (DESIGN.md §9.4 item 6: a candle overlapping the
                       start leaves no usable close), or two records of the latest interval
                       (the live and historical tiers) disagree on its bid or ask close
    stale_book         the latest candle ended more than max_quote_age_s before as_of
    no_quotes          the latest candle has no bid or no ask close: the book was one-sided

A quiet minute has no candle at all, so the latest candle's age is the time since the book
last changed or traded; it is stratified, not excluded, below max_quote_age_s.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from fractions import Fraction

from clv.venues.protocol import PRICE_SCALE


@dataclass(frozen=True)
class CandleClose:
    p: Fraction | None
    reason: str | None
    candle_id: int | None = None
    cutoff_candle_active: bool | None = None
    quote_age_ms: int | None = None
    spread_e4: int | None = None


def candle_close(conn: sqlite3.Connection, instrument_id: int, side: int, as_of_ms: int, max_quote_age_s: int,
                 prefix: str = "f_") -> CandleClose:
    """The candle close of `side` of the instrument as of `as_of_ms`, or why there is none."""
    active = conn.execute(f"SELECT 1 FROM {prefix}historical_candle WHERE venue_instrument_id = ?"
                          f" AND interval_start_ms < ? AND interval_end_ms > ? LIMIT 1",
                          (instrument_id, as_of_ms, as_of_ms)).fetchone() is not None
    end = conn.execute(f"SELECT max(interval_end_ms) FROM {prefix}historical_candle WHERE venue_instrument_id = ?"
                       f" AND interval_end_ms <= ?", (instrument_id, as_of_ms)).fetchone()[0]
    if end is None:
        return CandleClose(None, "ambiguous_candle" if active else "no_quotes")
    rows = conn.execute(f"SELECT candle_id, side, bid_close_e4, ask_close_e4 FROM {prefix}historical_candle"
                        f" WHERE venue_instrument_id = ? AND interval_end_ms = ? ORDER BY candle_id",
                        (instrument_id, end)).fetchall()
    seen = dict(candle_id=rows[0][0], cutoff_candle_active=active, quote_age_ms=as_of_ms - end)
    if len({(s, bid, ask) for _, s, bid, ask in rows}) > 1:
        return CandleClose(None, "ambiguous_candle", **seen)
    _, candle_side, bid, ask = rows[0]
    if seen["quote_age_ms"] > max_quote_age_s * 1000:
        return CandleClose(None, "ambiguous_candle" if active else "stale_book", **seen)
    if bid is None or ask is None:
        return CandleClose(None, "no_quotes", **seen)
    if side != candle_side:                     # the complement: bid and ask swap (DESIGN.md §4.3)
        bid, ask = PRICE_SCALE - ask, PRICE_SCALE - bid
    return CandleClose(Fraction(bid + ask, 2 * PRICE_SCALE), None, spread_e4=ask - bid, **seen)
