"""One Novig execution, however many records describe it (DESIGN.md §7.4).

Novig reports a trade three ways, and none of them carries an ID the others share:
- its daily trades.csv has one MAKER row per fill, the execution itself;
- one TAKER row per taker order, which may have filled against several makers;
- the stream's `trades` channel sends each fill with its own tradeId.

B0 matched all 1,876 trades streamed for 849832 to exactly one MAKER row
(docs/feasibility.md §3). The rule, from tools/b0/reconcile.py:
- **Fields:** same instrument, same outcome, same payout quantity (stream qty is
  contracts of $0.01: qty / 100 = the file's USD).
- **Price:** the stream's 3-place price within 0.0006 of the row's cost / qty.
- **Time:** the nearest such record within MATCH_WINDOW_MS. The file stamps a trade
  14 ms to 68 s after the stream.
- **One-to-one:** a record already matched to another source is not matched again.

A streamed fill is imported only after its day's file (clv.backfill.novig_public), so a fill
the two share is always the file's execution, at its exact cost / qty. A streamed fill with no
row is its own execution, marked `unmatched`.
"""
from __future__ import annotations

from decimal import Decimal
from fractions import Fraction

MATCH_WINDOW_MS = 120_000
PRICE_TOLERANCE = Fraction(6, 10_000)


def _nearest(w, iid: int, side: int, payout_cents: int, price: Fraction, ts_ms: int, has_role: str,
             lacks_role: str) -> int | None:
    """The execution with a `has_role` sighting matching these fields nearest in time, among
    those with no `lacks_role` sighting yet."""
    best = None
    for eid, venue_ts, num, den in w.conn.execute(f"""
            SELECT x.execution_id, s.venue_ts_ms, x.price_num, x.price_den
            FROM trade_source_observation s JOIN trade_execution x USING (execution_id)
            WHERE s.venue_instrument_id = ? AND s.role = ? AND x.side = ? AND x.payout_cents = ?
              AND s.venue_ts_ms BETWEEN ? AND ?
              AND NOT EXISTS (SELECT 1 FROM trade_source_observation o
                              WHERE o.execution_id = x.execution_id AND o.role = '{lacks_role}')""",
            (iid, has_role, side, payout_cents, ts_ms - MATCH_WINDOW_MS, ts_ms + MATCH_WINDOW_MS)):
        if abs(Fraction(num, den) - price) >= PRICE_TOLERANCE:
            continue
        if best is None or abs(venue_ts - ts_ms) < best[0]:
            best = (abs(venue_ts - ts_ms), eid)
    return best[1] if best else None


def maker_match(w, iid, side, payout_cents, price, stream_ts_ms) -> int | None:
    """A streamed fill arriving after the file's MAKER row."""
    return _nearest(w, iid, side, payout_cents, price, stream_ts_ms, "maker", "stream")


def usd_cents(text: str, ref: str) -> int:
    cents = Decimal(text) * 100
    if cents != cents.to_integral_value():
        raise ValueError(f"{ref}: {text} USD is not a whole number of cents")
    return int(cents)
