"""Trade VWAP over unique executions (DESIGN.md §5.6, §7.4), in exact arithmetic.

An execution is counted once however many records describe it: the VWAP reads
trade_execution, never its sightings. Its time is the earliest time its records
give, preferring the venue's own stream time: the Novig daily file stamps a trade
14 ms to 68 s after the stream does (docs/vendor-capabilities.md), so a file time
alone can only move a trade later, never into a pre-cutoff window it was not in.

    vwap(side) = sum(price_side_j * payout_j) / sum(payout_j)

where price_side is the execution's price if it was quoted for `side`, else
1 - price. B2 builds the historical close family on this (`close_hist_vwap_*`).
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from fractions import Fraction


@dataclass(frozen=True)
class TradeVwap:
    p: Fraction | None
    executions: int
    payout_cents: int


def execution_times(conn: sqlite3.Connection, instrument_id: int, prefix: str = "") -> dict[int, int]:
    """Each execution's time: its stream sighting's venue time if any, else its earliest sighting's."""
    rows = conn.execute(f"""
        SELECT execution_id, coalesce(min(CASE WHEN role = 'stream' THEN venue_ts_ms END), min(venue_ts_ms))
        FROM {prefix}trade_source_observation
        WHERE venue_instrument_id = ? AND execution_id IS NOT NULL GROUP BY execution_id""", (instrument_id,))
    return dict(rows.fetchall())


def trade_vwap(conn: sqlite3.Connection, instrument_id: int, side: int, lo_ms: int, hi_ms: int,
               prefix: str = "") -> TradeVwap:
    """VWAP for `side` over the unique executions timed in [lo_ms, hi_ms)."""
    times = execution_times(conn, instrument_id, prefix)
    num, payout, n = Fraction(0), 0, 0
    for eid, s, pnum, pden, cents in conn.execute(
            f"SELECT execution_id, side, price_num, price_den, payout_cents FROM {prefix}trade_execution"
            f" WHERE venue_instrument_id = ? ORDER BY execution_id", (instrument_id,)):
        t = times.get(eid)
        if t is None or not lo_ms <= t < hi_ms:
            continue
        price = Fraction(pnum, pden)
        num += (price if s == side else 1 - price) * cents
        payout += cents
        n += 1
    return TradeVwap(num / payout if payout else None, n, payout)
