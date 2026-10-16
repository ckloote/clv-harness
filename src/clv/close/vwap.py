"""Trade VWAP over unique executions (DESIGN.md §5.6, §7.4), in exact arithmetic.

An execution is counted once however many records describe it: the VWAP reads
trade_execution, never its sightings, so a Novig TAKER row (a sighting of a
dual-side group, never an execution) cannot enter it. Its time is the earliest
time its records give, preferring the venue's own stream time: the Novig daily
file stamps a trade 14 ms to 68 s after the stream does
(docs/vendor-capabilities.md), so a file time alone can only move a trade later,
never into a pre-cutoff window it was not in.

    vwap(side) = sum(price_side_j * payout_j) / sum(payout_j)

where price_side is the execution's price if it was quoted for `side`, else
1 - price. The historical close family `close_hist_vwap` is built on this
(clv.close.definitions).
"""
from __future__ import annotations

import bisect
import sqlite3
from dataclasses import dataclass
from fractions import Fraction


@dataclass(frozen=True)
class TradeVwap:
    p: Fraction | None
    executions: int
    payout_cents: int
    last_ms: int | None = None              # the latest execution time in the window


def execution_times(conn: sqlite3.Connection, instrument_id: int, prefix: str = "") -> dict[int, int]:
    """Each execution's time: its stream sighting's venue time if any, else its earliest sighting's."""
    rows = conn.execute(f"""
        SELECT execution_id, coalesce(min(CASE WHEN role = 'stream' THEN venue_ts_ms END), min(venue_ts_ms))
        FROM {prefix}trade_source_observation
        WHERE venue_instrument_id = ? AND execution_id IS NOT NULL GROUP BY execution_id""", (instrument_id,))
    return dict(rows.fetchall())


class Executions:
    """Each instrument's executions in time order, read once: a run prices many windows of the
    same instrument (every buffer and offset). Read through one fact snapshot (`prefix`)."""

    def __init__(self, conn: sqlite3.Connection, prefix: str = ""):
        self.conn, self.prefix = conn, prefix
        self._by_instrument: dict[int, tuple[list[int], list[tuple]]] = {}

    def of(self, instrument_id: int) -> tuple[list[int], list[tuple]]:
        """(times, rows) sorted by (time, execution id); a row is (time, id, side, price, payout cents)."""
        if instrument_id not in self._by_instrument:
            times = execution_times(self.conn, instrument_id, self.prefix)
            rows = sorted((times[eid], eid, s, Fraction(pnum, pden), cents) for eid, s, pnum, pden, cents in
                          self.conn.execute(f"SELECT execution_id, side, price_num, price_den, payout_cents"
                                            f" FROM {self.prefix}trade_execution WHERE venue_instrument_id = ?",
                                            (instrument_id,)) if eid in times)
            self._by_instrument[instrument_id] = ([r[0] for r in rows], rows)
        return self._by_instrument[instrument_id]

    def window(self, instrument_id: int, lo_ms: int, hi_ms: int) -> list[tuple]:
        """The executions timed in [lo_ms, hi_ms)."""
        times, rows = self.of(instrument_id)
        return rows[bisect.bisect_left(times, lo_ms):bisect.bisect_left(times, hi_ms)]

    def vwap(self, instrument_id: int, side: int, lo_ms: int, hi_ms: int) -> TradeVwap:
        num, payout, last = Fraction(0), 0, None
        rows = self.window(instrument_id, lo_ms, hi_ms)
        for t, _, s, price, cents in rows:
            num += (price if s == side else 1 - price) * cents
            payout += cents
            last = t
        return TradeVwap(num / payout if payout else None, len(rows), payout, last)


def trade_vwap(conn: sqlite3.Connection, instrument_id: int, side: int, lo_ms: int, hi_ms: int,
               prefix: str = "") -> TradeVwap:
    """VWAP for `side` over the unique executions timed in [lo_ms, hi_ms)."""
    return Executions(conn, prefix).vwap(instrument_id, side, lo_ms, hi_ms)
