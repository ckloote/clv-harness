"""Novig's historical records, as the B1 backfill archives them (raw_recorder.backfill).

- `/v3/history/markets/{id}`: a market, closed ones included, in the catalog's shape but
  without `startsTs`, with each outcome's grade once settled: `WIN`, `LOSS`, or a fair-value price
  (`FMV`); `TBD` before.
- `/v3/history/events/{id}`: its event: `description` ("Away @ Home", in Novig's team
  names), `startsTs` and `status`.
- `<date>/trades.csv`: every executed trade of one Eastern day, one row per side
  (docs/vendor-capabilities.md, Public exchange data). The columns are read by name: Novig
  may add columns. `qty` is the payout in USD; `cost` what that side paid.
"""
from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterator
from dataclasses import dataclass

from clv.archive import RestExchange
from clv.timeutil import iso_ms
from clv.venues.novig.parser import CatalogMarket, _catalog_market

HISTORY_MARKET = re.compile(r"/v3/history/markets/([^/]+)$")
HISTORY_EVENT = re.compile(r"/v3/history/events/([^/]+)$")
DAILY_TRADES = re.compile(r"/(\d{4}-\d{2}-\d{2})/trades\.csv$")
TRADE_COLUMNS = ("timestamp", "outcomeId", "marketId", "league", "marketType", "tradeType", "cost", "qty", "side")


def history_market(x: RestExchange) -> CatalogMarket | None:
    if not x.ok or not HISTORY_MARKET.search(x.path):
        return None
    return _catalog_market(x.body.json(), x)


@dataclass(frozen=True)
class HistoryEvent:
    event: str
    description: str
    league: str
    status: str
    starts_ts_ms: int | None
    recv_ts_ms: int
    refs: tuple[str, ...]

    @property
    def teams(self) -> tuple[str, str] | None:
        """(away, home) in Novig's names, from "Away @ Home"."""
        away, sep, home = self.description.partition(" @ ")
        return (away, home) if sep else None


def history_event(x: RestExchange) -> HistoryEvent | None:
    if not x.ok or not HISTORY_EVENT.search(x.path):
        return None
    e = x.body.json()
    return HistoryEvent(e["eventId"], e.get("description", ""), e.get("league", ""), e.get("status", ""),
                        e.get("startsTs"), x.body.recv_ts_ms, x.refs)


@dataclass(frozen=True)
class DailyRow:
    item: int                   # 0-based data row: the citation within the file's frame
    timestamp: str              # as written; some have no milliseconds
    ts_ms: int
    outcome: str
    market: str
    league: str
    market_type: str
    trade_type: str             # STRAIGHT | COMBO
    cost: str                   # USD this side paid
    qty: str                    # payout in USD if this side wins
    side: str                   # MAKER | TAKER
    native: dict


def daily_trades(x: RestExchange) -> Iterator[DailyRow]:
    """The rows of one day's trades.csv, in file order."""
    if not x.ok or not DAILY_TRADES.search(x.path):
        return
    reader = csv.DictReader(io.StringIO(x.body.frame))
    missing = [c for c in TRADE_COLUMNS if c not in (reader.fieldnames or ())]
    if missing:
        raise ValueError(f"{x.body.ref}: trades.csv has no {', '.join(missing)} column")
    for i, r in enumerate(reader):
        yield DailyRow(i, r["timestamp"], iso_ms(r["timestamp"]), r["outcomeId"], r["marketId"], r["league"],
                       r["marketType"], r["tradeType"], r["cost"], r["qty"], r["side"], r)


def day_of(x: RestExchange) -> str | None:
    m = DAILY_TRADES.search(x.path)
    return m[1] if m else None
