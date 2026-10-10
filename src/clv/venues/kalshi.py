"""Kalshi: parse archived REST responses (orderbook polls and market listings).

`GET /markets/{ticker}/orderbook` returns `{"orderbook_fp": {"yes_dollars":
[[price, qty], ...], "no_dollars": [...]}}`, lowest price first: resting YES
bids and resting NO bids. A NO bid at p is a YES ask at 1 - p. The response
does not name its ticker, so the ticker comes from the archived request
envelope (docs/vendor-capabilities.md). Quantities are fractional contracts
(`_fp`), each paying the market's `notional_value_dollars` (1.00 for
`KXMLBGAME`).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import parse_qs

from clv.archive import RestExchange
from clv.venues.protocol import BinaryBook, aggregate

VENUE = "kalshi"
ORDERBOOK_PATH = re.compile(r"/markets/([A-Z0-9.-]+)/orderbook$")
SIDES = ("yes", "no")


def ticker_of(x: RestExchange) -> str | None:
    m = ORDERBOOK_PATH.search(x.path)
    return m[1] if m else None


def orderbook(x: RestExchange, payout_usd_per_contract: Decimal) -> BinaryBook:
    """One orderbook poll. The ticker is taken from the request path and must
    agree with the request's declared subject."""
    ticker = ticker_of(x)
    if ticker is None or f"kalshi:market:{ticker}" not in x.subjects:
        raise ValueError(f"{x.refs[0]}: not an orderbook request for one declared ticker")
    book = x.body.json()["orderbook_fp"]
    sides = {side: aggregate([(p, Decimal(q)) for p, q in book.get(f"{side}_dollars") or []]) for side in SIDES}
    depth = int(parse_qs(x.request.get("query") or "").get("depth", ["0"])[0])
    return BinaryBook(VENUE, ticker, sides, payout_usd_per_contract, complete=depth == 0,
                      source="poll", seq=None, venue_ts_ms=None, recv_ts_ms=x.body.recv_ts_ms, refs=x.refs)


@dataclass(frozen=True)
class Market:
    ticker: str
    event_ticker: str
    title: str
    yes_sub_title: str
    status: str
    result: str
    notional_value_dollars: Decimal
    occurrence_datetime: str | None
    close_time: str
    rules_primary: str
    rules_secondary: str
    price_level_structure: str
    can_close_early: bool
    recv_ts_ms: int
    refs: tuple[str, ...]


def markets(x: RestExchange) -> list[Market]:
    """`GET /markets?tickers=...`: one row per market listed."""
    return [Market(m["ticker"], m["event_ticker"], m["title"], m.get("yes_sub_title", ""), m["status"],
                   m.get("result", ""), Decimal(m["notional_value_dollars"]), m.get("occurrence_datetime"),
                   m["close_time"], m.get("rules_primary", ""), m.get("rules_secondary", ""),
                   m.get("price_level_structure", ""), bool(m.get("can_close_early")),
                   x.body.recv_ts_ms, x.refs)
            for m in x.body.json()["markets"]]
