"""Kalshi: parse archived REST responses (orderbook polls, market listings, and the
historical candles and trades the B1 backfill archives).

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
from clv.timeutil import iso_ms
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
    settlement_ts: str | None = None
    custom_strike: str | None = None    # the team the market names, as Kalshi identifies it
    item: int = 0                       # the market's position in its response
    settlement_value: str | None = None  # a scalar (fair-price) settlement's YES value, in dollars


def _market(m: dict, x: RestExchange, item: int) -> Market:
    strike = (m.get("custom_strike") or {}).get("baseball_team")
    return Market(m["ticker"], m["event_ticker"], m["title"], m.get("yes_sub_title", ""), m["status"],
                  m.get("result", ""), Decimal(m["notional_value_dollars"]), m.get("occurrence_datetime"),
                  m["close_time"], m.get("rules_primary", ""), m.get("rules_secondary", ""),
                  m.get("price_level_structure", ""), bool(m.get("can_close_early")),
                  x.body.recv_ts_ms, x.refs, m.get("settlement_ts"), strike, item,
                  m.get("settlement_value_dollars") or None)


def markets(x: RestExchange) -> list[Market]:
    """`GET /markets?tickers=...`: one row per market listed."""
    return [_market(m, x, i) for i, m in enumerate(x.body.json()["markets"])]


# -- the B1 backfill (raw_recorder.backfill) -----------------------------------------------------

HISTORICAL_MARKETS = re.compile(r"/historical/markets$")
EVENTS = re.compile(r"/events$")
CANDLES = re.compile(r"/markets/([A-Z0-9.-]+)/candlesticks$")
TRADES = re.compile(r"/(historical/trades|markets/trades)$")


def listed_markets(x: RestExchange) -> list[Market]:
    """Every market a backfill listing names: the historical tier's `/historical/markets`, or
    the markets nested in `/events?with_nested_markets=true`. `item` numbers them in the
    order the response lists them, nested ones included."""
    if not x.ok:
        return []
    if HISTORICAL_MARKETS.search(x.path):
        return [_market(m, x, i) for i, m in enumerate(x.body.json().get("markets") or [])]
    if EVENTS.search(x.path):
        nested = [m for ev in x.body.json().get("events") or [] for m in ev.get("markets") or []]
        return [_market(m, x, i) for i, m in enumerate(nested)]
    return []


def e4(text: str | None, ref: str) -> int | None:
    """A dollar price on Kalshi's grid as an integer probability x 10,000, exactly."""
    if text is None:
        return None
    v = Decimal(text) * 10_000
    if v != v.to_integral_value():
        raise ValueError(f"{ref}: price {text} is finer than 1/10,000")
    return int(v)


def _ohlc(d: dict | None) -> dict[str, str | None]:
    """open/high/low/close, whichever tier's names (historical `close`, live `close_dollars`)."""
    d = d or {}
    return {k: d.get(k) if d.get(k) is not None else d.get(f"{k}_dollars") for k in ("open", "high", "low", "close")}


@dataclass(frozen=True)
class Candle:
    ticker: str
    tier: str                   # "historical" | "live"
    end_ms: int
    resolution_s: int
    yes_bid: dict[str, str | None]
    yes_ask: dict[str, str | None]
    price: dict[str, str | None]
    volume: str | None
    open_interest: str | None
    native: dict
    item: int
    recv_ts_ms: int
    ref: str


def candles(x: RestExchange) -> list[Candle]:
    """`/historical/markets/{t}/candlesticks` or `/series/{s}/markets/{t}/candlesticks`."""
    m = CANDLES.search(x.path)
    if not x.ok or m is None:
        return []
    ticker = m[1]
    body = x.body.json()
    if body.get("ticker", ticker) != ticker:
        raise ValueError(f"{x.body.ref}: candles for {body.get('ticker')}, requested {ticker}")
    resolution = int(parse_qs(x.request.get("query") or "").get("period_interval", ["1"])[0]) * 60
    tier = "historical" if "/historical/" in x.path else "live"
    out = []
    for i, c in enumerate(body.get("candlesticks") or []):
        out.append(Candle(ticker, tier, c["end_period_ts"] * 1000, resolution, _ohlc(c.get("yes_bid")),
                          _ohlc(c.get("yes_ask")), _ohlc(c.get("price")),
                          c.get("volume", c.get("volume_fp")), c.get("open_interest", c.get("open_interest_fp")),
                          c, i, x.body.recv_ts_ms, x.body.ref))
    return out


@dataclass(frozen=True)
class TapeTrade:
    trade_id: str
    ticker: str
    tier: str
    yes_price: str
    count: str                  # contracts, as received
    created_ms: int
    taker_side: str | None
    native: dict
    item: int
    recv_ts_ms: int
    ref: str


def tape(x: RestExchange) -> list[TapeTrade]:
    """`/historical/trades` or `/markets/trades`: one page of executions, each with its trade_id."""
    m = TRADES.search(x.path)
    if not x.ok or m is None:
        return []
    tier = "historical" if m[1].startswith("historical") else "live"
    return [TapeTrade(t["trade_id"], t["ticker"], tier, t["yes_price_dollars"], t["count_fp"],
                      iso_ms(t["created_time"]), t.get("taker_side"), t, i, x.body.recv_ts_ms, x.body.ref)
            for i, t in enumerate(x.body.json().get("trades") or [])]
