"""Kalshi public market data (no authentication).

Endpoint knowledge from edge-scanner/scanner/connectors/kalshi.py and
docs.kalshi.com. The single-market order-book body omits its ticker, so each
request names its subject in the REST envelope (DESIGN.md §7.1).
"""
from __future__ import annotations

import json

from .rest import RestRecorder


def subject(ticker: str) -> str:
    return f"kalshi:market:{ticker}"


async def poll_orderbooks(rest: RestRecorder, base: str, tickers: tuple[str, ...]) -> None:
    for ticker in tickers:
        await rest.request("GET", base, f"/markets/{ticker}/orderbook",
                           subjects=[subject(ticker)], purpose="orderbook")


async def fetch_markets(rest: RestRecorder, base: str, tickers: tuple[str, ...]) -> None:
    """Catalog evidence for the tickers being polled (rules, status, times)."""
    if tickers:
        await rest.request("GET", base, "/markets", params={"tickers": ",".join(tickers)},
                           subjects=[subject(t) for t in tickers], purpose="catalog")


async def fetch_series(rest: RestRecorder, base: str, series_ticker: str) -> list[dict]:
    """All open markets of a series, paginated; archived and returned for `catalog`."""
    markets, cursor = [], None
    while True:
        params = {"series_ticker": series_ticker, "status": "open", "limit": "200"}
        if cursor:
            params["cursor"] = cursor
        res = await rest.request("GET", base, "/markets", params=params,
                                 subjects=[f"kalshi:series:{series_ticker}"], purpose="catalog")
        if not res.ok or res.body is None:
            return markets
        page = json.loads(res.body)
        markets.extend(page.get("markets", []))
        cursor = page.get("cursor")
        if not cursor:
            return markets
