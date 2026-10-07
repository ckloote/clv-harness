"""Novig's unsigned public REST surface: catalog and order book.

`/v3/public/...` routes need no key and are throttled per IP at the edge
(docs.novig.com, read 2026-10-06). The catalog is how markets are chosen. The
public book poll is a fallback for when no read key is provisioned: it gives
periodic snapshots with `seq`, not the stream's every-change evidence.
"""
from __future__ import annotations

import json

from ..rest import RestRecorder


def subject(market_id: str) -> str:
    return f"novig:market:{market_id}"


async def poll_books(rest: RestRecorder, host: str, market_ids: frozenset[str], depth: int) -> None:
    for market_id in sorted(market_ids):
        await rest.request("GET", host, f"/v3/public/catalog/markets/{market_id}/book",
                           params={"depth": str(depth)}, subjects=[subject(market_id)],
                           purpose="public_book")


async def fetch_markets(rest: RestRecorder, host: str, market_ids: frozenset[str]) -> None:
    """Catalog evidence (outcomes, fee terms, voids, status) for recorded markets."""
    for market_id in sorted(market_ids):
        await rest.request("GET", host, f"/v3/public/catalog/markets/{market_id}",
                           subjects=[subject(market_id)], purpose="catalog")


async def _paged(rest: RestRecorder, host: str, path: str, params: dict, subject_: str) -> list[dict]:
    items, after = [], None
    while True:
        p = dict(params, **({"after": after} if after else {}))
        res = await rest.request("GET", host, path, params=p, subjects=[subject_], purpose="catalog")
        if not res.ok or res.body is None:
            return items
        page = json.loads(res.body)
        items.extend(page.get("items", []))
        after = page.get("next")
        if not after:
            return items


async def fetch_catalog(rest: RestRecorder, host: str, league: str = "MLB") -> tuple[list[dict], list[dict]]:
    """Open events and moneyline (`MONEY`) markets for a league, archived."""
    events = await _paged(rest, host, "/v3/public/catalog/events",
                          {"league": league, "limit": "100"}, f"novig:league:{league}")
    markets = await _paged(rest, host, "/v3/public/catalog/markets",
                           {"league": league, "marketType": "MONEY", "limit": "100"},
                           f"novig:league:{league}")
    return events, markets
