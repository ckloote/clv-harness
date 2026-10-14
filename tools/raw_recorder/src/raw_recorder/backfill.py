"""B1 backfill: archive the historical reference inputs (DESIGN.md §13 Track B, §7.1).

Every request goes through the REST envelope into a separate archive root
(`[backfill] root`, default `<repo>/archive-hist`), so the imported bytes are
sealed, hashed segments like everything the recorder captures, and `clv`
cites them the same way. It has its own archive lock, so it runs beside the
live recorder service.

    mlb_hist     StatsAPI teams and schedule (the configured seasons, month by month)
                 and playByPlay for every Final game: the first pitch (§5.2)
    kalshi_hist  /historical/cutoff, the KXMLBGAME markets (the live events'
                 nested markets and the historical tier's listing), and per settled
                 market its 1-minute candles and its trades over
                 [close_time - lookback_s, close_time], each on its own tier
    novig_hist   the daily trades.csv and markets.csv from novig_first_day, and
                 /v3/history/markets|events for every MLB MONEY market in them,
                 signed with the trading::read key (never a management key)

**Resumable.** A run first reads what the archive already holds: a request
that completed with a 2xx is not sent again, and anything that failed (a
timeout, a 5xx, DNS at wake) is retried. Rerunning after an interruption
finishes the job. Listings that can still change (the cutoff, the Kalshi
listings, Novig's index) are fetched every run, and a schedule month until a
copy fetched after the month's games ended is archived, and a Novig market's
and event's history until the market is settled and the event final (a day's
markets.csv lists markets still open). Two runs that fetched the same request
resolve to the later response.
The cutoff moves forward over time. A market fetched on the live tier before
it moved is not fetched again on the historical one, unless the cutoff now
falls inside its trades window: those trades are fetched again, split at the
new cutoff, and the import deduplicates them by trade_id.

**Why close_time.** Kalshi ticker dates and times do not identify the game
(docs/feasibility.md §4), but a market closes when its game ends, so the window
before close_time covers the game the market settled on, whatever its ticker
says. lookback_s covers the 24 h offset (controls.martingale_offsets), the
game itself and a margin.
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import time
from collections import Counter
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from . import redact
from .archive import SEALED_SUFFIX, iter_file
from .rest import RestRecorder, Signer, build_query

SOURCES = {"mlb": "mlb_hist", "kalshi": "kalshi_hist", "novig": "novig_hist"}

# Bodies an import or a later step reads back are kept from the archive index; for the large
# ones (candles, CSVs, play-by-play) only what a later step needs is kept.
_KEEP = {"schedule", "teams", "cutoff", "events", "markets", "index", "history_market", "history_event"}
_SETTLED = {"settled", "finalized"}
_RETRY_STATUSES = {429}


def _iso_s(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())


def mlb_money_markets(purpose: str, body: str) -> frozenset[str]:
    """The MLB moneyline market IDs in a Novig daily file. The moneyline's reportTicker was
    MLB-MONEY in August 2026 and MLB-WINNER from September (tools/b0/fetch.py)."""
    rows = csv.DictReader(io.StringIO(body))
    if purpose == "trades_csv":
        return frozenset(r["marketId"] for r in rows if r.get("league") == "MLB" and r.get("marketType") == "MONEY")
    return frozenset(r["marketId"] for r in rows if r.get("reportTicker") in ("MLB-MONEY", "MLB-WINNER"))


def summarize(purpose: str, body: str):
    if purpose in _KEEP:
        return body
    if purpose == "trades":
        return {"cursor": json.loads(body).get("cursor") or ""}
    if purpose in ("trades_csv", "markets_csv"):
        return mlb_money_markets(purpose, body)
    return True


def clock() -> int:
    """Milliseconds now: the backfill archive's frame stamps and the freshness of what it
    holds are read on this one clock."""
    return int(time.time() * 1000)


def _key(source: str, url: str) -> tuple[str, str, str]:
    """A request's identity as its rest_request event records it (origin, path, redacted query)."""
    parts = redact.url_parts(url)
    return source, parts["origin"] + parts["path"], parts["query"]


def load_done(root: Path) -> dict[tuple[str, str, str], tuple[object, int]]:
    """What earlier runs fetched: every request in the sealed archive that completed with a
    2xx, keyed by source, URL and query, with its summary and when it completed. A request
    fetched more than once keeps its latest response: segment names carry random session
    IDs, so their order says nothing about time. Reads one segment at a time."""
    done: dict[tuple[str, str, str], tuple[object, int]] = {}
    for source in SOURCES.values():
        pending: dict[str, dict] = {}
        for path in sorted(Path(root).glob(f"{source}/*/*{SEALED_SUFFIX}")):
            for f in iter_file(path):
                if f["dir"] == "event":
                    meta = json.loads(f["frame"])
                    if meta.get("type") == "rest_request":
                        pending[f["conn_id"]] = {"start": meta}
                    elif meta.get("type") == "rest_complete" and f["conn_id"] in pending:
                        x = pending.pop(f["conn_id"])
                        status = meta.get("status")
                        if status is not None and 200 <= status < 300 and "body" in x:
                            s, at = x["start"], f["recv_ts_ms"]
                            key = (source, s["origin"] + s["path"], s["query"])
                            if key not in done or done[key][1] <= at:
                                done[key] = (summarize(s.get("purpose", ""), x["body"]), at)
                elif f["dir"] == "in" and f["conn_id"] in pending:
                    pending[f["conn_id"]]["body"] = f["frame"]
    return done


@dataclass
class Stats:
    fetched: Counter = field(default_factory=Counter)
    skipped: Counter = field(default_factory=Counter)
    failed: list = field(default_factory=list)          # (source, path, query, status or failure)


class Fetcher:
    """GETs through the REST envelope, skipping what the archive already holds."""

    def __init__(self, rests: dict[str, RestRecorder], done: dict, min_interval_s: dict[str, float],
                 retries: int = 3, retry_wait_s: float = 2.0):
        self.rests, self.done, self.min_interval = rests, done, min_interval_s
        self.retries, self.retry_wait_s = retries, retry_wait_s
        self.last: dict[str, float] = {}
        self.this_run: set[tuple[str, str, str]] = set()        # fetched by this run: current as is
        self.locks: dict[str, asyncio.Lock] = {}
        self.stats = Stats()

    async def _pace(self, host: str) -> None:
        lock = self.locks.setdefault(host, asyncio.Lock())
        async with lock:
            wait = self.last.get(host, 0) + self.min_interval.get(host, 0) - time.monotonic()
            if wait > 0:
                await asyncio.sleep(wait)
            self.last[host] = time.monotonic()

    async def get(self, source: str, host: str, base: str, path: str, *, params: dict | None = None,
                  purpose: str, subjects: Iterable[str] = (), signer: Signer | None = None, fresh: bool = False,
                  same_as: Iterable[str] = (), final_after_ms: int | None = None,
                  final: Callable[[object], bool] | None = None):
        """The request's summary (the body, for the kept purposes), or None if it failed.
        `same_as`: other paths that return the same data, already fetched counts as done.
        `final_after_ms`: a copy fetched before then may have changed since, and is fetched again.
        `final`: whether a copy's content can no longer change; one that can is fetched again."""
        query = build_query(params)
        key = _key(source, base.rstrip("/") + path + (f"?{query}" if query else ""))
        if not fresh:
            for k in [key] + [_key(source, base.rstrip("/") + p + (f"?{query}" if query else "")) for p in same_as]:
                if k in self.this_run or k in self.done and (
                        (final_after_ms is None or self.done[k][1] >= final_after_ms)
                        and (final is None or final(self.done[k][0]))):
                    self.stats.skipped[source] += 1
                    return self.done[k][0]
        res = None
        for attempt in range(self.retries):
            await self._pace(host)
            res = await self.rests[source].request("GET", base, path, params=params, subjects=list(subjects),
                                                   purpose=purpose, signer=signer)
            if res.status not in _RETRY_STATUSES:
                break
            await asyncio.sleep(self.retry_wait_s * (attempt + 1))
        if res.ok and res.body is not None:
            self.done[key] = (summarize(purpose, res.body.decode("utf-8", "surrogateescape")), clock())
            self.this_run.add(key)
            self.stats.fetched[source] += 1
            return self.done[key][0]
        self.stats.failed.append((source, path, query, res.status or res.failure))
        return None


# -- date ranges ---------------------------------------------------------------------------------------

Ranges = list[tuple[date, date]]


def parse_ranges(texts: Iterable[str] | None) -> Ranges:
    out = []
    for t in texts or ():
        lo, _, hi = t.partition(":")
        out.append((date.fromisoformat(lo), date.fromisoformat(hi or lo)))
    return out


def in_ranges(d: date, ranges: Ranges, slack_days: int = 0) -> bool:
    return not ranges or any(lo - timedelta(slack_days) <= d <= hi + timedelta(slack_days) for lo, hi in ranges)


# -- MLB StatsAPI ----------------------------------------------------------------------------------------

def _months(season: int) -> list[tuple[date, date]]:
    out = []
    for month in range(2, 12):                       # spring training to the World Series
        lo = date(season, month, 1)
        hi = (date(season + (month == 12), month % 12 + 1, 1)) - timedelta(1)
        out.append((lo, hi))
    return out


async def mlb(f: Fetcher, cfg: dict, base: str, ranges: Ranges, today: date) -> dict[int, dict]:
    """Schedule rows by gamePk (the Final row of a postponed game wins), and their play-by-play."""
    games: dict[int, dict] = {}
    for season in cfg["seasons"]:
        # The schedule names teams without abbreviations; the season's team list has them.
        await f.get("mlb_hist", "mlb", base, "/api/v1/teams", purpose="teams",
                    params={"sportId": "1", "season": str(season)}, subjects=["mlb:teams"])
        for lo, hi in _months(season):
            if lo > today:
                continue
            # A month's games end by two days after it (a late game, a suspended one resumed):
            # only a copy fetched after that is final, however long ago the month was.
            final = datetime.combine(hi + timedelta(days=3), datetime.min.time(), timezone.utc)
            body = await f.get("mlb_hist", "mlb", base, "/api/v1/schedule", purpose="schedule",
                               params={"sportId": "1", "startDate": lo.isoformat(), "endDate": hi.isoformat(),
                                       "gameType": cfg["game_types"]},
                               subjects=["mlb:schedule"], final_after_ms=int(final.timestamp() * 1000))
            for day in json.loads(body).get("dates", []) if body else ():
                for g in day.get("games", []):
                    if g["gamePk"] not in games or g["status"].get("detailedState") == "Final":
                        games[g["gamePk"]] = g
    for pk, g in sorted(games.items()):
        if g["status"].get("detailedState") == "Final" and in_ranges(date.fromisoformat(g["officialDate"]), ranges):
            await f.get("mlb_hist", "mlb", base, f"/api/v1/game/{pk}/playByPlay", purpose="play_by_play",
                        subjects=[f"mlb:game:{pk}"])
    return games


# -- Kalshi ------------------------------------------------------------------------------------------------

def candle_path(series: str, ticker: str, historical: bool) -> str:
    """A market listed on the historical tier (settled before the cutoff) has its candles there."""
    if historical:
        return f"/historical/markets/{ticker}/candlesticks"
    return f"/series/{series}/markets/{ticker}/candlesticks"


TRADE_PATHS = ("/historical/trades", "/markets/trades")


def trade_windows(lo_s: int, hi_s: int, cutoff: dict) -> list[tuple[str, int, int]]:
    """(path, min_ts, max_ts) per tier: trades created before trades_created_ts are historical."""
    split = _iso_s(cutoff["trades_created_ts"])
    out = []
    if lo_s < split:
        out.append(("/historical/trades", lo_s, min(hi_s, split)))
    if hi_s > split:
        out.append(("/markets/trades", max(lo_s, split), hi_s))
    return out


async def _pages(f: Fetcher, base: str, path: str, params: dict, purpose: str, subjects: list[str], key: str):
    """Every page of a cursor-paged Kalshi listing, fetched fresh; stops at a failed page."""
    out, cursor = [], ""
    while True:
        body = await f.get("kalshi_hist", "kalshi", base, path, purpose=purpose, subjects=subjects, fresh=True,
                           params=dict(params, **({"cursor": cursor} if cursor else {})))
        if body is None:
            break                                   # retried next run; what was listed so far proceeds
        page = json.loads(body)
        out += page.get(key) or []
        cursor = page.get("cursor") or ""
        if not cursor:
            break
    return out


async def kalshi(f: Fetcher, cfg: dict, base: str, series: str, ranges: Ranges, now_s: int) -> Counter:
    """Markets come from two listings: the live events with their nested markets, and the
    historical tier's market listing, which holds every market settled before the cutoff (an
    event on that tier lists no markets of its own). A market's tier is the listing it is in."""
    seen = Counter()
    body = await f.get("kalshi_hist", "kalshi", base, "/historical/cutoff", purpose="cutoff",
                       subjects=["kalshi:historical_cutoff"], fresh=True)
    if body is None:
        return seen
    cutoff = json.loads(body)
    subjects = [f"kalshi:series:{series}"]
    markets: dict[str, tuple[dict, bool]] = {}
    for ev in await _pages(f, base, "/events", {"series_ticker": series, "limit": "200", "with_nested_markets": "true"},
                           "events", subjects, "events"):
        for m in ev.get("markets") or []:
            markets[m["ticker"]] = (m, False)
    for m in await _pages(f, base, "/historical/markets", {"series_ticker": series, "limit": "1000"},
                          "markets", subjects, "markets"):
        markets[m["ticker"]] = (m, True)
    lookback, per_request = cfg["lookback_s"], cfg["candles_per_request"] * 60
    for ticker, (m, historical) in sorted(markets.items()):
        if m.get("status") not in _SETTLED or not m.get("close_time"):
            seen["not_settled"] += 1
            continue
        hi_s = _iso_s(m["close_time"])
        if hi_s > now_s or not in_ranges(datetime.fromtimestamp(hi_s, timezone.utc).date(), ranges, 1):
            seen["out_of_range"] += 1
            continue
        seen["historical_markets" if historical else "live_markets"] += 1
        lo_s, msubjects = hi_s - lookback, [f"kalshi:market:{ticker}"]
        for start in range(lo_s, hi_s, per_request):
            await f.get("kalshi_hist", "kalshi", base, candle_path(series, ticker, historical), purpose="candles",
                        same_as=[candle_path(series, ticker, not historical)], subjects=msubjects, params={"start_ts": str(start), "end_ts": str(min(start + per_request, hi_s)),
                                                    "period_interval": "1"})
        for tpath, tlo, thi in trade_windows(lo_s, hi_s, cutoff):
            cursor = ""
            while True:
                params = {"ticker": ticker, "min_ts": str(tlo), "max_ts": str(thi),
                          "limit": str(cfg["trades_page_limit"])}
                if cursor:
                    params["cursor"] = cursor
                page = await f.get("kalshi_hist", "kalshi", base, tpath, params=params, purpose="trades",
                                   subjects=msubjects, same_as=[p for p in TRADE_PATHS if p != tpath])
                cursor = (page or {}).get("cursor", "")
                if not cursor:
                    break
    return seen


# -- Novig -------------------------------------------------------------------------------------------------

def _status(body) -> dict:
    try:
        out = json.loads(body)
    except (TypeError, ValueError):
        return {}
    return out if isinstance(out, dict) else {}


def market_final(body) -> bool:
    """A market's history is final once it is settled: its grades and settledTs are set."""
    m = _status(body)
    return m.get("status") == "SETTLED" and m.get("settledTs") is not None


def event_final(body) -> bool:
    return _status(body).get("status") in ("FINAL", "CANCELED")


async def novig(f: Fetcher, cfg: dict, data_url: str, host: str, signer: Signer | None, ranges: Ranges,
                today: date) -> Counter:
    seen = Counter()
    prefix = cfg["novig_data_path"]
    body = await f.get("novig_hist", "novig_data", data_url, f"{prefix}/index.json", purpose="index",
                       subjects=["novig:trade_data"], fresh=True)
    if body is None:
        return seen
    index = json.loads(body)
    first = date.fromisoformat(cfg["novig_first_day"])
    markets: set[str] = set()
    for name, purpose, days in (("trades", "trades_csv", index.get("dates", [])),
                                ("markets", "markets_csv", index.get("marketDates", []))):
        for day in sorted(days):
            d = date.fromisoformat(day)
            if d < first or d >= today or not in_ranges(d, ranges):
                continue
            seen[purpose] += 1
            got = await f.get("novig_hist", "novig_data", data_url, f"{prefix}/{day}/{name}.csv", purpose=purpose,
                              subjects=[f"novig:trade_data:{day}"])
            markets |= got or set()
    seen["markets"] = len(markets)
    if signer is None:
        seen["history_skipped_no_key"] = len(markets)
        return seen
    open_events = set()
    for mid in sorted(markets):
        body = await f.get("novig_hist", "novig_history", host, f"/v3/history/markets/{mid}",
                           purpose="history_market", subjects=[f"novig:market:{mid}"], signer=signer,
                           final=market_final)
        if body is not None and not market_final(body):
            seen["history_markets_not_final"] += 1          # fetched again next run
        event = json.loads(body).get("eventId") if body else None
        if event:
            got = await f.get("novig_hist", "novig_history", host, f"/v3/history/events/{event}",
                              purpose="history_event", subjects=[f"novig:event:{event}"], signer=signer,
                              final=event_final)
            if got is not None and not event_final(got):
                open_events.add(event)
    if open_events:
        seen["history_events_not_final"] = len(open_events)
    return seen


# -- one run ---------------------------------------------------------------------------------------------

async def run(rests: dict[str, RestRecorder], done: dict, cfg, sources: Iterable[str], ranges: Ranges,
              signer: Signer | None, now_s: int | None = None) -> tuple[Stats, dict[str, Counter]]:
    """Fetch what the archive lacks for each source, the three hosts in parallel."""
    b = cfg.backfill
    now_s = int(time.time()) if now_s is None else now_s
    today = datetime.fromtimestamp(now_s, timezone.utc).date()
    f = Fetcher(rests, done, b["min_interval_s"])
    jobs = {
        "mlb": lambda: mlb(f, b, cfg.mlb["base_url"], ranges, today),
        "kalshi": lambda: kalshi(f, b, cfg.kalshi["base_url"], cfg.kalshi["series_ticker"], ranges, now_s),
        "novig": lambda: novig(f, b, b["novig_data_url"], cfg.novig["host"], signer, ranges, today),
    }
    names = [s for s in jobs if s in set(sources)]
    results = await asyncio.gather(*(jobs[s]() for s in names))
    seen = {s: (Counter({"games": len(r)}) if s == "mlb" else r) for s, r in zip(names, results)}
    return f.stats, seen
