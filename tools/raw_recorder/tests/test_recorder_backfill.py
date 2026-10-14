"""B1 backfill: every request archived, a rerun fetches only what is missing or can change,
the Kalshi tier follows the cutoff, and the Novig history routes are signed."""
import asyncio
import dataclasses
import errno
import itertools
import json
import os
from collections import Counter
from datetime import date

from aiohttp import web
from aiohttp.test_utils import TestServer
from raw_recorder import backfill, cli
from raw_recorder.archive import iter_archive
from raw_recorder.replay import rest_exchanges
from raw_recorder.session import ArchiveLock
from test_recorder_runtime import config

NOW_S = 1_796_083_200                       # 2026-12-01T00:00Z: every schedule month of 2026 is past
CUTOFF = {"market_settled_ts": "2026-08-10T00:00:00Z", "trades_created_ts": "2026-08-10T00:00:00Z"}


def market(ticker, close, status="finalized", settled=None):
    return {"ticker": ticker, "status": status, "close_time": close, "settlement_ts": settled or close}


class Vendors:
    """StatsAPI, Kalshi and Novig, in their real shapes, counting every request."""

    def __init__(self):
        self.hits: Counter = Counter()
        self.fail_once = {"/historical/markets/KX-A-AAA/candlesticks"}
        self.signed: list[str] = []
        self.app = web.Application()
        r = self.app.router
        r.add_get("/api/v1/schedule", self.schedule)
        r.add_get("/api/v1/teams", self.json({"teams": []}))
        r.add_get("/api/v1/game/{pk}/playByPlay", self.play_by_play)
        self.unplayed: set[int] = set()             # games the schedule still lists as Scheduled
        self.delay_s = 0.0
        r.add_get("/historical/cutoff", self.json(CUTOFF))
        r.add_get("/events", self.events)
        r.add_get("/historical/markets", self.historical_markets)
        self.moved = []                             # markets the cutoff has since moved to the historical tier
        r.add_get("/historical/markets/{t}/candlesticks", self.candles)
        r.add_get("/series/{s}/markets/{t}/candlesticks", self.candles)
        r.add_get("/historical/trades", self.trades)
        r.add_get("/markets/trades", self.trades)
        r.add_get("/reporting/trade-data/index.json",
                  self.json({"dates": ["2026-08-02", "2026-08-03", "2026-08-04"], "marketDates": ["2026-08-04"]}))
        r.add_get("/reporting/trade-data/{day}/trades.csv", self.text(
            "timestamp,outcomeId,marketId,contractSeries,league,marketType,tradeType,legs,cost,qty,side\n"
            "2026-08-04T10:17:19.715Z,o1,m1,Baseball Moneyline,MLB,MONEY,STRAIGHT,1,50,156.25,TAKER\n"
            "2026-08-04T10:17:20.000Z,o9,n9,Basketball,NBA,MONEY,STRAIGHT,1,5,10,TAKER\n"))
        r.add_get("/reporting/trade-data/{day}/markets.csv", self.text(
            "date,marketId,reportTicker,openInterest,dailyVolume,open,high,low,close,status\n"
            "2026-08-04,m2,MLB-WINNER,0,1,50,50,50,50,finalized\n"))
        r.add_get("/v3/history/markets/{m}", self.history_market)
        r.add_get("/v3/history/events/{e}", self.json({"eventId": "e"}))

    def count(self, request):
        self.hits[request.path] += 1

    def json(self, body):
        async def handler(request):
            self.count(request)
            return web.json_response(body)
        return handler

    def text(self, body):
        async def handler(request):
            self.count(request)
            return web.Response(text=body, content_type="text/csv")
        return handler

    async def schedule(self, request):
        self.count(request)
        games = []
        if request.query["startDate"] == "2026-08-01":
            games = [{"gamePk": 1001, "officialDate": "2026-08-04", "status": {"detailedState": "Final"}},
                     {"gamePk": 1002, "officialDate": "2026-08-04", "status": {"detailedState": "Postponed"}},
                     {"gamePk": 1003, "officialDate": "2026-08-20", "status": {"detailedState": "Final"}}]
            for g in games:
                if g["gamePk"] in self.unplayed:
                    g["status"] = {"detailedState": "Scheduled"}
        return web.json_response({"dates": [{"games": games}] if games else []})

    async def play_by_play(self, request):
        self.count(request)
        await asyncio.sleep(self.delay_s)
        return web.json_response({"allPlays": []})

    async def events(self, request):
        self.count(request)
        if request.query.get("cursor") == "c2":     # an event on the historical tier lists no markets
            return web.json_response({"events": [{"event_ticker": "KX-A", "markets": []}], "cursor": ""})
        live = [m for m in (market("KX-B-BBB", "2026-09-01T02:00:00Z"),
                            market("KX-B-STR", "2026-08-10T10:00:00Z"),     # its window straddles the trades cutoff
                            market("KX-B-CCC", "2026-12-24T02:00:00Z", "active")) if m["ticker"] not in self.moved]
        return web.json_response({"cursor": "c2", "events": [{"event_ticker": "KX-B", "markets": live}]})

    async def historical_markets(self, request):
        self.count(request)
        if request.query.get("cursor") == "h2":
            moved = [market(t, "2026-09-01T02:00:00Z") for t in self.moved]
            return web.json_response({"markets": [market("KX-A-AAA", "2026-08-05T01:00:00Z")] + moved, "cursor": ""})
        return web.json_response({"markets": [], "cursor": "h2"})

    async def candles(self, request):
        self.count(request)
        if request.path in self.fail_once:
            self.fail_once.discard(request.path)
            return web.Response(status=503)
        return web.json_response({"candlesticks": [], "ticker": request.match_info["t"]})

    async def trades(self, request):
        self.count(request)
        self.hits[(request.path, request.query["ticker"], request.query.get("cursor", ""))] += 1
        cursor = "" if request.query.get("cursor") else "p2"
        return web.json_response({"cursor": cursor, "trades": []})

    async def history_market(self, request):
        self.count(request)
        self.signed.append(request.headers.get("Novig-Signature", ""))
        return web.json_response({"marketId": request.match_info["m"], "eventId": "e"})


def backfill_config(tmp_path, base):
    cfg = config(tmp_path, base)
    return dataclasses.replace(cfg, kalshi={"base_url": base, "series_ticker": "KXMLBGAME"}, backfill={
        "root": str(tmp_path / "archive-hist"), "seasons": [2026], "game_types": "R,F,D,L,W",
        "lookback_s": 115200, "candles_per_request": 5000, "trades_page_limit": 1000,
        "novig_first_day": "2026-08-03", "novig_data_url": base, "novig_data_path": "/reporting/trade-data",
        "rest_timeout_s": 10, "segment_max_s": 300, "min_interval_s": {}})


def fake_signer(method, path, raw_query, body):
    return {"Novig-Key-Id": "read-key", "Novig-Timestamp": "1", "Novig-Signature": "sig"}


async def run_cli(cfg, monkeypatch, sources=("mlb", "kalshi", "novig"), ranges=(), now_s=NOW_S):
    monkeypatch.setattr(cli, "load_signer", lambda c: fake_signer)
    monkeypatch.setattr(backfill.time, "time", lambda: now_s)
    return await cli._backfill(cfg, list(sources), backfill.parse_ranges(ranges))


async def test_a_rerun_fetches_only_what_is_missing_or_can_change(tmp_path, monkeypatch):
    v = Vendors()
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch) == 1                  # one candle request failed
        first = Counter(v.hits)
        v.hits.clear()
        assert await run_cli(cfg, monkeypatch) == 0
        second = Counter(v.hits)
    finally:
        await srv.close()
    # First run: 10 schedule months (Feb to Nov); play-by-play for the two Final games only.
    assert first["/api/v1/schedule"] == 10 and first["/api/v1/teams"] == 1
    assert first["/api/v1/game/1001/playByPlay"] == first["/api/v1/game/1003/playByPlay"] == 1
    assert first["/api/v1/game/1002/playByPlay"] == 0
    # Kalshi: both listings are read to their last page; the open market is skipped.
    assert first["/events"] == first["/historical/markets"] == 2
    assert first["/series/KXMLBGAME/markets/KX-B-CCC/candlesticks"] == 0
    # Novig: files from novig_first_day; history for the MLB markets in either file, signed.
    assert first["/reporting/trade-data/2026-08-02/trades.csv"] == 0
    assert first["/v3/history/markets/m1"] == first["/v3/history/markets/m2"] == 1
    assert first["/v3/history/markets/n9"] == 0
    assert v.signed and set(v.signed) == {"sig"}
    # The rerun: the listings that can change (November's schedule ends within two days of today),
    # and the one failed request. Nothing else.
    assert second == Counter({"/historical/cutoff": 1, "/events": 2, "/historical/markets": 2,
                              "/reporting/trade-data/index.json": 1, "/api/v1/schedule": 1,
                              "/historical/markets/KX-A-AAA/candlesticks": 1})
    # Every request, both runs, is a complete envelope in the backfill root, never in archive/.
    xs = rest_exchanges(iter_archive(cfg.backfill_root))
    assert len(xs) == sum(first.values()) + sum(second.values()) - trade_page_keys(first) - trade_page_keys(second)
    assert all(x.complete is not None for x in xs.values())
    assert {p.parent.parent.name for p in cfg.backfill_root.rglob("*.jsonl.gz")} == set(backfill.SOURCES.values())
    assert not cfg.archive_root.exists()


def trade_page_keys(hits):
    """The trades handler also counts (path, ticker, cursor) keys, which are not requests."""
    return sum(n for k, n in hits.items() if isinstance(k, tuple))


async def test_kalshi_tiers_and_trade_pages_follow_the_cutoff(tmp_path, monkeypatch):
    v = Vendors()
    v.fail_once.clear()
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, sources=("kalshi",)) == 0
    finally:
        await srv.close()
    h = v.hits
    # Settled before market_settled_ts: the historical routes. After it: the live series route.
    assert h["/historical/markets/KX-A-AAA/candlesticks"] == 1
    assert h["/series/KXMLBGAME/markets/KX-B-BBB/candlesticks"] == 1
    # The straddling market settled after the cutoff, so its candles are live; its trades come
    # from both tiers, split at trades_created_ts. Each tier is paged to its end.
    assert h["/series/KXMLBGAME/markets/KX-B-STR/candlesticks"] == 1
    for path in ("/historical/trades", "/markets/trades"):
        assert h[(path, "KX-B-STR", "")] == h[(path, "KX-B-STR", "p2")] == 1
    assert h[("/markets/trades", "KX-A-AAA", "")] == 0
    assert h[("/historical/trades", "KX-B-BBB", "")] == 0


def test_trade_windows_split_at_the_cutoff():
    split = backfill._iso_s(CUTOFF["trades_created_ts"])
    assert backfill.trade_windows(split - 100, split - 10, CUTOFF) == [("/historical/trades", split - 100, split - 10)]
    assert backfill.trade_windows(split + 10, split + 100, CUTOFF) == [("/markets/trades", split + 10, split + 100)]
    assert backfill.trade_windows(split - 100, split + 100, CUTOFF) == [
        ("/historical/trades", split - 100, split), ("/markets/trades", split, split + 100)]


async def test_dates_limit_a_pilot_run(tmp_path, monkeypatch):
    v = Vendors()
    v.fail_once.clear()
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, ranges=("2026-08-04",)) == 0
    finally:
        await srv.close()
    assert v.hits["/api/v1/game/1001/playByPlay"] == 1 and v.hits["/api/v1/game/1003/playByPlay"] == 0
    assert v.hits["/reporting/trade-data/2026-08-03/trades.csv"] == 0
    assert v.hits["/reporting/trade-data/2026-08-04/trades.csv"] == 1
    # Kalshi markets within a day of the range (ticker dates can be a day off): KX-A-AAA closed 08-05.
    assert v.hits["/historical/markets/KX-A-AAA/candlesticks"] == 1
    assert v.hits["/series/KXMLBGAME/markets/KX-B-BBB/candlesticks"] == 0


async def test_it_runs_beside_the_recorder_and_refuses_a_second_backfill(tmp_path, monkeypatch):
    v = Vendors()
    v.fail_once.clear()
    srv = TestServer(v.app)
    await srv.start_server()
    recorder = ArchiveLock(tmp_path / "archive")
    assert recorder.acquire()                       # the live recorder holds archive/
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, sources=("mlb",)) == 0
        other = ArchiveLock(cfg.backfill_root)
        assert other.acquire()
        try:
            try:
                await run_cli(cfg, monkeypatch, sources=("mlb",))
                raise AssertionError("a second backfill ran on the same root")
            except cli.RecorderBusy as exc:
                assert "already writing" in str(exc)
        finally:
            other.release()
    finally:
        recorder.release()
        await srv.close()


async def test_without_the_read_key_the_history_routes_are_skipped(tmp_path, monkeypatch):
    v = Vendors()
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        monkeypatch.setattr(cli, "load_signer", lambda c: None)
        assert await cli._backfill(cfg, ["novig"], []) == 0
    finally:
        await srv.close()
    assert not [k for k in v.hits if isinstance(k, str) and k.startswith("/v3/history")]


def test_the_committed_config_writes_to_its_own_root():
    from raw_recorder.config import DEFAULT_CONFIG, load_config
    cfg = load_config(DEFAULT_CONFIG)
    assert cfg.backfill_root.name == "archive-hist" and cfg.backfill_root != cfg.archive_root
    assert cfg.backfill["lookback_s"] >= 86400 + 4 * 3600          # the 24 h offset, plus a game
    assert cfg.backfill["segment_max_s"] <= 600                    # segments small enough to read back
    assert json.dumps(cfg.backfill)                                # plain data, no secrets
    assert date.fromisoformat(cfg.backfill["novig_first_day"]) == date(2026, 8, 3)
    assert asyncio.iscoroutinefunction(backfill.run)


async def test_a_market_the_cutoff_moved_is_not_fetched_again(tmp_path, monkeypatch):
    v = Vendors()
    v.fail_once.clear()
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, sources=("kalshi",)) == 0
        assert v.hits["/series/KXMLBGAME/markets/KX-B-BBB/candlesticks"] == 1
        v.moved.append("KX-B-BBB")                  # now listed on the historical tier only
        v.hits.clear()
        assert await run_cli(cfg, monkeypatch, sources=("kalshi",)) == 0
    finally:
        await srv.close()
    assert v.hits["/historical/markets/KX-B-BBB/candlesticks"] == 0
    assert not [k for k in v.hits if isinstance(k, tuple) and k[1] == "KX-B-BBB"]


def iso_s(text):
    return backfill._iso_s(text)


async def test_a_month_fetched_before_its_games_ended_is_fetched_again(tmp_path, monkeypatch):
    # August cached mid-month, game 1003 still to play; a resume in November must refresh it
    # (PR #16 review). Once a copy from after the month's end is archived, it is final.
    v = Vendors()
    v.unplayed.add(1003)
    srv = TestServer(v.app)
    await srv.start_server()
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, sources=("mlb",), now_s=iso_s("2026-08-14T12:00:00Z")) == 0
        assert v.hits["/api/v1/game/1003/playByPlay"] == 0
        v.unplayed.clear()
        v.hits.clear()
        assert await run_cli(cfg, monkeypatch, sources=("mlb",), now_s=iso_s("2026-11-03T12:00:00Z")) == 0
        assert v.hits["/api/v1/game/1003/playByPlay"] == 1
        assert v.hits["/api/v1/schedule"] == 4              # August, whose copy was early; Sep to Nov, new
        v.hits.clear()
        assert await run_cli(cfg, monkeypatch, sources=("mlb",), now_s=iso_s("2026-12-01T00:00:00Z")) == 0
    finally:
        await srv.close()
    # October's copy from November 3 is final; November's from then is not, nor is a copy made today.
    assert v.hits["/api/v1/schedule"] == 1


async def test_the_latest_of_two_responses_is_the_one_resumed_from(tmp_path, monkeypatch):
    # Two runs on one UTC day both fetch November's schedule, still open. Segment names sort by
    # their random session IDs, here against time; the later response must still win (PR #16
    # review), or a finished month could resume from a copy taken before its games were played.
    v = Vendors()
    srv = TestServer(v.app)
    await srv.start_server()
    sessions = iter(["f" * 32, "0" * 32])
    monkeypatch.setattr(cli, "new_session_id", lambda: next(sessions))
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        for hour in ("01", "02"):
            assert await run_cli(cfg, monkeypatch, sources=("mlb",), now_s=iso_s(f"2026-11-20T{hour}:00:00Z")) == 0
    finally:
        await srv.close()
    names = sorted(p.name for p in (cfg.backfill_root / "mlb_hist").rglob("*.jsonl.gz"))
    assert names[0].startswith("backfill-000") and names[-1].startswith("backfill-fff")
    done = backfill.load_done(cfg.backfill_root)
    (_, at), = [x for k, x in done.items() if "startDate=2026-11-01" in k[2]]
    assert at == iso_s("2026-11-20T02:00:00Z") * 1000


async def test_the_archive_is_fsynced_on_cadence_while_the_backfill_runs(tmp_path, monkeypatch):
    # The recorder's housekeeping runs beside the fetch (PR #16 review): frames reach the disk
    # every fsync_interval_s, not only when the run ends.
    v = Vendors()
    v.fail_once.clear()
    v.delay_s = 0.1
    srv = TestServer(v.app)
    await srv.start_server()
    fsyncs, closing = [], []
    real_fsync, real_close = os.fsync, cli.ArchiveWriter.close
    monkeypatch.setattr("raw_recorder.archive.os.fsync", lambda fd: (fsyncs.append(bool(closing)), real_fsync(fd)))
    monkeypatch.setattr(cli.ArchiveWriter, "close", lambda self: (closing.append(1), real_close(self)))
    monkeypatch.setattr(cli, "TICK_S", 0.01)
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        cfg = dataclasses.replace(cfg, params=dataclasses.replace(cfg.params, fsync_interval_s=0.02))
        assert await run_cli(cfg, monkeypatch, sources=("mlb",)) == 0
    finally:
        await srv.close()
    assert False in fsyncs                          # during the run, not only at its close


def no_space(part, **kw):
    raise OSError(errno.ENOSPC, "No space left on device", str(part))


async def test_a_seal_that_fails_mid_run_stops_the_backfill_and_the_next_run_recovers(tmp_path, monkeypatch, capsys):
    # A segment rotates mid-run and cannot be sealed (PR #16 review): the run stops and fails,
    # rather than reporting success with requests missing from the sealed archive. The next
    # run seals what was left (recover_orphans) and resumes from it.
    v = Vendors()
    v.fail_once.clear()
    v.delay_s = 0.1
    srv = TestServer(v.app)
    await srv.start_server()
    monkeypatch.setattr(cli, "TICK_S", 0.01)
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        cfg = dataclasses.replace(cfg, backfill=dict(cfg.backfill, segment_max_s=1))
        ticks = itertools.count()
        monkeypatch.setattr(cli, "load_signer", lambda c: fake_signer)
        monkeypatch.setattr(backfill.time, "time", lambda: NOW_S + next(ticks) / 2)     # segments come due
        monkeypatch.setattr("raw_recorder.archive.seal_part", no_space)
        assert await cli._backfill(cfg, ["mlb"], []) == 2
        first = Counter(v.hits)
        assert "No space left on device" in capsys.readouterr().err
        assert list(cfg.backfill_root.rglob("*.jsonl.part"))
        monkeypatch.undo()
        v.hits.clear()
        assert await run_cli(cfg, monkeypatch, sources=("mlb",)) == 0
    finally:
        await srv.close()
    # The fetch stopped: a full run makes 13 requests (10 schedule months, teams, 2 play-by-play).
    assert sum(first.values()) < 13
    assert "sealed orphan" in capsys.readouterr().out
    assert not list(cfg.backfill_root.rglob("*.jsonl.part"))
    # What the stopped run completed was recovered, not fetched again; a request it cancelled
    # mid-response (play-by-play) was.
    assert first["/api/v1/teams"] == 1 and v.hits["/api/v1/teams"] == 0
    assert v.hits["/api/v1/game/1001/playByPlay"] == v.hits["/api/v1/game/1003/playByPlay"] == 1
    assert sum(v.hits.values()) < 13


async def test_a_seal_that_fails_at_close_fails_the_backfill(tmp_path, monkeypatch, capsys):
    v = Vendors()
    v.fail_once.clear()
    srv = TestServer(v.app)
    await srv.start_server()
    monkeypatch.setattr("raw_recorder.archive.seal_part", no_space)
    try:
        cfg = backfill_config(tmp_path, str(srv.make_url("")).rstrip("/"))
        assert await run_cli(cfg, monkeypatch, sources=("mlb",)) == 2
    finally:
        await srv.close()
    captured = capsys.readouterr()
    assert "sealing failed" in captured.err and "rerun" in captured.err
    assert "fetched" not in captured.out                # no per-source summary to mislead
