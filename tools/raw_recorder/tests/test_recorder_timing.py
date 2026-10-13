"""Periodic tasks keep wall-clock time across a host suspend, and the odds poller polls as
an odds window opens rather than at its next interval."""
import asyncio
import time

from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder import runtime
from raw_recorder.archive import iter_archive
from raw_recorder.config import Game
from raw_recorder.replay import rest_exchanges
from raw_recorder.runtime import Recorder
from test_recorder_runtime import FakeInhibitor, config


async def test_an_overdue_task_runs_within_seconds_of_a_wake(tmp_path, monkeypatch):
    # The event loop's clock stops while the host is suspended; the wall clock does not.
    # Here the wall clock jumps an hour ahead, as on waking, and the hourly task runs at once.
    monkeypatch.setattr(Recorder, "WAKE_CHECK_S", 0.05)
    offset = [0.0]
    real = time.time
    monkeypatch.setattr(runtime.time, "time", lambda: real() + offset[0])
    rec = Recorder(config(tmp_path, "http://unused"), [], inhibitor=FakeInhibitor())
    runs = []

    async def fn():
        runs.append(runtime.time.time())
    task = asyncio.create_task(rec.every("hourly", 3600, fn))
    await asyncio.sleep(0.2)
    assert len(runs) == 1
    offset[0] = 3600.5                                  # the host slept through the next run
    await asyncio.sleep(0.2)
    assert len(runs) == 2
    await asyncio.sleep(0.2)
    assert len(runs) == 2                               # then back on its hourly schedule
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def odds_vendor(fail_first: list[bool]):
    app = web.Application()

    async def odds(request):
        if fail_first and fail_first.pop():
            request.transport.close()                   # the connection drops: no HTTP status at all
            await asyncio.sleep(1)
        return web.json_response([], headers={"x-requests-remaining": "400", "x-requests-used": "100",
                                               "x-requests-last": "1"})

    async def ok(request):
        return web.json_response({})

    app.router.add_get("/v4/sports/{sport}/odds", odds)
    app.router.add_get("/v4/sports", ok)
    return app


async def run_odds(tmp_path, monkeypatch, opens_in_ms, seconds, fail_first=()):
    monkeypatch.setattr(Recorder, "ODDS_CHECK_S", 0.1)
    monkeypatch.setenv("R0_TEST_UNSET_ODDS", "test-key")
    srv = TestServer(odds_vendor(list(fail_first)))
    await srv.start_server()
    try:
        cfg = config(tmp_path, str(srv.make_url("")).rstrip("/"))
        for section in (cfg.kalshi, cfg.mlb, cfg.novig):
            section["enabled"] = False
        cfg.odds_api.update(sport="baseball_mlb", regions="us", markets="h2h", odds_format="american")
        now = time.time_ns() // 1_000_000
        start = now + cfg.params.odds_window_lead_s * 1000 + opens_in_ms     # the odds window opens then
        rec = Recorder(cfg, [Game(1, start, "", (), ())], inhibitor=FakeInhibitor())
        task = asyncio.create_task(rec.run())
        await asyncio.sleep(seconds)
        rec.stop.set()
        await task
    finally:
        await srv.close()
    return now + opens_in_ms, [x for x in rest_exchanges(list(iter_archive(cfg.archive_root))).values()
                               if x.start["path"].endswith("/odds")]


async def test_odds_are_polled_as_the_window_opens(tmp_path, monkeypatch):
    opened, polls = await run_odds(tmp_path, monkeypatch, opens_in_ms=500, seconds=1.2)
    (poll,) = polls                                     # once: the next is odds_poll_interval_s later
    assert 0 <= poll.start["start_ts_ms"] - opened <= 400


async def test_a_poll_with_no_response_is_retried_at_the_next_check(tmp_path, monkeypatch):
    # Two drops: aiohttp itself retries a dropped keep-alive connection once.
    _, polls = await run_odds(tmp_path, monkeypatch, opens_in_ms=0, seconds=0.8, fail_first=[True, True])
    assert [x.complete["status"] for x in polls] == [None, 200]
