"""REST envelope fixtures (DESIGN.md §7.1 R0 archive verification).

Local aiohttp servers only; no network.
"""
import asyncio
import gzip
import json

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder import kalshi
from raw_recorder.archive import ArchiveWriter, iter_archive, verify
from raw_recorder.odds_api import OddsPoller
from raw_recorder.replay import rest_exchanges
from raw_recorder.rest import RestRecorder

BOOK = '{"orderbook_fp":{"yes_dollars":[["0.5100","11.00"]],"no_dollars":[["0.4800","12.00"]]}}'
HTML_403 = "<html><body><h1>403 Forbidden</h1>Request blocked.</body></html>"
SECRET = "sk-test-SECRET-5f1e"


@pytest.fixture
def archive(tmp_path):
    w = ArchiveWriter(tmp_path, session_id="t" * 32, segment_max_s=3600, fsync_interval_s=5)
    yield w
    w.close()


def make_app():
    app = web.Application()

    async def orderbook(request):
        ticker = request.match_info["ticker"]
        # The first ticker answers last, so responses complete out of request order.
        await asyncio.sleep(0.2 if ticker.endswith("-A") else 0.0)
        return web.Response(text=BOOK, content_type="application/json")   # identical bodies

    async def slow(request):
        await asyncio.sleep(5)
        return web.Response(text="late")

    async def forbidden(request):
        return web.Response(status=403, text=HTML_403, content_type="text/html")

    quota = {"remaining": 51}

    async def odds(request):
        remaining = quota["remaining"]
        quota["remaining"] -= 1
        return web.json_response([], headers={"x-requests-remaining": str(remaining),
                                              "x-requests-used": str(500 - remaining),
                                              "x-requests-last": "1"})

    app.router.add_get("/markets/{ticker}/orderbook", orderbook)
    app.router.add_get("/slow", slow)
    app.router.add_get("/forbidden", forbidden)
    app.router.add_get("/v4/sports/baseball_mlb/odds", odds)
    app.router.add_get("/v4/sports", odds)
    return app


@pytest.fixture
async def server():
    srv = TestServer(make_app())
    await srv.start_server()
    yield srv
    await srv.close()


def base(server):
    return str(server.make_url("")).rstrip("/")


async def test_interleaved_identical_bodies_associate_by_request_id(archive, server):
    async with aiohttp.ClientSession() as http:
        rest = RestRecorder(http, archive.stream("kalshi", "kalshi-test"), timeout_s=2)
        await asyncio.gather(kalshi.poll_orderbooks(rest, base(server), ("KX-A",)),
                             kalshi.poll_orderbooks(rest, base(server), ("KX-B",)))
    archive.close()
    frames = list(iter_archive(archive.root))
    # Bodies are byte-identical and B completed first, so adjacency would mislead...
    bodies = [f for f in frames if f["dir"] == "in"]
    assert bodies[0]["frame"] == bodies[1]["frame"] == BOOK
    # ...but every body is still attributable to its own ticker.
    exchanges = rest_exchanges(frames)
    by_subject = {ex.subjects[0]: ex for ex in exchanges.values()}
    assert set(by_subject) == {"kalshi:market:KX-A", "kalshi:market:KX-B"}
    for subject, ex in by_subject.items():
        assert ex.response_body == BOOK
        assert ex.complete["status"] == 200 and ex.failure is None
        assert ex.start["path"] == f"/markets/{subject.rsplit(':', 1)[1]}/orderbook"
    first_completed = min(by_subject.values(), key=lambda ex: ex.complete["complete_ts_ms"])
    assert first_completed.subjects == ["kalshi:market:KX-B"]
    assert verify(archive.root) == []


async def test_timeout_keeps_request_identity_and_has_no_body(archive, server):
    async with aiohttp.ClientSession() as http:
        rest = RestRecorder(http, archive.stream("kalshi", "kalshi-test"), timeout_s=0.3)
        res = await rest.request("GET", base(server), "/slow", subjects=["kalshi:market:SLOW"])
    archive.close()
    assert res.failure == "timeout" and res.body is None
    ex = rest_exchanges(iter_archive(archive.root))[res.request_id]
    assert ex.subjects == ["kalshi:market:SLOW"]
    assert ex.response_body is None
    assert ex.complete["failure"] == "timeout" and ex.complete["status"] is None


async def test_html_error_body_preserved_verbatim(archive, server):
    async with aiohttp.ClientSession() as http:
        rest = RestRecorder(http, archive.stream("novig", "rest-test"), timeout_s=2)
        res = await rest.request("GET", base(server), "/forbidden")
    archive.close()
    ex = rest_exchanges(iter_archive(archive.root))[res.request_id]
    assert ex.complete["status"] == 403 and ex.response_body == HTML_403
    assert ("Content-Type", "text/html; charset=utf-8") in map(tuple, ex.complete["response_headers"])


async def test_secrets_never_reach_the_archive(archive, server):
    def signer(method, path, query, body):
        return {"Novig-Key-Id": "key-id-ok", "Novig-Timestamp": "1",
                "Novig-Signature": SECRET + "-sig"}

    async with aiohttp.ClientSession() as http:
        rest = RestRecorder(http, archive.stream("odds_api", "odds-test"), timeout_s=2)
        await rest.request("GET", base(server), "/v4/sports/baseball_mlb/odds",
                           params={"apiKey": SECRET, "regions": "us"},
                           headers={"Authorization": f"Bearer {SECRET}", "Cookie": SECRET},
                           signer=signer)
    archive.close()
    raw = b"".join(gzip.decompress(p.read_bytes()) for p in archive.root.rglob("*.jsonl.gz"))
    assert SECRET.encode() not in raw
    assert b"regions=us" in raw and b"key-id-ok" in raw   # non-secret metadata is kept


async def test_odds_poller_stops_below_quota_floor(archive, server):
    section = {"base_url": base(server), "sport": "baseball_mlb", "regions": "us",
               "markets": "h2h", "odds_format": "american", "key_env": "ODDS_API_KEY"}
    async with aiohttp.ClientSession() as http:
        stream = archive.stream("odds_api", "odds-test")
        poller = OddsPoller(RestRecorder(http, stream, timeout_s=2), stream, section,
                            api_key="k", quota_floor=50)
        for _ in range(4):
            await poller.poll()
    archive.close()
    frames = list(iter_archive(archive.root))
    calls = [f for f in frames if f["dir"] == "event" and json.loads(f["frame"])["type"] == "rest_request"]
    stops = [json.loads(f["frame"]) for f in frames
             if f["dir"] == "event" and json.loads(f["frame"])["type"] == "quota_floor_stop"]
    assert len(calls) == 3              # responses report 51, 50, then 49 < floor: stop
    assert len(stops) == 1 and stops[0]["remaining"] == 49 and stops[0]["floor"] == 50
    assert poller.stopped


def test_odds_poller_without_key_records_why(archive):
    stream = archive.stream("odds_api", "odds-test")
    poller = OddsPoller(None, stream, {"key_env": "ODDS_API_KEY"}, api_key=None, quota_floor=50)
    archive.close()
    assert poller.stopped
    ev = [json.loads(f["frame"]) for f in iter_archive(archive.root)]
    assert ev[0]["type"] == "odds_disabled" and "ODDS_API_KEY" in ev[0]["reason"]


REVIEW_SECRET = "DUMMY_REVIEW_SECRET"


async def test_exception_text_never_carries_query_secrets(archive):
    """Review finding 3: ClientResponseError's str() embeds the request URL."""
    async def bad_response(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: abc\r\n\r\nbody")
        await writer.drain()
        writer.close()

    srv = await asyncio.start_server(bad_response, "127.0.0.1", 0)
    port = srv.sockets[0].getsockname()[1]
    try:
        async with aiohttp.ClientSession() as http:
            rest = RestRecorder(http, archive.stream("odds_api", "odds-test"), timeout_s=2)
            res = await rest.request("GET", f"http://127.0.0.1:{port}", "/v4/sports/x/odds",
                                     params={"apiKey": REVIEW_SECRET, "regions": "us"})
    finally:
        srv.close()
        await srv.wait_closed()
    archive.close()
    assert res.failure is not None and "ClientResponseError" in res.failure
    assert REVIEW_SECRET not in res.failure
    raw = b"".join(gzip.decompress(p.read_bytes()) for p in archive.root.rglob("*.jsonl.gz"))
    assert REVIEW_SECRET.encode() not in raw
    assert b"regions=us" in raw


def test_scrub_redacts_urls_and_known_values_only():
    from raw_recorder import redact
    text = ("400, message='Invalid header', url='https://api.example.com/v4/odds?apiKey=s3cr3t&regions=us' "
            "and wss://x.test/ws?token=abcd plus bare s3cr3t and harmless words")
    out = redact.scrub(text, {"s3cr3t"})
    assert "s3cr3t" not in out and "abcd" not in out
    assert "regions=us" in out and "harmless words" in out and "https://api.example.com/v4/odds?" in out
    assert redact.scrub("nothing secret here", {"ab"}) == "nothing secret here"   # short values ignored
    assert redact.secret_values("apiKey=k1&x=1", {"Novig-Signature": "sig", "Accept": "a"}) == {"k1", "sig"}


async def test_cancelled_request_still_writes_rest_complete(archive):
    """Review finding 4: shutdown cancels an in-flight body read."""
    headers_sent = asyncio.Event()

    async def stalled(request):
        resp = web.StreamResponse(status=200, headers={"Content-Length": "100"})
        await resp.prepare(request)
        await resp.write(b"partial")
        headers_sent.set()
        await asyncio.sleep(10)
        return resp

    app = web.Application()
    app.router.add_get("/stalled", stalled)
    srv = TestServer(app)
    await srv.start_server()
    try:
        async with aiohttp.ClientSession() as http:
            rest = RestRecorder(http, archive.stream("kalshi", "kalshi-test"), timeout_s=30)
            task = asyncio.create_task(rest.request("GET", base(srv), "/stalled",
                                                    subjects=["kalshi:market:X"]))
            await headers_sent.wait()
            await asyncio.sleep(0.1)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
    finally:
        await srv.close()
    archive.close()
    (ex,) = rest_exchanges(iter_archive(archive.root)).values()
    assert ex.complete is not None
    assert ex.complete["failure"] == "cancelled" and ex.complete["status"] == 200
    assert ex.subjects == ["kalshi:market:X"]
    assert verify(archive.root) == []
