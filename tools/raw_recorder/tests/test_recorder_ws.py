"""Novig stream recorder against a local fake server (DESIGN.md §7.1 quiet-market fixture).

The fake server sends control Pings, answers subscribe and snapshot with
nonce-tagged snapshots, and never sends a delta: a quiet market. Replay must
keep transport health (Ping/Pong) apart from channel evidence (probe replies).
"""
import asyncio
import json
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder.archive import ArchiveWriter, iter_archive, verify
from raw_recorder.config import Params
from raw_recorder.novig.signing import NovigSigner
from raw_recorder.novig.stream import NovigStream, TokenBudget
from raw_recorder.replay import ws_evidence

SPEC = json.loads((Path(__file__).parent / "fixtures" / "novig_signature_vectors.json").read_text())
SIGNER = NovigSigner.from_pem("key-1", SPEC["keypairs"]["ed25519-test-1"]["private_key_pkcs8_pem"].encode())
MARKET = "01a10979-c85a-7773-a716-87ee393b1156"


def params(**overrides):
    base = dict(kalshi_poll_interval_s=10, odds_poll_interval_s=1200, odds_quota_floor=50,
                capture_lead_s=10800, capture_tail_s=5400, odds_window_lead_s=10800,
                segment_max_s=3600, fsync_interval_s=5, rest_timeout_s=2, min_free_disk_mb=1,
                stream_probe_reserve_fraction=0.5, novig_public_book_poll_interval_s=10,
                channel_probe_interval_s=1, transport_liveness_max_s=5,
                reconnect_backoff_initial_s=0.05, reconnect_backoff_max_s=0.2)
    base.update(overrides)
    return Params(**base)


def snapshot_reply(nonce, markets, channel):
    return json.dumps({"ts": 1, "nonce": nonce, "snapshot": {
        m: {"eventId": "e1", channel: {"seq": 7, "orders": {}}, "lifecycle": {"seq": 1, "status": "OPEN"}}
        for m in markets}})


def make_app(state, close_after_subscribe=False):
    async def ws_handler(request):
        state["upgrades"].append(dict(request.headers))
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)

        async def pinger():
            i = 0
            while not ws.closed:
                await asyncio.sleep(0.3)
                i += 1
                await ws.ping(f"p{i}".encode())
                state["pings_sent"] += 1

        task = asyncio.create_task(pinger())
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.PONG:
                    state["pongs"].append(msg.data)
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    req = json.loads(msg.data)
                    state["requests"].append(req)
                    for verb in ("subscribe", "snapshot"):
                        if verb in req:
                            sel = req[verb]["markets"]
                            await ws.send_str(snapshot_reply(req["nonce"], sel, next(iter(sel.values()))))
                    if close_after_subscribe and "subscribe" in req:
                        await ws.close()
        finally:
            task.cancel()
        return ws

    app = web.Application()
    app.router.add_get("/v3/ws", ws_handler)
    return app


def new_state():
    return {"upgrades": [], "requests": [], "pongs": [], "pings_sent": 0}


async def record(tmp_path, state, seconds, budget=None, close_after_subscribe=False, **param_overrides):
    srv = TestServer(make_app(state, close_after_subscribe))
    await srv.start_server()
    archive = ArchiveWriter(tmp_path, session_id="w" * 32, segment_max_s=3600, fsync_interval_s=5)
    try:
        async with aiohttp.ClientSession() as http:
            ws_url = str(srv.make_url("/v3/ws")).replace("http://", "ws://")
            rec = NovigStream(http=http, archive=archive, signer=SIGNER, ws_url=ws_url,
                              channel="book", markets_fn=lambda: frozenset({MARKET}),
                              params=params(**param_overrides), budget=budget or TokenBudget())
            task = asyncio.create_task(rec.run())
            await asyncio.sleep(seconds)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    finally:
        archive.close()
        await srv.close()
    return list(iter_archive(tmp_path))


async def test_quiet_market_transport_vs_channel_evidence(tmp_path):
    state = new_state()
    frames = await record(tmp_path, state, seconds=3.6)

    # Signed upgrade with the read key, and no compression requested.
    up = state["upgrades"][0]
    assert up["Novig-Key-Id"] == "key-1" and "Novig-Signature" in up
    assert "X-Novig-WS-Compress" not in up

    ev = ws_evidence(frames)
    assert len(ev) == 1
    conn = next(iter(ev.values()))
    pings = [t for t in conn.transport if t["op"] == "ping" and t["direction"] == "in"]
    pongs_out = [t for t in conn.transport if t["op"] == "pong" and t["direction"] == "out"]
    assert len(pings) >= 5
    assert [p["payload_hex"] for p in pings] == [p["payload_hex"] for p in pongs_out]
    assert len(state["pongs"]) == len(pongs_out)            # the server really got every Pong

    # Probes: the quiet market is probed about once per interval after the
    # subscribe snapshot, and every probe reply is matched by nonce.
    assert len(conn.probes) >= 2
    for probe in conn.probes.values():
        assert probe.markets == [MARKET] and probe.channel == "book"
        assert probe.reply is not None and json.loads(probe.reply)["nonce"] == probe.nonce
        assert probe.reply_ts_ms >= probe.sent_ts_ms
    sent = [e for e in conn.events if e["type"] == "probe_sent"]
    assert {e["nonce"] for e in sent} == set(conn.probes)

    # Transport events never count as channel messages and vice versa.
    assert all(json.loads(m["frame"]).get("snapshot") for m in conn.channel_messages)
    assert not any(t.get("type") != "ws_control" for t in conn.transport)

    # Every frame we sent is archived exactly as sent.
    outs = [json.loads(f["frame"]) for f in frames if f["dir"] == "out"]
    assert outs == state["requests"]
    assert outs[0] == {"nonce": 1, "subscribe": {"markets": {MARKET: "book"}}}
    assert verify(tmp_path) == []


async def test_disconnect_reconnects_with_new_conn_id_and_resubscribes(tmp_path):
    state = new_state()
    frames = await record(tmp_path, state, seconds=1.0, close_after_subscribe=True)
    ev = ws_evidence(frames)
    assert len(state["upgrades"]) >= 2 and len(ev) == len(state["upgrades"])
    for conn in ev.values():
        kinds = [e["type"] for e in conn.events]
        assert kinds[0] == "ws_connect_attempt" and "ws_connected" in kinds
        assert "ws_disconnected" in kinds
    subs = [json.loads(f["frame"]) for f in frames if f["dir"] == "out"]
    assert all(s["nonce"] == 1 and "subscribe" in s for s in subs)   # nonces restart per connection
    waits = [e for c in ev.values() for e in c.events if e["type"] == "reconnect_wait"]
    assert waits and all(0 < w["delay_s"] <= 0.2 for w in waits)


async def test_unaffordable_probe_is_recorded_as_skipped(tmp_path):
    state = new_state()
    # Bucket barely above the reserve and refilling slowly: no probe fits.
    budget = TokenBudget(capacity=100, refill_per_s=0.01)
    frames = await record(tmp_path, state, seconds=2.6, budget=budget)
    conn = next(iter(ws_evidence(frames).values()))
    assert not conn.probes
    skipped = [e for e in conn.events if e["type"] == "probe_skipped"]
    assert skipped and skipped[0]["reason"] == "stream_budget" and skipped[0]["markets"] == [MARKET]
    assert not [r for r in state["requests"] if "snapshot" in r]


def fail_once(monkeypatch, method, when=lambda *a: True):
    """Make ClientWebSocketResponse.<method> raise a connection reset once."""
    original = getattr(aiohttp.ClientWebSocketResponse, method)
    state = {"failed": False}

    async def flaky(self, *args, **kwargs):
        if not state["failed"] and when(*args):
            state["failed"] = True
            raise aiohttp.ClientConnectionResetError("Cannot write to closing transport")
        return await original(self, *args, **kwargs)

    monkeypatch.setattr(aiohttp.ClientWebSocketResponse, method, flaky)
    return state


def io_errors(frames):
    return [e for c in ws_evidence(frames).values() for e in c.events if e["type"] == "ws_io_error"]


async def test_subscribe_write_failure_reconnects(tmp_path, monkeypatch):
    """Review finding 2: a reset during the initial subscribe killed the channel."""
    state = new_state()
    failed = fail_once(monkeypatch, "send_str")
    frames = await record(tmp_path, state, seconds=1.0)
    assert failed["failed"]
    assert len(state["upgrades"]) >= 2                     # it came back
    (err,) = io_errors(frames)
    assert err["error_type"] == "ClientConnectionResetError"
    evs = [e for c in ws_evidence(frames).values() for e in c.events]
    assert any(e["type"] == "reconnect_wait" for e in evs)
    assert any(r.get("subscribe") for r in state["requests"])   # resubscribed on the new socket


async def test_pong_write_failure_reconnects(tmp_path, monkeypatch):
    state = new_state()
    failed = fail_once(monkeypatch, "pong")
    frames = await record(tmp_path, state, seconds=1.5)
    assert failed["failed"] and len(state["upgrades"]) >= 2
    assert len(io_errors(frames)) == 1


async def test_probe_write_failure_ends_the_connection_and_reconnects(tmp_path, monkeypatch):
    """A failure inside maintain() used to be swallowed while receiving carried on."""
    state = new_state()
    failed = fail_once(monkeypatch, "send_str", when=lambda text: '"snapshot"' in text)
    frames = await record(tmp_path, state, seconds=3.0)
    assert failed["failed"] and len(state["upgrades"]) >= 2
    assert len(io_errors(frames)) == 1
