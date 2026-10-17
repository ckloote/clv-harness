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
                channel_probe_interval_s=1, quiet_probe_interval_s=1, dense_probe_lead_s=900,
                dense_probe_tail_s=1800, transport_liveness_max_s=5, probe_timeout_s=1,
                subscription_check_interval_s=60,
                reconnect_backoff_initial_s=0.05, reconnect_backoff_max_s=0.2,
                a1_capture_lead_s=90000, a1_capture_tail_s=14400)
    base.update(overrides)
    return Params(**base)


def snapshot_reply(nonce, markets, channel, subscribed=False, seq=7):
    """A snapshot reply; a subscribe's also lists what it subscribed, as Production does."""
    msg = {"ts": 1, "nonce": nonce}
    if subscribed:
        msg["subscribed"] = {"markets": {m: channel for m in markets}, "events": {}, "private": []}
    msg["snapshot"] = {m: {"eventId": "e1", channel: {"seq": seq, "orders": {}}, "lifecycle": {"seq": 1, "status": "OPEN"}}
                       for m in markets}
    return json.dumps(msg)


def make_app(state, close_after_subscribe=False, golive_at_s=None, busy_feed=False):
    async def busy(ws, market):
        """A book delta every 0.1 s, so the channel is never quiet, and GOLIVE once. A seq in
        state["skip"] is never sent, as if lost on the way."""
        seq, t0 = 7, asyncio.get_running_loop().time()
        while not ws.closed:
            await asyncio.sleep(0.1)
            seq += 1
            state["seq"] = seq
            if seq in state["skip"]:
                continue
            await ws.send_str(json.dumps({"ts": seq, "delta": {market: {"eventId": "e1", "book": {"seq": seq, "deltas": []}}}}))
            if golive_at_s is not None and asyncio.get_running_loop().time() - t0 >= golive_at_s:
                await ws.send_str(json.dumps({"ts": seq, "delta": {market: {"eventId": "e1", "lifecycle": {
                    "seq": 2, "deltas": [{"kind": "GOLIVE", "status": "OPEN"}]}}}}))
                return

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
        feeder = None
        subs: dict[str, str] = {}               # this connection's subscriptions, as the venue holds them
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.PONG:
                    state["pongs"].append(msg.data)
                elif msg.type == aiohttp.WSMsgType.TEXT:
                    req = json.loads(msg.data)
                    state["requests"].append(req)
                    if "subscribe" in req and state.get("on_subscribe"):
                        override = state["on_subscribe"](req)
                        if override is not None:      # refused or throttled: nothing subscribed
                            await ws.send_str(json.dumps(override))
                            continue
                    if "subscribe" in req:
                        sel = {m: c for m, c in req["subscribe"]["markets"].items()
                               if m not in state["unlisted_once"]}
                        state["unlisted_once"] -= set(req["subscribe"]["markets"])
                        subs.update(sel)
                        if sel:
                            await ws.send_str(snapshot_reply(req["nonce"], sel, next(iter(sel.values())),
                                                             subscribed=True))
                        else:
                            await ws.send_str(json.dumps({"ts": 1, "nonce": req["nonce"], "subscribed": {
                                "markets": {}, "events": {}, "private": []}, "snapshot": {}}))
                    if "snapshot" in req:
                        sel = req["snapshot"]["markets"]
                        await ws.send_str(snapshot_reply(req["nonce"], sel, next(iter(sel.values())),
                                                         seq=7 if state["stale_replies"] else state.get("seq", 7)))
                    if "unsubscribe" in req:
                        for subject in req["unsubscribe"]:
                            subs.pop(subject.removeprefix("market:"), None)
                    if "status" in req:
                        for m in state["forget"]:     # the venue dropped these without telling us
                            subs.pop(m, None)
                        state["forget"] = set()
                        await ws.send_str(json.dumps({"nonce": req["nonce"], "status": {"subscriptions": {
                            "markets": dict(subs), "events": {}, "private": []}}}))
                    if (golive_at_s is not None or busy_feed) and "subscribe" in req:
                        feeder = asyncio.create_task(busy(ws, next(iter(req["subscribe"]["markets"]))))
                    if close_after_subscribe and "subscribe" in req:
                        await ws.close()
        finally:
            task.cancel()
            if feeder:
                feeder.cancel()
        return ws

    app = web.Application()
    app.router.add_get("/v3/ws", ws_handler)
    return app


def new_state(**venue):
    """`venue` sets the fake's behaviour: `on_subscribe(req)` returns a reply that replaces the
    subscribe (nothing is subscribed), `unlisted_once` markets are left out of the first reply
    that asks for them, and `forget` markets are dropped before the next status reply."""
    return {"upgrades": [], "requests": [], "pongs": [], "pings_sent": 0,
            "on_subscribe": None, "unlisted_once": set(), "forget": set(), "skip": set(),
            "stale_replies": False, **venue}


async def record(tmp_path, state, seconds, budget=None, close_after_subscribe=False, golive_at_s=None,
                 markets=frozenset({MARKET}), busy_feed=False, dense=None, **param_overrides):
    srv = TestServer(make_app(state, close_after_subscribe, golive_at_s, busy_feed))
    await srv.start_server()
    archive = ArchiveWriter(tmp_path, session_id="w" * 32, segment_max_s=3600, fsync_interval_s=5)
    try:
        async with aiohttp.ClientSession() as http:
            ws_url = str(srv.make_url("/v3/ws")).replace("http://", "ws://")
            rec = NovigStream(http=http, archive=archive, signer=SIGNER, ws_url=ws_url,
                              channel="book", markets_fn=lambda: frozenset(markets),
                              params=params(**param_overrides), budget=budget or TokenBudget(),
                              dense_fn=None if dense is None else (lambda: frozenset(dense)))
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


async def test_lifecycle_transition_gets_a_book_probe_on_a_busy_channel(tmp_path):
    # The venue drops resting orders at GOLIVE without book deltas: the harness distrusts the
    # book until a snapshot, so the recorder asks for one at once, though the channel is busy.
    state = new_state()
    frames = await record(tmp_path, state, seconds=2.0, golive_at_s=0.5, channel_probe_interval_s=60)
    (conn,) = ws_evidence(frames).values()
    sent = [e for e in conn.events if e["type"] == "probe_sent"]
    assert len(sent) == 1 and sent[0]["markets"] == [MARKET] and sent[0]["channel"] == "book"
    probe = conn.probes[sent[0]["nonce"]]
    golive = next(f for f in frames if f["dir"] == "in" and "GOLIVE" in f["frame"])
    assert probe.reply is not None and 0 < probe.sent_ts_ms - golive["recv_ts_ms"] <= 1500


async def test_a_skipped_seq_gets_a_resync_probe_on_a_busy_channel(tmp_path):
    # A busy channel is never quiet, so without a resync a lost delta would leave its book
    # untrusted until the connection ended: the recorder asks for a snapshot at once.
    state = new_state(skip={12})
    frames = await record(tmp_path, state, seconds=2.0, busy_feed=True, channel_probe_interval_s=60)
    (seen,) = events(frames, "sequence_gap_seen")
    assert (seen["market"], seen["channel"], seen["last_seq"], seen["got_seq"]) == (MARKET, "book", 11, 13)
    (conn,) = ws_evidence(frames).values()
    sent = [e for e in conn.events if e["type"] == "probe_sent"]
    assert len(sent) == 1 and sent[0]["markets"] == [MARKET]        # answered once: no repeat
    probe = conn.probes[sent[0]["nonce"]]
    skipped_to = next(f for f in frames if f["dir"] == "in"
                      and json.loads(f["frame"]).get("delta", {}).get(MARKET, {}).get("book", {}).get("seq") == 13)
    assert probe.reply is not None and 0 < probe.sent_ts_ms - skipped_to["recv_ts_ms"] <= 1500
    assert json.loads(probe.reply)["snapshot"][MARKET]["book"]["seq"] >= 13


async def test_an_unanswered_resync_is_asked_again(tmp_path):
    # The probe reply never catches up (the fake answers seq 7 throughout): the recorder asks again
    # every stream.probe_timeout_s rather than leaving the channel untrusted.
    state = new_state(skip={12}, stale_replies=True)
    frames = await record(tmp_path, state, seconds=2.6, busy_feed=True, channel_probe_interval_s=60,
                          probe_timeout_s=0.5)
    assert len(events(frames, "sequence_gap_seen")) == 1
    (conn,) = ws_evidence(frames).values()
    assert len([e for e in conn.events if e["type"] == "probe_sent"]) >= 2


async def test_a_quiet_market_outside_its_dense_window_is_probed_at_the_slow_interval(tmp_path):
    # Probe policy (docs/decisions/2026-10-17-a1-probe-policy.md): near its start a quiet market is
    # probed every stream.channel_probe_interval_s, elsewhere every stream.quiet_probe_interval_s.
    other = "02b20979-c85a-7773-a716-87ee393b1156"
    state = new_state()
    frames = await record(tmp_path, state, seconds=3.2, markets=frozenset({MARKET, other}), dense={MARKET},
                          channel_probe_interval_s=0.5, quiet_probe_interval_s=60)
    probed = [m for e in events(frames, "probe_sent") for m in e["markets"]]
    assert probed.count(MARKET) >= 2 and other not in probed
    assert all(e["outside_dense"] == [] for e in events(frames, "probe_sent"))


async def test_a_short_bucket_probes_dense_markets_before_quiet_ones(tmp_path):
    # One probe affordable per tick, both markets due every tick: the dense one always goes first.
    other = "02b20979-c85a-7773-a716-87ee393b1156"
    state = new_state()
    frames = await record(tmp_path, state, seconds=3.5, markets=frozenset({MARKET, other}), dense={MARKET},
                          budget=TokenBudget(capacity=56, refill_per_s=40),
                          channel_probe_interval_s=0.5, quiet_probe_interval_s=0.5)
    sent = events(frames, "probe_sent")
    assert sent and all(e["markets"] == [MARKET] for e in sent)
    skipped = events(frames, "probe_skipped")            # the first tick, just after subscribing, affords none
    assert sum(e["markets"] == [other] for e in skipped) >= 2
    assert all(e["outside_dense"] == [m for m in e["markets"] if m == other] for e in skipped)


async def test_disconnect_reconnects_with_new_conn_id_and_resubscribes(tmp_path):
    state = new_state()
    frames = await record(tmp_path, state, seconds=1.0, close_after_subscribe=True)
    ev = ws_evidence(frames)
    # Each connection is its own archive stream, so order them by their first event. The
    # recorder is cancelled at a fixed time, which can land inside a reconnect: only the last
    # connection may be cut short, and its upgrade may or may not have reached the server.
    conns = sorted(ev.values(), key=lambda c: min(e["mono_ns"] for e in c.events))
    assert len(conns) >= 2 and len(conns) - len(state["upgrades"]) in (0, 1)
    for conn in conns[:-1]:
        kinds = [e["type"] for e in conn.events]
        assert kinds[0] == "ws_connect_attempt" and "ws_connected" in kinds
        assert "ws_disconnected" in kinds
    assert conns[-1].events[0]["type"] == "ws_connect_attempt"
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


# --- Subscriptions: affordable, confirmed, checked (A1; DESIGN.md §7.2) ---------------------

def events(frames, kind):
    return [e for c in ws_evidence(frames).values() for e in c.events if e["type"] == kind]


def requests(state, verb):
    return [r for r in state["requests"] if verb in r]


async def test_subscribes_are_batched_to_what_the_bucket_affords(tmp_path):
    markets = frozenset(f"m{i:02d}" for i in range(10))
    # 64 tokens, 32 spent on the upgrade: room for 2 book markets now, then 1 a second.
    budget = TokenBudget(capacity=64, refill_per_s=16)
    state = new_state()
    frames = await record(tmp_path, state, seconds=3.5, budget=budget, markets=markets,
                          channel_probe_interval_s=60)
    subs = [sorted(r["subscribe"]["markets"]) for r in requests(state, "subscribe")]
    assert len(subs[0]) == 2 and len(subs) >= 3
    assert all(len(batch) <= 64 // 16 for batch in subs)
    asked = [m for batch in subs for m in batch]
    assert len(asked) == len(set(asked))                 # nothing asked twice while awaiting a reply
    deferred = events(frames, "subscribe_deferred")
    assert deferred[0]["reason"] == "stream_budget" and len(deferred[0]["markets"]) == 8


async def test_throttled_subscribe_times_out_and_is_retried(tmp_path):
    calls = []

    def throttle_first(req):
        calls.append(req)
        return {"code": "RATE_LIMITED", "message": "slow down"} if len(calls) == 1 else None

    state = new_state(on_subscribe=throttle_first)
    frames = await record(tmp_path, state, seconds=4.0, probe_timeout_s=0.5, channel_probe_interval_s=0.5)
    (err,) = events(frames, "ws_error_reply")
    assert err["code"] == "RATE_LIMITED" and err["nonce"] is None and err["markets"] is None
    (timeout,) = events(frames, "subscribe_timeout")
    assert timeout["markets"] == [MARKET]
    assert len(requests(state, "subscribe")) == 2
    # Probed only once the retry was confirmed.
    probes = [i for i, r in enumerate(state["requests"]) if "snapshot" in r]
    second_sub = [i for i, r in enumerate(state["requests"]) if "subscribe" in r][1]
    assert probes and min(probes) > second_sub


async def test_refused_subscribe_is_never_counted_and_backs_off(tmp_path):
    state = new_state(on_subscribe=lambda req: {"nonce": req["nonce"], "code": "SUBSCRIPTION_LIMIT_EXCEEDED",
                                                "message": "too many"})
    frames = await record(tmp_path, state, seconds=2.5, channel_probe_interval_s=0.5)
    errs = events(frames, "ws_error_reply")
    assert errs and all(e["markets"] == [MARKET] for e in errs)
    assert not requests(state, "snapshot")               # a refused market is never probed as if subscribed
    assert 2 <= len(requests(state, "subscribe")) <= 3   # retried once a second at most, not every reply


async def test_market_left_out_of_the_reply_is_retried(tmp_path):
    other = "01a10979-c85a-7773-a716-000000000002"
    state = new_state(unlisted_once={other})
    frames = await record(tmp_path, state, seconds=2.5, markets=frozenset({MARKET, other}),
                          channel_probe_interval_s=60)
    (unconfirmed,) = events(frames, "subscribe_unconfirmed")
    assert unconfirmed["markets"] == [other]
    subs = [sorted(r["subscribe"]["markets"]) for r in requests(state, "subscribe")]
    assert subs == [sorted([MARKET, other]), [other]]


async def test_status_check_finds_a_lost_subscription_and_resubscribes(tmp_path):
    state = new_state(forget={MARKET})
    frames = await record(tmp_path, state, seconds=3.0, subscription_check_interval_s=0.5,
                          channel_probe_interval_s=60)
    (lost,) = events(frames, "subscription_lost")
    assert lost["markets"] == [MARKET] and lost["channel"] == "book"
    verbs = [next(k for k in r if k != "nonce") for r in state["requests"]]
    first_status = verbs.index("status")
    assert verbs[0] == "subscribe" and "subscribe" in verbs[first_status + 1:]
    assert not events(frames, "subscription_unexpected")
