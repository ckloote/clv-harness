"""Gate A1 fault injection (raw_recorder.faults): the relay forwards the venue unchanged and
causes socket kills, channel stalls and sequence skips; the recorder's control socket labels
each fault and records its result; the harness ties them together, process kill included.

The fake venue is test_recorder_ws's: subscribe and snapshot replies, Pings, and (busy_feed) a
book delta every 0.1 s."""
import asyncio
import json
import os
import signal
import time

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from raw_recorder import faults
from raw_recorder.archive import ArchiveWriter, iter_archive
from raw_recorder.novig.stream import NovigStream, TokenBudget
from raw_recorder.replay import ws_evidence
from raw_recorder.runtime import Recorder
from test_recorder_runtime import FakeInhibitor, config, fake_vendors
from test_recorder_ws import MARKET, SIGNER, events, make_app, new_state, params


class Rig:
    """Fake venue <- relay <- one NovigStream per channel, a control socket, and an archive."""

    def __init__(self, tmp_path, state, busy_feed=True, channels=("book",), **param_overrides):
        self.tmp_path, self.state, self.busy_feed = tmp_path, state, busy_feed
        self.channels, self.overrides = channels, param_overrides
        self.control_path = tmp_path / "ctl.sock"

    async def __aenter__(self):
        self.venue = TestServer(make_app(self.state, busy_feed=self.busy_feed))
        await self.venue.start_server()
        self.relay = faults.Relay(str(self.venue.make_url("")).rstrip("/").replace("http://", "ws://"))
        self.relay_srv = TestServer(self.relay.app())
        await self.relay_srv.start_server()
        self.relay_url = str(self.relay_srv.make_url("")).rstrip("/")
        self.archive = ArchiveWriter(self.tmp_path / "archive", session_id="f" * 32, segment_max_s=3600,
                                     fsync_interval_s=5)
        self.rec = self.archive.stream("recorder", "session-ffff")
        self.http = aiohttp.ClientSession()
        self.streams = [NovigStream(http=self.http, archive=self.archive, signer=SIGNER,
                                    ws_url=self.relay_url.replace("http://", "ws://") + "/v3/ws", channel=ch,
                                    markets_fn=lambda: frozenset({MARKET}),
                                    params=params(**{"channel_probe_interval_s": 1, **self.overrides}), budget=TokenBudget())
                        for ch in self.channels]
        self.control = faults.Control(self.rec, lambda: self.streams)
        self.tasks = [asyncio.create_task(s.run()) for s in self.streams]
        self.tasks.append(asyncio.create_task(self.control.serve(self.control_path)))
        await self.until(lambda: all(s.conn and s.conn.markets for s in self.streams))
        return self

    async def until(self, cond, timeout=5.0):
        deadline = time.monotonic() + timeout
        while not cond():
            assert time.monotonic() < deadline, "timed out"
            await asyncio.sleep(0.05)

    async def inject(self, kind, **target):
        return await faults.inject(kind, target, control_path=self.control_path, relay_url=self.relay_url)

    async def __aexit__(self, *exc):
        for t in self.tasks:
            t.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.http.close()
        self.archive.close()
        await self.relay_srv.close()
        await self.venue.close()

    def frames(self):
        return list(iter_archive(self.tmp_path / "archive"))


def recorder_events(frames, kind):
    return [json.loads(f["frame"]) for f in frames
            if f["dir"] == "event" and json.loads(f["frame"])["type"] == kind]


async def test_the_relay_forwards_the_signed_upgrade_and_every_frame_unchanged(tmp_path):
    state = new_state()
    async with Rig(tmp_path, state, busy_feed=False) as rig:
        await asyncio.sleep(1.5)
    up = state["upgrades"][0]
    assert up["Novig-Key-Id"] == "key-1" and "Novig-Signature" in up and "Novig-Timestamp" in up
    frames = rig.frames()
    outs = [json.loads(f["frame"]) for f in frames if f["dir"] == "out"]
    assert outs == state["requests"]                         # the venue got exactly what was archived
    (conn,) = ws_evidence(frames).values()
    pings = [t for t in conn.transport if t["op"] == "ping" and t["direction"] == "in"]
    assert pings and len(state["pongs"]) >= len(pings) - 1   # Pings reach the recorder; its Pongs the venue
    assert conn.probes and all(p.reply is not None for p in conn.probes.values())


async def test_a_sequence_skip_drops_one_delta_and_the_recorder_resyncs(tmp_path):
    async with Rig(tmp_path, new_state(), channel_probe_interval_s=60) as rig:
        cid = rig.streams[0].conn.conn_id
        rep = await rig.inject("sequence_skip", market=MARKET, channel="book", timeout_s=5)
        await asyncio.sleep(1.5)                # the next maintenance tick
    assert rep["ok"] and len(rep["dropped"]) == 1 and rep["spared"] == 0
    (dropped,) = rep["dropped"]
    seq = dropped["markets"][MARKET]["book"]
    frames = rig.frames()
    (seen,) = events(frames, "sequence_gap_seen")
    assert (seen["last_seq"], seen["got_seq"]) == (seq - 1, seq + 1)
    assert [e["markets"] for e in events(frames, "probe_sent")] == [[MARKET]]
    (label,) = recorder_events(frames, "fault_injected")
    (applied,) = recorder_events(frames, "fault_applied")
    assert label["fault_id"] == applied["fault_id"] == rep["fault_id"]
    assert label["kind"] == "sequence_skip" and label["target"] == {"market": MARKET, "channel": "book", "timeout_s": 5}
    assert label["subscriptions"] == [{"conn_id": cid, "channel": "book", "markets": [MARKET]}]
    assert applied["report"]["dropped"] == rep["dropped"]


async def test_a_channel_stall_drops_deltas_for_its_duration_but_passes_replies(tmp_path):
    async with Rig(tmp_path, new_state()) as rig:
        cid = rig.streams[0].conn.conn_id
        rep = await rig.inject("channel_stall", market=MARKET, channel="book", seconds=1.6)
        await asyncio.sleep(0.5)
    assert rep["ok"] and len(rep["dropped"]) >= 10
    assert rep["end_ts_ms"] - rep["start_ts_ms"] >= 1500
    assert all(set(d["markets"]) == {MARKET} for d in rep["dropped"])
    frames = rig.frames()
    # The channel went quiet, so it was probed, and the reply came through the stall.
    conn = ws_evidence(frames)[cid]
    in_stall = [p for p in conn.probes.values() if rep["start_ts_ms"] < p.sent_ts_ms < rep["end_ts_ms"]]
    assert in_stall and all(p.reply is not None for p in in_stall)


async def test_a_socket_kill_aborts_the_connection_and_the_recorder_reconnects(tmp_path):
    async with Rig(tmp_path, new_state(), channels=("book", "trades")) as rig:
        first = {s.channel: s.conn.conn_id for s in rig.streams}
        rep = await rig.inject("socket_kill", channel="book")
        await rig.until(lambda: all(s.conn and s.conn.markets for s in rig.streams)
                        and rig.streams[0].conn.conn_id != first["book"])
        assert rig.streams[1].conn.conn_id == first["trades"]          # the other channel was spared
    assert rep["ok"] and [c["channel"] for c in rep["connections"]] == ["book"]
    frames = rig.frames()
    killed = [e for e in events(frames, "ws_disconnected") if e["reason"] not in ("recorder_stop", "no_active_markets")]
    assert len(killed) == 1                                 # an outage, never a planned close
    (label,) = recorder_events(frames, "fault_injected")
    assert sorted(s["channel"] for s in label["subscriptions"]) == ["book", "trades"]


async def test_a_resubscribe_goes_through_the_recorders_own_connection(tmp_path):
    async with Rig(tmp_path, new_state(), busy_feed=False) as rig:
        cid = rig.streams[0].conn.conn_id
        rep = await rig.inject("resubscribe", market=MARKET)
        await asyncio.sleep(0.3)
        assert MARKET in rig.streams[0].conn.markets and rig.streams[0].conn.conn_id == cid
    assert rep["ok"] and rep["connections"] == [{"conn_id": cid, "channel": "book"}]
    verbs = [next(k for k in json.loads(f["frame"]) if k != "nonce") for f in rig.frames() if f["dir"] == "out"]
    i = verbs.index("unsubscribe")
    assert verbs[0] == "subscribe" and verbs[i + 1] == "subscribe"


async def test_a_resubscribe_of_an_unknown_market_fails_and_says_so(tmp_path):
    async with Rig(tmp_path, new_state(), busy_feed=False) as rig:
        rep = await rig.inject("resubscribe", market="not-subscribed")
    assert not rep["ok"] and "not-subscribed" in rep["error"]
    (failed,) = recorder_events(rig.frames(), "fault_failed")
    assert failed["fault_id"] == rep["fault_id"] and failed["report"]["error"] == rep["error"]


async def test_a_skip_that_finds_no_delta_is_recorded_as_failed(tmp_path):
    async with Rig(tmp_path, new_state(), busy_feed=False) as rig:
        rep = await rig.inject("sequence_skip", market=MARKET, channel="book", timeout_s=0.5)
    assert not rep["ok"] and rep["dropped"] == []
    assert recorder_events(rig.frames(), "fault_failed")


async def test_a_process_kill_result_reaches_the_restarted_recorder(tmp_path, monkeypatch):
    killed = []
    async with Rig(tmp_path, new_state(), busy_feed=False) as rig:
        serving = rig.tasks[-1]

        def kill(pid, sig):
            killed.append((pid, sig))
            serving.cancel()                # the session dies; its socket goes with it

            async def restart():
                await asyncio.sleep(1.2)    # the service manager's restart delay
                await rig.control.serve(rig.control_path)
            rig.tasks.append(asyncio.create_task(restart()))

        monkeypatch.setattr(faults.os, "kill", kill)
        inject = faults.inject("process_kill", {}, control_path=rig.control_path, relay_url=rig.relay_url,
                               restart_wait_s=10)
        rep = await inject
    assert killed == [(os.getpid(), signal.SIGKILL)]
    assert rep["ok"] and rep["pid"] == os.getpid()
    (applied,) = recorder_events(rig.frames(), "fault_applied")
    assert applied["kind"] == "process_kill" and applied["report"]["killed_ts_ms"] == rep["killed_ts_ms"]


async def test_a_long_stalls_result_is_archived_whole(tmp_path, monkeypatch):
    # A stall's result lists every dropped frame; a long one on a busy market runs past asyncio's
    # 64 KiB line limit, and its result must still reach the archive.
    dropped = [{"ts_ms": 1_000 + i, "markets": {MARKET: {"book": i}, "other-market": {"book": i}}}
               for i in range(5_000)]

    async def big_stall(url, req, timeout_s):
        return {"ok": True, "start_ts_ms": 1, "end_ts_ms": 2, "dropped": dropped, "spared": 0}

    monkeypatch.setattr(faults, "relay_fault", big_stall)
    async with Rig(tmp_path, new_state(), busy_feed=False) as rig:
        rep = await rig.inject("channel_stall", market=MARKET, channel="book", seconds=1)
    assert len(json.dumps(rep)) > 4 * 65_536
    assert rep["ok"]
    (applied,) = recorder_events(rig.frames(), "fault_applied")
    assert applied["report"]["dropped"] == dropped


async def test_a_result_the_recorder_does_not_archive_is_never_reported_as_success(tmp_path, monkeypatch):
    dispatch = faults.Control.dispatch

    async def refuse_results(self, req):
        if req.get("op") == "result":
            raise OSError("No space left on device")
        return await dispatch(self, req)

    monkeypatch.setattr(faults.Control, "dispatch", refuse_results)
    async with Rig(tmp_path, new_state(), channel_probe_interval_s=60) as rig:
        rep = await rig.inject("sequence_skip", market=MARKET, channel="book", timeout_s=5)
    assert rep["dropped"] and not rep["ok"]
    assert "not archived" in rep["error"] and "No space left" in rep["error"]
    assert not recorder_events(rig.frames(), "fault_applied")


async def test_the_relay_spares_a_frame_carrying_a_lifecycle_delta():
    relay = faults.Relay("ws://unused")
    loop = asyncio.get_running_loop()
    d = faults._Drop(MARKET, "book", loop.time() + 60, loop.create_future())
    relay.drops.append(d)
    both = json.dumps({"ts": 1, "delta": {MARKET: {"book": {"seq": 9, "deltas": []},
                                                   "lifecycle": {"seq": 2, "deltas": [{"kind": "GOLIVE"}]}}}})
    other = json.dumps({"ts": 1, "delta": {MARKET: {"trades": {"seq": 4, "deltas": []}}}})
    reply = json.dumps({"ts": 1, "nonce": 3, "snapshot": {MARKET: {"book": {"seq": 9, "orders": {}}}}})
    shared = json.dumps({"ts": 1, "delta": {MARKET: {"book": {"seq": 10, "deltas": []}},
                                            "other": {"book": {"seq": 3, "deltas": []}}}})
    assert [relay._dropped(t) for t in (both, other, reply, shared)] == [False, False, False, True]
    assert d.spared == 1
    assert d.dropped[0]["markets"] == {MARKET: {"book": 10}, "other": {"book": 3}}


async def test_the_relay_passes_a_handshake_refusal_through(tmp_path):
    async def refuse(request):
        return web.Response(status=403, text="location check failed")
    app = web.Application()
    app.router.add_get("/v3/ws", refuse)
    venue = TestServer(app)
    await venue.start_server()
    relay = TestServer(faults.Relay(str(venue.make_url("")).rstrip("/").replace("http://", "ws://")).app())
    await relay.start_server()
    try:
        async with aiohttp.ClientSession() as http:
            with pytest.raises(aiohttp.WSServerHandshakeError) as err:
                await http.ws_connect(str(relay.make_url("/v3/ws")).replace("http://", "ws://"))
            assert err.value.status == 403
            async with http.post(relay.make_url(faults.FAULT_PATH), json={"kind": "process_kill"}) as r:
                assert r.status == 400 and not (await r.json())["ok"]
    finally:
        await relay.close()
        await venue.close()


async def test_a_control_socket_path_too_long_for_unix_is_refused_clearly(tmp_path):
    ctl = faults.Control(None, list)
    with pytest.raises(faults.FaultError, match="control_socket"):
        await ctl.serve(tmp_path / ("x" * 120))
    with pytest.raises(faults.FaultError, match="control_socket"):
        await faults.control(tmp_path / ("x" * 120), {"op": "label"})


def test_the_relay_listens_on_loopback_only():
    assert faults.split_listen("127.0.0.1:8765") == ("127.0.0.1", 8765)
    assert faults.split_listen("[::1]:8765") == ("::1", 8765)
    with pytest.raises(faults.FaultError):
        faults.split_listen("0.0.0.0:8765")


async def test_the_control_socket_answers_a_bad_request_and_keeps_serving(tmp_path):
    archive = ArchiveWriter(tmp_path / "archive", session_id="c" * 32, segment_max_s=3600, fsync_interval_s=5)
    path = tmp_path / "ctl.sock"
    path.write_text("")
    ctl = faults.Control(archive.stream("recorder", "session-cccc"), list)
    with pytest.raises(faults.FaultError):
        await ctl.serve(path)                   # a regular file is never replaced
    path.unlink()
    task = asyncio.create_task(ctl.serve(path))
    try:
        while not path.is_socket():
            await asyncio.sleep(0.01)
        assert path.stat().st_mode & 0o777 == 0o600
        assert not (await faults.control(path, {"op": "nonsense"}))["ok"]
        assert not (await faults.control(path, {"op": "label", "fault_id": "x", "kind": "meteor"}))["ok"]
        ok = await faults.control(path, {"op": "label", "fault_id": "x", "kind": "socket_kill"})
        assert ok["ok"] and ok["subscriptions"] == []
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        archive.close()
    assert not path.exists()


async def test_the_recorder_serves_the_control_socket_only_when_faults_are_enabled(tmp_path):
    srv = TestServer(fake_vendors())
    await srv.start_server()
    try:
        for enabled in (False, True):
            cfg = config(tmp_path / str(enabled), str(srv.make_url("")).rstrip("/"))
            cfg = cfg.__class__(**{**cfg.__dict__, "faults": {"enabled": enabled}})
            for section in (cfg.kalshi, cfg.odds_api, cfg.mlb, cfg.novig):
                section["enabled"] = False
            rec = Recorder(cfg, [], inhibitor=FakeInhibitor())
            task = asyncio.create_task(rec.run())
            await asyncio.sleep(0.5)
            path = cfg.control_socket_path
            if enabled:
                assert path.is_socket()
                reply = await faults.control(path, {"op": "label", "fault_id": "f1", "kind": "process_kill"})
                assert reply["ok"]
            else:
                assert not path.exists()
            rec.stop.set()
            await task
            kinds = [json.loads(f["frame"])["type"] for f in iter_archive(cfg.archive_root) if f["dir"] == "event"]
            assert ("fault_control" in kinds) == enabled and ("fault_injected" in kinds) == enabled
    finally:
        await srv.close()
