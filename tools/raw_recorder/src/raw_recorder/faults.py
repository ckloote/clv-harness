"""Gate A1 fault injection (DESIGN.md §13; docs/decisions/2026-10-16-a1-scope.md §7).

Three parts, each in its own process, so killing one never takes another with it:

* **The relay** (`raw-recorder relay`): a WebSocket relay on loopback between the
  recorder and Novig; point `[novig] ws_url` at it. NOVIG-V3 signs the method, path,
  query and body, never the host, so the recorder's signed upgrade passes through as
  it is. Every frame is forwarded unchanged both ways (text byte for byte, and Ping,
  Pong and Close), and the relay archives nothing. On command (`POST /_relay/fault`,
  from loopback only) it causes the faults Novig cannot be made to cause:

      socket_kill      abort the recorder's TCP connections (all, or one channel's)
      channel_stall    for N seconds, drop every delta frame naming a market's channel
      sequence_skip    drop the next delta frame naming a market's channel

  A frame is dropped whole, never edited: one that also names other markets drops
  their deltas too, and the report lists every market, channel and seq the frame
  carried. A frame carrying a lifecycle delta is never dropped, since a lost GOLIVE
  has no sequence to expose it; it is counted as spared. Replies to the recorder's own
  requests (they carry its nonce) always pass, so a stalled channel's probes still
  answer, as they would if only the venue's delta feed had stalled.

* **The recorder's control socket** (`[faults] enabled`): a Unix socket, by default
  `<archive>/.recorder.control`, that only the recorder's user can open. It takes one
  JSON request per connection:

      label        archive `fault_injected`, with the fault, its target and every
                   Novig connection's confirmed markets at that moment, and fsync it
                   before answering
      resubscribe  unsubscribe and resubscribe one market on each connection that has it
      result       archive `fault_applied` with the report, or `fault_failed`

* **The harness** (`raw-recorder fault <kind>`): labels the fault, causes it, and
  archives the result, so `clv gate a1` can tell an injected gap from a real one and
  knows exactly what to expect. A process kill is SIGKILL to the recorder's own PID
  (from the label's answer); its result goes to the next session's control socket,
  once the service has restarted the recorder.
"""
from __future__ import annotations

import asyncio
import ipaddress
import json
import os
import signal
import stat
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
from aiohttp import web
from multidict import CIMultiDict
from yarl import URL

from .archive import Stream, now_ms
from .novig.stream import CHANNEL_WEIGHT, IO_ERRORS, NovigStream

KINDS = ("socket_kill", "process_kill", "channel_stall", "sequence_skip", "resubscribe")
RELAY_KINDS = ("socket_kill", "channel_stall", "sequence_skip")
SEQUENCED = tuple(ch for ch in CHANNEL_WEIGHT if ch != "lifecycle")
FAULT_PATH = "/_relay/fault"
SUN_PATH_MAX = 107                  # bytes in a Unix socket path, less the terminating NUL (Linux)
# The longest control request or answer, one JSON line: a stall's result lists every dropped frame
# (about 80 bytes each), so asyncio's 64 KiB default is a few seconds of a busy market.
CONTROL_LINE_MAX = 64 * 1024 * 1024
# Request headers that belong to the hop, not the request: the relay's own upgrade replaces them.
HOP_HEADERS = frozenset({"host", "upgrade", "connection", "sec-websocket-key", "sec-websocket-version",
                         "sec-websocket-extensions", "sec-websocket-protocol", "sec-websocket-accept",
                         "content-length", "transfer-encoding", "keep-alive"})


class FaultError(RuntimeError):
    pass


def socket_path(path: Path) -> str:
    text = str(path)
    if len(text.encode()) > SUN_PATH_MAX:
        raise FaultError(f"{text} is longer than a Unix socket path may be ({SUN_PATH_MAX} bytes): "
                         "set [faults] control_socket to a shorter absolute path")
    return text


def delta_routes(text: str) -> dict[str, dict[str, int | None]] | None:
    """For a delta frame (no nonce), each market it names and the seq of each sequenced
    channel it carries, plus `lifecycle` if it carries one; None for any other frame."""
    try:
        obj = json.loads(text)
    except ValueError:
        return None
    if not isinstance(obj, dict) or "nonce" in obj or not isinstance(obj.get("delta"), dict):
        return None
    out = {}
    for market, body in obj["delta"].items():
        if isinstance(body, dict):
            out[market] = {ch: (b.get("seq") if isinstance(b, dict) else None)
                           for ch, b in body.items() if ch in CHANNEL_WEIGHT}
    return out


# --------------------------------------------------------------------------
# The relay
# --------------------------------------------------------------------------

@dataclass
class _Drop:
    """An armed drop: until a loop time (a stall), or for one frame (a skip)."""
    market: str
    channel: str
    until: float | None
    done: asyncio.Future
    dropped: list = field(default_factory=list)
    spared: int = 0


@dataclass
class _Link:
    n: int
    request: web.Request
    client: web.WebSocketResponse
    upstream: aiohttp.ClientWebSocketResponse
    channel: str | None = None          # from the recorder's first subscribe: one channel per connection


class Relay:
    def __init__(self, upstream: str):
        self.upstream = upstream.rstrip("/")
        self.links: dict[int, _Link] = {}
        self.drops: list[_Drop] = []
        self.http: aiohttp.ClientSession | None = None
        self._n = 0

    def app(self) -> web.Application:
        app = web.Application()
        app.router.add_post(FAULT_PATH, self.fault)
        app.router.add_get("/{path:.*}", self.relay)
        app.on_startup.append(self._start)
        app.on_cleanup.append(self._stop)
        return app

    async def _start(self, app) -> None:
        self.http = aiohttp.ClientSession()

    async def _stop(self, app) -> None:
        if self.http is not None:
            await self.http.close()

    async def relay(self, request: web.Request) -> web.StreamResponse:
        headers = CIMultiDict((k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS)
        try:
            # The path and query as sent, never re-encoded: the signature covers them verbatim.
            up = await self.http.ws_connect(URL(self.upstream + request.raw_path, encoded=True), headers=headers,
                                            autoping=False, autoclose=False, max_msg_size=0, compress=0)
        except aiohttp.WSServerHandshakeError as exc:
            # The venue's refusal goes back to the recorder as its own handshake failure.
            return web.Response(status=exc.status, text=str(exc.message))
        except IO_ERRORS as exc:
            return web.Response(status=502, text=f"relay: upstream unreachable: {type(exc).__name__}: {exc}")
        client = web.WebSocketResponse(autoping=False, autoclose=False, max_msg_size=0, compress=False)
        await client.prepare(request)
        self._n += 1
        link = self.links[self._n] = _Link(self._n, request, client, up)
        try:
            await self._pump(link)
        finally:
            del self.links[link.n]
            for ws in (up, client):
                if not ws.closed:
                    try:
                        await asyncio.wait_for(ws.close(), 5)
                    except (asyncio.TimeoutError, *IO_ERRORS):
                        pass
        return client

    async def _pump(self, link: _Link) -> None:
        async def down() -> None:                   # venue -> recorder
            while True:
                msg = await link.upstream.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if not self._dropped(msg.data):
                        await link.client.send_str(msg.data)
                elif not await _forward(msg, link.client):
                    return

        async def up() -> None:                     # recorder -> venue
            while True:
                msg = await link.client.receive()
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if link.channel is None:
                        link.channel = _subscribed_channel(msg.data)
                    await link.upstream.send_str(msg.data)
                elif not await _forward(msg, link.upstream):
                    return

        tasks = {asyncio.create_task(down()), asyncio.create_task(up())}
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    def _dropped(self, text: str) -> bool:
        """Whether an armed drop takes this frame; recorded in the drop's report if so."""
        live = [d for d in self.drops if not d.done.done() and d.market in text]
        if not live:
            return False
        routes = delta_routes(text)
        if not routes:
            return False
        t = asyncio.get_running_loop().time()
        for d in live:
            if (d.until is not None and t >= d.until) or d.channel not in routes.get(d.market, {}):
                continue
            if any("lifecycle" in chans for chans in routes.values()):
                d.spared += 1
                continue
            d.dropped.append({"ts_ms": now_ms(), "markets": {
                m: {ch: seq for ch, seq in chans.items() if ch in SEQUENCED} for m, chans in routes.items()}})
            if d.until is None:
                d.done.set_result(None)
            return True
        return False

    def kill(self, channel: str | None = None) -> list[dict]:
        """Abort the recorder's TCP connections: no Close frame, as a reset socket."""
        killed = []
        for link in list(self.links.values()):
            if channel is not None and link.channel != channel:
                continue
            transport = link.request.transport
            if transport is not None:
                transport.abort()
                killed.append({"link": link.n, "channel": link.channel})
        return killed

    async def fault(self, request: web.Request) -> web.Response:
        if not _loopback(request.remote):
            return web.json_response({"ok": False, "error": "faults are taken from loopback only"}, status=403)
        try:
            req = await request.json()
            kind = req["kind"]
            if kind == "socket_kill":
                killed = self.kill(req.get("channel"))
                rep = {"ok": bool(killed), "applied_ts_ms": now_ms(), "connections": killed}
                if not killed:
                    rep["error"] = "no recorder connection through the relay" + (
                        f" on channel {req['channel']}" if req.get("channel") else "")
                return web.json_response(rep)
            if kind not in ("channel_stall", "sequence_skip"):
                raise ValueError(f"the relay causes {', '.join(RELAY_KINDS)}, not {kind!r}")
            market, channel = str(req["market"]), str(req["channel"])
            if channel not in SEQUENCED:
                raise ValueError(f"channel {channel!r} carries no seq to skip; one of {', '.join(SEQUENCED)}")
            loop = asyncio.get_running_loop()
            stall = kind == "channel_stall"
            seconds = float(req["seconds"]) if stall else float(req.get("timeout_s", 300))
            if seconds <= 0:
                raise ValueError("seconds must be positive")
        except (ValueError, KeyError, TypeError) as exc:
            return web.json_response({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=400)
        d = _Drop(market, channel, loop.time() + seconds if stall else None, loop.create_future())
        start = now_ms()
        self.drops.append(d)
        try:
            await asyncio.wait([d.done], timeout=seconds)
        finally:
            self.drops.remove(d)
            if not d.done.done():
                d.done.cancel()
        # A stall that dropped nothing touched no market, so it is not applied, like a skip that
        # found no delta: clv gate a1 lists it and does not count it, and it can be repeated.
        rep = {"ok": bool(d.dropped), "start_ts_ms": start, "end_ts_ms": now_ms(),
               "market": market, "channel": channel, "dropped": d.dropped, "spared": d.spared}
        if not rep["ok"]:
            rep["error"] = (f"no delta for {market} {channel} during the {seconds:g} s stall" if stall
                            else f"no delta for {market} {channel} within {seconds:g} s")
        return web.json_response(rep)


async def _forward(msg: aiohttp.WSMessage, to) -> bool:
    """Forward one non-text frame; False once the connection is over."""
    t = msg.type
    if t == aiohttp.WSMsgType.PING:
        await to.ping(msg.data)
    elif t == aiohttp.WSMsgType.PONG:
        await to.pong(msg.data)
    elif t == aiohttp.WSMsgType.BINARY:
        await to.send_bytes(msg.data)
    elif t == aiohttp.WSMsgType.CLOSE:
        await to.close(code=msg.data or aiohttp.WSCloseCode.OK, message=(msg.extra or "").encode())
        return False
    else:                                           # CLOSING, CLOSED, ERROR
        return False
    return True


def _subscribed_channel(text: str) -> str | None:
    try:
        markets = json.loads(text)["subscribe"]["markets"]
        return next(iter(markets.values()), None)
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


def _loopback(remote: str | None) -> bool:
    try:
        return remote is not None and ipaddress.ip_address(remote).is_loopback
    except ValueError:
        return False


def split_listen(listen: str) -> tuple[str, int]:
    host, _, port = listen.rpartition(":")
    host = host.strip("[]")
    if not _loopback(host):
        raise FaultError(f"the relay listens on loopback only, not {host!r}")
    return host, int(port)


async def run_relay(listen: str, upstream: str, stop: asyncio.Event | None = None) -> None:
    host, port = split_listen(listen)
    runner = web.AppRunner(Relay(upstream).app())
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    try:
        await (stop or asyncio.Event()).wait()
    finally:
        await runner.cleanup()


# --------------------------------------------------------------------------
# The recorder's control socket
# --------------------------------------------------------------------------

class Control:
    """Serves the fault harness inside the recorder: labels and results go to `rec` (the
    session's recorder stream), resubscribes to the live Novig streams."""

    def __init__(self, rec: Stream, streams: Callable[[], list[NovigStream]]):
        self.rec = rec
        self.streams = streams

    async def serve(self, path: Path) -> None:
        where = socket_path(path)
        if path.is_socket():
            path.unlink()                           # left by a killed session
        elif path.exists():
            raise FaultError(f"{path} exists and is not a socket")
        server = await asyncio.start_unix_server(self.handle, path=where, limit=CONTROL_LINE_MAX)
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
        try:
            async with server:
                await server.serve_forever()
        finally:
            if path.is_socket():
                path.unlink()

    async def handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            req = json.loads(await asyncio.wait_for(reader.readline(), 10))
            reply = await self.dispatch(req)
        except Exception as exc:  # a bad request is answered, never fatal to the recorder
            reply = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        try:
            writer.write(json.dumps(reply).encode() + b"\n")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def dispatch(self, req: dict) -> dict:
        op, ev = req.get("op"), self.rec.event
        if op == "label":
            kind = req["kind"]
            if kind not in KINDS:
                raise ValueError(f"unknown fault {kind!r}")
            subs = [s for st in self.streams() if (s := st.subscriptions()) is not None]
            ts = ev(self.rec.stream_id, "fault_injected", fault_id=str(req["fault_id"]), kind=kind,
                    target=req.get("target") or {}, subscriptions=subs, pid=os.getpid())
            self.rec.fsync()
            return {"ok": True, "ts_ms": ts, "subscriptions": subs, "pid": os.getpid()}
        if op == "resubscribe":
            market = str(req["market"])
            done = [r for st in self.streams() if (r := await st.resubscribe(market)) is not None]
            rep = {"ok": bool(done), "applied_ts_ms": now_ms(), "market": market, "connections": done}
            if not done:
                rep["error"] = f"no Novig connection has {market} confirmed"
            return rep
        if op == "result":
            ok = bool(req.get("ok"))
            ts = ev(self.rec.stream_id, "fault_applied" if ok else "fault_failed", fault_id=str(req["fault_id"]),
                    kind=req.get("kind"), report=req.get("report") or {}, pid=os.getpid())
            self.rec.fsync()
            return {"ok": True, "ts_ms": ts}
        raise ValueError(f"unknown op {op!r}")


# --------------------------------------------------------------------------
# The harness
# --------------------------------------------------------------------------

async def control(path: Path, req: dict, timeout_s: float = 30) -> dict:
    reader, writer = await asyncio.open_unix_connection(socket_path(path), limit=CONTROL_LINE_MAX)
    try:
        writer.write(json.dumps(req).encode() + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), timeout_s)
    finally:
        writer.close()
    if not line:
        raise FaultError(f"{path}: no answer")
    return json.loads(line)


async def relay_fault(relay_url: str, req: dict, timeout_s: float) -> dict:
    async with aiohttp.ClientSession() as http, http.post(
            relay_url.rstrip("/") + FAULT_PATH, json=req, timeout=aiohttp.ClientTimeout(total=timeout_s)) as r:
        return await r.json()


def touched_markets(kind: str, target: dict, subscriptions: list[dict], rep: dict) -> list[str]:
    """Every Novig market a fault touched, as clv gate a1 checks them (its `expectations`, and a
    resubscribe's target). The gate needs each one ingested, so each one's capture window must
    close inside the run. A dropped frame counts whole: it may carry markets besides the target."""
    out: set[str] = set()
    if kind == "process_kill" or (kind == "socket_kill" and rep.get("ok")):
        channel = target.get("channel") if kind == "socket_kill" else None
        out.update(m for s in subscriptions if channel is None or s.get("channel") == channel
                   for m in s.get("markets", ()))
    elif kind in ("channel_stall", "sequence_skip"):
        out.update(m for d in rep.get("dropped", []) for m in d.get("markets", {}))
    elif kind == "resubscribe":
        out.add(target["market"])
    return sorted(out)


async def inject(kind: str, target: dict, *, control_path: Path, relay_url: str,
                 restart_wait_s: float = 180, log: Callable[[str], None] = lambda s: None) -> dict:
    """Label one fault in the running recorder, cause it, and archive what happened.
    Returns the report (`ok`, and what the fault touched), with `touched_markets` added after
    the result is archived."""
    if kind not in KINDS:
        raise FaultError(f"unknown fault {kind!r}")
    fault_id = uuid.uuid4().hex
    label = await control(control_path, {"op": "label", "fault_id": fault_id, "kind": kind, "target": target})
    if not label.get("ok"):
        raise FaultError(f"the recorder refused the label: {label.get('error')}")
    log(f"labelled {kind} {fault_id} at {label['ts_ms']}")
    try:
        if kind in RELAY_KINDS:
            budget = float(target.get("seconds") or target.get("timeout_s") or 0) + 30
            rep = await relay_fault(relay_url, {"kind": kind, **target}, budget)
        elif kind == "resubscribe":
            rep = await control(control_path, {"op": "resubscribe", "market": target["market"]})
        else:                                       # process_kill
            os.kill(int(label["pid"]), signal.SIGKILL)
            rep = {"ok": True, "pid": label["pid"], "killed_ts_ms": now_ms()}
            log(f"killed pid {label['pid']}; waiting up to {restart_wait_s:g} s for the next session")
    except Exception as exc:  # the fault did not happen as labelled: say so in the archive
        rep = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    req = {"op": "result", "fault_id": fault_id, "kind": kind, "ok": bool(rep.get("ok")), "report": rep}
    if kind != "process_kill" or not rep.get("ok"):
        ack = await control(control_path, req)
    else:
        deadline = asyncio.get_running_loop().time() + restart_wait_s
        while True:                                 # the killed session's socket refuses until replaced
            try:
                ack = await control(control_path, req)
                break
            except (ConnectionError, FileNotFoundError, FaultError, asyncio.TimeoutError):
                if asyncio.get_running_loop().time() >= deadline:
                    ack = {"ok": False, "error": f"no recorder answered on {control_path} within {restart_wait_s:g} s"}
                    break
                await asyncio.sleep(1)
    if not ack.get("ok"):
        # The fault happened but its evidence did not reach the archive: never report success.
        rep["ok"] = False
        rep["error"] = (f"the result is not archived ({ack.get('error')}), so clv gate a1 will fail this fault"
                        + (f"; the fault itself: {rep['error']}" if rep.get("error") else ""))
    rep["fault_id"] = fault_id
    if rep.get("ok"):
        rep["touched_markets"] = touched_markets(kind, target, label.get("subscriptions") or [], rep)
    return rep
