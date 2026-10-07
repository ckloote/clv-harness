"""Novig v3 WebSocket recorder for one public channel (DESIGN.md §3.2, §7.1, §7.2).

Per connection (new conn_id each time; stream_id == conn_id):

* Signed upgrade (`GET /v3/ws`), no compression (text frames replay simply).
* `subscribe` maps each market to this connection's single channel. Every
  frame we send is archived as `out`; every text frame received as `in`.
* Control frames are handled explicitly (`autoping=False`): each received
  Ping is archived, answered with a Pong, and the Pong archived. Transport
  health is not channel evidence.
* Quiet-channel probes: a market whose channel has produced no message for
  `stream.channel_probe_interval_s` gets an authoritative `snapshot` request
  (the reply echoes our nonce). Probes cost the same tokens as a subscribe
  (book 16, trades 4 per market; the `stream` bucket refills 4/s), so they
  never draw the shared bucket below `r0.stream_probe_reserve_fraction` of
  capacity; a probe that cannot be afforded is archived as `probe_skipped`.
* Disconnects reconnect with backoff and resubscribe. No sequence
  interpretation or gap repair: later replay finds gaps from raw `seq`.

The only parsing here is routing: the top-level market IDs of a message, to
know which channels are quiet.
"""
from __future__ import annotations

import asyncio
import base64
import json
import random
import time
import traceback
import uuid
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

import aiohttp
from yarl import URL

from .. import redact
from ..archive import ArchiveWriter, Stream, now_ms
from ..config import Params
from .signing import NovigSigner

CHANNEL_WEIGHT = {"lifecycle": 1, "trades": 4, "bbo": 8, "book": 16}
UPGRADE_COST = 32
# Failures of an established socket (a write to a reset transport, a dropped
# connection): recorded, then the connection is replaced.
IO_ERRORS = (aiohttp.ClientError, ConnectionError, OSError, asyncio.TimeoutError)


class TokenBudget:
    """Client-side model of the per-key `stream` token bucket.

    Novig counts throttles per key and shares counts between servers
    gradually, so this is an estimate (docs.novig.com/api/throttling); its
    job is to keep probing from starving subscribes and reconnects.
    """

    def __init__(self, capacity: float = 512, refill_per_s: float = 4,
                 clock: Callable[[], float] = time.monotonic):
        self.clock = clock
        self.configure(capacity, refill_per_s)

    def configure(self, capacity: float, refill_per_s: float) -> None:
        self.capacity = float(capacity)
        self.refill_per_s = float(refill_per_s)
        self._tokens = self.capacity
        self._t = self.clock()

    def available(self) -> float:
        now = self.clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._t) * self.refill_per_s)
        self._t = now
        return self._tokens

    def spend(self, cost: float) -> None:
        self.available()
        self._tokens -= cost


class NovigStream:
    def __init__(self, *, http: aiohttp.ClientSession, archive: ArchiveWriter,
                 signer: NovigSigner, ws_url: str, channel: str,
                 markets_fn: Callable[[], frozenset[str]], params: Params,
                 budget: TokenBudget,
                 on_handshake_failure: Callable[[], Awaitable[None]] | None = None,
                 mono: Callable[[], float] = time.monotonic):
        if channel not in CHANNEL_WEIGHT:
            raise ValueError(f"unknown public channel {channel!r}")
        self.http = http
        self.archive = archive
        self.signer = signer
        self.ws_url = ws_url
        self.ws_path = urlsplit(ws_url).path
        self.channel = channel
        self.weight = CHANNEL_WEIGHT[channel]
        self.markets_fn = markets_fn
        self.params = params
        self.budget = budget
        self.on_handshake_failure = on_handshake_failure
        self.mono = mono
        self.reserve = budget.capacity * params.stream_probe_reserve_fraction

    # ------------------------------------------------------------------ run

    async def run(self) -> None:
        """Runs until cancelled; no failure of one connection ends it."""
        backoff = self.params.reconnect_backoff_initial_s
        while True:
            if not self.markets_fn():
                await asyncio.sleep(1)
                continue
            started = self.mono()
            conn_id = str(uuid.uuid4())
            stream = self.archive.stream("novig", conn_id)
            try:
                await self._connection(stream, conn_id)
            except asyncio.CancelledError:
                self.archive.release(stream)
                raise
            except Exception as exc:  # a bug must not leave the channel silently dead
                stream.event(conn_id, "ws_unexpected_error", error_type=type(exc).__name__,
                             error=redact.scrub(str(exc)),
                             traceback=redact.scrub(traceback.format_exc()))
            if self.mono() - started >= self.params.reconnect_backoff_max_s:
                backoff = self.params.reconnect_backoff_initial_s
            if not self.markets_fn():
                self.archive.release(stream)
                continue
            delay = backoff * (0.5 + random.random() / 2)
            stream.event(stream.stream_id, "reconnect_wait", delay_s=round(delay, 3))
            self.archive.release(stream)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, self.params.reconnect_backoff_max_s)

    async def _connection(self, stream: Stream, conn_id: str) -> None:
        """One connection attempt and its lifetime, archived into `stream`."""
        desired = self.markets_fn()
        stream.event(conn_id, "ws_connect_attempt", url=self.ws_url, channel=self.channel,
                     markets=sorted(desired))
        self.budget.spend(UPGRADE_COST)
        try:
            ws = await self.http.ws_connect(
                URL(self.ws_url), headers=self.signer("GET", self.ws_path, "", b""),
                autoping=False, autoclose=False, max_msg_size=0, compress=0,
            )
        except aiohttp.WSServerHandshakeError as exc:
            stream.event(conn_id, "ws_handshake_failed", status=exc.status,
                         response_headers=redact.headers(list((exc.headers or {}).items())),
                         message=redact.scrub(str(exc.message)))
            if self.on_handshake_failure is not None:
                # The handshake error drops the body; a signed REST request
                # through the envelope captures the refusal body as evidence.
                await self.on_handshake_failure()
            return
        except IO_ERRORS as exc:
            stream.event(conn_id, "ws_connect_failed", error_type=type(exc).__name__,
                         error=redact.scrub(str(exc)))
            return

        resp = getattr(ws, "_response", None)
        stream.event(conn_id, "ws_connected", status=getattr(resp, "status", 101),
                     response_headers=redact.headers(list(resp.headers.items())) if resp else [])
        conn = _Connection(self, ws, stream, conn_id)
        try:
            await conn.serve(desired)
        except IO_ERRORS as exc:
            stream.event(conn_id, "ws_io_error", error_type=type(exc).__name__,
                         error=redact.scrub(str(exc)))
            conn.close_reason = "io_error"
        finally:
            await conn.close()


class _Connection:
    def __init__(self, owner: NovigStream, ws: aiohttp.ClientWebSocketResponse,
                 stream: Stream, conn_id: str):
        self.o = owner
        self.ws = ws
        self.stream = stream
        self.conn_id = conn_id
        self.nonce = 0
        self.markets: set[str] = set()
        self.last_activity: dict[str, float] = {}
        self.last_probe: dict[str, float] = {}
        self.opened = owner.mono()
        self.close_reason: str | None = None

    async def send(self, payload: dict) -> int:
        self.nonce += 1
        text = json.dumps({"nonce": self.nonce, **payload}, separators=(",", ":"))
        await self.ws.send_str(text)
        self.stream.write("out", self.conn_id, text)
        return self.nonce

    async def subscribe(self, markets: set[str]) -> None:
        if markets:
            await self.send({"subscribe": {"markets": {m: self.o.channel for m in sorted(markets)}}})
            self.o.budget.spend(self.o.weight * len(markets))
            now = self.o.mono()
            for m in markets:
                self.last_probe[m] = now  # the subscribe snapshot is this interval's evidence
            self.markets |= markets

    async def unsubscribe(self, markets: set[str]) -> None:
        if markets:
            await self.send({"unsubscribe": [f"market:{m}" for m in sorted(markets)]})
            self.o.budget.spend(len(markets))
            self.markets -= markets

    def note_activity(self, text: str) -> None:
        try:
            obj = json.loads(text)
        except ValueError:
            return
        if not isinstance(obj, dict):
            return
        now = self.o.mono()
        for key in ("snapshot", "delta"):
            body = obj.get(key)
            if isinstance(body, dict):
                for market_id, payload in body.items():
                    if isinstance(payload, dict) and self.o.channel in payload:
                        self.last_activity[market_id] = now

    async def serve(self, desired: frozenset[str]) -> None:
        """Receive and maintain until either stops; a failure in either one
        (e.g. a write to a reset socket) ends the connection and propagates."""
        await self.subscribe(set(desired))
        tasks = {asyncio.create_task(self.receive_loop()), asyncio.create_task(self.maintain())}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        for t in done:
            t.result()

    async def receive_loop(self) -> None:
        ev = self.stream.event
        cid = self.conn_id
        while True:
            try:
                msg = await self.ws.receive(timeout=self.o.params.transport_liveness_max_s)
            except asyncio.TimeoutError:
                ev(cid, "transport_silence", seconds=self.o.params.transport_liveness_max_s)
                self.close_reason = "transport_silence"
                return
            ts = now_ms()
            t = msg.type
            if t == aiohttp.WSMsgType.TEXT:
                self.stream.write("in", cid, msg.data, recv_ts_ms=ts)
                self.note_activity(msg.data)
            elif t == aiohttp.WSMsgType.PING:
                ev(cid, "ws_control", op="ping", direction="in",
                   payload_hex=bytes(msg.data).hex(), recv_ts_ms=ts)
                await self.ws.pong(msg.data)
                ev(cid, "ws_control", op="pong", direction="out", payload_hex=bytes(msg.data).hex())
            elif t == aiohttp.WSMsgType.PONG:
                ev(cid, "ws_control", op="pong", direction="in",
                   payload_hex=bytes(msg.data).hex(), recv_ts_ms=ts)
            elif t == aiohttp.WSMsgType.BINARY:
                ev(cid, "ws_binary", payload_b64=base64.b64encode(msg.data).decode("ascii"),
                   recv_ts_ms=ts)
            elif t == aiohttp.WSMsgType.CLOSE:
                ev(cid, "ws_close_received", code=msg.data, reason=msg.extra, recv_ts_ms=ts)
                self.close_reason = "server_close"
                return
            elif t in (aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                ev(cid, "ws_closed", code=self.ws.close_code)
                self.close_reason = self.close_reason or "closed"
                return
            elif t == aiohttp.WSMsgType.ERROR:
                ev(cid, "ws_error", error=repr(self.ws.exception()))
                self.close_reason = "error"
                return

    async def maintain(self) -> None:
        """Once a second: follow the desired market set and probe quiet channels."""
        interval = self.o.params.channel_probe_interval_s
        while True:
            await asyncio.sleep(1)
            desired = set(self.o.markets_fn())
            if not desired:
                self.close_reason = "no_active_markets"
                await self.ws.close()
                return
            await self.unsubscribe(self.markets - desired)
            await self.subscribe(desired - self.markets)

            now = self.o.mono()
            due = sorted(m for m in self.markets
                         if now - max(self.last_activity.get(m, float("-inf")),
                                      self.last_probe.get(m, float("-inf"))) >= interval)
            if not due:
                continue
            for m in due:
                self.last_probe[m] = now
            tokens = self.o.budget.available()
            affordable = max(0, int((tokens - self.o.reserve) // self.o.weight))
            send, skip = due[:affordable], due[affordable:]
            if send:
                nonce = await self.send({"snapshot": {"markets": {m: self.o.channel for m in send}}})
                self.o.budget.spend(self.o.weight * len(send))
                self.stream.event(self.conn_id, "probe_sent", nonce=nonce, channel=self.o.channel,
                                  markets=send, est_tokens_before=round(tokens, 1))
            if skip:
                self.stream.event(self.conn_id, "probe_skipped", reason="stream_budget",
                                  channel=self.o.channel, markets=skip,
                                  est_tokens=round(tokens, 1), reserve=self.o.reserve)

    async def close(self) -> None:
        if not self.ws.closed:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=5)
            except (asyncio.TimeoutError, aiohttp.ClientError, OSError):
                pass
        self.stream.event(self.conn_id, "ws_disconnected", reason=self.close_reason or "recorder_stop",
                          close_code=self.ws.close_code,
                          duration_s=round(self.o.mono() - self.opened, 3))
