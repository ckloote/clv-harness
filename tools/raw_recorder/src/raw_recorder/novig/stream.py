"""Novig v3 WebSocket recorder for one public channel (DESIGN.md §3.2, §7.1, §7.2).

Per connection (new conn_id each time; stream_id == conn_id):

* Signed upgrade (`GET /v3/ws`), no compression (text frames replay simply).
* `subscribe` maps each market to this connection's single channel. Every
  frame we send is archived as `out`; every text frame received as `in`.
* Subscribes are sent only in batches the `stream` bucket can afford (one
  request never exceeds a full bucket, which Novig passes only when full);
  the rest wait, archived once as `subscribe_deferred`. A market counts as
  subscribed only when the reply to its own nonce lists it on this channel
  with a snapshot. An unanswered, refused or unconfirmed subscribe is
  archived and retried on the reconnect backoff schedule, per market.
* Every `stream.subscription_check_interval_s` a `status` request lists the
  venue's subscriptions; a market the venue no longer lists is archived as
  `subscription_lost` and resubscribed (DESIGN.md §7.2).
* Control frames are handled explicitly (`autoping=False`): each received
  Ping is archived, answered with a Pong, and the Pong archived. Transport
  health is not channel evidence.
* Quiet-channel probes: a market whose channel has produced no message for
  `stream.channel_probe_interval_s` gets an authoritative `snapshot` request
  (the reply echoes our nonce). Probes cost the same tokens as a subscribe
  (book 16, trades 4 per market; the `stream` bucket refills 4/s), so they
  never draw the shared bucket below `r0.stream_probe_reserve_fraction` of
  capacity; a probe that cannot be afforded is archived as `probe_skipped`.
* Lifecycle probes: on a `book` connection, a lifecycle delta for a market
  (e.g. GOLIVE) gets that market a probe on the next maintenance tick, busy or
  not. The venue drops resting orders at GOLIVE without book deltas, so replay
  distrusts the book from the transition until a snapshot taken after it.
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
        self.markets: set[str] = set()          # confirmed: the reply to their subscribe listed them
        self.confirmed_at: dict[str, float] = {}
        self.pending: dict[int, tuple[frozenset[str], float]] = {}   # subscribe nonce -> (markets, sent)
        self.retry_at: dict[str, float] = {}     # market -> earliest resubscribe after a failure
        self.retry_delay: dict[str, float] = {}
        self.deferred: frozenset[str] = frozenset()      # last archived subscribe_deferred set
        self.status_pending: dict[int, float] = {}       # status nonce -> sent
        self.last_status = owner.mono()
        self.last_activity: dict[str, float] = {}
        self.last_probe: dict[str, float] = {}
        self.transitioned: set[str] = set()     # markets with a lifecycle delta awaiting a book probe
        self.opened = owner.mono()
        self.close_reason: str | None = None

    async def send(self, payload: dict) -> int:
        self.nonce += 1
        text = json.dumps({"nonce": self.nonce, **payload}, separators=(",", ":"))
        await self.ws.send_str(text)
        self.stream.write("out", self.conn_id, text)
        return self.nonce

    def in_flight(self) -> set[str]:
        return {m for asked, _ in self.pending.values() for m in asked}

    async def subscribe(self, wanted: set[str]) -> None:
        """Subscribe the wanted markets that are neither confirmed, awaiting a reply nor
        waiting out a retry delay, as many as the bucket affords now."""
        now, w = self.o.mono(), self.o.weight
        todo = sorted(m for m in wanted - self.markets - self.in_flight()
                      if self.retry_at.get(m, float("-inf")) <= now)
        tokens = self.o.budget.available()
        n = min(len(todo), max(0, int(tokens // w)), int(self.o.budget.capacity // w))
        send, wait = todo[:n], frozenset(todo[n:])
        if send:
            nonce = await self.send({"subscribe": {"markets": {m: self.o.channel for m in send}}})
            self.o.budget.spend(w * len(send))
            self.pending[nonce] = (frozenset(send), now)
        if wait and wait != self.deferred:
            self.stream.event(self.conn_id, "subscribe_deferred", reason="stream_budget",
                              channel=self.o.channel, markets=sorted(wait), est_tokens=round(tokens, 1))
        self.deferred = wait

    def subscribe_failed(self, markets: set[str], now: float) -> None:
        """Retry each market after its own backoff (stream.reconnect_backoff_s), so a market the
        venue keeps refusing cannot drain the bucket."""
        p = self.o.params
        for m in markets:
            delay = self.retry_delay.get(m, p.reconnect_backoff_initial_s)
            self.retry_at[m] = now + delay
            self.retry_delay[m] = min(delay * 2, p.reconnect_backoff_max_s)

    async def unsubscribe(self, markets: set[str]) -> None:
        if markets:
            await self.send({"unsubscribe": [f"market:{m}" for m in sorted(markets)]})
            self.o.budget.spend(len(markets))
            self.markets -= markets
            self.transitioned -= markets
            for m in markets:
                self.confirmed_at.pop(m, None)
                self.retry_at.pop(m, None)
                self.retry_delay.pop(m, None)

    def on_reply(self, obj: dict) -> None:
        """Settle the subscribe or status request a reply answers, by its nonce."""
        ev, cid, ch = self.stream.event, self.conn_id, self.o.channel
        nonce, now = obj.get("nonce"), self.o.mono()
        if "code" in obj and "snapshot" not in obj and "delta" not in obj:
            # An error reply. Without a nonce (a throttled or unparsable request) it names no
            # request: an unanswered subscribe then times out and is retried.
            asked = self.pending.pop(nonce, (None, None))[0] if nonce is not None else None
            self.status_pending.pop(nonce, None)
            ev(cid, "ws_error_reply", code=obj.get("code"), message=redact.scrub(str(obj.get("message"))),
               nonce=nonce, channel=ch, markets=sorted(asked) if asked else None)
            if asked:
                self.subscribe_failed(set(asked), now)
            return
        if nonce in self.pending:
            asked, _ = self.pending.pop(nonce)
            listed = (obj.get("subscribed") or {}).get("markets") or {}
            snap = obj.get("snapshot") or {}
            ok = {m for m in asked if listed.get(m) == ch and m in snap}
            for m in ok:
                self.last_probe[m] = now  # the subscribe snapshot is this interval's evidence
                self.confirmed_at[m] = now
                self.retry_at.pop(m, None)
                self.retry_delay.pop(m, None)
            self.markets |= ok
            if missing := set(asked) - ok:
                ev(cid, "subscribe_unconfirmed", nonce=nonce, channel=ch, markets=sorted(missing))
                self.subscribe_failed(missing, now)
        elif nonce in self.status_pending:
            sent = self.status_pending.pop(nonce)
            listed = ((obj.get("status") or {}).get("subscriptions") or {}).get("markets") or {}
            # Only markets confirmed before the request was sent can be judged by its reply.
            lost = {m for m in self.markets if self.confirmed_at.get(m, now) <= sent and listed.get(m) != ch}
            if lost:
                ev(cid, "subscription_lost", nonce=nonce, channel=ch, markets=sorted(lost))
                self.markets -= lost
                self.transitioned -= lost
                for m in lost:
                    self.confirmed_at.pop(m, None)
            unexpected = {m for m, c in listed.items() if c == ch} - self.markets - self.in_flight() - lost
            if unexpected:
                ev(cid, "subscription_unexpected", nonce=nonce, channel=ch, markets=sorted(unexpected))

    def expire(self, now: float) -> None:
        """Requests unanswered after stream.probe_timeout_s: archived; a subscribe is retried."""
        timeout = self.o.params.probe_timeout_s
        for nonce, (asked, sent) in list(self.pending.items()):
            if now - sent >= timeout:
                del self.pending[nonce]
                self.stream.event(self.conn_id, "subscribe_timeout", nonce=nonce, channel=self.o.channel,
                                  markets=sorted(asked))
                self.subscribe_failed(set(asked), now)
        for nonce, sent in list(self.status_pending.items()):
            if now - sent >= timeout:
                del self.status_pending[nonce]
                self.stream.event(self.conn_id, "status_timeout", nonce=nonce, channel=self.o.channel)

    async def check_subscriptions(self, now: float) -> None:
        if self.markets and now - self.last_status >= self.o.params.subscription_check_interval_s:
            self.last_status = now
            nonce = await self.send({"status": {}})
            self.o.budget.spend(1)
            self.status_pending[nonce] = now

    def note_activity(self, obj: dict) -> None:
        now = self.o.mono()
        for key in ("snapshot", "delta"):
            body = obj.get(key)
            if isinstance(body, dict):
                for market_id, payload in body.items():
                    if not isinstance(payload, dict):
                        continue
                    if self.o.channel in payload:
                        self.last_activity[market_id] = now
                    if key == "delta" and "lifecycle" in payload and self.o.channel == "book":
                        self.transitioned.add(market_id)

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
                try:
                    obj = json.loads(msg.data)
                except ValueError:
                    obj = None
                if isinstance(obj, dict):
                    self.note_activity(obj)
                    self.on_reply(obj)
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
        """Once a second: follow the desired market set, settle unanswered requests, check
        the venue's subscriptions, and probe quiet channels and books after a lifecycle
        transition."""
        interval = self.o.params.channel_probe_interval_s
        while True:
            await asyncio.sleep(1)
            desired = set(self.o.markets_fn())
            if not desired:
                self.close_reason = "no_active_markets"
                await self.ws.close()
                return
            await self.unsubscribe(self.markets - desired)
            now = self.o.mono()
            self.expire(now)
            await self.subscribe(desired)
            await self.check_subscriptions(now)

            due = sorted(m for m in self.markets
                         if m in self.transitioned or now - max(self.last_activity.get(m, float("-inf")),
                                                                self.last_probe.get(m, float("-inf"))) >= interval)
            if not due:
                continue
            for m in due:
                self.last_probe[m] = now
            tokens = self.o.budget.available()
            affordable = max(0, int((tokens - self.o.reserve) // self.o.weight))
            # After a transition the book is untrusted until a probe: those go first, and one
            # that cannot be afforded stays due and is retried next tick.
            due.sort(key=lambda m: m not in self.transitioned)
            send, skip = due[:affordable], due[affordable:]
            self.transitioned -= set(send)
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
