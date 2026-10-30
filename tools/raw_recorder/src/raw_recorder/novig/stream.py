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
  `stream.channel_probe_interval_s` inside its dense window (around its scheduled
  start, where every close cutoff falls), or `stream.quiet_probe_interval_s`
  outside it, gets an authoritative `snapshot` request (the reply echoes our
  nonce). A later unchanged probe vouches for the quiet interval before it, so
  the slow cadence loses no coverage (docs/decisions/2026-10-17-a1-probe-policy.md);
  `GOLIVE` breaks that bracket, hence the dense window. When the bucket is short,
  resync and lifecycle probes go first, then dense ones. Probes cost the same tokens as a subscribe
  (book 16, trades 4 per market; the `stream` bucket refills 4/s), so they
  never draw the shared bucket below `r0.stream_probe_reserve_fraction` of
  capacity; a probe that cannot be afforded is archived as `probe_skipped`.
  The venue refuses a whole probe if one of its markets is unknown (a settled
  market, until its capture window ends): the markets of a refused multi-market
  probe are asked again one per request, and a market refused alone as
  `MARKET_NOT_FOUND` is archived as `probe_market_gone` and no longer probed.
* Lifecycle probes: a lifecycle delta for a market (e.g. GOLIVE) on any
  connection, or a snapshot reply whose lifecycle `seq` is ahead of the last one
  that connection saw (a missed transition, archived as `lifecycle_missed_seen`),
  gets the market a book probe on the next maintenance tick, busy or not. The
  venue drops resting orders at a transition without book deltas, so replay
  distrusts every book of the market from it until a snapshot taken after it;
  `on_lifecycle` carries the news to the `book` connection, whichever connection
  heard it (`alert_books`).
* Resync (A1): a delta whose `seq` skips ahead of the last one seen for its
  market on this connection is archived as `sequence_gap_seen`, and the market
  gets a probe on the next tick, busy or not, repeated every
  `stream.probe_timeout_s` until a snapshot at or past the skipped-to `seq`
  arrives. Without it a busy channel, never quiet, would stay untrusted until
  the connection ended. Replay, not the recorder, decides what the gap was.
* Disconnects reconnect with backoff and resubscribe.

The only parsing here is routing: the top-level market IDs of a message and each
channel's `seq`, to know which channels are quiet and which skipped.

The fault harness (raw_recorder.faults, Gate A1) reads `NovigStream.subscriptions()`
and calls `NovigStream.resubscribe()`; nothing else reaches into a connection.
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


def alert_books(streams: list[NovigStream], market: str) -> None:
    """A market's lifecycle changed, or a connection missed a transition: every live `book`
    connection carrying it probes its book on the next tick (`on_lifecycle` for each stream)."""
    for s in streams:
        if s.channel == "book" and s.conn is not None:
            s.conn.transitioned.add(market)


class NovigStream:
    def __init__(self, *, http: aiohttp.ClientSession, archive: ArchiveWriter,
                 signer: NovigSigner, ws_url: str, channel: str,
                 markets_fn: Callable[[], frozenset[str]], params: Params,
                 budget: TokenBudget,
                 on_handshake_failure: Callable[[], Awaitable[None]] | None = None,
                 dense_fn: Callable[[], frozenset[str]] | None = None,
                 on_lifecycle: Callable[[str], None] | None = None,
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
        self.dense_fn = dense_fn                # markets in their dense window; None: every market
        self.on_lifecycle = on_lifecycle        # a market's lifecycle changed or was missed; None: this stream only
        self.mono = mono
        self.reserve = budget.capacity * params.stream_probe_reserve_fraction
        self.conn: _Connection | None = None    # the live connection, for the fault harness

    def subscriptions(self) -> dict | None:
        """The live connection and the markets it has confirmed, or None between connections."""
        c = self.conn
        return None if c is None else {"conn_id": c.conn_id, "channel": self.channel, "markets": sorted(c.markets)}

    async def resubscribe(self, market: str) -> dict | None:
        """Unsubscribe a confirmed market and subscribe it again at once, through the live
        connection, so both commands are archived as `out` frames (Gate A1's resubscribe
        fault). None if no connection has the market confirmed."""
        c = self.conn
        if c is None or market not in c.markets:
            return None
        await c.unsubscribe({market})
        await c.subscribe(set(self.markets_fn()))
        return {"conn_id": c.conn_id, "channel": self.channel}

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
        conn = self.conn = _Connection(self, ws, stream, conn_id)
        try:
            await conn.serve(desired)
        except IO_ERRORS as exc:
            stream.event(conn_id, "ws_io_error", error_type=type(exc).__name__,
                         error=redact.scrub(str(exc)))
            conn.close_reason = "io_error"
        finally:
            self.conn = None
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
        self.seq: dict[str, int] = {}           # market -> the latest seq of this channel seen here
        self.lifecycle_seq: dict[str, int] = {}  # market -> the latest lifecycle seq seen here
        self.resync: set[str] = set()           # markets whose seq skipped: probed until a snapshot catches up
        self.probes: dict[int, tuple[frozenset[str], float]] = {}   # probe nonce -> (markets, sent)
        self.solo: set[str] = set()             # in a refused multi-market probe: next asked one per request
        self.gone: set[str] = set()             # the venue answers MARKET_NOT_FOUND: no longer probed
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

    def probe_failed(self, markets: set[str], code) -> None:
        """The venue refuses a whole probe for one bad market: after a game settles, Paper answers
        MARKET_NOT_FOUND for its market until the capture window ends. A refused probe of several
        markets is asked again at once, one market per request, so the live ones lose no evidence;
        a market refused alone as not found is archived as `probe_market_gone` and probed no more."""
        if len(markets) > 1:
            self.solo |= markets
            for m in markets:
                self.last_probe.pop(m, None)    # due at once
        elif code == "MARKET_NOT_FOUND":
            self.gone |= markets
            self.solo -= markets
            self.stream.event(self.conn_id, "probe_market_gone", channel=self.o.channel, markets=sorted(markets))

    async def unsubscribe(self, markets: set[str]) -> None:
        if markets:
            await self.send({"unsubscribe": [f"market:{m}" for m in sorted(markets)]})
            self.o.budget.spend(len(markets))
            self.markets -= markets
            self.forget(markets)
            for m in markets:
                self.retry_at.pop(m, None)
                self.retry_delay.pop(m, None)

    def forget(self, markets: set[str]) -> None:
        """A subscription ended: its sequence and pending probes go with it."""
        self.transitioned -= markets
        self.resync -= markets
        self.solo -= markets
        self.gone -= markets
        for m in markets:
            self.confirmed_at.pop(m, None)
            self.seq.pop(m, None)
            self.lifecycle_seq.pop(m, None)

    def on_reply(self, obj: dict) -> None:
        """Settle the subscribe or status request a reply answers, by its nonce."""
        ev, cid, ch = self.stream.event, self.conn_id, self.o.channel
        nonce, now = obj.get("nonce"), self.o.mono()
        if "code" in obj and "snapshot" not in obj and "delta" not in obj:
            # An error reply. Without a nonce (a throttled or unparsable request) it names no
            # request: an unanswered subscribe then times out and is retried.
            asked = self.pending.pop(nonce, (None, None))[0] if nonce is not None else None
            probed = self.probes.pop(nonce, (None, None))[0] if nonce is not None else None
            self.status_pending.pop(nonce, None)
            ev(cid, "ws_error_reply", code=obj.get("code"), message=redact.scrub(str(obj.get("message"))),
               nonce=nonce, channel=ch, markets=sorted(asked or probed) if asked or probed else None)
            if asked:
                self.subscribe_failed(set(asked), now)
            elif probed:
                self.probe_failed(set(probed), obj.get("code"))
            return
        self.probes.pop(nonce, None)
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
                self.forget(lost)
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
        for nonce, (_, sent) in list(self.probes.items()):
            if now - sent >= timeout:
                del self.probes[nonce]          # replay reads an unanswered probe from the archive

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
                        self.gone.discard(market_id)    # the venue knows it again
                        channel = payload[self.o.channel]
                        if isinstance(channel, dict):
                            self.note_seq(market_id, key, channel.get("seq"))
                    lc = payload.get("lifecycle")
                    if isinstance(lc, dict):
                        self.note_lifecycle(market_id, key == "delta", "subscribed" in obj, lc.get("seq"))

    def note_lifecycle(self, market: str, delta: bool, subscribed: bool, seq) -> None:
        """A transition, or a reply showing one this connection missed: the market's book is untrusted
        until a snapshot, on whichever connection carries it."""
        last = self.lifecycle_seq.get(market)
        changed = delta
        if not delta and not subscribed and isinstance(seq, int) and last is not None and seq > last:
            self.stream.event(self.conn_id, "lifecycle_missed_seen", channel=self.o.channel, market=market,
                              last_seq=last, got_seq=seq)
            changed = True
        if isinstance(seq, int) and not isinstance(seq, bool) and (last is None or seq > last):
            self.lifecycle_seq[market] = seq
        if changed:
            if self.o.on_lifecycle is not None:
                self.o.on_lifecycle(market)
            elif self.o.channel == "book":
                self.transitioned.add(market)

    def note_seq(self, market: str, kind: str, seq) -> None:
        """Follow a market's channel seq: a delta past the next one asks for a resync; a snapshot
        at or past the latest seq seen answers it (one behind was taken before, and proves nothing)."""
        if not isinstance(seq, int) or isinstance(seq, bool):
            return
        last = self.seq.get(market)
        if kind == "delta":
            if last is None:
                return                  # before this subscription's snapshot: nothing to compare with
            if seq > last + 1 and market not in self.resync:
                self.stream.event(self.conn_id, "sequence_gap_seen", channel=self.o.channel, market=market,
                                  last_seq=last, got_seq=seq)
                self.resync.add(market)
                self.last_probe.pop(market, None)       # due at once
        elif last is not None and seq < last:
            return
        else:
            self.resync.discard(market)
        if last is None or seq > last:
            self.seq[market] = seq

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
        dense_interval, quiet_interval = self.o.params.channel_probe_interval_s, self.o.params.quiet_probe_interval_s
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

            timeout = self.o.params.probe_timeout_s
            dense = self.markets if self.o.dense_fn is None else self.o.dense_fn()
            due = sorted(m for m in self.markets - self.gone
                         if m in self.transitioned
                         or (m in self.resync and now - self.last_probe.get(m, float("-inf")) >= timeout)
                         or now - max(self.last_activity.get(m, float("-inf")), self.last_probe.get(m, float("-inf")))
                         >= (dense_interval if m in dense else quiet_interval))
            if not due:
                continue
            for m in due:
                self.last_probe[m] = now
            tokens = self.o.budget.available()
            affordable = max(0, int((tokens - self.o.reserve) // self.o.weight))
            # After a transition or a skipped seq the channel is untrusted until a snapshot: those
            # go first, and one that cannot be afforded stays due and is retried next tick. A
            # resync stays pending until a snapshot catches up, so a lost reply is asked again.
            due.sort(key=lambda m: (m not in self.transitioned and m not in self.resync, m not in dense))
            send, skip = due[:affordable], due[affordable:]
            self.transitioned -= set(send)
            for m in skip:
                if m in self.resync:
                    self.last_probe.pop(m, None)        # still due next tick
            # A market from a refused multi-market probe goes alone, so one the venue no longer
            # knows cannot take the others' replies with it.
            batches = [[m] for m in send if m in self.solo] + [[m for m in send if m not in self.solo]]
            self.solo -= set(send)
            for batch in filter(None, batches):
                nonce = await self.send({"snapshot": {"markets": {m: self.o.channel for m in batch}}})
                self.o.budget.spend(self.o.weight * len(batch))
                self.probes[nonce] = (frozenset(batch), now)
                self.stream.event(self.conn_id, "probe_sent", nonce=nonce, channel=self.o.channel,
                                  markets=batch, est_tokens_before=round(tokens, 1),
                                  outside_dense=[m for m in batch if m not in dense])
                tokens -= self.o.weight * len(batch)
            if skip:
                self.stream.event(self.conn_id, "probe_skipped", reason="stream_budget",
                                  channel=self.o.channel, markets=skip,
                                  est_tokens=round(tokens, 1), reserve=self.o.reserve,
                                  outside_dense=[m for m in skip if m not in dense])

    async def close(self) -> None:
        if not self.ws.closed:
            try:
                await asyncio.wait_for(self.ws.close(), timeout=5)
            except (asyncio.TimeoutError, aiohttp.ClientError, OSError):
                pass
        self.stream.event(self.conn_id, "ws_disconnected", reason=self.close_reason or "recorder_stop",
                          close_code=self.ws.close_code,
                          duration_s=round(self.o.mono() - self.opened, 3))
