"""Novig v3: rebuild books from the archived stream, and parse REST responses.

Stream (wire shapes observed on Production, docs/vendor-capabilities.md):

    subscribe / snapshot reply   {"ts", "nonce", ["subscribed"], "snapshot": {market: {
                                     "eventId", "book": {"seq", "orders": {outcomeId: [order]}},
                                     "trades": {"seq", "trades": [{"seq", "deltas": [trade]}]},
                                     "lifecycle": {"seq", "status"}}}}
    delta                        {"ts", "delta": {market: {"eventId", <channel>: {"seq", "deltas": [...]}}}}

    book deltas    add {orderId, outcomeId, price, qty} | update {orderId, remaining}
                   | remove {orderId, reason: fill | cancel}
    trades deltas  {tradeId, outcomeId, price, qty, ts}
    lifecycle      {kind, status}, e.g. {"kind": "GOLIVE", "status": "OPEN"}

An order on an outcome is a bid for that outcome; it is liquidity for the
other outcome at 1 - price. `seq` is per connection, market and channel.

`StreamReplay.feed()` takes frames in receive order and returns what each one
established: a book state, a probe check, a sequence gap, a resync, a trade or
a lifecycle status. A sequence gap makes the book untrusted until a new
snapshot (DESIGN.md §7.2); the replay never guesses across it. A lifecycle
status is judged by the lifecycle channel's own sequence, never the book's: one
behind it is not emitted, even beside a current book. The replay is also the
one judge of channel evidence: its coverage spans are what gaps.stream_gaps
builds stream gaps from.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal
from urllib.parse import parse_qs

from clv.archive import Frame, RestExchange
from clv.venues.protocol import BinaryBook, aggregate

VENUE = "novig"
PAYOUT_USD_PER_CONTRACT = Decimal("0.01")     # docs/vendor-capabilities.md, Prices and quantities


@dataclass(frozen=True)
class ProbeCheck:
    """A snapshot reply compared with the replayed state at the same sequence.

    status: `confirmed` (same seq, same orders), `mismatch` (same seq, different
    orders: a parser or venue defect), `gap` (the reply is ahead: deltas were
    missed), `superseded` (the reply is behind: deltas already covered it).
    """
    conn_id: str
    market: str
    channel: str
    nonce: int | None
    replay_seq: int | None
    snapshot_seq: int
    status: str
    recv_ts_ms: int
    ref: str


@dataclass(frozen=True)
class SequenceGap:
    conn_id: str
    market: str
    channel: str
    last_seq: int
    got_seq: int
    last_ref: str | None        # the last frame of the contiguous run
    last_recv_ts_ms: int | None
    ref: str                    # the frame exposing the gap
    recv_ts_ms: int


@dataclass(frozen=True)
class Resync:
    """A snapshot re-established trusted state after a sequence gap."""
    conn_id: str
    market: str
    channel: str
    seq: int
    recv_ts_ms: int
    ref: str


@dataclass(frozen=True)
class Trade:
    market: str
    trade_id: str
    outcome_id: str
    price: str
    qty: int                    # native contracts
    venue_ts_ms: int
    recv_ts_ms: int
    ref: str


@dataclass(frozen=True)
class Lifecycle:
    conn_id: str
    market: str
    seq: int | None
    kind: str | None            # None for the status carried in a snapshot
    status: str
    venue_ts_ms: int
    recv_ts_ms: int
    ref: str


@dataclass
class Span:
    """Trusted coverage of one market's channel during one subscription on one connection.

    It runs from the subscribe snapshot to the last trustworthy observation: a
    snapshot install, a contiguous delta or a confirmed probe. A resubscription
    starts a new span, so an unsubscribed interval is never covered. Sequence
    gaps inside a span are reported separately (`StreamReplay.seq_gaps`). Mutable:
    the replay extends the current span as evidence arrives.
    """
    conn_id: str
    market: str
    channel: str
    first_ts: int
    first_ref: str
    last_ts: int
    last_ref: str


class _Channel:
    """Replayed state of one market's channel on one connection."""
    def __init__(self):
        self.seq: int | None = None         # None: no trusted state
        self.orders: dict[str, tuple[str, str, int]] = {}   # orderId -> (outcomeId, price, qty)
        self.outcomes: tuple[str, ...] = ()
        self.last_ref: str | None = None    # last trustworthy observation (channel evidence)
        self.last_ts: int | None = None
        self.snapshot_ref: str | None = None
        self.span: Span | None = None


class StreamReplay:
    """Replays any number of connections' frames, fed in receive order.

    Besides each frame's outputs, it keeps what continuity needs (gaps.stream_gaps):
    coverage `spans`, `seq_gaps` and the `resyncs` that close them. A gap on one
    connection is closed by trusted state for the same market and channel on any
    connection, including a replacement after a reconnect.
    """
    def __init__(self):
        self.channels: dict[tuple[str, str, str], _Channel] = {}   # (conn, market, channel)
        self.trade_ids: set[str] = set()
        self.spans: list[Span] = []
        self.seq_gaps: list[SequenceGap] = []
        self.resyncs: list[Resync] = []
        self.open_gaps: set[tuple[str, str]] = set()             # (market, channel) awaiting a resync

    def feed(self, f: Frame) -> list:
        if f.dir != "in":
            return []
        msg = json.loads(f.frame)
        out: list = []
        if "snapshot" in msg:
            for market, body in msg["snapshot"].items():
                out += self._snapshot(f, msg, market, body)
        if "delta" in msg:
            for market, body in msg["delta"].items():
                out += self._delta(f, msg, market, body)
        return out

    # -- snapshots -------------------------------------------------------------

    def _snapshot(self, f: Frame, msg: dict, market: str, body: dict) -> list:
        out: list = []
        if "lifecycle" in body:
            lc = body["lifecycle"]
            if self._lifecycle_seq(f, market, lc.get("seq")):
                out.append(Lifecycle(f.conn_id, market, lc.get("seq"), None, lc["status"], msg["ts"], f.recv_ts_ms,
                                     f.ref))
        if "book" in body:
            out += self._book_snapshot(f, msg, market, body["book"])
        if "trades" in body:
            out += self._trades_snapshot(f, msg, market, body["trades"])
        return out

    def _trades_snapshot(self, f: Frame, msg: dict, market: str, trades: dict) -> list:
        """A trades snapshot lists recent trades; it is checked by sequence only."""
        ch = self._ch(f.conn_id, market, "trades")
        subscribed = "subscribed" in msg
        out: list = []
        for batch in trades.get("trades", []):
            out += self._trades(f, market, batch["deltas"])
        if ch.seq is not None and not subscribed:
            seq = trades["seq"]
            status = "confirmed" if seq == ch.seq else "gap" if seq > ch.seq else "superseded"
            out.append(ProbeCheck(f.conn_id, market, "trades", msg.get("nonce"), ch.seq, seq, status,
                                  f.recv_ts_ms, f.ref))
            if status == "confirmed":
                self._evidence(ch, f, market, "trades")
                return out
            if status == "superseded":
                return out
            out.append(self._gap(ch, f, market, "trades", seq))
        out += self._resync(f, market, "trades", trades["seq"])
        ch.seq, ch.snapshot_ref = trades["seq"], f.ref
        self._evidence(ch, f, market, "trades", new_span=subscribed)
        return out

    def _book_snapshot(self, f: Frame, msg: dict, market: str, book: dict) -> list:
        ch = self._ch(f.conn_id, market, "book")
        subscribed = "subscribed" in msg
        orders = {o["orderId"]: (outcome, o["price"], o["qty"])
                  for outcome, lst in book["orders"].items() for o in lst}
        outcomes = tuple(book["orders"])
        out: list = []
        if ch.seq is not None and not subscribed:
            if book["seq"] == ch.seq:
                status = "confirmed" if orders == ch.orders else "mismatch"
            elif book["seq"] > ch.seq:
                status = "gap"
            else:
                status = "superseded"
            out.append(ProbeCheck(f.conn_id, market, "book", msg.get("nonce"), ch.seq, book["seq"],
                                  status, f.recv_ts_ms, f.ref))
            if status == "confirmed":
                # Unchanged: fresh channel evidence, but no new book state, so the
                # book's economic-change time stays where it was (§7.2).
                self._evidence(ch, f, market, "book")
                return out
            if status == "superseded":
                return out          # behind the replayed state: proves nothing new
            if status == "gap":
                out.append(self._gap(ch, f, market, "book", book["seq"]))
            # A mismatch installs the authoritative snapshot; the ProbeCheck reports the defect.
        out += self._resync(f, market, "book", book["seq"])
        ch.seq, ch.orders, ch.snapshot_ref = book["seq"], orders, f.ref
        ch.outcomes = outcomes or ch.outcomes
        self._evidence(ch, f, market, "book", new_span=subscribed)
        out.append(self._state(f, msg, market, ch))
        return out

    # -- deltas ----------------------------------------------------------------

    def _delta(self, f: Frame, msg: dict, market: str, body: dict) -> list:
        out: list = []
        for channel in ("book", "trades", "lifecycle"):
            if channel not in body:
                continue
            batch = body[channel]
            ch = self._ch(f.conn_id, market, channel)
            if channel != "lifecycle":
                if ch.seq is None:
                    continue        # untrusted until a snapshot; the raw archive keeps the delta
                if batch["seq"] != ch.seq + 1:
                    if batch["seq"] <= ch.seq:
                        continue    # already covered by a later snapshot
                    out.append(self._gap(ch, f, market, channel, batch["seq"]))
                    continue
                ch.seq = batch["seq"]
                self._evidence(ch, f, market, channel)
            if channel == "book":
                for d in batch["deltas"]:
                    self._apply(ch, d, f)
                out.append(self._state(f, msg, market, ch))
            elif channel == "trades":
                out += self._trades(f, market, batch["deltas"])
            elif self._lifecycle_seq(f, market, batch.get("seq")):
                for d in batch["deltas"]:
                    out.append(Lifecycle(f.conn_id, market, batch.get("seq"), d["kind"], d["status"],
                                         msg["ts"], f.recv_ts_ms, f.ref))
        return out

    @staticmethod
    def _apply(ch: _Channel, d: dict, f: Frame) -> None:
        kind, oid = d["kind"], d["orderId"]
        if kind == "add":
            if oid in ch.orders:
                raise ValueError(f"{f.ref}: add of resting order {oid}")
            ch.orders[oid] = (d["outcomeId"], d["price"], d["qty"])
        elif kind == "update":      # observed on Production; not in the docs (partial fill)
            outcome, price, qty = ch.orders[oid]
            if not 0 < d["remaining"] < qty:
                raise ValueError(f"{f.ref}: update of {oid} from {qty} to {d['remaining']}")
            ch.orders[oid] = (outcome, price, d["remaining"])
        elif kind == "remove":
            if d.get("reason") not in ("fill", "cancel"):
                raise ValueError(f"{f.ref}: remove reason {d.get('reason')!r}")
            del ch.orders[oid]
        else:
            raise ValueError(f"{f.ref}: unknown book delta kind {kind!r}")

    def _trades(self, f: Frame, market: str, deltas: list) -> list:
        out = []
        for t in deltas:
            if t["tradeId"] in self.trade_ids:
                continue            # a snapshot repeats recent trades
            self.trade_ids.add(t["tradeId"])
            out.append(Trade(market, t["tradeId"], t["outcomeId"], t["price"], t["qty"], t["ts"],
                             f.recv_ts_ms, f.ref))
        return out

    # -- continuity ------------------------------------------------------------

    def _evidence(self, ch: _Channel, f: Frame, market: str, channel: str, new_span: bool = False) -> None:
        """The one place a trustworthy observation is recorded: it moves the channel's
        evidence boundary and extends (or, on a subscribe snapshot, opens) its span."""
        ch.last_ref, ch.last_ts = f.ref, f.recv_ts_ms
        if new_span or ch.span is None:
            ch.span = Span(f.conn_id, market, channel, f.recv_ts_ms, f.ref, f.recv_ts_ms, f.ref)
            self.spans.append(ch.span)
        else:
            ch.span.last_ts, ch.span.last_ref = f.recv_ts_ms, f.ref

    def _gap(self, ch: _Channel, f: Frame, market: str, channel: str, got: int) -> SequenceGap:
        """Record a gap and drop the channel's state until a snapshot re-establishes it."""
        gap = SequenceGap(f.conn_id, market, channel, ch.seq, got, ch.last_ref, ch.last_ts, f.ref, f.recv_ts_ms)
        ch.seq = None
        self.seq_gaps.append(gap)
        self.open_gaps.add((market, channel))
        return gap

    def _resync(self, f: Frame, market: str, channel: str, seq: int) -> list:
        """Trusted state for a market and channel with an open gap, on any connection."""
        if (market, channel) not in self.open_gaps:
            return []
        self.open_gaps.discard((market, channel))
        r = Resync(f.conn_id, market, channel, seq, f.recv_ts_ms, f.ref)
        self.resyncs.append(r)
        return [r]

    def _lifecycle_seq(self, f: Frame, market: str, seq: int | None) -> bool:
        """Lifecycle has its own sequence, independent of book and trades. A status whose
        sequence is behind the latest one on this connection is stale, whatever the book
        sequence beside it says; one without a sequence carries no evidence either way."""
        ch = self._ch(f.conn_id, market, "lifecycle")
        if seq is None:
            return True
        if ch.seq is not None and seq < ch.seq:
            return False
        ch.seq = seq
        return True

    def _ch(self, conn: str, market: str, channel: str) -> _Channel:
        return self.channels.setdefault((conn, market, channel), _Channel())

    @staticmethod
    def _state(f: Frame, msg: dict, market: str, ch: _Channel) -> BinaryBook:
        per_side: dict[str, list] = {o: [] for o in ch.outcomes}
        for outcome, price, qty in ch.orders.values():
            per_side.setdefault(outcome, []).append((price, Decimal(qty)))
        if len(per_side) != 2:
            raise ValueError(f"{f.ref}: market {market} has outcomes {sorted(per_side)}; expected two")
        refs = (ch.snapshot_ref,) if ch.snapshot_ref == f.ref else (ch.snapshot_ref, f.ref)
        return BinaryBook(VENUE, market, {o: aggregate(v) for o, v in per_side.items()},
                          PAYOUT_USD_PER_CONTRACT, complete=True, source="stream", seq=ch.seq,
                          venue_ts_ms=msg["ts"], recv_ts_ms=f.recv_ts_ms, refs=refs)


# -- REST ------------------------------------------------------------------------

def public_book(x: RestExchange) -> BinaryBook:
    """`GET /v3/public/catalog/markets/{id}/book?depth=N`: the unsigned poll.

    `depth` limits price levels, not orders: on 849832 every poll showed all
    orders at 5-9 levels per side, identical to the stream snapshot 20 s later.
    Whether the limit is per side or in total is not documented, so the ladder
    is marked incomplete when either reading could have cut it.
    """
    body = x.body.json()
    depth = int(parse_qs(x.request.get("query") or "").get("depth", ["0"])[0]) or None
    sides = {o: aggregate([(r["price"], Decimal(r["qty"])) for r in lst]) for o, lst in body["orders"].items()}
    if len(sides) != 2:
        raise ValueError(f"{x.body.ref}: expected two outcomes, got {sorted(sides)}")
    n_levels = [len(lv) for lv in sides.values()]
    complete = depth is None or (max(n_levels) < depth and sum(n_levels) < depth)
    return BinaryBook(VENUE, body["marketId"], sides, PAYOUT_USD_PER_CONTRACT, complete=complete,
                      source="poll", seq=body.get("seq"), venue_ts_ms=None,
                      recv_ts_ms=x.body.recv_ts_ms, refs=x.refs)


@dataclass(frozen=True)
class CatalogMarket:
    market: str
    event: str
    description: str
    market_type: str
    status: str
    voids: str | None
    starts_ts_ms: int | None
    fee: dict
    outcomes: tuple[tuple[str, str, str], ...]     # (outcomeId, name, status)
    recv_ts_ms: int
    refs: tuple[str, ...]


def catalog_market(x: RestExchange) -> CatalogMarket:
    """`GET /v3/public/catalog/markets/{id}`."""
    m = x.body.json()
    return CatalogMarket(m["marketId"], m["eventId"], m["description"], m["marketType"], m["status"],
                         m.get("voids"), m.get("startsTs"), m.get("fee") or {},
                         tuple((o["outcomeId"], o["name"], o["status"]) for o in m["outcomes"]),
                         x.body.recv_ts_ms, x.refs)
