"""Collection gaps from the raw archive (DESIGN.md §7.2, §7.3).

A gap starts at the **last trustworthy observation** of a subject, not when the
problem was noticed, and ends at the first trustworthy observation after it.
A gap with no later observation stays open (`end_ms` None).

What the archive can prove in V0:

- Polls (Kalshi order books, Novig public book, The Odds API): a gap between two
  successful polls of a subject when a request between them failed, or when they
  came from different recorder sessions (a restart). A poller that silently
  slowed down inside one session is not detected here: that needs the schedule
  itself, which arrives with `poll_attempt` in A4.
- Streams (Novig): coverage is the stream replay's spans, one per subscription
  of a market's channel, extended by every trustworthy observation (snapshot,
  contiguous delta, confirmed probe). A gap at every sequence gap and every
  lifecycle transition (`lifecycle_reset`: the venue may drop orders without
  deltas), until trusted state returns on any connection; and between spans of
  the same market and channel that neither overlap nor were separated by a
  deliberate unsubscribe.
  A subscription the recorder ended on purpose (unsubscribed, stopped, or the
  capture window ended) ends coverage; it is not a gap. Any other end is an
  open gap from that market's last trustworthy observation. A subscription the
  venue dropped (the recorder's `status` check archived `subscription_lost`) is
  never a deliberate end, even on a connection closed on purpose; its gap is
  labelled `subscription_lost`. The loss belongs to the subscription that was
  live when it was reported, until that connection resubscribes the market, so
  evidence arriving after it (a probe already in flight, which the venue answers
  for an unsubscribed channel) cannot erase it; that evidence only moves the
  gap's start, which is still the last trustworthy observation.

Before a subject's first observation there is no coverage, and no gap either:
the close function requires coverage, so a missing start is never mistaken for
continuity.
"""
from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

from clv.archive import Frame, RestExchange
from clv.venues.novig.parser import Span, StreamReplay


# Recorder close reasons (tools/raw_recorder/.../novig/stream.py) that end coverage on purpose:
# the process was stopped, or the capture window for every subscribed market ended.
PLANNED_CLOSES = {"recorder_stop", "no_active_markets"}
# Gap reasons the ingest writes when it quarantines a state that replaced the stored one
# (DESIGN.md §2.4): from the stored state's last trustworthy observation to the next stored state.
QUARANTINE_REASONS = frozenset({"crossed_book"})


@dataclass(frozen=True)
class Gap:
    source: str
    scope: str                  # a subject ("kalshi:market:<ticker>") or "<subject>/<channel>" for streams
    reason: str                 # request_failed | recorder_restart | sequence_gap | lifecycle_reset
                                # | reconnect | connection_lost | connection_end_unrecorded
    start_ms: int               # last trustworthy observation
    end_ms: int | None          # first trustworthy observation after; None while open
    start_ref: str | None
    end_ref: str | None


def poll_gaps(source: str, exchanges: Iterable[RestExchange], purposes: set[str]) -> list[Gap]:
    """Gaps per subject among polls with the given purposes (e.g. {"orderbook"})."""
    by_subject: dict[str, list[RestExchange]] = defaultdict(list)
    for x in exchanges:
        if x.request.get("purpose") in purposes:
            for s in x.subjects:
                by_subject[s].append(x)
    gaps = []
    for subject, xs in by_subject.items():
        xs.sort(key=lambda x: x.request["start_ts_ms"])
        last_ok: RestExchange | None = None
        failed = False
        for x in xs:
            if not x.ok:
                failed = True
                continue
            if last_ok is not None:
                restarted = x.request["session_id"] != last_ok.request["session_id"]
                if failed or restarted:
                    gaps.append(Gap(source, subject, "request_failed" if failed else "recorder_restart",
                                    last_ok.body.recv_ts_ms, x.body.recv_ts_ms, last_ok.body.ref, x.body.ref))
            last_ok, failed = x, False
        if failed and last_ok is not None:
            gaps.append(Gap(source, subject, "request_failed", last_ok.body.recv_ts_ms, None, last_ok.body.ref, None))
    return sorted(gaps, key=lambda g: (g.scope, g.start_ms))


def stream_gaps(source: str, frames: Iterable[Frame], replay: StreamReplay) -> list[Gap]:
    """Gaps for a WebSocket source.

    `replay` (venues/novig/parser.StreamReplay, already fed) supplies what the
    data proves: coverage spans per subscription, sequence gaps and resyncs.
    `frames` supply what only the recorder knows: why each connection ended
    (`ws_disconnected`) and which subscriptions it ended on purpose (its
    `subscribe` and `unsubscribe` commands, the `out` frames).
    """
    end_reason: dict[str, str] = {}
    lost: dict[tuple[str, str], list[int]] = defaultdict(list)    # (conn, market) -> subscription_lost times
    # (conn, market) -> the recorder's subscribe/unsubscribe commands, as (send time, kind), in the
    # order sent: an unsubscribe and a resubscribe sent together share a millisecond, so only the
    # order says which subscription the unsubscribe ended.
    commands: dict[tuple[str, str], list[tuple[int, str]]] = defaultdict(list)
    for f in frames:
        if f.dir == "event":
            e = json.loads(f.frame)
            if e.get("type") == "ws_disconnected":     # the recorder's final word on the connection
                end_reason[f.conn_id] = e.get("reason") or "unknown"
            elif e.get("type") == "subscription_lost":
                for market in e.get("markets") or []:
                    lost[(f.conn_id, market)].append(f.recv_ts_ms)
        elif f.dir == "out":
            msg = json.loads(f.frame)
            for market in ((msg.get("subscribe") or {}).get("markets") or {}):
                commands[(f.conn_id, market)].append((f.recv_ts_ms, "subscribe"))
            for subject in msg.get("unsubscribe") or []:
                if subject.startswith("market:"):
                    commands[(f.conn_id, subject.removeprefix("market:"))].append((f.recv_ts_ms, "unsubscribe"))

    def ended_on_purpose(span: Span, by: int | None = None) -> bool:
        """The recorder unsubscribed the subscription this span belongs to (by time `by`, if given).

        Decided from the commands the recorder sent, not from the span's last evidence:
        frames already in flight can arrive after an unsubscribe and extend the span,
        and they must not turn a deliberate end into an outage. The span belongs to the
        last subscribe sent at or before its first evidence, and that subscription ends
        at the first unsubscribe before the next subscribe.
        """
        cmds = commands[(span.conn_id, span.market)]
        subs = [i for i, (t, kind) in enumerate(cmds) if kind == "subscribe" and t <= span.first_ts]
        start = subs[-1] if subs else -1
        resub = next((i for i in range(start + 1, len(cmds)) if cmds[i][1] == "subscribe"), len(cmds))
        return any(cmds[i][1] == "unsubscribe" and (by is None or cmds[i][0] <= by) for i in range(start + 1, resub))

    # (conn, market) -> the first evidence times of its subscriptions: a resubscribe starts a new span.
    span_starts: dict[tuple[str, str], list[int]] = defaultdict(list)
    for sp in replay.spans:
        span_starts[(sp.conn_id, sp.market)].append(sp.first_ts)

    def lost_during(span: Span, until: int | None = None) -> bool:
        """The venue dropped this span's subscription (reported by `until`, if given): a loss
        reported after the span opened and before its connection resubscribed the market."""
        key = (span.conn_id, span.market)
        resub = min((t for t in span_starts[key] if t > span.first_ts), default=float("inf"))
        return any(span.first_ts <= t < resub and (until is None or t <= until) for t in lost[key])

    gaps = []
    by_scope: dict[str, list[Span]] = defaultdict(list)
    for sp in replay.spans:
        by_scope[f"novig:market:{sp.market}/{sp.channel}"].append(sp)
    for scope, spans in by_scope.items():
        spans = sorted(spans, key=lambda sp: sp.first_ts)
        reach = spans[0]                    # the span whose coverage reaches furthest so far
        for nxt in spans[1:]:
            if nxt.first_ts > reach.last_ts and not ended_on_purpose(reach, by=nxt.first_ts):
                reason = "subscription_lost" if lost_during(reach, nxt.first_ts) else "reconnect"
                gaps.append(Gap(source, scope, reason, reach.last_ts, nxt.first_ts, reach.last_ref,
                                nxt.first_ref))
            if nxt.last_ts >= reach.last_ts:
                reach = nxt
        if ended_on_purpose(reach):
            continue
        if lost_during(reach):
            gaps.append(Gap(source, scope, "subscription_lost", reach.last_ts, None, reach.last_ref, None))
        elif end_reason.get(reach.conn_id) not in PLANNED_CLOSES:
            # Lost, or no end recorded (a killed process, or a segment not yet sealed):
            # either way coverage after the last trustworthy observation is unproven.
            reason = "connection_lost" if reach.conn_id in end_reason else "connection_end_unrecorded"
            gaps.append(Gap(source, scope, reason, reach.last_ts, None, reach.last_ref, None))
    resyncs = sorted(replay.resyncs, key=lambda r: r.recv_ts_ms)
    seen = set()
    for g in replay.seq_gaps:
        if (key := (g.market, g.channel, g.reason, g.ref)) in seen:
            continue                # one lifecycle transition, several connections' books: one gap
        seen.add(key)
        # Closed by trusted state for the same market and channel on any connection.
        end = next((r for r in resyncs if (r.market, r.channel) == (g.market, g.channel)
                    and r.recv_ts_ms >= g.recv_ts_ms), None)
        gaps.append(Gap(source, f"novig:market:{g.market}/{g.channel}", g.reason,
                        g.last_recv_ts_ms if g.last_recv_ts_ms is not None else g.recv_ts_ms,
                        end.recv_ts_ms if end else None, g.last_ref, end.ref if end else None))
    return sorted(gaps, key=lambda g: (g.scope, g.start_ms))
