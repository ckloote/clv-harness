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
- Streams (Novig): a gap at every sequence gap (until the next snapshot), and
  between connections for the same subject and channel. A connection the
  recorder closed on purpose (stopped, or the capture window ended) ends
  coverage; it is not a gap. Any other end is an open gap.

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
from clv.venues.novig.parser import Resync, SequenceGap


# Recorder close reasons (tools/raw_recorder/.../novig/stream.py) that end coverage on purpose:
# the process was stopped, or the capture window for every subscribed market ended.
PLANNED_CLOSES = {"recorder_stop", "no_active_markets"}


@dataclass(frozen=True)
class Gap:
    source: str
    scope: str                  # a subject ("kalshi:market:<ticker>") or "<subject>/<channel>" for streams
    reason: str                 # request_failed | recorder_restart | sequence_gap | reconnect
                                # | connection_lost | connection_end_unrecorded
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


@dataclass
class _Conn:
    subjects: tuple[str, ...] = ()
    channel: str = ""
    first_in: Frame | None = None
    last_in: Frame | None = None
    end_reason: str | None = None


def stream_gaps(source: str, frames: Iterable[Frame], seq_gaps: Iterable[SequenceGap] = (),
                resyncs: Iterable[Resync] = ()) -> list[Gap]:
    """Gaps for a WebSocket source, from its connection events, received frames
    and the sequence gaps and resyncs a replay found (venues/novig/parser.StreamReplay)."""
    conns: dict[str, _Conn] = {}
    for f in frames:
        c = conns.setdefault(f.conn_id, _Conn())
        if f.dir == "event":
            e = json.loads(f.frame)
            if e["type"] == "ws_connect_attempt":
                c.subjects = tuple(f"novig:market:{m}" for m in e.get("markets", []))
                c.channel = e.get("channel", "")
            elif e["type"] == "ws_disconnected":     # the recorder's final word on the connection
                c.end_reason = e.get("reason") or "unknown"
        elif f.dir == "in":
            c.first_in = c.first_in or f
            c.last_in = f
    gaps = []
    runs: dict[str, list[_Conn]] = defaultdict(list)
    for c in conns.values():
        if c.first_in is not None:
            for s in c.subjects:
                runs[f"{s}/{c.channel}"].append(c)
    for scope, cs in runs.items():
        cs.sort(key=lambda c: c.first_in.recv_ts_ms)
        for a, b in zip(cs, cs[1:]):
            gaps.append(Gap(source, scope, "reconnect", a.last_in.recv_ts_ms, b.first_in.recv_ts_ms,
                            a.last_in.ref, b.first_in.ref))
        last = cs[-1]
        if last.end_reason not in PLANNED_CLOSES:
            # Lost, or no end recorded (a killed process, or a segment not yet sealed):
            # either way coverage after the last frame is unproven.
            reason = "connection_lost" if last.end_reason else "connection_end_unrecorded"
            gaps.append(Gap(source, scope, reason, last.last_in.recv_ts_ms, None, last.last_in.ref, None))
    resyncs = sorted(resyncs, key=lambda r: r.recv_ts_ms)
    for g in seq_gaps:
        end = next((r for r in resyncs if (r.conn_id, r.market, r.channel) == (g.conn_id, g.market, g.channel)
                    and r.recv_ts_ms >= g.recv_ts_ms), None)
        gaps.append(Gap(source, f"novig:market:{g.market}/{g.channel}", "sequence_gap",
                        g.last_recv_ts_ms if g.last_recv_ts_ms is not None else g.recv_ts_ms,
                        end.recv_ts_ms if end else None, g.last_ref, end.ref if end else None))
    return sorted(gaps, key=lambda g: (g.scope, g.start_ms))
