"""Gate A1 (DESIGN.md §13): did every forced fault produce the right collection gap?

    clv gate a1 --from 2026-11-10T00:00Z --to 2026-11-12T00:00Z

It reads two things, both from the same archive:

- the fault harness's record (raw_recorder.faults): each `fault_injected` label in the
  recorder's session streams, with every Novig connection's confirmed markets at that
  moment, and its `fault_applied` report (what the relay dropped and when, which sockets it
  aborted, which PID was killed) or `fault_failed`;
- the facts `clv ingest --league-events` loaded from it: collection_gap, tick,
  liveness_evidence and book_snapshot.

Each (market, channel) a fault touched is one check:

    socket_kill, process_kill     every market confirmed on an affected connection at the
                                  label has a gap covering the fault (the abort, the kill)
    channel_stall, sequence_skip  every market and channel in every dropped frame has a gap
                                  covering the drop
    resubscribe                   a fresh subscribe snapshot follows on the same connection,
                                  and no gap: the recorder ended that subscription on purpose

and the gap covering it must:

- have a reason the fault can cause;
- open at the last trustworthy observation before the fault: for a book, the latest
  stream tick or liveness evidence at or before it (a trades channel has no such facts,
  so only its gap's existence and close are checked);

with the fault time taken loosely by `gate_a1.receive_slack_ms`: the recorder stamps a frame
when it reads it, which can trail the relay's or harness's own clock reading, so a frame the
relay forwarded just before the fault may carry a stamp just after it. The slack never
excuses a misplaced boundary: inside a gap there is no trustworthy observation, so the last
one before the gap's close, within the slack, must be exactly where it opens;
- close at an authoritative snapshot of that market and channel received after the fault,
  or stay open only if the market was never observed again (its capture had ended). On the
  connection that lost a dropped delta, the snapshot must be at or past the dropped seq: one
  behind it was taken before the delta and cannot hold it, however late it arrived. A seq
  belongs to one connection and one subscription, so a snapshot on a new connection, or a
  subscribe reply, is judged by its receive time alone.

Beside the faults: every closed stream gap in the run, injected or not, must close at a
snapshot, since a resync always starts from one, and a sequence gap closed on its own
connection by a probe reply must close at a seq past the observation it opened at (the facts
do not keep the seq that exposed it, so this is the tightest bound they allow); every gap no fault explains is listed, since
a real outage during the gate is evidence too; and a fault whose injection failed (the relay
found no delta to skip) is listed and not counted. Then the storage report (sealed bytes,
frames and segments per day and source) and the depth report (how often a book snapshot's
ladder was cut at the venue's level cap, and how often both sides reached each
`close.depth_notionals_usd`).

PASS needs every check to pass, every label to have a result, and each fault kind applied at
least `min_per_kind` times (the gate's decision record fixes the schedule). The archive must
be sealed through the end of the run, so no label or frame is missed.
"""
from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from clv import archive
from clv.config import param
from clv.gaps import QUARANTINE_REASONS
from clv.timeutil import ms_iso

KINDS = ("socket_kill", "process_kill", "channel_stall", "sequence_skip", "resubscribe")  # raw_recorder.faults.KINDS
CONNECTION_REASONS = frozenset({"reconnect", "connection_lost", "connection_end_unrecorded"})
# The gap reasons (clv.gaps) each fault can cause. A dropped delta is exposed by the next seq,
# or by a probe; a connection that ended before either leaves a connection gap instead.
ALLOWED = {
    "socket_kill": frozenset({"reconnect", "connection_lost"}),
    "process_kill": frozenset({"reconnect", "connection_end_unrecorded"}),
    "channel_stall": frozenset({"sequence_gap", "subscription_lost"}) | CONNECTION_REASONS,
    "sequence_skip": frozenset({"sequence_gap", "subscription_lost"}) | CONNECTION_REASONS,
}


class GateError(Exception):
    pass


@dataclass
class Fault:
    fault_id: str
    kind: str
    target: dict
    injected_ms: int
    subscriptions: list[dict]
    ref: str
    result: dict | None = None          # the fault_applied or fault_failed report
    applied: bool | None = None         # None: no result recorded


@dataclass(frozen=True)
class Gap:
    gap_id: int
    scope: str
    reason: str
    start_ms: int
    end_ms: int | None
    start_ref: str | None
    end_ref: str | None


@dataclass(frozen=True)
class Check:
    fault_id: str | None
    kind: str
    scope: str
    ok: bool
    detail: str


@dataclass
class Report:
    lo: int
    hi: int
    min_per_kind: int
    faults: list[Fault]
    checks: list[Check] = field(default_factory=list)
    unattributed: list[Gap] = field(default_factory=list)
    storage: list[tuple] = field(default_factory=list)     # (day, source, segments, bytes, frames, unclean)
    depth: dict = field(default_factory=dict)

    def applied(self) -> Counter:
        return Counter(f.kind for f in self.faults if f.applied)

    def verdict(self) -> bool:
        return (all(c.ok for c in self.checks) and all(f.applied is not None for f in self.faults)
                and all(self.applied()[k] >= self.min_per_kind for k in KINDS))


def parse_scope(scope: str) -> tuple[str, str]:
    subject, _, channel = scope.partition("/")
    return subject.removeprefix("novig:market:"), channel


def read_faults(root: Path, lo: int, hi: int) -> list[Fault]:
    """Labels in [lo, hi], each with its first result."""
    faults: dict[str, Fault] = {}
    results: dict[str, tuple[bool, dict]] = {}
    for f in archive.window(root, "recorder", lo, hi):
        if f.dir != "event":
            continue
        e = json.loads(f.frame)
        if e.get("type") == "fault_injected":
            faults.setdefault(e["fault_id"], Fault(e["fault_id"], e["kind"], e.get("target") or {}, f.recv_ts_ms,
                                                   e.get("subscriptions") or [], f.ref))
        elif e.get("type") in ("fault_applied", "fault_failed"):
            results.setdefault(e["fault_id"], (e["type"] == "fault_applied", e.get("report") or {}))
    for fid, (ok, rep) in results.items():
        if fid in faults:
            faults[fid].applied, faults[fid].result = ok, rep
    return sorted(faults.values(), key=lambda f: f.injected_ms)


class _Facts:
    """The facts and frames a check reads, with the archive's segments cached by path."""

    def __init__(self, conn: sqlite3.Connection, root: Path, lo: int, hi: int):
        self.conn, self.root = conn, root
        self.gaps: dict[str, list[Gap]] = defaultdict(list)
        for row in conn.execute("""
                SELECT o.collection_gap_id, o.scope, o.reason, o.boundary_ts_ms, c.boundary_ts_ms,
                       ao.relpath || '#' || o.raw_line, ac.relpath || '#' || c.raw_line
                FROM collection_gap o
                LEFT JOIN collection_gap c ON c.opens_gap_id = o.collection_gap_id
                LEFT JOIN raw_artifact ao ON ao.raw_artifact_id = o.raw_artifact_id
                LEFT JOIN raw_artifact ac ON ac.raw_artifact_id = c.raw_artifact_id
                WHERE o.kind = 'open' AND o.source = 'novig' AND o.scope LIKE 'novig:market:%/%'
                  AND o.boundary_ts_ms <= ? AND (c.boundary_ts_ms IS NULL OR c.boundary_ts_ms >= ?)
                ORDER BY o.boundary_ts_ms, o.collection_gap_id""", (hi, lo)):
            self.gaps[row[1]].append(Gap(*row))
        self._segments: dict[str, list] = {}

    def instruments(self, market: str) -> list[int]:
        return [r[0] for r in self.conn.execute(
            "SELECT venue_instrument_id FROM venue_instrument WHERE venue = 'novig' AND native_id = ?", (market,))]

    def last_evidence(self, market: str, t: int, below_seq: int | None = None) -> int | None:
        """The latest stream tick or liveness evidence of a market's book at or before t, and
        (given `below_seq`) from a frame whose book seq is below it: what preceded a dropped delta."""
        rows = self.conn.execute("""
            SELECT ts, seq, ref FROM (
                SELECT t.observed_ts_ms AS ts, t.seq AS seq, NULL AS ref
                FROM tick t JOIN venue_instrument v USING (venue_instrument_id)
                WHERE v.venue = 'novig' AND v.native_id = ? AND t.source = 'stream' AND t.observed_ts_ms <= ?
                UNION ALL
                SELECT e.observed_ts_ms, NULL, a.relpath || '#' || e.raw_line
                FROM liveness_evidence e JOIN venue_instrument v USING (venue_instrument_id)
                JOIN raw_artifact a ON a.raw_artifact_id = e.raw_artifact_id
                WHERE v.venue = 'novig' AND v.native_id = ? AND e.source = 'stream' AND e.observed_ts_ms <= ?)
            ORDER BY ts DESC""", (market, t, market, t))
        for ts, seq, ref in rows:
            if below_seq is None:
                return ts
            if seq is None:
                seq = frame_seq(self.frame(ref), market, "book")
            if seq is not None and seq < below_seq:
                return ts
        return None

    def observed_after(self, market: str, t: int) -> bool:
        return self.conn.execute("""
            SELECT EXISTS (SELECT 1 FROM tick t JOIN venue_instrument v USING (venue_instrument_id)
                           WHERE v.venue = 'novig' AND v.native_id = ? AND t.source = 'stream'
                             AND t.observed_ts_ms > ?)""", (market, t)).fetchone()[0] == 1

    def subscribe_snapshot_stored(self, ref: str) -> bool:
        relpath, _, line = ref.rpartition("#")
        return self.conn.execute("""
            SELECT EXISTS (SELECT 1 FROM book_snapshot b JOIN raw_artifact a USING (raw_artifact_id)
                           WHERE a.relpath = ? AND b.raw_line = ? AND b.reason = 'subscribe')""",
                                 (relpath, int(line))).fetchone()[0] == 1

    def frame(self, ref: str) -> archive.Frame:
        relpath, _, line = ref.rpartition("#")
        if relpath not in self._segments:
            source, day, file = relpath.split("/")
            seg = next((s for s in archive.segments(self.root, source, [day]) if s.file == file), None)
            if seg is None:
                raise GateError(f"{relpath}: cited by the facts but not journalled in {self.root}")
            self._segments[relpath] = archive.read_segment(self.root, seg)
        return self._segments[relpath][int(line)]


def is_snapshot(frame: archive.Frame, market: str, channel: str) -> bool:
    """An authoritative snapshot of the market's channel: a subscribe or probe reply."""
    if frame.dir != "in":
        return False
    try:
        msg = json.loads(frame.frame)
    except ValueError:
        return False                    # a REST body that is not JSON
    body = (msg.get("snapshot") or {}).get(market) if isinstance(msg, dict) else None
    return isinstance(body, dict) and channel in body


def frame_seq(frame: archive.Frame, market: str, channel: str) -> int | None:
    """The seq a delta or snapshot frame carries for a market's channel."""
    msg = json.loads(frame.frame)
    for key in ("delta", "snapshot"):
        body = ((msg.get(key) or {}).get(market) or {}).get(channel)
        if isinstance(body, dict) and isinstance(body.get("seq"), int):
            return body["seq"]
    return None


def expectations(f: Fault) -> dict[str, list[tuple[int, int | None]]]:
    """Every market and channel the fault touched, each with its fault times: a dropped
    delta's with its seq, a connection fault's with None."""
    rep = f.result or {}
    out: dict[str, set] = defaultdict(set)
    if f.kind in ("socket_kill", "process_kill"):
        t = rep["applied_ts_ms"] if f.kind == "socket_kill" else rep["killed_ts_ms"]
        channel = f.target.get("channel") if f.kind == "socket_kill" else None
        for s in f.subscriptions:
            if channel is None or s["channel"] == channel:
                for m in s["markets"]:
                    out[f"novig:market:{m}/{s['channel']}"].add((t, None))
    elif f.kind in ("channel_stall", "sequence_skip"):
        for d in rep.get("dropped", []):
            for m, chans in d["markets"].items():
                for ch, seq in chans.items():
                    out[f"novig:market:{m}/{ch}"].add((d["ts_ms"], seq))
    return {scope: sorted(ts, key=lambda x: x[0]) for scope, ts in sorted(out.items())}


def judge(facts: _Facts, f: Fault, g: Gap, t: int, seq: int | None, slack: int) -> list[str]:
    """What is wrong with `g` as the gap for fault `f` at time t (a dropped delta's seq, if
    any); empty if nothing."""
    market, channel = parse_scope(g.scope)
    problems = []
    if g.reason not in ALLOWED[f.kind]:
        problems.append(f"reason {g.reason} is not one a {f.kind} causes")
    if channel == "book":
        # A dropped delta's boundary is placed by seq: the slack only lets the frame before it
        # carry a late stamp, never lets a frame after it count as trustworthy.
        until = t + slack if g.end_ms is None else min(t + slack, g.end_ms - 1)
        last = facts.last_evidence(market, until, below_seq=seq)
        if last is not None and last != g.start_ms:
            problems.append(f"opens at {ms_iso(g.start_ms)}, but the last trustworthy observation before the "
                            f"fault was at {ms_iso(last)}")
    if g.end_ms is None:
        if facts.observed_after(market, t):
            problems.append("never closes, though the market's book was observed again")
    elif g.end_ms <= t:
        problems.append(f"closes at {ms_iso(g.end_ms)}, before the fault")
    elif g.end_ref is None or not is_snapshot(facts.frame(g.end_ref), market, channel):
        problems.append(f"closes at {g.end_ref}, which is not a snapshot of {market} {channel}")
    elif seq is not None and (got := same_subscription_seq(facts, g, market, channel)) is not None and got < seq:
        problems.append(f"closes at a snapshot at seq {got} ({g.end_ref}), behind the dropped seq {seq}")
    return problems


def same_subscription_seq(facts: _Facts, g: Gap, market: str, channel: str) -> int | None:
    """The closing snapshot's seq, when it is comparable with the seqs before the gap: a probe
    reply on the connection where the gap opened. None for a new connection or a subscribe
    reply, whose seq may start afresh."""
    if g.start_ref is None or g.end_ref is None:
        return None
    start, end = facts.frame(g.start_ref), facts.frame(g.end_ref)
    if start.conn_id != end.conn_id or "subscribed" in json.loads(end.frame):
        return None
    return frame_seq(end, market, channel)


def covering(facts: _Facts, f: Fault, scope: str, t: int, seq: int | None, used: set[int]) -> tuple[Gap | None, str]:
    """The gap that is right for one fault time, or None and what is wrong."""
    slack = param("gate_a1.receive_slack_ms")
    cands = [g for g in facts.gaps[scope] if g.start_ms <= t + slack and (g.end_ms is None or g.end_ms >= t)]
    if not cands:
        return None, f"no gap covers the fault at {ms_iso(t)}"
    used.update(g.gap_id for g in cands)
    first_problems = None
    # Gaps that open at or before the fault first, the latest of them first.
    for g in sorted(cands, key=lambda g: (g.start_ms > t, -g.start_ms, g.gap_id)):
        problems = judge(facts, f, g, t, seq, slack)
        if not problems:
            return g, ""
        first_problems = first_problems or f"{span(g)}: " + "; ".join(problems)
    return None, first_problems


def span(g: Gap) -> str:
    return f"{g.reason} {ms_iso(g.start_ms)} .. {ms_iso(g.end_ms) if g.end_ms else 'open (never observed again)'}"


def check_scope(facts: _Facts, f: Fault, scope: str, times: list[tuple[int, int | None]], used: set[int]) -> Check:
    """One check per fault and scope: every fault time on it (each dropped delta) is right."""
    market, _ = parse_scope(scope)
    if not facts.instruments(market):
        return Check(f.fault_id, f.kind, scope, False,
                     "market not ingested: run `clv ingest --league-events` once its window has settled")
    found, bad = [], []
    for t, seq in times:
        g, why = covering(facts, f, scope, t, seq, used)
        if g is None:
            bad.append((t, seq, why))
        elif g not in found:
            found.append(g)
    drops = times[0][1] is not None
    what = f"{len(times)} dropped delta{'s' * (len(times) != 1)}: " if drops else ""
    if bad:
        t, seq, why = bad[0]
        at = f"{len(bad)} of {len(times)} wrong; first, seq {seq} at {ms_iso(t)}: " if drops else ""
        return Check(f.fault_id, f.kind, scope, False, at + why)
    return Check(f.fault_id, f.kind, scope, True, what + "; ".join(span(g) for g in found))


def check_resubscribe(facts: _Facts, f: Fault, used: set[int]) -> list[Check]:
    rep, market = f.result or {}, f.target.get("market")
    t = rep.get("applied_ts_ms", f.injected_ms)
    wait_s = param("gate_a1.resubscribe_wait_s")
    out = []
    for c in rep.get("connections", []):
        scope = f"novig:market:{market}/{c['channel']}"
        snap = next((fr for fr in archive.window(facts.root, "novig", f.injected_ms, t + wait_s * 1000)
                     if fr.conn_id == c["conn_id"] and is_snapshot(fr, market, c["channel"])
                     and (json.loads(fr.frame).get("subscribed") or {}).get("markets", {}).get(market) == c["channel"]),
                    None)
        if snap is None:
            out.append(Check(f.fault_id, f.kind, scope, False,
                             f"no subscribe snapshot on {c['conn_id']} within {wait_s} s"))
            continue
        gaps = [g for g in facts.gaps[scope]
                if g.start_ms <= snap.recv_ts_ms and (g.end_ms is None or g.end_ms >= f.injected_ms)]
        used.update(g.gap_id for g in gaps)
        if gaps:
            out.append(Check(f.fault_id, f.kind, scope, False,
                             f"a {gaps[0].reason} gap from {ms_iso(gaps[0].start_ms)}, though the recorder "
                             "ended that subscription on purpose"))
        elif c["channel"] == "book" and not facts.subscribe_snapshot_stored(snap.ref):
            out.append(Check(f.fault_id, f.kind, scope, False,
                             f"the subscribe snapshot {snap.ref} is not stored as one (not ingested?)"))
        else:
            out.append(Check(f.fault_id, f.kind, scope, True,
                             f"fresh subscribe snapshot {ms_iso(snap.recv_ts_ms)} ({snap.ref}), no gap"))
    if not out:
        out.append(Check(f.fault_id, f.kind, f"novig:market:{market}", False, "the report names no connection"))
    return out


def storage(root: Path, lo: int, hi: int) -> list[tuple]:
    days = sorted({ms_iso(t)[:10] for t in range(lo - lo % 86_400_000, hi + 1, 86_400_000)})
    rows = []
    for src in sorted(p.name for p in Path(root).iterdir() if p.is_dir()):
        per_day: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])
        for s in archive.segments(root, src, days):
            row = per_day[s.day]
            row[0] += 1
            row[1] += s.bytes
            row[2] += s.frame_count
            row[3] += int(s.unclean_close)
        rows += [(day, src, *v) for day, v in sorted(per_day.items())]
    return sorted(rows)


def depth(conn: sqlite3.Connection, lo: int, hi: int) -> dict:
    notionals = list(param("close.depth_notionals_usd"))
    rows = conn.execute("""
        SELECT b.reason, b.ladder_complete,
               (SELECT coalesce(sum(payout_cents), 0) FROM book_snapshot_level l
                WHERE l.book_snapshot_id = b.book_snapshot_id AND l.side = 0),
               (SELECT coalesce(sum(payout_cents), 0) FROM book_snapshot_level l
                WHERE l.book_snapshot_id = b.book_snapshot_id AND l.side = 1)
        FROM book_snapshot b JOIN venue_instrument v USING (venue_instrument_id)
        WHERE v.venue = 'novig' AND b.source = 'stream' AND b.observed_ts_ms BETWEEN ? AND ?""", (lo, hi)).fetchall()
    return {"snapshots": len(rows), "by_reason": dict(Counter(r[0] for r in rows)),
            "cut_at_cap": sum(1 for r in rows if not r[1]),
            "reached": {n: sum(1 for r in rows if min(r[2], r[3]) >= n * 100) for n in notionals}}


def run(conn: sqlite3.Connection, root: Path, lo: int, hi: int, min_per_kind: int = 2) -> Report:
    sealed = archive.sealed_through(root)
    if sealed is None or sealed < hi:
        raise GateError(f"the archive is sealed only through {ms_iso(sealed) if sealed else 'nothing'}; "
                        f"run the gate once it is sealed past {ms_iso(hi)}")
    facts = _Facts(conn, root, lo, hi)
    rep = Report(lo, hi, min_per_kind, read_faults(root, lo, hi))
    used: set[int] = set()
    for f in rep.faults:
        if f.applied is None:
            rep.checks.append(Check(f.fault_id, f.kind, "-", False, "no fault_applied or fault_failed recorded"))
        elif not f.applied:
            continue                                # not injected: listed, not counted
        elif f.kind == "resubscribe":
            rep.checks += check_resubscribe(facts, f, used)
        else:
            exp = expectations(f)
            if not exp:
                rep.checks.append(Check(f.fault_id, f.kind, "-", False, "the fault touched no subscribed market"))
            rep.checks += [check_scope(facts, f, scope, times, used) for scope, times in exp.items()]
    for scope, gaps in sorted(facts.gaps.items()):
        market, channel = parse_scope(scope)
        for g in gaps:
            if g.end_ms is not None and g.reason not in QUARANTINE_REASONS and not (
                    g.end_ref and is_snapshot(facts.frame(g.end_ref), market, channel)):
                rep.checks.append(Check(None, "resync", scope, False,
                                        f"{g.reason} gap closes at {g.end_ref}, which is not a snapshot"))
            elif g.end_ms is not None and g.reason == "sequence_gap" and (
                    got := same_subscription_seq(facts, g, market, channel)) is not None:
                # A lifecycle reset may close at the seq it opened at (the venue drops orders without
                # deltas); a sequence gap exists because a later seq was missed.
                before = frame_seq(facts.frame(g.start_ref), market, channel)
                if before is not None and got <= before:
                    rep.checks.append(Check(None, "resync", scope, False,
                                            f"sequence gap opened at seq {before} closes at a snapshot at seq {got} "
                                            f"({g.end_ref}), which cannot hold what was missed"))
            if g.gap_id not in used:
                rep.unattributed.append(g)
    rep.storage = storage(root, lo, hi)
    rep.depth = depth(conn, lo, hi)
    return rep


def render(rep: Report) -> str:
    applied = rep.applied()
    lines = [f"# Gate A1: {'PASS' if rep.verdict() else 'FAIL'}", "",
             f"Run {ms_iso(rep.lo)} .. {ms_iso(rep.hi)}; at least {rep.min_per_kind} of each fault applied.", "",
             "## Faults", "", "| kind | applied | not applied | no result |", "|---|---:|---:|---:|"]
    for k in KINDS:
        fs = [f for f in rep.faults if f.kind == k]
        flag = "" if applied[k] >= rep.min_per_kind else " (short)"
        lines.append(f"| {k} | {applied[k]}{flag} | {sum(f.applied is False for f in fs)} | "
                     f"{sum(f.applied is None for f in fs)} |")
    lines += ["", "## Checks", ""]
    by_fault = {f.fault_id: f for f in rep.faults}
    for c in rep.checks:
        f = by_fault.get(c.fault_id)
        when = f" at {ms_iso(f.injected_ms)}" if f else ""
        lines.append(f"- {'ok' if c.ok else 'FAIL'} {c.kind}{when} {c.scope}: {c.detail}")
    if not rep.checks:
        lines.append("- none")
    not_applied = [f for f in rep.faults if f.applied is False]
    if not_applied:
        lines += ["", "## Not applied (not counted)", ""]
        lines += [f"- {f.kind} at {ms_iso(f.injected_ms)} {f.target}: {(f.result or {}).get('error')}"
                  for f in not_applied]
    lines += ["", "## Gaps no fault explains", ""]
    lines += [f"- {g.scope} {g.reason} {ms_iso(g.start_ms)} .. {ms_iso(g.end_ms) if g.end_ms else 'open'}"
              for g in rep.unattributed] or ["- none"]
    lines += ["", "## Storage (sealed segments)", "", "| day | source | segments | MB | frames | unclean |",
              "|---|---|---:|---:|---:|---:|"]
    lines += [f"| {d} | {s} | {n} | {b / 1e6:.1f} | {fr} | {u} |" for d, s, n, b, fr, u in rep.storage]
    d = rep.depth
    lines += ["", "## Depth (Novig stream book snapshots)", "",
              f"- snapshots: {d['snapshots']} ({', '.join(f'{k} {v}' for k, v in sorted(d['by_reason'].items()))})",
              f"- cut at the venue's level cap: {d['cut_at_cap']}{_pct(d['cut_at_cap'], d['snapshots'])}"]
    lines += [f"- both sides reach ${n}: {k}{_pct(k, d['snapshots'])}" for n, k in d["reached"].items()]
    return "\n".join(lines) + "\n"


def _pct(n: int, total: int) -> str:
    return f" ({100 * n / total:.1f}%)" if total else ""
