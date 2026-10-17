"""The A1 storage pilot report: what the follow-up record to the probe policy needs
(docs/decisions/2026-10-17-a1-probe-policy.md, Consequences) to set
stream.quiet_probe_interval_s, the dense window and book.full_snapshot_interval_s.

    clv pilot --from 2026-11-03T00:00Z --to 2026-11-10T00:00Z

Read-only: it writes nothing, not even a fact snapshot. It reads the archive (sealed past
`--to`) and the facts `clv ingest --league-events` loaded from it, for every league event whose
capture window overlaps the period. Sealing past `--to` does not settle them all: an event
starting after the period is recorded during it (from a1.capture_lead_s before its start) but
loads only once its own window has closed. Every overlapping event the recorder discovered and
the database lacks is listed first, and the report is then incomplete: such events count in
storage and probes but in no other section.

- storage: sealed segments, bytes and frames per day and source, as Gate A1 reports them;
- probes: per channel, the markets probed and the markets skipped for budget, inside and
  outside a dense window (the recorder's probe_sent and probe_skipped), with the tokens the
  sent probes cost against the stream bucket's refill;
- quiet intervals: for each event's book, the time between consecutive venue activity (a
  stream tick, or a delta that left the stored top unchanged), by when the interval began
  relative to the scheduled start. An interval a collection gap overlaps is not quiet, it is
  missing, and is counted apart;
- cutoffs: the live close function's verdict (close/definitions.py, the mid_top definition)
  at each close cutoff (close.buffer_sweep_s before the start) and each check-1 offset
  (controls.martingale_offsets): live on recent evidence, live on a bracketing observation,
  or the reason it is not;
- depth: as Gate A1, how often a stream book snapshot was cut at the venue's level cap, and
  how often both sides reached each close.depth_notionals_usd;
- listing lead: how long before its start each market first appeared in an archived catalog
  listing. A market already listed in the first listing archived has a lead of at least that
  much, and is marked so.

"The start" is Novig's startsTs, as the recorder discovered it: the trusted start bound is
A2's, and does not exist yet. Times to start are display buckets, never parameters: the
check-1 offsets and the dense window bound them.
"""
from __future__ import annotations

import itertools
import json
import math
import sqlite3
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import parse_qs

from clv import archive, lineage
from clv.close.definitions import Reference, live_defs, price_as_of
from clv.config import param
from clv.games import league_events, league_window
from clv.gate.a1 import depth, storage
from clv.ingest import unsettled
from clv.timeutil import iso_ms, ms_iso

# Novig's per-market cost of a snapshot probe, and the `stream` bucket's refill
# (docs/vendor-capabilities.md). The recorder spends by the same figures (raw_recorder.novig.stream
# CHANNEL_WEIGHT and TokenBudget); tests hold them equal.
PROBE_COST = {"book": 16, "trades": 4}
STREAM_REFILL_PER_S = 4.0
CATALOG_PATH = "/v3/public/catalog/markets"
CATALOG_STREAMS = ("discovery", "catalog")      # the recorder's discovery rounds and `raw-recorder catalog`
# A listing round's pages arrive within seconds, and rounds are an hour apart ([discovery]
# interval_s): a market first seen this soon after its league's first listing was already listed.
ROUND_MS = 600_000


class PilotError(Exception):
    pass


@dataclass(frozen=True)
class Event:
    event_id: str
    league: str
    label: str
    start_ms: int
    window_start_ms: int
    window_end_ms: int
    markets: tuple[str, ...]


@dataclass
class Probes:
    sent: Counter = field(default_factory=Counter)        # (channel, "dense" | "quiet" | "unclassified") -> count
    skipped: Counter = field(default_factory=Counter)

    def tokens(self) -> float:
        return sum(PROBE_COST.get(ch, 0) * n for (ch, _), n in self.sent.items())


@dataclass
class Bucket:
    name: str
    lo_s: float                                           # seconds relative to the start: [lo, hi)
    hi_s: float
    quiet_s: list[float] = field(default_factory=list)
    gapped: int = 0


@dataclass
class Report:
    lo: int
    hi: int
    events: list[Event]
    storage: list[tuple] = field(default_factory=list)
    probes: Probes = field(default_factory=Probes)
    buckets: list[Bucket] = field(default_factory=list)
    cutoffs: dict[str, Counter] = field(default_factory=dict)     # as-of label -> verdict counts
    depth: dict = field(default_factory=dict)
    leads: dict[str, tuple[int, bool]] = field(default_factory=dict)  # market -> (lead ms, censored)
    unlisted: list[str] = field(default_factory=list)
    missing: list[tuple[str, str, int, str]] = field(default_factory=list)   # (event, label, start, why)

    def complete(self) -> bool:
        return not self.missing


def events_in(conn: sqlite3.Connection, lo: int, hi: int) -> list[Event]:
    return [Event(e, lg, label, s, ws, we, tuple(json.loads(m))) for e, lg, label, s, ws, we, m in conn.execute(
        "SELECT native_event_id, league, label, scheduled_start_ms, window_start_ms, window_end_ms, markets_json"
        " FROM venue_ingest WHERE venue = 'novig' AND window_end_ms > ? AND window_start_ms < ?"
        " ORDER BY scheduled_start_ms, native_event_id", (lo, hi))]


def missing(root: Path, lo: int, hi: int, ingested: set[str]) -> list[tuple[str, str, int, str]]:
    """Discovered league events whose windows overlap the period and are not ingested, and why."""
    out = []
    for e in league_events(root):
        a, b = league_window(e)
        if b > lo and a < hi and e["event_id"] not in ingested:
            why = unsettled(Path(root), e) or "settled, not ingested yet: run `clv ingest --league-events`"
            out.append((e["event_id"], e.get("label", ""), iso_ms(e["scheduled_start_utc"]), why))
    return sorted(out, key=lambda m: (m[2], m[0]))


def probes(root: Path, lo: int, hi: int) -> Probes:
    out = Probes()
    for f in archive.window(root, "novig", lo, hi):
        if f.dir != "event":
            continue
        e = json.loads(f.frame)
        kind = e.get("type")
        if kind not in ("probe_sent", "probe_skipped"):
            continue
        # A recorder older than the probe policy archived no outside_dense: its probes are unclassified.
        quiet = set(e["outside_dense"]) if "outside_dense" in e else None
        tally = out.sent if kind == "probe_sent" else out.skipped
        for m in e.get("markets", ()):
            tally[(e.get("channel"), "unclassified" if quiet is None else "quiet" if m in quiet else "dense")] += 1
    return out


def buckets() -> list[Bucket]:
    """Times to start, bounded by the check-1 offsets above the dense window, the dense window,
    and after it."""
    lead, tail = param("stream.dense_probe_lead_s"), param("stream.dense_probe_tail_s")
    edges = sorted({o for o in param("controls.martingale_offsets") if o > lead}, reverse=True)
    out = [Bucket(f"more than {_dur(edges[0])} before", float("-inf"), -edges[0])]
    for a, b in zip(edges, [*edges[1:], lead]):
        out.append(Bucket(f"{_dur(a)} to {_dur(b)} before", -a, -b))
    out.append(Bucket(f"dense window ({_dur(lead)} before to {_dur(tail)} after)", -lead, tail))
    out.append(Bucket(f"more than {_dur(tail)} after", tail, float("inf")))
    return out


def quiet(conn: sqlite3.Connection, ev: Event, iid: int, scope: str, lo: int, hi: int, into: list[Bucket]) -> None:
    a, b = max(lo, ev.window_start_ms), min(hi, ev.window_end_ms)
    times = sorted(t for (t,) in conn.execute(
        "SELECT observed_ts_ms FROM tick WHERE venue_instrument_id = ? AND source = 'stream'"
        " AND observed_ts_ms BETWEEN ? AND ?"
        " UNION ALL SELECT observed_ts_ms FROM liveness_evidence WHERE venue_instrument_id = ?"
        " AND kind = 'delta_unchanged' AND observed_ts_ms BETWEEN ? AND ?", (iid, a, b, iid, a, b)))
    gaps = [(o, c if c is not None else float("inf")) for o, c in conn.execute(
        "SELECT o.boundary_ts_ms, c.boundary_ts_ms FROM collection_gap o"
        " LEFT JOIN collection_gap c ON c.opens_gap_id = o.collection_gap_id"
        " WHERE o.kind = 'open' AND o.scope = ?", (scope,))]
    for t0, t1 in itertools.pairwise(times):
        if t1 == t0:
            continue
        rel = (t0 - ev.start_ms) / 1000
        bucket = next(bk for bk in into if bk.lo_s <= rel < bk.hi_s)
        if any(o < t1 and c > t0 for o, c in gaps):
            bucket.gapped += 1
        else:
            bucket.quiet_s.append((t1 - t0) / 1000)


def cutoffs(conn: sqlite3.Connection, ev: Event, iid: int, native: str, into: dict[str, Counter]) -> None:
    d = live_defs()[0]
    ref = Reference("novig", native, iid, 0, None)
    points = [(f"offset {_dur(o)}", o) for o in param("controls.martingale_offsets")]
    points += [(f"cutoff {_dur(b) if b else '0 s'}", b) for b in param("close.buffer_sweep_s")]
    for label, before_s in points:
        p = price_as_of(conn, ref, ev.start_ms - before_s * 1000, d)
        verdict = p.reason or ("live, bracketed" if p.bracket_ms is not None else "live, recent")
        into.setdefault(label, Counter())[verdict] += 1


def listing_leads(root: Path, hi: int, markets: dict[str, tuple[int, str]]
                  ) -> tuple[dict[str, tuple[int, bool]], list[str]]:
    """Each market's lead (its start less its first archived catalog listing), and whether its
    league's first listing round archived already had it, so the lead is at least that.
    `markets` maps a market to its (start, league)."""
    first: dict[str, int] = {}
    earliest: dict[str, int] = {}                          # league -> its first listing archived
    segs = [s for s in archive.overlapping(archive.segments(root, "novig"), 0, hi)
            if s.stream_id.startswith(CATALOG_STREAMS)]
    for x in archive.rest_exchanges(archive.iter_frames(root, segs)):
        if x.request.get("path") != CATALOG_PATH or x.body is None or not x.ok or x.body.recv_ts_ms > hi:
            continue
        league = (parse_qs(x.request.get("query") or "").get("league") or [""])[0]
        try:
            items = json.loads(x.body.frame).get("items") or []
        except (ValueError, AttributeError):
            continue
        t = x.body.recv_ts_ms
        earliest[league] = min(earliest.get(league, t), t)
        for m in items:
            mid = m.get("marketId") if isinstance(m, dict) else None
            if mid in markets and (mid not in first or t < first[mid]):
                first[mid] = t
    leads = {m: (markets[m][0] - t, t - earliest.get(markets[m][1], t) < ROUND_MS) for m, t in first.items()}
    return leads, sorted(set(markets) - set(first))


def run(conn: sqlite3.Connection, root: Path, lo: int, hi: int) -> Report:
    sealed = archive.sealed_through(root)
    if sealed is None or sealed < hi:
        raise PilotError(f"the archive is sealed only through {ms_iso(sealed) if sealed else 'nothing'}; "
                         f"run the report once it is sealed past {ms_iso(hi)}")
    lineage.use_current(conn)
    rep = Report(lo, hi, events_in(conn, lo, hi))
    rep.missing = missing(root, lo, hi, {e.event_id for e in rep.events})
    rep.storage = storage(root, lo, hi)
    rep.probes = probes(root, lo, hi)
    rep.buckets = buckets()
    starts: dict[str, tuple[int, str]] = {}
    for ev in rep.events:
        for native in ev.markets:
            starts[native] = (ev.start_ms, ev.league)
            row = conn.execute("SELECT venue_instrument_id FROM venue_instrument WHERE venue = 'novig'"
                               " AND native_id = ?", (native,)).fetchone()
            if row is None:
                continue
            quiet(conn, ev, row[0], f"novig:market:{native}/book", lo, hi, rep.buckets)
            cutoffs(conn, ev, row[0], native, rep.cutoffs)
    rep.depth = depth(conn, lo, hi)
    rep.leads, rep.unlisted = listing_leads(root, hi, starts)
    return rep


def render(rep: Report) -> str:
    hours = (rep.hi - rep.lo) / 3_600_000
    lines = ["# A1 storage pilot", "", (f"Period {ms_iso(rep.lo)} .. {ms_iso(rep.hi)} ({hours:,.1f} h); "
             f"{len(rep.events)} league event(s), {sum(len(e.markets) for e in rep.events)} market(s) ingested."), ""]
    if rep.missing:
        lines += [(f"**Incomplete:** {len(rep.missing)} discovered league event(s) whose capture windows overlap the"
                   " period are not ingested. Storage and probes include them; the other sections do not. Rerun"
                   " once they are ingested."), ""]
        lines += [f"- {e} {label} (starts {ms_iso(s)}): {why}" for e, label, s, why in rep.missing] + [""]
    else:
        lines += ["Every discovered league event whose window overlaps the period is ingested.", ""]
    lines += ["## Storage (sealed segments)", "", "| day | source | segments | MB | frames | unclean |",
              "|---|---|---:|---:|---:|---:|"]
    lines += [f"| {d} | {s} | {n} | {b / 1e6:.1f} | {fr} | {u} |" for d, s, n, b, fr, u in rep.storage]
    total = sum(r[3] for r in rep.storage)
    days = len({r[0] for r in rep.storage})
    lines += ["", f"Total {total / 1e6:,.1f} MB over {days} day(s); {total / 1e6 / max(days, 1):,.1f} MB a day.", ""]

    lines += ["## Probes", "", "Market-probes per channel, inside (dense) and outside (quiet) a dense window.", "",
              "| channel | sent, dense | sent, quiet | skipped, dense | skipped, quiet | tokens |",
              "|---|---:|---:|---:|---:|---:|"]
    p = rep.probes
    for ch in sorted({c for c, _ in p.sent} | {c for c, _ in p.skipped}):
        tok = PROBE_COST.get(ch, 0) * sum(n for (c, _), n in p.sent.items() if c == ch)
        lines.append(f"| {ch} | {p.sent[(ch, 'dense')]} | {p.sent[(ch, 'quiet')]} | {p.skipped[(ch, 'dense')]} | "
                     f"{p.skipped[(ch, 'quiet')]} | {tok:,} |")
    unclassified = sum(n for (_, k), n in (p.sent + p.skipped).items() if k == "unclassified")
    if unclassified:
        lines += ["", (f"{unclassified} market-probe(s) came from a recorder older than the probe policy, which"
                       " did not record dense windows: they are in neither column, and their tokens are counted.")]
    secs = (rep.hi - rep.lo) / 1000
    lines += ["", f"Probe tokens: {p.tokens():,.0f}, {p.tokens() / secs:.2f} a second against a refill of "
              f"{STREAM_REFILL_PER_S:g} a second." if secs else "", ""]

    liveness, cadence = param("stream.liveness_max_s"), param("stream.quiet_probe_interval_s")
    lines += ["## Quiet intervals (book)", "",
              ("Time between consecutive venue activity on a book, by when the interval began. "
              "An interval a collection gap overlaps is missing, not quiet."), "",
              (f"| time to start | intervals | median s | p90 s | max s | over {liveness:g} s | over {cadence:g} s | "
              "missing |"), "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for bk in rep.buckets:
        q = sorted(bk.quiet_s)
        lines.append(f"| {bk.name} | {len(q)} | {_q(q, 0.5)} | {_q(q, 0.9)} | {_q(q, 1.0)} | "
                     f"{sum(x > liveness for x in q)} | {sum(x > cadence for x in q)} | {bk.gapped} |")

    lines += ["", "## Cutoffs (live close function, mid_top)", "",
              "Each market's verdict at each check-1 offset and close cutoff before the start."]
    verdicts = sorted({v for c in rep.cutoffs.values() for v in c}, key=lambda v: (not v.startswith("live"), v))
    lines += ["", "| as of | " + " | ".join(verdicts) + " |", "|---|" + "---:|" * len(verdicts)]
    for label, c in rep.cutoffs.items():
        lines.append(f"| {label} | " + " | ".join(str(c[v]) for v in verdicts) + " |")
    if not rep.cutoffs:
        lines.append("| none | |")

    d = rep.depth
    lines += ["", "## Depth (Novig stream book snapshots)", "",
              f"- snapshots: {d['snapshots']} ({', '.join(f'{k} {v}' for k, v in sorted(d['by_reason'].items()))})",
              f"- cut at the venue's level cap: {d['cut_at_cap']}{_pct(d['cut_at_cap'], d['snapshots'])}"]
    lines += [f"- both sides reach ${n}: {k}{_pct(k, d['snapshots'])}" for n, k in d["reached"].items()]

    lead_s = param("a1.capture_lead_s")
    leads = sorted(v[0] / 1000 for v in rep.leads.values())
    censored = sum(v[1] for v in rep.leads.values())
    lines += ["", "## Listing lead", "",
              ("How long before its start each market first appeared in an archived catalog listing. A market"
               " already in its league's first listing archived was listed earlier still: its lead is a lower"
               " bound."), "",
              (f"- markets: {len(leads)} listed ({censored} in the first listing archived), {len(rep.unlisted)}"
               " never seen in a listing"),
              f"- lead: min {_dur_or(leads, 0.0)}, median {_dur_or(leads, 0.5)}, max {_dur_or(leads, 1.0)}",
              (f"- listed at least a1.capture_lead_s ({_dur(lead_s)}) ahead: "
              f"{sum(x >= lead_s for x in leads)}{_pct(sum(x >= lead_s for x in leads), len(leads))}")]
    for o in sorted(param("controls.martingale_offsets"), reverse=True):
        n = sum(x >= o for x in leads)
        lines.append(f"- listed at least {_dur(o)} ahead: {n}{_pct(n, len(leads))}")
    return "\n".join(lines) + "\n"


def _rank(xs: list[float], q: float) -> float:
    """Nearest-rank quantile of sorted `xs`: the smallest value with at least q of them at or below."""
    return xs[min(len(xs) - 1, max(0, math.ceil(q * len(xs)) - 1))]


def _q(xs: list[float], q: float) -> str:
    return f"{_rank(xs, q):,.0f}" if xs else "-"


def _dur(s: float) -> str:
    s = float(s)
    if s >= 3600 and s % 3600 == 0:
        return f"{s / 3600:g} h"
    if s >= 60 and s % 60 == 0:
        return f"{s / 60:g} min"
    return f"{s:g} s"


def _dur_or(xs: list[float], q: float) -> str:
    return f"{_rank(xs, q) / 3600:,.1f} h" if xs else "-"


def _pct(n: int, total: int) -> str:
    return f" ({100 * n / total:.1f}%)" if total else ""
