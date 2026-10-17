"""The versioned close function (DESIGN.md §5.3, §5.4).

    close_def + off resolution + fact snapshot + outcome -> close price OR unscoreable reason

    cutoff = earliest plausible start - buffer_s
    book   = the reference instrument's latest tick at or before the cutoff, from the
             reference source with the freshest liveness evidence
    price  = benchmark(book), on the outcome's canonical ladder

The same function prices the reference at an entry's decision time (null EV),
with the decision time in place of the cutoff.

A close definition carries every value it uses (`CloseDef.params()`), so a
changed parameter is a new definition with a new hash, never a new meaning for
an old one (DESIGN.md §10). Checks, in order, each an unscoreable reason:

    no_trusted_off / off_disagreement   the off resolution is untrusted
    unmapped / ambiguous_reference /    no single direct mapping of the venue to the
      mapping_inconsistent              outcome, or the book's other side contradicts it
    no_quotes                           no tick at or before the cutoff
    collection_gap / crossed_book       a gap on the book's scope overlaps (tick, cutoff]; a quarantine
                                        gap (a later state replaced the book and was quarantined) is
                                        reported by its own reason
    feed_stalled                        liveness evidence older than the source's limit, and (a stream
                                        book) no later observation continuing the same state
    halted                              the book's venue status is not open
    venue_lag                           received more than stream.max_venue_lag_ms after venue time
    stale_book                          the book is older than close.max_quote_age_s
    no_quotes / insufficient_depth /    the benchmark cannot be computed (clv.close.depth)
      truncated_ladder

The historical family (`HistCloseDef`, DESIGN.md §5.6) prices the same reference
from a candle (clv.close.candles) or a trade window (clv.close.vwap) instead of a
tick, through `hist_price_as_of`:

    close_hist_candle_mid_top   the last candle wholly before the as-of time
    close_hist_vwap             unique executions in [as_of - window, as_of); few_trades
                                below close.hist_vwap_min_executions or _min_notional_usd

A stream book with no evidence within its limit is still live when it is bracketed: the first
channel observation after the as-of time continues the same state (a contiguous delta that left
it unchanged or changed it on the same snapshot chain, or a confirmed probe), with no gap between
(docs/decisions/2026-10-17-a1-probe-policy.md). An unchanged sequence proves the venue changed
nothing in between, and every break (a sequence gap, a lifecycle transition, a reconnect) is a
gap; a fresh snapshot (a subscription or resync) never brackets. The later observation decides
only that the book chosen at or before the as-of time was still live, never which book or its
price.

Cohort eligibility (mapping status, settlement equivalence) is not a close
precondition here: a close is priced whenever the book permits, and clv_score
lists the cohort exclusions beside it (clv.score.clv).
"""
from __future__ import annotations

import hashlib
import sqlite3
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from fractions import Fraction

from clv import lineage
from clv.close import depth
from clv.close.candles import candle_close
from clv.close.vwap import Executions
from clv.config import param
from clv.gaps import QUARANTINE_REASONS
from clv.identity import Mapping, canonical_ladder
from clv.off.resolver import RESOLVER_VERSION
from clv.venues.protocol import PRICE_SCALE, BinaryBook, Level

CLOSE_VERSION = "close-v0.2"           # v0.2: a stream book's liveness may come from a bracketing observation
HIST_CLOSE_VERSION = "close-hist-v0.1"
# The venue status in which a book is tradeable (docs/vendor-capabilities.md).
OPEN_STATUS = {"novig": "OPEN", "kalshi": "active"}
# Reference sources per venue. Novig was polled before its stream was available.
SOURCES = {"novig": ("stream", "poll"), "kalshi": ("poll",)}


@dataclass(frozen=True)
class CloseDef:
    name: str
    benchmark: str                      # "mid_top" | "mid_depth"
    notional_usd: int | None
    buffer_s: int
    primary: bool
    max_quote_age_s: int
    stream_liveness_max_s: int
    poll_liveness_max_s: int
    max_venue_lag_ms: int
    depth_notional_basis: str
    sources: dict = field(default_factory=lambda: {v: list(s) for v, s in SOURCES.items()})
    open_status: dict = field(default_factory=lambda: dict(OPEN_STATUS))
    version: str = CLOSE_VERSION
    off_resolver: str = RESOLVER_VERSION

    family = "close_live"

    def params(self) -> dict:
        return asdict(self)

    def params_json(self) -> str:
        return lineage.canonical_json(self.params())


@dataclass(frozen=True)
class HistCloseDef:
    """A historical close (DESIGN.md §5.6): a weaker proxy than a book close, never pooled with one.
    It carries only the values its method uses, so changing another method's value never makes
    a new definition of this one."""
    name: str
    method: str                         # "candle_mid_top" | "trade_vwap"
    buffer_s: int
    max_quote_age_s: int
    candle_rule: str | None = None
    vwap_window_s: int | None = None
    vwap_min_executions: int | None = None
    vwap_min_notional_usd: int | None = None
    version: str = HIST_CLOSE_VERSION
    off_resolver: str = RESOLVER_VERSION

    family = "close_hist"

    def params(self) -> dict:
        return asdict(self)

    def params_json(self) -> str:
        return lineage.canonical_json(self.params())


def hist_defs(buffers: list[int] | None = None) -> list[HistCloseDef]:
    """The historical family at each buffer (default close.buffer_s): the candle and the VWAP close."""
    out = []
    for b in [param("close.buffer_s")] if buffers is None else buffers:
        age = param("close.max_quote_age_s")
        out.append(HistCloseDef("close_hist_candle_mid_top", "candle_mid_top", b, age,
                                candle_rule=param("close.hist_candle_rule")))
        out.append(HistCloseDef("close_hist_vwap", "trade_vwap", b, age,
                                vwap_window_s=param("close.hist_vwap_window_s"),
                                vwap_min_executions=param("close.hist_vwap_min_executions"),
                                vwap_min_notional_usd=param("close.hist_vwap_min_notional_usd")))
    return out


def live_defs() -> list[CloseDef]:
    """The live close family at `close.buffer_s`: mid_top and each depth notional."""
    common = dict(buffer_s=param("close.buffer_s"), max_quote_age_s=param("close.max_quote_age_s"),
                  stream_liveness_max_s=param("stream.liveness_max_s"),
                  poll_liveness_max_s=param("poll.liveness_max_s"),
                  max_venue_lag_ms=param("stream.max_venue_lag_ms"),
                  depth_notional_basis=param("close.depth_notional_basis"))
    primary = param("close.primary_benchmark")
    defs = [CloseDef("close_live_mid_top", "mid_top", None, primary=primary == "mid_top", **common)]
    for n in param("close.depth_notionals_usd"):
        defs.append(CloseDef(f"close_live_mid_depth_{n}", "mid_depth", n, primary=primary == f"mid_depth_{n}",
                             **common))
    return defs


def ensure_def(conn: sqlite3.Connection, d: CloseDef | HistCloseDef, now_ms: int) -> int:
    text = d.params_json()
    sha = hashlib.sha256(text.encode()).hexdigest()
    row = conn.execute("SELECT close_def_id FROM close_def WHERE params_sha256 = ?", (sha,)).fetchone()
    if row:
        return row[0]
    return conn.execute("INSERT INTO close_def (name, family, params_json, params_sha256, created_ts_ms)"
                        " VALUES (?, ?, ?, ?, ?)", (d.name, d.family, text, sha, now_ms)).lastrowid


# -- the reference instrument ----------------------------------------------------------------------

@dataclass(frozen=True)
class Reference:
    venue: str
    native_id: str
    venue_instrument_id: int
    side: int
    mapping: Mapping


def reference(conn: sqlite3.Connection, heads: dict[tuple[int, int], Mapping], venue: str,
              outcome_id: int) -> Reference | str:
    """The venue's instrument side mapped `direct` to the outcome, or why there is none.

    Kalshi lists one ticker per team: the outcome's own ticker is its reference, and the
    other team's ticker is a separate book, never merged in (measurement contract §4)."""
    return References(conn, heads).get(venue, outcome_id)


class References:
    """`reference` for many outcomes: the venues' instruments and the mapping heads indexed once."""

    def __init__(self, conn: sqlite3.Connection, heads: dict[tuple[int, int], Mapping]):
        self.heads = heads
        self.instruments = {(v, iid): native for iid, v, native in conn.execute(
            "SELECT venue_instrument_id, venue, native_id FROM f_venue_instrument")}
        self.venue_of = {iid: v for v, iid in self.instruments}
        self.direct: dict[int, list[Mapping]] = {}
        for m in heads.values():
            if m.polarity == "direct" and m.status != "rejected":
                self.direct.setdefault(m.outcome_id, []).append(m)

    def get(self, venue: str, outcome_id: int) -> Reference | str:
        found = [m for m in self.direct.get(outcome_id, ()) if self.venue_of.get(m.venue_instrument_id) == venue]
        if not found:
            return "unmapped"
        if len(found) > 1:
            return "ambiguous_reference"
        (m,) = found
        other = self.heads.get((m.venue_instrument_id, 1 - m.side))
        if other is not None and other.status != "rejected" and (
                (other.polarity == "direct") == (other.outcome_id == outcome_id)):
            return "mapping_inconsistent"   # the other side claims this same outcome
        return Reference(venue, self.instruments[(venue, m.venue_instrument_id)], m.venue_instrument_id, m.side, m)


# -- pricing at a moment -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Price:
    p: Fraction | None
    reason: str | None
    tick_id: int | None = None
    observed_ms: int | None = None
    source: str | None = None
    quote_age_ms: int | None = None
    liveness_age_ms: int | None = None
    bracket_ms: int | None = None       # the later observation that vouched for a quiet book, if one did
    spread_e4: int | None = None
    bench: depth.Benchmark | None = None
    # A historical close's lineage instead of a tick: a candle, or a trade window and its counts.
    candle_id: int | None = None
    cutoff_candle_active: bool | None = None
    vwap_lo_ms: int | None = None
    vwap_hi_ms: int | None = None
    vwap_executions: int | None = None
    vwap_payout_cents: int | None = None


def book_of(conn: sqlite3.Connection, tick_id: int, venue: str, native_id: str) -> BinaryBook:
    """A stored tick's two native sides as a book whose quantities are payout cents."""
    bids: dict[int, list[Level]] = {0: [], 1: []}
    for side, price_native, cents in conn.execute(
            "SELECT side, price_native, payout_cents FROM f_tick_level WHERE tick_id = ? ORDER BY side, rank",
            (tick_id,)):
        bids[side].append(Level(price_native, Decimal(cents)))
    return BinaryBook(venue, native_id, {s: tuple(lv) for s, lv in bids.items()}, Decimal("0.01"), True,
                      "stored", None, None, 0)


def benchmark(d: CloseDef, bids: tuple[Level, ...], asks: tuple[Level, ...], truncated: bool) -> depth.Benchmark:
    if d.benchmark == "mid_top":
        return depth.mid_top(bids, asks)
    if d.benchmark == "mid_depth":
        return depth.mid_depth(bids, asks, d.notional_usd, truncated)
    raise ValueError(f"unknown benchmark {d.benchmark!r}")


def price_as_of(conn: sqlite3.Connection, ref: Reference, as_of_ms: int, d: CloseDef) -> Price:
    """The reference's benchmark price as the facts stood at `as_of_ms`, or why there is none."""
    best = None
    for source in d.sources[ref.venue]:
        tick = conn.execute(
            "SELECT tick_id, observed_ts_ms, venue_ts_ms, venue_status, truncated, ladder_complete FROM f_tick"
            " WHERE venue_instrument_id = ? AND source = ? AND observed_ts_ms <= ?"
            " ORDER BY observed_ts_ms DESC, tick_id DESC LIMIT 1",
            (ref.venue_instrument_id, source, as_of_ms)).fetchone()
        if tick is None:
            continue
        evidence = conn.execute(
            "SELECT max(observed_ts_ms) FROM f_liveness_evidence WHERE venue_instrument_id = ? AND source = ?"
            " AND observed_ts_ms <= ?", (ref.venue_instrument_id, source, as_of_ms)).fetchone()[0]
        last = max(tick[1], evidence or tick[1])
        if best is None or last > best[2]:
            best = (source, tick, last)
    if best is None:
        return Price(None, "no_quotes")
    source, (tick_id, observed, venue_ts, status, truncated, complete), last = best
    seen = dict(tick_id=tick_id, observed_ms=observed, source=source, quote_age_ms=as_of_ms - observed,
                liveness_age_ms=as_of_ms - last)

    scope = f"{ref.venue}:market:{ref.native_id}" + ("/book" if source == "stream" else "")
    limit_s = d.stream_liveness_max_s if source == "stream" else d.poll_liveness_max_s
    vouched = None
    if seen["liveness_age_ms"] > limit_s * 1000 and source == "stream":
        vouched = bracket(conn, ref.venue_instrument_id, tick_id, as_of_ms)
    # No gap may overlap the stored book's interval up to the as-of time, or up to the bracket.
    gap = conn.execute("""
        SELECT o.reason FROM f_collection_gap o
        LEFT JOIN f_collection_gap c ON c.opens_gap_id = o.collection_gap_id
        WHERE o.kind = 'open' AND o.scope = ? AND o.boundary_ts_ms < ?
          AND (c.boundary_ts_ms IS NULL OR c.boundary_ts_ms > ?)
        ORDER BY o.boundary_ts_ms, o.collection_gap_id LIMIT 1""",
                       (scope, as_of_ms if vouched is None else vouched, observed)).fetchone()
    seen["bracket_ms"] = vouched
    book = book_of(conn, tick_id, ref.venue, ref.native_id)
    bids, asks = canonical_ladder(book, ref.side, "direct")
    spread = (round((Fraction(asks[0].price) - Fraction(bids[0].price)) * PRICE_SCALE)
              if bids and asks else None)
    if gap:
        reason = gap[0] if gap[0] in QUARANTINE_REASONS else "collection_gap"
    elif seen["liveness_age_ms"] > limit_s * 1000 and vouched is None:
        reason = "feed_stalled"
    elif status != d.open_status[ref.venue]:
        reason = "halted"
    elif venue_ts is not None and observed - venue_ts > d.max_venue_lag_ms:
        reason = "venue_lag"
    elif seen["quote_age_ms"] > d.max_quote_age_s * 1000:
        reason = "stale_book"
    else:
        bench = benchmark(d, bids, asks, bool(truncated) or not complete)
        return Price(bench.p, bench.reason, spread_e4=spread, bench=bench, **seen)
    return Price(None, reason, spread_e4=spread, **seen)


def bracket(conn: sqlite3.Connection, instrument_id: int, tick_id: int, as_of_ms: int) -> int | None:
    """When the first stream observation after `as_of_ms` continues the state of `tick_id` (the
    latest stream tick at or before it), that observation's time; otherwise None.

    It continues the state when it is liveness evidence (a delta that left the book unchanged, or
    a confirmed probe) before any later tick, since the ingest writes evidence only while the
    stored state is current; or when it is a later tick resting on the same snapshot, a
    contiguous delta that changed the book. A tick on a new snapshot (a subscription or resync)
    starts new state and never brackets."""
    snap = conn.execute("SELECT snapshot_raw_artifact_id, snapshot_raw_line FROM f_tick WHERE tick_id = ?",
                        (tick_id,)).fetchone()
    nxt = conn.execute(
        "SELECT observed_ts_ms, snapshot_raw_artifact_id, snapshot_raw_line FROM f_tick"
        " WHERE venue_instrument_id = ? AND source = 'stream' AND observed_ts_ms > ?"
        " ORDER BY observed_ts_ms, tick_id LIMIT 1", (instrument_id, as_of_ms)).fetchone()
    evidence = conn.execute(
        "SELECT min(observed_ts_ms) FROM f_liveness_evidence WHERE venue_instrument_id = ? AND source = 'stream'"
        " AND observed_ts_ms > ?", (instrument_id, as_of_ms)).fetchone()[0]
    if evidence is not None and (nxt is None or evidence < nxt[0]):
        return evidence
    if nxt is not None and snap[0] is not None and tuple(nxt[1:]) == tuple(snap):
        return nxt[0]
    return None


def write_close(conn: sqlite3.Connection, snapshot_id: int, def_id: int, off_id: int, venue: str, outcome_id: int,
                mapping_resolution_id: int | None, cutoff_ms: int | None, p: Price, now_ms: int) -> int:
    """One close_price row: the price or the reason, with its tick, candle or trade window."""
    num, den = (None, None) if p.p is None else (p.p.numerator, p.p.denominator)
    return conn.execute(
        "INSERT INTO close_price (fact_snapshot_id, close_def_id, off_resolution_id, ref_venue, outcome_id,"
        " mapping_resolution_id, cutoff_ts_ms, book_tick_id, book_observed_ts_ms, quote_age_ms, liveness_age_ms,"
        " p_close_num, p_close_den, spread_e4, unscoreable_reason, candle_id, cutoff_candle_active, vwap_lo_ms,"
        " vwap_hi_ms, vwap_executions, vwap_payout_cents, computed_ts_ms)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (snapshot_id, def_id, off_id, venue, outcome_id, mapping_resolution_id, cutoff_ms, p.tick_id, p.observed_ms,
         p.quote_age_ms, p.liveness_age_ms, num, den, p.spread_e4, p.reason, p.candle_id,
         None if p.cutoff_candle_active is None else int(p.cutoff_candle_active), p.vwap_lo_ms, p.vwap_hi_ms,
         p.vwap_executions, p.vwap_payout_cents, now_ms)).lastrowid


def hist_price_as_of(conn: sqlite3.Connection, ref: Reference, as_of_ms: int, d: HistCloseDef,
                     executions: Executions) -> Price:
    """The reference's historical close as of `as_of_ms`, from the current fact snapshot
    (`lineage.use`; `executions` must read the same snapshot), or why there is none."""
    if d.method == "candle_mid_top":
        c = candle_close(conn, ref.venue_instrument_id, ref.side, as_of_ms, d.max_quote_age_s)
        return Price(c.p, c.reason, quote_age_ms=c.quote_age_ms, spread_e4=c.spread_e4, candle_id=c.candle_id,
                     cutoff_candle_active=c.cutoff_candle_active)
    if d.method == "trade_vwap":
        lo = as_of_ms - d.vwap_window_s * 1000
        v = executions.vwap(ref.venue_instrument_id, ref.side, lo, as_of_ms)
        enough = v.executions >= d.vwap_min_executions and v.payout_cents >= d.vwap_min_notional_usd * 100
        return Price(v.p if enough else None, None if enough else "few_trades",
                     quote_age_ms=None if v.last_ms is None else as_of_ms - v.last_ms, vwap_lo_ms=lo,
                     vwap_hi_ms=as_of_ms, vwap_executions=v.executions, vwap_payout_cents=v.payout_cents)
    raise ValueError(f"unknown historical close method {d.method!r}")
