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
    collection_gap                      a gap on the book's scope overlaps (tick, cutoff]
    feed_stalled                        liveness evidence older than the source's limit
    halted                              the book's venue status is not open
    venue_lag                           received more than stream.max_venue_lag_ms after venue time
    stale_book                          the book is older than close.max_quote_age_s
    no_quotes / insufficient_depth /    the benchmark cannot be computed (clv.close.depth)
      truncated_ladder

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
from clv.config import param
from clv.identity import Mapping, canonical_ladder
from clv.off.resolver import RESOLVER_VERSION
from clv.venues.protocol import PRICE_SCALE, BinaryBook, Level

CLOSE_VERSION = "close-v0.1"
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

    def params(self) -> dict:
        return asdict(self)

    def params_json(self) -> str:
        return lineage.canonical_json(self.params())


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


def ensure_def(conn: sqlite3.Connection, d: CloseDef, now_ms: int) -> int:
    text = d.params_json()
    sha = hashlib.sha256(text.encode()).hexdigest()
    row = conn.execute("SELECT close_def_id FROM close_def WHERE params_sha256 = ?", (sha,)).fetchone()
    if row:
        return row[0]
    return conn.execute("INSERT INTO close_def (name, family, params_json, params_sha256, created_ts_ms)"
                        " VALUES (?, 'close_live', ?, ?, ?)", (d.name, text, sha, now_ms)).lastrowid


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
    instruments = {r[0]: r[1] for r in conn.execute(
        "SELECT venue_instrument_id, native_id FROM f_venue_instrument WHERE venue = ?", (venue,))}
    found = [m for m in heads.values() if m.venue_instrument_id in instruments and m.outcome_id == outcome_id
             and m.polarity == "direct" and m.status != "rejected"]
    if not found:
        return "unmapped"
    if len(found) > 1:
        return "ambiguous_reference"
    (m,) = found
    other = heads.get((m.venue_instrument_id, 1 - m.side))
    if other is not None and other.status != "rejected" and (
            (other.polarity == "direct") == (other.outcome_id == outcome_id)):
        return "mapping_inconsistent"   # the other side claims this same outcome
    return Reference(venue, instruments[m.venue_instrument_id], m.venue_instrument_id, m.side, m)


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
    spread_e4: int | None = None
    bench: depth.Benchmark | None = None


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
    gap = conn.execute("""
        SELECT 1 FROM f_collection_gap o
        LEFT JOIN f_collection_gap c ON c.opens_gap_id = o.collection_gap_id
        WHERE o.kind = 'open' AND o.scope = ? AND o.boundary_ts_ms < ?
          AND (c.boundary_ts_ms IS NULL OR c.boundary_ts_ms > ?)""", (scope, as_of_ms, observed)).fetchone()
    limit_s = d.stream_liveness_max_s if source == "stream" else d.poll_liveness_max_s
    book = book_of(conn, tick_id, ref.venue, ref.native_id)
    bids, asks = canonical_ladder(book, ref.side, "direct")
    spread = (round((Fraction(asks[0].price) - Fraction(bids[0].price)) * PRICE_SCALE)
              if bids and asks else None)
    if gap:
        reason = "collection_gap"
    elif seen["liveness_age_ms"] > limit_s * 1000:
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
