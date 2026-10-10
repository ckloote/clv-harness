"""Scoring runs: closes, CLV and null EV for every entry (DESIGN.md §2.2, §2.3, §8.2).

A run reads one fact snapshot (clv.lineage) and writes, all against it:

    mapping_resolution  every instrument side's current mapping and its cohort eligibility
    off_resolution      each event's start interval (clv.off.resolver)
    close_price         per close definition, reference venue and outcome (clv.close)
    scoring_run         this run
    clv_score           per entry, reference venue and close definition:

        clv_ev       = p_close * d_entry - 1
        null_ev      = p_ref_entry * d_entry - 1      (the reference at the decision time)
        clv_residual = (p_close - p_ref_entry) * d_entry

`d_entry` comes from the entry's decision quote. Every value is an exact
rational. A score with a missing input keeps the row, with the reason; the
cohort exclusions (mapping status, settlement equivalence, book licensing,
entry timing) are listed beside the numbers, never applied by dropping rows.
`fee_adjusted_close_ev` needs a fee model, and V0 has none.

A run over an earlier snapshot's facts (`snapshot_id`) recomputes exactly what
they imply, whatever has been appended since.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from fractions import Fraction

from clv import lineage
from clv.close import definitions as close
from clv.identity import current_mappings, write_resolutions
from clv.off import resolver

SCORER_VERSION = "score-v0.1"
FEE_MODEL_VERSION = "none-v0"
REF_VENUES = ("novig", "kalshi")
# State-licensed books available to the operator (measurement contract §5.3).
LICENSED_BOOKS = frozenset({"draftkings", "fanduel", "betmgm", "betrivers"})


@dataclass(frozen=True)
class RunResult:
    scoring_run_id: int
    fact_snapshot_id: int
    closes: int
    scores: int


def run(conn: sqlite3.Connection, now_ms: int, analysis_spec: str = "v0", snapshot_id: int | None = None) -> RunResult:
    """Score every entry over a new fact snapshot, in one transaction. With `snapshot_id`, the new
    snapshot replicates that one's facts exactly, so the run recomputes an earlier run's inputs."""
    conn.execute("BEGIN")
    try:
        result = _run(conn, now_ms, analysis_spec, snapshot_id)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return result


def _run(conn, now_ms, analysis_spec, snapshot_id) -> RunResult:
    snap = lineage.take(conn, now_ms) if snapshot_id is None else lineage.replicate(conn, snapshot_id, now_ms)
    lineage.use(conn, snap)
    heads = current_mappings(conn)
    resolution_ids = write_resolutions(conn, snap, heads, now_ms)
    defs = [(d, close.ensure_def(conn, d, now_ms)) for d in close.live_defs()]

    closes: dict[tuple[int, str, int], tuple[int, close.Price]] = {}
    offs: dict[int, resolver.OffResolution] = {}
    for (event_id,) in conn.execute("SELECT event_id FROM f_event ORDER BY event_id").fetchall():
        off = resolver.resolve(conn, event_id)
        off_id = resolver.write(conn, snap, off, now_ms)
        offs[event_id] = off
        outcomes = [r[0] for r in conn.execute(
            "SELECT outcome_id FROM f_outcome WHERE event_id = ? ORDER BY outcome_id", (event_id,))]
        for d, def_id in defs:
            cutoff = None if off.earliest_ms is None else off.earliest_ms - d.buffer_s * 1000
            for venue in REF_VENUES:
                for outcome_id in outcomes:
                    ref = close.reference(conn, heads, venue, outcome_id)
                    if not off.trusted or cutoff is None:
                        price = close.Price(None, off.reason)
                    elif isinstance(ref, str):
                        price = close.Price(None, ref)
                    else:
                        price = close.price_as_of(conn, ref, cutoff, d)
                    mres = None if isinstance(ref, str) else resolution_ids[(ref.venue_instrument_id, ref.side)]
                    cid = _insert_close(conn, snap, def_id, off_id, venue, outcome_id, mres, cutoff, price, now_ms)
                    closes[(def_id, venue, outcome_id)] = (cid, price)

    run_id = conn.execute(
        "INSERT INTO scoring_run (fact_snapshot_id, scorer_version, fee_model_version, analysis_spec, created_ts_ms)"
        " VALUES (?, ?, ?, ?, ?)", (snap, SCORER_VERSION, FEE_MODEL_VERSION, analysis_spec, now_ms)).lastrowid
    scores = 0
    for e in conn.execute("""
            SELECT e.entry_id, e.event_id, e.outcome_id, e.venue_instrument_id, e.side, e.decision_ts_ms,
                   v.venue, v.operator, q.entry_quote_observation_id, q.d_entry_num, q.d_entry_den
            FROM f_entry e JOIN f_venue_instrument v USING (venue_instrument_id)
            JOIN f_entry_quote_observation q ON q.entry_id = e.entry_id AND q.concept = 'decision_quote'
            ORDER BY e.entry_id""").fetchall():
        (entry_id, event_id, outcome_id, iid, side, decision, venue, operator, quote_id, dn, dd) = e
        d_entry = Fraction(dn, dd)
        cohort = _entry_exclusions(heads, iid, side, outcome_id, venue, operator)
        off = offs[event_id]
        for d, def_id in defs:
            cutoff = None if off.earliest_ms is None else off.earliest_ms - d.buffer_s * 1000
            # Pregame entries only (measurement contract §1, §5.5): decided, and so quoted, before the cutoff.
            timing = [] if cutoff is not None and decision < cutoff else ["entry_after_cutoff"]
            for venue_ref in REF_VENUES:
                cid, cp = closes[(def_id, venue_ref, outcome_id)]
                ref = close.reference(conn, heads, venue_ref, outcome_id)
                if isinstance(ref, str):
                    at_entry = close.Price(None, ref)
                    ref_reasons = [ref]
                else:
                    at_entry = close.price_as_of(conn, ref, decision, d)
                    ref_reasons = [] if ref.mapping.eligible else [ref.mapping.ineligible_reason]
                reasons = _unique(cohort + ref_reasons + timing + ([cp.reason] if cp.reason else []))
                _insert_score(conn, run_id, entry_id, venue_ref, def_id, cid, quote_id, d_entry, cp.p, at_entry,
                              reasons, now_ms)
                scores += 1
    return RunResult(run_id, snap, len(closes), scores)


def _entry_exclusions(heads, iid: int, side: int, outcome_id: int, venue: str, operator: str | None) -> list[str]:
    """Cohort exclusions that belong to the entry itself (measurement contract §5.3, §5.4)."""
    out = []
    m = heads.get((iid, side))
    if m is None or (m.polarity == "direct") != (m.outcome_id == outcome_id):
        out.append("entry_mapping_superseded")      # a correction says this side is not the entry's outcome
    elif not m.eligible:
        out.append(m.ineligible_reason)             # including mapping_rejected
    if venue == "odds_api" and operator not in LICENSED_BOOKS:
        out.append("unlicensed_book")
    return out


def _unique(xs: list[str]) -> list[str]:
    return list(dict.fromkeys(xs))


def _nd(x: Fraction | None) -> tuple[int | None, int | None]:
    return (None, None) if x is None else (x.numerator, x.denominator)


def _insert_close(conn, snap, def_id, off_id, venue, outcome_id, mres, cutoff, p: close.Price, now_ms) -> int:
    num, den = _nd(p.p)
    return conn.execute(
        "INSERT INTO close_price (fact_snapshot_id, close_def_id, off_resolution_id, ref_venue, outcome_id,"
        " mapping_resolution_id, cutoff_ts_ms, book_tick_id, book_observed_ts_ms, quote_age_ms, liveness_age_ms,"
        " p_close_num, p_close_den, spread_e4, unscoreable_reason, computed_ts_ms)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (snap, def_id, off_id, venue, outcome_id, mres, cutoff, p.tick_id, p.observed_ms, p.quote_age_ms,
         p.liveness_age_ms, num, den, p.spread_e4, p.reason, now_ms)).lastrowid


def _insert_score(conn, run_id, entry_id, venue, def_id, close_id, quote_id, d: Fraction, p_close: Fraction | None,
                  at_entry: close.Price, reasons: list[str], now_ms) -> int:
    p_ref = at_entry.p
    clv_ev = None if p_close is None else p_close * d - 1
    null_ev = None if p_ref is None else p_ref * d - 1
    residual = None if p_close is None or p_ref is None else (p_close - p_ref) * d
    return conn.execute(
        "INSERT INTO clv_score (scoring_run_id, entry_id, ref_venue, close_def_id, close_price_id,"
        " entry_quote_observation_id, d_entry_num, d_entry_den, p_close_num, p_close_den, p_ref_entry_num,"
        " p_ref_entry_den, ref_entry_tick_id, ref_entry_reason, clv_ev_num, clv_ev_den, null_ev_num, null_ev_den,"
        " clv_residual_num, clv_residual_den, fee_adjusted_close_ev_num, fee_adjusted_close_ev_den,"
        " exclusion_reasons, computed_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
        " NULL, NULL, ?, ?)",
        (run_id, entry_id, venue, def_id, close_id, quote_id, d.numerator, d.denominator, *_nd(p_close), *_nd(p_ref),
         at_entry.tick_id, at_entry.reason, *_nd(clv_ev), *_nd(null_ev),
         *_nd(residual), json.dumps(reasons), now_ms)).lastrowid
