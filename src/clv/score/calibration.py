"""Calibration runs for the historical close families (DESIGN.md §9.2 check 1, §9.3 oracle sweep).

A family is one reference venue and one historical close method (clv.close.definitions):
Kalshi candle, Kalshi VWAP and Novig VWAP. `run` calibrates each over one new fact
snapshot, in one transaction, and holds no statistics of its own (clv.score.controls):

1. `off_resolution` and `mapping_resolution` for the snapshot.
2. `close_price` for both outcomes of every event, at every buffer in close.buffer_sweep_s,
   priced or with its reason. Every event has its rows: the coverage denominator.
3. `calibration_observation`: each outcome's price at each offset (controls.martingale_offsets
   and controls.oracle_offsets) before the earliest plausible start, by the same close
   function as the close at close.buffer_s, or why it is missing.
4. `calibration_run` and `calibration_result`, by mode:
   - `check`: check 1 and its binned diagnostic on the home outcome, the verdict, and the oracle sweep.
   - `dispersion`: the spread of Δp at each offset and the §9.4 item-12 operating characteristics
     under it (clv.score.simulation). Nothing is fitted, so nothing about drift is seen.

**Check-1 population**, per offset: events with a trusted off whose home outcome's reference
mapping is primary-cohort eligible (mapping.primary_cohort: `exact` or `manual_verified`, not in
conflict), priced at the offset and at the close. Only a pending settlement-equivalence verdict
is admitted: equivalence is between an entry venue and the reference, and check 1 has no entry.
Every other exclusion (`mapping_unverified`, `mapping_conflict`, `mapping_rejected`,
`rules_mismatch`) keeps the mapping out, under its own reason.

**Verdict:**
- `insufficient_data`: an offset lacks the data to fit (controls.InsufficientCalibrationData),
  or has fewer than controls.sample_events events. The statistics are still recorded,
  as diagnostics.
- `passed` / `failed`: whether every component at every offset lies inside its margin
  (controls.check1).

A flagged bin does not change the verdict; each needs a decision record (Gate B2).

**Oracle (§9.3):** the winner is the outcome StatsAPI settles `win`, alone, with the family's
venue settling that outcome's reference side `win` too; otherwise the event is excluded with
its reason. At each oracle offset and close buffer, `oracle_clv = p_close / p_offset - 1` on
the winner's own reference. The mean is reported over every such event and over the events
priced at every buffer (matched), since a buffer that loses events can itself make a kink.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import Counter
from dataclasses import asdict, dataclass, field
from fractions import Fraction
from pathlib import Path

import numpy as np

from clv import lineage
from clv.close import definitions as close
from clv.close.vwap import Executions
from clv.config import param
from clv.identity import current_mappings, write_resolutions
from clv.off import resolver
from clv.score import controls, simulation

CALIBRATION_VERSION = "calibration-v0.1"
FAMILIES = (("kalshi", "candle_mid_top"), ("kalshi", "trade_vwap"), ("novig", "trade_vwap"))
# The only mapping exclusion calibration admits: no entry venue, so no equivalence to settle.
ADMITTED_EXCLUSIONS = ("equivalence_pending",)
MAPPING_EXCLUSIONS = ("mapping_unverified", "mapping_conflict", "mapping_rejected", "rules_mismatch")
OC_SEED = 20261015


def label(offset_s: int) -> str:
    return f"{offset_s // 3600}h" if offset_s % 3600 == 0 else f"{offset_s // 60}m"


@dataclass(frozen=True)
class Spec:
    """Every value a run uses, in probability units: recorded as the run's `spec_json`."""
    offsets: tuple[int, ...]
    oracle_offsets: tuple[int, ...]
    buffers: tuple[int, ...]
    buffer_s: int
    cohort: tuple[str, ...]
    admitted_exclusions: tuple[str, ...]
    sample_events: int
    min_clusters: int
    margin_intercept: float
    margin_slope: float
    margin_curvature: float
    curvature_radius: float
    price_bins: int
    bin_trigger: float
    controls_sha256: str
    simulation_sha256: str
    version: str = CALIBRATION_VERSION

    @classmethod
    def from_params(cls) -> Spec:
        sha = lambda m: hashlib.sha256(Path(m.__file__).read_bytes()).hexdigest()
        return cls(tuple(param("controls.martingale_offsets")), tuple(param("controls.oracle_offsets")),
                   tuple(param("close.buffer_sweep_s")), param("close.buffer_s"), tuple(param("mapping.primary_cohort")),
                   ADMITTED_EXCLUSIONS, param("controls.sample_events"), controls.MIN_CLUSTERS,
                   param("controls.martingale_margin_intercept_pp") / 100, param("controls.martingale_margin_slope"),
                   param("controls.martingale_margin_curvature_pp") / 100,
                   param("controls.martingale_curvature_radius_pp") / 100, param("controls.martingale_price_bins"),
                   param("controls.martingale_bin_trigger_pp") / 100, sha(controls), sha(simulation))

    def json(self) -> str:
        return lineage.canonical_json(asdict(self))


@dataclass
class FamilyRun:
    calibration_run_id: int
    venue: str
    method: str
    mode: str
    verdict: str | None
    verdict_reason: str | None
    n_by_offset: dict[int, int] = field(default_factory=dict)


@dataclass(frozen=True)
class Event:
    event_id: int
    home: int | None
    outcomes: tuple[int, ...]


def events(conn: sqlite3.Connection) -> list[Event]:
    """Every event with its outcomes and the home team's, from its StatsAPI alias."""
    home = dict(conn.execute("""
        SELECT o.event_id, min(o.outcome_id) FROM f_outcome o JOIN f_event_alias a ON a.event_id = o.event_id
        WHERE a.provider = 'mlb_statsapi' AND o.team_abbreviation = json_extract(a.detail_json, '$.home')
        GROUP BY o.event_id""").fetchall())
    outs: dict[int, list[int]] = {}
    for e, o in conn.execute("SELECT event_id, outcome_id FROM f_outcome ORDER BY event_id, outcome_id"):
        outs.setdefault(e, []).append(o)
    return [Event(e, home.get(e), tuple(outs.get(e, ()))) for (e,) in
            conn.execute("SELECT event_id FROM f_event ORDER BY event_id").fetchall()]


def settlement_heads(conn: sqlite3.Connection) -> list[tuple]:
    """Every source's current answer: the settlements nothing supersedes."""
    return conn.execute("""
        SELECT source, event_id, outcome_id, venue_instrument_id, side, result FROM f_settlement_observation s
        WHERE NOT EXISTS (SELECT 1 FROM f_settlement_observation t WHERE t.supersedes_id = s.settlement_obs_id)
        """).fetchall()


def winners(heads: list[tuple]) -> dict[int, int]:
    """event -> its outcome StatsAPI settles `win`, where exactly one is."""
    wins: dict[int, list[int]] = {}
    for source, event, outcome, _, _, result in heads:
        if source == "mlb_statsapi" and result == "win":
            wins.setdefault(event, []).append(outcome)
    return {e: os[0] for e, os in wins.items() if len(os) == 1}


def run(conn: sqlite3.Connection, now_ms: int, mode: str = "check", snapshot_id: int | None = None,
        families=FAMILIES, replicates: int = 1_000) -> tuple[int, list[FamilyRun]]:
    """Calibrate each family over a new fact snapshot (or a replica of `snapshot_id`), in one transaction."""
    if mode not in ("check", "dispersion"):
        raise ValueError(f"unknown calibration mode {mode!r}")
    conn.execute("BEGIN")
    try:
        out = _run(conn, now_ms, mode, snapshot_id, families, replicates)
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    return out


def _run(conn, now_ms, mode, snapshot_id, families, replicates) -> tuple[int, list[FamilyRun]]:
    snap = lineage.take(conn, now_ms) if snapshot_id is None else lineage.replicate(conn, snapshot_id, now_ms)
    lineage.use(conn, snap)
    spec = Spec.from_params()
    heads = current_mappings(conn)
    resolution_ids = write_resolutions(conn, snap, heads, now_ms)
    refs = close.References(conn, heads)
    evs = events(conn)
    offs = {e.event_id: resolver.resolve(conn, e.event_id) for e in evs}
    off_ids = {e: resolver.write(conn, snap, off, now_ms) for e, off in offs.items()}
    settled = settlement_heads(conn)
    winner = winners(settled)
    venue_result = {(iid, side): result for source, _, _, iid, side, result in settled if iid is not None}
    executions = Executions(conn, "f_")
    out = []
    for venue, method in families:
        defs = {d.buffer_s: (d, close.ensure_def(conn, d, now_ms))
                for d in close.hist_defs(list(spec.buffers)) if d.method == method}
        primary, primary_id = defs[spec.buffer_s]
        closes: dict[tuple[int, int], tuple[int, close.Price]] = {}
        observed: dict[tuple[int, int, int], tuple[int | None, close.Price]] = {}
        for e in evs:
            off = offs[e.event_id]
            for o in e.outcomes:
                ref = refs.get(venue, o)
                mres = None if isinstance(ref, str) else resolution_ids[(ref.venue_instrument_id, ref.side)]
                for b, (d, def_id) in defs.items():
                    cutoff = None if off.earliest_ms is None else off.earliest_ms - b * 1000
                    p = _price(conn, off, ref, cutoff, d, executions, spec, cohort_required=False)
                    closes[(o, b)] = (close.write_close(conn, snap, def_id, off_ids[e.event_id], venue, o, mres, cutoff,
                                                        p, now_ms), p)
                for k in sorted(set(spec.offsets) | set(spec.oracle_offsets), reverse=True):
                    as_of = None if off.earliest_ms is None else off.earliest_ms - k * 1000
                    observed[(e.event_id, o, k)] = (as_of, _price(conn, off, ref, as_of, primary, executions, spec,
                                                                  cohort_required=True))
        pairs = {k: _pairs(evs, observed, closes, k, spec.buffer_s) for k in spec.offsets}
        if mode == "dispersion":
            verdict, reason, results = None, None, _dispersion(pairs, spec, replicates)
        else:
            verdict, reason, results = _check(pairs, spec)
            results += _oracle(evs, observed, closes, spec, winner, venue_result, refs, venue, offs)
        run_id = conn.execute(
            "INSERT INTO calibration_run (fact_snapshot_id, close_def_id, ref_venue, mode, spec_json, calibration_version,"
            " verdict, verdict_reason, computed_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (snap, primary_id, venue, mode, spec.json(), CALIBRATION_VERSION, verdict, reason, now_ms)).lastrowid
        for (event_id, o, k), (as_of, p) in observed.items():
            _insert_observation(conn, run_id, event_id, o, k, as_of, closes[(o, spec.buffer_s)][0], p, now_ms)
        for r in results:
            _insert_result(conn, run_id, r, now_ms)
        out.append(FamilyRun(run_id, venue, method, mode, verdict, reason,
                             {k: len(pairs[k][2]) for k in spec.offsets}))
    return snap, out


def _price(conn, off, ref, as_of, d, executions, spec: Spec, cohort_required: bool) -> close.Price:
    """The family's price as of a moment, or the first reason there is none."""
    if not off.trusted or as_of is None:
        return close.Price(None, off.reason)
    if isinstance(ref, str):
        return close.Price(None, ref)
    if cohort_required and not (ref.mapping.eligible or ref.mapping.ineligible_reason in spec.admitted_exclusions):
        return close.Price(None, ref.mapping.ineligible_reason)
    return close.hist_price_as_of(conn, ref, as_of, d, executions)


def _pairs(evs, observed, closes, offset_s, buffer_s) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Check 1's fixed side at one offset: (p_offset, p_close, event) over the home outcomes priced at both."""
    rows = []
    for e in evs:
        if e.home is None:
            continue
        _, at = observed[(e.event_id, e.home, offset_s)]
        _, c = closes[(e.home, buffer_s)]
        if at.p is not None and c.p is not None:
            rows.append((float(at.p), float(c.p), e.event_id))
    a = np.array(rows, dtype=float).reshape(-1, 3)
    return a[:, 0], a[:, 1], a[:, 2].astype(int)


# -- check 1 -------------------------------------------------------------------------------------------

def _check(pairs, spec: Spec) -> tuple[str, str | None, list[dict]]:
    labels = {k: label(k) for k in spec.offsets}
    by_offset = {labels[k]: pairs[k] for k in spec.offsets if len(pairs[k][2])}
    margins = (spec.margin_intercept, spec.margin_slope, spec.margin_curvature)
    try:
        passed, _ = controls.check1(by_offset, *margins, spec.curvature_radius,
                                    expected_offsets=[labels[k] for k in spec.offsets])
        verdict, reason = ("passed", None) if passed else ("failed", "a component's interval leaves its margin")
    except controls.InsufficientCalibrationData as exc:
        verdict, reason = "insufficient_data", str(exc)
    below = [f"{labels[k]} ({len(pairs[k][2])})" for k in spec.offsets if len(pairs[k][2]) < spec.sample_events]
    if below and verdict != "insufficient_data":
        reason = (f"fewer than controls.sample_events = {spec.sample_events} events at "
                  f"{', '.join(below)}; check 1 alone would say {verdict}")
        verdict = "insufficient_data"
    results, flags = [], 0
    for k in spec.offsets:
        p_e, p_c, ev = pairs[k]
        n = len(np.unique(ev))
        base = dict(kind="check1", offset_s=k, n_events=n, n_rows=len(ev))
        try:
            m = controls.martingale_test(p_e, p_c, ev, labels[k], *margins, spec.curvature_radius)
            detail = {"x_center": m.x_center, "q_center": m.q_center, "below_sample_target": n < spec.sample_events}
            for comp, est, margin in (("intercept", m.intercept, spec.margin_intercept),
                                      ("slope", m.slope, spec.margin_slope),
                                      ("curvature", m.curvature, spec.margin_curvature)):
                results.append(dict(base, component=comp, estimate=est.value, se=est.se, lo=est.lo, hi=est.hi,
                                    margin=margin, outcome="passed" if est.within(margin) else "failed",
                                    detail=detail))
        except controls.InsufficientCalibrationData as exc:
            results += [dict(base, component=comp, outcome="insufficient_data", detail={"reason": str(exc)})
                        for comp in ("intercept", "slope", "curvature")]
        try:
            bins = controls.binned_lack_of_fit(p_e, p_c, ev, spec.curvature_radius, spec.price_bins, spec.bin_trigger)
            for i, b in enumerate(bins):
                flags += b.flagged
                results.append(dict(kind="bin", offset_s=k, component=f"bin:{i}", n_events=b.n_rows, n_rows=b.n_rows,
                                    estimate=b.residual.value, se=b.residual.se, lo=b.residual.lo, hi=b.residual.hi,
                                    margin=spec.bin_trigger, outcome="flagged" if b.flagged else "not_flagged",
                                    detail={"price_lo": b.price_lo, "price_hi": b.price_hi}))
        except controls.InsufficientCalibrationData as exc:
            results.append(dict(kind="bin", offset_s=k, component="bin:all", n_events=n, n_rows=len(ev),
                                outcome="insufficient_data", detail={"reason": str(exc)}))
    if flags:
        note = f"{flags} bin(s) flagged: each needs a decision record (Gate B2)"
        reason = f"{reason}; {note}" if reason else note
    return verdict, reason, results


# -- the oracle sweep ----------------------------------------------------------------------------------

def _oracle(evs, observed, closes, spec: Spec, winner, venue_result, refs, venue, offs) -> list[dict]:
    excluded, entries = Counter(), []
    for e in evs:
        if not offs[e.event_id].trusted:
            excluded[offs[e.event_id].reason] += 1
            continue
        w = winner.get(e.event_id)
        if w is None:
            excluded["no_official_winner"] += 1
            continue
        ref = refs.get(venue, w)
        if isinstance(ref, str):
            excluded[ref] += 1
            continue
        settled = venue_result.get((ref.venue_instrument_id, ref.side))
        if settled != "win":
            excluded[{None: "venue_unsettled", "fair_value": "settled_fair_value"}.get(settled, "settlement_disagreement")] += 1
            continue
        entries.append((e.event_id, w))
    results = []
    for k in spec.oracle_offsets:
        values: dict[int, dict[int, float]] = {}            # buffer -> event -> oracle CLV
        for event_id, w in entries:
            _, at = observed[(event_id, w, k)]
            if at.p is None or at.p == 0:
                continue
            for b in spec.buffers:
                _, c = closes[(w, b)]
                if c.p is not None:
                    values.setdefault(b, {})[event_id] = float(c.p / at.p - 1)
        matched = set.intersection(*(set(values.get(b, {})) for b in spec.buffers)) if spec.buffers else set()
        for b in spec.buffers:
            for name, events_ in (("all", sorted(values.get(b, {}))), ("matched", sorted(matched))):
                v = np.array([values[b][x] for x in events_], dtype=float)
                base = dict(kind="oracle", offset_s=k, buffer_s=b, component=f"{name}:b{b}", n_events=len(events_),
                            n_rows=len(events_), detail={"excluded": dict(excluded)} if name == "all" else {})
                try:
                    est = controls.oracle_mean(v, np.array(events_))
                    results.append(dict(base, estimate=est.value, se=est.se, lo=est.lo, hi=est.hi))
                except controls.InsufficientCalibrationData as exc:
                    results.append(dict(base, outcome="insufficient_data", detail={**base["detail"], "reason": str(exc)}))
    return results


# -- dispersion ----------------------------------------------------------------------------------------

def _dispersion(pairs, spec: Spec, replicates: int) -> list[dict]:
    """The spread of Δp at each offset, the entry-price distribution, and item 12 under them."""
    results, sds, counts = [], [], []
    for k in spec.offsets:
        p_e, p_c, ev = pairs[k]
        n = len(ev)
        if n < 2:
            results.append(dict(kind="dispersion", offset_s=k, component="sd_dp", n_events=n, n_rows=n,
                                outcome="insufficient_data", detail={"reason": f"{n} events"}))
            continue
        sd = float(np.std(p_c - p_e, ddof=1))
        sds.append(sd)
        counts.append(n)
        quantiles = dict(zip(("p05", "p25", "p50", "p75", "p95"),
                             map(float, np.quantile(p_e, [0.05, 0.25, 0.5, 0.75, 0.95]))))
        results.append(dict(kind="dispersion", offset_s=k, component="sd_dp", n_events=n, n_rows=n, estimate=sd,
                            detail={"p_offset_quantiles": quantiles}))
    names = ("correct_pipeline_passes", "level_drift_fails", "curvature_drift_fails")
    if len(sds) != len(spec.offsets) or min(counts) < spec.min_clusters:
        return results + [dict(kind="dispersion", offset_s=0, component=f"oc:{name}", n_events=0, n_rows=0,
                               outcome="insufficient_data", detail={"reason": "an offset has too few events"})
                          for name in names]
    increments = simulation.increments_from_spread(sds)
    start = pairs[spec.offsets[0]][0]                   # the earliest offset's entry prices
    detail = {"increment_sd": increments, "counts": counts, "replicates": replicates, "seed": OC_SEED}
    try:
        oc = simulation.operating_characteristics(
            np.random.default_rng(OC_SEED), offsets=[label(k) for k in spec.offsets], counts=counts,
            increment_sd=increments, start_prices=start, margin_intercept=spec.margin_intercept,
            margin_slope=spec.margin_slope, margin_curvature=spec.margin_curvature,
            curvature_radius=spec.curvature_radius, replicates=replicates)
    except controls.InsufficientCalibrationData as exc:
        # The measured series cannot be fitted (e.g. too little price variation): keep the
        # measurements; the rates are unavailable, as for too few events (PR #22 review).
        return results + [dict(kind="dispersion", offset_s=0, component=f"oc:{name}", n_events=max(counts),
                               n_rows=max(counts), outcome="insufficient_data", detail={**detail, "reason": str(exc)})
                          for name in names]
    return results + [dict(kind="dispersion", offset_s=0, component=f"oc:{name}", n_events=max(counts),
                           n_rows=max(counts), estimate=oc[name], detail=detail) for name in names]


# -- writing -------------------------------------------------------------------------------------------

def _nd(x: Fraction | None) -> tuple[int | None, int | None]:
    return (None, None) if x is None else (x.numerator, x.denominator)


def _insert_observation(conn, run_id, event_id, outcome_id, offset_s, as_of, close_id, p: close.Price, now_ms) -> None:
    conn.execute(
        "INSERT INTO calibration_observation (calibration_run_id, event_id, outcome_id, offset_s, as_of_ts_ms,"
        " close_price_id, p_num, p_den, candle_id, cutoff_candle_active, vwap_lo_ms, vwap_hi_ms, vwap_executions,"
        " vwap_payout_cents, quote_age_ms, spread_e4, reason, computed_ts_ms)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, event_id, outcome_id, offset_s, as_of, close_id, *_nd(p.p), p.candle_id,
         None if p.cutoff_candle_active is None else int(p.cutoff_candle_active), p.vwap_lo_ms, p.vwap_hi_ms,
         p.vwap_executions, p.vwap_payout_cents, p.quote_age_ms, p.spread_e4, p.reason, now_ms))


def _insert_result(conn, run_id, r: dict, now_ms) -> None:
    conn.execute(
        "INSERT INTO calibration_result (calibration_run_id, kind, offset_s, component, buffer_s, n_events, n_rows,"
        " estimate, se, lo, hi, margin, outcome, detail_json, computed_ts_ms)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (run_id, r["kind"], r["offset_s"], r["component"], r.get("buffer_s"), r["n_events"], r["n_rows"],
         r.get("estimate"), r.get("se"), r.get("lo"), r.get("hi"), r.get("margin"), r.get("outcome"),
         json.dumps(r.get("detail", {}), sort_keys=True), now_ms))


# -- the report ----------------------------------------------------------------------------------------

AUDITS = {
    "selected candles ending after the earliest plausible start (must be 0)": """
        SELECT count(*) FROM close_price c JOIN historical_candle h ON h.candle_id = c.candle_id
        JOIN off_resolution o ON o.off_resolution_id = c.off_resolution_id
        WHERE c.fact_snapshot_id = ? AND h.interval_end_ms > o.earliest_plausible_start_ts_ms""",
    "offset prices from a candle ending after their as-of time (must be 0)": """
        SELECT count(*) FROM calibration_observation b JOIN calibration_run r USING (calibration_run_id)
        JOIN historical_candle h ON h.candle_id = b.candle_id
        WHERE r.fact_snapshot_id = ? AND h.interval_end_ms > b.as_of_ts_ms""",
    "executions with no record but a TAKER row (must be 0)": """
        SELECT count(*) FROM f_trade_execution x WHERE ? IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM f_trade_source_observation s WHERE s.execution_id = x.execution_id AND s.role <> 'taker')""",
    "executions with two MAKER rows from one file (must be 0)": """
        SELECT count(*) FROM (SELECT 1 FROM f_trade_source_observation WHERE ? IS NOT NULL AND role = 'maker'
                              GROUP BY execution_id, raw_artifact_id, raw_line HAVING count(*) > 1)""",
    "Novig stream trades no MAKER row matched, counted as executions": """
        SELECT count(*) FROM f_trade_source_observation WHERE ? IS NOT NULL AND role = 'stream'
          AND reconcile = 'unmatched'""",
}
DISAGREEMENT_BUCKETS_S = (120, 300, 600, 1800)


def _pp(x: float | None) -> str:
    return "—" if x is None else f"{100 * x:+.3f}"


def report(conn: sqlite3.Connection, snapshot_id: int, out_dir: Path) -> tuple[Path, Path]:
    """`calibration.md` (aggregates) and `calibration_games.csv` (per game, vendor-derived prices) for
    every calibration run over one fact snapshot, rebuilt from the database alone."""
    import csv
    lineage.use(conn, snapshot_id)
    runs = conn.execute("""SELECT r.calibration_run_id, r.ref_venue, d.name, r.mode, r.verdict, r.verdict_reason,
                                  r.spec_json, r.close_def_id
                           FROM calibration_run r JOIN close_def d USING (close_def_id)
                           WHERE r.fact_snapshot_id = ? ORDER BY r.calibration_run_id""", (snapshot_id,)).fetchall()
    home = {e.home for e in events(conn) if e.home is not None}
    game = dict(conn.execute("SELECT o.outcome_id, e.league_game_id FROM f_outcome o JOIN f_event e USING (event_id)"))
    team = dict(conn.execute("SELECT outcome_id, team_abbreviation FROM f_outcome"))
    out_dir.mkdir(parents=True, exist_ok=True)
    md_path, csv_path = out_dir / "calibration.md", out_dir / "calibration_games.csv"
    lines = [f"# B2 calibration, fact snapshot {snapshot_id}", "",
             "Generated by `clv calibrate`. Aggregates only; the per-game prices are in calibration_games.csv,",
             "which is vendor-derived and stays out of the repository.", ""]
    games = []
    for run_id, venue, name, mode, verdict, reason, spec_json, def_id in runs:
        spec = json.loads(spec_json)
        lines += [f"## {name} on {venue} ({mode} run {run_id})", "",
                  f"**Verdict:** {verdict or '— (a dispersion run fits nothing)'}" + (f". {reason}" if reason else ""), ""]
        closes = {o: (p, d, r, a, f) for o, p, d, r, a, f in conn.execute(
            "SELECT outcome_id, p_close_num, p_close_den, unscoreable_reason, quote_age_ms, cutoff_candle_active"
            " FROM close_price WHERE fact_snapshot_id = ? AND close_def_id = ? AND ref_venue = ?",
            (snapshot_id, def_id, venue))}
        obs = conn.execute("SELECT outcome_id, offset_s, p_num, p_den, reason FROM calibration_observation"
                           " WHERE calibration_run_id = ?", (run_id,)).fetchall()
        # A close is priced whatever its mapping's status; the check-1 population needs the cohort.
        excluded = {r[0]: r[4] for r in obs if r[4] in MAPPING_EXCLUSIONS}
        waterfall = Counter((closes[o][2] or (f"priced, {excluded[o]}" if o in excluded else "priced"))
                            for o in home if o in closes)
        lines += [f"Home-outcome closes at close.buffer_s = {spec['buffer_s']} s, of {len(home)} games:", "",
                  "| Result | Games |", "|---|---:|"] + [f"| {k} | {v} |" for k, v in waterfall.most_common()]
        priced = [o for o in home if o in closes and closes[o][2] is None and o not in excluded]
        if any(closes[o][4] is not None for o in priced):
            flagged = sum(1 for o in priced if closes[o][4])
            lines += ["", f"A candle contained the cutoff for {flagged} of {len(priced)} priced closes in the cohort "
                          "(`cutoff_candle_active`): each is the earlier candle's, as of its end."]
        lines += ["", "Home-outcome prices at each offset:", "", "| Offset | Priced | Missing, by reason |", "|---|---:|---|"]
        for k in spec["offsets"]:
            at = [r for r in obs if r[0] in home and r[1] == k]
            missing = Counter(r[4] for r in at if r[2] is None)
            lines.append(f"| {label(k)} | {sum(r[2] is not None for r in at)} | "
                         + ", ".join(f"{m} {n}" for m, n in missing.most_common()) + " |")
        res = conn.execute("SELECT kind, offset_s, component, buffer_s, n_events, estimate, lo, hi, margin, outcome,"
                           " detail_json FROM calibration_result WHERE calibration_run_id = ?"
                           " ORDER BY kind, offset_s DESC, buffer_s, component", (run_id,)).fetchall()
        lines += _results_md(res, spec) + [""]
        by_obs = {(r[0], r[1]): r for r in obs}
        for o in sorted(closes):
            p, d, r, a, f = closes[o]
            row = {"family": name, "venue": venue, "game_pk": game[o], "outcome": team[o], "home": int(o in home),
                   "p_close": None if p is None else f"{p / d:.6f}", "close_reason": r, "close_age_s":
                   None if a is None else a // 1000, "cutoff_candle_active": f}
            for k in sorted(set(spec["offsets"]) | set(spec["oracle_offsets"]), reverse=True):
                b = by_obs.get((o, k))
                row[f"p_{label(k)}"] = None if b is None or b[2] is None else f"{b[2] / b[3]:.6f}"
                row[f"reason_{label(k)}"] = None if b is None else b[4]
            games.append(row)
    lines += ["## Audits", "", "| Check | Count |", "|---|---:|"]
    lines += [f"| {k} | {conn.execute(q, (snapshot_id,)).fetchone()[0]} |" for k, q in AUDITS.items()]
    lines += _disagreement_md(conn, snapshot_id)
    md_path.write_text("\n".join(lines) + "\n")
    with csv_path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(games[0]) if games else ["family"])
        w.writeheader()
        w.writerows(games)
    return md_path, csv_path


def _results_md(res, spec) -> list[str]:
    out = []
    check1 = [r for r in res if r[0] == "check1"]
    if check1:
        out += ["", "Check 1 (pp for the intercept and curvature; 90% intervals; margins "
                    f"±{100 * spec['margin_intercept']:.2f} pp, ±{spec['margin_slope']:.2f}, "
                    f"±{100 * spec['margin_curvature']:.2f} pp):", "",
                "| Offset | Events | Intercept | Slope | Curvature |", "|---|---:|---|---|---|"]
        for k in spec["offsets"]:
            c = {r[2]: r for r in check1 if r[1] == k}
            cell = lambda r, s: (r[9] if r[5] is None else
                                 f"{s(r[5])} [{s(r[6])}, {s(r[7])}] {r[9]}")
            pp, raw = _pp, lambda x: f"{x:+.4f}"
            out.append(f"| {label(k)} | {c['intercept'][4]} | {cell(c['intercept'], pp)} | {cell(c['slope'], raw)} |"
                       f" {cell(c['curvature'], pp)} |")
    bins = [r for r in res if r[0] == "bin"]
    if bins:
        flagged = [r for r in bins if r[9] == "flagged"]
        out += ["", f"Binned diagnostic: {len(flagged)} bin(s) flagged of {len(bins)}"
                    f" (trigger ±{100 * spec['bin_trigger']:.2f} pp)."]
        for r in flagged:
            d = json.loads(r[10])
            out.append(f"- {label(r[1])}, prices {d['price_lo']:.3f}–{d['price_hi']:.3f}: mean residual {_pp(r[5])} pp "
                       f"[{_pp(r[6])}, {_pp(r[7])}]: needs a decision record")
        out += [f"- {label(r[1])}: {r[9]} ({json.loads(r[10]).get('reason', '')})" for r in bins
                if r[9] == "insufficient_data"]
    oracle = [r for r in res if r[0] == "oracle"]
    if oracle:
        out += ["", "Oracle sweep (§9.3; mean oracle CLV in pp, 90% interval, events). A calibration construct, never "
                    "a strategy result:", "",
                "| Offset | Buffer | All | Matched |", "|---|---:|---|---|"]
        for k in spec["oracle_offsets"]:
            for b in spec["buffers"]:
                cells = []
                for name in ("all", "matched"):
                    r = next((r for r in oracle if r[1] == k and r[2] == f"{name}:b{b}"), None)
                    cells.append("—" if r is None else f"insufficient ({r[4]})" if r[5] is None else
                                 f"{_pp(r[5])} [{_pp(r[6])}, {_pp(r[7])}] ({r[4]})")
                out.append(f"| {label(k)} | {b} s | {cells[0]} | {cells[1]} |")
        excluded = next((json.loads(r[10]).get("excluded") for r in oracle if r[2].startswith("all:")), None)
        if excluded:
            out += ["", "Excluded from the oracle: " + ", ".join(f"{k} {v}" for k, v in sorted(excluded.items()))]
    disp = [r for r in res if r[0] == "dispersion"]
    if disp:
        out += ["", "Measured spread of Δp = p_close − p_offset (nothing fitted):", "",
                "| Offset | Events | sd (pp) | Entry-price quantiles 5/25/50/75/95 |", "|---|---:|---:|---|"]
        for r in disp:
            if r[1] == 0 or r[2] != "sd_dp":
                continue
            q = json.loads(r[10]).get("p_offset_quantiles", {})
            out.append(f"| {label(r[1])} | {r[4]} | {'—' if r[5] is None else f'{100 * r[5]:.3f}'} | "
                       + " / ".join(f"{v:.3f}" for v in q.values()) + " |")
        oc = {r[2]: r for r in disp if r[1] == 0}
        if oc:
            d = json.loads(next(iter(oc.values()))[10])
            out += ["", "§9.4 item 12 under the measured series"
                        + (f" ({d['replicates']} replicates, seed {d['seed']}, steps "
                           + ", ".join(f"{100 * s:.3f}" for s in d["increment_sd"]) + " pp):" if "replicates" in d
                           else ":"), ""]
            for name, need in (("correct_pipeline_passes", "≥ 95%"), ("level_drift_fails", "≥ 90%"),
                               ("curvature_drift_fails", "≥ 90%")):
                r = oc[f"oc:{name}"]
                why = json.loads(r[10]).get("reason")
                out.append(f"- {name.replace('_', ' ')}: "
                           + (f"insufficient data ({why})" if r[5] is None else f"{r[5]:.1%}") + f" (requirement {need})")
    return out


def _disagreement_md(conn, snapshot_id) -> list[str]:
    rows = [r[0] for r in conn.execute("SELECT source_disagreement_ms FROM off_resolution WHERE fact_snapshot_id = ?"
                                       " AND reason = 'off_disagreement'", (snapshot_id,))]
    out = ["", f"## Off disagreement: {len(rows)} games beyond off.max_source_disagreement_s", "",
           "The span of the accepted start claims (a diagnostic for A2; the rule is unchanged in B2):", "",
           "| Span | Games |", "|---|---:|"]
    edges = (*DISAGREEMENT_BUCKETS_S, None)
    for lo, hi in zip(edges, edges[1:]):
        n = sum(1 for ms in rows if ms > lo * 1000 and (hi is None or ms <= hi * 1000))
        out.append(f"| {lo}–{hi} s | {n} |" if hi else f"| over {lo} s | {n} |")
    return out
