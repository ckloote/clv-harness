"""Committed calibration simulation (DESIGN.md §9.2, §9.4 items 1, 2, 11–22).

This file is the authority for what the calibration checks can and cannot
detect. §9.2 may not be changed, and the §10 control margins may not be
changed, unless this file is updated to match and passes.

Run with:  pytest -s tests/test_calibration_simulation.py   (-s shows the rates)
"""
import numpy as np
import pytest

from clv.score.controls import (
    MIN_CLUSTERS,
    Estimate,
    InsufficientCalibrationData,
    agreement_gate,
    agreement_test,
    binned_lack_of_fit,
    check1,
    ols_cluster,
    random_side_residual,
    two_way_proportional_devig,
)

# Mirror of the §10 defaults. If §10 changes, change these and rerun.
MARGIN_INTERCEPT = 0.005   # controls.martingale_margin_intercept_pp = 0.5 pp
MARGIN_SLOPE = 0.05        # controls.martingale_margin_slope
MARGIN_CURVATURE = 0.016   # controls.martingale_margin_curvature_pp = 1.6 pp
CURVATURE_RADIUS = 0.20    # controls.martingale_curvature_radius_pp = 20 pp
PRICE_BINS = 5             # controls.martingale_price_bins
BIN_TRIGGER = 0.005        # controls.martingale_bin_trigger_pp = 0.5 pp
MARGIN_AGREE_SLOPE = 0.15  # controls.agreement_margin_slope
OUTLIER = 0.08             # controls.agreement_outlier_pp = 8 pp
MAX_VERIFIED_RATE = 0.01   # controls.agreement_max_verified_rate = 1%
N_EVENTS = 1_500           # controls.sample_events
OC_REPLICATES = 1_000      # replicates behind every operating-characteristic assertion
MIN_CLUSTERS_SPEC = 30     # controls.min_clusters (fixed in code as MIN_CLUSTERS)

# Planning assumptions about the reference series (replace with B1/A1 measurements).
OFFSETS = ("24h", "6h", "1h", "15m")
INCREMENT_SD = (0.03, 0.02, 0.01, 0.01)   # 24h->6h, 6h->1h, 1h->15m, 15m->close
XVENUE_SD = 0.01                          # genuine entry-venue vs reference disagreement
XVENUE_SLOPE = 0.03                       # plausible price-level cross-venue disagreement
ENTRY_OVERROUND = 0.045                   # only to exercise the fixed proportional de-vig path


def simulate(
    rng,
    n=N_EVENTS,
    p_range=(0.30, 0.70),
    level_drift=0.0,
    price_drift=0.0,
    curvature_drift=0.0,
    band_drift=None,          # (price_lo, price_hi, size): drift only inside a price band
    close_flip=False,
    ref_flip=False,
    entry_flip=False,
):
    """One sample of events as the pipeline would observe them.

    True reference prices follow a martingale between offsets. Drift, if any,
    is injected on the final step into the close, so every offset sees it.
    `curvature_drift` is the centered symmetric quadratic departure at
    p = 0.5 +/- CURVATURE_RADIUS relative to p = 0.5. Faults are applied to what
    the pipeline OBSERVES, not to the truth. Entry IDs are strings, so nothing
    downstream can confuse them with array positions.
    """
    events = np.arange(n)
    entry_ids = np.array([f"E{i:06d}" for i in range(n)])
    cur = rng.uniform(*p_range, n)
    path = [cur]
    for sd in INCREMENT_SD[:-1]:
        cur = np.clip(cur + rng.normal(0, sd, n), 0.02, 0.98)
        path.append(cur)
    last = path[-1]

    q_curve = ((last - 0.5) / CURVATURE_RADIUS) ** 2
    q_curve -= np.mean(q_curve)
    drift = level_drift + price_drift * (last - 0.5) + curvature_drift * q_curve
    if band_drift is not None:
        lo, hi, size = band_drift
        drift = drift + np.where((last >= lo) & (last < hi), size, 0.0)
    close = np.clip(last + rng.normal(0, INCREMENT_SD[-1], n) + drift, 0.02, 0.98)

    obs = lambda x: 1 - x if ref_flip else x          # reference-instrument mapping
    close_obs = 1 - obs(close) if close_flip else obs(close)

    # Check 1 input: fixed canonical side (home) at every offset.
    by_offset = {lab: (obs(path[i]), close_obs, events) for i, lab in enumerate(OFFSETS)}

    # Check 2 input: entry venue's own probability at the 1h offset, through the
    # fixed two-way proportional de-vig convention of §9.2.
    p1h = path[2]
    fair_q = np.clip(p1h + XVENUE_SLOPE * (p1h - 0.5) + rng.normal(0, XVENUE_SD, n), 0.01, 0.99)
    overround = 1.0 + ENTRY_OVERROUND
    q = two_way_proportional_devig(fair_q * overround, (1 - fair_q) * overround)
    q_obs = 1 - q if entry_flip else q

    # Summary input: random side at the 6h offset, same-reference pricing (d = 1/p_e).
    home = rng.random(n) < 0.5
    pe_side = np.where(home, obs(path[1]), 1 - obs(path[1]))
    pc_side = np.where(home, close_obs, 1 - close_obs)

    return dict(
        by_offset=by_offset,
        agreement=(q_obs, obs(p1h), events, entry_ids),
        residual=(pe_side, pc_side, 1 / pe_side, events),
    )


def run_check1(sim):
    return check1(sim["by_offset"], MARGIN_INTERCEPT, MARGIN_SLOPE,
                  MARGIN_CURVATURE, CURVATURE_RADIUS, expected_offsets=OFFSETS)


def run_check2(sim, resolved=()):
    result = agreement_test(*sim["agreement"], MARGIN_AGREE_SLOPE, OUTLIER)
    return result, agreement_gate(result, resolved, max_verified_rate=MAX_VERIFIED_RATE)


def flagged_bins(sim):
    return [
        (lab, b) for lab, (p_e, p_c, ev) in sim["by_offset"].items()
        for b in binned_lack_of_fit(p_e, p_c, ev, CURVATURE_RADIUS, PRICE_BINS, BIN_TRIGGER)
        if b.flagged
    ]


# ------------------------------------------------------------------ mechanics

def test_interval_that_includes_zero_is_not_a_pass():
    est = Estimate(0.003, 0.002)          # 90% interval about [-0.0003, +0.0063]
    assert est.lo < 0 < est.hi
    assert not est.within(MARGIN_INTERCEPT)
    assert Estimate(0.0, 0.001).within(MARGIN_INTERCEPT)


def test_min_clusters_matches_specification():
    assert MIN_CLUSTERS == MIN_CLUSTERS_SPEC


def test_too_few_clusters_refuses_to_produce_an_interval():
    with pytest.raises(InsufficientCalibrationData):
        ols_cluster(np.zeros(20), np.ones((20, 1)), np.arange(20) % 10)


def test_rank_deficient_regression_is_rejected():
    x = np.ones(100)
    with pytest.raises(InsufficientCalibrationData, match="rank-deficient"):
        ols_cluster(np.zeros(100), np.column_stack([x, x]), np.arange(100))


@pytest.mark.parametrize("prices", [np.full(100, 0.5), np.tile([0.45, 0.55], 50)])
def test_check1_without_price_variation_is_insufficient_evidence(prices):
    with pytest.raises(InsufficientCalibrationData, match="rank-deficient"):
        check1({"1h": (prices, prices, np.arange(100))}, MARGIN_INTERCEPT, MARGIN_SLOPE,
               MARGIN_CURVATURE, CURVATURE_RADIUS, expected_offsets=("1h",))


def test_insufficient_data_is_not_a_value_error():
    assert not issubclass(InsufficientCalibrationData, ValueError)


def test_intercept_is_sample_average_drift_even_when_prices_are_skewed():
    # Home sides favored on average (mean p about 0.56), uniform +1 pp drift:
    # the intercept recovers the average drift, not drift at p = 0.5 less slope * offset.
    sim = simulate(np.random.default_rng(12), p_range=(0.42, 0.70), level_drift=0.01,
                   price_drift=0.04)
    _, res = run_check1(sim)
    for r, (p_e, p_c, _) in zip(res, sim["by_offset"].values()):
        assert r.intercept.value == pytest.approx(np.mean(p_c - p_e), abs=1e-12)
        assert r.x_center == pytest.approx(np.mean(p_e - 0.5), abs=1e-12)


# ------------------------------------------------------------------ §9.4 item 1

def test_correct_pipeline_passes_both_gates_and_flags_no_bins():
    sim = simulate(np.random.default_rng(1))
    ok1, _ = run_check1(sim)
    c2, gate2 = run_check2(sim)
    assert ok1
    assert c2.passed_slope
    assert len(c2.outlier_entry_ids) == 0
    assert gate2.passed
    assert flagged_bins(sim) == []


# ------------------------------------------------------------------ §9.4 item 2

def test_close_leg_flip_fails_check1_only():
    sim = simulate(np.random.default_rng(2), close_flip=True)
    ok1, res = run_check1(sim)
    assert not ok1
    assert all(r.slope.value < -1.8 for r in res)     # signature: slope near -2
    _, gate2 = run_check2(sim)
    assert gate2.passed                               # entry-time comparison unaffected


def test_entry_leg_flip_fails_check2_only():
    sim = simulate(np.random.default_rng(3), entry_flip=True)
    c2, gate2 = run_check2(sim)
    assert not c2.passed_slope
    assert not gate2.passed
    assert -1.1 < c2.slope.value < -0.9               # signature: slope near -1
    ok1, _ = run_check1(sim)
    assert ok1                                         # documented blindness of check 1


def test_reference_instrument_flip_fails_check2_only():
    # Entry-time and close reference both flipped: the series is still a martingale.
    sim = simulate(np.random.default_rng(4), ref_flip=True)
    c2, gate2 = run_check2(sim)
    assert not c2.passed_slope
    assert not gate2.passed
    ok1, _ = run_check1(sim)
    assert ok1


# ------------------------------------------------------------------ §9.4 item 11

def test_level_drift_fails_check1_and_symmetric_population_residual_misses_it():
    rng = np.random.default_rng(5)
    ok1, res = run_check1(simulate(rng, level_drift=0.02))
    assert not ok1
    assert all(abs(r.intercept.value - 0.02) < 0.003 for r in res)

    # Same drift, large symmetric price population: the EV residual nearly cancels.
    big = simulate(rng, n=50_000, level_drift=0.02)
    resid = random_side_residual(*big["residual"])
    print(f"\nlevel drift +2.00 pp -> random-side mean residual {resid.value*100:+.3f} pp")
    assert abs(resid.value) < 0.002


def test_random_side_ev_residual_does_not_cancel_with_asymmetric_prices():
    sim = simulate(np.random.default_rng(501), n=50_000, p_range=(0.42, 0.70),
                   level_drift=0.02)
    passed, results = run_check1(sim)
    assert not passed
    assert all(abs(r.intercept.value - 0.02) < 0.001 for r in results)
    resid = random_side_residual(*sim["residual"])
    p, pc, _ = sim["by_offset"]["6h"]
    conditional_mean = np.mean((pc - p) * (1 / p - 1 / (1 - p)) / 2)
    print(f"\nasymmetric prices, +2 pp drift: EV residual {resid.value*100:+.3f} pp")
    assert resid.hi < -0.004
    assert resid.value == pytest.approx(conditional_mean, abs=0.001)

    # Both sides of every event isolate the identity from randomization noise.
    paired = random_side_residual(
        np.r_[p, 1-p], np.r_[pc, 1-pc], np.r_[1/p, 1/(1-p)],
        np.tile(np.arange(len(p)), 2),
    )
    assert paired.value == pytest.approx(conditional_mean, abs=1e-12)


def test_linear_price_dependent_drift_fails_check1_via_slope():
    ok1, res = run_check1(simulate(np.random.default_rng(6), price_drift=0.10))
    assert not ok1
    assert any(r.slope.lo > MARGIN_SLOPE for r in res)
    assert all(abs(r.slope.value - 0.10) < 0.04 for r in res)


# ------------------------------------------------------------------ §9.4 item 12

def test_operating_characteristics_at_sample_target():
    rng = np.random.default_rng(8)
    null_pass = np.mean([run_check1(simulate(rng))[0] for _ in range(OC_REPLICATES)])
    level_fail = np.mean([not run_check1(simulate(rng, level_drift=MARGIN_INTERCEPT))[0]
                          for _ in range(OC_REPLICATES)])
    curvature_fail = np.mean([not run_check1(simulate(rng, curvature_drift=MARGIN_CURVATURE))[0]
                              for _ in range(OC_REPLICATES)])
    print(f"\ncheck 1 at n={N_EVENTS}, {OC_REPLICATES} replicates: correct pipeline passes "
          f"{null_pass:.1%}; level drift at {MARGIN_INTERCEPT*100:.1f} pp fails {level_fail:.1%}; "
          f"curvature at {MARGIN_CURVATURE*100:.1f} pp fails {curvature_fail:.1%}")
    assert null_pass >= 0.95
    assert level_fail >= 0.90
    assert curvature_fail >= 0.90


# ------------------------------------------------------------------ §9.4 item 13

def test_near_coinflip_systematic_flip_caught_by_slope_not_outliers():
    # Condition on the price AT ENTRY. Restricting the starting price is not enough:
    # by the 1h offset prices have wandered, and |1 - 2p| is no longer small.
    sim = simulate(np.random.default_rng(7), n=4_000, entry_flip=True)
    q, p, ev, ids = sim["agreement"]
    near = np.abs(p - 0.5) <= 0.02                    # entry prices 0.48-0.52
    c2 = agreement_test(q[near], p[near], ev[near], ids[near], MARGIN_AGREE_SLOPE, OUTLIER)
    print(f"\nnear-coin-flip flip: {c2.n_entries} entries, {len(c2.outlier_entry_ids)} outliers, "
          f"slope {c2.slope.value:+.2f}")
    assert c2.n_entries >= 100
    assert len(c2.outlier_entry_ids) / c2.n_entries < 0.01   # per-entry threshold sees almost nothing
    assert not c2.passed_slope                               # the slope still fails decisively


# ------------------------------------------------------------------ §9.4 item 14

def test_symmetric_quadratic_drift_invisible_to_linear_terms_is_caught():
    # A centered symmetric drift has zero mean and zero linear slope, so an
    # intercept-and-slope fit passes while edge prices move materially. This is
    # why check 1 has a curvature term.
    sim = simulate(np.random.default_rng(9), n=4_000, curvature_drift=0.02)
    ok1, res = run_check1(sim)
    assert not ok1
    assert all(r.curvature.lo > MARGIN_CURVATURE for r in res)

    for label, (p_e, p_c, ev) in sim["by_offset"].items():
        x = (p_e - 0.5) - np.mean(p_e - 0.5)
        beta, se = ols_cluster(p_c - p_e, np.column_stack([np.ones_like(x), x]), ev)
        assert Estimate(beta[0], se[0]).within(MARGIN_INTERCEPT), label
        assert Estimate(beta[1], se[1]).within(MARGIN_SLOPE), label


# ------------------------------------------------------------------ §9.4 item 15

def test_unresolved_outlier_blocks_gate_and_resolution_is_by_entry_id():
    sim = simulate(np.random.default_rng(10))
    q, p, ev, ids = sim["agreement"]
    q = q.copy()
    q[0] = np.clip(p[0] + 0.20, 0.01, 0.99)           # isolated wrong-game/mapping-like error
    c2 = agreement_test(q, p, ev, ids, MARGIN_AGREE_SLOPE, OUTLIER)
    assert c2.passed_slope                            # one isolated row barely moves the slope
    assert c2.outlier_entry_ids == {ids[0]}

    unresolved = agreement_gate(c2, max_verified_rate=MAX_VERIFIED_RATE)
    assert not unresolved.passed
    assert unresolved.unresolved_entry_ids == {ids[0]}

    resolved = agreement_gate(c2, [ids[0]], max_verified_rate=MAX_VERIFIED_RATE)
    assert resolved.passed

    # A resolution that matches no flagged entry is an error, not a no-op:
    # a positional index, or a review attached to the wrong row, cannot pass silently.
    with pytest.raises(ValueError, match="match no flagged entry"):
        agreement_gate(c2, [0], max_verified_rate=MAX_VERIFIED_RATE)
    with pytest.raises(ValueError, match="match no flagged entry"):
        agreement_gate(c2, [ids[0], ids[1]], max_verified_rate=MAX_VERIFIED_RATE)


# ------------------------------------------------------------------ §9.4 item 16

def test_check2_devig_convention_is_fixed_two_way_proportional():
    fair = np.array([0.35, 0.50, 0.65])
    overround = np.array([1.03, 1.07, 1.05])
    got = two_way_proportional_devig(fair * overround, (1 - fair) * overround)
    np.testing.assert_allclose(got, fair, atol=1e-12)


# ------------------------------------------------------------------ §9.4 item 17

def test_event_clustering_does_not_treat_duplicate_rows_as_independent_events():
    # Identical repeated observations inside each event leave the point estimate
    # unchanged and do not shrink the cluster-robust SE as if they were new games.
    rng = np.random.default_rng(11)
    n, repeats = 500, 5
    p = rng.uniform(0.25, 0.75, n)
    q = np.clip(p + rng.normal(0, 0.015, n), 0.01, 0.99)
    ev = np.arange(n)
    base = agreement_test(q, p, ev, np.arange(n), MARGIN_AGREE_SLOPE, OUTLIER)
    dup = agreement_test(np.repeat(q, repeats), np.repeat(p, repeats), np.repeat(ev, repeats),
                         np.arange(n * repeats), MARGIN_AGREE_SLOPE, OUTLIER)
    assert dup.slope.value == pytest.approx(base.slope.value, abs=1e-12)
    assert dup.slope.se == pytest.approx(base.slope.se, rel=0.01)


# ------------------------------------------------------------------ §9.4 item 18

def test_verified_difference_ceiling_fails_gate_even_when_every_outlier_is_reviewed():
    sim = simulate(np.random.default_rng(13))
    q, p, ev, ids = sim["agreement"]
    q = q.copy()
    k = int(0.02 * len(q))                            # 2% genuine-looking disagreements
    q[:k] = np.clip(p[:k] + 0.12, 0.01, 0.99)
    c2 = agreement_test(q, p, ev, ids, MARGIN_AGREE_SLOPE, OUTLIER)
    assert c2.passed_slope
    gate = agreement_gate(c2, c2.outlier_entry_ids, max_verified_rate=MAX_VERIFIED_RATE)
    assert not gate.unresolved_entry_ids
    assert gate.verified_rate_exceeded and not gate.passed

    few = sorted(c2.outlier_entry_ids)[: int(0.5 * MAX_VERIFIED_RATE * len(q))]
    c2_few = agreement_test(np.where(np.isin(ids, few), q, sim["agreement"][0]),
                            p, ev, ids, MARGIN_AGREE_SLOPE, OUTLIER)
    assert agreement_gate(c2_few, c2_few.outlier_entry_ids,
                          max_verified_rate=MAX_VERIFIED_RATE).passed


# ------------------------------------------------------------------ §9.4 item 19

def test_binned_diagnostic_catches_localized_drift_that_check1_usually_misses():
    # +2.5 pp drift confined to entry prices 0.56-0.60. Check 1 sees only its
    # small effect on the three fitted terms and passes most samples; the bins
    # see it almost always, and only in bins overlapping the band (a property
    # of this fixture: projection can spread other faults across bins).
    rng = np.random.default_rng(14)
    reps = 200
    check1_pass, bin_flag, localized = [], [], []
    for _ in range(reps):
        sim = simulate(rng, band_drift=(0.56, 0.60, 0.025))
        check1_pass.append(run_check1(sim)[0])
        flags = flagged_bins(sim)
        bin_flag.append(bool(flags))
        localized.append(all(b.price_lo <= 0.60 and b.price_hi >= 0.56 for _, b in flags))
    print(f"\nlocalized band drift, {reps} replicates: check 1 passes {np.mean(check1_pass):.1%}; "
          f"bins flag {np.mean(bin_flag):.1%}")
    assert np.mean(check1_pass) >= 0.50      # check 1 alone is unreliable for this shape
    assert np.mean(bin_flag) >= 0.95
    assert all(localized)


def test_binned_diagnostic_rarely_flags_a_correct_pipeline():
    rng = np.random.default_rng(15)
    reps = 200
    any_flag = np.mean([bool(flagged_bins(simulate(rng))) for _ in range(reps)])
    print(f"\nbinned diagnostic, correct pipeline: any flag in {any_flag:.1%} of {reps} replicates")
    assert any_flag <= 0.02


# ------------------------------------------------------------------ §9.4 items 20–22

@pytest.mark.parametrize("offsets", [(), ("15m",), OFFSETS[:-1]])
def test_check1_missing_required_offsets_is_insufficient_evidence(offsets):
    sim = simulate(np.random.default_rng(20))
    sim["by_offset"] = {label: sim["by_offset"][label] for label in offsets}
    with pytest.raises(InsufficientCalibrationData, match="missing required offsets"):
        run_check1(sim)


def test_check1_rejects_unexpected_offsets_and_invalid_specifications():
    sim = simulate(np.random.default_rng(21))
    sim["by_offset"]["typo"] = sim["by_offset"]["15m"]
    with pytest.raises(ValueError, match="unexpected offsets"):
        run_check1(sim)
    for expected in ((), ("15m", "15m"), ("",), "15m"):
        with pytest.raises(ValueError, match="expected_offsets"):
            check1({}, MARGIN_INTERCEPT, MARGIN_SLOPE, MARGIN_CURVATURE,
                   CURVATURE_RADIUS, expected_offsets=expected)


def test_check1_empty_leg_with_populated_others_is_an_error():
    sim = simulate(np.random.default_rng(23))
    p, pc, ev = sim["by_offset"]["24h"]
    sim["by_offset"]["24h"] = (p[:0], pc, ev)
    with pytest.raises(ValueError, match="same length"):
        run_check1(sim)


@pytest.mark.parametrize("n", [0, 1, 2, 3, 10])
def test_check1_unavailable_offset_cannot_be_a_statistical_pass(n):
    sim = simulate(np.random.default_rng(22))
    sim["by_offset"]["24h"] = tuple(x[:n] for x in sim["by_offset"]["24h"])
    with pytest.raises(InsufficientCalibrationData):
        run_check1(sim)


def test_check1_returns_results_in_specification_order():
    sim = simulate(np.random.default_rng(1))
    sim["by_offset"] = dict(reversed(list(sim["by_offset"].items())))
    passed, results = run_check1(sim)
    assert passed
    assert tuple(r.offset for r in results) == OFFSETS


def test_binned_intervals_have_nominal_coverage_with_events_spanning_bins():
    # Fixed prices, correlated errors: the same event contributes observations
    # to different bins. Coverage must include the full-sample fitted-curve
    # uncertainty and cross-bin event covariance, not just within-bin residuals.
    rng = np.random.default_rng(617)
    events = np.repeat(np.arange(600), 2)
    p = rng.uniform(0.30, 0.70, len(events))
    covered = np.zeros(PRICE_BINS)
    for _ in range(OC_REPLICATES):
        error = np.repeat(rng.normal(0, 0.025, 600), 2) + rng.normal(0, 0.01, len(p))
        bins = binned_lack_of_fit(p, p + error, events, CURVATURE_RADIUS,
                                 PRICE_BINS, BIN_TRIGGER)
        covered += [b.residual.lo <= 0 <= b.residual.hi for b in bins]
    rates = covered / OC_REPLICATES
    print(f"\n90% bin interval coverage, {OC_REPLICATES} replicates: {rates}")
    # Monte Carlo tolerance; old within-bin-only intervals overcover especially
    # in the edge bins and fail this check.
    assert np.all((rates >= 0.865) & (rates <= 0.935))


def test_binned_diagnostic_requires_enough_events_in_each_bin():
    p = np.linspace(0.30, 0.70, 100)
    with pytest.raises(InsufficientCalibrationData, match="bin"):
        binned_lack_of_fit(p, p, np.arange(len(p)), CURVATURE_RADIUS,
                          PRICE_BINS, BIN_TRIGGER)


def test_binned_membership_is_independent_of_row_order_with_tied_prices():
    rng = np.random.default_rng(24)
    p = np.round(rng.uniform(0.30, 0.70, N_EVENTS), 2)    # coarse grid: many ties
    pc = np.clip(p + rng.normal(0, 0.02, N_EVENTS), 0, 1)
    events = np.arange(N_EVENTS)
    perm = rng.permutation(N_EVENTS)
    a = binned_lack_of_fit(p, pc, events, CURVATURE_RADIUS, PRICE_BINS, BIN_TRIGGER)
    b = binned_lack_of_fit(p[perm], pc[perm], events[perm], CURVATURE_RADIUS,
                           PRICE_BINS, BIN_TRIGGER)
    assert [(x.price_lo, x.price_hi, x.n_rows) for x in a] == \
           [(x.price_lo, x.price_hi, x.n_rows) for x in b]
    for x, y in zip(a, b):
        assert x.residual.value == pytest.approx(y.residual.value, abs=1e-12)
        assert x.residual.se == pytest.approx(y.residual.se, abs=1e-12)
    for lo_bin, hi_bin in zip(a, a[1:]):
        assert lo_bin.price_hi < hi_bin.price_lo             # no price in two bins
