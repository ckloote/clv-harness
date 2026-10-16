"""The calibration simulation's reference-series generator (DESIGN.md §9.2, §9.4).

tests/test_calibration_simulation.py is the authority for what the checks can and cannot
detect, and it draws its samples here. B2 reruns the §9.4 item-12 operating characteristics
with the measured reference series (DESIGN.md §10: "rerun with measured reference-series
dispersion once B1 or A1 provides it"): `increment_sd`, `start_prices` and per-offset counts
replace the planning assumptions below. The defaults reproduce the committed simulation
exactly, draw for draw.
"""
from __future__ import annotations

from collections.abc import Sequence

import numpy as np

from clv.score.controls import check1, two_way_proportional_devig

# Planning assumptions about the reference series, used unless measurements are given.
OFFSETS = ("24h", "6h", "1h", "15m")
INCREMENT_SD = (0.03, 0.02, 0.01, 0.01)   # 24h->6h, 6h->1h, 1h->15m, 15m->close
XVENUE_SD = 0.01                          # genuine entry-venue vs reference disagreement
XVENUE_SLOPE = 0.03                       # plausible price-level cross-venue disagreement
ENTRY_OVERROUND = 0.045                   # only to exercise the fixed proportional de-vig path


def simulate(
    rng,
    n,
    p_range=(0.30, 0.70),
    level_drift=0.0,
    price_drift=0.0,
    curvature_drift=0.0,
    band_drift=None,          # (price_lo, price_hi, size): drift only inside a price band
    close_flip=False,
    ref_flip=False,
    entry_flip=False,
    *,
    curvature_radius=0.20,
    offsets: Sequence[str] = OFFSETS,
    increment_sd: Sequence[float] = INCREMENT_SD,
    start_prices=None,        # draw the earliest offset's prices from these, else uniform on p_range
):
    """One sample of events as the pipeline would observe them.

    True reference prices follow a martingale between offsets. Drift, if any,
    is injected on the final step into the close, so every offset sees it.
    `curvature_drift` is the centered symmetric quadratic departure at
    p = 0.5 +/- curvature_radius relative to p = 0.5. Faults are applied to what
    the pipeline OBSERVES, not to the truth. Entry IDs are strings, so nothing
    downstream can confuse them with array positions.

    `increment_sd` has one step per offset: from each offset to the next, the last into the close.
    """
    if len(increment_sd) != len(offsets):
        raise ValueError("increment_sd needs one step per offset, the last into the close")
    events = np.arange(n)
    entry_ids = np.array([f"E{i:06d}" for i in range(n)])
    cur = rng.uniform(*p_range, n) if start_prices is None else rng.choice(np.asarray(start_prices, float), n)
    path = [cur]
    for sd in increment_sd[:-1]:
        cur = np.clip(cur + rng.normal(0, sd, n), 0.02, 0.98)
        path.append(cur)
    last = path[-1]

    q_curve = ((last - 0.5) / curvature_radius) ** 2
    q_curve -= np.mean(q_curve)
    drift = level_drift + price_drift * (last - 0.5) + curvature_drift * q_curve
    if band_drift is not None:
        lo, hi, size = band_drift
        drift = drift + np.where((last >= lo) & (last < hi), size, 0.0)
    close = np.clip(last + rng.normal(0, increment_sd[-1], n) + drift, 0.02, 0.98)

    obs = lambda x: 1 - x if ref_flip else x          # reference-instrument mapping
    close_obs = 1 - obs(close) if close_flip else obs(close)

    # Check 1 input: fixed canonical side (home) at every offset.
    by_offset = {lab: (obs(path[i]), close_obs, events) for i, lab in enumerate(offsets)}

    # Check 2 input: entry venue's own probability at the third offset (1h by default), through
    # the fixed two-way proportional de-vig convention of §9.2.
    p1h = path[min(2, len(path) - 1)]
    fair_q = np.clip(p1h + XVENUE_SLOPE * (p1h - 0.5) + rng.normal(0, XVENUE_SD, n), 0.01, 0.99)
    overround = 1.0 + ENTRY_OVERROUND
    q = two_way_proportional_devig(fair_q * overround, (1 - fair_q) * overround)
    q_obs = 1 - q if entry_flip else q

    # Summary input: random side at the second offset (6h by default), same-reference pricing (d = 1/p_e).
    home = rng.random(n) < 0.5
    p6h = path[min(1, len(path) - 1)]
    pe_side = np.where(home, obs(p6h), 1 - obs(p6h))
    pc_side = np.where(home, close_obs, 1 - close_obs)

    return dict(
        by_offset=by_offset,
        agreement=(q_obs, obs(p1h), events, entry_ids),
        residual=(pe_side, pc_side, 1 / pe_side, events),
    )


def increments_from_spread(sd_dp: Sequence[float]) -> tuple[float, ...]:
    """Martingale step sizes from the measured spread of Δp = p_close - p_offset at each offset,
    earliest first: Var(Δp_k) is the sum of the steps from offset k to the close, so each step is
    the difference of adjacent variances (floored at zero where sampling noise inverts them)."""
    v = [s * s for s in sd_dp]
    return tuple(float(np.sqrt(max(a - b, 0.0))) for a, b in zip(v, v[1:])) + (float(sd_dp[-1]),)


def operating_characteristics(rng, *, offsets: Sequence[str], counts: Sequence[int], increment_sd: Sequence[float],
                              start_prices, margin_intercept: float, margin_slope: float, margin_curvature: float,
                              curvature_radius: float, replicates: int = 1_000) -> dict[str, float]:
    """§9.4 item 12 under a measured reference series: how often check 1 passes a correct pipeline,
    and fails a level drift and a symmetric quadratic drift each equal to its margin. Each offset
    keeps its measured count of events, a random subset of the simulated ones (a missing offset
    price is treated as missing at random); offsets are fitted separately, as in check 1."""
    n = max(counts)

    def passes(**drift) -> bool:
        sim = simulate(rng, n, curvature_radius=curvature_radius, offsets=offsets, increment_sd=increment_sd,
                       start_prices=start_prices, **drift)
        by_offset = {}
        for lab, k in zip(offsets, counts):
            keep = np.sort(rng.choice(n, k, replace=False))
            p_e, p_c, ev = sim["by_offset"][lab]
            by_offset[lab] = (p_e[keep], p_c[keep], ev[keep])
        return check1(by_offset, margin_intercept, margin_slope, margin_curvature, curvature_radius,
                      expected_offsets=offsets)[0]

    return {"correct_pipeline_passes": float(np.mean([passes() for _ in range(replicates)])),
            "level_drift_fails": float(np.mean([not passes(level_drift=margin_intercept)
                                                for _ in range(replicates)])),
            "curvature_drift_fails": float(np.mean([not passes(curvature_drift=margin_curvature)
                                                    for _ in range(replicates)])),
            "replicates": replicates}
