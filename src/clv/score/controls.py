"""Statistical core of the calibration checks (DESIGN.md §9.2).

Pure functions over arrays; no database access. The harness feeds these from
close_price / clv_score rows. Every claim made about these checks in §9.2 is
backed by tests/test_calibration_simulation.py, which must pass before §9.2 or
the §10 control margins are changed.

Conventions
-----------
* Probabilities are floats in [0, 1] here; the database stores them as
  integers scaled by 10,000 and the caller converts at the edge.
* Every interval is a two-sided 90% interval, i.e. two one-sided tests at 5%
  (§10 `controls.equivalence_ci`). A check PASSES only when the whole interval
  lies inside the margin. An interval that merely includes zero is not a pass.
* Standard errors are cluster-robust by event (CR1) with a normal
  approximation, so a minimum cluster count is enforced.
* Check 1 gates level, linear price-dependent and symmetric quadratic drift.
  It does not prove the absence of every nonlinear departure;
  `binned_lack_of_fit` is the §9.2 diagnostic for shapes outside that model.
* Check 2 identifies entries by entry ID, never by array position.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

import numpy as np

Z90 = 1.6448536269514722   # two-sided 90% == two one-sided tests at 5%
MIN_CLUSTERS = 30          # below this the normal approximation is not trusted
                           # fixed in code, not read from params.toml (DESIGN.md §10)


class InsufficientCalibrationData(Exception):
    """No statistical verdict is available; callers must not record a pass.

    Deliberately not a ValueError, so `except ValueError` handlers for
    configuration/pipeline errors cannot absorb it (or vice versa).
    """


def _float_1d(values, name: str, allow_empty: bool = False) -> np.ndarray:
    arr = np.asarray(values, dtype=float)
    if arr.ndim != 1:
        raise ValueError(f"{name} must be one-dimensional")
    if len(arr) == 0 and not allow_empty:
        raise ValueError(f"{name} must not be empty")
    if not np.all(np.isfinite(arr)):
        raise ValueError(f"{name} contains non-finite values")
    return arr


def _prob_1d(values, name: str, allow_empty: bool = False) -> np.ndarray:
    arr = _float_1d(values, name, allow_empty)
    if np.any((arr < 0.0) | (arr > 1.0)):
        raise ValueError(f"{name} must lie in [0, 1]")
    return arr


def _ids(values) -> frozenset:
    """Normalize entry IDs (numpy scalars included) to plain hashable values."""
    return frozenset(v.item() if hasattr(v, "item") else v for v in values)


@dataclass(frozen=True)
class Estimate:
    value: float
    se: float

    @property
    def lo(self) -> float:
        return self.value - Z90 * self.se

    @property
    def hi(self) -> float:
        return self.value + Z90 * self.se

    def within(self, margin: float) -> bool:
        """Equivalence: the entire 90% interval lies inside +/- margin."""
        if margin < 0:
            raise ValueError("margin must be non-negative")
        return -margin <= self.lo and self.hi <= margin


def _cluster_index(clusters) -> tuple[np.ndarray, int]:
    """Event index per row and cluster count G, enforcing MIN_CLUSTERS."""
    _, idx = np.unique(clusters, return_inverse=True)
    G = int(idx.max()) + 1 if len(idx) else 0
    if G < MIN_CLUSTERS:
        raise InsufficientCalibrationData(
            f"{G} clusters < MIN_CLUSTERS={MIN_CLUSTERS}; interval not trusted"
        )
    return idx, G


def _cr1_factor(G: int, n: int, k: int) -> float:
    """CR1 small-sample correction G/(G-1) * (n-1)/(n-k)."""
    return (G / (G - 1)) * ((n - 1) / (n - k))


def ols_cluster(y, X, clusters) -> tuple[np.ndarray, np.ndarray]:
    """OLS with event-clustered (CR1) standard errors. Returns (beta, se)."""
    y = _float_1d(y, "y")
    X = np.asarray(X, dtype=float)
    clusters = np.asarray(clusters)

    if X.ndim != 2:
        raise ValueError("X must be two-dimensional")
    if not np.all(np.isfinite(X)):
        raise ValueError("X contains non-finite values")
    if len(y) != X.shape[0] or len(y) != len(clusters):
        raise ValueError("y, X, and clusters must have the same row count")

    n, k = X.shape
    idx, G = _cluster_index(clusters)
    # Every design here is built from the data (e.g. too little price variation),
    # so a rank-deficient or too-wide X is missing evidence, not a config error.
    if n <= k or np.linalg.matrix_rank(X) < k:
        raise InsufficientCalibrationData(
            f"X is rank-deficient or has too few rows (n={n}, k={k})"
        )

    xtx_inv = np.linalg.inv(X.T @ X)
    beta = xtx_inv @ (X.T @ y)
    u = y - X @ beta
    scores = np.zeros((G, k))
    np.add.at(scores, idx, X * u[:, None])
    cov = xtx_inv @ (scores.T @ scores) @ xtx_inv
    cov *= _cr1_factor(G, n, k)
    diag = np.diag(cov)
    if np.any(diag < -1e-15):
        raise ValueError("cluster covariance has materially negative diagonal")
    return beta, np.sqrt(np.maximum(diag, 0.0))


# --------------------------------------------------------------------------
# Check 1 — fixed-side martingale test (reference and close leg). HARD GATE.
# --------------------------------------------------------------------------

def _check1_design(p_entry: np.ndarray, curvature_radius: float):
    """Check-1 regressors. Both non-constant terms are centered on the sample,
    so the intercept is the sample-average drift. Returns (X, x_center, q_center).
    """
    x_raw = p_entry - 0.5
    q_raw = (x_raw / curvature_radius) ** 2
    x_center, q_center = float(x_raw.mean()), float(q_raw.mean())
    X = np.column_stack([np.ones_like(p_entry), x_raw - x_center, q_raw - q_center])
    return X, x_center, q_center


def _check1_inputs(p_entry, p_close, event_id, curvature_radius):
    if curvature_radius <= 0:
        raise ValueError("curvature_radius must be positive")
    p_entry = _prob_1d(p_entry, "p_entry", allow_empty=True)
    p_close = _prob_1d(p_close, "p_close", allow_empty=True)
    event_id = np.asarray(event_id)
    if len(p_entry) != len(p_close) or len(p_entry) != len(event_id):
        raise ValueError("p_entry, p_close, and event_id must have the same length")
    if len(p_entry) == 0:
        raise InsufficientCalibrationData("no observations")
    return p_entry, p_close, event_id


@dataclass(frozen=True)
class MartingaleResult:
    offset: str
    n_events: int
    intercept: Estimate   # sample-average drift
    slope: Estimate       # drift per unit of (p_entry - 0.5); about -2 => close-leg flip
    curvature: Estimate   # symmetric drift at 0.5 +/- radius relative to 0.5
    x_center: float       # centering constants: recorded with the calibration run,
    q_center: float       #   required by the research protocol's drift-bias bound
    passed: bool


def martingale_test(
    p_entry,
    p_close,
    event_id,
    offset: str,
    margin_intercept: float,
    margin_slope: float,
    margin_curvature: float,
    curvature_radius: float,
) -> MartingaleResult:
    """Check 1 at one entry offset.

    Inputs must be for ONE FIXED canonical side per event (home), never a
    randomly chosen side: side randomization cancels unweighted probability
    moves. Payout-weighted EV residuals need not cancel (§9.2).

    Regressors: x = (p - 0.5) and q = ((p - 0.5) / radius)^2, each centered on
    the sample. The intercept is therefore the sample-average drift; the
    curvature coefficient is the fitted extra drift at p = 0.5 +/- radius
    relative to p = 0.5 for a symmetric quadratic departure.
    """
    p_entry, p_close, event_id = _check1_inputs(p_entry, p_close, event_id, curvature_radius)
    X, x_center, q_center = _check1_design(p_entry, curvature_radius)
    beta, se = ols_cluster(p_close - p_entry, X, event_id)
    intercept = Estimate(beta[0], se[0])
    slope = Estimate(beta[1], se[1])
    curvature = Estimate(beta[2], se[2])
    return MartingaleResult(
        offset=offset,
        n_events=len(np.unique(event_id)),
        intercept=intercept,
        slope=slope,
        curvature=curvature,
        x_center=x_center,
        q_center=q_center,
        passed=(
            intercept.within(margin_intercept)
            and slope.within(margin_slope)
            and curvature.within(margin_curvature)
        ),
    )


def check1(
    by_offset: dict,
    margin_intercept: float,
    margin_slope: float,
    margin_curvature: float,
    curvature_radius: float,
    *,
    expected_offsets: Iterable[str],
):
    """Run check 1 at every offset.

    by_offset: {label: (p_entry, p_close, event_id)}. Passes only if every
    configured offset passes all three equivalence components. expected_offsets
    comes from the analysis specification, never from the available data keys.
    Missing offsets/observations raise InsufficientCalibrationData; invalid
    offset configuration or unexpected offsets raise ValueError. Neither is a
    statistical pass or failure. Results follow the configured offset order.
    """
    if isinstance(expected_offsets, str):
        raise ValueError("expected_offsets must be a sequence of labels, not a string")
    expected = tuple(expected_offsets)
    if (not expected or any(not isinstance(label, str) or not label for label in expected)
            or len(set(expected)) != len(expected)):
        raise ValueError("expected_offsets must be nonempty, unique, nonempty strings")
    unexpected = set(by_offset) - set(expected)
    if unexpected:
        raise ValueError(f"unexpected offsets: {sorted(unexpected)}")
    missing = set(expected) - set(by_offset)
    if missing:
        raise InsufficientCalibrationData(f"missing required offsets: {sorted(missing)}")
    results = [
        martingale_test(*by_offset[label], label,
                        margin_intercept, margin_slope, margin_curvature, curvature_radius)
        for label in expected
    ]
    return all(r.passed for r in results), results


@dataclass(frozen=True)
class BinResult:
    price_lo: float
    price_hi: float
    n_rows: int
    residual: Estimate    # mean residual from the fitted three-term curve
    flagged: bool


def binned_lack_of_fit(p_entry, p_close, event_id, curvature_radius: float,
                       n_bins: int, trigger: float) -> list[BinResult]:
    """§9.2 lack-of-fit diagnostic at one offset.

    Fits the check-1 model, then reports the mean residual in n_bins
    equal-count entry-price bins (tied prices share a bin, so counts are
    approximate) with event-clustered intervals. For bin weights
    w (1/n_bin inside the bin, zero elsewhere), its mean residual is a contrast
    a' dp, where a = (I - X (X'X)^-1 X') w. Cluster sums of a_i * residual_i
    over ALL rows propagate the uncertainty from fitting the curve on the
    same sample, including events spanning bins. Prices/bin membership are
    conditioned on; coverage is checked under this fixed-design convention.
    A bin is
    flagged when its whole 90% interval lies outside +/- trigger: evidence of
    material drift the three-term model does not describe. Each flagged bin
    blocks benchmark sign-off until a decision record explains it.
    """
    if not isinstance(n_bins, (int, np.integer)) or n_bins < 2:
        raise ValueError("n_bins must be at least 2")
    if trigger < 0:
        raise ValueError("trigger must be non-negative")
    p_entry, p_close, event_id = _check1_inputs(p_entry, p_close, event_id, curvature_radius)
    X, _, _ = _check1_design(p_entry, curvature_radius)
    dp = p_close - p_entry
    beta, _ = ols_cluster(dp, X, event_id)
    resid = dp - X @ beta
    n, k = X.shape
    cluster_idx, n_clusters = _cluster_index(event_id)
    correction = _cr1_factor(n_clusters, n, k)
    projection = np.linalg.solve(X.T @ X, X.T)

    # Rank by lowest position among equal prices: tied prices always share a
    # bin, so membership does not depend on input row order.
    rank = np.searchsorted(np.sort(p_entry), p_entry, side="left")
    bins = rank * n_bins // n

    out = []
    for b in range(n_bins):
        m = bins == b
        if len(np.unique(event_id[m])) < MIN_CLUSTERS:
            raise InsufficientCalibrationData(
                f"bin {b} has fewer than {MIN_CLUSTERS} events; interval not trusted"
            )
        weights = m.astype(float) / m.sum()
        contrast = weights - X @ (projection @ weights)
        scores = np.zeros(n_clusters)
        np.add.at(scores, cluster_idx, contrast * resid)
        se = np.sqrt(correction * (scores @ scores))
        est = Estimate(float(weights @ resid), float(se))
        out.append(BinResult(
            price_lo=float(p_entry[m].min()),
            price_hi=float(p_entry[m].max()),
            n_rows=int(m.sum()),
            residual=est,
            flagged=bool(est.lo > trigger or est.hi < -trigger),
        ))
    return out


# --------------------------------------------------------------------------
# Check 2 — entry-to-reference agreement (entry leg). HARD GATE.
# --------------------------------------------------------------------------

def two_way_proportional_devig(implied_side, implied_other) -> np.ndarray:
    """Fixed Check-2 de-vig convention for a two-outcome entry market.

    Inputs are the two raw implied probabilities from the same contemporaneous
    two-way market: q_side = pi_side / (pi_side + pi_other). This convention is
    fixed for Check 2; it does not assert that proportional de-vig is the best
    economic model for downstream CLV scoring.
    """
    side = _float_1d(implied_side, "implied_side")
    other = _float_1d(implied_other, "implied_other")
    if len(side) != len(other):
        raise ValueError("implied_side and implied_other must have the same length")
    if np.any(side <= 0) or np.any(other <= 0):
        raise ValueError("raw implied probabilities must be positive")
    return side / (side + other)


@dataclass(frozen=True)
class AgreementResult:
    n_entries: int
    slope: Estimate              # about 0 when mappings agree; about -1 under a flip on either venue
    intercept: Estimate          # reported, not gated
    outlier_entry_ids: frozenset # flagged entries; each needs a recorded disposition
    passed_slope: bool


@dataclass(frozen=True)
class AgreementGateResult:
    passed: bool
    unresolved_entry_ids: frozenset
    verified_rate: float         # verified market differences / entries
    verified_rate_exceeded: bool


def agreement_test(q_entry, p_entry, event_id, entry_id,
                   margin_slope: float, outlier_threshold: float) -> AgreementResult:
    """Check 2 statistical component.

    q_entry: the entry venue's own probability from the fixed two-way
    proportional de-vig of its contemporaneous two-sided quote. p_entry: the
    reference probability at the same as-of time. entry_id: unique per row.

    A polarity or mapping flip on either venue makes q_e - p_e close to
    1 - 2*p_e, i.e. a slope near -1 on (2*p_e - 1). The slope catches systematic
    flips even among near-coin-flip games; the outlier set catches isolated
    errors, which `agreement_gate` requires to be resolved by entry ID.
    """
    if outlier_threshold < 0:
        raise ValueError("outlier_threshold must be non-negative")
    q = _prob_1d(q_entry, "q_entry")
    p = _prob_1d(p_entry, "p_entry")
    event_id = np.asarray(event_id)
    entry_id = np.asarray(entry_id)
    if not (len(q) == len(p) == len(event_id) == len(entry_id)):
        raise ValueError("q_entry, p_entry, event_id and entry_id must have the same length")
    if len(np.unique(entry_id)) != len(entry_id):
        raise ValueError("entry_id must be unique")

    diff = q - p
    X = np.column_stack([np.ones_like(p), 2 * p - 1])
    beta, se = ols_cluster(diff, X, event_id)
    slope = Estimate(beta[1], se[1])
    return AgreementResult(
        n_entries=len(q),
        slope=slope,
        intercept=Estimate(beta[0], se[0]),
        outlier_entry_ids=_ids(entry_id[np.abs(diff) > outlier_threshold]),
        passed_slope=slope.within(margin_slope),
    )


def agreement_gate(result: AgreementResult, resolved_entry_ids: Iterable = (), *,
                   max_verified_rate: float) -> AgreementGateResult:
    """Combine Check 2's statistics with recorded outlier dispositions.

    `resolved_entry_ids` are the entries whose current-run review is
    `verified_market_difference` (§9.2); the orchestration layer supplies them.
    A resolution that matches no flagged entry is an error, never a no-op.
    If verified differences exceed `max_verified_rate` of entries, the gate
    fails however good the reviews are: at that rate the threshold or the venue
    pairing is the finding.
    """
    if not 0 <= max_verified_rate <= 1:
        raise ValueError("max_verified_rate must lie in [0, 1]")
    resolved = _ids(resolved_entry_ids)
    unknown = resolved - result.outlier_entry_ids
    if unknown:
        raise ValueError(f"{len(unknown)} resolution(s) match no flagged entry, e.g. {list(unknown)[:5]}")
    unresolved = result.outlier_entry_ids - resolved
    rate = len(resolved) / result.n_entries
    exceeded = rate > max_verified_rate
    return AgreementGateResult(
        passed=result.passed_slope and not unresolved and not exceeded,
        unresolved_entry_ids=unresolved,
        verified_rate=rate,
        verified_rate_exceeded=exceeded,
    )


# --------------------------------------------------------------------------
# Summary only — random-side residual. NEVER A GATE.
# --------------------------------------------------------------------------

def random_side_residual(p_entry, p_close, d_entry, event_id) -> Estimate:
    """Mean of (p_close - p_entry) * d for the RANDOMLY CHOSEN side.

    Diagnostic only: sensitivity to drift depends on prices and payout factors.
    Conditional on a home-side move dp, equally likely sides have expected EV
    residual dp * (d_home - d_away) / 2, not generally zero. The simulation
    exercises both symmetric cancellation and asymmetric price populations.
    """
    p_entry = _prob_1d(p_entry, "p_entry")
    p_close = _prob_1d(p_close, "p_close")
    d_entry = _float_1d(d_entry, "d_entry")
    event_id = np.asarray(event_id)
    if not (len(p_entry) == len(p_close) == len(d_entry) == len(event_id)):
        raise ValueError("residual inputs must have the same length")
    if np.any(d_entry <= 0):
        raise ValueError("d_entry must be positive")
    r = (p_close - p_entry) * d_entry
    beta, se = ols_cluster(r, np.ones((len(r), 1)), event_id)
    return Estimate(beta[0], se[0])
