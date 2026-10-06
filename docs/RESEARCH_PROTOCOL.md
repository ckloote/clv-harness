# Research Protocol — Model Evaluation

**Status:** inactive. Activates when a signal model exists.  
**Relationship to the harness:** the harness preserves the evidence this protocol needs (`DESIGN.md` §11.4); it does not implement model evaluation. Pure arithmetic fixtures for §6's bound already live in `tests/test_research_bound_specification.py` to validate the specification. The production cohort logic, tables and remaining tests are added when this protocol activates (`DESIGN.md` §8.5).

---

## 1. Claims

Claims about a model follow the hierarchy in `DESIGN.md` §1.1: positive CLV, robust positive CLV, executable fee-adjusted advantage, and outcome validation. Each level requires the evidence below in addition to harness gates G1–G3.

## 2. Analysis cohorts and preregistration

An analysis cohort specifies sport, season, markets, contract-rule equivalence, decision windows, entry venues, quote-age limits, reference venue, close definition, model version and exclusions. Freeze this specification before viewing holdout results.

This adds the `analysis_cohort` table: prespecified inclusion, stratification and holdout policy, with population counts. A cohort's coverage waterfall is the harness waterfall (`DESIGN.md` §11.2) restricted to that cohort.

## 3. Estimation and uncertainty

The headline result is the preregistered **event-weighted** or clearly specified entry-weighted mean CLV, with event-cluster bootstrap confidence intervals. Report both to expose domination by games generating many entries. Dates are a secondary clustering/sensitivity unit where common shocks affect several games. Never treat twenty quotes on the same game as twenty independent events.

Accompany the mean with median, percent positive, eligible sample size, unique events, missingness, effective time horizon and distributions by spread, depth, age, off confidence, time before off, sport and entry/reference venue. Consider uncertainty from the benchmark itself: disagreement and benchmark sensitivity are material, not just statistical noise.

For missing-not-at-random scoreability, disclose how excluded games differ from scored games. Provide transparent sensitivity bounds or alternate-cohort results where possible; **do not** fill missing closes with modeled prices and call the resulting mean observed CLV.

## 4. Chronological evaluation

Calibration and reference-benchmark selection use a **development period**. Lock the chosen close definition, off policy, entry/execution timing and analysis cohort before testing a model on a separate later period. Multiple versions/thresholds selected after viewing validation CLV require a fresh holdout or appropriate multiple-testing treatment.

For an eventual signal model, compare against prespecified alternatives and null controls under the same eligibility rules. Never use only the model's chosen bets as the basis for proving that the measurement pipeline is sound. Keep all emitted candidates and the contemporaneous feature as-of set.

## 5. Outcome corroboration and late-news analysis

When sufficient independent settlements accumulate, evaluate probability calibration, Brier/log loss or suitable proper scoring rules, and realized returns on truly filled entries. Do not demand a small-sample realized-profit result before declaring the CLV collector functional; do require outcome corroboration before escalating to strong profitability claims.

Segment entries around late news, but **do not automatically label a favorable move after a pitcher scratch as fake edge**. What matters is whether the model could have anticipated or known the relevant information at the actual signal time. Segment entries by the timestamp/provenance of the relevant news, the signal's feature availability and the market's adjustment; retain unknowns as unknown.

This adds the `real_fill` table: actual fills and their settlement, never overwriting hypothetical prices.

Downstream analysis: track information-time provenance for large pregame repricings, and add settlement-based calibration and real-fill ROI only when enough independent later games exist.

## 6. Gate: model-performance claims

Beyond harness gates G1–G3, a model-performance claim requires a frozen holdout policy with the corresponding sample collected; event-clustered uncertainty and a prespecified statistical summary on that holdout; and tracking of every model version and threshold, so that no inspected holdout is reused.

**Drift-bias bound.** A passing Check 1 provides statistical evidence of equivalence within its prespecified coefficient margins, not a guarantee of zero drift. The same probability-point drift costs more CLV at longshot payouts. Before any claim, compute the largest mean CLV that coefficients inside those margins could produce over the model's own entries under the calibrated three-term model.

Group entries by the calibration fit `g` that supports them: reference/close definition, calibration population/run and entry offset. Coefficients at different offsets are independently constrained; they need not share a sign. Each entry must have supported timing and price coverage in its assigned fit. The initial policy requires the calibrated offset and its exact as-of quote-selection rule. Do not assign arbitrary entry times to the nearest offset or interpolate coefficient bounds without a separately prespecified and validated calibration policy. Unsupported entries block that population's claim; any restriction to supported entries must be frozen and reflected in the coverage waterfall.

Use the **same normalized nonnegative weights** as the headline mean CLV: `sum_i w_i = 1`. Entry weighting has `w_i = 1/N`; equal event weighting with equal weighting inside each event has `w_i = 1/(G_events * n_entries_in_event_i)`. A different within-event policy must specify its weights explicitly. For each supported entry:

```text
s_i = +1 if the entry backs the fixed canonical side (home), -1 otherwise
p_i = reference probability of the canonical side at entry
x_i = (p_i - 0.5) - x_center_g
q_i = ((p_i - 0.5) / r_g)^2 - q_center_g  # constants from its calibration fit
d_i = entry payout factor

A_g = sum_{i in g} w_i*s_i*d_i
L_g = sum_{i in g} w_i*s_i*d_i*x_i
Q_g = sum_{i in g} w_i*s_i*d_i*q_i
B = sum_g (m_alpha_g*|A_g| + m_beta_g*|L_g| + m_gamma_g*|Q_g|)
```

The `m_*_g` values are the Check-1 margins for that fit and `r_g` is its curvature radius (`DESIGN.md` §10). Drift contributes `s_i * d_i * (alpha_g + beta_g*x_i + gamma_g*q_i)` to entry CLV. Taking absolute values **inside the sum over fits** maximizes this weighted contribution over the product of their coefficient boxes. `B` is the exact maximum over that box; it is not a simultaneous confidence bound on arbitrary reference error, nor proof that the functional form holds for a model-selected population. Additional probability constraints can only reduce the box maximum, so it remains conservative for that restricted model.

**Counterexample to pooling offsets:** two equally weighted entries with identical payout factors of 2 and centered regressors, backing opposite sides at different offsets, cancel in every pooled exposure sum. Yet allowed home-side intercepts of +0.004 at the first offset and −0.004 at the second give both selected entries +0.008 CLV. The grouped intercept contribution to the bound is 0.010 at the default 0.005 margin; other coefficient contributions may enlarge it. Grouping prevents this false zero bound. Cancellation remains legitimate among entries that share one fit and its coefficients.

A positive claim requires the lower bound of its CLV interval to exceed `B`: an edge inside the bound cannot be distinguished from drift allowed by this sensitivity model. `B` covers only drift the three-term model describes; any binned-diagnostic flag accepted by decision record must state its own additional bound. Arithmetic fixtures check the counterexample, agreement with exhaustive coefficient-box corners, and consistency with event weights. Production implementation must additionally test unsupported timing/price support, fit lineage and weight validation.

## 7. Open questions

1. What is the frozen reference benchmark, inclusion population, event/time weighting and chronological holdout split?
2. What independent-event sample size gives adequate precision to detect the economically meaningful mean CLV after event clustering?
3. How do missing closes correlate with liquidity, sport and opportunities the future model would select?
4. Which fee/limit and latency assumptions are defensible for the proposed execution venue? Which outcomes can be corroborated from real fills rather than hypothetical prices?
5. How will multiple model versions and candidate thresholds be tracked without reusing an already-inspected holdout?

## 8. Tests this protocol adds

`test_cohort_statistics.py`, plus tests for any `analysis_cohort` and `real_fill` invariants. The existing `test_research_bound_specification.py` remains the arithmetic reference: register the production bound in its `BOUND_IMPLEMENTATIONS` so every fixture runs against it.
