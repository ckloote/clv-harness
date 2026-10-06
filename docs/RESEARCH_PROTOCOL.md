# Research Protocol — Model Evaluation

**Status:** inactive. Activates when a signal model exists.  
**Relationship to the harness:** the harness implements nothing in this document. It preserves the evidence this protocol needs (`DESIGN.md` §11.4). When this protocol activates, its tables and tests are added as a new schema stage (`DESIGN.md` §8.5).

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

**Drift-bias bound.** Harness check 1 certifies that reference drift lies within its margins, not that it is zero, and the same probability-point drift costs more CLV at longshot payouts. Before any claim, compute the largest mean CLV that drift inside the certified margins could produce over the model's own entries. For each entry `i`, using the calibration offset nearest its entry time:

```text
s_i = +1 if the entry backs the fixed canonical side (home), -1 otherwise
p_i = reference probability of the canonical side at entry
x_i = (p_i - 0.5) - x_center
q_i = ((p_i - 0.5) / r)^2 - q_center      # centering constants recorded by that calibration run
d_i = entry payout factor

B = ( m_a * |sum s_i*d_i| + m_b * |sum s_i*d_i*x_i| + m_g * |sum s_i*d_i*q_i| ) / N
```

`m_a`, `m_b` and `m_g` are the check 1 margins for intercept, slope and curvature, and `r` is the curvature radius (`DESIGN.md` §10). Drift moves the backed side's probability by `s_i * (α + β*x_i + γ*q_i)`, so `B` is the exact maximum of drift's contribution to mean CLV anywhere inside the certified margins. A positive claim requires the lower bound of its CLV interval to exceed `B`: an edge inside the bound cannot be distinguished from reference drift. `B` covers only drift the three-term model describes; any binned-diagnostic flag accepted by decision record must state its own bound.

## 7. Open questions

1. What is the frozen reference benchmark, inclusion population, event/time weighting and chronological holdout split?
2. What independent-event sample size gives adequate precision to detect the economically meaningful mean CLV after event clustering?
3. How do missing closes correlate with liquidity, sport and opportunities the future model would select?
4. Which fee/limit and latency assumptions are defensible for the proposed execution venue? Which outcomes can be corroborated from real fills rather than hypothetical prices?
5. How will multiple model versions and candidate thresholds be tracked without reusing an already-inspected holdout?

## 8. Tests this protocol adds

`test_cohort_statistics.py`, plus tests for any `analysis_cohort` and `real_fill` invariants.
