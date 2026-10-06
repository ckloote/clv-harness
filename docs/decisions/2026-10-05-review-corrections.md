# Specification and control corrections

Date: 2026-10-05

Status: accepted for the R0/V0 implementation baseline

The repository review identified six corrections plus vendor-document discrepancies. This record explains the choices; DESIGN.md and RESEARCH_PROTOCOL.md contain the current specifications. No real-data calibration, access verification or model evaluation is established by this pass.

## Evidence collection

REST bodies do not always identify their market. R0 will preserve a sanitized request envelope and explicit request/response/failure association, including timing and subject IDs. The archive contract also preserves WebSocket control-frame observations and authoritative public-channel probe replies. Transport Pings alone cannot demonstrate a quiet subject's liveness. Snapshot probes can confirm an unchanged sequence or expose missing deltas; they cannot retroactively establish continuity across a gap. The initial probe/timeout/channel/transport limits are in DESIGN.md §10, to be revised from the R0/A1 pilot. Run manifests reference sealed archive segments so ongoing recording cannot alter a prior run's evidence.

## Depth benchmark

The target is equal USD **payout notional** on both canonical sides, with probability averaged by consumed payout and a proportional final level. This makes the benchmark invariant under complement normalization and separates it from cash-budget execution estimates. Native quantities retain their instrument payout multiplier and quantity increment. DESIGN.md §2.4 supplies the V0 fixture, including Novig's documented $0.01 payout conversion. Changing the notional basis later requires a new close definition.

## Statistical controls

Check 1 now takes the required offsets independently of available data. Missing offsets or insufficient observations yield `InsufficientCalibrationData`, which future orchestration records as `insufficient_data`; empty input cannot pass. Unexpected offsets and invalid specifications remain errors.

The follow-up review separates `InsufficientCalibrationData` from `ValueError`, preserves length-mismatch errors even when one input is empty, and treats too little price variation as insufficient evidence. The minimum cluster count remains a documented code constant. Tied prices share one diagnostic bin regardless of input row order; sparse or empty bins cannot produce a passing diagnostic. The bound fixtures also expose an implementation registry so the future production function can run against the same arithmetic cases.

The binned diagnostic uses the full residualized contrast and event scores across all observations, accounting for the fitted curve and events spanning bins. This replaces within-bin-only residual regressions. A fixed-design coverage simulation with correlated event errors now checks the claimed interval level. No equivalence margin or bin trigger was changed.

Random-side EV residuals remain nongating. Side randomization cancels an unweighted probability move, but EV multiplies by different payouts. Both symmetric-price cancellation and asymmetric-price noncancellation are covered by regression fixtures; the documentation no longer claims general blindness to drift.

## Future model claims

The drift sensitivity bound takes absolute values within each independently calibrated fit/offset before summing, using the headline estimator's weights. Pooling offsets can produce a false zero bound when opposite sides at different offsets benefit from opposite signed drift. Nearest-offset assignment has been removed; unsupported timing/price support needs its own validated policy before a claim. Pure arithmetic fixtures compare the formula against exhaustive coefficient-box corners and event-weighted examples. Production cohort logic remains deferred.

## Vendor facts and implementation gates

The capability log records the corrected Novig `trades` wire-channel name, per-market/per-channel sequencing, the new-market snapshot exception, 15-second control Pings and monetary units, with links to vendor documentation. These are documentation checks, not live verification. R0 still verifies access, actual subscription/probe behavior and heartbeat observations.

Validation after follow-up review: **41 tests passed**, using Python 3.12.14, NumPy 2.3.5 and pytest 9.0.2. At 1,500 events, Check 1 accepted the correct pipeline in 99.3% of 1,000 replicates and rejected boundary-level drift/curvature in 100.0%/99.8%. The localized-band diagnostic flagged 99.0% of 200 samples; no null sample was flagged in its 200-replicate check. The new nominal 90% interval coverage experiment returned 90.8%, 89.5%, 89.7%, 89.2% and 89.4% across its five bins over 1,000 replicates. These are synthetic operating characteristics, not real-data qualification. R0 archive fixtures and V0 depth fixtures become executable when those components are built. The next implementation milestones remain R0/P0 and the audited one-game V0 slice.
