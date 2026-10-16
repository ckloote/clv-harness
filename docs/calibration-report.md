# Calibration report

The fixture and real-data results of the harness's calibration gates (DESIGN.md §9, §13). Aggregate statistics only: per-game prices stay in git-ignored `data/` (B2 prespecification, Publishing).

## Fixtures: what the checks can detect

From `tests/test_calibration_simulation.py`, on the planning assumptions (DESIGN.md §10), 1,000 replicates at 1,500 events:

| Check | Rate |
|---|---:|
| Check 1 passes a correct pipeline | 99.3% |
| Check 1 fails a level drift equal to the 0.5 pp intercept margin | 100.0% |
| Check 1 fails a symmetric quadratic drift equal to the 1.6 pp curvature margin | 99.8% |
| Check 1 passes a +2.5 pp drift confined to prices 0.56–0.60 (200 replicates) | 74.5% |
| The binned diagnostic flags that localized drift | 99.0% |
| The binned diagnostic flags a correct pipeline | 0.0% |

`tests/test_controls_and_fault_injection.py` runs the whole calibration pipeline on a synthetic 200-game database:
- **A correct pipeline:** passes check 1, with no bins flagged.
- **A close-leg polarity flip:** fails with a slope near −2.
- **A 2 pp level drift:** fails through the intercept.
- **Below the sample target:** `insufficient_data`, never `passed`.
- **Conflicted mappings:** never enter.
- **A rerun over the same facts:** reproduces every result.

## B2: the historical close families (2026-10-16)

Prespecified in `docs/decisions/2026-10-16-b2-prespecification.md` before any statistic was computed. Fact snapshot of `data/clv.sqlite`: both seasons, 4,946 games.

### Verdicts

| Family | Verdict | Why |
|---|---|---|
| Kalshi candle (`close_hist_candle_mid_top`) | **failed** | The 24 h intercept's interval leaves the ±0.5 pp margin |
| Kalshi VWAP (`close_hist_vwap`) | insufficient_data | 126 games at 24 h and 1,419 at 6 h, below 1,500; check 1 alone would also fail it at 24 h |
| Novig VWAP (`close_hist_vwap`) | insufficient_data | 9 games at 24 h, below `MIN_CLUSTERS` |

**Gate B2 is not met.** Kalshi candle, the family that carried it, is abandoned as a historical benchmark under this definition.

### Coverage: home-outcome games priced at the close (60 s buffer)

| | Kalshi candle | Kalshi VWAP | Novig VWAP |
|---|---:|---:|---:|
| Priced, in the population | 4,101 | 3,284 | 654 |
| Priced, mapping unverified | 134 | 93 | 5 |
| `off_disagreement` | 395 | 395 | 395 |
| `no_trusted_off` | 18 | 18 | 18 |
| Unmapped | 291 | 291 | 3,851 |
| `few_trades` / `stale_book` / `no_quotes` / `ambiguous_candle` | 0 / 5 / 1 / 1 | 865 / 0 / 0 / 0 | 23 / 0 / 0 / 0 |

For 3,975 of the 4,101 Kalshi candle closes, a candle contained the cutoff. The close is then the earlier candle's, as of its own end.

### Measured spread of Δp and §9.4 item 12 (dispersion run; nothing fitted)

| Kalshi candle | 24 h | 6 h | 1 h | 15 min |
|---|---:|---:|---:|---:|
| Events | 2,145 | 3,824 | 4,086 | 4,100 |
| sd of Δp | 2.76 pp | 1.68 pp | 0.87 pp | 0.58 pp |

**Item 12 under that series** (1,000 replicates, seed 20261015):
- a correct pipeline passes **100.0%**;
- a level drift at the margin fails **99.7%**;
- a curvature drift at the margin fails **100.0%**.

The stop rule was met.

For Kalshi VWAP, a correct pipeline would pass only 2.1% under its measured series, because 126 events at 24 h leave its intervals too wide. Novig VWAP had too few events to simulate.

### Check 1: Kalshi candle

Pp for the intercept and curvature; 90% event-clustered intervals; margins ±0.50 pp, ±0.05 and ±1.60 pp.

| Offset | Events | Intercept | Slope | Curvature |
|---|---:|---|---|---|
| 24 h | 2,145 | **+0.443 [+0.345, +0.541] failed** | −0.0220 [−0.0358, −0.0081] | +0.201 [−0.222, +0.624] |
| 6 h | 3,824 | +0.109 [+0.065, +0.154] | −0.0065 [−0.0121, −0.0010] | −0.022 [−0.193, +0.149] |
| 1 h | 4,086 | −0.008 [−0.030, +0.015] | −0.0003 [−0.0070, +0.0064] | +0.107 [−0.119, +0.332] |
| 15 min | 4,100 | −0.008 [−0.023, +0.007] | +0.0025 [+0.0007, +0.0043] | −0.025 [−0.078, +0.029] |

The binned diagnostic flagged 0 of 20 bins.

### Check 1: the VWAP families (diagnostics; below the sample target)

| Offset | Kalshi VWAP intercept (events) | Novig VWAP intercept (events) |
|---|---|---|
| 24 h | +0.616 [+0.269, +0.963] (126) | — (9) |
| 6 h | +0.161 [+0.092, +0.230] (1,419) | +0.199 [+0.053, +0.345] (284) |
| 1 h | +0.027 [+0.003, +0.051] (2,440) | +0.027 [−0.016, +0.069] (554) |
| 15 min | +0.010 [−0.005, +0.025] (2,775) | +0.007 [−0.020, +0.034] (632) |

Neither family flagged a bin.

### Oracle sweep: Kalshi candle (§9.3)

Mean oracle CLV in pp, over matched games. A calibration construct: selecting the winner makes the level positive by construction. Only the shape across buffers is read.

| Offset | 0 s | 30 s | 60 s | 120 s | 600 s | Games |
|---|---:|---:|---:|---:|---:|---:|
| 24 h | +0.743 | +0.752 | +0.758 | +0.757 | +0.718 | 2,158 |
| 6 h | +0.149 | +0.149 | +0.160 | +0.182 | +0.149 | 3,825 |
| 1 h | +0.024 | +0.020 | +0.037 | +0.058 | +0.033 | 4,084 |
| 15 min | +0.000 | −0.003 | +0.013 | +0.034 | +0.008 | 4,098 |

There is no rise at the smallest buffers. At every offset the 0 s and 30 s values are at or below the 60 s close, so the sweep shows no sign of in-play contamination. The 90% intervals' half-widths run from about 0.02 pp (15 min) to 0.2 pp (24 h); they are in `data/b2/`.

Excluded from the oracle: 395 `off_disagreement`, 291 unmapped and 18 `no_trusted_off`. No game was excluded for a settlement disagreement or a fair-value settlement.

### Audits (Gate B2)

| Check | Count |
|---|---:|
| Selected candles ending after the earliest plausible start | 0 |
| Offset prices from a candle ending after their as-of time | 0 |
| Executions with no record but a TAKER row | 0 |
| Executions with two MAKER rows from one file | 0 |
| Novig stream trades no MAKER row matched, counted as executions | 3 |

### Off disagreement (diagnostic for A2; the rule is unchanged)

The 395 `off_disagreement` games, by the span of their accepted start claims: 386 at 120–300 s, 6 at 300–600 s, 2 at 600–1,800 s, and 1 over 1,800 s.

### Post hoc: the 24 h failure

These diagnostics were read-only and run after the verdict, so they cannot change it. They looked for a pipeline cause and found none.

- **Book quality:** no one-sided book at 24 h, a median spread of 1 cent and a median quote age of 1.2 minutes. The drift is +0.35 pp in the tightest-spread games and +0.45 pp in quotes under 5 minutes old. Every 2026 month shows +0.26 to +0.59 pp.
- **The two team markets.** Over the 2,124 games with both Kalshi team markets priced, the drift splits in two (90% intervals):

| At 24 h | Estimate |
|---|---|
| Home market's move | +0.453 pp [+0.355, +0.551] |
| Both YES midpoints rising together | +0.145 pp [+0.129, +0.160] |
| Home rising relative to away | +0.308 pp [+0.211, +0.406] |

  The common rise is +0.014 pp at 6 h, and the relative firming +0.091 pp. Both are within ±0.02 pp of zero at 1 h and 15 min.
- **The relative firming is broad:** home underdogs +0.59, near-even +0.59 and home favourites +0.24 pp. By start time it is +0.60 before 16:00 ET, +0.37 at 16:00–19:00 and +0.43 at 19:00 and later.
- **The bid moves more than the ask** (+0.59 against +0.32 pp), with the median spread unchanged at 1 cent.
- **Cross-venue:** the direction is the same on Kalshi VWAP and on Novig at 6 h.
- **Not testable:** whether a market's early-life prices cause it. B1 fetched only the 32 h before each market's `close_time`, so the data begin about 28 h before the start.

**Reading:** genuine predictable drift in Kalshi's midpoints at the 24 h horizon. Part of it is a structural rise of both YES midpoints, and part a home team's firming relative to the away team. It fades by 6 h and is absent at 1 h and 15 min. Under DESIGN.md §9.2 it is a finding about the reference, never an offset to subtract.
