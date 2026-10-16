# B2: the prespecified calibration of the historical close families

Date: 2026-10-16

Status: accepted. Committed before any statistic was computed on real data. Results are appended below once the runs exist; nothing above Results changes after that.

## Context

B2 builds the historical close families and runs DESIGN.md §9.2 check 1 and the §9.3 oracle sweep on them (§13). Gate B2: all three check-1 components pass at the §10 margins at every offset, and every binned-diagnostic flag has a decision record, or the affected family is abandoned. Also required: a reproducible coverage table, no candle straddling the actual off, and no side-duplicated trades.

The code that runs is `main` at `80dc642` (PRs #20–#22):
- calibration `calibration-v0.1`;
- historical closes `close-hist-v0.1`;
- off resolver `off-v0.1`;
- `src/clv/score/controls.py` sha256 `a19b578b7501c58c51b59ca5b3aa716bb8ce8f1ce4e644e84edc8b27716ffe5b`;
- `src/clv/score/simulation.py` sha256 `003a6e26f45dbdf6824c4073299369151b035bad0d9d0012a8ecf657c63594b9`.

Every run records these hashes in its `spec_json`.

**What was seen before this record:** coverage counts only. That means the B1 coverage table, and a run of the pipeline on a copy of the database with the statistics step removed (PR #22). No dispersion, check-1 or oracle statistic had been computed on real data.

## Decision

### Families

| Family | Venue | Role | Home-outcome games at 24 h / 6 h / 1 h / 15 min |
|---|---|---|---|
| `close_hist_candle_mid_top` | Kalshi | Carries Gate B2 | 2,145 / 3,824 / 4,086 / 4,100 |
| `close_hist_vwap` | Kalshi | Second Kalshi check, expected `insufficient_data` | 126 / 1,419 / 2,440 / 2,775 |
| `close_hist_vwap` | Novig | Separate family, expected `insufficient_data` (Track B verdict) | 9 / 284 / 554 / 632 |

**Every family uses the same offsets.** A family short of the sample target at any offset is recorded as `insufficient_data`, never passed. Its statistics are reported as diagnostics. No offset is dropped to reach a verdict (DESIGN.md §9.2), and the VWAP families get no offsets of their own (decided 2026-10-16).

### The closes (DESIGN.md §5.6; `clv.close.candles`, `clv.close.vwap`)

- **Candle:**
  - **The price:** the last 1-minute candle whose whole interval ends at or before the as-of time, priced at the midpoint of its YES bid close and ask close.
  - **A candle that contains the as-of time is never used.** The earlier candle is the close, as of its own end. Its age is recorded, and `cutoff_candle_active` stratifies it.
  - **Unscoreable cases:**
    - `ambiguous_candle`: only a candle containing the as-of time is near, or the two Kalshi tiers disagree on the latest minute;
    - `stale_book`: older than `close.max_quote_age_s` = 1,800 s;
    - `no_quotes`: no candle, or the latest one is one-sided.
- **VWAP:**
  - **The price:** unique executions in `[as_of − 600 s, as_of)`, counted once each. An execution is timed by its stream sighting where one exists.
  - **`few_trades`:** below 5 executions or $250 of payout (`close.hist_vwap_*`, unchanged).
  - **Unmatched stream trades:** Novig's three R0 stream trades that matched no MAKER row stay executions, as B1 stores them.
- **Buffers:** the close is at `close.buffer_s` = 60 s before the earliest plausible start. The oracle sweeps `close.buffer_sweep_s` = 0, 30, 60, 120 and 600 s. A historical close's cutoff may equal the start (the 0 s buffer), since candles and windows exclude their end (decided in PR #22).

### Off resolution

`off-v0.1`, with `off.max_source_disagreement_s` kept at 120 s (decided 2026-10-15).
- **Excluded:** 395 games as `off_disagreement`. The span of their start claims is 120–300 s for 386 of them, 300–600 s for 6, 600–1,800 s for 2, and over 1,800 s for 1.
- **18 games** are `no_trusted_off`.
- **The rule is not revised to gain coverage.** The distribution is reported for A2.

### Check-1 population

Per offset, games that have:
- a trusted off;
- a home outcome whose reference is the Kalshi home team's market (YES) or Novig's home outcome;
- a mapping that is primary-cohort eligible apart from its pending settlement-equivalence verdict;
- a price at both the offset and the close.

Settlement equivalence is between an entry venue and the reference, and check 1 has no entry. Every other mapping exclusion keeps the game out under its own reason (`mapping_unverified`, `mapping_conflict`, `mapping_rejected`, `rules_mismatch`). All 19,402 approved links are eligible apart from the pending equivalence; 578 mappings remain unverified.

### Check 1 (DESIGN.md §9.2, §10)

| Item | Value |
|---|---|
| Offsets | 24 h, 6 h, 1 h, 15 min before the earliest plausible start (`controls.martingale_offsets`) |
| Fit | `Δp = α + βx + γq` on the fixed home side, event-clustered (CR1), centered terms, curvature radius 20 pp |
| Pass | Every 90% interval inside its margin at every offset: α ±0.5 pp, β ±0.05, γ ±1.6 pp |
| Sample target | 1,500 events at every offset (`controls.sample_events`); below it, `insufficient_data` |
| Binned diagnostic | 5 equal-count bins per offset; a bin is flagged when its 90% interval lies wholly outside ±0.5 pp |

**A flagged bin** does not change the verdict. Each flag needs its own decision record before sign-off, and a structural pattern means either a new prespecified gate or abandoning the family.

**A failure** abandons that family as a benchmark. Genuine predictable drift is a finding about the reference, never an offset to subtract.

### Oracle sweep (DESIGN.md §9.3)

- **Entries:** for each game with a trusted off, take the outcome StatsAPI settles as the sole winner. The family's venue must also settle that outcome's reference side `win`.
- **Exclusions:** fair-value settlements, disagreements and unsettled markets are excluded and counted.
- **The value:** at each oracle offset (the same four) and each buffer, `oracle_clv = p_close / p_offset − 1` on the winner's own reference.
- **Reported:** the mean with an event-clustered 90% interval, over all such games and over the matched games priced at every buffer.
- **How it is read:** for its shape across buffers. A sharp rise at the smallest buffers raises suspicion; no kink is not proof of safety. It is never a strategy result. The buffer stays `close.buffer_s` until G2.

### Order of work

1. **Commit this record.**
2. **Dispersion run** (`clv calibrate --dispersion`): the spread of Δp at each offset and the entry-price quantiles, with nothing fitted. Then §9.4 item 12 rerun with 1,000 replicates, seed 20261015, under the measured steps, start prices and per-offset counts. The simulation fits offsets as missing at random and independently, which is conservative for the joint pass.
3. **Stop rule:** if, for Kalshi candle, a correct pipeline passes less than 95% of the time, or a level or curvature drift at its margin fails less than 90% of the time, stop. Margins or the sample target are then decided in a new decision record, still without any fit. The VWAP families' rates are reported for information.
4. **Check run** (`clv calibrate`): check 1, the bins and the oracle sweep for all three families.
5. **Reproducibility:** a rebuild from a clean database must give the same coverage table and the same calibration results. The steps are `clv migrate`, `clv backfill import`, `clv backfill link`, `clv correct` with `links.approved.toml` (sha256 `4b778be7…`), `clv backfill import`, `clv backfill coverage` and `clv calibrate`.

### Publishing

Committed docs carry aggregate statistics: check-1 estimates and 90% intervals, bin flags, oracle means per buffer, the measured spreads, and counts. Per-game prices stay in the git-ignored `data/b2/` (decided 2026-10-15).

## Consequences

- **Kalshi candle decides Gate B2.** If it passes and its flags are resolved, B3 may be considered for the windows it covers. If it fails, the historical close family is abandoned, and B3 is not bought on it.
- **The VWAP families' results are diagnostics** unless a later record prespecifies a definition they can meet.

## Results

**Runs, 2026-10-16, on `data/clv.sqlite`.** A copy taken before the runs is `data/clv.pre-b2.sqlite`. Migration 0005 and the settlement import came first: 9,856 StatsAPI, 9,368 Kalshi and 1,540 Novig settlements. Then the dispersion run, then the check run. The full aggregates are in `docs/calibration-report.md`.

**Dispersion and the stop rule (Kalshi candle).**
- **Measured sd of Δp:** 2.76, 1.68, 0.87 and 0.58 pp at 24 h, 6 h, 1 h and 15 min.
- **Item 12 under that series:** a correct pipeline passes 100.0%; a level drift at the margin fails 99.7%, and a curvature drift 100.0%.

The stop rule was met, and the check run went ahead as specified.

**Gate B2: not met.**

| Family | Verdict |
|---|---|
| Kalshi candle | **failed**: at 24 h the intercept is +0.443 pp [+0.345, +0.541] against ±0.5 pp. Every other component at every offset is inside its margin, and no bin is flagged |
| Kalshi VWAP | insufficient_data: 126 games at 24 h and 1,419 at 6 h. Check 1 alone would also fail it at 24 h (+0.616 pp [+0.269, +0.963]) |
| Novig VWAP | insufficient_data: 9 games at 24 h |

- **Under this definition, the Kalshi candle family is abandoned as a historical benchmark** (Decision, Check 1).
- **The oracle sweep shows no in-play contamination.** At every offset the 0 s and 30 s buffers are at or below the 60 s close.
- **Every audit the gate requires is 0:** no candle straddles the off, and no trade is counted from both sides.

**Post hoc** (read-only, after the verdict; it cannot change it). No pipeline cause was found.
- **Book quality:** at 24 h there is no one-sided book, the median spread is 1 cent, and the median quote is 1.2 minutes old.
- **The drift is steady:** it holds across months and in the tightest books.
- **It splits in two:**
  - a rise of both teams' YES midpoints, +0.145 pp [+0.129, +0.160];
  - the home team firming relative to the away team, +0.308 pp [+0.211, +0.406].

  Both fade by 6 h (+0.014 and +0.091 pp) and are absent at 1 h and 15 min.
- **Kalshi VWAP and Novig lean the same way at 6 h.**
- **Not testable:** whether early-life market prices cause it, because B1 fetched only the 32 h before each market's `close_time`.

**Reading:** genuine predictable drift in the reference at the 24 h horizon, which is a finding about Kalshi, not an offset to subtract (DESIGN.md §9.2). The machinery did its job: the audits, polarity and close selection hold, and check 1 caught a real drift.

**Reproducibility.** A clean rebuild in `data/rebuild/` reproduced everything exactly. It ran every step from an empty database: `clv migrate`, `backfill import`, `backfill link`, `clv correct` with `links.approved.toml` (sha256 checked), `backfill import`, `backfill coverage`, and both calibration runs, in 36 minutes.
- **The coverage table:** byte-identical to the current database's and to B1's full-run table (4,946 games). This is the first full-scale rebuild B1 left open.
- **All six calibration runs:** identical verdicts and statistics (every estimate and interval), offset prices and closes, compared game by game and team by team.

## Consequences of the results

- **B3 is not justified on this definition.** The Odds API history is bought only for windows that passed B2 (DESIGN.md §13).
- **Kalshi is not a benchmark for early entries.** At 24 h, a Kalshi midpoint close would lend home entries about +0.45 pp of drift in probability (about +0.9 pp of EV at even odds) and away entries the reverse. At 1 h and 15 min the midpoints are martingales to within ±0.02 pp.
- **Any later historical calibration is a new definition, never a revision of this verdict.** A definition without the 24 h offset is legitimate only if it rests on something independent of this result, such as a decision that model entries are taken within a few hours of the start. It must say it was chosen after this failure.
- **Track A's live Novig reference remains the real benchmark.** Its own check 1 should look hard at the long offsets, since Novig leans the same direction at 6 h here.
