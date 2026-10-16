# Track B closed: B3 cancelled, the 24 h drift carried to Track A

Date: 2026-10-16

Status: accepted. It changes no verdict. The B2 results stand as recorded in `2026-10-16-b2-prespecification.md`.

## Context

Gate B2 was not met (`2026-10-16-b2-prespecification.md`, Results).
- **Kalshi candle**, the family that carried the gate, failed check 1 at 24 h: an intercept of +0.443 pp [+0.345, +0.541] against ±0.5 pp. It passed at 6 h, 1 h and 15 min, with no bin flagged.
- **Kalshi VWAP and Novig VWAP** were `insufficient_data`.

DESIGN.md §13 buys B3 only for windows that passed B1 and B2, and none did. Gate B needs Gate B2, so it cannot be met on any historical family under the current definitions.

This record says what Track B leaves behind, what moves to Track A, and when Track B could be reopened.

## Decision

### Track B stops here

- **B3 is cancelled.** No Odds API history is bought. It would also have been the source for check 2's historical entries, so Track B never runs check 2.
- **Gate B is not met.** Track B did not produce a validated historical benchmark, and it is not a precondition of any Track A gate.
- **No more historical calibration runs.** The B2 runs, their fact snapshot and `data/clv.pre-b2.sqlite` are kept as they are, git-ignored, as the evidence behind the verdict.

### What Track B did establish

These carry forward as working, tested machinery. They do not make a benchmark.
- **Import and linking at full-season scale.** That covers 4,946 games, 19,402 approved links checked against evidence the link rules do not use, and execution deduplication across tiers and sources (B1).
- **Reproducibility from a clean database.** It held for the coverage table and all six calibration runs, game by game (B2, Reproducibility).
- **The close machinery's own audits.** No candle straddles the off, and no trade is counted from both sides.
- **Check 1 on real data.** The item-12 stop rule ran under measured dispersion, and the check caught a drift its fixtures said it would catch. The oracle sweep showed no in-play contamination at 0 s or 30 s.

### What moves to Track A

1. **The 24 h drift is a hypothesis for the live reference, not a known property of it.** On Kalshi, at 24 h, home teams firmed by about +0.3 pp relative to away teams, and both YES midpoints rose about +0.15 pp. Both had faded by 6 h. Kalshi VWAP and Novig leaned the same way at 6 h.
   - **Track A's check 1 keeps the 24 h offset.** Its prespecification is written before any live statistic is computed, and it reports the same two-part split as a diagnostic.
   - **The finding does not lower the bar.** A failure on the live Novig family abandons that family, exactly as here.
2. **Check 2's first real-data run** becomes Track A's (A4 polled entries, then G1). Until then, V0's golden game is check 2's only real exercise.
3. **The 395 `off_disagreement` games go to A2.** Nearly all of them have start claims 120–300 s apart. A2 measures how StatsAPI "In Progress" relates to first pitch on these games and on R0's. `off.max_source_disagreement_s` changes only on that report, never to gain coverage.
4. **Kalshi stays the live cross-check** (DESIGN.md §3.1), but it is not a benchmark for entries taken 24 h out (B2, Consequences of the results). Any Kalshi close used against early entries needs its own live check 1 at that offset.

### Reopening Track B

A historical family can be calibrated again only under a **new definition**, never as a revision of this verdict. That needs one of these:
- **An independent reason for its offsets**, recorded before any statistic. An example is an entry-timing policy in `docs/RESEARCH_PROTOCOL.md` that rules out entries more than a few hours before the start. The definition must say it was chosen after this failure.
- **A new source or close function**, prespecified in full as B2 was.

The cost case for B3 is then made again from scratch.

## Consequences

- **Track A is the critical path.** DESIGN.md §13's calendar slot for B3 (December–March) is freed, and A1 starts now (`2026-10-16-a1-scope.md`).
- **The historical data stays as evidence.** The B1 archive (`archive-hist/`) and database stay git-ignored and backed up. Nothing derived from them is committed beyond the aggregates already recorded.
- **Follow-ups, made on acceptance:**
  - the README status line;
  - DESIGN.md §13: Track B's status and the calendar;
  - `docs/feasibility.md` §6 item 2, whose Odds API recheck was deferred to B3.
