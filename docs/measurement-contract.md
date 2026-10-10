# Measurement Contract — v1 MLB Moneyline

**Status:** draft for sign-off (P0 item 1, DESIGN.md §13). Not frozen.  
**Date:** 2026-10-08

DESIGN.md §2 and §4–§6 define the measurement machinery. This document fixes the v1 choices that machinery needs:
- which contract is being measured
- which venue instruments count as that contract
- which events and entries form the primary analysis population
- the vocabulary every table, report and dashboard uses

Thresholds stay in DESIGN.md §10 and are not repeated here.

**After sign-off,** changes go through a dated record in `docs/decisions/`. A change that alters what a reported number means creates a new mapping, close or population version (DESIGN.md §10). It never edits an old one.

---

## 1. Scope

| In v1 | Not in v1 |
|---|---|
| MLB full-game moneyline: two outcomes, "team X wins this game" | Run lines, totals, first-five-innings and other partial-game markets, props |
| Regular-season and postseason games | Spring training, exhibitions, the All-Star Game (different rules; ties possible) |
| Pregame entries: the quote is observed before the close cutoff | In-play entries |
| Live book closes (`close_live_*`) and historical closes (`close_hist_*`), always reported separately | A pooled or blended close |

Other leagues join only through their own version of this document. Track A's live development leagues (DESIGN.md §13) use this contract's vocabulary and population rules, plus a league-specific §2–§3.

## 2. The canonical contract

**Canonical event:** one MLB game, identified by its StatsAPI `gamePk`, with the event-identity rules of DESIGN.md §4.1. Doubleheader games have distinct `gamePk` values. The date and teams alone never identify a game.

**Canonical outcome:** `P(team X wins game gamePk)`, with the winner taken from MLB's official final result. Each game has two canonical outcomes, one per team, and they are exact complements: MLB regular-season and postseason games cannot end tied.

**Settlement states.** Venues agree on the ordinary case and differ on the rare ones. Every event is assigned exactly one state from StatsAPI status history after the game:

| State | What happened | Licensed books | Kalshi `KXMLBGAME` | Novig `MONEY` |
|---|---|---|---|---|
| **S1** | Started on its scheduled date and completed, including same-day delays and called games already official (5 innings, or 4½ with the home team leading) | Winner | Winner | Winner (assumed; unverified) |
| **S2** | Suspended before becoming official, resumed and completed within 36 h | Winner | Winner (within 2 days) | Unverified |
| **S3** | Suspended, resumed 36–48 h later | Void at DraftKings, FanDuel and BetMGM; winner at Fanatics | Winner | Unverified |
| **S4** | Suspended and not completed within 48 h | Void | "Fair price" after 2 days | `FMV` (assumed from `voids`) |
| **S5** | Postponed before first pitch, played within 2 days | Void | Winner of the rescheduled game (observed: 824785) | Winner of the rescheduled game (observed: 824785) |
| **S6** | Postponed before first pitch and cancelled, or rescheduled more than 2 days out | Void | "Fair price" (observed: 823490 settled 0.47 / 0.53) | `FMV` (observed: 823490 settled 0.478 / 0.522) |

The sources and statuses behind this table are in `docs/vendor-capabilities.md`:
- Kalshi: its archived rule text.
- Novig: the `voids: FMV` catalog field, and the S5 and S6 settlements observed in B0 (`docs/feasibility.md` §3). Its MLB rules text is unverified, so S1–S4 are still assumed.
- Licensed books: a secondary source. Each book's own house rules still have to be checked.

## 3. Settlement equivalence

Each mapping (DESIGN.md §4.2) carries one of these `settlement_equivalence` verdicts:

| Verdict | Meaning | Primary cohort |
|---|---|---|
| `identical` | Same payoff in every state S1–S6 | Yes |
| `equivalent_when_played` | Same winner-takes-all payoff in S1 and S2; the documented differences are confined to S3–S6 | Yes, with the §5.4 exclusions |
| `pending` | One side's rules are not yet verified | No: reason `equivalence_pending` |
| `not_equivalent` | The payoffs differ in S1 or S2 | No: reason `rules_mismatch` |

**Why `equivalent_when_played` is acceptable.**
- **A postponement is known before the close.** S5 and S6 games have no first pitch on the scheduled date, so they never have a trusted off for that date, and licensed-book entries on them void anyway. Their exclusion is decided by pre-off facts.
- **S3 and S4 are decided after the off,** by weather and scheduling. That does not depend on which side an entry took. Excluding those events removes exactly the cases where the entry and the reference paid differently.
- **The ex-ante cost is bounded.** Suppose, at the close, a game has probability `π` of ending in S3–S6. Then the reference's price and the price of a contract that pays only in S1–S2 differ by at most `π` times the gap between the divergent-state payoff and the played-game price. With fair-price settlement that gap is small, and `π` is expected to be a small fraction of a percent for a typical game.
- **The bound is not measured.** Every season the waterfall therefore reports S3–S6 counts, plus a prespecified sensitivity (§5.6) that drops every event with a weather delay or suspension.

**Current verdicts** (2026-10-08; nothing is primary-eligible yet):

| Entry instrument | Reference instrument | Verdict | What confirms or changes it |
|---|---|---|---|
| Licensed-book `h2h` | Novig `MONEY` | `pending` | Novig's MLB rules text (§7, item 1) and each book's house rules (§7, item 2) |
| Licensed-book `h2h` | Kalshi `KXMLBGAME` YES | `pending` | Each book's house rules (§7, item 2). Kalshi's text already supports `equivalent_when_played` |
| Kalshi `KXMLBGAME` YES | Novig `MONEY` | `pending` | Novig's MLB rules text (§7, item 1) |
| Offshore-book `h2h` | Any | `pending`, quarantined | Not pursued in v1 (§5.3) |

**If Novig's rules diverge in S1 or S2,** the Novig pairs become `not_equivalent`. DESIGN.md §3.2 then applies: evaluate Kalshi as the primary reference, recorded in a decision record and a new close definition.

## 4. Instruments and polarity

The canonical side is always `P(team X wins)`, stored as an integer probability ×10,000 with the native string kept (DESIGN.md §8.3). Polarity follows DESIGN.md §4.3.

| Venue | Instrument | Canonical mapping | Native price → `d_entry` before fees |
|---|---|---|---|
| Novig | One `MONEY` market per game, two outcomes keyed by `outcomeId` and named by team abbreviation | Match the outcome by name to the team, then confirm it by hand against the `gamePk` (start time and teams). Never use array position | `1 / p` for a three-decimal price `p`; each contract pays $0.01 |
| Kalshi | One YES/NO market per team per game (`KXMLBGAME-<date><time><teams>-<TEAM>`) | YES on team X's market is the canonical side. NO on X's market is its complement within one book. Team Y's market is a **separate** book, never merged into X's | `1 / q` for a YES price `q` in dollars; each contract pays $1 |
| The Odds API | `h2h` outcome per book, named by full team name | Match by name to the event's home and away teams. Map The Odds API event `id` to `gamePk` through `commence_time` and teams, then verify it | From the integer American price `A`: `1 + A/100` if `A > 0`, `1 + 100/|A|` if `A < 0`. Exact; never from a rounded decimal |

Parser fixtures from the R0 captures (V0) fix the exact Novig and Kalshi book fields for each canonical bid and ask. Paired consistency such as `p_home + p_away = 1` is checked only within one exactly complementary book.

**Fees.** Read Novig fees per market from the catalog. The 2026-10-06 sample read `charged: WHEN_LIVE`, which means no taker fee pregame. Kalshi fees follow its published formula. Licensed books' prices already include their margin. Fees enter only `fee_adjusted_close_ev`, never `clv_ev`.

## 5. Primary analysis population

This is the population the harness gates and coverage waterfall are measured on. A later model's analysis cohort (`docs/RESEARCH_PROTOCOL.md` §2) must be a subset of it, or declare each deviation before its results are viewed.

### 5.1 Events

The denominator is every scheduled MLB regular-season and postseason game in the collection period. Each event either is in the population or carries an exclusion reason. None is dropped silently.

### 5.2 Reference

- **Primary reference:** the Novig `MONEY` book. Its close follows DESIGN.md §5.3. `close.primary_benchmark` and the other benchmarks stay provisional until G2 (§10).
- **Cross-check:** Kalshi, scored as separate rows, never pooled (DESIGN.md §5.5).
- **Changing the primary reference** takes a decision record. It produces a new series, never a silent substitution.

### 5.3 Entry venues

- **Primary:** state-licensed books available to the operator. In the current The Odds API data those are DraftKings, FanDuel, BetMGM and BetRivers. Each book needs its house rules verified (§7, item 2) before it counts.
- **Excluded from primary, archive kept:** offshore books (Bovada, MyBookie.ag, BetOnline.ag, LowVig.ag, BetUS), with reason `unlicensed_book`. Their rules are unverified, and the operator could not place the entry legally.
- **Separate population, never pooled with soft-book entries:** exchange entries (Kalshi, or Novig against Kalshi). They have different fee, collateral and execution conventions.

### 5.4 Event and entry exclusions

All are prespecified, and each is counted in the waterfall:

| Reason | Applies when |
|---|---|
| `exhibition` | Not a regular-season or postseason game |
| `postponed` | S5 or S6: no first pitch on the scheduled date |
| `settlement_divergence` | S3 or S4 |
| `equivalence_pending` / `rules_mismatch` | Verdict `pending` / `not_equivalent` (§3) |
| `mapping_unverified` | The mapping is not `exact` or `manual_verified`. Every doubleheader game needs `manual_verified` |
| `mapping_rejected` / `mapping_conflict` | The current mapping is `rejected`, or two corrections of one side both stand |
| `entry_mapping_superseded` | A correction says the entry's instrument side is no longer the entry's outcome |
| `listed_pitcher_terms` | The quote is conditional on listed pitchers rather than "action" |
| `unlicensed_book` | §5.3 |
| `entry_after_cutoff` | The entry was decided at or after the close cutoff (§5.5) |
| DESIGN.md §5.4 reasons (the close is unscoreable) | `no_trusted_off`, `off_disagreement`, `collection_gap`, `feed_stalled`, `halted`, `venue_lag`, `stale_book`; for the benchmark, `no_quotes`, `insufficient_depth` (DESIGN.md §2.4) and `truncated_ladder` (the stored ladder may have cut off the depth needed); for the reference instrument, `unmapped`, `ambiguous_reference` and `mapping_inconsistent`. Crossed books never reach a close: they are quarantined at ingest |

The harness lists every applicable reason beside each score (`clv_score.exclusion_reasons`) and computes the numbers whenever its inputs exist, so the waterfall can count each reason, and a correction that clears one shows exactly what changed.

**Wide-spread and thin markets stay in** and are stratified, not excluded (DESIGN.md §5.4).

**Pitcher changes are not exclusions.** They are news, preserved for information-time analysis (DESIGN.md §11.4).

### 5.5 Entry timing

- **The decision:** an entry is decided strictly before the close cutoff, and so its `observed_quote` is observed before it (`entry_after_cutoff`).
- **The decision quote:** its `decision_quote` must be known as of the decision time under the data-access contract (DESIGN.md §6.2): received by then, whatever its provider timestamp.
- **The harness's own entries** are the control entries of DESIGN.md §9.2, taken at the offsets in `controls.oracle_offsets`.
- **Historical entries** (Track B) are labeled by snapshot time, bookmaker `last_update` and retrieval time, and are never called actionable.

### 5.6 Prespecified strata and sensitivities

Report the primary result for the whole population and for each of these:
- regular season and postseason
- entry book
- reference depth available at the primary notional
- quote age
- time before the off
- doubleheader game 1 and game 2

Also report a sensitivity that drops every event with a weather delay or suspension recorded in StatsAPI.

Events cluster by `gamePk`, with date-level sensitivity (`stats.cluster_unit`).

## 6. Vocabulary

Use these terms, with these meanings, in schema names, reports and dashboards.

| Term | Meaning |
|---|---|
| **canonical event** | One MLB game (`gamePk`) under a stable internal `event_id` |
| **canonical outcome** / **side** | `P(team X wins)` for one canonical event |
| **instrument** | One venue's native tradable or quotable object (market, outcome, book line) |
| **mapping** | A versioned claim that an instrument represents a canonical outcome with a stated polarity, `mapping_status` and `settlement_equivalence` (§3) |
| **settlement state** | S1–S6 (§2), assigned per event after the game |
| **reference venue** / **cross-check venue** / **entry venue** | The venue whose close is the benchmark (Novig), the independent comparison (Kalshi), and the venue whose price is being judged |
| **observed quote** | A native price exactly as received, with venue time, observed or snapshot time, bookmaker `last_update` if given, and quote age |
| **decision quote** | The latest quote known as of a decision time under the data-access contract. An earlier provider timestamp alone does not make a quote known |
| **execution assumption** | Stake, latency, next admissible price, depth and a feasibility label (`confirmed_fill`, `plausible_simulation`, `unverified_historical`, `unavailable`) |
| **entry** | One side of one instrument at one decision time, tied to a signal or labeled as a control or manual entry |
| **`d_entry`** | Payout per unit staked, derived exactly from the native quote (§4) |
| **off** / **trusted start bound** | The game's actual start, as the interval `[earliest, latest]` from the off resolver. The cutoff uses `earliest` |
| **cutoff** | `earliest plausible start − close.buffer_s` |
| **close** | The output of a versioned close definition: a benchmark probability `p_close` for one canonical outcome at the cutoff, or an explicit unscoreable reason. Always the reference venue's, never the entry venue's closing price |
| **close family** | `close_live_*` (book-based) or `close_hist_*` (trades or candles), never mixed |
| **benchmark** | The function from a book to a probability (`mid_top`, `mid_depth_N`, …) |
| **`p_ref_entry`** | The same benchmark on the reference book at the entry's decision time |
| **`clv_ev`** | `p_close × d_entry − 1`. Fee-free; relative to a named benchmark |
| **`null_ev`** | `p_ref_entry × d_entry − 1` |
| **`clv_residual`** | `(p_close − p_ref_entry) × d_entry` |
| **`fee_adjusted_close_ev`** | Modeled EV after an explicit, versioned fee and cash-flow model. An estimate, not profit |
| **`realized_roi`** | Return on real fills after real settlement. Validation only |
| **scoreable** / **unscoreable reason** | Whether a close exists. A missing close is a measured result |
| **coverage waterfall** | Counts and reasons at each stage of DESIGN.md §11.2 |

**Terms not to use:**
- "net EV": say `fee_adjusted_close_ev`
- "edge" for `clv_ev`
- "closing line" for a soft book's last price: the close belongs to the reference venue
- "actionable" for any historical quote
- "fair probability" without naming the benchmark

## 7. Open items before sign-off

| # | Check | Source | If it fails |
|---|---|---|---|
| 1 | Novig's MLB settlement rules: when a called game counts, the suspension and postponement windows, and how `FMV` is determined | Novig's contract specification or support pages; else ask in the existing developers@novig.com thread | Novig pairs become `not_equivalent`; evaluate Kalshi as primary (§3) |
| 2 | Each licensed book's house rules: pitcher default on the line The Odds API returns, official-game threshold, suspension window | Each book's own rules page, archived with a retrieval date | The book is excluded or its states remapped |
| 3 | Kalshi's full `KXMLBGAME` contract terms beyond `rules_secondary`, especially the "fair price" procedure and called games | Kalshi's rulebook or contract filing for the series | Kalshi pairs drop to `pending` |
| 4 | Doubleheader identity on every venue: Kalshi tickers and Novig `startsTs` for game 2 | **Done in B0** (`docs/feasibility.md` §3–4). Novig lists each game as its own event. Kalshi ticker dates and times can name a different game, so every Kalshi doubleheader or rescheduled game is mapped by hand from settlement result and `close_time` | The `manual_verified` rule in §5.4 stands; Kalshi mappings never come from ticker date and teams alone |
| 5 | StatsAPI status codes that distinguish S1–S6 (suspended, postponed, resumed date) | **Partly done in B0.** `Postponed` and `Cancelled` with `reason`, `rescheduleDate` and `rescheduledFrom` were observed, and `gamePk` survives a postponement. No suspended game occurred 2026-08-03 to 2026-10-08; find one in 2025 or earlier 2026 | S2–S4 are assigned by hand and logged until a suspended game is sampled |

## 8. Sign-off

Sign-off means items 1–3 are resolved (items 4 and 5 may close in B0). §3's verdicts are then updated, and this status changes to **frozen v1** with the date and commit. Record the sign-off in `docs/decisions/`.
