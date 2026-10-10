# Schema changelog

DESIGN.md §8 lists the tables conceptually, and §13 (Gate V0) expects contact with real data to change them. This file records each change against those lists, why it was made, and the migration that made it. A change to an applied migration is never allowed (`clv.db` refuses it); a change is a new migration.

## 0001_v0 (V0, 2026-10-10)

The tables are the V0 row of DESIGN.md §8.5, built from the recorded golden-game candidate, `gamePk` 849832 (`docs/v0-data-shapes.md`). The differences are:

| Change | Against | Why |
|---|---|---|
| **Books are stored per native side.** `venue_instrument` has a fixed side 0 and side 1 (Novig outcomes, Kalshi YES/NO, a sportsbook's home/away). `tick` stores both sides' bids as received, and `instrument_mapping_observation` maps each side to a canonical outcome as `direct` or `complement` | DESIGN.md §4.2–4.3 left the stored polarity open | Both exchanges quote two-sided binary books. Storing them natively means a mapping or polarity correction re-derives the close and never rewrites a tick |
| **Child tables `tick_level` and `book_snapshot_level`** | §8.1 lists `tick` and `book_snapshot` only | Each level needs its own CHECK constraints (probability range, non-negative depth) and its native price and quantity strings |
| **Quantities are kept as native text plus integer payout cents** | §8.3 named integer probabilities only | Novig quantities are integer contracts paying $0.01; Kalshi quantities have two decimals and each contract pays $1.00. Both give a whole number of cents, and the ingest refuses one that doesn't |
| **Exact rationals as `_num`/`_den` pairs** for `p_close`, `d_entry` and the EV columns | §8.3: probabilities ×10,000 | A depth-walk VWAP and the decimal odds of an American price (−118 → 109/59) are not on the ×10,000 grid. Rounding them would break "derive `d_entry` from that quote" (§2.2) |
| **`liveness_evidence`** (new fact table) | Not in §8.1 | Confirmed probes, unchanged polls and deltas that leave the top N unchanged are evidence of liveness without being a book change (§7.2, §10). Without them, liveness and quote age could not be told apart (§10) until `poll_result` arrives in A4. This replaces the "coverage" companion proposed in `docs/v0-data-shapes.md` |
| **A tick never spans a gap.** The first observation after a subscription, resync, failed poll or recorder restart is always a tick, even if unchanged. Recovery polls are exactly the frames where `gaps.poll_gaps` closes a gap, so ticks and gaps can't disagree. The exception is a post-gap state that is quarantined: no tick follows the gap until a valid state arrives | §7.2 says a tick is written "on change" | The latest tick at or before a cutoff is then after any gap that ends before the cutoff, unless that tick precedes the gap. The close function rejects a tick with a gap after it (`collection_gap`) |
| **A venue status change is a tick** even when the book is unchanged. It restates the last stored book with the new status and cites the frame that carried the status. A frame is handled whole: the restatement happens only if the frame carries no book of its own (a probe confirmed the book, or was behind it). A frame's sequence gap, replacement book or quarantined book takes precedence. A status's freshness is judged by the lifecycle channel's own sequence, never the book's: a status behind it is ignored, and a newer one in a probe whose book is behind is kept | §7.2 says a tick is written when book state changes | Otherwise a market the venue reported `CLOSED` in a confirmed probe would keep an `OPEN` tick backed by fresh liveness evidence. And a snapshot that reports `OPEN` while replacing the book with a crossed one must not restate the old book as fresh and open. A delayed snapshot can carry an old book and a new `CLOSED`; dropping the status with the book would leave a closed market open |
| **Nothing vouches for a stored state once it is no longer current.** After a quarantined (crossed) state or a sequence gap, liveness evidence stops until a new state is stored, and that state is always a tick | — | Evidence means "the last tick is still the book"; that is false while the book is crossed or unknown. On 849832 this moved 4 in-play rows from evidence to ticks; pregame rows are unchanged |
| **`raw_artifact` is one sealed segment under one parser version**, cited with `raw_line` | §7.1/§8.1: payload location, hash and parser version | A frame is the unit of evidence. A segment plus a line number points to it exactly, and the segment's sha256 proves the bytes |
| **`event.league_game_id`** (the `gamePk`) is on the immutable event | §4.1: "attach … as attributes" | `gamePk` survives postponement and separates doubleheader games, so it is identity, not an observation. Doubleheader flags and game numbers, which can change, stay in `event_alias.detail_json` |
| **`settlement_equivalence` is per mapping, relative to the canonical contract** | The measurement contract §3 tabulates verdicts per (entry, reference) pair | A pair is primary-eligible when both of its mappings are `identical` or `equivalent_when_played`. Per-mapping verdicts compose, and one changes in one place when a venue's rules are verified |
| **Crossed books are quarantined, not stored** | §2.4: quarantine an internally inconsistent book | On 849832 the Novig stream book was crossed by 0.5–10 pp for 61,621 of its states. Every one was in-play, starting 19 s after first pitch. The ingest reports a count and the first and last frame; the raw archive keeps them |
| **Every derived table is immutable too** (no UPDATE or DELETE) | §8.2 required immutable runs | Recomputation is a new run with its own `fact_snapshot` |

Not in 0001, by design (§8.5):
- `trade_execution` and `trade_source_observation` arrive with A1 or B1.
- `event_schedule_observation` arrives with A2. Until then, provider schedules are recorded on `event_alias`.
- `poll_attempt` and `poll_result` arrive with A4.
- `settlement_observation` arrives with B2 or G2.
- `historical_candle` arrives with B1.
- `contract_rule_observation`: until it exists, a mapping carries the venue's rule text in `rules_native`.

## 0002_v0_scoring (V0, 2026-10-10)

Computing closes and scores on the golden-game candidate forced two changes to derived tables. Neither had been written by code before, and both are rebuilt with their rows copied (SQLite cannot relax `NOT NULL` in place).

| Change | Against | Why |
|---|---|---|
| **`close_price.cutoff_ts_ms` may be NULL**, only for an unscoreable row with no book | 0001: `NOT NULL` | A game with no start claim at all (a postponement, or nothing recorded) has no earliest bound, so there is no cutoff. The row still exists and says `no_trusted_off`: a missing close is a measured result (DESIGN.md §5.4) |
| **`clv_score` cites the reference book behind null EV** (`ref_entry_tick_id`), or says why there is none (`ref_entry_reason`). A trigger requires that book to be the reference venue's and received by the entry's decision time | 0001 stored `p_ref_entry` with no lineage | Every number must trace to raw frames (Gate V0). `p_ref_entry` is priced from a different book than `p_close`, and using a book the decision could not have seen would make null EV time-travel (§8.3) |
| **Each metric exists exactly when its inputs do**: `clv_ev` with `p_close`, `null_ev` with `p_ref_entry`, `clv_residual` with both | Not constrained | A score row is kept when an input is missing, so the constraint is what stops a metric without its inputs |

Rows scored before 0002 get `ref_entry_reason = 'not_recorded'` when they had no `p_ref_entry`.
