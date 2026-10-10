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
| **A venue status change is a tick** even when the book is unchanged. It restates the last stored book with the new status and cites the frame that carried the status. A frame is handled whole: the restatement happens only if the frame carries no book of its own, or a probe confirmed the book unchanged. A frame's sequence gap, replacement book or quarantined book takes precedence, and a superseded probe's status is ignored | §7.2 says a tick is written when book state changes | Otherwise a market the venue reported `CLOSED` in a confirmed probe would keep an `OPEN` tick backed by fresh liveness evidence. And a snapshot that reports `OPEN` while replacing the book with a crossed one must not restate the old book as fresh and open |
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
