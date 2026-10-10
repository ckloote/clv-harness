# V0 closes, scoring and the golden game

Date: 2026-10-10

Status: decided (V0 part 3)

This records the choices V0 part 3 made in closing and scoring the golden-game candidate, CLE @ CWS, ALDS Game 4 (`gamePk` 849832). It covers the off resolver, the close function, scoring, the mapping-correction replay and the golden-game fixture (DESIGN.md §13, V0).

## 1. The golden game is committed as a synthetic game; 849832's raw frames stay local

**Decision.** The repository is public. Committing 849832's frames would republish Novig, Kalshi and The Odds API data. So:
- **CI:** a synthetic golden game in the real wire shapes, written by the recorder's own `ArchiveWriter`, is committed with its archive (`tests/fixtures/golden/v0-synthetic/`). CI regenerates it byte for byte and replays it to a committed trace (`tests/test_golden_game.py`).
- **849832:** its specs (`entries.toml`, `corrections.toml`) and expected trace are committed, without book levels. The trace has numbers, frame refs and the sha256 of each cited frame line. The test runs wherever the archive is present.

**Departure from DESIGN.md §13**, which says to commit the recorded game itself. The synthetic game exercises every stage the recorded one does. Its numbers are checked by hand: its Novig close is the DESIGN.md §2.4 worked fixture. The recorded game's replay is still checked exactly, locally. Making the repository private would allow committing the real frames instead.

## 2. The off resolver takes the earliest credible bound

`off-v0.1` (`src/clv/off/resolver.py`):
- **Accepted start claims:**
  - StatsAPI first pitch;
  - StatsAPI "In Progress";
  - Novig `GOLIVE`.

  "Warmup" is a pregame status, not a start claim.
- **Bounds:**
  - **earliest:** the earliest claim of any accepted source (DESIGN.md §5.2);
  - **selected:** the first pitch, or else the official transition;
  - **latest:** the latest first pitch.
- **Untrusted:**
  - a venue transition alone is `no_trusted_off`;
  - claims spread over more than `off.max_source_disagreement_s` are `off_disagreement`.

On 849832 the claims are:
- "In Progress" at 00:08:04.996;
- `GOLIVE` at 00:08:23.244;
- the first pitch at 00:08:51.917.

So the earliest bound is "In Progress", the source spread is 46.9 s, and the cutoff is 00:07:04.996.

## 3. Close preconditions versus cohort exclusions

A close is priced whenever the book permits: the off resolution, reference instrument, continuity, liveness, status, lag, age and depth all pass (`src/clv/close/definitions.py`).

Mapping status and settlement equivalence are cohort questions, not book questions. So `clv_score` computes the numbers and lists every exclusion beside them. It does not drop the score. On 849832 every score carries `equivalence_pending`: no rules text is verified yet (measurement contract §3). A correction that clears an exclusion therefore shows exactly what changed.

Supporting rules:
- **Reference source:** Novig's reference source is whichever of its stream and public-book poll has the freshest liveness evidence. Novig was polled until 23:31:54 and streamed from 23:32:14. So the 23:05 entry's reference is the poll, and the close is the stream.
- **Poll liveness:** `poll.liveness_max_s` (30 s) is new in DESIGN.md §10, beside `stream.liveness_max_s`.
- **Short stored ladders:** a stored ladder that runs out before the notional fails closed as `truncated_ladder` when the tick was truncated. Otherwise it is `insufficient_depth`.

## 4. The 849832 mapping correction verifies the DraftKings line

The ingest maps The Odds API by teams and `commence_time` and leaves it `fuzzy_candidate`. The correction (`tests/fixtures/golden/849832/corrections.toml`) verifies the DraftKings line by hand against every vendor event between these teams in the archive. One of those is `e0448f7b…`, the vendor's earlier ID for the same matchup, with a 21:00Z start, quoted only on 10-07 (`docs/vendor-capabilities.md`). Settlement equivalence stays `pending`.

Run 1 lists `mapping_unverified`; run 2 does not. The numbers are identical. Recomputing run 1's facts after the correction (run 3) reproduces run 1's trace exactly.

The synthetic game's correction also rejects a Kalshi ticker that the game list named wrongly: the next day's game, a planted error. That shows a correction changing numbers. Kalshi's CLE closes and scores become `unmapped`, and the original run survives.

## 5. Results on 849832 (local replay)

The entry is DraftKings Guardians at −117 (`d_entry` 217/117), decided 2026-10-08 23:05Z on the quote received at 23:02:08.

| Reference | `p_close` (all four benchmarks) | `p_ref_entry` | `clv_ev` | `null_ev` |
|---|---|---|---|---|
| Novig (stream, book 3.8 s old) | 0.5175 | 0.5125 (poll) | −4.02% | −4.95% |
| Kalshi (poll, book 0.9 s old) | 0.515 | 0.515 | −4.48% | −4.48% |

Both books held more than $1,000 of payout at the top levels, so every depth benchmark equals the top-of-book midpoint. The Novig close book is from 00:07:01.214, before the first pitch and before the in-play crossing quarantined at ingest (from 00:09:11).

## Revisit when

- **The repository goes private:** commit 849832's trimmed frames as DESIGN.md §13 describes.
- **Novig or a licensed book's rules are verified:** settlement equivalence changes, the `equivalence_pending` exclusions clear, and a new run shows it.
- **A2 adds schedule observations:** the resolver gains a polled pregame bound, `[t0, t1]` in DESIGN.md §5.2.
