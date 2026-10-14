# B1: how the historical reference import is fetched, linked and loaded

Date: 2026-10-14

Status: accepted. The counts are filled in from the pilot and the full run.

## Context

B1 imports two historical reference sources, Kalshi `KXMLBGAME` and Novig's public trades (`2026-10-09-track-b-verdict.md`), together with MLB first pitches. It also produces the per-game coverage table. B2 builds the historical close families and check 1 on it. The vendors' terms were checked on 2026-10-13: the data may be used for personal research, with no resale or redistribution (feasibility §6, item 2).

## Decision

**Nothing vendor-derived is committed.** `ckloote/clv-harness` is public. These all live under git-ignored paths:
- the raw archives: `archive/`, `archive-hist/`;
- the database: `data/`;
- the link review files and the coverage table: `data/b1/`.

Docs and PRs carry aggregate counts only, never per-game prices, and test fixtures are synthetic.

**The inputs are archived like R0's.** `raw-recorder backfill` fetches through the REST envelope into `archive-hist/`, a separate root with its own lock, so it runs beside the live recorder. It is resumable: a rerun sends only what failed or was never fetched. Segments seal every 5 minutes, because Kalshi trade pages fill an hourly segment past 10 GB.

**Kalshi markets come from two listings**, the live events and `/historical/markets`. Events on the historical tier no longer list their own markets (vendor-capabilities discrepancy log, 2026-10-14). Each market's window is the 32 h before its `close_time`, not around its ticker date, because ticker dates do not identify the game.

**Links: the rule proposes, a person approves.** `clv backfill link` applies tested rules (`clv.backfill.links`).
- **Kalshi** (`kalshi settlement rule v1`): the one Final game between the ticker's teams, within 3 days of its date, that started within 8 h before `close_time` and whose winner is the market that resolved `yes`.
- **Novig** (`novig event rule v1`): the one game whose teams are the history event's, starting within 6 h of `startsTs`, with a settled market whose `WIN` grade is the Final game's winner. A game or market without a result (unplayed, unsettled or cancelled) is never clean.
- **What gets written:**
  - Matches are stored as `fuzzy_candidate` mappings.
  - The clean ones are written as `clv correct` corrections to approve (`links.proposed.toml`).
  - `links.review.md` holds a seeded random spot-check of 50 per venue and every edge case.
- **Edge cases are never proposed:**
  - doubleheaders, postponed or rescheduled games, a game on another day than the ticker;
  - several candidates or none;
  - a disagreeing result;
  - a cancelled (`scalar`) settlement.
- **Approval:** the approved corrections make the clean links `manual_verified`. This is the Track B verdict's "manual_verified from settlement result and close_time": the rule does the lookup, and a person signs off on the sample and decides every edge case. Only the approved file's sha256 and its counts are recorded here.

**Only what a pregame close can use is loaded.** Candles and trades are imported only for linked markets, and only before their game's first pitch. In-play trading is most of the volume (the pilot's 56 markets held about 690,000 trades over 32 h), and nothing after the start can enter a close (DESIGN.md §5.4). The archive keeps everything.

**One execution, however many records** (DESIGN.md §7.4):
- **Kalshi:** keyed by `trade_id` across both tiers.
- **Novig MAKER row:** each is an execution, keyed by its fields and its occurrence in the file, so a republished file adds sightings, not executions.
- **Novig TAKER row:** names its dual-side group, marked `balanced` or `ambiguous`.
- **R0 stream trade:** joins its MAKER row by B0's rule. It waits for its day's file, so an execution's price is always the file's exact `cost / qty`.

`clv trace --execution` shows every record of one execution, cited to frame and item.

## Consequences

- B2 reads candles and executions from the database and resolves its own cutoffs from the off observations. The coverage table (`clv backfill coverage`) shows in advance how many games each close family can score, and where the rest drop out.
- **Approval is a review step:** a spot-check and a decision on each edge case, recorded as a correction file.
- **Rerunning:** the backfill and import can be rerun at any time. Each adds only what is new.

## Results

**Pilot, 2026-10-13** (August 2026 plus B0's September and October days; aggregate counts only). It was loaded into a scratch database with every proposed link applied, as an approval would.

- **Fetch:** 652 MB archived, no failed requests.
  - Kalshi: 1,076 markets in range, out of 9,376 listed (about 4,690 games).
  - Novig: 36 daily files and 491 MLB moneyline markets.
- **Links, across every listed market:**
  - Kalshi: 4,475 events clean, 197 for review, 16 excluded. The review cases are mostly doubleheaders (100). The exclusions are cancelled games and All-Star events.
  - Novig: 472 markets clean, 19 for review. The cancelled game is one of the review cases (recounted after the PR #18 review; it was proposed clean before).
  - Some April 2025 Kalshi markets have a `close_time` almost a year after their game. The rule finds no game for them and lists them for review.
- **Data, for the pilot's linked games, before their first pitch:**
  - **Kalshi:** 998 markets, 1.06M candles and 1.93M executions. Another 8.3M trades and 170k candles were after the first pitch and not loaded. No two tiers' candles disagreed.
  - **Novig executions:** 384,530 MAKER executions.
  - **Novig TAKER rows:** 308,034 balanced and 6,603 ambiguous (2%).
  - **Stream:** all 916 pregame stream trades of the recorded games matched a MAKER row.
- **Timing:** the data import took 11 minutes, the link step 25 s and the coverage table 8 s.
- **Coverage, 504 games with a trusted off:**
  - **Kalshi:** a candle wholly before the cutoff for 440 of the 440 games with a verified link. All 440 also have one at the 6 h, 1 h and 15 min offsets, and 427 at 24 h.
  - **Novig:** 401 of 413 verified games are VWAP-eligible.
- **Off disagreement:** 41 games (8%) are untrusted as `off_disagreement`. In those, StatsAPI's "In Progress" status precedes the first pitch by more than `off.max_source_disagreement_s` (120 s). This is for B2 to review against the A2 agreement report.
- **Against B0's 24 games:**
  - Novig fills and Kalshi trades in the window match B0 within a few counts per game.
  - They differ where B1's cutoff sits at the earliest start claim, not the first pitch.
  - B0's hand-mapped edge cases come out as cases for review.

**Full run:** to be filled in once it has been fetched, reviewed and approved.
