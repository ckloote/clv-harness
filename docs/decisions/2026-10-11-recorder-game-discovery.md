# The recorder discovers its games

Date: 2026-10-11

Status: accepted

## Problem

Each morning a scheduled task matched the day's games to Novig and Kalshi markets by hand-written rules and edited `games.toml` and `games.paper.toml`. Then the operator had to:
- stop the recorder, because `catalog` and `mlb-feed` refuse to run beside it and `run` read the game list only at startup;
- run `mlb-feed` for yesterday's finals;
- restart the recorder;
- commit the edited lists.

That's a manual step a day that the recorder can't run without, and every restart is a recorded gap in every source. The routine also still restarted the Paper recorder, which stopped being needed once Production access arrived (`2026-10-09-r0-gate.md`). And it assumed `uv run` in a terminal, not the systemd service.

## Decision

**The running recorder discovers games (`raw_recorder.discovery`).**
- At startup and then every `discovery.interval_s` (hourly), it reads:
  - the StatsAPI schedule for today and `discovery.days_ahead` days (US Eastern);
  - Novig's MLB `MONEY` catalog;
  - Kalshi's `KXMLBGAME` series.
- Every response is archived, as DESIGN.md §13 R0 requires for the catalog responses used.
- It matches with the routine's rules, now in code and tested:
  - **Novig:** outcome names equal the two teams' Novig abbreviations, and `startsTs` is within 6 h of the StatsAPI start.
  - **Kalshi:** the event's Eastern date is the game's, and its two tickers name the two teams.
  - **Conflicting starts:** a venue start more than 15 min from MLB's widens the window to cover every claimed start. Recording begins 3 h before the earliest and runs 90 min past the latest.
- It merges the result into `<archive>/games.discovered.toml` and reloads at once.
  - Merging never loses capture: a market once listed stays listed.
  - Before a window opens, the newest schedule wins. Once it is open, the window only grows. An ended game is left as recorded.
- A catalog that could not be read is not read as empty.

**Capture fails open; scoring fails closed.**
- An ambiguous match, such as a doubleheader, records every candidate and warns.
- Discovered links are rule matches that nobody checked. So `clv ingest` loads them as `fuzzy_candidate`, outside the primary cohort (measurement contract §4, §5.4, DESIGN.md §4.2). A `corrections.toml` entry verifies a game's links when it is scored, as it already does for The Odds API's links.
- Checking happens for the games that are scored, not every listed game every morning.
- The contract's Kalshi doubleheader rule still holds: those mappings never come from ticker date and teams alone.

**`games.toml` stays, for hand-checked overrides.**
- An entry there replaces the discovered entry with its gamePk whole, and its links ingest as `manual_verified`, as before.
- The recorder rereads it within seconds of an edit. Edits are committed, which keeps their provenance.
- `raw-recorder games` and the recorder warn when an entry hides markets that discovery found.

**Final MLB feeds are fetched by the recorder.** It fetches each game's feed when the window ends, as before, and again `mlb.refetch_after_s` (12 h) later, for the final data and scoring corrections. `mlb-feed` remains for anything else.

**Paper needs no list.** `games.paper.toml` is removed. The Paper config discovers Paper's own markets into `archive-paper/` if it is ever run again.

**The morning task becomes a read-only check.** It reports:
- `raw-recorder status`;
- `raw-recorder games`;
- the service state and its recent warnings.

It edits nothing, stops nothing and commits nothing.

## Consequences

- Recording new games needs no person: no restart, no edit, no commit.
- A restart is needed only for code, unit or secret changes, between capture windows.
- Every round's schedule and catalog responses are in the archive.
  - The `games_discovered` and `games_loaded` events record what was matched and what was recorded, with notes and warnings.
  - `process_start` lists every game with its source.
- Scoring a discovered game takes one correction per instrument side, which is the same review the morning routine did, now done once, for the games that are scored.
- The regular season needs only `"R"` in `discovery.game_types`. With 15 games a day, check The Odds API credit use first: polling runs whenever any game's odds window is open.

## Revisit when

- A venue changes its catalog shape, or its team names, or Novig its abbreviations. The harness and recorder tables are checked to agree (`tests/test_ingest.py`).
- A doubleheader or a postponement is recorded: confirm the candidates and notes look as described.
