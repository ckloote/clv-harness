# V0 data shapes: the golden-game candidate, parsed

Date: 2026-10-10

DESIGN.md §8.5 says contact with real data will change the V0 schema. This note records that contact for Cleveland Guardians @ Chicago White Sox, ALDS Game 4, `gamePk` 849832, before any table is written. Reproduce it with:

```bash
uv run clv inspect --game-pk 849832
```

## What the archive holds

Capture window: 2026-10-08 18:00 to 2026-10-09 01:30 UTC.

| Source | Parsed |
|---|---|
| Novig stream | 82,341 frames on two connections (`book`, `trades`) from 23:32:14 to 01:30:00. 78,562 book states (the subscribe snapshot plus one per delta), 1,942 trades (66 of them from the subscribe snapshot's recent-trade list), `GOLIVE` on each connection. No sequence gaps. All 68 probe replies confirmed the replayed state: the 2 `book` replies order for order, the 66 `trades` replies by sequence |
| Novig public book | 1,968 polls from 18:00:07 to 23:31:54, every ladder complete (5–9 levels per side). The poll stops when the stream starts |
| Novig catalog | One `MONEY` market. Outcomes `CWS` and `CLE` by abbreviation; `voids: FMV`; fee coefficient 0.06, `WHEN_LIVE`; `startsTs` 00:00:00 |
| Kalshi | 5,350 order-book polls, all 200, for `…CLECWS-CLE` and `…CLECWS-CWS`; four market listings (status `active`, no result) |
| The Odds API | 11 polls, all 200; 198 quotes from 9 books, one vendor event ID. The last pregame poll is at 23:52 |
| MLB StatsAPI | Feeds at 01:30 (in progress, CWS leading 2–0) and at 12:03 the next day (final, CLE 9–5) |

At first pitch minus `close.buffer_s` (00:07:51.9) the latest books were:

| Venue | Received | CLE |
|---|---|---|
| Novig stream, seq 12125 | 00:07:50.4 | bid 0.515, ask 0.52 |
| Kalshi `…-CLE` YES | 00:07:44.1 | bid 0.51, ask 0.52 |

The licensed books' last pregame prices for CLE, from the 23:52 poll, were DraftKings −118, FanDuel −114, BetMGM −118 and BetRivers −114.

## Off observations

| Source | Claim | UTC |
|---|---|---|
| StatsAPI | Status Change - Warmup | 23:43:15.182 |
| StatsAPI | Status Change - In Progress | 00:08:04.996 |
| Novig stream | `GOLIVE` (venue time) | 00:08:23.244 |
| StatsAPI | first pitch | 00:08:51.917 |

Each StatsAPI claim appears in four archived responses: the feed and the play-by-play, at 01:30 and at 12:03. All four agree.

**Decision needed before PR 3: which source sets the earliest plausible start.** DESIGN.md §5.2 derives the conservative close boundary from the earliest credible start bound across accepted sources. The official In Progress status change came 47 s before the first pitch, and Novig's `GOLIVE` 19 s after that status change. If In Progress is an accepted source, the cutoff moves from 00:07:51.9 to 00:07:05.0.

My recommendation: select the first pitch as the start, accept In Progress as the earliest plausible bound, and keep `GOLIVE` as the venue-perception alternative. This costs 47 s of pregame data. It goes in a decision record with the resolver.

## What this changes in the V0 schema

1. **Books are stored per venue side, not per canonical outcome.** Novig quotes two outcomes of one market. Kalshi quotes YES and NO of two tickers per game. Each source's native book is "bids on side 1, bids on side 2", and asks are the other side's bids complemented. `tick` and `book_snapshot` store both sides' bids as received. Mapping to a canonical outcome, with its polarity, is applied when a close is computed (§4.3), so a mapping correction never rewrites a tick.
2. **Novig is an order-level book.** `tick` stores the top `book.tick_levels` (10) aggregated price levels per side. That covered the whole book on this game (5–9 levels per side). The order-level state is never stored: it is replayed from the raw archive (§7.2), and the replay is checked against every probe snapshot.
3. **Quantities.**
   - Native quantity is an integer number of contracts on Novig and has two decimals on Kalshi.
   - A Novig contract pays $0.01 and a Kalshi contract $1.00, so payout is a whole number of cents on both venues.
   - So the schema keeps the native quantity as text, plus an integer payout in cents. It checks that the conversion is exact.
   - Prices fit the §8.3 integer ×10,000 scale on both venues: Novig quotes three decimals, Kalshi whole cents written with four.
4. **Citations.** `raw_artifact` gets one row per sealed segment: its path, sha256, byte count and frame count. A frame is cited as (`raw_artifact`, line). A replayed book state rests on its snapshot frame and its latest delta frame. The deltas between them are the contiguous sequence numbers in between, so the tick stores the snapshot citation, the latest citation and `seq`.
5. **Off observations: one row per sighting.** Each row keeps its citations. Duplicated claims are cheap and show when each source first made a claim available: StatsAPI's first pitch was first available here at 01:30, 81 minutes after it happened.
6. **Coverage, not just gaps.** The archive proves where coverage begins and ends:
   - Recorder restarts left three gaps on the polls; the Novig public poll has only the first two, because it stopped when the stream took over.
   - The stream's end at 01:30 was planned (`no_active_markets`).
   - The 20 s handoff from the Novig public poll to the stream is a change of source, not a gap within either one.

   A close needs continuous coverage by the source it uses, so `collection_gap` has a companion: the first and last trustworthy observation of each subject and channel.

   For the stream, coverage comes from the replay, which is the only judge of channel evidence:
   - Each subscription of a market's channel is one span, including a market subscribed after the socket opened. A resubscription starts a new span.
   - A span runs from its subscribe snapshot to the last trustworthy observation: a snapshot, a contiguous delta, or a confirmed probe.
   - A confirmed probe extends the span but adds no book state, so the book's own change time stays put.
   - A sequence gap closes at the next trusted state for that market and channel, on any connection.
   - A deliberate unsubscribe ends a span without a gap.
7. **Poll gaps can't detect a silent slowdown yet.** Within one recorder session, a poller that silently slows down is invisible until `poll_attempt` arrives with A4. Until then, a stale Kalshi book shows up as quote age (`close.max_quote_age_s`), not as a gap.

## Outside V0

- **Trades.** They are parsed, but `trade_execution` arrives with A1 or B1.
- **Settlement.** No settlement was captured on the stream: there was no `END` or `GRADE`, and Kalshi was not polled after the game. The result is known from the StatsAPI final feed. `settlement_observation` arrives with B2 or G2. CLV itself does not need the result.
