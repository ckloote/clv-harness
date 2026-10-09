# Feasibility — P0

**Status:** Track B verdict accepted 2026-10-09 (`docs/decisions/2026-10-09-track-b-verdict.md`). Gate P0 stays open until §6 items 1–2 are resolved or deferred.  
**Date:** 2026-10-09

P0 (DESIGN.md §13) asks for three things before any historical data is bought:
- a check of real historical samples from each reference source (B0)
- confirmation of vendor permissions and endpoint shapes
- a Track B verdict: *proceed*, *proceed with a proven subset* or *stop*

This file records the B0 evidence and the verdict. `tools/b0/` reproduces every number in it. Vendor facts it establishes are also logged in `docs/vendor-capabilities.md`; contract consequences go to `docs/measurement-contract.md`.

---

## 1. Sample

24 MLB games, chosen to cover the cases DESIGN.md §13 names. Every one was checked against StatsAPI, Novig and Kalshi.

| Case | Games (`gamePk`) |
|---|---|
| Ordinary slate | All 15 games of 2026-08-04, the first day with Novig market files |
| Postponed, played next day | 824785 TOR @ BAL: postponed 2026-09-22 (rain), played 2026-09-23 as game 1 of a split doubleheader |
| Split doubleheaders | 824785 / 824784 TOR @ BAL (09-23); 824703 / 824706 CHC @ BOS (09-25) |
| Traditional doubleheader | 823491 / 823489 BAL @ NYY (09-25); game 2 started 3 h 21 min after its listed time |
| Cancelled | 823490 BAL @ NYY (09-27, rain) |
| Thin market | 824705 CHC @ BOS (09-27): 5 Novig fills in the close window |
| High volume, streamed | 849832 CLE @ CWS, ALDS G4 (10-08): the R0 golden-game candidate |

No suspended game occurred between 2026-08-03 and 2026-10-08. That case is still open (§6).

**Inputs** (retrieved 2026-10-09; sha256 in §7):
- Novig daily files for 2026-08-04, -08-15, -09-22, -09-23, -09-25, -09-26, -09-27 and -10-08
- Novig `/v3/history/markets/{id}` and `/v3/history/events/{id}` for 127 MLB `MONEY` markets, signed with the read key
- Kalshi public `/events`, `/series/…/candlesticks`, `/markets/trades` and their `/historical/…` counterparts
- StatsAPI `schedule` and `playByPlay`

## 2. Coverage at the close

Cutoff = StatsAPI first pitch − 60 s (`close.buffer_s`).
- **Novig columns:** STRAIGHT MAKER fills in the 600 s before the cutoff (`close.hist_vwap_window_s`). Eligible means at least 5 fills (`close.hist_vwap_min_executions`) and at least $250 of payout (`close.hist_vwap_min_notional_usd`, read as payout USD).
- **Kalshi column:** the home YES bid/ask from the last 1-minute candle ending at or before the cutoff.

| `gamePk` | Game | DH | Sched → first pitch | Novig fills | Novig payout $ | Novig VWAP (home) | Kalshi bid/ask (home) | Kalshi trades in window |
|---|---|---|---:|---:|---:|---:|---|---:|
| 824805 | LAA @ BAL | | 84 s | 65 | 10,619 | 0.582 | 0.58 / 0.59 | 225 |
| 824403 | NYM @ CLE | | 99 s | 81 | 9,346 | 0.556 | 0.56 / 0.57 | 260 |
| 824484 | ATH @ CIN | | 138 s | 42 | 3,949 | 0.553 | 0.55 / 0.56 | 155 |
| 823432 | WSH @ PHI | | 167 s | 38 | 2,324 | 0.735 | 0.73 / 0.74 | 112 |
| 823517 | STL @ NYY | | 253 s | 59 | 45,316 | 0.664 | 0.65 / 0.66 | 115 |
| 824731 | CWS @ BOS | | 51 s | 103 | 10,119 | 0.581 | 0.58 / 0.59 | 211 |
| 824888 | MIA @ ATL | | 92 s | 51 | 10,902 | 0.590 | 0.58 / 0.59 | 304 |
| 824084 | MIN @ KC | | 619 s | 24 | 3,239 | 0.415 | 0.41 / 0.42 | 56 |
| 823756 | PIT @ MIL | | 28 s | 44 | 9,710 | 0.571 | 0.58 / 0.59 | 193 |
| 824645 | LAD @ CHC | | 36 s | 63 | 39,435 | 0.357 | 0.36 / 0.37 | 350 |
| 822865 | SF @ TEX | | 237 s | 33 | 2,884 | 0.644 | 0.64 / 0.65 | 91 |
| 824161 | TOR @ HOU | | 22 s | 57 | 6,231 | 0.569 | 0.56 / 0.57 | 195 |
| 824321 | TB @ COL | | 60 s | 47 | 23,076 | 0.390 | 0.38 / 0.39 | 44 |
| 825054 | SD @ AZ | | 54 s | 88 | 22,507 | 0.538 | 0.53 / 0.54 | 292 |
| 823108 | DET @ SEA | | 75 s | 81 | 12,790 | 0.529 | 0.52 / 0.53 | 156 |
| 824785 | TOR @ BAL | S1 | 177 s | 42 | 27,310 | 0.539 | 0.54 / 0.55 | 234 |
| 824784 | TOR @ BAL | S2 | 49 s | 34 | 16,456 | 0.521 | 0.53 / 0.54 | 258 |
| 824703 | CHC @ BOS | S1 | 117 s | 181 | 134,515 | 0.478 | 0.47 / 0.49 | 336 |
| 823491 | BAL @ NYY | Y1 | 222 s | 149 | 115,348 | 0.484 | 0.48 / 0.50 | 461 |
| 823489 | BAL @ NYY | Y2 | 12,088 s | 26 | 62,494 | 0.509 | 0.51 / 0.52 | 190 |
| 824706 | CHC @ BOS | S2 | 22 s | 57 | 33,649 | 0.474 | 0.47 / 0.48 | 170 |
| 823490 | BAL @ NYY | | cancelled | — | — | — | — | — |
| 824705 | CHC @ BOS | | 50 s | 5 | 1,596 | 0.479 | 0.47 / 0.48 | 19 |
| 849832 | CLE @ CWS | | 532 s | 223 | 62,624 | 0.481 | 0.48 / 0.49 | ≥1,000 (page limit) |

**What the table shows:**
- **Every played game is VWAP-eligible on Novig.** The thinnest, 824705, sits exactly at the 5-fill minimum.
- **Kalshi quoted a 1-cent spread at the cutoff in 21 of 23 games** (2 cents in the other two).
- **The venues agree closely.** Novig's pre-cutoff VWAP minus Kalshi's mid is within ±1.4 pp in all 23 games, with a mean absolute difference of 0.49 pp. That is a sample observation, not a calibration result.
- **The schedule is a poor stand-in for the off:** first pitch came 22 s to 10.3 min after the scheduled time, and 3.4 h after it for the traditional doubleheader's game 2. This is why `off.scheduled_only` makes a game unscoreable.

## 3. Novig historical data

| Finding | Consequence |
|---|---|
| `trades.csv` starts **2026-08-03**; `markets.csv` 2026-08-04. 67 contiguous days through 2026-10-08. | At most about 770 MLB games (2026-08-03 to the end of the postseason), below the 1,500-event `controls.sample_events` target. Novig alone cannot carry B2. |
| Columns: `timestamp, outcomeId, marketId, contractSeries, league, marketType, tradeType, legs, cost, qty, side`. `qty` is payout USD (native contracts ÷ 100); `cost` is USD paid. No trade ID. | Depth and VWAP use `qty` as payout directly. |
| Each trade appears as one TAKER row and one or more MAKER rows. All 1,876 trades streamed for 849832 matched exactly one MAKER row (outcome, price and quantity). Taker rows aggregate across makers, and summed taker and maker quantities agree per outcome. | **Deduplicate on MAKER rows.** One MAKER row is one execution. Never add TAKER rows to it. |
| `tradeType = COMBO` rows (parlays) have empty `league` and `marketType`, and are 40 % of rows. | Import `STRAIGHT` rows only. |
| File `timestamp` lags the streamed trade's `ts` by 14 ms to 68 s (median 0.67 s, p90 1.2 s) for 849832. | A file timestamp is never earlier than the execution, so a strictly-before-cutoff window is conservative. Report the lag. |
| `markets.csv` OHLC is daily, includes in-play trading (849832 lows at 0.1), and is in percentage points. | **Unusable for a close.** It is a daily summary only. |
| `reportTicker` for moneylines changed from `MLB-MONEY` (August) to `MLB-WINNER` (September). The files carry no team or game. | Map through `/v3/history/markets/{id}` and `/v3/history/events/{id}` (read key; the public catalog returns 404 for closed markets). All 127 sampled markets resolved. |
| Novig names and codes differ from MLB's: `Oakland Athletics`/`OAK`, `KAN`, `ARI`, `WAS`. Event `startsTs` equals StatsAPI's scheduled `gameDate`, except a traditional doubleheader's game 2 (Novig 23:25 Z, StatsAPI placeholder 20:10 Z, actual 23:31 Z). | An alias table per venue, plus manual verification for doubleheaders (measurement contract §5.4). |
| Fee `coefficient` read 0.06 on every sampled market via history. The same live market (CWS, 2026-10-06) read 0.03 at 23:15 Z and 0.06 by 16:58 Z the next day. | A market's fee changes during its life. Fees are versioned by time, never read once (DESIGN.md §2.3). |

**Settlement evidence** (measurement contract §2):
- **Postponed (824785):** Novig listed the rescheduled game as a new event (`01a0ca85…`, 2026-09-23 17:35 Z). It also kept the original event (`01a0c55e…`, `startsTs` 2026-09-22 22:35 Z) and settled that `WIN`/`LOSS` on the same game's result. That is S5: settled on the rescheduled game, not voided. Two Novig markets therefore map to one `gamePk`, so the mapping must pick one per game and record why.
- **Cancelled (823490):** `CANCELED`, settled `FMV` at NYY 0.522, BAL 0.478 (S6).
- **Doubleheaders:** each game is its own event and market, settled on its own result.

## 4. Kalshi historical data

| Finding | Consequence |
|---|---|
| `KXMLBGAME` events run from **2025-04-16** to now: 4,673 events, covering the full 2025 and 2026 seasons. | Enough for the B2 sample target on Kalshi. |
| Live and historical tiers split at `GET /historical/cutoff`, which read 2026-08-10 for markets and trades on 2026-10-09. Older markets, candles and trades come from `/historical/…`. Field names differ by tier (`close` and `volume` in historical, `close_dollars` and `volume_fp` in live). | The importer reads the cutoff at run time, uses both tiers and normalizes field names. Deduplicate trades by `trade_id` across the handoff. |
| 1-minute candles carry YES bid and ask OHLC plus trade-price OHLC. In 2026 samples, candles are present every minute near first pitch; a 2025 game had 147 candles in 360 minutes. | Genuine bid/ask candles support `close_hist_candle_*`. Quiet minutes have no candle, so the close's quote age must be measured, never assumed to be 60 s or less. |
| Trades carry `trade_id`, `created_time` (µs), `count_fp`, YES/NO prices and taker side. | Supports `close_hist_vwap_*` on Kalshi with exact deduplication. |
| **Ticker times are unreliable.** `occurrence_datetime` equals the ticker time read as Pacific; several tickers are 3 h off StatsAPI. 2025 tickers carry no time at all. | Never derive start or identity from a Kalshi ticker. |
| **Ticker dates can name a different game.** `…26SEP261915BALNYY` settled on the 09-25 traditional doubleheader game 1 (BAL won). `…26SEP251905BALNYY` settled on game 2 (NYY won). `…26SEP261915CHCBOS` settled on the 09-27 game. The split doubleheader used a `G1` suffix on one ticker only. | Kalshi mappings are `manual_verified` from settlement result and `close_time` against StatsAPI, or excluded. A date-and-teams rule alone would have mis-mapped 3 of the 9 edge-case games (824785, 823491, 823489) and found no market for a fourth (824705). |
| **Settlement evidence.** Postponed 824785: the 09-22 market stayed open and settled on the 09-23 game (S5, as its rules say). Cancelled 823490: `result: scalar`, BAL 0.47, NYY 0.53 (S6 fair price; Novig's FMV was 0.478/0.522). | Kalshi and Novig both settle S5 on the played game and S6 at a fair value; licensed books void both. |

## 5. MLB StatsAPI

| Finding | Consequence |
|---|---|
| `playByPlay` gives the first pitch's `startTime` to the millisecond for every played game in the sample. For 849832 it matched the feed archived during R0 exactly. | The authoritative first-pitch source for off resolution. Corrections over longer horizons are not yet tested. |
| A postponed game keeps its `gamePk`. The schedule then lists it twice: a `Postponed` row (with `reason` and `rescheduleDate`) on the original date, and a `Final` row (with `rescheduledFrom`) on the new one. | Event identity keys on `gamePk`. Schedule observations are append-only (DESIGN.md §4.1). |
| `detailedState` values seen: `Final`, `Postponed`, `Cancelled`, each with a `reason` such as `Rain`. Doubleheaders are flagged `doubleHeader` `S` (split) or `Y` (traditional) with `gameNumber`. A traditional game 2's `gameDate` is a placeholder (game 1 + 5 min). | Settlement-state assignment (measurement contract §2) can be automated for S5 and S6. A suspended game still needs a sample. |

## 6. Open items

1. **Suspended games** (S2–S4): find one in the 2025 or 2026 season, and record its StatsAPI states plus Novig's and Kalshi's settlement.
2. **P0 item 3:**
   - Novig and Kalshi terms for research use of their data.
   - The Odds API pricing and historical availability, rechecked immediately before any B3 purchase.
3. **Kalshi depth:** candles give only the top of book. A depth benchmark (`mid_depth_*`) has no Kalshi history, so Kalshi historical closes are `mid_top` only.
4. **Novig before 2026-08-03:** confirm with Novig that no earlier public trade data exists.

## 7. Track B verdict: proceed with a proven subset (accepted)

- **B1 imports two reference sources.**
  - **Kalshi `KXMLBGAME`, 2025–2026:** trades and 1-minute bid/ask candles. About 4,600 events, so B2 can reach the 1,500-event target on Kalshi alone. Mappings are `manual_verified` or excluded.
  - **Novig trades, 2026-08-03 onward:** STRAIGHT MAKER rows only, mapped via the history routes. About 770 games: a second close family, reported separately, below the sample target on its own.
- **B2** runs check 1 on each family independently. A Novig-family result below the sample target is labeled `insufficient_data`, never passed.
- **B3** (paid The Odds API history) is decided only after B2, as DESIGN.md §13 requires. Nothing in B0 justifies a purchase yet.

The verdict was accepted 2026-10-09. Gate P0 closes when §6 items 1–2 are resolved or explicitly deferred.

**Input hashes** (sha256, files as retrieved 2026-10-09):

```text
62160c9f3f968a41b22a6c7d3f05dc2efb249293337bfe11fa8305b1cab7096d  novig 2026-08-04/trades.csv
46e3340c0147e459d3ab3e702ec9e3f9e3c92b264e9b4d52ea31c1b74fa7959d  novig 2026-09-22/trades.csv
56bf7a6f396a87c666f6cbf335ddbfc21844a766132cc476c854c509ccbd6ffe  novig 2026-09-23/trades.csv
091002707986141408bb7b179354fb0464e589c3018519da4ff0de299b27cc8f  novig 2026-09-25/trades.csv
25cffda6848d2c26dfaed2b256c469410f4a6c42fa8afff91ea098ab24a489dc  novig 2026-09-26/trades.csv
20aeb3379060be536e3c5c9eab116eac11f595c6f9c934f8590cb01f9a3f1556  novig 2026-09-27/trades.csv
bcd0c4ead9a5b81478f941a0e76bcca384ad14f66bcf7f8c50d9438857f0a4bd  novig 2026-10-08/trades.csv
10af54ed0d6c1776918ba392e2116cdb64b0f8b09b036ad4775b9d4810e55459  statsapi schedule 2026-08-03..2026-10-08
0cb906e3671defb221d5667945d1606af9060b3718462b92f33ac256b6ae4af1  kalshi KXMLBGAME events listing
```
