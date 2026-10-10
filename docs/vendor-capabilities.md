# Vendor Capabilities

Facts the harness depends on, each with a verification status. Implement against **Verified** entries and re-verify them at first contact. Treat **Reported** entries as leads and **Unverified** entries as open questions with an owning phase. When something turns out different, add a dated row to the discrepancy log at the bottom — never silently edit an entry.

- **Verified** — read in the vendor's own documentation on the stated date.
- **Reported** — secondary source, search snippet or vendor statement not yet tested by us.
- **Unverified** — an assumption the plan depends on; the owning phase must test it.
- **Own work** — established by our own code or observation, not by vendor documentation.

---

## Novig (v3 API) — primary reference

Source: `docs.novig.com`, read 2026-10-04; targeted streaming and monetary corrections checked 2026-10-05. **Verified** means documented, not exercised with a live credential.

### Keys and signing

| Fact | Status |
|---|---|
| The management key is created in the web app at Profile → Settings → Novig API. The browser generates the keypair (Ed25519, P-256 fallback) and shows the private key once, as a PKCS#8 PEM download. | Verified 2026-10-04 |
| Management key: one live per trader. It opens, funds and labels subaccounts and creates and revokes keys, but cannot place orders. | Verified 2026-10-04 |
| Scopes and limits: `management` (1 per trader), `management::read` (up to 128 per trader), `trading` (1 per subaccount), `trading::read` (up to 128 per subaccount). At most 5 subaccounts. | Verified 2026-10-04 |
| `POST /v3/account/subaccounts` creates a subaccount together with its `trading` key. `POST /v3/account/subaccounts/{keyId}/keys` creates a `trading::read` key (scope required). | Verified 2026-10-04 |
| Both `trading` and `trading::read` can open the websocket and read the catalog and order book. **The harness uses `trading::read` only.** | Verified 2026-10-04 |
| Requests are signed with the `NOVIG-V3` scheme, Ed25519 preferred, P-256 allowed. The key ID goes in the `Novig-Key-Id` header. Novig publishes 30 sample signatures; `POST /v3/echo` returns the body with `200` when a signature is correct and costs no throttle tokens. | Verified 2026-10-04 |
| Key revocation takes effect within 60 s. | Verified 2026-10-04 |
| Harness Production read key: `trading::read` key `29dbded0-8dc1-4d50-8ff7-05d4449e4757` on subaccount `e9d15e4c-98ae-48ff-9257-b20826f37acb`, created 2026-10-08 about 23:18 UTC by `novig-provision`, fingerprint `sha256:e5e37565…0657`. The management private key is removed from the recorder host after provisioning (`docs/decisions/2026-10-08-single-host-provisioning.md`). | Own work 2026-10-08 |
| Whether a subaccount must be funded before its `trading::read` key can stream. Opening and funding are separate routes. | Own work 2026-10-08 (Production): no funding needed. The read key on the zero-balance subaccount `e9d15e4c…` passed echo (200) and limits (200), was refused key metadata (403), and opened `book` and `trades` connections with snapshots and sequenced deltas. Novig had said so in its reply the same day |
| The docs include an LP-onboarding page about API access for liquidity providers. Whether it gates anything relevant to read-only access. | Reported 2026-10-08 (Novig): it does not; Novig enabled Production key creation for read-only use without the LP deposit |
| `GET /v3/keys` and `GET /v3/keys/{id}` accept only `management` and `management::read` keys. A `trading::read` key therefore cannot read its own scope; a 200 on that route means a management key was loaded, which the recorder refuses. | Verified 2026-10-06 ([keys](https://docs.novig.com/api/api-keys)) |
| A Paper environment (`https://api.paper.novig.com`, play money, no location check) runs the same API with its own keys; a Paper key is rejected by Production. Usable for signer and stream tests without real-money access. | Verified 2026-10-06 ([environments](https://docs.novig.com/api/environments)) |
| Production key creation is gated per account. The web app shows **Settings → Novig API** only when `IS_DEMO` (the Paper build) or the PostHog feature flag `novig-api` is on, so on Production the entry is absent until Novig enables it. The API also has a `NOVIG_API_DISABLED` (403, "Novig API not enabled") error beside `KYC_REQUIRED` and `REGION_RESTRICTED`. The only documented route to Production access is LP onboarding (email developers@novig.com; QA in 2 business days; about $30,000 minimum deposit, for liquidity providers). | Own work 2026-10-06 (Paper web bundle; our Production account shows no Novig API entry) — read-only access terms unverified; to ask Novig |

### Location checks

| Fact | Status |
|---|---|
| Every signed route runs an IP check (refuses restricted states and VPN/proxy/Tor; datacenter addresses pass) and a companion check (the key holder's last device geolocation must exist and be in a permitted state). | Verified 2026-10-04 |
| The companion check's 3-day freshness window applies to order placement only; reads accept an older geolocation. | Verified 2026-10-04 |
| A failed check returns `451`. Harness policy: record a `collection_gap` with cause `location_check` and alert; never work around it. | Verified (status code); policy is ours |

### Throttles

| Fact | Status |
|---|---|
| Per-key token buckets (burst / refill per second): place 256/8, cancel 256/16, read 64/16, account 64/8, stream 512/4, history 512/4. A separate per-IP edge filter sits in front. | Verified 2026-10-04 |
| `GET /v3/limits` reports live bucket values and the subscription cap. Read it at startup instead of hardcoding. | Verified 2026-10-04 |
| One websocket connection may watch up to 2,048 markets; an event subscription counts all its markets. Exceeding the cap returns `SUBSCRIPTION_LIMIT_EXCEEDED` and subscribes to nothing. | Verified 2026-10-04 |
| Over-limit requests get `429` with `Retry-After`. Throttles count per key, not per connection, and servers share token counts gradually, so a client-side model is never exact. | Verified 2026-10-04 |
| The edge filter refuses with `403` and an **HTML body** that has no JSON `code` or `message`. Handle it distinctly from API `403`s. | Verified 2026-10-04 |
| The `history` bucket covers only your own fills, transactions and settled orders. There is no order-book history API. | Verified 2026-10-04 |

### Streaming

| Fact | Status |
|---|---|
| `GET /v3/ws` supports the public and private channels. Subscribe selections use `markets`/`events` maps from ID to a **single channel name**, plus a `private` channel list. Confirm simultaneous book/trades subscription behavior; use separate connections if needed. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection)); actual subscription combination unverified — R0 |
| A subscription starts with a snapshot. A market opening later under an event subscription is an exception: it starts with `OPEN`, channel sequence 0 and first delta 1. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection)) |
| Public wire channels: `book`, `trades`, `bbo`, `lifecycle`; book/trades/bbo include lifecycle. The page titled “Tape” describes the **`trades`** channel. `private` is a selection/subject for `orders` and `positions`, not the trade-tape channel; the harness does not subscribe to private data. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection), [tape](https://docs.novig.com/api/streaming/tape)) |
| Public `seq` is per market, per channel; private `seq` is per subaccount, per channel. There is no global sequence. Never compare sequences across connections. Apply each batch atomically; snapshot carries its sequence and the first subsequent delta is next. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection)); live confirmation — R0/A1 |
| `snapshot` returns authoritative state and sequence without changing subscriptions. After a reconnect, subscribe again; after a gap, take a snapshot and discard buffered deltas it covers. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection)) |
| Server sends WebSocket control Pings every 15 s and drops a client that sends no Pong between two Pings. These are transport health evidence; capture explicitly since text receive loops may hide control frames. | Verified 2026-10-05 ([connection](https://docs.novig.com/api/streaming/connection)); observed cadence — R0 |
| The endpoint page describes sequence heartbeats for private channels; that does not establish subject-level liveness for public books. The harness requires public-channel evidence or authoritative snapshot probes under DESIGN.md §7.2. | Verified 2026-10-05 ([endpoint](https://docs.novig.com/api-reference/streaming/open-the-websocket)); public evidence/probe behavior unverified — R0/A1 |
| Lifecycle status values, and whether any transition reliably marks the off. | Partly answered (Paper, three games; Production, one game): `GOLIVE` arrives before first pitch and status stays `OPEN`; no transition marks the off itself. See the observed rows below |
| `X-Novig-WS-Compress: deflate` on the upgrade request switches to compressed binary frames. Not used by the R0 recorder. | Verified 2026-10-04 |
| Stream costs: the upgrade costs 32 `stream` tokens; `subscribe` and `snapshot` cost the channel weight per market (lifecycle 1, trades 4, bbo 8, book 16); `unsubscribe` 1 per subject; `status` 1. A request above the 512 capacity passes only on a full bucket and empties it. Probing every book market at a 15 s cadence exceeds the 4/s refill beyond about three markets. | Verified 2026-10-06 ([connection](https://docs.novig.com/api/streaming/connection)) |
| Requests carry an increasing `nonce` per connection (start at 1; repeats or lower values get `STALE_NONCE`), and the snapshot reply echoes it, so probe replies associate by nonce. A frame that fails to parse or hits the throttle gets a reply with no nonce. | Verified 2026-10-06 ([connection](https://docs.novig.com/api/streaming/connection)); live behavior — R0 |
| Close reasons: `1008 SLOW_CONSUMER` (a write to the client stalled), `1008` with a 451 geolocation code (companion check failed while connected), and a close with no frame when no Pong arrives between two Pings. | Verified 2026-10-06 ([connection](https://docs.novig.com/api/streaming/connection)) |
| Lifecycle transitions documented as `OPEN`, `CLOSE`, `GRADE`, `START`, `END`, `GOLIVE`, `UNLIVE`; `GOLIVE`/`UNLIVE` can repeat on delays or reviews. Which transition, if any, reliably marks first pitch remains the R0 question above. | Verified 2026-10-06 ([lifecycle](https://docs.novig.com/api/streaming/lifecycle)) |
| **Paper, observed:** a `trading::read` key on a zero-balance subaccount (opened by `novig-provision`) can open the WebSocket and subscribe to `book` and `trades`. Production funding requirement still unverified. | Own work 2026-10-07 (Paper) — re-verify on Production |
| **Paper, observed:** control Pings every 15.0 s (range 14.87–15.15 s), about 4,400 over 3 connections and 9 h; every Pong accepted; no drops or reconnects. | Own work 2026-10-06/07 (Paper) — re-verify on Production |
| **Paper, observed:** one channel per market per connection. Subscribing `trades` for a market already on `book` replaced `book` (the `status` reply lists only `trades`). Separate connections per channel are required. A `snapshot` of a channel the connection is not subscribed to is still answered. | Own work 2026-10-06 (Paper) — re-verify on Production |
| **Paper, observed:** `seq` is per market and per channel, and `lifecycle` has its own. No gaps in about 11,800 `book` and 3,100 `trades` deltas over 9 h. | Own work 2026-10-06/07 (Paper) |
| **Paper, observed:** `snapshot` probes echo our `nonce`. All of about 5,300 probes were answered: median about 55 ms, maximum under 1 s, well inside `stream.probe_timeout_s`. Every reply's `seq` equalled the last contiguous one, confirming an unchanged channel. Quiet periods: `book` up to 152 s; `trades` silent for a whole 40 min in-game session. Three concurrent markets per channel: no probe skipped for budget. | Own work 2026-10-06/07 (Paper) |
| **Paper, observed:** `GOLIVE` arrives as `[{"kind":"GOLIVE","status":"OPEN"}]`, and status stays `OPEN`. On three games it **preceded** the MLB play-by-play first pitch by 10 s, 79 s and 123 s (first pitch 8–9 min after the scheduled time). No `START`, `CLOSE` or `UNLIVE` appeared within the capture windows. The venue transition is therefore an early bound, not the physical start (DESIGN.md §5.2). | Own work 2026-10-07 (Paper, 3 games) — re-verify on Production |
| **Paper, observed:** venue `ts` to receive time is median 36–59 ms and at most 277 ms across both sessions, far below `stream.max_venue_lag_ms`. | Own work 2026-10-06/07 (Paper) |
| **Paper, observed:** wire field names are `orderId` and `outcomeId` in `book` and `trades` (the docs show `order` and `outcome`). Trade deltas carry a `tradeId`, an execution identifier for DESIGN.md §7.4. Price strings vary in decimal places (`"0.57"`, `"0.415"`), so keep the native strings. | Own work 2026-10-06 (Paper); `orderId`, `outcomeId` and `tradeId` confirmed on Production 2026-10-08 |
| **Production, observed:** one game (CLE @ CWS, ALDS G4, `gamePk` 849832) streamed for 1.96 h on separate `book` and `trades` connections from an unfunded subaccount. Control Pings every 15.0 s (range 14.5–15.5 s, 942 in total). The connections stayed up, so our Pongs were accepted, with no drops, reconnects or close frames until the recorder closed them at the end of the window. | Own work 2026-10-08/09 (Production) |
| **Production, observed:** no sequence gaps in 78,561 `book` and 1,678 `trades` deltas; `lifecycle` has its own `seq` on each connection. All 68 `snapshot` probes were answered and matched by `nonce` (median 53–59 ms, maximum 149 ms), and every reply's `seq` equalled the last contiguous one. `book` was quiet long enough to probe only twice. | Own work 2026-10-08/09 (Production) |
| **Production, observed:** `GOLIVE` (`[{"kind":"GOLIVE","status":"OPEN"}]`, lifecycle `seq` 1) arrived at 00:08:23.3 UTC, **28.6 s before** the StatsAPI first pitch (00:08:51.9 UTC), 8.4 min after the scheduled start. Status stayed `OPEN`. No `START`, `CLOSE`, `END` or `UNLIVE` arrived in the window (to scheduled start + 90 min). As on Paper, `GOLIVE` precedes the first pitch, so it is a venue-perception bound, not the off. | Own work 2026-10-08/09 (Production, one game) |
| **Production, observed:** venue `ts` to receive time: median 35–36 ms, p99 124–163 ms, maximum 1.24 s, minimum −18 ms (receive before venue time, so the venue and local clocks differ by tens of milliseconds). | Own work 2026-10-08/09 (Production) |
| **Production, observed:** the `book` channel is order-level. A snapshot lists every resting order per outcome (`orderId`, `price`, `qty`); an order on an outcome is a bid for it and liquidity for the other outcome at 1 − price ([book](https://docs.novig.com/api/streaming/book)). Deltas are `add` (full order), `remove` with `reason` `fill` or `cancel`, and an undocumented `update` carrying `remaining`, a partial fill. On 849832: 38,557 `add`, 38,342 `remove`/`cancel`, 232 `remove`/`fill`, 1,644 `update`, and every `update` lowered `remaining`. Replaying all 78,561 deltas from the subscribe snapshot reproduced both later probe snapshots order for order. | Own work 2026-10-10 (Production, one game; `clv inspect`) |
| **Production, observed:** the unsigned public book (`/v3/public/catalog/markets/{id}/book?depth=20`) lists whole price levels with every order at each; `depth` limits levels, not orders. All 1,968 polls on 849832 showed 5–9 levels per side (39–80 orders), and the last poll matched the stream snapshot taken 20 s later level for level. Whether `depth` counts per side or in total is undocumented; the harness marks a poll incomplete if either reading could cut it. | Own work 2026-10-10 (Production, one game) |

### Prices and quantities

| Fact | Status |
|---|---|
| Native quantity is an integer contract count; each winning contract pays **$0.01**. `price` is a three-decimal probability string, so cost in USD is `price * qty * 0.01`. A displayed quantity of 110 at 0.665 costs $0.73150 and pays $1.10. | Verified 2026-10-05 ([monetary representations](https://docs.novig.com/api/concepts/money)) |
| Money/balances use five decimal places in USD; documentation specifies half-up rounding. Native price-grid increments vary by price band. Preserve strings and the contract multiplier; do not infer depth dollars directly from contract count. | Verified 2026-10-05 ([monetary representations](https://docs.novig.com/api/concepts/money)); parser/quantity fixtures — V0 |

### Fees

| Fact | Status |
|---|---|
| Takers pay a fee on each fill; game and futures markets have separate schedules. A Maker Credit Program exists. Never register Novig as zero-fee. | Verified 2026-10-04 |
| Monetary precision is $0.00001, also used for fees, with no one-cent minimum. Exact applicable fee values/schedules still need verification before fee-adjusted EV. | Precision verified 2026-10-05 ([monetary representations](https://docs.novig.com/api/concepts/money)); applicable fee schedule unverified |
| Each catalog market carries its own `fee` object: `coefficient` (`c` in the fee formula), `makerCredit` (the maker's share of the taker fee) and `charged` (`ALWAYS` or `WHEN_LIVE`). `WHEN_LIVE` charges the taker only while the event is `OPEN_INGAME`. Read it per market; never derive it from a league list. | Verified 2026-10-08 (catalog schema in the docs read 2026-10-06) |
| Every MLB market in the 2026-10-06 catalog sample, including the `MONEY` market, read `coefficient` 0.03, `makerCredit` 0.5, `charged` `WHEN_LIVE`, so a pregame fill carries no taker fee. The fee formula itself is not yet recorded here. | Own work 2026-10-06 (live public catalog, one event) — re-read per market at scoring |
| A market's fee changes during its life. The CWS `MONEY` market read `coefficient` 0.03 at 2026-10-06 23:15 UTC and 0.06 by 2026-10-07 16:58 UTC; every MLB market read through `/v3/history/markets` (127 markets, August to October) shows 0.06. Version fees by time and read them per market at the entry time. | Own work 2026-10-09 (archived catalog entries; history route) |

### Settlement

| Fact | Status |
|---|---|
| Each market declares `voids`: `PUSH` refunds every fill at its cost; `FMV` settles every outcome at a fair-market-value price. "The exchange never pushes an `FMV` market." Grades are `Winner`, `Pushes` or `FMV(price)`, and remediation can reopen a settled market for regrading. | Verified 2026-10-08 (catalog schema and event-lifecycle docs read 2026-10-06) |
| Every MLB market in the 2026-10-06 catalog sample, including `MONEY`, read `voids: FMV`. | Own work 2026-10-06 (live public catalog, one event) |
| Novig's MLB rules for when a game counts (called, suspended, postponed) and the time window for a rescheduled game. A secondary source says postponed or cancelled events void and refund, which contradicts `voids: FMV`. | Unverified — P0 (`docs/measurement-contract.md` §3) |
| **Observed settlements:** a game postponed 2026-09-22 and played 2026-09-23 (`gamePk` 824785) was settled `WIN`/`LOSS` on the played game, on both the original event and a new event Novig listed for the rescheduled game. A cancelled game (823490) was `CANCELED` and settled `FMV` at 0.522/0.478. Each doubleheader game is its own event. | Own work 2026-10-09 (history route; `docs/feasibility.md` §3) |

### Public exchange data

| Fact | Status |
|---|---|
| Daily anonymized CSVs at `data.novig.com`: `/reporting/trade-data/<date>/trades.csv` (every executed trade, **one row per side**) and `/reporting/trade-data/<date>/markets.csv` (one row per market per day: open interest, volume, OHLC). | Verified 2026-10-04 |
| The manifest `/reporting/trade-data/index.json` lists dates per file separately: `dates` for trades, `marketDates` for markets. A missing `marketDates` means an empty list. | Verified 2026-10-04 |
| Each file covers midnight to midnight Eastern and publishes around 5 a.m. ET the next day. A day's trades file is withheld if it fails validation; its markets file still publishes. | Verified 2026-10-04 |
| Columns may be added, so read header rows. Past files are immutable except announced corrections, which republish in place. | Verified 2026-10-04 |
| Earliest available date. | Own work 2026-10-09: `trades.csv` from 2026-08-03, `markets.csv` from 2026-08-04, contiguous through 2026-10-08 (`index.json`) |
| `trades.csv` columns: `timestamp, outcomeId, marketId, contractSeries, league, marketType, tradeType, legs, cost, qty, side`. `qty` is payout USD (native contracts ÷ 100) and `cost` is USD paid. There is no trade ID. Some timestamps have no milliseconds. | Own work 2026-10-09 |
| Each trade is one TAKER row plus one or more MAKER rows. All 1,876 trades streamed for `gamePk` 849832 matched exactly one MAKER row by outcome, price and quantity; TAKER rows aggregate across makers. Deduplicate on MAKER rows. `COMBO` (parlay) rows carry no league or market type and are excluded. | Own work 2026-10-09 (`docs/feasibility.md` §3) |
| The file `timestamp` lags the streamed trade's `ts` by 14 ms to 68 s (median 0.67 s) for 849832, so it is never earlier than the execution. | Own work 2026-10-09 |
| `markets.csv` OHLC is daily, in percentage points, and includes in-play trading; it cannot provide a close. Its moneyline `reportTicker` was `MLB-MONEY` in August and `MLB-WINNER` from September. Neither file names the teams or game. | Own work 2026-10-09 |
| The public catalog returns 404 `MARKET_NOT_FOUND` for closed markets. `GET /v3/history/markets/{id}` and `/v3/history/events/{id}` (read key, `history` bucket) return any market or event, including outcome names, grades, `settledTs`, `voids`, `fee`, the event description and `startsTs`. | Own work 2026-10-09 (127 markets resolved) |
| Novig team naming differs from MLB's: `Oakland Athletics`/`OAK`, `KAN`, `ARI`, `WAS`. Event `startsTs` equalled StatsAPI's scheduled `gameDate` in the sample, except a traditional doubleheader's game 2 (Novig's own estimate). | Own work 2026-10-09 |
| Requests from Python's `urllib` to `api.novig.com` get the edge's HTML 403 even with a User-Agent; `aiohttp` and `curl` pass. | Own work 2026-10-09 |
| Unsigned public REST under `/v3/public/...`: catalog events, markets, single market, and order book (`/v3/public/catalog/markets/{id}/book`, `depth` 1–20, with `seq` and an ETag), throttled per IP at the edge. Live on 2026-10-06 the book response carried `cache-control: max-age=5`. | Verified 2026-10-06 (docs and live call) |
| MLB moneylines are one `MONEY` market per game with two outcomes named by team abbreviation; `startsTs` matched the StatsAPI `gameDate` for every postseason game checked. | Own work 2026-10-06 (live public catalog) |

### API history and third parties

| Fact | Status |
|---|---|
| The older NBX API (OAuth 2.0, REST, GraphQL) is documented under a deprecated section. Build against v3 only. | Verified 2026-10-04 |
| Several resellers sell Novig data, and some claim Novig has no usable API. That is false for v3 and was false for NBX. Harness policy: resellers are never a reference source. Their history is their own snapshots at their own capture cadence, with provenance the harness cannot audit. | Verified (claim false); policy is ours |

---

## The Odds API (`the-odds-api.com`) — entry quotes

| Fact | Status |
|---|---|
| Plans as of 2026-10: Starter free (500 credits/month), 20K $30, 100K $59, 5M $119, 15M $249. | Reported 2026-10-04 — recheck immediately before any purchase |
| Odds-call cost = markets × regions. `h2h` in `us` costs 1 credit per call and returns every event in the sport. Historical calls cost 10 × markets × regions. | Reported 2026-10-04 |
| Historical featured-market snapshots run from June 2020 at 10-minute intervals and 5-minute intervals from September 2022; a request returns the closest snapshot at or before the requested time. Snapshot spacing is not bookmaker update frequency: keep per-book `last_update`. | Reported |
| The documented in-play test treats an event as in-play once `commence_time` is earlier than now: scheduled time as a proxy. Not usable as an off source. | Reported |
| On the scores endpoint, `last_update` is null and `scores` is empty until the event starts; live scores update roughly every 30 s. | Reported |
| `commence_time` may be backfilled to actual start: a vendor sample shows `2024-08-20T22:41:00Z`, which is not a scheduled slot. Snapshot it at collection, re-read at scoring, and compare across historical snapshots. | Unverified — A4/B3 |
| Scores-endpoint credit cost. The cost model assumes about 1 credit per call. | Unverified — A4 |
| Quota headers `x-requests-remaining` and `x-requests-used`. | Reported (third-party source) — verify in A4 |
| Responses carry `x-requests-remaining`, `x-requests-used` and `x-requests-last`. `GET /v4/sports` cost 0 credits; `h2h`/`us` odds for MLB cost 1 credit per call (21 calls on 2026-10-07: 500 → 479). | Own work 2026-10-07 |
| Monthly credits reset on a calendar boundary, possibly independent of the billing date. | Reported — check before timing a one-month purchase |
| Coverage is mainstream US soft books, not Pinnacle or Betfair Exchange. | Reported |
| MLB `h2h`/`us` responses on 2026-10-07 carried nine books: FanDuel, DraftKings, BetMGM and BetRivers (state-licensed), plus Bovada, MyBookie.ag, BetOnline.ag, LowVig.ag and BetUS (offshore). Prices are integer American odds with per-book and per-market `last_update` at one-second resolution. | Own work 2026-10-07 (archived responses) |
| Licensed-book MLB moneyline rules: the bet is "action", standing whatever the starting pitchers; it stands once the game is official (5 innings, or 4½ with the home team leading); a game postponed before first pitch voids; a suspended game resumed within 36 h (DraftKings, FanDuel, BetMGM) or 48 h (Fanatics) stands. Rules for BetRivers and for the offshore books, including whether the offshore default is listed pitchers, are not checked. | Reported 2026-10-08 ([Action Network, 2025-04-24](https://www.actionnetwork.com/mlb/suspended-shortened-mlb-betting-rules)) — verify from each book's house rules in P0 |
| `theoddsapi.com` is a different product. Confirm which vendor any credential or doc page belongs to. | Reported 2026-10-04 (search results, not vendor docs) |

---

## Kalshi — cross-check and historical reference candidate

| Fact | Status |
|---|---|
| A public market-data connector exists in the edge scanner; R0 adapts it to poll game-winner order books. | Own work |
| Sports event contracts carry explicit fees; the edge scanner holds a fee formula checked in prior work. | Own work |
| Public market-data endpoints need no authentication; rate limits apply (a third-party guide cites about 10 requests per second). | Reported — confirm in R0 |
| Unauthenticated `GET /markets/{ticker}/orderbook` every 10 s for up to 6 tickers (9,720 calls on 2026-10-07): every response was 200, with no 429s. | Own work 2026-10-07 |
| `orderbook_fp` holds `yes_dollars` and `no_dollars`: resting YES bids and NO bids as `[price, quantity]` string pairs, lowest price first. Prices have four decimals on a one-cent grid (`linear_cent`); quantities are fractional contracts (`_fp`), each paying the market's `notional_value_dollars` (1.0000 for `KXMLBGAME`). A NO bid at p is a YES ask at 1 − p. With no `depth` parameter the whole book is returned. | Own work 2026-10-10 (849832 archive) |
| The single-market order-book response has an `orderbook_fp` object with YES/NO price/quantity arrays and does not include the requested ticker. Archive the request path/ticker with a request ID; the response body alone cannot establish market identity. | Verified 2026-10-05 ([order-book endpoint](https://docs.kalshi.com/api-reference/market/get-market-orderbook)); archive-envelope fixture — R0 |
| Shape of the history endpoints (bid/ask candles, trades or both), their resolution, and coverage of 2026 MLB game-winner markets. | Own work 2026-10-09: both. 1-minute candles carry YES bid/ask OHLC and trade-price OHLC; trades carry `trade_id`, `created_time` (µs), `count_fp`, prices and taker side. `KXMLBGAME` covers 2025-04-16 onward (4,673 events). Quiet minutes have no candle (a 2025 game: 147 candles in 360 minutes). See `docs/feasibility.md` §4 |
| Data splits into live and historical tiers at `GET /historical/cutoff` (2026-08-10 for markets and trades when read 2026-10-09). Older markets, candles and trades come only from `/historical/…`, whose field names differ (`close`, `volume`, `open_interest` versus `close_dollars`, `volume_fp`, `open_interest_fp`). | Verified 2026-10-09 ([historical data](https://docs.kalshi.com/getting_started/historical_data.md)) and own work |
| Ticker dates and times are not game identity. `occurrence_datetime` equals the ticker time read as Pacific, several tickers are 3 h off StatsAPI, and 2025 tickers have no time. `…26SEP261915BALNYY` settled on the 2026-09-25 doubleheader game 1, and `…26SEP261915CHCBOS` on the 2026-09-27 game. Map from settlement result and `close_time`, by hand. | Own work 2026-10-09 (`docs/feasibility.md` §4) |
| Observed settlements: a game postponed and played the next day settled on the played game (824785), and a cancelled game settled `result: scalar` at 0.47/0.53 (823490). | Own work 2026-10-09 |
| MLB game-winner markets are series `KXMLBGAME`, one YES/NO market per team per game; the event ticker encodes the scheduled start in Eastern time and both teams (`KXMLBGAME-26OCT071800LADATL`). The order-book endpoint accepts `depth`. | Own work 2026-10-06 (live public API) |
| `KXMLBGAME` rules: YES if the named team wins the game "originally scheduled" for the encoded date and time. A postponed or delayed game keeps the market open until the rescheduled game finishes, within two days. A game cancelled, or rescheduled more than two days away, resolves "to a fair price". The market closes early once a winner is declared (`can_close_early`, `settlement_timer_seconds` 120). Price structure `linear_cent`. | Own work 2026-10-07 (`rules_primary`/`rules_secondary` in archived market responses) |

---

## MLB StatsAPI — authoritative MLB start

| Fact | Status |
|---|---|
| Free; unofficial and undocumented. | Reported |
| Game status progresses Scheduled → Warmup → In Progress → Final. | Reported (client-library docs) |
| Play-by-play timestamps every pitch with `startTime`; the first pitch's `startTime` is the actual off and is available retroactively. | Own work 2026-10-09: present to the millisecond for all 23 played games in the B0 sample, and unchanged for 849832 between the in-game feed and the final play-by-play. Longer-horizon corrections untested |
| The first play's `playEvents` open with the pregame status changes as `action` events, each with `startTime` and `endTime`: `Status Change - Pre-Game`, `Warmup`, `In Progress`, then the first pitch. On 849832: Warmup 23:43:15.2, In Progress 00:08:05.0, first pitch 00:08:51.9 UTC. The In Progress change preceded Novig's `GOLIVE` (00:08:23.2) and the first pitch, so it is the earliest official start claim. | Own work 2026-10-10 (one game, final play-by-play) |
| `gamePk` is the stable game ID; doubleheader games have distinct `gamePk` values. | Own work 2026-10-09: confirmed on split and traditional doubleheaders. A postponed game keeps its `gamePk` and appears twice in the schedule: a `Postponed` row (`reason`, `rescheduleDate`) and a `Final` row (`rescheduledFrom`). A traditional game 2's `gameDate` is a placeholder |

---

## Ruled out

| Source | Reason |
|---|---|
| Betfair Exchange | Live API and historical archive both require an account US residents cannot open. |
| Pinnacle | Public API closed in 2025. |
| Sportsbook internal endpoints | Scraping violates terms of service and breaks without notice. |

---

## Access context

| Fact | Status |
|---|---|
| The operator is in Indiana. The Indiana Gaming Commission has stated that prediction markets fall under federal (CFTC) jurisdiction. | Reported (news, 2026) |
| The federal picture is unsettled: the 3rd Circuit ruled for Kalshi in April 2026, the 9th Circuit against it on Nevada in August 2026, and CFTC rulemaking is pending. Revalidate access assumptions whenever the operating location or venue set changes. | Reported (news, 2026) |

---

## Discrepancy log

| Date | Entry | Expected | Observed | Action |
|---|---|---|---|---|
| 2026-10-05 | Novig wire channels | Table named `tape` and `private` as channels | Tape page specifies `trades`; `private` selects orders/positions; connection page also lists `bbo` | Corrected names; R0 checks subscription acknowledgements and simultaneous feeds |
| 2026-10-05 | Novig sequencing and heartbeat | Scope/cadence left wholly unverified | Docs specify per-market/per-channel sequence, no cross-connection comparisons and 15 s transport Pings | Recorded documented contract; retain live confirmation and public-channel probe gate |
| 2026-10-05 | Novig subscription snapshots | Every new covered market assumed to arrive with a snapshot | Newly opened markets under event subscriptions start with OPEN/sequence initialization | Recorded exception for A1 reconstruction fixtures |
| 2026-10-05 | Novig native monetary units | Contract payout and precision unresolved | Native payout $0.01, three-place probabilities and five-place USD money | Recorded multiplier/precision; depth target is payout USD under DESIGN.md §2.4 |
| 2026-10-05 | Kalshi archive identity | REST body plus status/quota metadata sufficient | Documented single-market book body omits ticker | Require sanitized request envelope, request ID and explicit response/failure association |
| 2026-10-06 | Novig key creation | Self-serve: Profile → Settings → Novig API creates the management key (api-keys page) | No Novig API entry on our Production account; the web app gates it behind the `novig-api` feature flag (always shown on Paper); docs examples default to the Paper host | Ask developers@novig.com to enable read-only access; R0 records Production via the unsigned public book poll meanwhile; Paper key for signer/stream checks; Kalshi as primary is the §3.2 fallback if access is refused |
| 2026-10-07 | Novig wire field names (Paper) | `order` / `outcome` in book and tape payloads | `orderId` / `outcomeId`, plus an undocumented `tradeId` on each trade | Parsers use the observed names; `tradeId` is the candidate `execution_id`; re-verify on Production |
| 2026-10-07 | Novig lifecycle delta shape (Paper) | `"deltas": ["GOLIVE"]` | `"deltas": [{"kind": "GOLIVE", "status": "OPEN"}]` | Parse the object form; re-verify on Production |
| 2026-10-08 | Novig Production API access | LP onboarding (about $30,000 minimum deposit) is the only documented route to Production API access | On request to developers@novig.com, Novig enabled Production key creation on our account, with no minimum balance for read-only use | Provision a `trading::read` key with `novig-provision`; R0 re-verifies the Paper findings on Production |
| 2026-10-09 | Novig MLB fee coefficient | 0.03 on every MLB market (2026-10-06 catalog sample, logged above) | The same markets read 0.06 from 2026-10-07; history shows 0.06 for every sampled market | Fees are versioned by time and read per market; the 0.03 row stays as the dated observation it was |
| 2026-10-10 | Novig book delta kinds (Production) | `add`, and `remove` with `reason` `fill` or `cancel` ([book](https://docs.novig.com/api/streaming/book)) | Also `{"kind": "update", "orderId", "remaining"}` for a partial fill: 1,644 on 849832, each lowering `remaining` | The replay applies `update` and refuses any other unknown kind, or an `update` that does not lower `remaining`; ask Novig to document it |
