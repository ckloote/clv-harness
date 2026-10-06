# Vendor Capabilities

Facts the harness depends on, each with a verification status. Implement against **Verified** entries and re-verify them at first contact. Treat **Reported** entries as leads and **Unverified** entries as open questions with an owning phase. When something turns out different, add a dated row to the discrepancy log at the bottom — never silently edit an entry.

- **Verified** — read in the vendor's own documentation on the stated date.
- **Reported** — secondary source, search snippet or vendor statement not yet tested by us.
- **Unverified** — an assumption the plan depends on; the owning phase must test it.
- **Own work** — established by our own code or observation, not by vendor documentation.

---

## Novig (v3 API) — primary reference

Source: `docs.novig.com`, read 2026-10-04.

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
| Harness read key: ID and creation date. The management private key is never stored on the Pi. | Own work — fill in at R0 provisioning |
| Whether a subaccount must be funded before its `trading::read` key can stream. Opening and funding are separate routes. | Unverified — R0 |
| The docs include an LP-onboarding page about API access for liquidity providers. Whether it gates anything relevant to read-only access. | Unverified — R0 |

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
| One websocket at `GET /v3/ws` carries every channel. Subscribe messages carry a nonce and select `markets`, `events` (which also covers markets that open later) or `private`. | Verified 2026-10-04 |
| Each subscription starts with a snapshot. | Verified 2026-10-04 |
| Channels: `book` (every order-book change; includes lifecycle), `lifecycle` (market status transitions), `tape` (every execution), `private` (own orders and positions; unused). | Verified 2026-10-04 |
| Messages carry sequence numbers, and the connection docs define gap handling. Read that page in full before A1. | Verified (existence) |
| Sequence scope: per connection or per subject. | Unverified — R0 |
| Heartbeat interval and liveness expectations. | Unverified — R0 |
| Lifecycle status values, and whether any transition reliably marks the off. | Unverified — R0 |
| `X-Novig-WS-Compress: deflate` on the upgrade request switches to compressed binary frames. Not used by the R0 recorder. | Verified 2026-10-04 |

### Fees

| Fact | Status |
|---|---|
| Takers pay a fee on each fill; game and futures markets have separate schedules. A Maker Credit Program exists. Never register Novig as zero-fee. | Verified 2026-10-04 |
| Exact fee values and their precision. | Unverified — before any fee-adjusted EV |

### Public exchange data

| Fact | Status |
|---|---|
| Daily anonymized CSVs at `data.novig.com`: `/reporting/trade-data/<date>/trades.csv` (every executed trade, **one row per side**) and `/reporting/trade-data/<date>/markets.csv` (one row per market per day: open interest, volume, OHLC). | Verified 2026-10-04 |
| The manifest `/reporting/trade-data/index.json` lists dates per file separately: `dates` for trades, `marketDates` for markets. A missing `marketDates` means an empty list. | Verified 2026-10-04 |
| Each file covers midnight to midnight Eastern and publishes around 5 a.m. ET the next day. A day's trades file is withheld if it fails validation; its markets file still publishes. | Verified 2026-10-04 |
| Columns may be added, so read header rows. Past files are immutable except announced corrections, which republish in place. | Verified 2026-10-04 |
| Earliest available date. | Unverified — P0/B0 |

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
| Monthly credits reset on a calendar boundary, possibly independent of the billing date. | Reported — check before timing a one-month purchase |
| Coverage is mainstream US soft books, not Pinnacle or Betfair Exchange. | Reported |
| `theoddsapi.com` is a different product. Confirm which vendor any credential or doc page belongs to. | Reported 2026-10-04 (search results, not vendor docs) |

---

## Kalshi — cross-check and historical reference candidate

| Fact | Status |
|---|---|
| A public market-data connector exists in the edge scanner; R0 adapts it to poll game-winner order books. | Own work |
| Sports event contracts carry explicit fees; the edge scanner holds a fee formula checked in prior work. | Own work |
| Public market-data endpoints need no authentication; rate limits apply (a third-party guide cites about 10 requests per second). | Reported — confirm in R0 |
| Shape of the history endpoints (bid/ask candles, trades or both), their resolution, and coverage of 2026 MLB game-winner markets. | Unverified — P0/B0 |

---

## MLB StatsAPI — authoritative MLB start

| Fact | Status |
|---|---|
| Free; unofficial and undocumented. | Reported |
| Game status progresses Scheduled → Warmup → In Progress → Final. | Reported (client-library docs) |
| Play-by-play timestamps every pitch with `startTime`; the first pitch's `startTime` is the actual off and is available retroactively. | Reported — verify semantics and corrections in P0 |
| `gamePk` is the stable game ID; doubleheader games have distinct `gamePk` values. | Reported — verify on a real doubleheader in P0 |

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
| — | — | — | — | — |
