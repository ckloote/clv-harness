# CLV Harness — Design

**Status:** proposed; unimplemented  
**Date:** 2026-10-05

**Companion documents**

| Document | Role |
|---|---|
| `docs/vendor-capabilities.md` | Vendor facts with verification status and date. Implement against them, re-verify at first contact, log discrepancies there. |
| `docs/RESEARCH_PROTOCOL.md` | Model-evaluation protocol. Inactive until a signal model exists. The harness preserves the evidence it needs (§11) but does not implement it. |
| `config/params.toml` | Every tunable parameter. Created in V0 from §10, authoritative thereafter. |

**Code that ships with this design:** `src/clv/score/controls.py` and `tests/test_calibration_simulation.py` (§9.2, §9.4), plus `tests/test_research_bound_specification.py` (arithmetic fixtures for the inactive research protocol).

**Schema:** grown in stages (§8.5): the V0 table set first, then each table when the phase that writes to it begins, each stage with its §8.4 tests. Do not write the complete DDL up front.

**Maintaining this document:** it describes the current design only. Decisions and their reasons go in `docs/decisions/` as dated records; superseded text here is replaced, never annotated with its history.

---

## 1. Purpose, claims, and boundaries

The CLV Harness is measurement infrastructure for evaluating hypothetical and real sports-betting entries against an independently observed pre-event market benchmark. It is deliberately built **before** a betting model. Its job is to make a proposed edge testable and to expose when apparent edge depends on timestamp errors, stale prices, contract mismatches, illiquid reference markets, or research choices.

**Primary research question:** For a prespecified population of eligible entries, did the entry obtain a better price than a defensible estimate of the *same outcome's* pre-event fair probability, and is that finding robust to reasonable benchmark and timing definitions?

### 1.1 What a positive result establishes

1. **Positive CLV:** the observed entry price compares favorably with a specified later reference probability. This is a useful *leading indicator* of potential edge, not proof of profitability.
2. **Robust positive CLV:** the result survives defensible choices of venue, close cutoff, book depth and analysis cohort, with uncertainty quantified at the event level. This is stronger evidence, but still rests on market-efficiency and data-quality assumptions.
3. **Executable, fee-adjusted advantage:** the result survives an explicit entry-timing/execution model and applicable fees. This is a separate claim; a historical quote alone cannot establish executability.
4. **Outcome validation:** on a later chronological holdout, probabilities and actual outcomes are consistent with the claimed advantage. This is corroboration, not a substitute for CLV, and requires substantially more observations.

Do **not** write that CLV is the only ground truth or that failure to beat one potentially inefficient exchange close conclusively disproves a model. The harness evaluates a model *relative to a named benchmark and eligibility policy*.

**Design principle:** an instrument that has not been calibrated and stress-tested against known faults is not a trustworthy instrument.

### 1.2 Non-goals

- No automated order placement, bankroll management or staking strategy. The harness uses read-only venue credentials.
- No low-latency arbitrage execution engine.
- No signal-generation model. The harness accepts and preserves all candidate signals, including rejected candidates.
- No full operational P&L product in v1. Actual settlements and fills can support later outcome validation, but the central research output remains CLV with explicit assumptions.
- No spread-to-moneyline conversion or cross-line spread scoring in v1. Those require a separately validated margin distribution.

### 1.3 Scope and sequencing

**v1 scope:** two-outcome, pre-event moneylines on properly matched contracts, starting with an MLB historical feasibility cohort and live sports available during development. Start with one league and one venue-to-reference pair. Add totals only where the line and settlement rules are identical; add spreads only after separate validation of any distribution-based conversion.

**Do not conflate three datasets:** live book-based closes (`close_live_*`), historical trade/candle closes (`close_hist_*`), and actual executions. They have different observation and selection properties and must be reported separately.

Model evaluation — preregistered cohorts, chronological holdouts, multiple-testing control, outcome corroboration — is governed by `docs/RESEARCH_PROTOCOL.md`. The harness's obligation is to preserve the evidence that protocol needs (§11), not to implement it.

---

## 2. Measurement contract

### 2.1 Unit of analysis and comparability

The atomic comparison is **one side of one precisely defined settlement contract** at a specific entry time, measured against the same side of the same contract at a later, strictly pre-event close. Canonical event/team names alone are insufficient. Record overtime rules, postponement/void rules, listed-pitcher conditions where relevant, treatment of abandoned games, and other material contract terms.

Examples:

- A US sportsbook's MLB moneyline and an exchange contract are comparable only if their settlement conditions agree or differences are explicitly modeled and the affected cases are excluded from the primary cohort.
- `-3` and `-3.5` are **different outcomes**. A price movement between them cannot be expressed as one model-free CLV number.
- A YES price for “Team A wins” and a NO price for its exact complement can be normalized to the same outcome **only after** correctly swapping bid/ask and depth and checking the venue's contract rules.

Each mapping has an explicit `eligible`, `quarantined`, or `ineligible` status and a reason. Fuzzy mappings cannot enter the primary cohort until independently verified.

### 2.2 Primary metric

For an entry offering exact decimal payout factor `d_entry` per unit of stake at risk, and a named reference fair-probability estimate `p_close` for that identical outcome:

```text
clv_ev = p_close * d_entry - 1
```

Display as `100 * clv_ev` percentage points; store the metric with its declared units. Preserve the original native quote and derive `d_entry` from **that quote**, not from a rounded `entry_prob`. For a simple fully collateralized YES share costing `q` per $1 payout, `d_entry = 1/q` before fees. Do not reuse this relationship for instruments with different payoff or collateral conventions.

A soft-book entry at -110 compared with an unchanged -110 *offered* close generally has negative CLV once the close is de-vigged. For an exchange, the spread-adjusted midpoint is a *candidate benchmark*, not an automatically efficient or unbiased fair probability.

### 2.3 Separate estimands and columns

| Quantity | Meaning | Formula / source | Interpretation |
|---|---|---|---|
| `clv_ev` | Fee-free comparison with close benchmark | `p_close * d_entry - 1` | Relative price advantage versus named benchmark |
| `null_ev` | Comparison with the same reference at entry | `p_ref_entry * d_entry - 1` | Baseline for the exact entry quote |
| `clv_residual` | Change in reference-implied EV | `(p_close - p_ref_entry) * d_entry` | Used for diagnostic controls; not universally zero by assumption |
| `fee_adjusted_close_ev` | Hypothetical EV after modeled costs | Explicit stake/payout/fee function | **Estimate**, not realized profit |
| `realized_roi` | Actual result after settlement and real fills | Real settlement/fill ledger, if available | Later validation; never substitute for CLV |

Name the column `fee_adjusted_close_ev`, not a generic "net EV": it is a modeled estimate, not realized profit. Version every fee schedule by venue, product, side and effective interval; retain the exact fee components. Exchange fees can require sub-cent precision, so use an explicitly scaled fixed-point representation or decimal arithmetic until final reporting. Never assume a fee-free venue because its displayed price lacks embedded bookmaker vig.

### 2.4 Fair-probability benchmarks and sensitivity

For each eligible live close compute, when the data permit:

1. Unweighted best bid/ask midpoint (`mid_top`).
2. Depth-walk midpoint at **prespecified payout notionals**, initially `$100`, `$500`, and `$1,000` (`mid_depth_*`). The $500 result is a candidate *primary* benchmark, subject to an early liquidity pilot, not an immutable truth.
3. The executable acquisition price for the measured side and the executable disposal price where those operations are defined. These bound what the market actually offers; they are not equivalent to a midpoint probability.
4. Aligned quotes from the independent reference venue, reported separately with cross-venue difference and time alignment.

If insufficient depth exists on either side at a required notional, that benchmark is unavailable with reason `insufficient_depth`; never extrapolate through an empty book. If the bid exceeds the ask, status is inappropriate, or price/depth is internally inconsistent, quarantine the observation.

**Primary estimate selection is preregistered** after an initial liquidity/coverage pilot and *before* examining candidate-model CLV. Preserve every alternative for sensitivity analyses; do not switch to the benchmark that produces the best result. Starting notionals and the provisional primary benchmark are in §10.

**Depth measurement contract.** Normalize each level's quantity to USD paid if the contract wins: `level_payout_usd = native_qty * payout_usd_per_native_contract`. Retain both the native quantity and the versioned contract multiplier. Novig documents a $0.01 native payout; do not assume every venue's contract pays $1. Canonical bids are walked highest price first and asks lowest first. For a target payout notional `N`, consume exactly `N` on **each** side, including a proportional fraction of the final level for this benchmark calculation:

```text
side_vwap = sum(price_j * consumed_payout_usd_j) / N
mid_depth_N = (bid_vwap_N + ask_vwap_N) / 2
```

Require at least `N` of actual payout depth on both sides; never renormalize a short ladder to `N`. A fractional final level is an integration convention for the benchmark, not a claim that a fractional native contract can be executed. Cash-budget acquisition/disposal estimates use separate execution assumptions and native size increments. Polarity normalization precedes the walk: reversing a complete complementary ladder gives `mid_complement_N = 1 - mid_N` at the same payout notional.

**V0 worked fixture:** with canonical bid levels `(0.40, $300), (0.35, $400)` and ask levels `(0.60, $200), (0.65, $500)`, where quantities are payout dollars, `N = $500` consumes $300/$200 on the bid and $200/$300 on the ask. Bid VWAP is 0.38, ask VWAP 0.63, midpoint 0.505; the complemented midpoint is 0.495. The $500 target corresponds to 50,000 native contracts at a $0.01 payout. Cash proceeds/cost are $190/$315 before fees, so this is not a $500 cash-budget trade. At `N = $1,000` both ladders are insufficient. V0 tests must reproduce these values, unit conversion, partial final levels and complement identity.

### 2.5 De-vigging

Exchange midpoint references normally use `devig_method = none`: applying bookmaker de-vig a second time is inappropriate. A midpoint can still be biased by spread asymmetry, thin liquidity and informed-flow effects; those are tested with benchmark sensitivity and outcomes.

For references with embedded margin, implement `proportional`, `shin` and `power` **later**, with fixtures and numerical tolerances. Their validity depends on market type and assumptions; no method is universally correct. Keep their results separate and do not block the first exchange-only vertical slice on all three implementations.

---

## 3. Venues, availability and dependency risk

### 3.1 Intended venue roles

| Source | Intended role | Key uncertainty / gate |
|---|---|---|
| Novig v3 | Primary live reference: streamed book, lifecycle, tape | Read-only access, stream sequencing, venue timestamps, market depth and access continuity |
| Kalshi | Independent live cross-check; possible historical reference | Historical coverage and candle semantics for the exact sports contracts; liquidity and settlement comparability |
| The Odds API (`the-odds-api.com`) | Soft-book entry quotes; paid historical snapshots | Per-book quote age, event-ID changes, historical availability and quota budget |
| MLB StatsAPI | MLB schedule/status and retrospective first-pitch observations | Completeness, timestamp semantics and corrections for the chosen seasons |
| Novig public reports | Historical trades / daily market aggregates, **not** historical order books | Actual date coverage and whether both trade sides can be reconciled without double-counting |

Keep the reference venue configurable. No other provider is a prerequisite for v1. No aggregate exchange-volume statistic substitutes for measured depth and freshness in the actual instrument being scored.

Vendor features, access policies, fee tables, historical coverage, pricing and legal availability can change. Treat the facts in `docs/vendor-capabilities.md` as **working assumptions** with stated verification status, and confirm each before the gate that depends on it. Do not assume an access check that passes today will keep passing. Respect all location and access restrictions and log access failure as missing data rather than working around it.

### 3.2 Novig integration contract

Novig integration is v3-only and read-only, subject to confirmation at first contact:

- Dedicated `trading::read` credential only; management/trading credentials never loaded by the harness.
- Signed REST setup with published signature fixtures and a successful authenticated echo test.
- One or more documented WebSocket channels for book, lifecycle and trades; use the vendor's **actual** sequence scope and resynchronization protocol, not an assumed per-instrument sequence.
- Snapshot followed by sequenced deltas, with atomic snapshot replacement after any unrepairable gap.
- Startup discovery of actual limits and supported channels; handle `429`, access refusal and HTML/non-JSON error bodies as first-class operational events.
- Confirm whether streaming requires a funded subaccount, how lifecycle statuses map to market state and whether the public trade tape identifies executions across live and daily data.

Record which capabilities were actually verified, when, and with which API/docs version. If any critical capability is missing, stop that part of the plan and evaluate Kalshi as primary instead of manufacturing continuity assumptions.

**Already-verified capabilities.** The key model and creation path, signing scheme and test vectors, throttle buckets and the 2,048-market subscription cap, location-check semantics, the HTML-body edge `403`, fee structure and public-data layout are recorded in `docs/vendor-capabilities.md`. The wire-channel names (`trades`, not `tape`), per-market/per-channel sequencing, 15-second transport Pings and native payout units were checked in the 2026-10-05 correction pass. Implement against the dated entries and re-verify them at first contact (R0, A1). Do not rediscover them from scratch, and do not trust them blindly either: any discrepancy becomes a dated entry in that file's log.

### 3.3 Historical-data cost assumptions

The Odds API's published historical odds interface returns the closest snapshot **at or before** the requested timestamp; featured-market history has five-minute snapshot intervals from September 2022, with a stated historical call cost of ten credits per market per region. Five-minute *snapshot* spacing does **not** mean that every underlying bookmaker updated its quote every five minutes. Preserve each bookmaker's reported last-update time where available.

Planning estimates, not authorized purchases:

| Workload | Planning estimate | Decision rule |
|---|---:|---|
| Live MLB soft-book polling every five minutes | ~8,600 credits/month | Size for ≤60–70% planned quota usage |
| Live NBA + NHL plus relevant scores checks | ~35,000 credits/month | Recalculate from actual coverage and endpoint cost |
| Full 2026 MLB historical five-minute backfill | ~270,000 credits once | Purchase **only after** B1 and B2 pass, and only for windows that passed (§13) |
| Lean first year | ~$330 plus storage | Conditional on quoted plans, season window and actual consumption |
| Continuous multi-sport first year | ~$770 plus storage | Reprice before committing |

The quoted historical costs and subscription prices must be checked immediately before purchase. A partial, well-characterized historical season is acceptable for pipeline calibration; it is not interchangeable with full-season coverage.

**References to recheck before implementation:** [Novig API documentation](https://docs.novig.com/), [Kalshi API documentation](https://docs.kalshi.com/), [The Odds API historical data](https://the-odds-api.com/historical-odds-data/), [The Odds API v4 documentation](https://the-odds-api.com/liveapi/guides/v4/), and [MLB StatsAPI](https://statsapi.mlb.com/).

---

## 4. Identity and contract equivalence

### 4.1 Canonical event identity

**Never derive the primary key solely from `date:AWAY@HOME`.** MLB doubleheaders share that tuple; postponements and schedule corrections change dates; providers may issue new IDs after major changes.

Use a generated stable internal `event_id` and an append-only `event_alias` observation stream (`provider`, `provider_event_id`, `first_seen`, `last_observed`, raw title and schedule). Where available, attach the league's authoritative stable game ID (e.g., MLB gamePk) and doubleheader/game number as attributes. A reconciliation process proposes alias merges; approved corrections are recorded as new facts with provenance. Do not rewrite the original observations or silently merge uncertain events.

Represent scheduled-start updates in `event_schedule_observation` rather than mutating `event.scheduled_start_ts`. Store all known timestamps in UTC and preserve the native strings and source time zones in raw observations. Schedule is context, **never** the basis for a trusted close.

### 4.2 Versioned market mapping

`outcome` and `venue_instrument` hold stable identity only. Mutable interpretation lives in append-only `instrument_mapping_observation` records and a derived, versioned mapping view. Each mapping records:

- Venue and native instrument ID, source event alias, canonical event/outcome IDs, polarity and effective/observed times.
- Native contract text and normalized rule attributes, including void/postponement rules, overtime, listed-pitcher conditions and result authority.
- `exact`, `manual_verified`, `fuzzy_candidate`, or `rejected` status; mapping method, confidence, reviewer or test evidence, and superseded mapping ID if corrected.
- An explicit `settlement_equivalence` verdict and exclusions for ambiguous terms.

**Primary-cohort eligibility:** only exact or independently verified manual mappings with confirmed rule equivalence. Fuzzy matches enter quarantine. Emit an audit report of all excluded/unmapped instruments; never silently discard them from coverage denominators.

### 4.3 Polarity normalization

Define the canonical probability as `P(outcome = TRUE)`. For a complement-quoted instrument, `p_canonical = 1 - p_native`; **bid and ask swap** (`bid_canonical = 1 - ask_native`, `ask_canonical = 1 - bid_native`), and depth follows the side being transformed. Apply this transformation consistently to the entire ladder, executed trades and settlement. Retain the raw native message so polarity bugs can be repaired without re-collection.

Tests must round-trip both directions, including asymmetric spreads, unequal side depth and partially filled ladders. Require paired HOME/AWAY or YES/NO consistency only when the contracts are exact complements; do not impose artificial `p_home + p_away = 1` on non-equivalent or asynchronously sampled books.

---

## 5. Time, off resolution and closing-price definitions

### 5.1 Four times that must remain distinct

1. **Venue event time:** when the provider says a quote/trade/status changed.
2. **Observed time:** when this collector actually received it. Use a high-resolution UTC timestamp (milliseconds or finer where the native feed warrants it); preserve the raw native timestamp.
3. **Decision time:** when the model emitted a signal and when a simulated/real entry decision could have used the data.
4. **Actual event start interval:** best available range for the earliest plausible beginning of realized game information, resolved from multiple independent observations.

For historical data, retrieval time is *not* the historical observation time. Label a quote's snapshot time, underlying bookmaker update time where provided, and retrospective retrieval time separately. Later downloading a historical record cannot prove a bettor saw it when it was first published.

### 5.2 Off observations and resolver

Retain every `off_observation` independently: authoritative MLB PBP first pitch when obtainable, official status transitions, scores changes, and venue lifecycle changes. Store `detected_off_ts`, `observed_ts`, uncertainty and the raw source payload. Add a versioned derived `off_resolution` containing:

```text
off_resolution_id, resolver_version, event_id,
selected_start_ts, earliest_plausible_start_ts, latest_plausible_start_ts,
source_set, source_disagreement_s, confidence, reason, computed_ts
```

For a polled transition between a last confirmed pregame observation at `t0` and first in-progress observation at `t1`, the defensible start range is **[t0, t1]**, not a falsely exact `t1`. A reliable retrospective first-pitch timestamp may narrow that interval, but its source semantics and corrections must be tested. A venue lifecycle transition marks the venue's own perception, not necessarily the physical first pitch. Unresolved conflicts widen uncertainty or make the event unscoreable.

The conservative close boundary is derived from the **earliest credible** start bound across accepted sources. Do not select whichever source produces the latest possible close. Keep a reported “venue transition” alternative for research on when the market itself began responding to in-play information. A scheduled time alone results in `no_trusted_off` and is excluded from the primary cohort.

### 5.3 Versioned close function

A close is a deterministic, versioned computation over an immutable fact snapshot and a specified off resolution:

```text
close_definition + resolver_version + fact_snapshot_id + outcome_id
    -> close_result OR explicit_unscoreable_reason

cutoff = earliest_plausible_start_ts - buffer_s
book   = latest complete trustworthy book observed at or before cutoff
require: book is pre-cutoff; feed continuity holds; book age <= max_age_s
price  = benchmark_function(book, required_notional)
```

The 60-second buffer and $500 depth walk are **pilot parameters**, not validated constants. A starting operational profile may use 60 seconds, multiple sensitivity cutoffs at `{0, 30, 60, 120, 600}` seconds, and a conservative book-age cap chosen by sport/venue. No same-day tuning on model performance. Starting values for every cutoff, age and liveness limit, and the gate that revises each, are in §10.

Do not define `t_close` merely as “last tick strictly before off minus buffer” without also storing the *target cutoff*, actual observation time, quote age and feed-liveness age. An old, continuously unchanged book and a broken feed produce different evidence; both need visible diagnostics.

### 5.4 Preconditions and unscoreable states

A live close is scoreable only when all of these hold:

- The mapping is approved and the contract's start/settlement definitions permit comparison.
- Off resolution has trusted bounds and passes a prespecified disagreement policy.
- The selected book is complete, synchronized, open and neither halted nor suspended at the effective close.
- No stream/poll gap overlaps the required lookback/continuity window, including a connection-wide gap.
- Heartbeat/channel liveness is confirmed; selected book age and observed-to-venue lag satisfy prespecified limits.
- Both sides of the reference book are valid; requested depth exists on each side for a depth-based benchmark.
- No input used in selection or computation occurred after the applicable as-of cutoff.

Examples of explicit reasons: `no_trusted_off`, `off_disagreement`, `mapping_unverified`, `rules_mismatch`, `collection_gap`, `feed_stalled`, `stale_book`, `venue_lag`, `halted`, `crossed_book`, `no_depth`, `no_quotes`, `ambiguous_candle`, `few_trades`. A missing close is a **measured result**, not an exception to be discarded from the reporting denominator.

Do not exclude otherwise valid wide-spread markets by default. Preserve spread, depth, turnover and quote age; stratify results and report estimates for the entire eligible population **and** prespecified liquid subpopulations. Missing depth still makes a depth-dependent benchmark impossible and must not be filled synthetically.

### 5.5 Independent reference and robustness

Compute Novig and Kalshi close results in separate rows, not one “best available” pooled result. Cross-venue comparisons require the **same canonical contract, same side, compatible timing and comparable book semantics**. Save the time-alignment lag and each venue's depth and spread. An `agreement_delta` is informative only on such aligned observations.

If the primary venue fails, report its missingness. A prespecified fallback reference may produce a separate sensitivity result; it must not silently replace the primary observation in the headline series.

### 5.6 Historical close family

Do not encode an OHLC candle as an instantaneous live `tick`. Store `historical_candle` with start/end interval, sample resolution, native fields and source; store trades in the execution ledger with deduplication across live and daily imports.

`close_hist_vwap_*`: use an explicitly bounded pre-cutoff trade window, minimum independent execution count and minimum traded notional; retain those sample statistics. `close_hist_candle_*`: use bid/ask history only where the vendor supplies it; require the **entire contributing candle interval** to end before cutoff. If an OHLC field could include post-cutoff activity, exclude it rather than imputing a pregame midpoint.

Historical estimates are labeled **weaker proxies**. Never average them with live book-based closes, and never use them to claim actual execution at the displayed price.

---

## 6. Entry capture and execution assumptions

### 6.1 Log the eligible candidate universe

Store **every model emission**, including `would_bet = 0`, with model version/artifact hash, decision time, full as-of feature snapshot or immutable feature reference, output probability and threshold/policy version. Explicitly record whether features were actually available at decision time; a retrospective feature reconstruction without temporal provenance is not a live-equivalent signal.

Each scored entry references a signal (or is clearly labeled as a diagnostic control/manual entry) and its approved instrument mapping. A database constraint or transactional validation ensures the instrument and signal refer to the same canonical outcome and `signal.emitted_ts <= entry.decision_ts`.

### 6.2 Three entry-price concepts

Keep these distinct in both schema and dashboard:

1. `observed_quote`: native odds, source event time, observed/snapshot time, bookmaker update time if provided, source and quote age. This is the actual observation.
2. `decision_quote`: the most recent quote **known as of the signal's decision timestamp** under the specified data-access contract. The mere existence of an earlier provider timestamp is not proof of timely receipt.
3. `execution_assumption`: requested stake/notional, assumed latency, next observed price, available depth/limit when known, and explicit feasibility (`confirmed_fill`, `plausible_simulation`, `unverified_historical`, `unavailable`).

For the initial research cohort, the observed-quote CLV can be reported as a **price-signal diagnostic**, but no historical result is called “actionable” merely because a nominally pregame quote appears in a five-minute vendor snapshot.

### 6.3 Execution and latency stress tests

For live entries, report at the observed quote and at the next admissible quote after prespecified delays (e.g., 5, 15 and 30 seconds, as capture cadence permits). If the feed cannot resolve a delay, label the estimate unavailable—do not assume the price held. Include the quote's age, order-size assumptions, venue betting limits where documented and the execution venue's rules.

For soft-book entries where only a displayed price is available, mark fill probability and permitted stake as unknown unless real-fill data exist. For exchange entries, a depth walk estimates price impact but not queue priority or guaranteed execution.

Fee-adjusted hypothetical EV must identify every modeled cost (entry, possible hedge, withdrawal/lockup where relevant) and the cashflow convention. Real fills and actual settlement are separate evidence, not overwritten by a hypothetical price.

---

## 7. Collection architecture and replay

### 7.1 Two durable evidence layers

**Raw evidence:** persist the received vendor payload or an exact retrievable raw artifact, ingestion time, provider/source, transport channel, parser version and content hash before or atomically with interpretation. Use daily compressed files with a manifest and checksums; SQLite stores stable pointers. Secrets, tokens and personal account data must be excluded or redacted before archiving.

**Normalized facts:** append-only event aliases, mapping observations, quote/book snapshots, deltas, poll outcomes, trades, start observations, signals, entries and settlements. Normalized data are efficient for queries but never the sole record needed to repair a parser, quote-format or polarity bug.

**Archive format** — shared by the R0 recorder and the harness, so R0 captures replay without conversion:

```text
archive/<source>/<YYYY-MM-DD, UTC>/<stream_id>.<segment_id>.jsonl.gz
  one line per frame:
  {"recv_ts_ms": <int>, "conn_id": "<str>", "dir": "in" | "out" | "event",
   "frame": "<exactly the received text, as a JSON string>"}
archive/<source>/<YYYY-MM-DD, UTC>/manifest.jsonl
  append-only journal, one line per sealed file: file, stream_id, segment_id, sha256, bytes,
  frame_count, first/last recv_ts_ms, recorder_version, redaction_policy, session_id,
  unclean_close, dropped_tail_bytes
archive/<source>/<YYYY-MM-DD, UTC>/manifest.json
  a view of the journal, built when the day closes
```

A stream is one WebSocket connection (`stream_id = conn_id`) or one REST poller. A poller's lines each carry their own `conn_id = request_id`, so a file holds many request envelopes without merging them. The active segment is written as `.jsonl.part`, flushed per frame and fsynced every `r0.fsync_interval_s`; sealing at `r0.segment_max_s`, UTC midnight or shutdown gzips it, hashes it, appends a journal line and makes the file read-only. A segment orphaned by a killed process is sealed by the next session with `unclean_close` set and any torn final line dropped and counted.

**`frame` is always a JSON string holding exactly the received text — never the parsed message re-embedded as an object.** Re-serializing a parsed message changes bytes (key order, whitespace, number formatting), which breaks the file hashes and the byte-for-byte golden replay in §9.4. Decoding the string returns the original text exactly.

`in` frames are verbatim vendor text messages. `out` frames are our own subscribe, unsubscribe and snapshot-probe messages, with credentials and signatures redacted. `event` frames hold JSON-encoded recorder metadata as a string and mark connect, disconnect, error, process start/stop and transport Ping/Pong observations. Every process start has a new recorder session ID; an unclean previous session is recorded on restart, since a killed process cannot emit its own stop event. Do not enable vendor-side compression in the recorder: text messages keep replay simple.

**REST envelope.** Each attempted request, including failures and timeouts, has a globally unique `request_id`. Before sending, archive an `event` with that ID, method, sanitized origin/path/query, requested native subjects, request-start UTC time and recorder-session monotonic time. Use a request-specific `conn_id = request_id` for its archive records, even when the HTTP client reuses a physical connection; the records are written into the poller's stream. Archive the response body verbatim as an `in` frame under the same ID, then a completion `event` carrying receive/completion times, elapsed monotonic duration, HTTP status, content type, quota headers and failure reason where applicable. No response is associated by adjacency alone. Empty bodies, non-JSON errors, and timeouts retain the original request identity. Remove API keys, signatures, authorization/cookie values and other secrets from request metadata before writing it. Kalshi's single-market book response does not carry its ticker; the envelope is required to identify it during replay.

**Transport and channel evidence.** Hook WebSocket control-frame handling explicitly: a text-message receive loop may never see Ping/Pong. Archive control direction, observation time and session monotonic time as `event` records; do not pretend a control frame is vendor JSON text. Preserve application-level heartbeat payloads verbatim. Transport Ping/Pong proves connection health only. Record every public-channel probe's nonce, requested markets/channels, send/receive times and raw reply so §7.2 can establish subject-level freshness. Successful probes with unchanged books are evidence; silence is not.

R0 archive verification includes interleaved REST requests with identical response bodies, a timeout, and a quiet WebSocket market with control Pings and a snapshot probe. Replay must recover the correct subject/request association and distinguish transport health from channel evidence. Manifests used by a derived run reference sealed files; rotate an active segment before hashing it into a run. The append-only journal preserves every earlier manifest version as a prefix.

Derived close and score runs declare the raw/normalized fact snapshot, parser version, off-resolver version, benchmark definition, fee version and scorer code/version. Reprocessing new facts creates a new run instead of silently changing previously reported numbers.

### 7.2 Stream continuity

For each subscription/channel, maintain documented sequence state, snapshot synchronization state, last message and heartbeat times, and last trustworthy book observation. On sequence skip, disconnect, auth failure, rate limit, restart or heartbeat timeout:

1. Append a scoped `collection_gap` open observation beginning at the **last trustworthy observation**, not detection time.
2. Mark affected books untrusted and stop producing scoreable closes.
3. Reconnect according to documented backoff, obtain a fresh snapshot and verify its sequencing.
4. Append the corresponding close observation **only after** successful resynchronization. Preserve any overlapping/connection-wide gaps for audit.

A healthy socket is not proof that every subscribed channel is live. Check per-channel/per-subject liveness as the provider permits. Never infer “unchanged” from an absent tick without independent proof that the relevant feed or poll scheduler was functioning.

**Quiet public channels.** Recent contiguous subject/channel messages or a documented heartbeat naming that subject/channel are admissible liveness evidence. Otherwise send an authoritative `snapshot` probe at `stream.channel_probe_interval_s`, with response timeout `stream.probe_timeout_s`; a subscription-list/status acknowledgement alone is insufficient. Novig probes use the documented per-market/per-channel sequence within the same connection. Reconcile the reply with buffered deltas under the provider's ordering contract: a sequence equal to the last contiguous sequence confirms an unchanged channel; a jump without the intervening deltas exposes a gap. A delayed snapshot already covered by contiguous deltas is superseded and must not roll state back. A new snapshot can establish fresh state after a gap but never retrospectively prove the missing interval was continuous. A probe costs the same `stream` tokens as a subscription (Novig: 16 per market on `book`, 4 on `trades`, against a bucket refilling 4 per second), so a recorder never lets probes draw the shared bucket below `r0.stream_probe_reserve_fraction` of capacity and archives each unaffordable probe as a `probe_skipped` event: missing evidence, recorded. R0 only archives these probes/replies; A1 implements interpretation and resynchronization. Validate that all required channels remain subscribed after probing. If the API or quota cannot provide this evidence at the configured cadence, record `feed_stalled` or `collection_gap`; revise the policy through a dated decision rather than treating socket Pings as channel proof.

**Periodic full snapshots:** in addition to change-driven ticks, persist complete ladders at a measured interval and after every resync. Select that interval using a one-week storage/pipeline pilot. Preserve at least enough depth for the largest prespecified benchmark notional; fail closed if the sampled ladder is truncated before the required notional.

**Exactly three book representations.** (1) The raw archive holds every received message, including every delta, and is authoritative for replay. (2) `tick` holds normalized top-N book state, written when that state changes, for queries and close computation. (3) `book_snapshot` holds complete ladders at the periodic interval and after every resync. There is no normalized delta table: when deltas are needed, replay them from the raw archive.

Probe replies always remain in the raw archive; they do not require a new normalized `tick` when state is unchanged. A successful unchanged probe refreshes channel evidence, not the book's last economic-change timestamp or quote-age clock. Persist normalized full ladders at the configured storage cadence and after resync independently of the probe cadence.

### 7.3 Poll continuity

Record `poll_attempt` / `poll_result` for **every** scheduled poll, including HTTP status, success with unchanged quote, timeout, throttling, request quota, actual completion time, source payload hash and list of subjects covered. A no-change result is a fact even when no new per-instrument quote row is written. Treat partial responses as coverage failures for the omitted expected subjects, not as evidence that their prices did not change.

Poller restart checks the last scheduled and completed interval, writes the missing interval as a gap and recovers by a fresh poll. Backfill after an outage is retained as **historical recovery**, not a retroactive claim that the live feed was uninterrupted.

### 7.4 Trade deduplication

Novig public daily trade reports may contain one row per side; live tape and next-day CSV may describe the same execution. A uniqueness key of `(instrument, source, trade_key)` alone does **not** prevent cross-source duplication.

Use a canonical `execution_id` when the venue exposes one and a separate append-only `trade_source_observation` table for every sighting. Where no execution ID exists, use a documented matching/reconciliation rule over native side, time, price, size and row identifiers; mark ambiguous matches rather than inventing perfect deduplication. VWAP counts **unique, reconciled executions only**. Reconcile per-market volume against provider aggregates when available.

### 7.5 Storage and operations

SQLite WAL on a wired small machine remains a reasonable initial choice. Put the database and compressed raw archive on a reliable SSD, back up both, verify backup restores and monitor disk space and WAL growth. Retain checksums and source manifests for imported CSV/candles. Separate raw/facts and derived files or databases if doing so simplifies read-only enforcement and recomputation; do not introduce distributed infrastructure without measured need.

---

## 8. Data-model contract

Table names below are conceptual; exact naming can vary if the behavior and constraints do not.

### 8.1 Facts: append-only observations

| Entity | Purpose / required revision |
|---|---|
| `event` | Stable internal identity; immutable basic identity only; avoid date/team-derived collisions |
| `event_alias`, `event_schedule_observation` | Provider-ID history, schedule revisions, league identifiers and mapping corrections |
| `outcome`, `contract_rule_observation` | Precisely defined outcomes and provider-specific settlement/void conditions |
| `venue_instrument`, `instrument_mapping_observation` | Stable native instruments and versioned mapping/polarity/equivalence decisions, including payout currency, USD payout per native contract and quantity increment for depth conversion |
| `raw_artifact` / archive manifest | Payload location, content hash, source, parser version, receive time and redaction policy |
| `book_snapshot`, `tick` | Normalized book state: full ladders (periodic and after resync) and top-N state on change, with complete-ladder flags, sequence, native/observed time and status. Deltas live only in the raw archive (§7.2) |
| `poll_attempt`, `poll_result` | Successful unchanged polls and failures, expected/received subject set and quota data |
| `trade_execution`, `trade_source_observation` | Canonical reconciled executions and every raw sighting; one economic execution counted once |
| `collection_gap` | Append-only open/close pairs, per-subject or connection-scoped, including heartbeat and partial-poll failures |
| `off_observation` | Multiple independent off observations with source uncertainty and native payload |
| `signal`, `entry`, `entry_quote_observation` | All emitted candidates, precise as-of features, entry quote and assumed execution lineage |
| `settlement_observation` | Official/venue outcomes and their corrections, without overwriting history. (Actual fills, `real_fill`, are defined in `docs/RESEARCH_PROTOCOL.md`.) |
| `control_outlier_review` | Append-only Check-2 review: calibration run, entry ID, input fingerprint (raw quote hashes, mapping version, as-of times), reviewer/time, evidence reference and disposition; discovered errors invalidate the run rather than being waived |
| `historical_candle` | Native candle interval and meaning; explicitly separate from instantaneous books |

**Fact immutability:** enforce with `BEFORE UPDATE` / `BEFORE DELETE` triggers or a restricted append-only write API. Corrections are new observations with `supersedes_id`/provenance. Retractions are tombstone observations, never deletion of source history. Mutable scheduling, mapping, fee and settlement information therefore belongs in observation tables or derived current-state views, not mutable “facts” columns.

If a fact references an unknown event or outcome during asynchronous ingest, quarantine the raw observation and reconcile later; do not disable foreign keys or create guessed identities to force ingestion through.

### 8.2 Derived: explicit lineage and replay

| Entity | Purpose |
|---|---|
| `fact_snapshot` | Stable high-water marks / raw-artifact manifest identifying the exact inputs to a run |
| `mapping_resolution` | Approved mapping version and validity for each scored instrument |
| `off_resolution` | Versioned trusted start interval and source disagreement |
| `close_def` | Immutable serialized parameters: venue, price basis, cutoff, book-age policy, liquidity notional, fallback rules, off-resolver version |
| `close_price` | One row per fact snapshot, definition, reference venue and canonical outcome; actual close time, liveness, liquidity, confidence and exclusion reason |
| `scoring_run` | Scorer code version, fee model version, analysis specification and fact snapshot |
| `clv_score` | One row **per reference venue/definition/entry/run**; all component prices, CLV, null EV, residual and modeled fees |
| `analysis_cohort` | **Deferred** to `docs/RESEARCH_PROTOCOL.md`. Until then the harness produces the coverage waterfall (§11.2) without a cohort table |

**Score keys:** `clv_score` includes `ref_venue` and a run/fact-snapshot identifier in its primary key (or a unique composite key) and references the exact `close_price` row, so independently computed Novig and Kalshi scores for the same entry coexist.

`fact_snapshot` must stay cheap: per-table rowid high-water marks plus the raw-archive manifest hashes. Because facts are append-only, that identifies the exact inputs to a run without copying data. Fact tables must therefore be ordinary rowid tables (no `WITHOUT ROWID`).

For convenience, denormalize selected stratification fields into `close_price` / `clv_score` **with lineage**: spread, depth, volume/trade count, quote age, source lag, seconds before off, off confidence, reference agreement and unscoreable reason. Never compute coverage denominators exclusively from rows that happened to score.

### 8.3 Numeric and temporal constraints

Retain integer probability quotes scaled by `10,000` and integer point lines scaled by `100`, with native strings preserved. Extend precision deliberately only if actual venue ticks require it; do not round a native price and then attempt to reconstruct payout from the rounded value.

Enforce at least:

```text
0 <= normalized_probability <= 10000
0 <= bid_probability <= ask_probability <= 10000
bid_depth >= 0; ask_depth >= 0; quantity >= 0
source='stream' => appropriate sequence/snapshot state
historical candle interval_start < interval_end
signal.emitted_ts <= entry.decision_ts
entry outcome == approved instrument mapping outcome at entry time
reference observation time <= close cutoff
close cutoff < earliest plausible actual start
```

Some are multi-table/time-dependent validations and belong in transactional application logic plus tests rather than naive SQLite `CHECK` clauses. Add relevant indexes on `(instrument_id, observed_ts)`, `(instrument_id, venue_ts)`, `(event_id, source, observed_ts)`, gap scope/time, and source alias IDs. Document all uniqueness keys and avoid nullable values in SQLite unique keys where duplicates must be impossible.

Native event and observation timestamps should use a consistent **high-resolution** integer UTC representation with an explicit unit suffix (`_ms`, if milliseconds); convert at the edges. Distinguish provider-reported update time from ingestion time, and record local clock/NTP health for collector timing audits.

### 8.4 Required migration tests

- The cumulative migrations apply cleanly to a fresh database at every stage of §8.5 and create the declared indexes, foreign keys and immutability triggers.
- An attempted update or delete of each append-only fact table fails; adding a superseding observation succeeds.
- The same doubleheader can be stored as two games; a rescheduled game retains its canonical identity across new provider aliases.
- Correctly store two reference-venue scores for the same entry; reject duplicate same-venue/same-run scores.
- Reject invalid probability, inverted bid/ask, negative depth, invalid source/status combinations and untrusted post-cutoff data.
- Reject signal/entry outcome mismatch and time-traveling feature or quote availability.
- Importing the same economic trade via stream and CSV twice changes **neither** unique-execution count nor VWAP.
- Reverse a complement-quoted ladder with asymmetric depth twice and reproduce every native price/depth exactly.

Each test runs from the stage that introduces the tables it exercises.

### 8.5 Staged schema growth

The schema grows with the build. A table exists from the stage that first writes to it. Contact with real data in V0 will change the stage-1 design; that is the point of V0. Each stage is one migration file (`migrations/NNNN_<stage>.sql`) containing DDL, constraints, immutability triggers for fact tables and the §8.4 tests that apply.

| Stage | Facts introduced | Derived introduced | Why then |
|---|---|---|---|
| R0 | None: raw archive files and manifests only (§7.1) | — | Capture before parsing |
| V0 | `event`, `event_alias`, `outcome`, `venue_instrument`, `instrument_mapping_observation`, `raw_artifact`, `tick`, `book_snapshot`, `collection_gap`, `off_observation`, `signal`, `entry`, `entry_quote_observation` | `fact_snapshot`, `mapping_resolution`, `off_resolution`, `close_def`, `close_price`, `scoring_run`, `clv_score` | Smallest set that traces one game from raw frame to score, including a mapping-correction replay |
| A1 or B1, whichever first | `trade_execution`, `trade_source_observation` | — | Live tape or historical trade import |
| A2 | `event_schedule_observation` | — | Schedule revisions once many games flow |
| A4 | `poll_attempt`, `poll_result` | — | Soft-book polling |
| B1 | `historical_candle`, if Kalshi supplies candles | — | Historical reference import |
| B2 or G2, whichever first | `settlement_observation` | — | Oracle controls need outcomes |
| G1 or B3, whichever first | `control_outlier_review` | — | Check 2 cannot pass with undocumented outlier disposition |
| When contract rules must be tracked per provider over time | `contract_rule_observation` | — | Until then, v1 moneyline rules live in `docs/measurement-contract.md` and each mapping carries its `settlement_equivalence` verdict |
| Research protocol activation | `analysis_cohort`, `real_fill` | — | Model evaluation only |

**Rule:** a table nobody writes to yet does not exist yet.

---

## 9. Calibration and adversarial testing

### 9.1 Diagnostic hierarchy

The harness must pass **mechanical correctness** first, **causal/time integrity** second, **benchmark robustness** third and **statistical research validity** fourth. A passing random-control mean is not a substitute for direct unit tests or source audits.

| Test | Intended detection | Important limitation |
|---|---|---|
| Fixture transformations and invariants | Odds unit errors, swapped sides, rounding, outcome mismatch, crossed books | Cannot establish economic fairness |
| Stream/poll fault injection | Disconnection, lost deltas, silent channel stalls, partial polls, restart handling | Cannot establish actual event-start accuracy |
| Fixed-side martingale test (§9.2, check 1) | Level drift, linear favorite/longshot drift, prespecified symmetric quadratic curvature; close-leg polarity and close-selection errors | Does **not** prove absence of arbitrary nonlinear drift (the binned diagnostic covers localized shapes); cannot see a mapping error that flips entry-time and close reference prices together (check 2 does); blind to in-play leakage (§9.3) |
| Binned lack-of-fit (§9.2, check 1 diagnostic) | Localized or non-quadratic drift that the three-term fit misses | Resolution limited by bin count and trigger; every flag needs a decision record |
| Entry-to-reference agreement (§9.2, check 2) | Mapping and polarity disagreements between entry venue and reference, whichever side is wrong; odds-format errors; wrong-game joins; misaligned entry times | Genuine cross-venue disagreement produces a tail; needs the entry venue's two-way price |
| Random-side residual (§9.2, summary) | Close-leg polarity flips; price/payout-dependent response to drift | **Not a gate.** It may cancel in symmetric populations; payout weighting prevents general cancellation |
| Outcome-aware oracle buffer sweep | Late/in-play contamination and off-resolver problems | No kink is **not** proof of zero contamination; late pregame news can create abrupt moves |
| Artificial contamination injection | Whether controls detect **known** post-start leakage | Synthetic path may not represent every real-game pattern |
| Cross-reference/price-basis sensitivity | Thin or distorted primary venue, benchmark choice artifacts | Two venues may share information or errors |
| Chronological holdout | Research overfitting and repeated metric selection | Needs enough independent events |

### 9.2 Calibration checks: one per pipeline leg

The pipeline has two legs that fail differently — the reference/close leg and the entry leg — so there are two gating checks, plus one summary statistic that is reported but never gates.

**This section is subordinate to `tests/test_calibration_simulation.py`.** Every claim below about what a check can or cannot detect is asserted there against `src/clv/score/controls.py`. A change to this section, or to the §10 control margins, is accepted only with a matching change to that test, passing. Properties of statistical checks are established by simulation, not by argument.

```text
p_e = reference fair probability of the outcome at entry time
p_c = reference fair probability of the outcome at the close
d   = entry payout factor, derived from the native quote
q_e = entry venue's own fair probability of the outcome at entry time,
      de-vigged within its two-way market at that time

null_ev  = p_e * d - 1
clv_ev   = p_c * d - 1
residual = (p_c - p_e) * d
```

**Check 1 — fixed-side martingale test: the reference and close leg (hard gate).** For every scoreable event take one **fixed** canonical side (home), never a random one, and compute `Δp = p_c − p_e` at each offset in `controls.martingale_offsets`. At each offset fit the prespecified three-term regression, with event-clustered errors:

```text
u = p_e - 0.5
x = u - mean(u)
q = (u / r)^2 - mean((u / r)^2)       # r = controls.martingale_curvature_radius_pp
Δp = α + β*x + γ*q + ε
```

Both terms are centered on the sample, so `α` is the sample-average drift even when entry prices are skewed — home sides are favored on average. `γ` is the fitted extra drift at `p_e = 0.5 ± r` relative to `p_e = 0.5` for a symmetric quadratic departure. The calibration run records the centering constants `mean(u)` and `mean((u / r)^2)` with its results; the research protocol's drift-bias bound needs them.

*Why three terms.* A symmetric nonlinear drift can have zero average and zero linear slope while still moving favorites and longshots materially, so an intercept-and-slope fit cannot see it.

- A nonzero **intercept `α`** is average level drift, for example the fixed home side firming into every close.
- A nonzero **linear slope `β`** is first-order price-dependent drift, such as favorites firming and longshots fading. A slope near −2 remains the signature of a close-leg polarity flip, which turns `p_c` into `1 − p_c`.
- A nonzero **curvature `γ`** is symmetric nonlinear price-level drift, which an intercept-and-slope fit cannot see.

To pass, at every offset all three 90% intervals must lie entirely inside their prespecified equivalence margins: `α` within ±`controls.martingale_margin_intercept_pp`, `β` within ±`controls.martingale_margin_slope`, and `γ` within ±`controls.martingale_margin_curvature_pp`. These are two one-sided equivalence tests at 5% each. An interval that merely includes zero is not a pass: that rewards noise. Check 1 needs only reference data, so it gates the close machinery in Track B before any soft-book history is bought (§13, B2).

The caller supplies the complete ordered `controls.martingale_offsets` as `expected_offsets`, independently of available observations. An empty/partial offset map, empty observations, fewer than `controls.min_clusters` events, or too little price variation to fit the three terms raises `InsufficientCalibrationData` (deliberately not a `ValueError`); misaligned input lengths remain errors; the orchestration layer records `insufficient_data`, never `passed` or a statistical rejection. Duplicate/empty configured labels and unexpected supplied offsets are configuration errors. Never drop a difficult or unavailable offset to make a gate pass. A legitimate change to the required set creates a new prespecified calibration definition.

This is a **prespecified functional-form check, not a theorem that all predictable drift is absent**. A **binned lack-of-fit diagnostic** covers shapes outside the three terms: at every offset, the mean residual from the fitted curve is reported in `controls.martingale_price_bins` equal-count entry-price bins, with event-clustered intervals. A bin is **flagged** when its whole 90% interval lies outside ±`controls.martingale_bin_trigger_pp`. Each flag blocks benchmark sign-off until a decision record explains it; a structural pattern is then either added to a new prespecified gate with simulation coverage, or the reference is abandoned. Do not add basis terms after seeing model CLV. In the committed simulation, a +2.5 pp drift confined to entry prices 0.56–0.60 passes check 1 in about three samples of four, and the bins flag it in 99% of samples, always in a bin overlapping the band. This localization is a property of that fixture; projection onto the fitted curve can spread other faults across bins.

**Binned interval calculation.** Condition on the observed prices and equal-count bin membership. Tied prices always share a bin (each takes the rank of its lowest tied position), so membership is independent of row order and counts are approximately equal. If `w_b` is `1/n_b` inside bin `b` and zero outside, its fitted mean residual is the full-sample contrast `a_b' Δp`, where `a_b = w_b - X (X'X)^(-1) X' w_b`. With full-model residuals `e_i`, compute event scores `S_g = sum_{i in event g} a_bi * e_i` over **all** rows and `SE_b² = G/(G-1) * (n-1)/(n-k) * sum_g S_g²`, with `k = 3`. This accounts for fitting the curve on the same data and for events spanning bins. Running an intercept regression on only a bin's fitted residuals is not this covariance estimator. Each bin still needs at least `controls.min_clusters` distinct events. The simulation checks nominal 90% coverage under the fixed-design model with correlated observations across bins; unsupported/sparse bins yield `insufficient_data`.

Margins are in probability points, but what they cost a model depends on the prices it bets: the same drift costs more CLV at longshot payouts. Every model claim must therefore clear the drift-bias bound in `docs/RESEARCH_PROTOCOL.md` §6, computed from these margins over the model's own entries.

A failure stops model claims for that reference and close definition. If the cause is genuine predictable drift rather than a bug, the reference is a defective benchmark: a finding to report, never an offset to subtract.

*Why a fixed side.* In a two-outcome market every probability move is antisymmetric across sides: if home firms by two points, away fades by two. Fair side randomization cancels that **unweighted probability move** in expectation. The EV residual multiplies by a side-dependent payout, however: conditional on home-side move `Δp`, its expectation is `Δp * (d_home - d_away) / 2`. Cancellation therefore depends on the price/payout population. In the committed symmetric-price simulation a +2 pp drift leaves the EV residual near zero; with initial home prices in 0.42–0.70 it is about −0.55 pp. The fixed-side probability regression measures the drift directly in both cases.

**Check 2 — entry-to-reference agreement: the entry leg (hard gate).** Compare `q_e` with `p_e` entry by entry, using random-control entries priced at the entry venue.

**The calibration de-vig convention is fixed.** For this check only, `q_e` is the two-way proportional normalization of the entry venue's contemporaneous raw implied probabilities:

```text
q_e = π_side / (π_side + π_other)
```

Both sides must come from the same two-way market and same admissible as-of observation. This is a deliberately simple diagnostic convention chosen to make mapping/polarity faults testable; it does **not** assert that proportional de-vig is the preferred economic method for downstream CLV analysis. If the contemporaneous opposite side is unavailable, the entry is unavailable for Check 2 rather than silently using another method. `controls.agreement_devig_method` is fixed to `proportional_two_way`.

Two statistical/review components are required:

- **Slope.** Regress `q_e − p_e` on `(2p_e − 1)` with event-clustered errors. Correct mappings give a slope near 0. A systematic polarity or mapping flip on *either* venue makes `q_e − p_e ≈ 1 − 2p_e`, a slope near −1. The 90% interval must lie entirely within ±`controls.agreement_margin_slope`. The slope catches systematic flips even among near-coin-flip games, where each individual error is small.
- **Per-entry outliers.** Flag every entry with `abs(q_e − p_e)` above `controls.agreement_outlier_pp`. Check 2 **cannot pass while any flagged row is unresolved**. Each review is append-only and tied to the calibration run and entry, with evidence and one of these dispositions:
  - `verified_market_difference` — mapping, timing, native quotes and contract equivalence were independently rechecked and the disagreement is genuine; this row may remain in the run.
  - `pipeline_error_fixed` — a parser/mapping/timing/odds bug was found; the current run remains failed and a corrected run is required.
  - `entry_ineligible` — the row should not have entered this calibration population; the current run remains failed and must be regenerated under the corrected eligibility rule.

Only `verified_market_difference` counts as resolved **within the current run**. The other dispositions explain why the run must be superseded; they cannot be used to review away a bug. This catches isolated mapping errors, wrong-game joins and misaligned entry times that a slope can miss.

- **Matching is by entry ID.** The orchestration layer passes the gate the entry IDs of valid current-run `verified_market_difference` reviews, never array positions. A resolution that matches no flagged entry is an error, not a no-op.
- **Carry-over.** A `verified_market_difference` review carries into a later run only if that entry's input fingerprint — raw quote artifact hashes, mapping version and as-of times — is unchanged. Otherwise the row is re-reviewed.
- **Ceiling.** If verified market differences exceed `controls.agreement_max_verified_rate` of entries, check 2 fails regardless of review quality. At that rate the outlier threshold or the venue pairing is the finding.

Separately, mean `null_ev` should be consistent with the entry venue's own quoted margin in those markets — that venue's margin, not a universal −4.5%. A mismatch points to odds-format or payout-convention errors.

**Summary, never a gate — random-side residual.** Report the mean `residual` over random-side controls, with the price and payout distribution. Its response to drift depends on that distribution; a near-zero mean does not establish a sound reference, and a nonzero mean alone does not diagnose a pipeline fault. §9.4 exercises both symmetric cancellation and asymmetric noncancellation, as well as the separate gating checks.

Neither gating check sees in-play contamination, because a contaminated close is still a martingale. That is §9.3's job.

Report every statistic with event-clustered intervals, date-level sensitivity where events share news, and stratification by entry offset. Margins come from §10 and are fixed before real data are examined.

### 9.3 Oracle and conservative buffer selection

For calibration **only**, select the eventual winning side at fixed genuinely pre-event times (for example 24h, 6h, 1h and 15m before the trusted start boundary). Score those same oracle entries with close definitions at cutoff buffers `{0, 30, 60, 120, 600}` seconds when each close is otherwise scoreable. Plot mean oracle CLV with event-clustered uncertainty and a matched-event subset; changing availability between buffers can itself generate an apparent kink.

A sharp advantage concentrated in the smallest buffers **raises suspicion**, but the curve is not a sufficient safety proof. Choose the primary buffer on the conservative side of both the start-uncertainty bounds and injected-contamination tests; document any remaining tradeoff against lost coverage. Never report oracle performance as a strategy result.

### 9.4 Tests that must be shown to fail

Synthetic event-book fixtures should include:

1. Known martingale prices, known sportsbook margin and no contamination: random residual passes within simulation tolerances.
2. Polarity inversion on each leg separately. A close-leg inversion fails check 1 (slope near −2) and leaves check 2 passing. An entry-leg inversion fails check 2 (slope near −1) and leaves check 1 passing. A reference-instrument inversion, flipping entry-time and close reference prices together, fails check 2 and leaves check 1 passing. Assert every outcome, so each check's documented blindness is itself under test.
3. Swapped games or event aliases: mapping/invariant checks reject them.
4. A delayed start signal and in-play book updates: the off resolver and close logic reject or flag contaminated closes.
5. Out-of-order/duplicate/missing deltas and a live socket with a stalled channel: the gap/liveness mechanisms reject affected closes.
6. A five-minute historical candle that overlaps first pitch: candle close becomes unscoreable.
7. Identical/overlapping live tape and daily CSV executions: deduplication produces one economic execution.
8. A valid but wide pregame spread and a thin ladder: width is reported; required-notional benchmark is unavailable rather than manufactured.
9. A correction to mapping, settlement or schedule: original observations remain intact; new derived runs show the revised result and lineage.
10. A quote posted before a signal but *received* after it: the causal as-of policy rejects the quote as known at decision time.
11. Injected reference drift of two kinds — a level drift independent of price, and a **linear** price-dependent drift — fails check 1 through the intercept and the linear slope respectively. On the level drift in the specified symmetric-price population, the random-side EV residual nearly cancels. Assert all three outcomes; item 22 tests the asymmetric case.
12. Operating characteristics at the §10 sample target, over 1,000 replicates: a correct pipeline passes the full three-component check 1 in at least 95% of replicates; a level drift equal to the intercept margin fails it in at least 90%; and a symmetric quadratic drift equal to the curvature margin fails it in at least 90%. The test prints all three rates. True rates must clear these thresholds comfortably, not by sampling luck; a rate near its threshold means the margin or the sample target is wrong.
13. A systematic polarity flip among near-coin-flip games (entry prices 0.48–0.52): fewer than 1% of entries exceed the per-entry outlier threshold, and the check 2 slope still fails.
14. A centered symmetric quadratic reference drift large enough to matter, which an intercept-and-slope-only fit passes: the curvature component fails at every offset.
15. One isolated Check-2 disagreement above the outlier threshold while the slope still passes: the gate stays failed until that entry ID is supplied as a valid current-run resolution. Supplying an ID that was not flagged — including a bare array position — raises an error.
16. The Check-2 de-vig path is fixed: proportional normalization of a two-way market exactly recovers the fair side when the same multiplicative overround is applied to both sides.
17. Repeating identical rows within an event does not create fake precision: event-clustered Check-2 point estimates are unchanged and standard errors do not collapse as if duplicates were independent games.
18. Every outlier validly resolved, but verified differences above `controls.agreement_max_verified_rate`: check 2 fails. Below the ceiling it passes.
19. The binned diagnostic: a localized drift band that check 1 passes in most replicates is flagged in at least 95% of them, only in bins overlapping the band in this fixture; a correct pipeline is flagged in at most 2% of replicates.
20. Check 1 cannot pass an empty/partial offset map or an offset with empty/insufficient observations. The configured offset set is explicit; duplicate/empty labels and unexpected offsets are errors, and result ordering follows the specification.
21. Binned intervals account for the full-sample curve fit and events spanning bins: over 1,000 fixed-design correlated-error replicates, each nominal 90% interval has coverage between 86.5% and 93.5% (Monte Carlo tolerance). A bin with fewer than `controls.min_clusters` events refuses an interval.
22. Random-side EV residuals under asymmetric prices do not generally cancel level drift. A paired-side calculation reproduces `mean(Δp * (d_home - d_away) / 2)` exactly; the randomized sample is consistent with it and clearly nonzero.

Items 1, 2 and 11–22 are implemented in `tests/test_calibration_simulation.py` against `src/clv/score/controls.py`. Both ship with this design and must keep passing; the simulation's reference-series assumptions are replaced with B1 and A1 measurements when those exist.

Tests should assert not only that scores differ, but that the **expected exclusion or diagnostic reason** is emitted. Establish an audited “golden game” containing raw messages, transformations, start resolution, entry, close and score; replay it byte-for-byte in CI.

---

## 10. Provisional parameters

Every threshold, interval, notional and tolerance the harness uses is listed here with a starting value. `config/params.toml` is created from this table in V0 and is authoritative thereafter; each key carries `status` and `revised_by`. Until then, the R0 recorder copies its few values from this table into `tools/raw_recorder/config.toml`.

- **No threshold exists outside this table.** A coding agent that needs a value not listed here adds it here first, with a default and a revising gate. A silent constant is a defect.
- **Fixed** values change only by a design change. **Provisional** values change by a dated decision record in `docs/decisions/`.
- A change that alters a metric's meaning creates a new `close_def`, resolver or run version. It never edits the meaning of a reported result.
- Anything that could be tuned on model performance is locked on development data before any model is evaluated (§9.3, G2).

| Key | Default | Status | Set or revised by |
|---|---|---|---|
| **Time** | | | |
| `time.unit` | Integer UTC milliseconds, `_ms` suffix | fixed | — |
| `clock.ntp_offset_alert_ms` | 100 | provisional | A1 time-health telemetry |
| `clock.min_claimable_buffer_ms` | 1,000: no claims about cutoffs finer than 1 s | provisional | A1 |
| **R0 recorder** (copied into `tools/raw_recorder/config.toml`) | | | |
| `r0.kalshi_poll_interval_s` | 10 | provisional | R0: observed Kalshi rate limits |
| `r0.odds_poll_interval_s` | 1,200 (20 min), game windows only | provisional | R0: free-tier credit budget |
| `r0.odds_quota_floor` | 50 credits: stop polling below this | provisional | R0 |
| `r0.capture_lead_s` | 10,800 (3 h) before scheduled start; per-game override allowed | provisional | R0: covers the 1 h and 15 min offsets with margin |
| `r0.capture_tail_s` | 5,400 (90 min) after scheduled start; per-game override for delays | provisional | R0: at least 30 min after first pitch |
| `r0.odds_window_lead_s` | 10,800 (3 h) before scheduled start, ending at it | provisional | R0: free-tier credit budget |
| `r0.segment_max_s` | 3,600; UTC midnight also rotates | provisional | R0/A1 measured volume |
| `r0.fsync_interval_s` | 5: at most this much received data lost on a crash | provisional | R0/A1 host storage |
| `r0.rest_timeout_s` | 10, total per request; a timeout is a recorded failure | provisional | R0 observed latency |
| `r0.min_free_disk_mb` | 5,000: below this, an alert event; recording never stops silently | provisional | R0/A1 measured volume |
| `r0.stream_probe_reserve_fraction` | 0.5 of the `stream` bucket kept for subscribes and reconnects | provisional | R0/A1 measured probe demand |
| `r0.novig_public_book_poll_interval_s` | 10: unsigned public book poll, only without a read key | provisional | R0; the public response is cached 5 s |
| **Streams and books** | | | |
| `stream.liveness_max_s` | 30 since admissible subject/channel evidence, including a successful unchanged snapshot probe | provisional | R0/A1: subject-level evidence and probe latency |
| `stream.transport_liveness_max_s` | 45 since received traffic, including received control Ping/Pong; local sends do not refresh it | provisional | R0: 3× documented Novig 15 s Ping interval; independent of channel liveness |
| `stream.channel_probe_interval_s` | 15 for selected channels lacking a documented subject heartbeat | provisional | R0/A1: subscription semantics and actual quota |
| `stream.probe_timeout_s` | 5 from send to authoritative reply | provisional | R0/A1: measured latency; timeout records a coverage failure |
| `stream.max_venue_lag_ms` | 5,000 (receive time minus venue time) | provisional | R0/A1: about 2× observed p99 |
| `stream.reconnect_backoff_s` | 1 doubling to 60, with jitter | provisional | Vendor docs, A1 |
| `book.tick_levels` | 10 per side | provisional | R0: must cover the largest benchmark notional; fail closed if truncated |
| `book.full_snapshot_interval_s` | 300, plus after every resync | provisional | A1 one-week storage pilot |
| **Polling** | | | |
| `odds.poll_interval_s` | 300 | provisional | A4 quota budget |
| `scores.poll_interval_s` | 60, from scheduled −20 min until in-progress is observed or scheduled +4 h | provisional | A2 |
| `odds.quota_target_utilization` | 0.65 of monthly credits | provisional | A4 |
| `odds.quota_degrade_at` | 0.85 used, or projected exhaustion before reset | provisional | A4 |
| **Off resolution** | | | |
| `off.max_source_disagreement_s` | 120; beyond it, `off_disagreement` | provisional | A2 agreement report |
| `off.scheduled_only` | Unscoreable: `no_trusted_off` | fixed | — |
| **Close** | | | |
| `close.buffer_s` | 60 | provisional | G2: conservative side of the oracle kink and contamination-injection tests |
| `close.buffer_sweep_s` | 0, 30, 60, 120, 600 | fixed | — |
| `close.depth_notional_basis` | USD payout on a win, equal target on both canonical sides; proportional final level | fixed | §2.4; contract multiplier retained per instrument |
| `close.depth_notionals_usd` | 100, 500, 1,000 of payout | provisional | A3 liquidity pilot, before any model data |
| `close.primary_benchmark` | `mid_depth_500` | provisional | Locked at G2 on development data |
| `close.max_quote_age_s` | 1,800; beyond it, `stale_book`; all ages below are stratified | provisional | A3, per sport |
| `close.hist_vwap_window_s` | 600 before cutoff | provisional | B2 coverage |
| `close.hist_vwap_min_executions` | 5 unique reconciled executions | provisional | B2 |
| `close.hist_vwap_min_notional_usd` | 250 | provisional | B2 |
| `close.hist_candle_rule` | Entire candle interval ends before cutoff | fixed | — |
| **Entries** | | | |
| `entry.execution_delays_s` | 5, 15, 30, as capture cadence permits | provisional | A4 |
| `mapping.primary_cohort` | `exact` or `manual_verified`, with confirmed settlement equivalence | fixed | — |
| **Controls and acceptance** | | | |
| `controls.sample_events` | About 1,500 independent events per close family | provisional | §9.4 item 12, then observed dispersion |
| `controls.min_clusters` | 30 distinct events per fit and per diagnostic bin | fixed | Normal-approximation floor, the code constant `MIN_CLUSTERS` in `controls.py`; not read from `params.toml`, changed only with the code and a simulation rerun; not the sample target |
| `controls.equivalence_ci` | 90% event-clustered interval, entirely inside the margin (two one-sided tests at 5%) | fixed | — |
| `controls.martingale_offsets` | 24 h, 6 h, 1 h, 15 min before the trusted start bound | provisional | B2 |
| `controls.martingale_margin_intercept_pp` | 0.5 pp, about 1 pp of CLV at even odds | provisional | §9.4 item 12; tighter needs more events |
| `controls.martingale_margin_slope` | 0.05 | provisional | §9.4 item 12 |
| `controls.martingale_curvature_radius_pp` | 20 pp from 0.50 (`p = 0.30` or `0.70`) | fixed | Defines the interpretable scale of the quadratic basis |
| `controls.martingale_margin_curvature_pp` | 1.6 pp edge-vs-center drift at the curvature radius | provisional | §9.4 items 12 and 14; tighten after B1/A1 dispersion is measured |
| `controls.martingale_price_bins` | 5 equal-count bins per offset | provisional | B1/A1 sample support |
| `controls.martingale_bin_trigger_pp` | 0.5 pp: a bin is flagged when its 90% interval lies entirely outside ± this | provisional | §9.4 item 19 |
| `controls.agreement_devig_method` | `proportional_two_way` | fixed | Calibration diagnostic only; §9.2 |
| `controls.agreement_margin_slope` | 0.15 | provisional | V0/B3 observed distribution |
| `controls.agreement_outlier_pp` | `abs(q_e − p_e)` above 8 pp: requires an auditable disposition | provisional | V0/B3 observed distribution |
| `controls.agreement_max_verified_rate` | 1% of entries; above this, check 2 fails however good the reviews | provisional | V0/B3 observed distribution |
| `controls.oracle_offsets` | 24 h, 6 h, 1 h, 15 min before the trusted start bound | provisional | G2 |
| `stats.cluster_unit` | Event, with date-level sensitivity | fixed | — |

**Where the sample target and margins come from.** Under the simulation's planning assumptions (moves of 1–4 pp from each offset to the close), 1,500 events let a correct pipeline pass the full three-component check 1 in about 99% of replicates, while a level drift equal to the 0.5 pp intercept margin and a symmetric quadratic drift equal to the 1.6 pp curvature margin each fail it in more than 99%. Nearly all false alarms come from the 24-hour offset, where moves are largest. At 1,000 events the correct-pipeline pass rate falls to about 95% — exactly the requirement, with no room for sampling luck — which is why the target is 1,500. Margin and sample size move together: a full MLB regular season (about 2,430 games) supports tightening the curvature margin to 1.2 pp at the same pass rate. Tighten only before model results are inspected. These figures come from `tests/test_calibration_simulation.py`, and sign-off rests on its output, rerun with measured reference-series dispersion once B1 or A1 provides it. A partial historical subset may not reach the target, which is why B3's purchase is conditioned on it.

**Why liveness and quote age are separate parameters.** A continuously unchanged book on a live feed and a dead feed look identical in `tick`. Liveness is a hard precondition. Quote age measures economic staleness: it excludes only beyond a generous cap and is otherwise stratified.

---

## 11. Harness obligations to model evaluation

### 11.1 Scope boundary

Model-evaluation study design — preregistered analysis cohorts, headline estimands, chronological holdouts, multiple-testing control, model comparison, outcome corroboration, realized ROI on real fills and late-news information-time analysis — is specified in `docs/RESEARCH_PROTOCOL.md`. It governs claims about a signal model, which does not exist yet, and the harness does not implement it.

The harness must make that protocol possible later. That means three things now: report its own coverage honestly (§11.2), quantify uncertainty correctly in its own gates (§11.3), and preserve the evidence the protocol will need (§11.4).

### 11.2 Coverage waterfall

The waterfall is an instrument output, produced for every close family from V0 onward.

Report a complete waterfall for **all candidate events/signals**, not just scored entries:

```text
eligible scheduled events
  -> discovered at every required provider
  -> mapped and settlement-equivalent
  -> had trustworthy off resolution
  -> had admissible reference close (per definition)
  -> had timely entry quote / execution assumption
  -> scored observations and unique events
```

Provide counts and reasons at each stage, including missing data by league, venue, liquidity, weekday/time, event popularity and period before the game. A pipeline that scores only easy, liquid games does not establish an edge in obscure games merely because its overall coverage exceeds 90%.

### 11.3 Uncertainty in harness gates

Every acceptance criterion in §9 and §13 uses event-clustered intervals, with date-level sensitivity where events share news. Never treat twenty quotes on the same game as twenty independent events. Missing closes are measured results: disclose how unscoreable games differ from scored games, and never fill a missing close with a modeled price and call the result observed CLV.

### 11.4 Evidence to preserve

- Every emitted signal, including `would_bet = 0`, with model version or artifact hash, decision time, an as-of feature reference and feature-availability provenance (§6.1).
- Decision-quote lineage and labeled execution assumptions (§6.2).
- Stable event identities, dates and league game IDs, which are the clustering units.
- Settlements as append-only observations (§8.5).
- Timestamps of collected observations that can carry news — lifecycle and status changes, schedule revisions, listed-pitcher changes where collected — so information-time analysis is possible later.
- Close-definition, resolver, fee and run lineage for every score.

### 11.5 Dashboard and auditability

A Streamlit dashboard is sufficient initially. Every aggregate links to `scoring_run`, scored rows, their close-def/off-resolver versions and underlying raw data manifest. Show primary versus alternate benchmark estimates together, **never** a blended historical/live number. Separate scoreability/operations dashboards from model-performance dashboards so missing-data failures cannot be hidden by high observed CLV.

---

## 12. Operational reliability, security and reproducibility

- **Read-only by construction:** the running collector is incapable of order placement. Store narrowly scoped credentials outside code and archived payloads; rotate/revoke on compromise.
- **Access control:** honor jurisdiction, venue location checks and vendor terms. A `451` or other policy refusal is an alert and a collection gap, not a reason to evade controls. Revalidate legal/access assumptions when venues or operating location change.
- **Time health:** monitor NTP/clock offset, receive-to-provider timestamp lag and monotonic durations; refuse ultra-short cutoff claims when clock precision is insufficient.
- **Quota:** track request credits from provider headers; alert at a prespecified runway and degrade documented polling coverage before exhaustion rather than silently missing intervals.
- **Durability:** SSD, WAL maintenance, compressed archival checksums, daily backups, periodic restore drills, bounded raw-retention policy and disk-space alerts.
- **Observability:** surface open gaps, expected-but-missing subscriptions, heartbeat age, poll success, depth availability, unresolved mapping percentage, off disagreement, closed-book status and derived-run failure counts.
- **Reproducibility:** lock parser/scorer code, dependencies, fee schedule, parameter JSON and source-artifact hashes for each published result. A revised interpretation gets a new version/run.

A one-week collector pilot should measure raw archive growth, normalized row sizes, full-book snapshot costs and WAL/SSD behavior before fixing book-level and snapshot-retention policies.

---

## 13. Implementation plan and exit gates

**Governing rule:** stop at a failed gate, explain the missing capability or invalid assumption, and repair or rescope it. Build one audited end-to-end example early. Historical and live gates are related but not interchangeable.

**Starting point:** `src/clv/score/controls.py` and `tests/test_calibration_simulation.py` already exist and must keep passing. The first artifacts to build are the R0 recorder, then `migrations/0001_v0.sql` with its tests.

### R0 — Raw postseason recorder (now; the window closes with the MLB postseason)

**Why:** live Novig books for MLB games, which have authoritative retrospective first-pitch times, make the best possible golden game for V0, and they are the only such data available before the 2027 season. R0 also answers the top blocking access question (§15, question 3) with evidence rather than documentation.

**Key provisioning (one-time, manual, before R0 runs).** Creating a `trading::read` key requires signing with the management key. Do it once, from a trusted machine: open a subaccount if none exists, create the read key, and copy only the read key's private key to the recorder host. **The management private key never touches the recorder host**, or any machine that runs the recorder or the harness (§3.2). Record the read key's ID and creation date in `docs/vendor-capabilities.md`.

**Scope:** a standalone tool in `tools/raw_recorder/` with minimal dependencies.

- `NOVIG-V3` signing, validated against Novig's published sample signatures and `POST /v3/echo`, using the read key only.
- Select MLB postseason moneyline markets (a hand-maintained list is acceptable) and archive the catalog responses used.
- Subscribe to `book` and `trades` for those markets, retaining their included `lifecycle` data. Confirm the actual subscription acknowledgements: selection values name one channel per subject, so use separate connections if needed to retain both feeds without replacing a subscription. Archive acknowledgements and every frame in the §7.1 format, from as early as practical before first pitch through at least 30 minutes after it.
- Capture transport Ping/Pong evidence, and send/archive scheduled authoritative snapshot probes for quiet public channels using the §10 cadence and timeout. These are evidence collection only; R0 does not interpret sequences or repair gaps. Archive every REST request envelope and response/failure under its request ID.
- Log connection events. Reconnect with backoff and resubscribe, but never attempt gap repair: later replay detects gaps from the raw sequence numbers.
- **Kalshi:** poll the matching game-winner markets' order books through Kalshi's public market-data endpoints (no authentication; adapt the edge scanner's connector) every `r0.kalshi_poll_interval_s` during the same windows, archiving every response.
- **The Odds API:** poll `h2h`, region `us`, for MLB every `r0.odds_poll_interval_s` during game windows only, archiving every response and its quota headers. At 20 minutes over about six hours per game day that is 18 credits a day; across the remaining postseason it stays inside the 500-credit free tier, but with little margin, so stop polling below `r0.odds_quota_floor` and record why in an `event` frame.
- After each game, archive the MLB StatsAPI game feed and play-by-play for its `gamePk`.

**Why the extra venues.** With Novig alone, V0's entry can only be priced on Novig itself: check 2 cannot run and the cross-venue path is never traced. With all three, the golden game carries a reference, a cross-check venue and a real soft-book entry quote.

**Out of scope:** parsing, book reconstruction, SQLite and any order route.

**Gate R0:** at least one game captured end to end from all three sources; connection/restart/control-frame events and REST request associations preserved; the §7.1 archive verification fixtures pass; sealed manifests verify; and The Odds API credit spend is recorded. Written answers in `docs/vendor-capabilities.md`: does `trading::read` streaming work on an unfunded subaccount; observed transport Ping cadence and usable public-channel evidence; confirmation of per-market/per-channel sequencing and simultaneous book/trades subscriptions; lifecycle status values around first pitch; the distribution of receive time minus venue time. A live mismatch with the documented contract is recorded explicitly.

**If R0 misses the window:** V0 uses one historical game or a live game in another sport. Nothing else depends on R0.

### P0 — Feasibility and the measurement contract (in parallel with R0)

1. Freeze the v1 moneyline contract-equivalence rules, primary analysis population and vocabulary for observed quote, decision quote, close and fee-adjusted EV in `docs/measurement-contract.md`.
2. **B0:** check an actual sample of Novig and Kalshi historical dates, instruments and timestamp semantics, and MLB official starts, before buying any historical data. Include a doubleheader, a postponed game, a high-volume game and a thin market if available.
3. Confirm vendor permissions and the Kalshi historical endpoint shape. Recalculate current The Odds API pricing and live quota needs.

**Gate P0:** signed-off `docs/feasibility.md`; no unresolved ambiguity in the selected reference contract; a Track B verdict recorded as *proceed*, *proceed with a proven subset* or *stop*. The purchase itself is decided at B3. P0 requires no schema: schema design starts in V0, from data.

### V0 — One-game vertical slice

**Input:** an R0-captured postseason game if available; otherwise one historical game.

Write `migrations/0001_v0.sql` (the V0 stage of §8.5) with its §8.4 tests, and create `config/params.toml` from §10, carrying over the R0 values. Ingest the game from raw archive files, establish canonical event aliases and contract equivalence, record one entry — priced on the R0 soft-book or Kalshi capture where available, so the cross-venue path and check 2's per-entry comparison are exercised — and one no-bet signal, resolve the off from play-by-play and venue lifecycle, derive a candidate close, and calculate CLV, null EV and explicit exclusion reasons. Replay a corrected mapping to demonstrate immutable evidence and versioned recomputation. Commit the game as the first golden-game CI fixture.

**Gate V0:** a reviewer can trace every output number to the original raw frames and reproduce the result from a clean database, and the original score survives the mapping correction. Schema changes forced by real data are recorded in the changelog — expect some.

### Track A — Live collection (in-season sports)

**A1: Reference connector.** Novig v3 signing tests, catalog, websocket book/lifecycle/tape, sequencing, atomic snapshots, per-channel liveness, scoped gaps, raw archive and normalized tick/trade data. Introduces the A1 schema stage; the one-week storage pilot that sets `book.full_snapshot_interval_s` runs here. Kalshi joins as an independently stored cross-check after the primary path works.

**Gate A1:** 48 hours continuous on a representative league with forced mid-stream socket kill, process kill, channel stall, sequence skip and resubscribe. Every event produces the correct open/close gap or documented unscoreable interval; all resynchronizations begin with authoritative fresh snapshots. Report storage and depth coverage.

**A2: Off sources and resolution.** Add available official/status/scores/lifecycle observations and the versioned earliest-plausible-start resolver. Cross-check against retrospectively authoritative MLB examples — the R0 captures, where available — even though live development uses NFL/NBA/NHL.

**Gate A2:** prespecified source agreement/uncertainty report over a full slate, every disagreement investigated, and injected delayed/off-by-one-poll transitions rejected or widened appropriately. Do not use “95% agreement” alone as a proof of correctness when no trusted source exists.

**A3: Live close family.** Implement best-mid and multiscale depth walks, cutoff/staleness/gap rules, aligned alternative-venue closes and complete audit lineage.

**Gate A3:** one full slate with coverage waterfall and all unscoreable reasons; every manually audited sample is correct. Record scoreability by liquidity and time, not a standalone global missing-rate promise.

**A4: Live scoring leg.** The Odds API polled entries, successful-no-change evidence, per-book update times, quotas, model signal ingestion and latency-sensitivity estimates. Build on a free or low-cost allocation where feasible, increasing cadence only after stable measurement.

**Gate A4:** one week at target cadence and actual coverage with no unexplained gaps, quota use inside the §10 headroom budget, entry/cutoff causality tests passing, and all nonexecutability assumptions labeled.

### Track B — Historical reference and calibration

Ordered so that everything free, including the close machinery's own check, happens before money is spent.

**B0** is P0 item 2.

**B1: Reference import (free).** Import a representative proven subset of Novig daily trades and Kalshi history; reconcile one economic trade across dual-side and multi-source records; attach raw manifests; import MLB play-by-play first pitches. Produce the per-game coverage table.

**B2: Historical closes and the martingale check (free).** Implement the VWAP and/or genuine bid/ask-candle close families the sources support, with strictly pre-cutoff windows and insufficient-trade and candle-overlap exclusions. Introduce settlements, then run §9.2 check 1 (the fixed-side martingale test) and the §9.3 oracle sweep.

**Gate B2:** all three Check-1 components pass at the §10 margins at every offset and every binned-diagnostic flag has a decision record, or the affected close family is abandoned. Reproducible coverage table; no candle straddles the actual off; no side-duplicated trades.

**B3: Conditional paid entries.** Buy The Odds API historical `h2h` snapshots **only** for event windows that passed B1 and B2, and only if the resulting sample can meet the §10 sample target. Test whether provider `commence_time` or event IDs change; keep actual returned snapshot times and per-book `last_update` values; record real credit spend against the estimate. Then run §9.2 check 2.

**Gate B:** Gate B2 holds; Check 2 passes — slope within margin, every outlier resolved by entry ID as a `verified_market_difference`, verified rate within its ceiling; and historical/live comparability limits are shown in reporting. Track B validates calculations and controls, **not** the definitive live-book benchmark.

### Shared evidence gates — repeat for each close family

**G1: Mechanical and pipeline calibration.** All fixture, integrity and fault-injection tests pass (§9.4), including `tests/test_calibration_simulation.py`; all three components of Check 1 pass at the §10 margins and every binned-diagnostic flag has a decision record; and Check 2 passes — slope within margin, zero unresolved outliers for the current run, verified rate within its ceiling. Investigate any failure before model claims; never subtract an observed offset or mark a discovered pipeline error as “reviewed” to force a pass.

**G2: Contamination and benchmark sensitivity.** Conservative start bound; injected in-play leakage caught; oracle buffer sweep with *matched games* and intervals; quote-age and reference-venue/price-basis sensitivity documented. Lock the default buffer and primary benchmark on development data.

**G3: Reporting validity.** The coverage waterfall (§11.2) and event-clustered intervals (§11.3) are reproducible from a clean database. Model-performance claims additionally require the gates in `docs/RESEARCH_PROTOCOL.md`.

### Downstream

**D1: Analysis surface.** A small Streamlit app exposing scoreability, the preregistered primary CLV, robustness intervals, alternate reference venues, execution-delay sensitivity and raw-lineage drill-down.

Late-news analysis and outcome corroboration are specified in `docs/RESEARCH_PROTOCOL.md`.

### Calendar (tentative; dependent on gates)

| Period | Priority | Decision checkpoint |
|---|---|---|
| Now through the end of the MLB postseason | R0 recorder; P0 in parallel | At least one golden game captured; access questions answered |
| October–November 2026 | V0, A1, B1 | First reproducible game; reliable live collector |
| November–December 2026 | A2–A3, B2 | Off resolution and a defensible live close; historical close family passes or is abandoned |
| December 2026–March 2027 | B3 (conditional), A4, G1/G2, D1 | Purchase justified or cancelled; stable operational sample; benchmark locked |
| 2027 MLB season | Full live MLB G1–G3 | First authoritative-start, live-depth prospective evaluation |

Do not peg the definitive live MLB gate to an assumed exact Opening Day until the published 2027 schedule and venue coverage are verified.

---

## 14. Repository and test layout

Keep the harness in a separate repository and database from the existing edge scanner. Copy useful connector shells rather than extracting a shared package prematurely. Operational separation matters: a missed scanner quote loses an opportunity; a missed harness observation alters a measurement.

```text
clv-harness/
  DESIGN.md                     # this document
  README.md                     # overview, install (uv), usage
  pyproject.toml                # uv workspace root: the clv package; tools/raw_recorder is a member
  uv.lock
  config/
    params.toml                 # created in V0 from §10; authoritative thereafter
  migrations/
    0001_v0.sql                 # one file per §8.5 stage, each with its tests
  docs/
    vendor-capabilities.md      # vendor facts with status + dated discrepancy log
    RESEARCH_PROTOCOL.md        # model evaluation; inactive until a model exists
    feasibility.md              # P0 verdict
    measurement-contract.md     # v1 contract rules, close, entry & cohort policy
    calibration-report.md       # fixture and real-data gate results
    decisions/                  # dated decision records (§10, §15)
  tools/
    raw_recorder/               # R0: standalone uv workspace member; writes the §7.1 archive format
      config.toml               # R0 values copied from §10
      games.toml                # hand-maintained games, Novig markets and Kalshi tickers to record
      src/raw_recorder/         # archive, REST envelope, Novig signing/stream, pollers, CLI
      tests/                    # §7.1 archive verification fixtures, signing vectors
  src/clv/
    config.py                   # loads params.toml; no threshold defined elsewhere
    db.py
    lineage.py                  # fact snapshots and immutable run manifests
    archive.py                  # reads/writes the §7.1 format, hashes, manifests
    identity.py                 # event aliases, mappings, rule equivalence
    gaps.py                     # gap pairs, heartbeat and poll continuity
    venues/
      protocol.py
      novig/{signing,catalog,stream,parser}.py
      kalshi.py
    scoring/
      odds_api.py               # live quotes, quota and successful poll evidence
    backfill/
      novig_public.py
      kalshi_hist.py
      odds_api_hist.py
      mlb_pbp.py
      reconcile_trades.py
    off/{sources,resolver}.py
    close/{definitions,depth,vwap,candles}.py
    fee/{registry,estimate}.py
    score/{clv,controls}.py     # controls: martingale test, agreement, residual summary, oracle
    dashboard/app.py
  tests/
    fixtures/                   # golden games (the first from V0) and synthetic fault streams
    test_params.py              # every §10 key present, with status and revised_by
    test_schema_invariants.py
    test_identity_and_rules.py
    test_polarity.py
    test_raw_replay.py
    test_clock_and_causality.py
    test_gaps_and_heartbeats.py
    test_trade_reconciliation.py
    test_off_resolution.py
    test_close_depth_and_candle.py
    test_entry_execution.py
    test_controls_and_fault_injection.py
    test_calibration_simulation.py   # §9.4 items 1, 2, 11–22; gates any change to §9.2
    test_research_bound_specification.py # executable arithmetic fixtures; no model pipeline
```

Have the CI suite prove that a deliberately broken pipeline **fails** the relevant control. Include deterministic property-based tests where possible (polarity involution, no post-cutoff source use, append-only supersession). Keep manual golden-game checks as a documented acceptance artifact, not a one-off debugging exercise. `test_cohort_statistics.py` arrives with the research protocol.

---

## 15. Open questions and decisions to record

**Block R0 / P0 / V0.** R0 answers question 3 and part of question 5 with evidence; record the answers in `docs/vendor-capabilities.md`.

1. Which exact MLB reference contract is settlement-equivalent to the desired soft-book moneyline? What are the exclusions for listed pitchers, postponed games and doubleheaders?
2. How much historical 2026 MLB **reference** data is genuinely available, with what timestamp and candle meaning? Is a bounded subset sufficient to make B2 worthwhile?
3. Does Novig v3 read-only streaming work without funding, and what are its actual sequence/heartbeat and lifecycle contracts?
4. Which observations are sufficient to define the conservative earliest-plausible start for the first live non-MLB league?
5. What precision does each native venue expose for quotes, fees and timestamps? Are one-basis-point probabilities and millisecond timestamps lossless for the selected products?

**Resolve from the live pilot, before G2**

6. Can the reference venue supply both sides of the book at $100/$500/$1,000 often enough? Should the prespecified headline notional or league coverage change **before** examining candidate-model performance?
7. How old may a book observation be at each sport's cutoff while retaining meaningful economic freshness? What heartbeat and provider-lag policies are enforceable?
8. How should official start, first market in-play response and venue lifecycle transitions be bounded when they disagree? What is the minimum conservative buffer after uncertainty is included?
9. Can public trade rows be paired exactly with live tape and daily volume summaries, or must ambiguous executions be excluded from VWAP?
10. Can a quoted historical soft-book price be associated with its underlying last-update timestamp, or must historical execution analysis remain unavailable?

**Resolve before model-performance claims:** see `docs/RESEARCH_PROTOCOL.md` §7.

Each resolved question becomes a dated decision record with evidence and a code/config version. A decision that changes the metric must create a new `close_def`, resolver or analysis specification, never mutate the meaning of a previously reported result.
