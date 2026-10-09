# Track B verdict: proceed with a proven subset

Date: 2026-10-09

Status: accepted. Gate P0 stays open until feasibility §6 items 1–2 are resolved or deferred.

## Evidence

The B0 sample in `docs/feasibility.md` covers 24 games, every one checked against StatsAPI, Novig and Kalshi. It is reproducible with `tools/b0/`. In short:
- **Novig's public trades start 2026-08-03,** at most about 770 MLB games. That is too few for the 1,500-event `controls.sample_events` target.
- **Kalshi covers 2025-04-16 onward,** with 1-minute bid/ask candles and individual trades.
- **Both are usable at the close.** Every played sample game had a VWAP-eligible Novig window and a 1–2 cent Kalshi spread at the cutoff, and the two agreed within 1.4 pp.
- **Kalshi ticker dates and times don't identify the game,** so Kalshi mappings are made by hand.

## Decision

- **B1 imports two reference sources:**
  - **Kalshi `KXMLBGAME`, 2025–2026:** trades and 1-minute bid/ask candles. This is the family that can reach the B2 sample target. Each mapping is `manual_verified` from settlement result and `close_time`, or excluded.
  - **Novig trades from 2026-08-03:** STRAIGHT MAKER rows only, mapped through `/v3/history`. A separate close family, reported on its own.
- **B2 runs check 1 on each family independently.** A family below the sample target is recorded as `insufficient_data`, never passed.
- **No historical data is bought.** B3 (The Odds API history) is decided after B2, as DESIGN.md §13 requires.

## Consequences

- **Kalshi historical closes are top-of-book only.** Candles carry no depth, so the historical families use `mid_top` and VWAP. Depth benchmarks stay live-only.
- **Track B validates the calculations and controls on Kalshi, not on the live Novig reference** (DESIGN.md §13, Gate B). Novig's own historical family is a smaller cross-check.
