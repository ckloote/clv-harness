# B0 — historical data sample (P0)

These scripts produce the evidence in [docs/feasibility.md](../../docs/feasibility.md). They are research scripts, not harness code: B1 builds the real importers.

```bash
uv run python tools/b0/fetch.py
```

```bash
uv run python tools/b0/coverage.py
```

```bash
uv run python tools/b0/reconcile.py
```

| Script | What it does |
|---|---|
| `fetch.py` | Downloads and caches every input: Novig daily files for the sample days, Novig `/v3/history` markets and events (signed with the recorder's `trading::read` key), the StatsAPI schedule, teams and play-by-play, and the Kalshi `KXMLBGAME` events listing. Writes `sample_games.json` |
| `coverage.py` | Builds the feasibility §2 table: Novig pre-cutoff fills and VWAP, and Kalshi bid/ask and trades at each sample game's cutoff. Fetches and caches Kalshi candles and trades |
| `reconcile.py` | Matches Novig's streamed trades for ALDS G4 (from `archive/novig`) to the daily `trades.csv` |
| `common.py` | The sample definition, hand-made Kalshi mappings, Novig name aliases, the DESIGN.md §10 values used, and HTTP helpers |

**Work directory.** Everything goes in `b0-work/` (git-ignored; `--work` to change it). That's about 450 MB, mostly Novig trade files. Fetches are cached, so a rerun is offline and reads the same bytes; delete a file to refetch it. The sha256 of the main inputs is in feasibility.md §7.

**The Kalshi mappings are made by hand.** Kalshi ticker dates and times don't identify the game, so `KALSHI_MANUAL` in `common.py` maps each edge-case game from its settlement result and `close_time`. Ordinary games on the sample date are matched on date and teams, which is unambiguous there and nowhere else.

**The read key.** `fetch.py` reads `NOVIG_READ_KEY_ID` and `NOVIG_READ_KEY_PATH` from the environment, or from `~/.config/raw-recorder/env`. It makes read-only calls to the `history` bucket, separate from the recorder's `stream` bucket.
