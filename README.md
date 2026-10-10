# CLV Harness

Measurement infrastructure for evaluating sports-betting entries by **closing-line value (CLV)**: did an entry get a better price than an independent, defensible estimate of the same outcome's pre-event fair probability? It is deliberately built **before** any betting model. Its job is to make a claimed edge testable and to expose apparent edge that depends on timestamp errors, stale prices, mismatched contracts, illiquid reference markets or research choices.

**Status: V0 in progress.** The R0 raw postseason recorder is built and recording. V0 so far has the parameters (`config/params.toml`) and parsers that read the raw archive; the schema, ingestion, closes and scoring come next. See the implementation plan in [DESIGN.md §13](DESIGN.md).

| Document | What it is |
|---|---|
| [DESIGN.md](DESIGN.md) | The design: measurement contract, venues, archive format, schema, calibration, parameters (§10), plan (§13) |
| [docs/vendor-capabilities.md](docs/vendor-capabilities.md) | Vendor facts with verification status and dates, plus a discrepancy log |
| [docs/measurement-contract.md](docs/measurement-contract.md) | The v1 measurement contract: settlement states, venue equivalence, population |
| [docs/feasibility.md](docs/feasibility.md) | The P0 feasibility verdict and the B0 historical sample |
| [docs/v0-data-shapes.md](docs/v0-data-shapes.md) | What the recorded golden-game candidate looks like when parsed, and what that means for the V0 schema |
| [docs/RESEARCH_PROTOCOL.md](docs/RESEARCH_PROTOCOL.md) | The model-evaluation protocol; inactive until a signal model exists |
| [docs/decisions/](docs/decisions/) | Dated decision records explaining why the design is what it is |

## Codebase map

```text
pyproject.toml              uv workspace root: the `clv` harness package
config/params.toml          every threshold the harness uses, from DESIGN.md §10; authoritative
src/clv/
  config.py                 loads params.toml; an unknown key is an error, never a default
  archive.py                reads sealed §7.1 segments, checks their hashes, cites frames, joins REST envelopes
  venues/protocol.py        normalized two-sided books: native prices and quantities, bids per venue side
  venues/novig/parser.py    Novig stream replay (snapshot + deltas, sequence and probe checks), public book, catalog
  venues/kalshi.py          Kalshi order-book polls and market listings
  scoring/odds_api.py       The Odds API quotes, exact American-to-decimal odds, credits
  mlb.py                    StatsAPI game identity, result and first-play times
  off/sources.py            off observations: first pitch, status changes, Novig GOLIVE
  gaps.py                   collection gaps from polls and streams
  cli.py                    `clv inspect`
  score/controls.py         statistical core of the calibration checks (DESIGN.md §9.2)
tests/                      parameters, archive reader, parsers, gaps; calibration simulation and research-bound fixtures
tools/raw_recorder/         R0 recorder: a standalone workspace member (aiohttp + cryptography only)
  config.toml               recorder parameters, each copied from DESIGN.md §10
  games.toml                the games, Novig markets and Kalshi tickers to record
  config.paper.toml         Novig Paper (play money) test configuration: own archive, Novig only
  games.paper.toml          the same games under Paper's market IDs
  src/raw_recorder/
    archive.py              §7.1 archive: append, rotate, seal, journal, read, verify
    rest.py                 REST envelope: every request, body and failure under its request_id
    novig/signing.py        NOVIG-V3 request signing (checked against Novig's 30 published vectors)
    novig/stream.py         Novig WebSocket recorder: control frames, quiet-channel probes, reconnect
    novig/public.py         Novig unsigned catalog and public order book
    provision.py            `novig-provision`: management key -> trading::read key (never part of the recorder)
    kalshi.py, odds_api.py, mlb.py   pollers for Kalshi order books, The Odds API, MLB StatsAPI
    scheduler.py, runtime.py, cli.py capture windows, the long-running process, commands
  tests/                    archive, REST, WebSocket and runtime fixtures (local fakes, no network)
tools/b0/                   P0 B0 research scripts that reproduce docs/feasibility.md (cache in b0-work/)
deploy/raw-recorder.service systemd user unit
archive/                    raw evidence (git-ignored; back it up separately)
```

The layout the harness grows into is in [DESIGN.md §14](DESIGN.md). Tables appear stage by stage (§8.5); the harness has none yet: the V0 migration comes next.

## Install

The project uses [uv](https://docs.astral.sh/uv/) for Python, virtual environments and dependencies. uv installs the pinned Python (3.12, see `.python-version`) by itself.

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

```bash
uv sync
```

`uv sync` creates `.venv/` with both packages and the dev tools. To install only the recorder on a capture host:

```bash
uv sync --package raw-recorder
```

## Test

```bash
uv run pytest
```

To print the calibration operating characteristics, which back the §10 margins:

```bash
uv run pytest -s tests/test_calibration_simulation.py
```

## Reading a recorded game

```bash
uv run clv inspect --game-pk 849832
```

`inspect` runs every V0 parser over one game's capture window from `tools/raw_recorder/games.toml` and prints what each source shows: the Novig stream replay and its checks, Kalshi and Odds API polls, StatsAPI identity and result, off observations, the books at first pitch minus `close.buffer_s`, and collection gaps. It reads sealed segments only, checks each one's hash, and writes nothing. The Novig stream for a two-hour game takes about 15 s.

## The R0 recorder

R0 captures raw evidence for MLB postseason moneylines from four sources, in the [DESIGN.md §7.1](DESIGN.md) archive format:

- **Novig:** the `book` and `trades` WebSocket channels, with lifecycle data, using a read-only key. Without a key it falls back to polling the unsigned public order book.
- **Kalshi:** public game-winner order books.
- **The Odds API:** `h2h` soft-book quotes, pregame only, stopping at a credit floor.
- **MLB StatsAPI:** the game feed and play-by-play.

It never parses books, repairs gaps or places orders. V0 replays what it records.

### Secrets

The recorder reads secrets from environment variables, never from files in the repo. All of them are optional:

| Variable | What | Without it |
|---|---|---|
| `NOVIG_READ_KEY_ID` | UUID of a Novig **`trading::read`** key | Novig falls back to the public book poll |
| `NOVIG_READ_KEY_PATH` | Path to that key's PKCS#8 PEM, `chmod 600` | (same) |
| `NOVIG_PAPER_READ_KEY_ID`, `NOVIG_PAPER_READ_KEY_PATH` | The same, for a Novig **Paper** key (`config.paper.toml` only) | Paper uses the public book |
| `ODDS_API_KEY` | The Odds API key (the free tier is enough for R0) | Odds polling is disabled and the reason recorded |

**Getting a Novig read key ([DESIGN.md §13](DESIGN.md)).** You need a management key first. You create it in the web app at Settings → Novig API, which downloads a `.pem`. On **Production**, that entry appears only after Novig enables the API on your account (see `docs/decisions/2026-10-07-novig-paper.md`; ask developers@novig.com). On **Paper** (`paper.novig.com`, play money) it is always there.

Then mint the read key with `novig-provision`. It opens a subaccount, keeping no key that can trade, creates a `trading::read` key, writes it with `chmod 600`, checks it, and prints the environment lines to set:

```bash
uv run novig-provision --env paper --management-key-id <management key ID> --management-key ~/novig-paper-mgmt.pem --out ~/.config/raw-recorder/novig-paper-read.pem
```

For Production, run it with `--env production`. The recorder never loads the management key, and the key must not stay on the recorder host. Once `raw-recorder echo` succeeds with the new read key, remove the management `.pem` from the host. Keep it offline if you want it again ([decision record](docs/decisions/2026-10-08-single-host-provisioning.md)). Record the Production read key's ID and creation date in `docs/vendor-capabilities.md`.

Then check the key:

```bash
uv run raw-recorder echo
```

`echo` sends a signed `POST /v3/echo`, reads `/v3/limits`, and confirms the key is not a management key. Everything it does is archived. For the Paper key, add `--config tools/raw_recorder/config.paper.toml` (it goes before the subcommand).

**Testing on Paper.** `--config tools/raw_recorder/config.paper.toml` switches every command to Paper. It uses Paper's host, `NOVIG_PAPER_READ_KEY_*`, `games.paper.toml`, and its own archive, `archive-paper/`, and it records only Novig. Paper books are play money: they test the signer and stream against the real API, never the measurement. A Paper recorder can run alongside the Production one.

```bash
uv run raw-recorder --config tools/raw_recorder/config.paper.toml probe-subscriptions --market <paper market id>
```

```bash
uv run raw-recorder --config tools/raw_recorder/config.paper.toml run
```

### Choosing games

```bash
uv run raw-recorder catalog --days 2
```

This archives the StatsAPI schedule and the Novig and Kalshi catalogs, then prints suggested `[[game]]` entries. Check each match by hand and paste it into `tools/raw_recorder/games.toml`. Each game records from 3 h before scheduled first pitch to 90 min after it. Odds polling runs only during the 3 h before first pitch. A game can override its window (`capture_lead_s`, `capture_tail_s`), for example for a rain delay.

### Recording

```bash
uv run raw-recorder run
```

`run` stays up, captures every game whose window is open, and idles in between. While a window is open it holds a systemd sleep inhibitor, so keep the host on through those windows. If the host goes down anyway, the gap is recorded rather than hidden. Stop it with Ctrl-C or SIGTERM: it writes a `process_stop` event and seals everything. A killed process is recovered on the next start, which seals the orphaned segments and records `previous_session_unclean`.

To run it as a service:

```bash
cp deploy/raw-recorder.service ~/.config/systemd/user/
```

```bash
systemctl --user enable --now raw-recorder.service
```

Put the secrets in `~/.config/raw-recorder/env` (`chmod 600`). See the comments in the unit file.

### After a game

| Command | What it does |
|---|---|
| `uv run raw-recorder mlb-feed --date 2026-10-07` | Re-archive StatsAPI feeds, e.g. the next day for final data and corrections |
| `uv run raw-recorder seal` | Seal active segments (a running recorder gets SIGHUP and rotates) |
| `uv run raw-recorder verify` | Recheck every sealed file's hash, size and frame count against its journal |
| `uv run raw-recorder report` | Volume per source and day, Odds credit spend, Novig Ping and probe counts |
| `uv run raw-recorder probe-subscriptions --market <id>` | One-off experiment: can one Novig connection carry `book` and `trades`? |

`echo`, `catalog`, `mlb-feed` and `probe-subscriptions` write to the archive, so they refuse to run while `run` holds it.

### The archive

```text
archive/<source>/<YYYY-MM-DD UTC>/<stream_id>.<segment>.jsonl.gz   sealed, read-only
archive/<source>/<YYYY-MM-DD UTC>/manifest.jsonl                   append-only journal of sealed files
```

Each line is `{"recv_ts_ms", "conn_id", "dir": "in"|"out"|"event", "frame"}`. `frame` is always a string holding exactly the received text. REST records use `conn_id = request_id`, so every response stays tied to its request. Sealed files never change, so `rsync -a archive/ <backup>/` is a complete incremental backup.

## Working rules

- **No threshold outside [DESIGN.md §10](DESIGN.md).** Recorder values are copied from it into `config.toml`. V0 creates `config/params.toml` from it.
- **Decisions go in dated records** in `docs/decisions/`. DESIGN.md describes only the current design.
- **§9.2 and the §10 control margins change only together with `tests/test_calibration_simulation.py`**, and that test must pass.
- **Vendor facts go in `docs/vendor-capabilities.md`** with a status and a date. A discrepancy gets a dated log row, never a silent edit.
