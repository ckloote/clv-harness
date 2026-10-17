# CLV Harness

Measurement infrastructure for evaluating sports-betting entries by **closing-line value (CLV)**: did an entry get a better price than an independent, defensible estimate of the same outcome's pre-event fair probability? It is deliberately built **before** any betting model. Its job is to make a claimed edge testable and to expose apparent edge that depends on timestamp errors, stale prices, mismatched contracts, illiquid reference markets or research choices.

**Status: V0 built, Track B closed, A1 next.**

- **R0:** the raw postseason recorder is built and recording.
- **V0:** the parameters (`config/params.toml`), parsers that read the raw archive, the V0 schema (`migrations/`), an ingest of one recorded game, and its off resolution, closes, CLV and null EV scores and check 2's per-entry comparison. All of it is traced to raw frames, with a mapping-correction replay and a golden game in CI ([results](docs/decisions/2026-10-10-v0-golden-game.md)).
- **Track B:** B1 imported two seasons of Kalshi and Novig history (4,946 games). B2 calibrated the historical close families, and Gate B2 was not met: the Kalshi candle family failed check 1 at 24 h ([results](docs/decisions/2026-10-16-b2-prespecification.md)). B3 is cancelled, and Track B is closed without a historical benchmark ([decision](docs/decisions/2026-10-16-track-b-closed.md)).
- **Next:** A1, the live Novig reference connector ([scope](docs/decisions/2026-10-16-a1-scope.md)).

See the implementation plan in [DESIGN.md §13](DESIGN.md).

| Document | What it is |
|---|---|
| [DESIGN.md](DESIGN.md) | The design: measurement contract, venues, archive format, schema, calibration, parameters (§10), plan (§13) |
| [docs/vendor-capabilities.md](docs/vendor-capabilities.md) | Vendor facts with verification status and dates, plus a discrepancy log |
| [docs/measurement-contract.md](docs/measurement-contract.md) | The v1 measurement contract: settlement states, venue equivalence, population |
| [docs/measurement-contract-nba.md](docs/measurement-contract-nba.md) | The NBA version of the contract's §2–§3 (draft), for Track A |
| [docs/feasibility.md](docs/feasibility.md) | The P0 feasibility verdict and the B0 historical sample |
| [docs/v0-data-shapes.md](docs/v0-data-shapes.md) | What the recorded golden-game candidate looks like when parsed, and what that means for the V0 schema |
| [docs/RESEARCH_PROTOCOL.md](docs/RESEARCH_PROTOCOL.md) | The model-evaluation protocol; inactive until a signal model exists |
| [docs/decisions/](docs/decisions/) | Dated decision records explaining why the design is what it is |

## Codebase map

```text
pyproject.toml              uv workspace root: the `clv` harness package
config/params.toml          every threshold the harness uses, from DESIGN.md §10; authoritative
migrations/0001_v0.sql      the V0 schema: facts and derived tables, constraints, append-only triggers
migrations/0002_v0_scoring.sql  V0 changes forced by scoring: a close with no cutoff, null EV lineage
migrations/0003_v0_agreement.sql  V0 check 2 per entry: the opposite quote, q_e and the outlier flag
migrations/0004_b1.sql      B1: executions and their sightings, historical candles
migrations/0005_b2.sql      B2: settlements, historical close lineage, calibration runs
migrations/CHANGELOG.md     schema changes forced by real data, against DESIGN.md §8
src/clv/
  config.py                 loads params.toml; an unknown key is an error, never a default
  db.py                     connection settings and the migration runner
  ingest.py                 one recorded game from the raw archive into the facts, each row citing its frame
  identity.py               team naming across venues, polarity normalization, current mappings and corrections
  lineage.py                fact snapshots: the exact facts a derived run reads
  entries.py                signals and entries from a spec file, each quote cited to its frame
  games.py                  the recorder's game lists: hand-checked (games.toml) and discovered
  archive.py                reads sealed §7.1 segments, checks their hashes, cites frames, joins REST envelopes
  venues/protocol.py        normalized two-sided books: native prices and quantities, bids per venue side
  venues/novig/parser.py    Novig stream replay (snapshot + deltas, sequence and probe checks), public book, catalog
  venues/kalshi.py          Kalshi order-book polls and market listings
  scoring/odds_api.py       The Odds API quotes, exact American-to-decimal odds, credits
  mlb.py                    StatsAPI game identity, result and first-play times
  off/sources.py            off observations: first pitch, status changes, Novig GOLIVE
  off/resolver.py           the trusted start interval from those observations
  close/depth.py            benchmarks: top-of-book and depth-walk midpoints, exact
  close/definitions.py      the versioned close functions, live and historical, and their unscoreable reasons
  close/candles.py          the historical candle close: the last candle wholly before the cutoff
  close/vwap.py             trade VWAP over unique executions, timed by the stream where it saw them
  backfill/                 B1 historical import: games, markets, links, candles, trades, settlements, coverage
  score/clv.py              scoring runs: mapping and off resolution, closes, CLV, null EV, exclusions
  trace.py                  a scoring run traced to the raw frames behind every number
  gaps.py                   collection gaps from polls and streams
  gate/a1.py                Gate A1: each injected fault's gap checked against the facts; storage and depth
  pilot.py                  the A1 storage pilot report: storage, probes, quiet intervals, cutoffs, depth, listing lead
  cli.py                    `clv inspect`, `migrate`, `ingest`, `record`, `correct`, `score`, `trace`, `backfill`,
                            `calibrate`, `gate`
  score/controls.py         statistical core of the calibration checks (DESIGN.md §9.2) and the oracle mean
  score/simulation.py       the calibration simulation's reference-series generator and item-12 rerun
  score/calibration.py      B2 calibration runs: closes at every buffer, offset prices, check 1, oracle, report
tests/                      parameters, schema invariants, polarity, archive reader, parsers, gaps, ingest,
                            off resolution, depth, scoring; calibration simulation and research-bound fixtures
  golden.py                 golden games: the synthetic generator and the end-to-end replay
  fixtures/golden/          the synthetic golden game (archive included) and 849832's specs and trace
tools/raw_recorder/         R0 recorder: a standalone workspace member (aiohttp + cryptography only)
  config.toml               recorder parameters, each copied from DESIGN.md §10
  games.toml                hand-checked games, Novig markets and Kalshi tickers; override discovery
  config.paper.toml         Novig Paper (play money) test configuration: own archive, Novig only
  config.paper-rehearsal.toml   Paper with the Gate A1 fault harness on: the A1 rehearsal on the Pi
  config.gate.toml          Production with the Gate A1 fault harness on: the gate run
  src/raw_recorder/
    archive.py              §7.1 archive: append, rotate, seal, journal, read, verify
    rest.py                 REST envelope: every request, body and failure under its request_id
    novig/signing.py        NOVIG-V3 request signing (checked against Novig's 30 published vectors)
    novig/stream.py         Novig WebSocket recorder: control frames, quiet-channel probes, resyncs, reconnect
    faults.py               Gate A1 fault injection: the relay, the recorder's control socket, the harness
    novig/public.py         Novig unsigned catalog and public order book
    provision.py            `novig-provision`: management key -> trading::read key (never part of the recorder)
    kalshi.py, odds_api.py, mlb.py   pollers for Kalshi order books, The Odds API, MLB StatsAPI
    discovery.py            finds the games to record from the schedule and catalogs (archived)
    scheduler.py, runtime.py, cli.py capture windows, the long-running process, commands
    status.py               `raw-recorder status`: is the current game being captured, source by source
  tests/                    archive, REST, WebSocket and runtime fixtures (local fakes, no network)
tools/b0/                   P0 B0 research scripts that reproduce docs/feasibility.md (cache in b0-work/)
deploy/raw-recorder.service systemd user unit (the workstation, R0)
deploy/raw-recorder-pi.service, raw-recorder-relay.service   the A1 host's recorder and Gate A1 relay (system units)
deploy/raw-recorder-paper.service, raw-recorder-paper-relay.service   the same on Paper, for the A1 rehearsal
deploy/raw-recorder-gate.conf   drop-in: the A1 host's recorder on config.gate.toml, for the gate run
deploy/archive-mirror.*     the workstation's mirror of the recorder host's sealed archive: script, service, timer
deploy/pi-host.md           runbook: setting up the Pi, the mirror, the cutover, the Paper rehearsal, the gate run
archive/                    raw evidence (git-ignored; back it up separately)
```

The layout the harness grows into is in [DESIGN.md §14](DESIGN.md). Tables appear stage by stage (§8.5): `0001_v0.sql` is the V0 stage, `0002` and `0003` are the changes scoring and check 2 forced, and `0004` and `0005` are the B1 and B2 stages.

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

`inspect` runs every V0 parser over one game's capture window (from `tools/raw_recorder/games.toml`, or the recorder's discovered list in the archive) and prints what each source shows: the Novig stream replay and its checks, Kalshi and Odds API polls, StatsAPI identity and result, off observations, the books at first pitch minus `close.buffer_s`, and collection gaps. It reads sealed segments only, checks each one's hash, and writes nothing. The Novig stream for a two-hour game takes about 15 s.

## Loading a recorded game

```bash
uv run clv ingest --game-pk 849832
```

`ingest` creates or upgrades `data/clv.sqlite` (git-ignored; `--db` to change it) and loads the game's facts: identity and mappings, Novig and Kalshi book states, liveness evidence, collection gaps and off observations. Each row cites the sealed segment and line it came from. It refuses a game that is already loaded, since a clean database must reproduce it exactly. For the same reason it refuses a game until the archive's seal watermark is past the end of its capture window (see league events below). Loading part of a window would leave the rest out for good. An archive written only by a recorder older than the watermark has none, so its games load once a current recorder has run on it, or from a sealed mirror. Game 4 takes about 15 s and makes a 24 MB database. `clv migrate` alone creates or upgrades the database.

**League events (A1).** A league event is loaded from the venue side only: its Novig instruments, books, liveness evidence and collection gaps, with no event, outcome, mapping or off observation. Which league game it is gets decided in A2.

```bash
uv run clv ingest --league-events
```

This loads every event in `archive/events.discovered.toml` that is not loaded yet and whose window has settled, meaning the archive's seal watermark (`archive/sealed_through.json`, written by the recorder and published by the mirror last) is past the window's end. Elapsed time alone never settles a window, since a mirror can lag. Events not yet settled are left for a later run, so it is safe to run on a timer. A `venue_ingest` row records each one, and `--league-event <Novig event ID>` loads a single one.

## Scoring a recorded game

```bash
uv run clv record --spec tests/fixtures/golden/849832/entries.toml
```

```bash
uv run clv score
```

`record` writes the spec's signals and entries: each entry's decision quote is the latest one the archive had received by the decision time, cited to its frame. `score` takes a fact snapshot and writes one run against it: every mapping's eligibility, each game's off resolution, the closes per definition, reference venue and outcome, and per entry CLV, null EV and the residual, as exact fractions with every exclusion reason listed, and check 2's per-entry comparison of the entry book's de-vigged price with the reference at the decision time. It prints the scores.

```bash
uv run clv correct --spec tests/fixtures/golden/849832/corrections.toml
```

```bash
uv run clv score
```

```bash
uv run clv score --fact-snapshot 1
```

A correction appends a superseding mapping and rewrites nothing. A new run shows its effect, and `--fact-snapshot 1` recomputes the first run's facts as a new run, which reproduces the first run exactly.

```bash
uv run clv trace --run 1 > run1.json
```

`trace` prints a run as JSON with no database ids: the off claims, books, closes and scores, each number with the raw frame it came from and the sha256 of that frame's line. `tests/golden.py` replays a golden game this way from a clean database; `uv run python tests/golden.py` regenerates the committed golden outputs after a deliberate change.

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
| `ODDS_API_KEY` | The Odds API key (the free tier is enough for R0) | Odds polling is disabled, the reason recorded, and a warning printed (below) |

A missing key or disabled source is printed as `raw-recorder: WARNING: …` at startup and again whenever a capture window opens. Under systemd it is logged at warning priority, so `systemctl --user status raw-recorder` and `journalctl --user -u raw-recorder -p warning` show it. `raw-recorder status` reports it as a FAIL.

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

**Testing on Paper.** `--config tools/raw_recorder/config.paper.toml` switches every command to Paper. It uses Paper's host, `NOVIG_PAPER_READ_KEY_*`, and its own archive, `archive-paper/`, discovers Paper's markets itself, and records only Novig. Paper books are play money: they test the signer and stream against the real API, never the measurement. A Paper recorder can run alongside the Production one. It also discovers NBA (`[[league]]`), which Paper lists as Production does, so it rehearses the A1 path. `config.paper-rehearsal.toml` adds the Gate A1 fault harness and records NBA only (MLB discovery off), for the rehearsal on the Pi ([deploy/pi-host.md](deploy/pi-host.md) section 9).

```bash
uv run raw-recorder --config tools/raw_recorder/config.paper.toml probe-subscriptions --market <paper market id>
```

```bash
uv run raw-recorder --config tools/raw_recorder/config.paper.toml run
```

### Choosing games

The recorder finds games itself. At startup and then hourly it reads the StatsAPI schedule for today and tomorrow (US Eastern) and the Novig and Kalshi catalogs, archiving every response. It matches each postseason game (`[discovery] game_types`) to its markets:
- **Novig:** the `MONEY` market whose outcome names are the two teams and whose `startsTs` is within 6 h of the StatsAPI start.
- **Kalshi:** the event on the game's Eastern date whose two tickers name the two teams.

It writes the result to `archive/games.discovered.toml` and starts recording at once; no restart and no commit. If the venues disagree on the start time, the window stretches to cover every claimed start. If a match is ambiguous (a doubleheader, say), it records every candidate and prints a warning.

Each game records from 3 h before scheduled first pitch to 90 min after it. Odds polling runs only during the 3 h before first pitch.

Discovered links are rule matches, so `clv ingest` loads them as `fuzzy_candidate`: outside the primary cohort until a `corrections.toml` verifies them for the game you score. To override discovery, or to add a game it can't find, put a hand-checked `[[game]]` entry in `tools/raw_recorder/games.toml` and commit it. That entry replaces the discovered one whole, and its links ingest as `manual_verified`. The recorder rereads both files within seconds of a change. An entry can override its window (`capture_lead_s`, `capture_tail_s`), for example for a rain delay.

```bash
uv run raw-recorder games
```

`games` is read-only and lists what the recorder will record over the next two days: each game's source, windows in ET, markets and notes, and when discovery last ran. It exits 1 if discovery is stale or a `games.toml` entry hides markets that discovery found. With no recorder running, `uv run raw-recorder catalog` runs one discovery round by hand.

**Other leagues (A1).** Each enabled `[[league]]` in `config.toml` is found from Novig's catalog alone, with no schedule match. Every discovery interval the recorder reads the league's events and `MONEY` markets (archived), lists each event with a moneyline that starts within `days_ahead`, and merges it into `archive/events.discovered.toml` under the same never-lose-capture rules. An event records on Novig only, from `a1.capture_lead_s` (25 h) before its `startsTs` so the 24 h offset is captured, to `a1.capture_tail_s` (4 h) after. Which league game it is gets decided in A2, never here. NBA is committed with `enabled = false` until the A1 cutover (`docs/decisions/2026-10-16-a1-host.md`). `games` lists league events too, and `uv run raw-recorder status --event <Novig event ID>` checks one.

### Recording

```bash
uv run raw-recorder run
```

`run` stays up, captures every game whose window is open, and idles in between. While a window is open it holds a systemd sleep inhibitor, so keep the host on through those windows. If the host goes down or suspends anyway (closing a laptop lid suspends it regardless), the gap is recorded rather than hidden, and on waking every poller and discovery resume within seconds. The Odds API is polled as each odds window opens, then every `r0.odds_poll_interval_s`. If the host says its clock is not NTP-synchronized when the recorder starts, as a Raspberry Pi without a clock battery says after a power cut, nothing is captured until it is: the wait is archived as `clock_unsynced`, then `clock_synced`. Stop it with Ctrl-C or SIGTERM: it writes a `process_stop` event and seals everything. A killed process is recovered on the next start, which seals the orphaned segments and records `previous_session_unclean`.

To run it as a service:

```bash
cp deploy/raw-recorder.service ~/.config/systemd/user/
```

```bash
systemctl --user enable --now raw-recorder.service
```

Put the secrets in `~/.config/raw-recorder/env` (`chmod 600`). See the comments in the unit file. That unit is the workstation's. The A1 host, a Raspberry Pi, runs `deploy/raw-recorder-pi.service` instead, and the workstation mirrors its archive. [deploy/pi-host.md](deploy/pi-host.md) sets up both and the cutover. New games and edits to `games.toml` need no restart. After changing the env file, the unit or the recorder's code, restart **between** capture windows: a restart mid-game is a recorded gap in every source.

```bash
systemctl --user daemon-reload
```

```bash
systemctl --user restart raw-recorder
```

### Checking a game while it records

```bash
uv run raw-recorder status
```

`status` checks the game whose capture window is open (or the next one; `--game-pk` picks another). It is read-only, so it is safe beside the running recorder. Every line is OK, WARN or FAIL, and the exit code is 0, 1 or 2 for the worst. It checks:
- **The recorder:** the process holds the archive lock; its session started with no source disabled (a missing key is a FAIL); no task or seal errors; disk above the minimum.
- **Novig:** each channel (`book`, `trades`) is connected, its transport is alive (a control Ping within `stream.transport_liveness_max_s`, each answered), every snapshot probe was answered and the sequences are contiguous. Any reconnect is a WARN, with the longest stretch it left uncovered.
- **Kalshi:** each ticker is polled at its interval, without failures; the longest interval between polls.
- **The Odds API:** polls made against those expected in its window (3 h before first pitch), failures and credits left.
- **StatsAPI:** the feed is fetched when the window ends.
- **Games:** whether the game was discovered or hand-checked (with any notes), and whether discovery ran within the last two hours.

It is an operational check. After the game, `clv inspect --game-pk N` and `clv ingest` interpret the full capture and record its gaps.

### After a game

| Command | What it does |
|---|---|
| `uv run raw-recorder mlb-feed --date 2026-10-07` | Re-archive StatsAPI feeds by hand. The recorder already fetches each feed when the window ends and again 12 h later (`[mlb] refetch_after_s`) |
| `uv run raw-recorder seal` | Seal active segments (a running recorder gets SIGHUP and rotates) |
| `uv run raw-recorder verify` | Recheck every sealed file's hash, size and frame count against its journal |
| `uv run raw-recorder report` | Volume per source and day, Odds credit spend, Novig Ping and probe counts |
| `uv run raw-recorder status --game-pk <pk>` | The checks above, for a game whose window has closed |
| `uv run raw-recorder probe-subscriptions --market <id>` | One-off experiment: can one Novig connection carry `book` and `trades`? |

`echo`, `catalog`, `mlb-feed` and `probe-subscriptions` write to the archive, so they refuse to run while `run` holds it.

### The archive

```text
archive/<source>/<YYYY-MM-DD UTC>/<stream_id>.<segment>.jsonl.gz   sealed, read-only
archive/<source>/<YYYY-MM-DD UTC>/manifest.jsonl                   append-only journal of sealed files
```

Each line is `{"recv_ts_ms", "conn_id", "dir": "in"|"out"|"event", "frame"}`. `frame` is always a string holding exactly the received text. REST records use `conn_id = request_id`, so every response stays tied to its request. Sealed files never change, so `rsync -a archive/ <backup>/` is a complete incremental backup.

## Gate A1: forced faults

Gate A1 (DESIGN.md §13) needs every forced fault to produce the right collection gap. Three pieces do it (`tools/raw_recorder/src/raw_recorder/faults.py`), each in its own process:
- **The relay** sits on loopback between the recorder and Novig. It forwards every frame unchanged and causes the faults Novig can't be made to cause: a socket kill, a channel stall (one market's channel loses its deltas for N seconds) and a sequence skip (one delta lost). It drops frames whole and never edits one. It never drops a frame carrying a lifecycle delta.
- **The recorder's control socket** archives each fault as `fault_injected` with the markets every connection had confirmed, and its outcome as `fault_applied` or `fault_failed`.
- **The harness** labels a fault, causes it and records the result. A process kill is a real SIGKILL; its result goes to the restarted recorder.

To run with faults, set `[faults] enabled = true` and point `[novig] ws_url` at `ws://127.0.0.1:8765/v3/ws`, then:

```bash
uv run raw-recorder relay
```

```bash
uv run raw-recorder fault sequence-skip --market <id> --channel book
```

The other faults are `channel-stall --market <id> --channel book --seconds 30`, `socket-kill [--channel book]`, `process-kill` and `resubscribe --market <id>`. Each prints its report and exits 1 if the fault did not happen, including a skip that found no delta and a stall that dropped nothing: those are listed by the gate and not counted. The report names every market the fault touched, a dropped frame's other markets included. It also gives the time the last of their capture windows closes: the gate needs each of those markets ingested, so keep recording until then. A process kill needs a service manager to restart the recorder within `--restart-wait` seconds.

Once the run's events are ingested (`clv ingest --league-events`) from an archive sealed past the run, the gate reads the labels and the facts:

```bash
uv run clv gate a1 --from 2026-11-10T00:00Z --to 2026-11-12T00:00Z --out data/a1/gate.md
```

It checks, for each fault and each market and channel it touched, that:
- a gap covers the fault, with a reason that fault can cause;
- the gap opens at the last trustworthy observation (for a dropped delta, the last one before its seq);
- the gap closes at a snapshot received after the fault.

A resubscribe must instead show a fresh subscribe snapshot and no gap. Every closed stream gap in the run must close at a snapshot. It lists the gaps no fault explains, and reports storage per day and source and book depth against `close.depth_notionals_usd`. It prints PASS only if every check passes and each fault kind was applied `--min-per-kind` times (default 2). Otherwise it exits 1.

## The A1 storage pilot

The week after the cutover measures what the probe policy left provisional (`docs/decisions/2026-10-17-a1-probe-policy.md`). Once that week's league events are ingested from an archive sealed past its end, run:

```bash
uv run clv pilot --from 2026-11-03T00:00Z --to 2026-11-10T00:00Z --out data/pilot.md
```

It is read-only, and records nothing in the database. Sealing the archive past `--to` doesn't settle every event the period touched. An event starting after the period is recorded during it, but loads only once its own window has closed. So the report first lists any discovered event overlapping the period that isn't ingested yet, and exits 1 until there are none. It reports:
- **storage:** per day and source;
- **probes:** markets probed and skipped for budget per channel, inside and outside a dense window, with their token spend against the refill;
- **quiet intervals:** each book's time between venue activity, by time to the scheduled start (an interval a gap overlaps counts as missing);
- **cutoffs:** the live close function's verdict at every check-1 offset and close cutoff: live on recent evidence, live on a bracketing observation, or why not;
- **depth:** book snapshots cut at the level cap, and how often both sides reach each depth notional;
- **listing lead:** how long before its start each market first appeared in a catalog listing, which says whether the 24 h offset is reachable.

The follow-up record sets the probe intervals and `book.full_snapshot_interval_s` from these numbers.

## The historical reference import (B1)

`raw-recorder backfill` archives B1's inputs (DESIGN.md §13 Track B) into `archive-hist/`, in the same format.
- **Sources:**
  - `mlb_hist`: the StatsAPI schedule and play-by-play.
  - `kalshi_hist`: `KXMLBGAME` candles and trades over the 32 h before each market closed.
  - `novig_hist`: Novig's daily trade files and `/v3/history` for their MLB markets, signed with the read key.
- **Runs beside the recorder.** It has its own archive lock.
- **Resumable.** A rerun sends only what failed or was never fetched, plus the listings that can still change. Rerun after the host sleeps.

```bash
uv run raw-recorder backfill --dates 2026-08-01:2026-08-31
```

```bash
uv run raw-recorder backfill
```

The first command is a pilot over one month. The second fetches everything, which takes hours. `archive-hist/` holds vendor data for your own research only: it is git-ignored, and nothing from it is committed.

Then load it. Each step can be rerun, and adds only what is new.

```bash
uv run clv backfill import
```

```bash
uv run clv backfill link
```

```bash
uv run clv correct --spec data/b1/links.approved.toml
```

```bash
uv run clv backfill import
```

```bash
uv run clv backfill coverage
```

1. The first `import` loads the games, their first pitches, every market, and how each game and market settled (StatsAPI winners, Kalshi results, Novig grades).
2. `link` ties markets to games by rule (`clv.backfill.links`). It writes `data/b1/links.review.md`, with a spot-check sample and every edge case, and `data/b1/links.proposed.toml`.
3. Review the file, keep the corrections you approve as `links.approved.toml`, and apply them with `clv correct`.
4. The second `import` loads candles and trades for the linked markets, up to each game's first pitch.
5. `coverage` writes the per-game table and its waterfall to `data/b1/`.

`clv trace --execution N` shows one trade and every record of it. `clv trace --close N` shows one historical close (B2) with its candle, or every execution in its trade window, each cited to its frame. The method is in `docs/decisions/2026-10-14-b1-reference-import.md`.

## Historical closes and check 1 (B2)

```bash
uv run clv calibrate --dispersion
```

```bash
uv run clv calibrate
```

`clv calibrate` prices each historical close family (Kalshi candle, Kalshi VWAP, Novig VWAP) at every buffer in the §9.3 sweep and at each check-1 offset, over a new fact snapshot.
- **`--dispersion`:** measures the spread of price moves and reruns the §9.4 item-12 operating characteristics under it, fitting nothing. Run it first, after the prespecification is committed.
- **Without the flag:** runs check 1, its binned diagnostic and the oracle sweep, and records a verdict per family.

Both write `data/b2/calibration.md` (aggregates) and `calibration_games.csv` (per-game prices: vendor-derived, never committed).

## Working rules

- **No threshold outside [DESIGN.md §10](DESIGN.md).** Recorder values are copied from it into `config.toml`. V0 creates `config/params.toml` from it.
- **Decisions go in dated records** in `docs/decisions/`. DESIGN.md describes only the current design.
- **§9.2 and the §10 control margins change only together with `tests/test_calibration_simulation.py`**, and that test must pass.
- **Vendor facts go in `docs/vendor-capabilities.md`** with a status and a date. A discrepancy gets a dated log row, never a silent edit.
