# Gate A1 run, December 2026: the checklist

The step-by-step for the gate run, one section per calendar reminder. What the run is and why: `docs/decisions/2026-10-17-a1-gate-plan.md`. The host's steps in general: `deploy/pi-host.md` section 10. This page is those two applied to the chosen dates.

- **The run:** Sat 2026-12-12 and Sun 2026-12-13, with Mon 12-14 as the third day. The fallback, if this run fails and needs a fix: Sun 12-20 and Mon 12-21, with Tue 12-22 as the third day. Shift every time below by the same days and re-check the first tip-offs.
- **Times:** US Eastern (EST, UTC−5). The slates are from ESPN's schedule, read 2026-11-25:
  - Sat 12-12: 8 games, the first at 17:00;
  - Sun 12-13: 6 games, the first at 15:00;
  - Mon 12-14: 11 games.
- **Help at any step:** open Claude Code in `~/code/clv-harness` and say which section you're at (for example, "gate run: Sat 15:00 skip"). It can read the Pi's archive through the mirror and pick the targets.

**Every Pi command below** runs as `recorder` (`ssh ckloote@raspberrypi.local`, then `sudo -iu recorder`) unless it says **(Pi, root)**, which means your admin shell. In each new `recorder` shell, run this first:

```bash
cd ~/clv-harness && C=tools/raw_recorder/config.gate.toml
```

Append every fault report to the run's file: `| tee -a ~/gate-faults.json`.

## Before

### Tue 12-01: the pilot record

Ask Claude: "pick up the A1 gate: the pilot report and the probe-interval record". That record sets the probe intervals before the run. The gate plan is accepted once it exists. If the quiet probe interval changes, the quiet stall below changes with it: it is that interval plus 60 s, 360 s today.

### Thu 12-10: preparation

1. Check the official schedule (nba.com) for 12-12, 12-13 and 12-14: at least 6 games on each of the first two days, and the first tip-offs as above. If they've moved, ask Claude to redo the times.
2. Update the Pi's checkout:

   ```bash
   cd ~/clv-harness && git pull --ff-only
   ```

3. Check the recorder: `.venv/bin/raw-recorder status` should end `overall: OK`.
4. **(Pi, root):** check that `tmux` is installed (`tmux -V`; if not, `sudo apt install tmux`), and that the hardware is healthy (`sudo vcgencmd get_throttled` prints `throttled=0x0`).
5. On the workstation, check that the mirror is current: `cat ~/code/clv-harness/archive/sealed_through.json` should be under an hour old.

## Sat 12-12, day 1

### 08:45: start (T0)

**(Pi, root):**

```bash
sudo install -D -m 644 /home/recorder/clv-harness/deploy/raw-recorder-gate.conf /etc/systemd/system/raw-recorder.service.d/gate.conf
```

```bash
sudo cp /home/recorder/clv-harness/deploy/raw-recorder-relay.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart raw-recorder.service
```

Then check: `systemctl is-active raw-recorder raw-recorder-relay` prints `active` twice. As `recorder`, `.venv/bin/raw-recorder --config $C status` should end `overall: OK`.

**Write down `date -u` now: it is T0.** Injection stops 48 hours later.

### 09:55: socket kill (10:00)

First, `.venv/bin/raw-recorder --config $C games`: every open window must be for today's games. Sunday's 15:00 game opens at 14:00 today, so this is fine before then.

```bash
.venv/bin/raw-recorder --config $C fault socket-kill | tee -a ~/gate-faults.json
```

Then `status` until both channels show a new connection and no resync is pending.

### 10:40: process kill (10:45)

```bash
.venv/bin/raw-recorder --config $C fault process-kill | tee -a ~/gate-faults.json
```

It waits for the restarted session (about 10 s). Then `status`: a new pid, both channels connected.

### 14:55: quiet skip (15:00)

`games` again first. Target: a market of the 17:00 game that has had recent book deltas (Claude can pick one from the mirror).

```bash
M=<market> && .venv/bin/raw-recorder --config $C fault sequence-skip --market $M --channel book --timeout 1800 | tee -a ~/gate-faults.json
```

### 15:40: quiet stall (15:45)

The same kind of market. `--seconds` is the quiet probe interval plus 60 (360 unless the pilot record changed it). It takes that long to return.

```bash
M=<market> && .venv/bin/raw-recorder --config $C fault channel-stall --market $M --channel book --seconds 360 | tee -a ~/gate-faults.json
```

### 16:40: start the close-window script (game at 17:00)

Three markets of 17:00 games: one each if several games start then, otherwise the same game's markets. Inside `tmux` (`tmux new -s gate`):

```bash
cd ~/clv-harness && C=tools/raw_recorder/config.gate.toml && START="2026-12-12 17:00" && M1=<market> && M2=<market> && M3=<market>
```

```bash
S=$(date -d "$START" +%s); at() { t=$(( S - $1 - $(date +%s) )); [ "$t" -gt 0 ] && sleep "$t"; date; }; F=~/gate-faults.json; at 540; .venv/bin/raw-recorder --config $C fault sequence-skip --market $M1 --channel book --timeout 150 | tee -a $F; at 360; .venv/bin/raw-recorder --config $C fault channel-stall --market $M2 --channel book --seconds 60 | tee -a $F; at 180; .venv/bin/raw-recorder --config $C fault resubscribe --market $M3 | tee -a $F
```

It runs the three faults at 16:51, 16:54 and 16:57, printing the time before each. Detach with `Ctrl-b d`; `tmux attach -t gate` to look again.

### 17:15: dense-window resubscribe (17:20)

A market of a game that started at 17:00:

```bash
M=<market> && .venv/bin/raw-recorder --config $C fault resubscribe --market $M | tee -a ~/gate-faults.json
```

## Sun 12-13, day 2

The first game is at 15:00, so the quiet faults move to midday.

### 09:55: socket kill, book only (10:00)

`games` first: Monday's games open from 18:00 today, so nothing of theirs should be open yet.

```bash
.venv/bin/raw-recorder --config $C fault socket-kill --channel book | tee -a ~/gate-faults.json
```

### 10:40: process kill (10:45)

As on day 1.

### 11:55: quiet skip (12:00)

As on day 1, on a market of the 15:00 game.

### 12:40: quiet stall (12:45)

As on day 1, on a market of the 15:00 game.

### 14:40: start the close-window script (game at 15:00)

As on day 1, with `START="2026-12-13 15:00"` and three markets of 15:00 games. Faults at 14:51, 14:54 and 14:57.

### 15:15: dense-window resubscribe (15:20)

As on day 1, on a game that started at 15:00.

## Mon 12-14: end of injection

### 08:45: injection ends at T0 + 48 h

Inject nothing more. Find the latest `keep_recording_until_utc` in `~/gate-faults.json`:

```bash
grep -o '"keep_recording_until_utc": "[^"]*"' ~/gate-faults.json | sort | tail -1
```

That is the run's end. It is probably early Tue 12-15: Sunday evening's faults can touch Monday's games. Leave the gate configuration running until then.

## Tue 12-15: the end

### 09:00: switch back and run the gate

Once past that time, **(Pi, root):**

```bash
sudo rm /etc/systemd/system/raw-recorder.service.d/gate.conf && sudo systemctl daemon-reload && sudo systemctl restart raw-recorder.service && sudo systemctl stop raw-recorder-relay.service
```

Check that `systemctl cat raw-recorder` shows no drop-in, and that `status` (without `--config`) ends `overall: OK`. **Write down `date -u`: it is the gate's `--to`.**

Then ask Claude: "gate run done: T0 <time>, end <time>", and paste `~/gate-faults.json` (or copy it to the workstation). Claude mirrors the archive, ingests, runs `clv gate a1 --min-per-kind 2` and the pilot report, and drafts the results section.
