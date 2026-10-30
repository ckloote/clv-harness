# The A1 recorder host: setting up the Pi, the mirror and the cutover

This is the runbook for `docs/decisions/2026-10-16-a1-host.md`:
- **The Pi** (a Raspberry Pi 5 with a Samsung T5 SSD) runs the recorder and nothing else.
- **The workstation** mirrors its sealed archive every 15 minutes and does all derived work from that mirror.

Section 9 rehearses all of it on Novig Paper before the cutover, without touching the Production key or archive. Section 10 runs Gate A1 itself.

Commands marked **(Pi, root)** run as the Pi's administrator with `sudo`. Commands marked **(Pi, recorder)** run as the `recorder` account (`sudo -iu recorder`). Never ssh in as `recorder`: its authorized keys are the mirror's restricted keys (section 7), and an SSH agent holding them gets `rrsync`, not a shell. Unmarked commands run on the workstation.

**One key, one process.** Novig throttles per key. Until the cutover (step 8) the workstation's recorder holds the Production key. Until then the Pi may run only short commands that don't take the stream budget (`echo`), or the Paper configuration.

## 1. Hardware and OS

- **Power:** the official 27 W (5 V, 5 A) supply. On less, the Pi 5 limits its USB ports to 600 mA in total, and the T5 can exceed that under write load.
- **Cooling:** the Active Cooler or a fan case.
- **Network:** wired Ethernet at the home address (Novig's IP and companion checks already pass there).
- **OS:** Raspberry Pi OS Lite (64-bit) on the SD card, with SSH enabled for a key, not a password. Update it, then set the timezone to UTC so logs line up with the archive's timestamps:

  ```bash
  sudo apt update && sudo apt full-upgrade -y && sudo timedatectl set-timezone UTC
  ```

- **No sleep:** nothing on the Pi should suspend. **(Pi, root)**

  ```bash
  sudo systemctl mask sleep.target suspend.target hibernate.target hybrid-sleep.target
  ```

## 2. The clock

The Pi 5 has a real-time clock, but without the optional battery it boots with the wrong time after a power cut. Two layers keep a wrong clock out of the archive:
- **The unit waits for sync.** `deploy/raw-recorder-pi.service` starts after `time-sync.target`. Enable the service that holds that target back until NTP has set the clock. **(Pi, root)**

  ```bash
  sudo systemctl enable systemd-time-wait-sync.service
  ```

  If the OS uses chrony instead of systemd-timesyncd, enable `chrony-wait.service` instead.
- **The recorder checks too.** If the clock loses sync later, the recorder archives `clock_unsynced` and captures nothing until it is back.

**Check:** the command below should print `yes`.

```bash
timedatectl show -p NTPSynchronized --value
```

A clock battery is still worth fitting: it makes a power cut cost less time.

## 3. The SSD

**Reformatting erases the drive.** The T5 ships formatted exFAT. Find its device with `lsblk`, and check it is the T5 (about 1 TB, on USB), not the SD card (`mmcblk0`). **(Pi, root)**, with the T5 at `/dev/sda`:

```bash
sudo parted --script /dev/sda mklabel gpt mkpart clv-archive ext4 0% 100%
```

```bash
sudo mkfs.ext4 -L clv-archive /dev/sda1
```

**The mount point is made immutable while empty.** Nothing can then be written to the SD card under `/mnt/ssd` when the SSD is missing:

```bash
sudo mkdir -p /mnt/ssd && sudo chattr +i /mnt/ssd
```

**Mount it by UUID.** Read the UUID with `sudo blkid /dev/sda1`, then add this line to `/etc/fstab`:

```text
UUID=<uuid>  /mnt/ssd  ext4  defaults,noatime,nofail,x-systemd.device-timeout=30s  0  2
```

```bash
sudo systemctl daemon-reload && sudo mount /mnt/ssd && findmnt /mnt/ssd
```

`nofail` lets the Pi boot without the SSD. The recorder unit's `RequiresMountsFor` then keeps the recorder down.

**USB resets.** Check the kernel log after a few hours of writes:

```bash
sudo dmesg | grep -iE 'uas|reset|I/O error'
```

If it shows UAS resets for the T5, read its USB ID with `lsusb`, for example `04e8:61f5`. Append `usb-storage.quirks=<id>:u` to the single line in `/boot/firmware/cmdline.txt`, then reboot. That entry turns UAS off for that drive only.

## 4. The `recorder` account and the checkout

**(Pi, root):**

```bash
sudo adduser --disabled-password --gecos '' recorder
```

```bash
sudo install -d -o recorder -g recorder -m 750 /mnt/ssd/archive
```

**(Pi, recorder):**
- Install uv (README "Install").
- Clone the repository, sync only the recorder package, and link the archive root onto the SSD:

  ```bash
  git clone https://github.com/ckloote/clv-harness.git ~/clv-harness
  ```

  ```bash
  cd ~/clv-harness && ~/.local/bin/uv sync --frozen --package raw-recorder
  ```

  ```bash
  ln -s /mnt/ssd/archive ~/clv-harness/archive
  ```

**Why a link:** `config.toml`'s archive root is `../../archive`, the checkout's `archive/`. With the link, the service and every command run by hand (`status`, `verify`, `seal`) use the SSD, and the committed config needs no Pi copy. Git ignores the link.

**Never on the Pi:** `clv` ingest and calibration, and `raw-recorder backfill`. The workstation does those from the mirror.

**The Pi's archive stays empty until the cutover seeds it** (step 8). Each day's journal, `manifest.jsonl`, is append-only on the host that writes it, and rsync replaces whole files. So anything written there first by the Pi would collide with the workstation's journal for the same UTC day: the seed would leave the Pi's own segments unjournaled, or the mirror would drop the workstation's. Every check below writes elsewhere.

**For a Paper rehearsal,** `config.paper.toml` writes to `archive-paper/`, a separate root. Put that on the SSD too. **(Pi, root):**

```bash
sudo install -d -o recorder -g recorder -m 750 /mnt/ssd/archive-paper
```

Then, **(Pi, recorder):**

```bash
ln -s /mnt/ssd/archive-paper ~/clv-harness/archive-paper
```

## 5. The read key

**The Pi gets only the `trading::read` key.** The management key never goes on it (README "Secrets"). Copy the read key's PEM from the workstation:

```bash
scp ~/.config/raw-recorder/novig-read.pem <admin>@<pi>:/tmp/
```

Then, **(Pi, root)**, create the configuration directory as `recorder`, so it and its parents are the account's own, and move the key into it with mode 0600:

```bash
sudo -u recorder install -d -m 700 /home/recorder/.config/raw-recorder
```

```bash
sudo install -o recorder -g recorder -m 600 /tmp/novig-read.pem /home/recorder/.config/raw-recorder/novig-read.pem && rm /tmp/novig-read.pem
```

**(Pi, recorder):** write `~/.config/raw-recorder/env`, mode 0600. It has the same lines as the workstation's, except the key path:

```text
NOVIG_READ_KEY_ID=<read key UUID>
NOVIG_READ_KEY_PATH=/home/recorder/.config/raw-recorder/novig-read.pem
```

`ODDS_API_KEY` is used only for MLB odds; add it before the 2027 season.

**Check the key, against a throwaway archive.** `echo` archives what it does, and the Pi's archive must stay empty. So run it with a copy of the config whose archive root is a temporary directory, then delete both:

```bash
cd ~/clv-harness && sed '0,/^root = /s|^root = .*|root = "/tmp/key-check"|' tools/raw_recorder/config.toml > /tmp/key-check.toml
```

```bash
set -a && . ~/.config/raw-recorder/env && set +a && .venv/bin/raw-recorder --config /tmp/key-check.toml echo
```

```bash
rm -rf /tmp/key-check /tmp/key-check.toml
```

## 6. The units

**(Pi, root):**

```bash
sudo cp /home/recorder/clv-harness/deploy/raw-recorder-pi.service /etc/systemd/system/raw-recorder.service
```

```bash
sudo cp /home/recorder/clv-harness/deploy/raw-recorder-relay.service /etc/systemd/system/
```

```bash
sudo systemctl daemon-reload
```

**Don't enable the recorder until the cutover (step 8).** `deploy/raw-recorder-relay.service` is only for the gate run; see its comments.

**Every day:**
- **Check:** `sudo -iu recorder`, then `cd clv-harness && .venv/bin/raw-recorder status` and `.venv/bin/raw-recorder games`. Call the venv's binary, not `uv run`: on the Pi, `uv run` would sync the whole workspace, `clv` included.
- **The log:** `journalctl -u raw-recorder`. Warnings only: `journalctl -u raw-recorder -p warning`.

**Updating the code:**
1. **(Pi, recorder):** run `git pull`.
2. **(Pi, root):** run `sudo systemctl restart raw-recorder` and, if a unit file changed, copy it first and `daemon-reload`.

**Choose the restart time.** NBA windows run 29 hours (`a1.capture_lead_s` plus `a1.capture_tail_s`) and overlap, so there is rarely a moment between windows. A restart is a recorded gap in every open window. Restart away from any game's dense window, which runs from 15 minutes before a scheduled start to 30 minutes after it.

## 7. The workstation mirror

**A read-only key.** The workstation pulls with its own key. The Pi restricts that key to reading the archive through `rrsync`, which ships with the `rsync` package (check with `command -v rrsync` on the Pi).

1. **Create the key:**

   ```bash
   ssh-keygen -t ed25519 -N '' -C archive-mirror -f ~/.ssh/pi-archive
   ```

2. **(Pi, recorder):** add one line to `recorder`'s `~/.ssh/authorized_keys` (mode 600, in a mode 700 `~/.ssh`), and to no other account's. Your SSH agent offers every key it holds, the mirror's included, and the Pi takes the first one an account lists: in the administrator's file, it would turn your own logins into `rrsync`.

   ```text
   command="rrsync -ro /mnt/ssd/archive",restrict ssh-ed25519 AAAA… archive-mirror
   ```

3. **On the workstation,** add an alias to `~/.ssh/config`:

   ```text
   Host pi-archive
       HostName <pi address>
       User recorder
       IdentityFile ~/.ssh/pi-archive
       IdentitiesOnly yes
   ```

4. **Accept the Pi's host key once.** This also lists the archive root, since paths are relative to it:

   ```bash
   rsync --list-only pi-archive:
   ```

5. **Install the units, but don't enable or start them yet.** Copy the files and write the env file as `deploy/archive-mirror.service`'s comments say, with `ARCHIVE_MIRROR_SRC=pi-archive:`. Stop before its `enable` and `start` lines: those wait for the cutover (step 8.7).
   - **Why wait:** until then the workstation's `archive/` is R0's live archive, not a mirror. A run would publish the Pi's watermark over an archive still being written, and could vouch for windows the workstation hasn't sealed.
   - **The guard:** the mirror refuses, and copies nothing, if a Pi journal does not extend the workstation's for the same day.
   - **Logged out:** the timer runs while you are logged out only with lingering enabled (`loginctl enable-linger "$USER"`, already on for R0). A run missed while the workstation slept runs on wake.

**Check the mirror.** Each run's log is in `journalctl --user -u archive-mirror`. Its watermark shows how far it vouches:

```bash
cat ~/code/clv-harness/archive/sealed_through.json
```

`clv ingest --league-events` loads only events whose windows end before that watermark.

## 8. The cutover, after the World Series

R0 records the postseason on the workstation until the last game's window closes and its 12-hour feed refetch (`[mlb] refetch_after_s`) has run. Then:

1. **Seed the Pi's archive while R0 still runs.** This bulk copy takes the time. First check that the Pi's archive is still empty. **(Pi, recorder):** this must print nothing:

   ```bash
   find /mnt/ssd/archive -mindepth 1 -print -quit
   ```

   Then, **(Workstation):**

   ```bash
   rsync -rt --rsync-path='sudo -u recorder rsync' --exclude '*.part' --exclude 'sealed_through.json' --exclude '.recorder.lock' --exclude '.recorder.control' --exclude '*.tmp' ~/code/clv-harness/archive/ <admin>@<pi>:/mnt/ssd/archive/
   ```

2. **Stop the workstation recorder** for good. It seals every segment on the way down:

   ```bash
   systemctl --user disable --now raw-recorder.service
   ```

3. **Finish the seed.** Run the same rsync again without `--exclude 'sealed_through.json'`. Everything is sealed now, so any watermark it copies is complete.
   The postseason recorder predates the watermark, so its archive may have no `sealed_through.json`. The Pi's recorder writes one when it starts, and the mirror needs it to run.
4. **Verify the Pi's copy.** **(Pi, recorder):** this rechecks every sealed file against its journal:

   ```bash
   cd ~/clv-harness && .venv/bin/raw-recorder verify
   ```

5. **Enable NBA** with a commit that sets `[[league]] enabled = true` in `config.toml` and in `config.gate.toml` (tests hold the two equal), then `git pull` on the Pi as `recorder`. Doing this after step 2 keeps it out of the workstation recorder, which a crash restart would otherwise reload with NBA on.
6. **Start the Pi's recorder.** **(Pi, root):**

   ```bash
   sudo systemctl enable --now raw-recorder.service
   ```

   Then, as `recorder`, run `.venv/bin/raw-recorder status` and `.venv/bin/raw-recorder games`. NBA events should be listed, and Novig connected.
7. **Start the mirror** installed in step 7: enable its timer, then run it once.

   ```bash
   systemctl --user enable --now archive-mirror.timer
   ```

   ```bash
   systemctl --user start archive-mirror.service
   ```

8. **The restore test.** First make sure the workstation's local trace is current. Before step 1, write it from the workstation's own archive with `uv run python tests/golden.py`; `git status` should then show no change to committed files.
   Then mirror into an empty directory, so nothing local can stand in for the Pi's copy, and replay 849832 from it against its golden trace:

   ```bash
   RSYNC_RSH='ssh -o BatchMode=yes' ARCHIVE_MIRROR_SRC=pi-archive: ARCHIVE_MIRROR_DST=/tmp/restore-test sh deploy/archive-mirror.sh
   ```

   ```bash
   CLV_GOLDEN_ARCHIVE=/tmp/restore-test uv run pytest tests/test_golden_game.py -k 849832
   ```

   It must pass, not skip. Then delete `/tmp/restore-test`.

**The workstation's `archive/` is now the mirror.** It is written only by `archive-mirror.sh`. Never run the recorder or a writing `raw-recorder` command (`echo`, `catalog`, `mlb-feed`, `seal`) there again: those belong on the Pi.

## 9. Rehearsal on Paper, before the cutover

A day of the whole A1 stack on the Pi, on Novig Paper: hardware, clock, SSD, the units, the relay and fault harness, a process kill under systemd, the mirror, then ingest and `clv gate a1` on the workstation. Paper lists the same NBA moneylines as Production, so the recorder discovers and subscribes as it will after the cutover, with a separate key, archive and relay:

| | Rehearsal | Production |
|---|---|---|
| Config | `config.paper-rehearsal.toml` | `config.toml` |
| Key | `NOVIG_PAPER_READ_KEY_*` | `NOVIG_READ_KEY_*` |
| Pi archive | `/mnt/ssd/archive-paper` | `/mnt/ssd/archive` |
| Units | `raw-recorder-paper{,-relay}.service` | `raw-recorder{,-relay}.service` |
| Relay port | 8766 | 8765 |
| Workstation copy | `archive-rehearsal/`, `data/rehearsal.sqlite` | `archive/`, `data/clv.sqlite` |

So it can run while R0 still records on the workstation. **Paper books are play money:** the rehearsal tests the collector, never the measurement. Its data never goes into the real archive or database.

It needs sections 1–7, including the Paper archive directory from section 4. The Production recorder and mirror stay installed but not enabled.

1. **The Paper key.** Copy `novig-paper-read.pem` to the Pi as in section 5, into the same directory. Add its lines to `~/.config/raw-recorder/env`:

   ```text
   NOVIG_PAPER_READ_KEY_ID=<Paper read key UUID>
   NOVIG_PAPER_READ_KEY_PATH=/home/recorder/.config/raw-recorder/novig-paper-read.pem
   ```

   Check it. This writes to `archive-paper/`, Paper's own archive, which nothing seeds. **(Pi, recorder):**

   ```bash
   cd ~/clv-harness && set -a && . ~/.config/raw-recorder/env && set +a && .venv/bin/raw-recorder --config tools/raw_recorder/config.paper.toml echo
   ```

2. **A read-only key for the Paper archive.** `rrsync` restricts a key to one directory, so the Paper archive needs a second key. Set it up as in section 7, steps 1–4:
   - the key is `~/.ssh/pi-archive-paper`;
   - its forced command is `rrsync -ro /mnt/ssd/archive-paper`;
   - its alias is `pi-archive-paper`.
3. **Start in the morning, US Eastern, on a day with NBA games.** The 2026–27 regular season starts on 2026-10-20. Each event's window opens 25 hours before its start, so tonight's games are subscribed from the first minute, and their windows close 4 hours after tip-off, inside the run. Write down the start time in UTC; it is the gate's `--from`. **(Pi, root):**

   ```bash
   sudo cp /home/recorder/clv-harness/deploy/raw-recorder-paper.service /home/recorder/clv-harness/deploy/raw-recorder-paper-relay.service /etc/systemd/system/
   ```

   ```bash
   sudo systemctl daemon-reload && sudo systemctl enable --now raw-recorder-paper-relay.service raw-recorder-paper.service
   ```

   Then, **(Pi, recorder)**, check the stream is up and tonight's events are listed:

   ```bash
   cd ~/clv-harness && C=tools/raw_recorder/config.paper-rehearsal.toml && .venv/bin/raw-recorder --config $C games && .venv/bin/raw-recorder --config $C status --event <Novig event ID>
   ```

4. **Inject faults.** At least one of each kind. The gate needs every market a fault touched loaded, and a market loads only once its window has closed inside the run.
   - **Each report says how long to keep recording.** It names the markets the fault touched (`touched_markets`), and when the last of their windows closes (`keep_recording_until_utc`, also printed). The run ends after the latest of these across all faults.
   - **A resubscribe** touches only its market. Aim it, a sequence skip and a channel stall at games that tip off tonight (`games` lists them). These three can go inside a dense window, which runs from 15 minutes before a start to 30 minutes after it.
   - **A sequence skip or channel stall** drops whole frames, and a frame can carry other markets besides the target, tomorrow's included. Its report names them all. If its time to keep recording falls tomorrow, the run has to last until then.
   - **A socket kill or process kill** touches every market subscribed at that moment. Windows open 25 hours ahead, so tomorrow's games are subscribed from about this evening on. To end tonight, do these two while `games` shows no window open yet for any game after tonight's.

   **(Pi, recorder)**, with `C` as above. A new shell needs the `cd … && C=…` line again: without it, `--config $C status` reads `status` as the config's path. Call `.venv/bin/raw-recorder`, never `uv run` (section 6):

   ```bash
   .venv/bin/raw-recorder --config $C fault sequence-skip --market <id> --channel book
   ```

   ```bash
   .venv/bin/raw-recorder --config $C fault channel-stall --market <id> --channel book --seconds 30
   ```

   ```bash
   .venv/bin/raw-recorder --config $C fault socket-kill
   ```

   ```bash
   .venv/bin/raw-recorder --config $C fault resubscribe --market <id>
   ```

   ```bash
   .venv/bin/raw-recorder --config $C fault process-kill
   ```

   Each prints its report and exits 1 if the fault did not happen. After `process-kill`, systemd restarts the recorder, and the harness reads the result from the new session.
5. **Watch the hardware** during the run, **(Pi, root)**: `recorder` isn't in the `video` group that `vcgencmd` needs, and needn't be.
   - `sudo vcgencmd get_throttled` should print `throttled=0x0`. Anything else means under-voltage or heat.
   - `sudo vcgencmd measure_temp` shows the temperature.
   - `sudo dmesg | grep -iE 'uas|reset|I/O error'` shows USB resets (section 3).
   - `df -h /mnt/ssd` shows the free space.
6. **Optionally, a power cut.** Pull the power for a minute, outside any dense window, and note the time. On boot:
   - the recorder waits for the clock (`journalctl -b -u systemd-time-wait-sync`);
   - it then starts and archives `previous_session_unclean`.

   The gate lists the outage as unattributed gaps. It is not a labelled fault, so it doesn't fail the gate.

   **Explaining an unattributed reconnect.** The relay logs each connection's end: which side ended it, the close code, and how long before the venue had last sent anything. A close by the venue, or the recorder closing after a long silence from it, is the venue's or the network's. **(Pi, root):**

   ```bash
   journalctl -u raw-recorder-paper-relay --utc --since "<gap start> UTC" --until "<gap end> UTC"
   ```

   Write the gate's times as `2026-10-28 09:13:00 UTC`: without the `UTC`, `--since` and `--until` read them in the Pi's local time. `--utc` shows the log's own times in UTC too.
7. **Mirror** a few times during the run, and once after the end:

   ```bash
   RSYNC_RSH='ssh -o BatchMode=yes' ARCHIVE_MIRROR_SRC=pi-archive-paper: ARCHIVE_MIRROR_DST=$HOME/code/clv-harness/archive-rehearsal sh deploy/archive-mirror.sh
   ```

8. **End** once every window a fault touched has closed: after the latest `keep_recording_until_utc` in the faults' reports. Write down the time in UTC: the gate's `--to`, which must be after the last fault's result.
   - **Stopping early can't be fixed afterwards.** A window still open when the recorder stops never passes the seal watermark, so its markets never load. Stopping seals every segment. **(Pi, root):**

   ```bash
   sudo systemctl disable --now raw-recorder-paper.service raw-recorder-paper-relay.service
   ```

   Then mirror once more (step 7).
9. **Ingest and gate on the workstation,** into the rehearsal's own database:

   ```bash
   uv run clv ingest --league-events --archive archive-rehearsal --db data/rehearsal.sqlite
   ```

   ```bash
   uv run clv gate a1 --archive archive-rehearsal --db data/rehearsal.sqlite --from <start> --to <end> --min-per-kind 1 --out data/rehearsal-gate.md
   ```

   **Expected:** PASS. Every unattributed gap needs an explanation, such as the power cut or a Paper disconnect seen in the recorder's log.
   - **`market not ingested`** names a market a fault touched whose window hadn't closed when the run ended. The fault's report gave the time to keep recording until (step 4). Rerun the rehearsal and stop after that time.

The Paper archive can stay on the SSD; it is a separate root. To rehearse again, start from step 3.

## 10. The gate run

Gate A1 runs on Production after the storage pilot and its record. `docs/decisions/2026-10-17-a1-gate-plan.md` fixes when it runs, the fault schedule, when it ends and what passes. These are the host's steps.

1. **Switch the recorder to the gate configuration**, away from any dense window. `config.gate.toml` is `config.toml` with the fault harness on and the stream through the relay. **(Pi, root):**

   ```bash
   sudo install -D -m 644 /home/recorder/clv-harness/deploy/raw-recorder-gate.conf /etc/systemd/system/raw-recorder.service.d/gate.conf
   ```

   ```bash
   sudo cp /home/recorder/clv-harness/deploy/raw-recorder-relay.service /etc/systemd/system/ && sudo systemctl daemon-reload && sudo systemctl restart raw-recorder.service
   ```

   The drop-in pulls in the relay. Check that both units are active, then write down the time in UTC. It is `T0`, the gate's `--from`.
2. **Inject the record's schedule.** **(Pi, recorder)**, with `C=tools/raw_recorder/config.gate.toml`, as in section 9, step 4: `.venv/bin/raw-recorder --config $C fault …`. Keep each report: the results need its time, target and outcome. The latest `keep_recording_until_utc` among them sets the end. Run `games` before each fault (the record says why).

   **The close-window group, as one script.** Start it in `tmux` (or another shell that survives a dropped SSH session) at least 10 minutes before the game's start. Set the start, and three markets of that game, or of games starting at the same time, before you paste. **(Pi, recorder):**

   ```bash
   cd ~/clv-harness && C=tools/raw_recorder/config.gate.toml && START="<YYYY-MM-DD HH:MM, the start in Pi local time>" && M1=<market> && M2=<market> && M3=<market>
   ```

   ```bash
   S=$(date -d "$START" +%s); at() { t=$(( S - $1 - $(date +%s) )); [ "$t" -gt 0 ] && sleep "$t"; date; }; F=~/gate-faults.json; at 540; .venv/bin/raw-recorder --config $C fault sequence-skip --market $M1 --channel book --timeout 150 | tee -a $F; at 360; .venv/bin/raw-recorder --config $C fault channel-stall --market $M2 --channel book --seconds 60 | tee -a $F; at 180; .venv/bin/raw-recorder --config $C fault resubscribe --market $M3 | tee -a $F
   ```

   It runs the skip at start − 9 min, the stall at start − 6 min and the resubscribe at start − 3 min, printing the time before each. If a step starts late, the `date` line shows it; note it for the results.
3. **After the end**, switch back to Production, again away from a dense window. **(Pi, root):**

   ```bash
   sudo rm /etc/systemd/system/raw-recorder.service.d/gate.conf && sudo systemctl daemon-reload && sudo systemctl restart raw-recorder.service && sudo systemctl stop raw-recorder-relay.service
   ```

   Check with `systemctl cat raw-recorder` that no drop-in is left.
4. **On the workstation,** once the mirror has caught up:

   ```bash
   uv run clv ingest --league-events
   ```

   ```bash
   uv run clv gate a1 --from <T0> --to <end> --min-per-kind 2 --out data/gate-a1.md
   ```

   Then, once every overlapping event is ingested:

   ```bash
   uv run clv pilot --from <T0> --to <end> --out data/gate-a1-pilot.md
   ```

   The results go in the record's results section, without prices or raw frames. Explain each unattributed reconnect from the relay's log, as in section 9, step 6, with `-u raw-recorder-relay`.

