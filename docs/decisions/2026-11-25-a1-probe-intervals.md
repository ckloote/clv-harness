# A1 probe intervals and capture parameters, set from the storage pilot

Date: 2026-11-25

Status: accepted. It is the follow-up record that `2026-10-17-a1-probe-policy.md` asks for: it sets the probe intervals, the dense window and `book.full_snapshot_interval_s` from the storage pilot, before the gate run. For open decision 3 of `2026-10-16-a1-scope.md` (how far ahead markets list), it records NBA's coverage fact and defers the offset question until MLB's listing lead is measured. Once it is accepted, the gate plan (`2026-10-17-a1-gate-plan.md`) is accepted with it.

## Context

**The pilot ran far longer than planned.** It was planned as one week. The Pi recorder has run since 2026-10-30T02:12Z, and `clv pilot` was run over three full weeks of NBA (10-30 to 11-20, 190 league events), with the archive mirrored to 11-25. Each week's report is in `data/pilot-week{1,2,3}.md`. These are local and not committed: they are derived from vendor data.

**The numbers, week by week:**

| | Week 1 (10-30) | Week 2 (11-06) | Week 3 (11-13) |
|---|---:|---:|---:|
| Events ingested | 64 | 61 | 65 |
| Archive, MB a day | 27.7 | 23.6 | 24.9 |
| Probe tokens a second (refill 4) | 0.12 | 0.11 | 0.11 |
| `book` probes, dense / quiet | 842 / 245 | 656 / 153 | 636 / 240 |
| Probes skipped | 1 (trades, dense) | 0 | 0 |
| `book` quiet stretches over 300 s, 24 h to 6 h before | 81 | 17 | 100 |
| Longest `book` quiet stretch, 6 h to 1 h before (s) | 182 | 119 | 541 |
| Longest `book` quiet stretch, dense window (s) | 65 | 63 | 61 |
| Snapshots reaching $1,000 on both sides | 98.5% | 98.4% | 98.6% |
| Snapshots cut at the venue's level cap | 5.3% | 1.2% | 3.2% |
| Listing lead, median (h) | 23.9 | 24.9 | 22.9 |
| Listed at least 24 h ahead | 43.5% | 52.5% | 29.5% |
| Listed at least 6 h ahead | 95.2% | 100% | 100% |

**Closes.** Of the markets recorded before their start, every close cutoff from 0 s to 10 min was `live` with these exceptions:
- **Week 1, 11-02 evening:** 5 markets at the 15 min offset and the 10 min cutoff were `venue_lag`. The venue's own timestamps ran 10–26 s behind the receive time (`stream.max_venue_lag_ms` is 5,000), a Novig-side delay, and the close function correctly refused those prices.
- **Week 2:**
  - at the 1 h offset, 1 market was `collection_gap` from the Pi's DNS outage below;
  - at the 15 min offset and the 0 s to 2 min cutoffs, 1 market was `collection_gap`, but `live` at the 10 min cutoff. No gap in the database spans those times, so it is not yet traced.
- **Week 3:**
  - 11-14: 2 markets were `collection_gap`, from the subscribe bug below;
  - 11-19: 2 markets were `venue_lag` at the 1 h offset and the 30 s cutoff, at about 7 s.

No close failed for lack of quotes. The quote cycling seen on Paper did not occur on Production.

**Two collection failures the pilot found, neither a probe-interval matter:**
- **A refused resubscribe kept every live market unsubscribed.** On a late-evening reconnect, the recorder resubscribed all its markets in one request. Novig refused the whole request because a settled market was in it, and the same batch was retried about once a minute. This happened 5 times, for 20 min to 4.5 h each (10-31, 11-06, 11-14, 11-16, 11-21). It is fixed by PR #36 (merged 2026-11-25), which splits a refused multi-market subscribe as #34 did for probes. It reaches the Pi at the gate's 12-10 preparation step.
- **The Pi lost DNS for 20 minutes** on 11-09 (23:25–23:45Z): every connect attempt failed with `ClientConnectorDNSError`. The recorder reconnected as designed, and the gaps are recorded. It was the host's network, not the venue. It cost no close cutoff: the affected games started at 00:05Z and 00:35Z, after the gaps ended at 23:45Z. It did cost the 1 h offset of the 00:35Z game.

**The workstation database is large.** After ingesting the pilot, `data/clv.sqlite` is 34 GB, mostly `tick_level` (16 GB, plus 8.6 GB of index). It was 7.0 GB before the NBA ingest, so the 193 NBA events added about 27 GB, about 0.14 GB each. A full NBA season would add about 170 GB. The workstation has 620 GB free. The database is derived and can be rebuilt from the archive, so this is a cost to manage in A2, not a collection risk.

## Decision

### 1. Probe intervals: unchanged

| Parameter | Value | Why |
|---|---:|---|
| `stream.channel_probe_interval_s` | 15 | Inside the dense window, `book` was never quiet for more than 65 s, and every close cutoff before the start had recent evidence except those above. |
| `stream.quiet_probe_interval_s` | 300 | Stretches over 300 s happen only well before the start (at most 100 a week, 24 h to 6 h before). Bracketing covers them, and a quiet interval only bounds how long a silent stall goes unnoticed. No silent stall was seen; every loss was a reconnect. |
| `stream.dense_probe_lead_s` | 900 | The 15 min offset falls at the window's edge, and its verdicts were `live` apart from the venue lag. |
| `stream.dense_probe_tail_s` | 1,800 | Covers late tip-offs. |

**Why not probe faster outside the dense window:** it would be affordable, since probes use about 3% of the refill. A 120 s quiet interval would add about 0.15 tokens a second. But the pilot shows no failure it would prevent, and a change now would also change the gate's quiet-stall length and the checklist written around it. It stays a cheap lever if the gate or A2 shows silent stalls.

The quiet stall in the gate plan therefore stays at 360 s (the quiet interval plus 60 s), as in `docs/gate-run-2026-12.md`.

### 2. Snapshots and archive: unchanged

- **`book.full_snapshot_interval_s` stays 300.** About 15,000 periodic snapshots a week, a small part of the database next to `tick_level`.
- **`r0.segment_max_s` (3,600) and `r0.fsync_interval_s` (5) stay.** No segment was sealed unclean in the three weeks.
- **Disk headroom:** at about 25 MB a day, a full season of archive is under 10 GB.

All of these stay `provisional` in `config/params.toml`: the gate run and A2's full season can still revise them.

### 3. The 24 h offset: a coverage fact; the decision waits for MLB

**The coverage fact, for NBA:** Novig lists NBA moneylines a median of about 24 h before the start, and only 30–53% of markets a week are listed at least 24 h ahead. At the 24 h offset, 37 of 64, 29 of 61 and 46 of 65 markets had no price (`no_quotes`). The 6 h, 1 h and 15 min offsets are fully covered. Every market in weeks 2 and 3 was listed at least 15.9 h ahead.

**The offset set is not changed now.** The scope record keeps the 24 h offset ("the offset is not dropped", DESIGN.md §9.2), and `2026-10-16-track-b-closed.md` carried the 24 h drift to Track A's check 1. A different offset set would be a new prespecified calibration definition with its own reason, recorded before any statistic. It is not a parameter this record can set.

**It is decided once MLB's listing lead is measured,** from about Opening Day 2027, and before any check-1 statistic is computed. A1 may count coverage per offset in the meantime; nothing fitted. What that decision weighs:

- **Keeping 24 h.**
  - **For:** entries up to 24 h before the start stay scoreable, and it tests the drift Kalshi showed at 24 h. The listed-early subset is not obviously a biased sample for this purpose: an entry at 24 h needs a Novig price then too, so check 1 at 24 h tests the population it would be used on.
  - **Against, time:** `controls.sample_events` is 1,500 events at every offset. NBA gives about 61 a week at 6 h, 1 h and 15 min, but about 26 at 24 h. On NBA alone, 24 h would take about 1.5 seasons longer than the other offsets. Pooled with MLB, the delay is small if MLB lists most games 24 h ahead, and about 5 months (to the end of the 2027 MLB season) if it lists like NBA.
  - **Against, risk:** check 1 passes only if every offset passes, and the 24 h offset is the likeliest to fail. Kalshi drifted there, and the simulation's false alarms come mostly from it (DESIGN.md §9.2). A 24 h drift would abandon the Novig family for every offset. The way back is a new definition without 24 h, with an entry-timing policy in `docs/RESEARCH_PROTOCOL.md`, chosen after the failure. It would most defensibly be run on a fresh sample, at a cost of months to a season.
- **Changing it before any statistic** (replacing 24 h with 12 h, say, which every listed market in weeks 2 and 3 reached).
  - **For:** full coverage, a faster check 1, and no 24 h drift able to sink the shorter offsets. The reason is a venue fact measured in advance, the independent reason `2026-10-16-track-b-closed.md` asks for.
  - **Against:** entries earlier than the largest offset become unsupported (RESEARCH_PROTOCOL.md §6), which needs an entry-timing rule. The 24 h drift question stays open for Novig, apart from an optional diagnostic on the markets listed early.

**The deciding question** is whether models are expected to enter more than about 6–12 h before the start. If MLB lists early, keeping 24 h costs little; if it lists like NBA, changing becomes the stronger case.

**`a1.capture_lead_s` stays 25 h,** so every market listed 24 h ahead is captured from listing, whichever way the decision goes.

**Action:** ask developers@novig.com whether listing times follow a schedule, and whether markets can be listed earlier. Record the answer in `docs/vendor-capabilities.md`.

## Consequences

- **No configuration change:** `config.toml`, `config.gate.toml` and `config/params.toml` keep their values.
- **The gate plan is accepted** with this record. PR #36 is merged, and the Pi picks it up at the 12-10 preparation step (`git pull`, then the restart at T0).
- **The gate checklist is unchanged:** the quiet stall stays 360 s.
- **Follow-ups, after the gate:**
  - trace the week-2 `collection_gap`;
  - the single-pass ingest;
  - the database size;
  - the question to Novig;
  - **the offset decision,** once MLB's listing lead is measured (about Opening Day 2027), before any check-1 statistic.
