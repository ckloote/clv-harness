# A1 probe policy: liveness by bracketing, dense probes only near the start

Date: 2026-10-17

Status: accepted. It settles open decision 4 of `2026-10-16-a1-scope.md` (item 5, the probe budget). Its intervals are provisional: the storage pilot measures quiet intervals and probe spend, and a later record sets them.

## Context

**The budget does not cover the fixed interval.** DESIGN.md §7.2 probes every quiet channel every `stream.channel_probe_interval_s` (15 s), and the close function needs channel evidence within `stream.liveness_max_s` (30 s) before every as-of time, or the close is `feed_stalled`.
- **Cost:** a `book` probe costs 16 tokens. The `stream` bucket refills at 4 a second, and probes may not draw it below half its capacity (`r0.stream_probe_reserve_fraction`).
- **At NBA slate scale,** 20–30 markets are on each channel (`2026-10-16-a1-league.md`). With 25 quiet `book` markets, 15 s probes cost about 27 tokens a second, nearly seven times the refill.
- **Quiet is common.** On Paper, `book` was quiet for up to 152 s and `trades` for a whole 40-minute session. On Production, `book` was busy enough to need only two probes in a game (`docs/vendor-capabilities.md`). Most of a 25-hour window is quiet.

**The scope record's proposal, "probe where a price will be read," needs times nobody knows in advance.** Check 1's offsets (`controls.martingale_offsets`) and the close cutoffs (`close.buffer_sweep_s`) are measured from the *trusted start bound*, which A2 resolves after the game. An NBA tip-off is often minutes after the scheduled time. A4's entries come at arbitrary times.

**What a probe proves is more than recency.** A probe reply whose `seq` equals the last contiguous one proves the venue issued no `book` deltas on that channel since that `seq`. So the state was unchanged across the whole interval, not just at the probe (§7.2). A contiguous delta proves the same for the interval before it. The replay already treats both as coverage: a span runs from the subscribe snapshot to the last trustworthy observation, and every break is a gap (`gaps.stream_gaps`).
- **The exception: unsequenced changes.** Every known one comes with a lifecycle frame. At `GOLIVE` the venue drops resting orders with no deltas, and the replay treats any lifecycle transition as a `lifecycle_reset` gap.

## Decision

### 1. Liveness: a later observation can vouch for a quiet interval

A stream book at as-of time `t` is live when either holds, and no collection gap on its scope overlaps the interval:

- **(a) Recency, unchanged:** channel evidence within `stream.liveness_max_s` at or before `t`.
- **(b) Bracketing, new:** the first channel evidence after `t` continues the same state. It must be a contiguous delta or a confirmed probe, with no new tick between `t`'s book and it. A subscribe or resync snapshot never brackets, since it starts new state:
  - **A deliberate end:** a resubscription, or a capture window that ends and reopens, is never bridged.
  - **A connection change** is never bridged, since `seq` is per connection.
  - **Breaks** (lifecycle transitions, sequence gaps, lost connections) are already gaps, so they always break the bracket.

In the facts:
- `t`'s book is the latest stream tick at or before `t`.
- The bracket is the first `liveness_evidence` row (`delta_unchanged` or `probe_confirmed`) after `t`, with no stream tick between, or the first tick after `t` that rests on the same snapshot.
- No `collection_gap` may overlap (tick, bracket].

Nothing is added to the schema.

**A probe confirms only if it also shows no missed transition.** A probe reply carries the market's lifecycle `seq`. If it is ahead of the last lifecycle `seq` the replay saw on that connection, a transition was missed, perhaps with unsequenced book changes. The probe is then treated as a `lifecycle_reset`: a gap from the last trustworthy observation to the probe, which re-establishes the state. Today a confirmed `book` seq alone would vouch across it.

**Post-cutoff evidence may vouch for liveness, never for a price.** DESIGN.md §5.4 says no input used in selection or computation may occur after the as-of cutoff.
- **What the amendment allows:** a later observation may show only that the state chosen at or before `t` was still the venue's state at `t`.
- **What it changes and what it doesn't:** it changes which closes are scoreable. It never changes which tick is chosen or its price.
- **Missingness:** a close whose bracket never arrives (the feed dies before any later evidence) is `feed_stalled`. Feed health is not information about the game, so this kind of missingness is reported, not corrected. A close with recent evidence (a) stays scoreable as before, and any stall that a later probe exposes is a retroactive gap.

### 2. Cadence: slow everywhere, dense near the start

- **Quiet-channel probes** use `stream.quiet_probe_interval_s` (300 s, provisional) in place of 15 s outside a market's dense window. With bracketing, a slow probe still vouches for every as-of time before it.
- **The dense window** runs from `stream.dense_probe_lead_s` (900 s, provisional) before the scheduled start to `stream.dense_probe_tail_s` (1,800 s, provisional) after it. Inside it, quiet channels are probed every `stream.channel_probe_interval_s` (15 s, unchanged).
  - **Why:** every close cutoff falls between the start bound minus 600 s and the start, and `GOLIVE` breaks any bracket. A close needs evidence between its cutoff and `GOLIVE`, or within 30 s before the cutoff. The window covers the largest buffer, 300 s of margin, and a late tip-off.
  - **Busy markets** need no probes, since their deltas are the evidence.
  - **A game tipping off after the window** loses only what the window misses. Its closes may then be `feed_stalled`, which is reported.
- **Priority when the bucket is short:**
  1. resyncs and lifecycle probes, as now;
  2. dense-window probes;
  3. quiet probes.

  Every unaffordable probe is still archived as `probe_skipped`.
- **Budget:** 25 quiet `book` markets at 300 s cost about 1.3 tokens a second. Each quiet market inside its dense window adds about 1.1. A slate with several quiet markets tipping off together can still run short. The pilot measures how often, and the skipped probes and `feed_stalled` closes record it.

### 3. Not adopted now: more read keys

One `trading::read` key per channel connection would give `book` its own bucket. It is reported acceptable (`docs/vendor-capabilities.md`), but not confirmed in writing by Novig. It stays a lever if the pilot shows the budget short with this policy.

## Consequences

- **DESIGN.md:**
  - §7.2: quiet-channel probes follow the dense window and the quiet interval; liveness may come from a bracketing observation.
  - §5.4: the post-cutoff exception for liveness.
  - §10: rows for `stream.quiet_probe_interval_s`, `stream.dense_probe_lead_s` and `stream.dense_probe_tail_s`; `stream.liveness_max_s` notes the bracketing alternative.
- **PR 6, the code:**
  - **The recorder** (`novig/stream.py`, with dense windows from the scheduler): the cadence and priority above.
  - **The replay** (`venues/novig/parser.py`): the lifecycle-`seq` check on probes.
  - **The close function** (`close/definitions.py`, `price_as_of`): rule (b).
  - **§9.4 fixtures:**
    - an interval bracketed by an unchanged probe is trusted;
    - one spanning a lifecycle transition is not;
    - a sequence jump at the probe opens a gap;
    - a subscribe snapshot after a deliberate end does not bracket;
    - a probe showing a lifecycle `seq` ahead is a reset.
- **The storage pilot** reports:
  - quiet intervals by time to start;
  - probe spend and `probe_skipped` counts, inside and outside dense windows;
  - how often a close cutoff would have had neither recent nor bracketing evidence.

  A follow-up record sets the three intervals from those numbers, before the gate run.
- **Gate A1** runs with this policy. Its channel-stall fault tests the slow cadence: outside a dense window, a stall is exposed by the next quiet probe, up to 300 s later, and the gap still opens at the last trustworthy observation.
- **A4's arbitrary entry times** need nothing more: bracketing doesn't depend on knowing the as-of time in advance.
