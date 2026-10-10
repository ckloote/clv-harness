"""Off resolution (DESIGN.md §5.2): a trusted start interval from independent sightings.

Accepted start claims, each a distinct `detected_off_ts_ms` (repeated sightings
of one claim count once):

- StatsAPI `first_pitch`: the physical first pitch.
- StatsAPI `status_in_progress`: the official transition to "In Progress".
- Novig `venue_golive`: the venue's own perception of the start. It has preceded
  the first pitch on every observed game, so it can only move the earliest bound
  earlier.

StatsAPI "Warmup" is a pregame status, not a start claim, and is ignored.

    earliest = the earliest claim of any accepted source (DESIGN.md §5.2: the
               conservative boundary comes from the earliest credible bound)
    latest   = the latest StatsAPI first pitch, else the latest StatsAPI claim
    selected = the earliest StatsAPI first pitch, else the earliest StatsAPI claim

Untrusted, with the bounds still recorded when there are any:
- `no_trusted_off`: no StatsAPI start claim. A venue transition alone is the
  venue's perception, and a scheduled time is never an observation.
- `off_disagreement`: the claims span more than `off.max_source_disagreement_s`.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass

from clv.config import param

RESOLVER_VERSION = "off-v0.1"
START_KINDS = {("mlb_statsapi", "first_pitch"), ("mlb_statsapi", "status_in_progress"),
               ("novig_stream", "venue_golive")}


@dataclass(frozen=True)
class OffResolution:
    event_id: int
    selected_ms: int | None
    earliest_ms: int | None
    latest_ms: int | None
    source_ids: tuple[int, ...]         # off_observation ids of every accepted sighting
    disagreement_ms: int | None
    reason: str | None                  # None: trusted

    @property
    def trusted(self) -> bool:
        return self.reason is None


def resolve(conn: sqlite3.Connection, event_id: int) -> OffResolution:
    """Resolve one event from the current fact snapshot (`lineage.use`)."""
    rows = [r for r in conn.execute(
        "SELECT off_observation_id, source, kind, detected_off_ts_ms FROM f_off_observation WHERE event_id = ? "
        "ORDER BY off_observation_id", (event_id,)) if (r[1], r[2]) in START_KINDS]
    if not rows:
        return OffResolution(event_id, None, None, None, (), None, param("off.scheduled_only"))
    claims = sorted({r[3] for r in rows})
    mlb = sorted({r[3] for r in rows if r[1] == "mlb_statsapi"})
    pitches = sorted({r[3] for r in rows if r[2] == "first_pitch"})
    earliest = claims[0]
    disagreement = claims[-1] - claims[0]
    ids = tuple(r[0] for r in rows)
    if not mlb:
        return OffResolution(event_id, earliest, earliest, claims[-1], ids, disagreement, "no_trusted_off")
    latest = pitches[-1] if pitches else mlb[-1]
    selected = pitches[0] if pitches else mlb[0]
    reason = "off_disagreement" if disagreement > param("off.max_source_disagreement_s") * 1000 else None
    return OffResolution(event_id, selected, earliest, latest, ids, disagreement, reason)


def write(conn: sqlite3.Connection, snapshot_id: int, r: OffResolution, now_ms: int) -> int:
    return conn.execute(
        "INSERT INTO off_resolution (fact_snapshot_id, resolver_version, event_id, selected_start_ts_ms,"
        " earliest_plausible_start_ts_ms, latest_plausible_start_ts_ms, source_set, source_disagreement_ms,"
        " confidence, reason, computed_ts_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (snapshot_id, RESOLVER_VERSION, r.event_id, r.selected_ms, r.earliest_ms, r.latest_ms,
         json.dumps(list(r.source_ids)), r.disagreement_ms, "trusted" if r.trusted else "untrusted", r.reason,
         now_ms)).lastrowid
