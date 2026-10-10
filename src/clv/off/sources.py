"""Off observations (DESIGN.md §5.2): every independent sign that a game began.

Each observation keeps its source, what it claims (`detected_off_ts_ms`), when
the harness received the evidence (`observed_ts_ms`) and the raw frames it came
from. Sources are not ranked here; `off/resolver.py` decides how they combine.

- StatsAPI first pitch: the first pitch event's `startTime`. Usually read
  retrospectively, so `observed_ts_ms` can be hours after the claimed time.
- StatsAPI status changes ("Warmup", "In Progress"): official transitions,
  logged with their own times in the first play.
- Novig `GOLIVE`: the venue's own perception of the start. It has preceded the
  first pitch on every game observed, so it is an early venue bound, not the off.

A scheduled time (StatsAPI `gameDate`, Novig `startsTs`, The Odds API
`commence_time`) is never an off observation.
"""
from __future__ import annotations

from dataclasses import dataclass

from clv.mlb import PlayEvent
from clv.venues.novig.parser import Lifecycle


@dataclass(frozen=True)
class OffObservation:
    game_pk: int | None         # resolved to an internal event by identity at ingest
    subject: str                # what the source observed: "mlb:game:<pk>" | "novig:market:<id>"
    source: str                 # "mlb_statsapi" | "novig_stream"
    kind: str                   # "first_pitch" | "status_warmup" | "status_in_progress" | "venue_golive"
    detected_off_ts_ms: int
    observed_ts_ms: int
    refs: tuple[str, ...]


STATUS_KINDS = {"Status Change - Warmup": "status_warmup",
                "Status Change - In Progress": "status_in_progress"}


def from_play_events(events: list[PlayEvent]) -> list[OffObservation]:
    out = []
    for e in events:
        kind = "first_pitch" if e.kind == "first_pitch" else STATUS_KINDS.get(e.description)
        if kind:
            out.append(OffObservation(e.game_pk, f"mlb:game:{e.game_pk}", "mlb_statsapi", kind, e.start_ms,
                                      e.recv_ts_ms, e.refs))
    return out


def from_lifecycle(lc: Lifecycle, game_pk: int | None = None) -> OffObservation | None:
    if lc.kind != "GOLIVE":
        return None
    return OffObservation(game_pk, f"novig:market:{lc.market}", "novig_stream", "venue_golive", lc.venue_ts_ms,
                          lc.recv_ts_ms, (lc.ref,))
