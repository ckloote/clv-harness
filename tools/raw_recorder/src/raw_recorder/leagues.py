"""League discovery (A1): which Novig events to record, for leagues other than MLB.

MLB games are found from the StatsAPI schedule and matched to venue markets
(raw_recorder.discovery). Other leagues are found from Novig's catalog alone:
every `[discovery] interval_s`, for each enabled `[[league]]`, the recorder
reads (and archives) the league's open events and `MONEY` markets, and lists
each event that has a moneyline and starts within the league's `days_ahead`.
It merges the result into `<archive>/events.discovered.toml`, which the
recorder reloads at once, with the same rules as games: a market once listed
stays listed, the newest catalog wins before a window opens, an open window
only grows, and an ended one is left as it was (discovery.merge).

The unit is the Novig event. Which league game it is (the league's own game ID)
is decided in A2, from the league's official schedule; nothing here claims it.
A window runs `a1.capture_lead_s` before the event's `startsTs`, so the 24 h
martingale offset is captured, unless the market is listed later than that:
nothing can be recorded before discovery finds it.

If a market's `startsTs` differs from its event's by more than CONFLICT_S, the
window stretches to cover both, as for MLB.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field

from .config import Config, LeagueEvent, Params, load_events
from .discovery import CONFLICT_S, _et, _utc, merge, write
from .novig import public as novig_public
from .rest import RestRecorder

DAY_MS = 86_400_000


@dataclass
class LeagueDiscovery:
    events: list[LeagueEvent]
    notes: list[str] = field(default_factory=list)      # leagues unavailable, events skipped
    warnings: list[str] = field(default_factory=list)   # an event with several moneylines: all recorded


def match(league: str, events: list[dict], markets: list[dict] | None, params: Params, now_ms: int,
          days_ahead: float) -> LeagueDiscovery:
    """One league's events with a moneyline, from its catalog. None for `markets` means the
    market listing could not be read whole: nothing is concluded from it."""
    out = LeagueDiscovery([])
    if markets is None:
        out.notes.append(f"{league}: Novig catalog unavailable; not matched this time")
        return out
    by_event: dict[str, list[dict]] = defaultdict(list)
    for m in markets:
        if m.get("marketType") == "MONEY" and m.get("eventId"):
            by_event[m["eventId"]].append(m)
    described = {e["eventId"]: e for e in events if e.get("eventId")}
    horizon = now_ms + days_ahead * DAY_MS
    lead, tail = params.a1_capture_lead_s, params.a1_capture_tail_s
    for event_id, ms in sorted(by_event.items()):
        ev = described.get(event_id, {})
        starts = {f"Novig market {m['marketId']}": m["startsTs"] for m in ms if m.get("startsTs") is not None}
        start = ev.get("startsTs")
        if start is None:
            start = min(starts.values(), default=None)      # the event page failed: its markets still say
        else:
            starts["Novig event"] = start
        what = f"{league} {ev.get('description') or '/'.join(o.get('name', '?') for o in ms[0].get('outcomes', []))}" \
               f" (event {event_id})"
        if start is None:
            out.notes.append(f"{what}: no start time listed")
            continue
        if start > horizon:
            continue                                        # beyond days_ahead: listed by a later round
        notes = []
        if len(ms) > 1:
            notes.append(f"{len(ms)} Novig MONEY markets, all recorded: check which is the game's moneyline")
            out.warnings.append(f"{what}: {notes[-1]}")
        ev_lead, ev_tail = lead, tail
        if any(abs(t - start) > CONFLICT_S * 1000 for t in starts.values()):
            ev_lead += max(0, start - min(starts.values())) // 1000
            ev_tail += max(0, max(starts.values()) - start) // 1000
            notes.append("start times differ; window covers all: "
                         + "; ".join(f"{k} {_et(t)}" for k, t in sorted(starts.items(), key=lambda kv: kv[1])))
        if ev and ev.get("league") not in (None, league):
            notes.append(f"the event lists league {ev.get('league')!r}")
        out.events.append(LeagueEvent(
            league=league, event_id=event_id, scheduled_start_ms=start,
            label=f"{ev.get('description') or what}, {league}, {_et(start)}",
            novig_markets=tuple(sorted(m["marketId"] for m in ms)),
            capture_lead_s=ev_lead if ev_lead != lead else None,
            capture_tail_s=ev_tail if ev_tail != tail else None,
            notes=tuple(notes)))
    return out


def merge_events(existing: list[LeagueEvent], found: list[LeagueEvent], now_ms: int, p: Params) -> list[LeagueEvent]:
    return merge(existing, found, now_ms, p, key=lambda e: e.event_id)


def render(events: list[LeagueEvent], discovered_at_ms: int, notes: list[str]) -> str:
    """The discovered league events as TOML (config.load_events reads it back)."""
    lines = ["# Written by the recorder (raw_recorder.leagues); do not edit. Each entry is one Novig",
             "# event in a league other than MLB, recorded from a1.capture_lead_s before its start.",
             f'discovered_at_utc = "{_utc(discovered_at_ms)}"',
             f"last_notes = {json.dumps(notes)}", ""]
    for e in events:
        lines += ["[[event]]", f"league = {json.dumps(e.league)}", f"event_id = {json.dumps(e.event_id)}",
                  f"label = {json.dumps(e.label)}", f'scheduled_start_utc = "{_utc(e.scheduled_start_ms)}"',
                  f"novig_markets = {json.dumps(list(e.novig_markets))}"]
        if e.capture_lead_s is not None:
            lines.append(f"capture_lead_s = {e.capture_lead_s}")
        if e.capture_tail_s is not None:
            lines.append(f"capture_tail_s = {e.capture_tail_s}")
        if e.notes:
            lines.append(f"notes = {json.dumps(list(e.notes))}")
        lines.append("")
    return "\n".join(lines)


async def discover(cfg: Config, novig_rest: RestRecorder, now_ms: int) -> tuple[list[LeagueEvent], LeagueDiscovery]:
    """One round over every enabled league: fetch (archived), match, merge into the discovered
    events file and write it. Returns the merged list and this round's matches."""
    found = LeagueDiscovery([])
    for lg in cfg.enabled_leagues():
        events, markets, complete = await novig_public.fetch_catalog(novig_rest, cfg.novig["host"], lg["name"])
        got = match(lg["name"], events, markets if complete else None, cfg.params, now_ms,
                    float(lg.get("days_ahead", 2)))
        found.events += got.events
        found.notes += got.notes
        found.warnings += got.warnings
    merged = merge_events(load_events(cfg.events_discovered_path), found.events, now_ms, cfg.params)
    write(cfg.events_discovered_path, render(merged, now_ms, found.warnings + found.notes))
    return merged, found
