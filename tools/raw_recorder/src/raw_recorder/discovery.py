"""Game discovery: which games to record, found from the schedule and the venue catalogs.

The running recorder calls `discover` every `discovery.interval_s` (and the
`catalog` command once). It archives the StatsAPI schedule and the Novig and
Kalshi catalogs it reads, matches each game to its markets with fixed rules,
and merges the result into `<archive>/games.discovered.toml`, which the
recorder reloads at once. Nobody has to stop the recorder or edit a file.

Matching (a game is a StatsAPI gamePk of one of `discovery.game_types`):
- Novig: every `MONEY` market whose two outcome names are the two teams'
  Novig abbreviations and whose `startsTs` is within `NOVIG_MATCH_S` of the
  StatsAPI `gameDate`.
- Kalshi: every event of the series on the game's Eastern date whose two
  market suffixes are the two teams' StatsAPI abbreviations.
- A start time the venues disagree on: the window is widened so recording
  begins `capture_lead_s` before the earliest claimed start and runs
  `capture_tail_s` past the latest (MLB's time stays the scheduled start).

Capture fails open and scoring fails closed. More than one candidate (a
doubleheader, say) means every candidate is recorded and the entry says so in
`notes`. A discovered link is never taken as checked: the harness ingests it as
`fuzzy_candidate`, and a correction verifies it when the game is scored
(clv.ingest, `clv correct`). A game listed in games.toml, checked by hand,
overrides its discovered entry.

Merging never loses capture. A market once listed stays listed. Before a game's
window opens, the newest schedule wins; once it is open the window only grows;
a game whose window has ended is left as it was.
"""
from __future__ import annotations

import json
import os
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from . import kalshi
from .config import Config, Game, Params, load_games
from .novig import public as novig_public
from .rest import RestRecorder

ET = ZoneInfo("America/New_York")
NOVIG_MATCH_S = 6 * 3600            # a Novig market's startsTs within this of the StatsAPI gameDate
CONFLICT_S = 15 * 60                # a venue start further than this from MLB's widens the window
# Novig names MONEY outcomes by its own abbreviations; StatsAPI -> Novig. The harness keeps
# the same table (clv.identity.NOVIG_ABBR); a test checks the two agree.
NOVIG_ABBR = {"KC": "KAN", "AZ": "ARI", "WSH": "WAS", "ATH": "OAK"}


@dataclass
class Discovery:
    games: list[Game]
    notes: list[str] = field(default_factory=list)      # games skipped, sources unavailable
    warnings: list[str] = field(default_factory=list)   # ambiguous matches: every candidate is recorded


def _utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _et(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, ET).strftime("%a %b %d %H:%M ET")


def kalshi_event_start_ms(event_ticker: str) -> int | None:
    """KXMLBGAME-26OCT071800LADATL -> 2026-10-07 18:00 ET, in UTC ms."""
    try:
        code = event_ticker.split("-")[1][:11]
        return int(datetime.strptime(code, "%y%b%d%H%M").replace(tzinfo=ET).timestamp() * 1000)
    except (IndexError, ValueError):
        return None


def match(schedule: list[dict], novig_markets: list[dict] | None, kalshi_markets: list[dict] | None,
          params: Params, game_types: set[str]) -> Discovery:
    """Match each scheduled game to its markets. None for a venue means its catalog could not
    be read: nothing is concluded about it (an empty list means it lists no market)."""
    out = Discovery([])
    if novig_markets is None:
        out.notes.append("Novig catalog unavailable: Novig markets not matched this time")
    if kalshi_markets is None:
        out.notes.append("Kalshi catalog unavailable or disabled: Kalshi markets not matched")
    k_events: dict[str, list[str]] = defaultdict(list)
    for m in kalshi_markets or []:
        k_events[m.get("event_ticker", "")].append(m["ticker"])
    for g in schedule:
        if g.get("gameType") not in game_types:
            continue
        pk, status = g["gamePk"], g.get("status", {})
        away, home = g["teams"]["away"]["team"], g["teams"]["home"]["team"]
        what = f"{away.get('name')} @ {home.get('name')} (gamePk {pk})"
        if status.get("startTimeTBD"):
            out.notes.append(f"{what}: start time TBD, not scheduled yet")
            continue
        if status.get("detailedState") in ("Postponed", "Cancelled"):
            out.notes.append(f"{what}: {status['detailedState']}")
            continue
        start = int(datetime.fromisoformat(g["gameDate"].replace("Z", "+00:00")).timestamp() * 1000)
        a, h = away.get("abbreviation"), home.get("abbreviation")
        notes, starts = [], {"MLB StatsAPI": start}

        novig = [m for m in novig_markets or []
                 if {o.get("name") for o in m.get("outcomes", [])} == {NOVIG_ABBR.get(a, a), NOVIG_ABBR.get(h, h)}
                 and m.get("startsTs") is not None and abs(m["startsTs"] - start) <= NOVIG_MATCH_S * 1000]
        for m in novig:
            starts[f"Novig {m['marketId']}"] = m["startsTs"]
        if len(novig) > 1:
            notes.append(f"{len(novig)} Novig candidates, all recorded: check which is this game")
            out.warnings.append(f"{what}: {notes[-1]}")
        elif not novig and novig_markets is not None:
            notes.append("no Novig MONEY market listed yet")

        day = datetime.fromtimestamp(start / 1000, ET).date()
        kal = {}
        for ev, tickers in k_events.items():
            t = kalshi_event_start_ms(ev)
            if t is not None and datetime.fromtimestamp(t / 1000, ET).date() == day \
                    and {x.rsplit("-", 1)[-1] for x in tickers} == {a, h}:
                kal[ev] = sorted(tickers)
                starts[f"Kalshi {ev}"] = t
        if len(kal) > 1:
            notes.append(f"{len(kal)} Kalshi events, all recorded: check which is this game")
            out.warnings.append(f"{what}: {notes[-1]}")
        elif not kal and kalshi_markets is not None:
            notes.append(f"no Kalshi event on {day} with tickers {a} and {h}")

        lead, tail = params.capture_lead_s, params.capture_tail_s
        if any(abs(t - start) > CONFLICT_S * 1000 for t in starts.values()):
            lead += max(0, start - min(starts.values())) // 1000
            tail += max(0, max(starts.values()) - start) // 1000
            notes.append("start times differ; window covers all: "
                         + "; ".join(f"{k} {_et(t)}" for k, t in sorted(starts.items(), key=lambda kv: kv[1])))
        if not novig and not kal:
            out.notes.append(f"{what}: no market matched yet")
            continue
        label = f"{away.get('name')} @ {home.get('name')}, {g.get('description') or g.get('seriesDescription', '')}, " \
                f"{_et(start)}"
        out.games.append(Game(
            game_pk=pk, scheduled_start_ms=start, label=label,
            novig_markets=tuple(sorted(m["marketId"] for m in novig)),
            kalshi_tickers=tuple(t for ev in sorted(kal) for t in kal[ev]),
            capture_lead_s=lead if lead != params.capture_lead_s else None,
            capture_tail_s=tail if tail != params.capture_tail_s else None,
            source="discovered", notes=tuple(notes)))
    return out


def _window(g: Game, p: Params) -> tuple[int, int]:
    lead = g.capture_lead_s if g.capture_lead_s is not None else p.capture_lead_s
    tail = g.capture_tail_s if g.capture_tail_s is not None else p.capture_tail_s
    return g.scheduled_start_ms - lead * 1000, g.scheduled_start_ms + tail * 1000


def merge(existing: list[Game], found: list[Game], now_ms: int, p: Params) -> list[Game]:
    """Fold a discovery into the discovered list without ever losing capture (module docstring)."""
    by_pk = {g.game_pk: g for g in existing}
    for new in found:
        old = by_pk.get(new.game_pk)
        if old is None:
            by_pk[new.game_pk] = new
            continue
        lo, hi = _window(old, p)
        if hi <= now_ms:
            continue                                    # over: the record of what was captured
        markets = tuple(dict.fromkeys(old.novig_markets + new.novig_markets))
        tickers = tuple(dict.fromkeys(old.kalshi_tickers + new.kalshi_tickers))
        if lo > now_ms:                                 # not open yet: the newest schedule wins
            g = replace(new, novig_markets=markets, kalshi_tickers=tickers)
        else:                                           # open: keep its start, only grow its end
            tail = max(hi, _window(new, p)[1]) - old.scheduled_start_ms
            g = replace(old, novig_markets=markets, kalshi_tickers=tickers, notes=new.notes,
                        capture_tail_s=tail // 1000 if tail // 1000 != p.capture_tail_s else None)
        by_pk[new.game_pk] = g
    return sorted(by_pk.values(), key=lambda g: (g.scheduled_start_ms, g.game_pk))


def render(games: list[Game], discovered_at_ms: int, notes: list[str]) -> str:
    """The discovered list as TOML. An entry can be copied into games.toml as it stands,
    once checked by hand, to make it a hand-checked override."""
    lines = ["# Written by the recorder (raw_recorder.discovery); do not edit. To fix or check an",
             "# entry by hand, copy it into tools/raw_recorder/games.toml, which overrides it.",
             f'discovered_at_utc = "{_utc(discovered_at_ms)}"',
             f"last_notes = {json.dumps(notes)}", ""]     # the latest round's notes and warnings
    for g in games:
        lines += ["[[game]]", f"game_pk = {g.game_pk}", f"label = {json.dumps(g.label)}",
                  f'scheduled_start_utc = "{_utc(g.scheduled_start_ms)}"',
                  f"novig_markets = {json.dumps(list(g.novig_markets))}",
                  f"kalshi_tickers = {json.dumps(list(g.kalshi_tickers))}"]
        if g.capture_lead_s is not None:
            lines.append(f"capture_lead_s = {g.capture_lead_s}")
        if g.capture_tail_s is not None:
            lines.append(f"capture_tail_s = {g.capture_tail_s}")
        if g.notes:
            lines.append(f"notes = {json.dumps(list(g.notes))}")
        lines.append("")
    return "\n".join(lines)


def last_run(path: Path) -> tuple[int | None, list[str]]:
    """When the discovered list was last written, and that round's notes."""
    import tomllib
    try:
        doc = tomllib.loads(path.read_text())
    except FileNotFoundError:
        return None, []
    at = doc.get("discovered_at_utc")
    ms = int(datetime.fromisoformat(at.replace("Z", "+00:00")).timestamp() * 1000) if at else None
    return ms, list(doc.get("last_notes", []))


def write(path: Path, text: str) -> None:
    """Atomically: a reader sees the old file or the new one, never half of one."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


async def fetch(cfg: Config, mlb_rest: RestRecorder, novig_rest: RestRecorder, kalshi_rest: RestRecorder | None,
                today: date) -> tuple[list[dict], list[dict] | None, list[dict] | None]:
    """Read (and archive) the schedule for `today` and the next `discovery.days_ahead` days,
    and the venue catalogs. A catalog that could not be read is None."""
    end = today + timedelta(days=int(cfg.discovery.get("days_ahead", 1)))
    res = await mlb_rest.request("GET", cfg.mlb["base_url"], "/api/v1/schedule",
                                 params={"sportId": "1", "startDate": today.isoformat(),
                                         "endDate": end.isoformat(), "hydrate": "team"},
                                 subjects=["mlb:schedule"], purpose="catalog")
    if not res.ok or res.body is None:
        raise RuntimeError(f"StatsAPI schedule unavailable ({res.status or res.failure})")
    schedule = [g for d in json.loads(res.body).get("dates", []) for g in d.get("games", [])]
    novig = None
    if cfg.enabled(cfg.novig):
        _, markets, complete = await novig_public.fetch_catalog(novig_rest, cfg.novig["host"])
        novig = markets if complete else None
    kal = None
    if kalshi_rest is not None and cfg.enabled(cfg.kalshi):
        markets, complete = await kalshi.fetch_series(kalshi_rest, cfg.kalshi["base_url"], cfg.kalshi["series_ticker"])
        kal = markets if complete else None
    return schedule, novig, kal


async def discover(cfg: Config, mlb_rest: RestRecorder, novig_rest: RestRecorder, kalshi_rest: RestRecorder | None,
                   now_ms: int) -> tuple[list[Game], Discovery]:
    """One discovery round: fetch, match, merge into the discovered file and write it.
    Returns the merged discovered list and this round's matches."""
    today = datetime.fromtimestamp(now_ms / 1000, ET).date()
    schedule, novig, kal = await fetch(cfg, mlb_rest, novig_rest, kalshi_rest, today)
    found = match(schedule, novig, kal, cfg.params, set(cfg.discovery.get("game_types", ["F", "D", "L", "W"])))
    merged = merge(load_games(cfg.discovered_path, "discovered"), found.games, now_ms, cfg.params)
    write(cfg.discovered_path, render(merged, now_ms, found.warnings + found.notes))
    return merged, found
