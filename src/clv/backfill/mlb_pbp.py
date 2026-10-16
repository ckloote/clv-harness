"""MLB games for the historical import: identity and results from the StatsAPI schedule, start
claims from play-by-play (DESIGN.md §5.2), all from the backfill archive (`mlb_hist`).

The schedule lists a postponed game twice under one gamePk: a `Postponed` row on the
original date and a `Final` row on the day it was played (docs/feasibility.md §5). Each
distinct row is an event_alias observation; the game's facts use the row it was played
under.
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from clv import archive, mlb
from clv.backfill import settlements
from clv.identity import ensure_event, ensure_outcome
from clv.off import sources
from clv.timeutil import iso_ms

SOURCE = "mlb_hist"


@dataclass(frozen=True)
class Team:
    id: int
    abbreviation: str
    name: str


@dataclass(frozen=True)
class ScheduledGame:
    game_pk: int
    season: int
    official_date: str              # of the row the game was played (or last listed) under
    start_ms: int                   # gameDate: the schedule, never an off observation
    status: str                     # detailedState of that row
    double_header: str              # N | S | Y
    game_number: int
    away: Team
    home: Team
    winner: str | None              # the winning team's abbreviation, for a Final game
    moved: bool                     # listed on more than one date, postponed or rescheduled

    @property
    def final(self) -> bool:
        return self.status == "Final"


def schedule(root: Path) -> tuple[dict[int, ScheduledGame], list[tuple]]:
    """Every game the archived schedules list, and each (exchange, row, item) for the aliases and
    settlements: `item` numbers the response's games in order, across its dates."""
    teams: dict[tuple[int, int], Team] = {}
    rows: list[tuple] = []
    segs = archive.segments(root, SOURCE)
    for x in archive.stream_exchanges(root, segs):
        if not x.ok:
            continue
        purpose = x.request.get("purpose")
        if purpose == "teams":
            season = int(dict(p.split("=") for p in x.request["query"].split("&"))["season"])
            for t in x.body.json().get("teams", []):
                teams[(season, t["id"])] = Team(t["id"], t["abbreviation"], t["name"])
        elif purpose == "schedule":
            games = [g for day in x.body.json().get("dates", []) for g in day.get("games", [])]
            rows += [(x, g, item) for item, g in enumerate(games)]
    by_pk: dict[int, list[dict]] = {}
    for _, g, _ in rows:
        by_pk.setdefault(g["gamePk"], []).append(g)
    games = {}
    for pk, gs in by_pk.items():
        played = [g for g in gs if g["status"].get("detailedState") == "Final"] or gs
        g = played[-1]
        season = int(g.get("season") or g["officialDate"][:4])
        away, home = (_team(teams, season, g["teams"][s]["team"]) for s in ("away", "home"))
        winner = next((t.abbreviation for s, t in (("away", away), ("home", home))
                       if g["teams"][s].get("isWinner")), None)
        dates = {r["officialDate"] for r in gs}
        moved = len(dates) > 1 or any(r["status"].get("detailedState") == "Postponed" or r.get("rescheduledFrom")
                                      or r.get("rescheduledFromDate") for r in gs)
        games[pk] = ScheduledGame(pk, season, g["officialDate"], iso_ms(g["gameDate"]),
                                  g["status"].get("detailedState", ""), g.get("doubleHeader", "N"),
                                  int(g.get("gameNumber", 1)), away, home, winner, moved)
    return games, rows


def _team(teams: dict, season: int, t: dict) -> Team:
    """The season's team, or one without an abbreviation when the team list lacks it."""
    return teams.get((season, t["id"])) or Team(t["id"], "", t.get("name", ""))


def import_games(w, root: Path, report: Counter) -> dict[int, ScheduledGame]:
    """Events, outcomes and schedule aliases for every listed game; off observations from each
    archived play-by-play. Idempotent: a row already stored is not stored again."""
    w.know(archive.segments(root, SOURCE))
    games, rows = schedule(root)
    for pk, g in sorted(games.items()):
        if not (g.away.abbreviation and g.home.abbreviation):
            report["mlb_game_without_team_abbreviation"] += 1
            continue
        event = ensure_event(w, pk)
        for t in (g.away, g.home):
            ensure_outcome(w, event, pk, t.id, t.abbreviation, t.name)
    for x, r, _ in rows:
        g = games.get(r["gamePk"])
        if g is None or not (g.away.abbreviation and g.home.abbreviation):
            continue
        event = ensure_event(w, g.game_pk)
        detail = json.dumps(dict(double_header=r.get("doubleHeader"), game_number=r.get("gameNumber"),
                                 official_date=r["officialDate"], status=r["status"].get("detailedState"),
                                 rescheduled_from=r.get("rescheduledFromDate") or r.get("rescheduledFrom"),
                                 away=g.away.abbreviation, home=g.home.abbreviation), sort_keys=True)
        start = iso_ms(r["gameDate"])
        if w.conn.execute("SELECT 1 FROM event_alias WHERE event_id = ? AND provider = 'mlb_statsapi'"
                          " AND scheduled_start_ms IS ? AND detail_json = ?", (event, start, detail)).fetchone():
            continue                        # an unchanged repeat is not a new observation
        w.insert("event_alias", dict(
            event_id=event, provider="mlb_statsapi", provider_event_id=str(g.game_pk),
            title=f"{g.away.name} @ {g.home.name}", scheduled_start_ms=start, detail_json=detail, status="exact",
            method="gamePk", evidence="StatsAPI schedule row for this gamePk", observed_ts_ms=x.body.recv_ts_ms,
            **w.cited(x.body.ref)))
    for x in archive.stream_exchanges(root, archive.segments(root, SOURCE)):
        if not (x.ok and x.request.get("purpose") == "play_by_play"):
            continue
        pk = mlb.game_pk_of(x)
        if pk not in games or not w.conn.execute(
                "SELECT 1 FROM event WHERE league = 'MLB' AND league_game_id = ?", (str(pk),)).fetchone():
            continue
        event = ensure_event(w, pk)
        aid, line = w.cite(x.body.ref)
        if w.conn.execute("SELECT 1 FROM off_observation WHERE raw_artifact_id = ? AND raw_line = ?",
                          (aid, line)).fetchone():
            continue
        for o in sources.from_play_events(mlb.first_play_events(x)):
            w.insert("off_observation", dict(event_id=event, source=o.source, kind=o.kind, subject=o.subject,
                                             detected_off_ts_ms=o.detected_off_ts_ms, observed_ts_ms=o.observed_ts_ms,
                                             raw_artifact_id=aid, raw_line=line))
    settlements.from_schedule(w, rows, report)
    report["mlb_games"] = len(games)
    return games
