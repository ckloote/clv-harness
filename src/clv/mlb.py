"""MLB StatsAPI: game identity, result and play-by-play times from archived responses.

Two archived responses carry a game: `/api/v1.1/game/{pk}/feed/live` (identity,
status, linescore and every play) and `/api/v1/game/{pk}/playByPlay` (plays
only). Both list plays in `allPlays`; the first play's `playEvents` carry the
status changes before the game (Pre-Game, Warmup, In Progress) and the first
pitch, each with a `startTime`.

`gamePk` is the canonical external identity: it survives postponement, and a
doubleheader's two games have different `gamePk`s (docs/vendor-capabilities.md).
"""
from __future__ import annotations

from dataclasses import dataclass

from clv.archive import RestExchange
from clv.timeutil import iso_ms


@dataclass(frozen=True)
class Team:
    id: int
    name: str
    abbreviation: str


@dataclass(frozen=True)
class Game:
    game_pk: int
    scheduled_start_ms: int     # gameData.datetime.dateTime: the schedule, never an off observation
    official_date: str
    original_date: str | None
    double_header: str          # N | S (split) | Y (traditional)
    game_number: int
    away: Team
    home: Team
    status: str                 # detailedState
    coded_state: str            # codedGameState: F final, I in progress, ...
    away_runs: int | None
    home_runs: int | None
    recv_ts_ms: int
    refs: tuple[str, ...]

    @property
    def final(self) -> bool:
        return self.coded_state == "F"

    @property
    def winner(self) -> Team | None:
        """The winning team of a final game, from the runs in the linescore."""
        if not self.final or self.away_runs is None or self.away_runs == self.home_runs:
            return None
        return self.away if self.away_runs > self.home_runs else self.home


@dataclass(frozen=True)
class PlayEvent:
    """A timed event in the first play: a status change or the first pitch."""
    game_pk: int
    kind: str                   # "first_pitch" | "status_change"
    description: str            # e.g. "Status Change - In Progress"
    start_ms: int
    end_ms: int | None
    play_id: str | None         # the pitch's playId, when it has one
    recv_ts_ms: int
    refs: tuple[str, ...]


def game(x: RestExchange) -> Game:
    """Identity, status and score from a `feed/live` response."""
    j = x.body.json()
    gd, ls = j["gameData"], j["liveData"].get("linescore", {}).get("teams", {})
    team = lambda side: Team(gd["teams"][side]["id"], gd["teams"][side]["name"], gd["teams"][side]["abbreviation"])
    return Game(gd["game"]["pk"], iso_ms(gd["datetime"]["dateTime"]), gd["datetime"]["officialDate"],
                gd["datetime"].get("originalDate"), gd["game"]["doubleHeader"], gd["game"]["gameNumber"],
                team("away"), team("home"), gd["status"]["detailedState"], gd["status"]["codedGameState"],
                ls.get("away", {}).get("runs"), ls.get("home", {}).get("runs"), x.body.recv_ts_ms, x.refs)


def game_pk_of(x: RestExchange) -> int | None:
    for s in x.subjects:
        if s.startswith("mlb:game:"):
            return int(s.removeprefix("mlb:game:"))
    return None


def first_play_events(x: RestExchange) -> list[PlayEvent]:
    """Status changes and the first pitch from a `feed/live` or `playByPlay` response.

    Only the first play is read: it holds the pregame status changes and the
    game's first pitch. Empty when no play has started.
    """
    j = x.body.json()
    plays = j["liveData"]["plays"]["allPlays"] if "liveData" in j else j.get("allPlays", [])
    if not plays:
        return []
    pk = game_pk_of(x)
    out = []
    for e in plays[0].get("playEvents", []):
        if e.get("isPitch"):
            out.append(PlayEvent(pk, "first_pitch", e.get("details", {}).get("description", ""),
                                 iso_ms(e["startTime"]), iso_ms(e["endTime"]) if e.get("endTime") else None,
                                 e.get("playId"), x.body.recv_ts_ms, x.refs))
            break
        if e.get("type") == "action" and e.get("details", {}).get("description", "").startswith("Status Change"):
            out.append(PlayEvent(pk, "status_change", e["details"]["description"], iso_ms(e["startTime"]),
                                 iso_ms(e["endTime"]) if e.get("endTime") else None, None,
                                 x.body.recv_ts_ms, x.refs))
    return out
