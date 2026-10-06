"""MLB StatsAPI: the game feed and play-by-play for a gamePk, and the schedule.

The play-by-play first pitch is retrospectively authoritative (DESIGN.md
§5.2), so these fetches can run any time after a game; re-fetching later
captures corrections as new archive records.
"""
from __future__ import annotations

import json

from .rest import RestRecorder


async def fetch_game(rest: RestRecorder, base: str, game_pk: int) -> None:
    subjects = [f"mlb:game:{game_pk}"]
    await rest.request("GET", base, f"/api/v1.1/game/{game_pk}/feed/live",
                       subjects=subjects, purpose="game_feed")
    await rest.request("GET", base, f"/api/v1/game/{game_pk}/playByPlay",
                       subjects=subjects, purpose="play_by_play")


async def fetch_schedule(rest: RestRecorder, base: str, start_date: str, end_date: str) -> list[dict]:
    res = await rest.request("GET", base, "/api/v1/schedule",
                             params={"sportId": "1", "startDate": start_date, "endDate": end_date},
                             subjects=["mlb:schedule"], purpose="catalog")
    if not res.ok or res.body is None:
        return []
    return [g for day in json.loads(res.body).get("dates", []) for g in day.get("games", [])]
