"""The Odds API (the-odds-api.com) h2h entry quotes, with a credit floor.

Credit usage comes from the response headers x-requests-remaining /
x-requests-used / x-requests-last, all archived in each rest_complete event.
Below `r0.odds_quota_floor` remaining credits, polling stops and an event
records why (DESIGN.md §13 R0).
"""
from __future__ import annotations

from .archive import Stream
from .rest import RestRecorder, RestResult

CONTROL_CONN = "odds_api-control"


def _num(value: str | None) -> float | None:
    try:
        return float(value) if value is not None else None
    except ValueError:
        return None


class OddsPoller:
    def __init__(self, rest: RestRecorder, stream: Stream, section: dict,
                 api_key: str | None, quota_floor: int):
        self.rest = rest
        self.stream = stream
        self.section = section
        self.api_key = api_key
        self.floor = quota_floor
        self.remaining: float | None = None
        self.used: float | None = None
        self.stopped = False
        if api_key is None:
            self.stopped = True
            stream.event(CONTROL_CONN, "odds_disabled",
                         reason=f"no API key in ${section.get('key_env')}")

    def _note_quota(self, res: RestResult) -> None:
        remaining = _num(res.headers.get("x-requests-remaining"))
        used = _num(res.headers.get("x-requests-used"))
        if remaining is not None:
            self.remaining = remaining
        if used is not None:
            self.used = used
        if self.remaining is not None and self.remaining < self.floor and not self.stopped:
            self.stopped = True
            self.stream.event(CONTROL_CONN, "quota_floor_stop", remaining=self.remaining,
                              used=self.used, floor=self.floor, request_id=res.request_id)

    async def check_quota(self) -> None:
        """GET /v4/sports costs no credits and reports the quota headers."""
        if self.api_key is None:
            return
        res = await self.rest.request("GET", self.section["base_url"], "/v4/sports",
                                      params={"apiKey": self.api_key}, purpose="quota_check")
        self._note_quota(res)

    async def poll(self) -> None:
        if self.stopped:
            return
        s = self.section
        res = await self.rest.request(
            "GET", s["base_url"], f"/v4/sports/{s['sport']}/odds",
            params={"apiKey": self.api_key, "regions": s["regions"], "markets": s["markets"],
                    "oddsFormat": s["odds_format"], "dateFormat": "iso"},
            subjects=[f"odds_api:sport:{s['sport']}"], purpose="odds",
        )
        self._note_quota(res)
