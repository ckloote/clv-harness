"""B0 fetch: download and cache every input of docs/feasibility.md.

    uv run python tools/b0/fetch.py [--work b0-work]

- Novig daily trades/markets files for the sample days, plus index.json
- Novig /v3/history markets and events for every MLB MONEY market in those files (read key)
- MLB StatsAPI schedule, teams and play-by-play for the sample games
- Kalshi KXMLBGAME events listing

Writes <work>/sample_games.json. Existing files are reused; delete one to refetch it.
"""
from __future__ import annotations

import csv
import json
import time

from common import (EDGE_GAMES, KALSHI, NOVIG_DATA, NOVIG_DAYS, SAMPLE_DATE, SCHEDULE_RANGE,
                    STATSAPI, NovigHistory, get_json, http_get, work_dir)


def novig_files(work) -> set[str]:
    http_get(f"{NOVIG_DATA}/index.json", work / "novig" / "index.json")
    ids: set[str] = set()
    for day in NOVIG_DAYS:
        for name in ("trades", "markets"):
            http_get(f"{NOVIG_DATA}/{day}/{name}.csv", work / "novig" / day / f"{name}.csv")
        with open(work / "novig" / day / "trades.csv", newline="") as f:
            ids |= {r["marketId"] for r in csv.DictReader(f) if r["league"] == "MLB" and r["marketType"] == "MONEY"}
        with open(work / "novig" / day / "markets.csv", newline="") as f:
            # The moneyline reportTicker was MLB-MONEY in August and MLB-WINNER from September.
            ids |= {r["marketId"] for r in csv.DictReader(f) if r["reportTicker"] in ("MLB-MONEY", "MLB-WINNER")}
    return ids


def novig_history(work, ids: set[str]) -> None:
    hist = work / "novig" / "hist"
    hist.mkdir(parents=True, exist_ok=True)
    client = NovigHistory()
    try:
        events = set()
        for mid in sorted(ids):
            f = hist / f"market_{mid}.json"
            if not f.exists():
                status, body = client.get(f"/v3/history/markets/{mid}")
                if status != 200:
                    print("market", mid, status, body[:200])
                    continue
                f.write_bytes(body)
                time.sleep(0.3)
            events.add(json.loads(f.read_bytes())["eventId"])
        for eid in sorted(events):
            f = hist / f"event_{eid}.json"
            if not f.exists():
                status, body = client.get(f"/v3/history/events/{eid}")
                if status != 200:
                    print("event", eid, status, body[:200])
                    continue
                f.write_bytes(body)
                time.sleep(0.3)
        print(f"novig history: {len(ids)} markets, {len(events)} events")
    finally:
        client.close()


def mlb(work) -> None:
    lo, hi = SCHEDULE_RANGE
    sched = get_json(f"{STATSAPI}/schedule?sportId=1&startDate={lo}&endDate={hi}&gameType=R,F,D,L,W",
                     work / "mlb" / "schedule.json")
    get_json(f"{STATSAPI}/teams?sportId=1&season=2026", work / "mlb" / "teams.json")
    games: dict[int, dict] = {}
    for day in sched["dates"]:
        for g in day["games"]:
            if g["officialDate"] == SAMPLE_DATE or g["gamePk"] in EDGE_GAMES:
                # A postponed game appears twice under one gamePk; keep the row it was played under.
                if g["gamePk"] not in games or g["status"]["detailedState"] == "Final":
                    games[g["gamePk"]] = g
    out = {}
    for pk, g in games.items():
        pbp = get_json(f"{STATSAPI}/game/{pk}/playByPlay", work / "mlb" / "pbp" / f"{pk}.json")
        first = next((e["startTime"] for p in pbp.get("allPlays", []) for e in p.get("playEvents", [])
                      if e.get("isPitch")), None)
        away, home = g["teams"]["away"], g["teams"]["home"]
        out[pk] = {"first_pitch": first, "gameDate": g["gameDate"], "officialDate": g["officialDate"],
                   "away_name": away["team"]["name"], "home": home["team"]["name"],
                   "status": g["status"]["detailedState"], "dh": g.get("doubleHeader"), "gn": g.get("gameNumber"),
                   "winner": "home" if home.get("isWinner") else "away" if away.get("isWinner") else None}
    (work / "sample_games.json").write_text(json.dumps(out, indent=1))
    print(f"statsapi: {len(out)} sample games")


def kalshi_events(work) -> None:
    dest = work / "kalshi" / "events.json"
    if dest.exists():
        return
    events, cursor = [], ""
    while True:
        d = get_json(f"{KALSHI}/events?series_ticker=KXMLBGAME&limit=200" + (f"&cursor={cursor}" if cursor else ""))
        events += d.get("events", [])
        cursor = d.get("cursor")
        if not cursor:
            break
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(events))
    print(f"kalshi: {len(events)} KXMLBGAME events")


def main() -> None:
    work, _ = work_dir(description=__doc__)
    novig_history(work, novig_files(work))
    mlb(work)
    kalshi_events(work)


if __name__ == "__main__":
    main()
