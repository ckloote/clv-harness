"""The recorder's hand-maintained game list (tools/raw_recorder/games.toml).

Each entry was checked by hand when it was added: its gamePk, Novig MONEY
markets and Kalshi tickers belong to one game. The harness reads it as the
provenance for those instrument-to-game links (measurement contract §4).
"""
from __future__ import annotations

import tomllib
from pathlib import Path

from clv.config import param
from clv.timeutil import iso_ms

REPO = Path(__file__).resolve().parents[2]
GAMES = REPO / "tools" / "raw_recorder" / "games.toml"


def load_game(game_pk: int, path: Path = GAMES) -> dict:
    with open(path, "rb") as f:
        games = tomllib.load(f)["game"]
    for g in games:
        if g["game_pk"] == game_pk:
            return g
    raise SystemExit(f"game_pk {game_pk} is not in {path}")


def capture_window(g: dict) -> tuple[int, int]:
    """[start, end] of the game's capture window, in UTC ms."""
    start = iso_ms(g["scheduled_start_utc"])
    lead = g.get("capture_lead_s", param("r0.capture_lead_s"))
    tail = g.get("capture_tail_s", param("r0.capture_tail_s"))
    return start - lead * 1000, start + tail * 1000
