"""The recorder's game lists: which venue instruments it recorded for which game.

Two lists, as the recorder reads them (raw_recorder.config):
- `tools/raw_recorder/games.toml`, committed: each entry was checked by hand when
  it was added, so its links are `manual_verified` (measurement contract §4).
- `<archive>/games.discovered.toml`, written by the recorder's discovery from the
  archived schedule and catalogs. Its links are rule matches, never checked, so
  they are `fuzzy_candidate` until a correction verifies them (`clv correct`).
A game in both is the hand-checked entry. `source` says which one an entry is.

League events (A1) are in `<archive>/events.discovered.toml`, written by the
recorder's league discovery (raw_recorder.leagues): one Novig event each, with
the `a1.capture_lead_s` and `a1.capture_tail_s` window unless it overrides one.
"""
from __future__ import annotations

import tomllib
from pathlib import Path

from clv.config import param
from clv.timeutil import iso_ms

REPO = Path(__file__).resolve().parents[2]
GAMES = REPO / "tools" / "raw_recorder" / "games.toml"
DISCOVERED = "games.discovered.toml"        # in the archive root (raw_recorder.config.DISCOVERED_FILE)
EVENTS_DISCOVERED = "events.discovered.toml"    # in the archive root (raw_recorder.config.EVENTS_DISCOVERED_FILE)


def load_game(game_pk: int, path: Path = GAMES, archive_root: Path | None = None) -> dict:
    """The game's entry, with `source` "manual" or "discovered"."""
    lists = [(path, "manual")] + ([(Path(archive_root) / DISCOVERED, "discovered")] if archive_root else [])
    for p, source in lists:
        if not p.exists():
            continue
        with open(p, "rb") as f:
            for g in tomllib.load(f).get("game", []):
                if g["game_pk"] == game_pk:
                    return dict(g, source=source)
    raise SystemExit(f"game_pk {game_pk} is in neither {' nor '.join(str(p) for p, _ in lists)}")


def capture_window(g: dict) -> tuple[int, int]:
    """[start, end] of the game's capture window, in UTC ms."""
    start = iso_ms(g["scheduled_start_utc"])
    lead = g.get("capture_lead_s", param("r0.capture_lead_s"))
    tail = g.get("capture_tail_s", param("r0.capture_tail_s"))
    return start - lead * 1000, start + tail * 1000


def league_events(archive_root: Path) -> list[dict]:
    """Every league event the recorder discovered, as written (raw_recorder.leagues.render)."""
    path = Path(archive_root) / EVENTS_DISCOVERED
    if not path.exists():
        return []
    with open(path, "rb") as f:
        return list(tomllib.load(f).get("event", []))


def load_league_event(event_id: str, archive_root: Path) -> dict:
    for e in league_events(archive_root):
        if e["event_id"] == event_id:
            return e
    raise SystemExit(f"league event {event_id} is not in {Path(archive_root) / EVENTS_DISCOVERED}")


def league_window(e: dict) -> tuple[int, int]:
    """[start, end] of a league event's capture window, in UTC ms (raw_recorder.scheduler.bounds)."""
    start = iso_ms(e["scheduled_start_utc"])
    lead = e.get("capture_lead_s", param("a1.capture_lead_s"))
    tail = e.get("capture_tail_s", param("a1.capture_tail_s"))
    return start - lead * 1000, start + tail * 1000
