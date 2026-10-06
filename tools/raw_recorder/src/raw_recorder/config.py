"""Recorder configuration: parameters (config.toml) and the game list (games.toml).

Every numeric parameter is copied from DESIGN.md §10; config.toml names the
§10 key beside each value. Secrets never live in these files: they come from
environment variables (a systemd EnvironmentFile in deployment).
"""
from __future__ import annotations

import hashlib
import os
import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

DEFAULT_DIR = Path(__file__).resolve().parents[2]   # tools/raw_recorder/
DEFAULT_CONFIG = DEFAULT_DIR / "config.toml"
DEFAULT_GAMES = DEFAULT_DIR / "games.toml"


@dataclass(frozen=True)
class Params:
    kalshi_poll_interval_s: float
    odds_poll_interval_s: float
    odds_quota_floor: int
    capture_lead_s: int
    capture_tail_s: int
    odds_window_lead_s: int
    segment_max_s: int
    fsync_interval_s: float
    rest_timeout_s: float
    min_free_disk_mb: int
    stream_probe_reserve_fraction: float
    novig_public_book_poll_interval_s: float
    channel_probe_interval_s: float
    transport_liveness_max_s: float
    reconnect_backoff_initial_s: float
    reconnect_backoff_max_s: float


@dataclass(frozen=True)
class Game:
    game_pk: int
    scheduled_start_ms: int
    label: str
    novig_markets: tuple[str, ...]
    kalshi_tickers: tuple[str, ...]
    capture_lead_s: int | None = None
    capture_tail_s: int | None = None


@dataclass(frozen=True)
class Config:
    path: Path
    sha256: str
    archive_root: Path
    params: Params
    novig: dict
    kalshi: dict
    odds_api: dict
    mlb: dict

    def env(self, section: dict, key: str) -> str | None:
        name = section.get(key)
        value = os.environ.get(name) if name else None
        return value or None


def _parse_utc(text: str) -> int:
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError(f"scheduled_start_utc must carry a UTC offset: {text}")
    return int(dt.timestamp() * 1000)


def load_config(path: Path | None = None) -> Config:
    path = Path(path or os.environ.get("RAW_RECORDER_CONFIG") or DEFAULT_CONFIG).resolve()
    raw = path.read_bytes()
    doc = tomllib.loads(raw.decode("utf-8"))
    r0, stream = doc["r0"], doc["stream"]
    params = Params(
        kalshi_poll_interval_s=r0["kalshi_poll_interval_s"],
        odds_poll_interval_s=r0["odds_poll_interval_s"],
        odds_quota_floor=r0["odds_quota_floor"],
        capture_lead_s=r0["capture_lead_s"],
        capture_tail_s=r0["capture_tail_s"],
        odds_window_lead_s=r0["odds_window_lead_s"],
        segment_max_s=r0["segment_max_s"],
        fsync_interval_s=r0["fsync_interval_s"],
        rest_timeout_s=r0["rest_timeout_s"],
        min_free_disk_mb=r0["min_free_disk_mb"],
        stream_probe_reserve_fraction=r0["stream_probe_reserve_fraction"],
        novig_public_book_poll_interval_s=r0["novig_public_book_poll_interval_s"],
        channel_probe_interval_s=stream["channel_probe_interval_s"],
        transport_liveness_max_s=stream["transport_liveness_max_s"],
        reconnect_backoff_initial_s=stream["reconnect_backoff_s"]["initial"],
        reconnect_backoff_max_s=stream["reconnect_backoff_s"]["max"],
    )
    for name, value in params.__dict__.items():
        if value < 0 or (value == 0 and name != "odds_quota_floor"):
            raise ValueError(f"{name} must be positive")
    if not 0 <= params.stream_probe_reserve_fraction < 1:
        raise ValueError("stream_probe_reserve_fraction must lie in [0, 1)")
    root = Path(doc["archive"]["root"])
    if not root.is_absolute():
        root = (path.parent / root).resolve()
    return Config(path=path, sha256=hashlib.sha256(raw).hexdigest(), archive_root=root,
                  params=params, novig=doc["novig"], kalshi=doc["kalshi"],
                  odds_api=doc["odds_api"], mlb=doc["mlb"])


def load_games(path: Path | None = None) -> list[Game]:
    path = Path(path or os.environ.get("RAW_RECORDER_GAMES") or DEFAULT_GAMES)
    if not path.exists():
        return []
    doc = tomllib.loads(path.read_text())
    games = []
    for g in doc.get("game", []):
        games.append(Game(
            game_pk=int(g["game_pk"]),
            scheduled_start_ms=_parse_utc(g["scheduled_start_utc"]),
            label=g.get("label", ""),
            novig_markets=tuple(g.get("novig_markets", ())),
            kalshi_tickers=tuple(g.get("kalshi_tickers", ())),
            capture_lead_s=g.get("capture_lead_s"),
            capture_tail_s=g.get("capture_tail_s"),
        ))
    pks = [g.game_pk for g in games]
    if len(pks) != len(set(pks)):
        raise ValueError("duplicate game_pk in games.toml")
    return games
