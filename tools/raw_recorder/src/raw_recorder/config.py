"""Recorder configuration: parameters (config.toml) and the games to record.

Every numeric parameter is copied from DESIGN.md §10; config.toml names the
§10 key beside each value. Secrets never live in these files: they come from
environment variables (a systemd EnvironmentFile in deployment).

Games come from two lists. The recorder discovers games itself and writes them
to `<archive>/games.discovered.toml` (raw_recorder.discovery). `games.toml`,
committed and checked by hand, adds games or overrides a discovered one: for a
gamePk in both, the hand-checked entry wins whole.

Leagues other than MLB (`[[league]]`, A1) are found from Novig's catalog alone
and written to `<archive>/events.discovered.toml` (raw_recorder.leagues), one
`LeagueEvent` per Novig event. Their windows use the `[a1]` lead and tail.

`[faults]` (Gate A1, raw_recorder.faults) is off except for the gate run and its
rehearsals: with it on, the recorder serves a control socket for the fault harness.
"""
from __future__ import annotations

import hashlib
import os
import tomllib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

DEFAULT_DIR = Path(__file__).resolve().parents[2]   # tools/raw_recorder/
DEFAULT_CONFIG = DEFAULT_DIR / "config.toml"
DEFAULT_GAMES = DEFAULT_DIR / "games.toml"
DISCOVERED_FILE = "games.discovered.toml"           # in the archive root; clv.games reads it too
EVENTS_DISCOVERED_FILE = "events.discovered.toml"   # in the archive root: league events (A1)
CONTROL_SOCKET = ".recorder.control"                # in the archive root: the fault harness's socket


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
    quiet_probe_interval_s: float
    dense_probe_lead_s: int
    dense_probe_tail_s: int
    transport_liveness_max_s: float
    probe_timeout_s: float
    subscription_check_interval_s: float
    reconnect_backoff_initial_s: float
    reconnect_backoff_max_s: float
    a1_capture_lead_s: int
    a1_capture_tail_s: int


@dataclass(frozen=True)
class Game:
    game_pk: int
    scheduled_start_ms: int
    label: str
    novig_markets: tuple[str, ...]
    kalshi_tickers: tuple[str, ...]
    capture_lead_s: int | None = None
    capture_tail_s: int | None = None
    source: str = "manual"                  # "manual" (games.toml, checked by hand) | "discovered"
    notes: tuple[str, ...] = ()             # why a discovered entry looks the way it does


@dataclass(frozen=True)
class LeagueEvent:
    """A game in a league other than MLB, keyed by its Novig event (raw_recorder.leagues).

    The recorder never decides which league game it is: A2 links it to the league's own
    game ID. Its window defaults to the `[a1]` lead and tail, not R0's. It has no Kalshi
    tickers until Kalshi joins as the cross-check, after Gate A1.
    """
    league: str
    event_id: str
    scheduled_start_ms: int                 # Novig's startsTs: what the venue said, never an off
    label: str
    novig_markets: tuple[str, ...]
    capture_lead_s: int | None = None
    capture_tail_s: int | None = None
    source: str = "discovered"
    notes: tuple[str, ...] = ()
    kalshi_tickers: tuple[str, ...] = ()


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
    games_path: Path = DEFAULT_GAMES
    discovery: dict = field(default_factory=lambda: {"enabled": False})
    backfill: dict = field(default_factory=dict)
    leagues: tuple[dict, ...] = ()          # [[league]] entries, enabled or not
    faults: dict = field(default_factory=lambda: {"enabled": False})

    @property
    def backfill_root(self) -> Path:
        root = Path(self.backfill["root"])
        return root if root.is_absolute() else (self.path.parent / root).resolve()

    @property
    def discovered_path(self) -> Path:
        return self.archive_root / DISCOVERED_FILE

    @property
    def events_discovered_path(self) -> Path:
        return self.archive_root / EVENTS_DISCOVERED_FILE

    @property
    def control_socket_path(self) -> Path:
        path = Path(self.faults.get("control_socket", CONTROL_SOCKET))
        return path if path.is_absolute() else self.archive_root / path

    @property
    def relay_url(self) -> str:
        return f"http://{self.faults.get('relay_listen', '127.0.0.1:8765')}"

    def enabled_leagues(self) -> list[dict]:
        return [lg for lg in self.leagues if self.enabled(lg)]

    def enabled(self, section: dict) -> bool:
        return bool(section.get("enabled", True))

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
    r0, stream, a1 = doc["r0"], doc["stream"], doc["a1"]
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
        quiet_probe_interval_s=stream["quiet_probe_interval_s"],
        dense_probe_lead_s=stream["dense_probe_lead_s"],
        dense_probe_tail_s=stream["dense_probe_tail_s"],
        transport_liveness_max_s=stream["transport_liveness_max_s"],
        probe_timeout_s=stream["probe_timeout_s"],
        subscription_check_interval_s=stream["subscription_check_interval_s"],
        reconnect_backoff_initial_s=stream["reconnect_backoff_s"]["initial"],
        reconnect_backoff_max_s=stream["reconnect_backoff_s"]["max"],
        a1_capture_lead_s=a1["capture_lead_s"],
        a1_capture_tail_s=a1["capture_tail_s"],
    )
    for name, value in params.__dict__.items():
        if value < 0 or (value == 0 and name != "odds_quota_floor"):
            raise ValueError(f"{name} must be positive")
    if not 0 <= params.stream_probe_reserve_fraction < 1:
        raise ValueError("stream_probe_reserve_fraction must lie in [0, 1)")
    root = Path(doc["archive"]["root"])
    if not root.is_absolute():
        root = (path.parent / root).resolve()
    games = Path(doc.get("games_file", DEFAULT_GAMES.name))
    if not games.is_absolute():
        games = (path.parent / games).resolve()
    return Config(path=path, sha256=hashlib.sha256(raw).hexdigest(), archive_root=root,
                  params=params, novig=doc["novig"], kalshi=doc["kalshi"],
                  odds_api=doc["odds_api"], mlb=doc["mlb"], games_path=games,
                  discovery=doc.get("discovery", {"enabled": False}), backfill=doc.get("backfill", {}),
                  leagues=tuple(_league(lg) for lg in doc.get("league", [])),
                  faults=doc.get("faults", {"enabled": False}))


def _league(lg: dict) -> dict:
    if not lg.get("name"):
        raise ValueError("a [[league]] entry needs a name (Novig's league code)")
    if lg["name"] == "MLB":
        raise ValueError("MLB is discovered from the StatsAPI schedule ([discovery]), not as a [[league]]")
    return lg


def load_games(path: Path | None = None, source: str = "manual") -> list[Game]:
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
            source=source,
            notes=tuple(g.get("notes", ())),
        ))
    pks = [g.game_pk for g in games]
    if len(pks) != len(set(pks)):
        raise ValueError(f"duplicate game_pk in {path.name}")
    return games


def load_events(path: Path) -> list[LeagueEvent]:
    """The league events in `<archive>/events.discovered.toml` (raw_recorder.leagues.render)."""
    if not path.exists():
        return []
    doc = tomllib.loads(path.read_text())
    events = [LeagueEvent(
        league=e["league"], event_id=e["event_id"], scheduled_start_ms=_parse_utc(e["scheduled_start_utc"]),
        label=e.get("label", ""), novig_markets=tuple(e.get("novig_markets", ())),
        capture_lead_s=e.get("capture_lead_s"), capture_tail_s=e.get("capture_tail_s"),
        notes=tuple(e.get("notes", ()))) for e in doc.get("event", [])]
    ids = [e.event_id for e in events]
    if len(ids) != len(set(ids)):
        raise ValueError(f"duplicate event_id in {path.name}")
    return events


def combine(manual: list[Game], discovered: list[Game]) -> list[Game]:
    """Every game once: a hand-checked entry replaces a discovered one with its gamePk."""
    by_pk = {g.game_pk: g for g in discovered}
    by_pk.update((g.game_pk, g) for g in manual)
    return sorted(by_pk.values(), key=lambda g: (g.scheduled_start_ms, g.game_pk))


def shadowed(manual: list[Game], discovered: list[Game]) -> list[str]:
    """Hand-checked entries that hide markets discovery found for the same game."""
    found = {g.game_pk: g for g in discovered}
    out = []
    for g in manual:
        d = found.get(g.game_pk)
        if d is None:
            continue
        missing = sorted((set(d.novig_markets) - set(g.novig_markets)) | (set(d.kalshi_tickers) - set(g.kalshi_tickers)))
        if missing:
            out.append(f"games.toml entry {g.game_pk} overrides discovery, which also found {', '.join(missing)}: "
                       f"add them there or delete the entry")
    return out


def discovered_games(cfg: Config) -> list[Game]:
    """The discovered list, when this config discovers games. With `[discovery]` off only
    games.toml is recorded, whatever a discovered list left in the archive root says."""
    return load_games(cfg.discovered_path, "discovered") if cfg.discovery.get("enabled", False) else []


def load_all_games(cfg: Config, games_path: Path | None = None) -> list[Game]:
    """The games the recorder records: games.toml (or `games_path`) over the discovered list."""
    return combine(load_games(games_path or cfg.games_path), discovered_games(cfg))
