"""Which games, markets and pollers are active at a given time.

A game's capture window runs from `capture_lead_s` before its scheduled start
to `capture_tail_s` after it (per-game overrides allowed). The Odds API polls
only from `odds_window_lead_s` before each scheduled start until that start:
entry quotes are pregame, and credits are scarce.

A league event (A1) has a window too, by default `a1.capture_lead_s` before its
scheduled start, so the 24 h offset is captured, to `a1.capture_tail_s` after.
It has no odds window and no StatsAPI feeds: those are MLB's.

Every entry's markets are in a dense probe window from `stream.dense_probe_lead_s`
before its scheduled start to `stream.dense_probe_tail_s` after it: every close cutoff
falls there, so quiet channels are probed every `stream.channel_probe_interval_s`, and
every `stream.quiet_probe_interval_s` elsewhere (docs/decisions/2026-10-17-a1-probe-policy.md).
"""
from __future__ import annotations

from dataclasses import dataclass

from .config import Game, LeagueEvent, Params


@dataclass(frozen=True)
class Window:
    game: Game | LeagueEvent
    start_ms: int
    end_ms: int

    def contains(self, t_ms: int) -> bool:
        return self.start_ms <= t_ms < self.end_ms


def default_lead_tail(g: Game | LeagueEvent, params: Params) -> tuple[int, int]:
    """The window an entry gets when it overrides neither end."""
    if isinstance(g, LeagueEvent):
        return params.a1_capture_lead_s, params.a1_capture_tail_s
    return params.capture_lead_s, params.capture_tail_s


def bounds(g: Game | LeagueEvent, params: Params) -> tuple[int, int]:
    """[start, end) of an entry's capture window, in UTC ms."""
    lead, tail = default_lead_tail(g, params)
    lead = g.capture_lead_s if g.capture_lead_s is not None else lead
    tail = g.capture_tail_s if g.capture_tail_s is not None else tail
    return g.scheduled_start_ms - lead * 1000, g.scheduled_start_ms + tail * 1000


def capture_windows(games: list[Game | LeagueEvent], params: Params) -> list[Window]:
    return sorted((Window(g, *bounds(g, params)) for g in games), key=lambda w: w.start_ms)


def odds_windows(games: list[Game], params: Params) -> list[Window]:
    return sorted((Window(g, g.scheduled_start_ms - params.odds_window_lead_s * 1000,
                          g.scheduled_start_ms) for g in games), key=lambda w: w.start_ms)


class Schedule:
    def __init__(self, games: list[Game], params: Params, events: list[LeagueEvent] = ()):
        self.capture = capture_windows([*games, *events], params)
        self.odds = odds_windows(games, params)
        self.dense_lead_ms, self.dense_tail_ms = params.dense_probe_lead_s * 1000, params.dense_probe_tail_s * 1000

    def active(self, t_ms: int) -> list[Window]:
        return [w for w in self.capture if w.contains(t_ms)]

    def any_active(self, t_ms: int) -> bool:
        return any(w.contains(t_ms) for w in self.capture)

    def novig_markets(self, t_ms: int) -> frozenset[str]:
        return frozenset(m for w in self.active(t_ms) for m in w.game.novig_markets)

    def dense_markets(self, t_ms: int) -> frozenset[str]:
        """The recorded Novig markets inside their dense probe window."""
        return frozenset(m for w in self.active(t_ms)
                         if w.game.scheduled_start_ms - self.dense_lead_ms <= t_ms < w.game.scheduled_start_ms + self.dense_tail_ms
                         for m in w.game.novig_markets)

    def kalshi_tickers(self, t_ms: int) -> tuple[str, ...]:
        return tuple(dict.fromkeys(t for w in self.active(t_ms) for t in w.game.kalshi_tickers))

    def odds_active(self, t_ms: int) -> bool:
        return any(w.contains(t_ms) for w in self.odds)

    def ended_between(self, t0_ms: int, t1_ms: int) -> list[Window]:
        """MLB games' capture windows whose end falls in (t0, t1]: their StatsAPI feeds are due."""
        return [w for w in self.capture if isinstance(w.game, Game) and t0_ms < w.end_ms <= t1_ms]
