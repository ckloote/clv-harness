"""Which games, markets and pollers are active at a given time.

A game's capture window runs from `capture_lead_s` before its scheduled start
to `capture_tail_s` after it (per-game overrides allowed). The Odds API polls
only from `odds_window_lead_s` before each scheduled start until that start:
entry quotes are pregame, and credits are scarce.
"""
from __future__ import annotations

from dataclasses import dataclass

from .config import Game, Params


@dataclass(frozen=True)
class Window:
    game: Game
    start_ms: int
    end_ms: int

    def contains(self, t_ms: int) -> bool:
        return self.start_ms <= t_ms < self.end_ms


def capture_windows(games: list[Game], params: Params) -> list[Window]:
    out = []
    for g in games:
        lead = g.capture_lead_s if g.capture_lead_s is not None else params.capture_lead_s
        tail = g.capture_tail_s if g.capture_tail_s is not None else params.capture_tail_s
        out.append(Window(g, g.scheduled_start_ms - lead * 1000, g.scheduled_start_ms + tail * 1000))
    return sorted(out, key=lambda w: w.start_ms)


def odds_windows(games: list[Game], params: Params) -> list[Window]:
    return sorted((Window(g, g.scheduled_start_ms - params.odds_window_lead_s * 1000,
                          g.scheduled_start_ms) for g in games), key=lambda w: w.start_ms)


class Schedule:
    def __init__(self, games: list[Game], params: Params):
        self.capture = capture_windows(games, params)
        self.odds = odds_windows(games, params)

    def active(self, t_ms: int) -> list[Window]:
        return [w for w in self.capture if w.contains(t_ms)]

    def any_active(self, t_ms: int) -> bool:
        return any(w.contains(t_ms) for w in self.capture)

    def novig_markets(self, t_ms: int) -> frozenset[str]:
        return frozenset(m for w in self.active(t_ms) for m in w.game.novig_markets)

    def kalshi_tickers(self, t_ms: int) -> tuple[str, ...]:
        return tuple(dict.fromkeys(t for w in self.active(t_ms) for t in w.game.kalshi_tickers))

    def odds_active(self, t_ms: int) -> bool:
        return any(w.contains(t_ms) for w in self.odds)

    def ended_between(self, t0_ms: int, t1_ms: int) -> list[Window]:
        """Capture windows whose end falls in (t0, t1]."""
        return [w for w in self.capture if t0_ms < w.end_ms <= t1_ms]
