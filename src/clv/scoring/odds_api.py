"""The Odds API: parse archived `h2h` responses into per-book quotes.

`GET /v4/sports/{sport}/odds` returns a list of events, each with bookmakers,
each with markets and outcomes priced in integer American odds, with
`last_update` at one-second resolution per book and per market. The event's
`commence_time` is the vendor's start time, not an off observation (DESIGN.md
§5.2: a scheduled time alone is `no_trusted_off`). Credit usage is read from
the response headers archived in the completion event.
"""
from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction

from clv.archive import RestExchange
from clv.timeutil import iso_ms


def american_to_decimal(american: int) -> Fraction:
    """Decimal odds (payout per unit staked, stake included), exactly:
    1 + A/100 for A > 0, 1 + 100/|A| for A < 0 (docs/measurement-contract.md)."""
    if not isinstance(american, int) or isinstance(american, bool) or -100 < american < 100:
        raise ValueError(f"American odds {american!r} must be an integer with |A| >= 100")
    return 1 + (Fraction(american, 100) if american > 0 else Fraction(100, -american))


@dataclass(frozen=True)
class Quote:
    vendor_event_id: str
    commence_time: str
    home_team: str
    away_team: str
    bookmaker: str
    market: str
    outcome_name: str
    price_american: int
    book_last_update_ms: int
    market_last_update_ms: int
    recv_ts_ms: int             # when the response arrived: the earliest anyone here could have used it
    refs: tuple[str, ...]


def quotes(x: RestExchange) -> list[Quote]:
    out = []
    for ev in x.body.json():
        for bk in ev.get("bookmakers", []):
            for mk in bk.get("markets", []):
                for o in mk["outcomes"]:
                    out.append(Quote(ev["id"], ev["commence_time"], ev["home_team"], ev["away_team"],
                                     bk["key"], mk["key"], o["name"], o["price"], iso_ms(bk["last_update"]),
                                     iso_ms(mk["last_update"]), x.body.recv_ts_ms, x.refs))
    return out


def credits(x: RestExchange) -> dict[str, int | None]:
    def num(name: str) -> int | None:
        v = x.header(name)
        return int(float(v)) if v not in (None, "") else None
    return {"remaining": num("x-requests-remaining"), "used": num("x-requests-used"),
            "last": num("x-requests-last")}
