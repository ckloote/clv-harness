"""Which game each historical market settled on: the rule proposes, a person approves
(docs/decisions/2026-10-14-b1-reference-import.md).

**Kalshi** (`kalshi settlement rule v1`). Ticker dates do not identify the game
(docs/feasibility.md §4), but settlement and close time do.
- **Candidates:** StatsAPI games between the ticker's two teams within CANDIDATE_DAYS of
  the ticker date.
- **The game:** the one Final candidate the market can have settled on. It started before
  the market's `close_time`, at most MAX_GAME_MS before it.
- **Its winner:** the team whose market resolved `yes`, with the other market `no`.

**Novig** (`novig event rule v1`).
- **Teams:** the history event's "Away @ Home" names the game's two teams, in Novig's
  names.
- **Start:** its `startsTs` is within NOVIG_START_MS of the StatsAPI start, and it is the
  only such game.
- **Outcomes:** the outcomes are the teams' Novig codes, and the winner's is graded `WIN`.

**Edge cases** are never proposed. They are listed for review with their candidates:
- doubleheaders, postponed or rescheduled games, and a game on another day than the ticker;
- several candidates or none;
- a disagreeing result or grade;
- a `scalar` (cancelled) settlement;
- a team code that maps to no team.

A clean match, and an edge case with exactly one candidate, is written as an unverified
mapping (`fuzzy_candidate`). Only the clean ones go into the proposed corrections, which
make them `manual_verified` once approved (`clv correct`).
"""
from __future__ import annotations

import json
import random
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from clv.backfill import kalshi_hist, novig_public
from clv.backfill.mlb_pbp import ScheduledGame
from clv.identity import ensure_mapping, novig_abbr
from clv.timeutil import iso_ms, ms_iso

KALSHI_RULE = "kalshi settlement rule v1"
NOVIG_RULE = "novig event rule v1"
CANDIDATE_DAYS = 3
MAX_GAME_MS = 8 * 3600 * 1000           # first pitch to final out, delays included
NOVIG_START_MS = 6 * 3600 * 1000        # the recorder's discovery window (raw_recorder.discovery)
KALSHI_ABBR = {"ARI": "AZ"}             # Kalshi code -> StatsAPI abbreviation, where they differ (2025 tickers)
NOVIG_NAME = {"Athletics": "Oakland Athletics"}     # StatsAPI name -> Novig's (tools/b0/common.py)
SAMPLE_SEED = 20261014
TICKER_DATE = re.compile(r"^KXMLBGAME-(\d{2})([A-Z]{3})(\d{2})")
MONTHS = {m: i + 1 for i, m in enumerate("JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split())}


@dataclass
class Link:
    venue: str
    native_event: str
    game: ScheduledGame | None
    status: str                     # "clean" | "edge" | "excluded"
    reasons: list[str] = field(default_factory=list)
    candidates: list[ScheduledGame] = field(default_factory=list)
    evidence: str = ""
    sides: list[tuple] = field(default_factory=list)    # (native_id, side, team abbreviation, polarity, exchange, rules)


def ticker_date(event_ticker: str) -> date | None:
    m = TICKER_DATE.match(event_ticker)
    return date(2000 + int(m[1]), MONTHS[m[2]], int(m[3])) if m else None


def _describe(g: ScheduledGame) -> str:
    return (f"gamePk {g.game_pk} {g.away.abbreviation} @ {g.home.abbreviation} {g.official_date}, start "
            f"{ms_iso(g.start_ms)}, {g.status}, winner {g.winner or '-'}"
            f"{', doubleheader ' + g.double_header + str(g.game_number) if g.double_header != 'N' else ''}"
            f"{', moved' if g.moved else ''}")


def kalshi_links(listing: dict[str, tuple], games: dict[int, ScheduledGame]) -> list[Link]:
    by_teams: dict[frozenset, list[ScheduledGame]] = defaultdict(list)
    for g in games.values():
        by_teams[frozenset((g.away.abbreviation, g.home.abbreviation))].append(g)
    events: dict[str, list] = defaultdict(list)
    for x, m in listing.values():
        events[m.event_ticker].append((x, m))
    out = []
    for ev, ms in sorted(events.items()):
        link = Link("kalshi", ev, None, "edge")
        out.append(link)
        if len(ms) != 2 or any(m.status not in ("finalized", "settled") for _, m in ms):
            link.status, link.reasons = "excluded", [f"{len(ms)} markets, not two settled ones"]
            continue
        codes = {m.ticker: m.ticker.rsplit("-", 1)[1] for _, m in ms}
        teams = {t: KALSHI_ABBR.get(c, c) for t, c in codes.items()}
        results = {m.ticker: m.result for _, m in ms}
        if set(results.values()) != {"yes", "no"}:
            link.status = "excluded" if set(results.values()) <= {"scalar"} else "edge"
            link.reasons = [f"results {sorted(set(results.values()))}" + (" (cancelled: a fair-price settlement)"
                                                                       if "scalar" in results.values() else "")]
        day = ticker_date(ev)
        close = max(iso_ms(m.close_time) for _, m in ms)
        pool = by_teams.get(frozenset(teams.values()), [])
        if not pool:
            link.status = "excluded" if link.status == "excluded" else "edge"
            link.reasons.append(f"team codes {sorted(codes.values())} name no StatsAPI pair")
            continue
        link.candidates = sorted((g for g in pool if day and abs((date.fromisoformat(g.official_date) - day).days)
                                  <= CANDIDATE_DAYS), key=lambda g: g.start_ms)
        fits = [g for g in link.candidates if g.final and g.start_ms < close <= g.start_ms + MAX_GAME_MS]
        winner = next((teams[t] for t, r in results.items() if r == "yes"), None)
        if len(fits) != 1:
            link.reasons.append(f"{len(fits)} Final games started within {MAX_GAME_MS // 3_600_000} h before "
                                f"close_time {ms_iso(close)}")
            continue
        g = link.game = fits[0]
        if g.winner != winner:
            link.reasons.append(f"Kalshi's winner {winner}, StatsAPI's {g.winner}")
        if g.double_header != "N" or any(c.double_header != "N" for c in link.candidates
                                         if abs(c.start_ms - g.start_ms) < 2 * 86_400_000):
            link.reasons.append("a doubleheader")
        if g.moved:
            link.reasons.append("postponed or rescheduled")
        if day and g.official_date != day.isoformat():
            link.reasons.append(f"played {g.official_date}, ticker dated {day.isoformat()}")
        link.evidence = (f"{KALSHI_RULE}: {ev} close_time {ms_iso(close)}; results "
                         f"{', '.join(f'{t} {r}' for t, r in sorted(results.items()))}. The only Final game between "
                         f"{' and '.join(sorted(teams.values()))} within {CANDIDATE_DAYS} days of the ticker date "
                         f"that started within {MAX_GAME_MS // 3_600_000} h before close_time is {_describe(g)}.")
        if not link.reasons:
            link.status = "clean"
        if winner is not None and g.winner == winner:
            for x, m in ms:
                rules = "\n\n".join(r for r in (m.rules_primary, m.rules_secondary) if r) or None
                link.sides += [(m.ticker, 0, teams[m.ticker], "direct", x, rules),
                               (m.ticker, 1, teams[m.ticker], "complement", x, rules)]
    return out


def novig_links(found: dict[str, tuple], events: dict, games: dict[int, ScheduledGame]) -> list[Link]:
    by_names: dict[tuple, list[ScheduledGame]] = defaultdict(list)
    for g in games.values():
        by_names[(NOVIG_NAME.get(g.away.name, g.away.name), NOVIG_NAME.get(g.home.name, g.home.name))].append(g)
    out = []
    for market, (x, m) in sorted(found.items()):
        e = events[m.event]
        link = Link("novig", market, None, "edge")
        out.append(link)
        if e.teams is None or e.starts_ts_ms is None:
            link.reasons.append(f"event {e.event} has no 'Away @ Home' description or start")
            continue
        link.candidates = sorted((g for g in by_names.get(e.teams, [])
                                  if abs(g.start_ms - e.starts_ts_ms) <= NOVIG_START_MS), key=lambda g: g.start_ms)
        if len(link.candidates) != 1:
            link.reasons.append(f"{len(link.candidates)} games named {' @ '.join(e.teams)} start within "
                                f"{NOVIG_START_MS // 3_600_000} h of {ms_iso(e.starts_ts_ms)}")
            continue
        g = link.game = link.candidates[0]
        names = {o[1]: o for o in m.outcomes}
        codes = {novig_abbr(t.abbreviation): t.abbreviation for t in (g.away, g.home)}
        if set(names) != set(codes):
            link.reasons.append(f"outcomes {sorted(names)} are not {sorted(codes)}")
            continue
        graded = {codes[n] for n, o in names.items() if o[2] == "WIN"}
        if graded != ({g.winner} if g.winner else set()):
            link.reasons.append(f"graded WIN {sorted(graded)}, StatsAPI's winner {g.winner}")
        if g.double_header != "N":
            link.reasons.append("a doubleheader")
        if g.moved:
            link.reasons.append("postponed or rescheduled")
        link.evidence = (f"{NOVIG_RULE}: market {market}, event {e.event} '{e.description}' startsTs "
                         f"{ms_iso(e.starts_ts_ms)}; outcomes {', '.join(f'{o[1]} {o[2]}' for o in m.outcomes)}. "
                         f"The only game between these teams starting within {NOVIG_START_MS // 3_600_000} h is "
                         f"{_describe(g)}.")
        if not link.reasons:
            link.status = "clean"
        if graded == ({g.winner} if g.winner else set()):
            for side, (oid, name, _) in enumerate(m.outcomes):
                link.sides.append((market, None, codes[name], "direct", x, None, oid))
    return out


def write_links(w, links: list[Link], report: Counter) -> None:
    """The unverified mappings and aliases: clean matches, and edge cases with one candidate."""
    for link in links:
        report[f"{link.venue}_{link.status}"] += 1
        if link.game is None or not link.sides:
            continue
        event = w.conn.execute("SELECT event_id FROM event WHERE league = 'MLB' AND league_game_id = ?",
                               (str(link.game.game_pk),)).fetchone()
        if event is None:
            continue
        outcomes = dict(w.conn.execute("SELECT team_abbreviation, outcome_id FROM outcome WHERE event_id = ?",
                                       (event[0],)).fetchall())
        evidence = link.evidence + (f" Edge case: {'; '.join(link.reasons)}." if link.reasons else "")
        exchange = link.sides[0][4]
        if not w.conn.execute("SELECT 1 FROM event_alias WHERE event_id = ? AND provider = ? AND provider_event_id = ?"
                              " AND method = ?", (event[0], link.venue, link.native_event,
                                                  KALSHI_RULE if link.venue == "kalshi" else NOVIG_RULE)).fetchone():
            w.insert("event_alias", dict(
                event_id=event[0], provider=link.venue, provider_event_id=link.native_event, title=link.native_event,
                scheduled_start_ms=None, detail_json=json.dumps({"reasons": link.reasons}), status="fuzzy_candidate",
                method=KALSHI_RULE if link.venue == "kalshi" else NOVIG_RULE, evidence=evidence,
                observed_ts_ms=w.now, **w.cited(exchange.body.ref)))
        for native, side, team, polarity, x, rules, *outcome in link.sides:
            iid, s0, s1 = w.conn.execute("SELECT venue_instrument_id, side0_id, side1_id FROM venue_instrument"
                                         " WHERE venue = ? AND native_id = ?", (link.venue, native)).fetchone()
            if side is None:                            # Novig: the stored side of this outcome
                side = (s0, s1).index(outcome[0])
            ensure_mapping(w, dict(
                venue_instrument_id=iid, side=side, outcome_id=outcomes[team], polarity=polarity,
                mapping_status="fuzzy_candidate", settlement_equivalence="pending",
                method=KALSHI_RULE if link.venue == "kalshi" else NOVIG_RULE, evidence=evidence, rules_native=rules,
                effective_from_ms=None, observed_ts_ms=w.now, **w.cited(x.body.ref)))


def _toml(s: str) -> str:
    return json.dumps(s)                    # a TOML basic string is a JSON string


def proposals(conn, links: list[Link]) -> str:
    """The clean links as `clv correct` corrections, verifying each mapped side."""
    out = ["# Proposed by `clv backfill link`. Review links.review.md, delete any entry you do not",
           "# approve, save as links.approved.toml and apply it with `clv correct --spec`.", ""]
    for link in links:
        if link.status != "clean":
            continue
        for native, side, team, polarity, x, rules, *outcome in link.sides:
            if side is None:
                s0, s1 = conn.execute("SELECT side0_id, side1_id FROM venue_instrument WHERE venue = ? AND native_id = ?",
                                      (link.venue, native)).fetchone()
                side = (s0, s1).index(outcome[0])
            rule = KALSHI_RULE if link.venue == "kalshi" else NOVIG_RULE
            out += ["[[correction]]", f"venue = {_toml(link.venue)}", f"native_id = {_toml(native)}",
                    f"side = {side}", 'status = "manual_verified"',
                    f"method = {_toml(rule + ', approved by hand from links.review.md')}",
                    f"evidence = {_toml(link.evidence)}", ""]
    return "\n".join(out)


def review(links: list[Link], sample: int = 50) -> str:
    counts = Counter((link.venue, link.status) for link in links)
    out = ["# B1 links for review", "",
           "Generated by `clv backfill link`. Vendor data: keep this file out of the repository.", "",
           "| Venue | Clean (proposed) | Edge (listed below) | Excluded |", "|---|---:|---:|---:|"]
    for venue in ("kalshi", "novig"):
        out.append(f"| {venue} | {counts[(venue, 'clean')]} | {counts[(venue, 'edge')]} | "
                   f"{counts[(venue, 'excluded')]} |")
    rng = random.Random(SAMPLE_SEED)
    for venue in ("kalshi", "novig"):
        clean = [link for link in links if link.venue == venue and link.status == "clean"]
        picked = rng.sample(clean, min(sample, len(clean)))
        out += ["", f"## Spot-check sample: {venue}, {len(picked)} of {len(clean)} clean links (seed {SAMPLE_SEED})", ""]
        out += [f"- {link.evidence}" for link in sorted(picked, key=lambda k: k.native_event)]
    for status in ("edge", "excluded"):
        rows = [link for link in links if link.status == status]
        out += ["", f"## {status.capitalize()} cases ({len(rows)})", ""]
        for link in rows:
            out.append(f"- **{link.venue} {link.native_event}**: {'; '.join(link.reasons)}")
            if link.evidence:
                out.append(f"  - {link.evidence}")
            out += [f"  - candidate: {_describe(g)}" for g in link.candidates]
    return "\n".join(out) + "\n"


def link_all(w, hist_root: Path, games: dict[int, ScheduledGame], out_dir: Path, report: Counter) -> list[Link]:
    links = kalshi_links(kalshi_hist.listings(hist_root), games)
    found, events = novig_public.markets(hist_root)
    links += novig_links(found, events, games)
    write_links(w, links, report)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "links.proposed.toml").write_text(proposals(w.conn, links))
    (out_dir / "links.review.md").write_text(review(links))
    return links
