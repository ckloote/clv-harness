"""`clv` command line.

    clv inspect --game-pk 849832     parse one recorded game from the raw archive and summarize it

`inspect` reads only sealed segments, runs every V0 parser over the game's
capture window and prints what each source shows: book reconstruction and its
checks, polls, quotes, off observations and gaps. It writes nothing.
"""
from __future__ import annotations

import argparse
import tomllib
from collections import Counter
from pathlib import Path

from clv import archive, gaps, mlb
from clv.config import param
from clv.off import sources
from clv.scoring import odds_api
from clv.timeutil import iso_ms, ms_iso
from clv.venues import kalshi
from clv.venues.novig import parser as novig
from clv.venues.protocol import BinaryBook

REPO = Path(__file__).resolve().parents[2]
GAMES = REPO / "tools" / "raw_recorder" / "games.toml"


def load_game(path: Path, game_pk: int) -> dict:
    with open(path, "rb") as f:
        games = tomllib.load(f)["game"]
    for g in games:
        if g["game_pk"] == game_pk:
            return g
    raise SystemExit(f"game_pk {game_pk} is not in {path}")


def capture_window(g: dict) -> tuple[int, int]:
    start = iso_ms(g["scheduled_start_utc"])
    lead = g.get("capture_lead_s", param("r0.capture_lead_s"))
    tail = g.get("capture_tail_s", param("r0.capture_tail_s"))
    return start - lead * 1000, start + tail * 1000


def fmt_book(book, side: str, names: dict[str, str]) -> str:
    bid, ask = book.best_bid(side), book.best_ask(side)
    return (f"{names.get(side, side)}: bid {bid.price if bid else '—'} ask {ask.price if ask else '—'}"
            f"{'' if book.complete else ' (truncated ladder)'}")


def inspect(root: Path, game_pk: int, games_path: Path) -> None:
    g = load_game(games_path, game_pk)
    lo, hi = capture_window(g)
    print(f"# {g['label']} (gamePk {game_pk})")
    print(f"capture window {ms_iso(lo)} .. {ms_iso(hi)}\n")
    markets = set(g["novig_markets"])

    # -- StatsAPI: identity, result and first-play times from every archived feed of the game
    feeds, events = [], []
    for x in archive.rest_exchanges(archive.iter_frames(root, archive.segments(root, "mlb_statsapi"))):
        if x.ok and mlb.game_pk_of(x) == game_pk:
            if "/feed/live" in x.path:
                feeds.append(mlb.game(x))
            events += mlb.first_play_events(x)
    off_obs = sources.from_play_events(events)
    print("## MLB StatsAPI")
    for gm in feeds:
        print(f"- {ms_iso(gm.recv_ts_ms)} feed: {gm.away.abbreviation} @ {gm.home.abbreviation}, {gm.status}, "
              f"{gm.away_runs}-{gm.home_runs}; scheduled {ms_iso(gm.scheduled_start_ms)}, DH {gm.double_header}"
              f"{gm.game_number}{f', winner {gm.winner.abbreviation}' if gm.winner else ''}")
    first_pitch = min((o.detected_off_ts_ms for o in off_obs if o.kind == "first_pitch"), default=None)

    # -- Novig: stream replay, public polls and catalog
    replay = novig.StreamReplay()
    n_frames, books, out_kinds = 0, [], Counter()
    probe, seq_gaps, resyncs, trades, lifecycle = Counter(), [], [], 0, []
    stream_frames, polls, catalog = [], [], {}
    segs = archive.overlapping(archive.segments(root, "novig"), lo, hi)
    # A WebSocket segment's stream_id is its connection ID; poller streams are named by role.
    ws = [s for s in segs if not s.stream_id.startswith(("rest-", "catalog-", "echo-"))]
    for f in archive.iter_frames(root, [s for s in segs if s not in ws]):
        if lo <= f.recv_ts_ms <= hi:
            polls.append(f)
    # Whole connections, not a time slice: a connection's end events can fall just past the window.
    for f in archive.iter_frames(root, ws):
        stream_frames.append(f)
        n_frames += 1
        for o in replay.feed(f):
            out_kinds[type(o).__name__] += 1
            if isinstance(o, BinaryBook) and o.market in markets:
                books.append(o)
            elif isinstance(o, novig.ProbeCheck):
                probe[(o.channel, o.status)] += 1
            elif isinstance(o, novig.SequenceGap):
                seq_gaps.append(o)
            elif isinstance(o, novig.Resync):
                resyncs.append(o)
            elif isinstance(o, novig.Trade):
                trades += 1
            elif isinstance(o, novig.Lifecycle) and o.kind:
                lifecycle.append(o)
                if (obs := sources.from_lifecycle(o, game_pk)) is not None:
                    off_obs.append(obs)
    public = []
    novig_x = list(archive.rest_exchanges(polls))
    for x in novig_x:
        if not x.ok:
            continue
        if x.request.get("purpose") == "public_book":
            public.append(novig.public_book(x))
        elif x.request.get("purpose") == "catalog" and "/catalog/markets/" in x.path and x.path.count("/") == 5:
            m = novig.catalog_market(x)
            catalog[m.market] = m
    names = {oid: name for m in catalog.values() for oid, name, _ in m.outcomes}
    print("\n## Novig")
    for m in catalog.values():
        print(f"- catalog {m.market}: {m.description} {m.market_type}, voids {m.voids}, fee {m.fee}, "
              f"startsTs {ms_iso(m.starts_ts_ms) if m.starts_ts_ms else '—'}, outcomes "
              + ", ".join(f"{n} ({oid[-6:]})" for oid, n, _ in m.outcomes))
    print(f"- stream: {n_frames} frames on {len({f.conn_id for f in stream_frames})} connections; "
          f"replay outputs {dict(out_kinds)}")
    print(f"- probe checks: {dict(probe)}; sequence gaps {len(seq_gaps)}; trades {trades}")
    for lc in lifecycle:
        print(f"- lifecycle {lc.kind} status {lc.status} at venue {ms_iso(lc.venue_ts_ms)} (conn {lc.conn_id[:8]})")
    print(f"- public book polls: {len(public)} ok, {sum(not b.complete for b in public)} with a truncated ladder"
          + (f"; {ms_iso(public[0].recv_ts_ms)} .. {ms_iso(public[-1].recv_ts_ms)}" if public else ""))
    if books:
        print(f"- stream book states: {len(books)}; {ms_iso(books[0].recv_ts_ms)} .. {ms_iso(books[-1].recv_ts_ms)}")

    # -- Kalshi: order book polls and market listings
    kx = list(archive.rest_exchanges(archive.window(root, "kalshi", lo, hi)))
    kmarkets = {m.ticker: m for x in kx if x.ok and x.request.get("purpose") == "catalog" for m in kalshi.markets(x)}
    kbooks = [kalshi.orderbook(x, kmarkets[kalshi.ticker_of(x)].notional_value_dollars) for x in kx
              if x.ok and x.request.get("purpose") == "orderbook" and kalshi.ticker_of(x) in g["kalshi_tickers"]]
    print("\n## Kalshi")
    for m in kmarkets.values():
        if m.ticker in g["kalshi_tickers"]:
            print(f"- {m.ticker}: {m.title!r}, status {m.status}, result {m.result or '—'}, "
                  f"payout ${m.notional_value_dollars}, occurrence {m.occurrence_datetime}")
    status = Counter((x.request.get("purpose"), x.ok) for x in kx)
    print(f"- requests by (purpose, ok): {dict(status)}; order books parsed {len(kbooks)}")

    # -- The Odds API
    ox = [x for x in archive.rest_exchanges(archive.window(root, "odds_api", lo, hi))
          if x.request.get("purpose") == "odds"]
    teams = {feeds[-1].home.name, feeds[-1].away.name} if feeds else set()
    qs = [q for x in ox if x.ok for q in odds_api.quotes(x) if {q.home_team, q.away_team} == teams]
    print("\n## The Odds API")
    print(f"- polls {len(ox)} ({sum(x.ok for x in ox)} ok); quotes for this game {len(qs)} from "
          f"{len({q.bookmaker for q in qs})} books; vendor event IDs {sorted({q.vendor_event_id for q in qs})}")
    if ox:
        print(f"- credits after the last poll: {odds_api.credits(ox[-1])}")

    # -- Off observations and the close preview
    print("\n## Off observations (each claim once: first observed, and how many responses repeat it)")
    claims: dict[tuple, list] = {}
    for o in off_obs:
        claims.setdefault((o.detected_off_ts_ms, o.source, o.kind), []).append(o)
    for (ts, source, kind), os_ in sorted(claims.items()):
        print(f"- {source} {kind}: {ms_iso(ts)} (first observed {ms_iso(min(o.observed_ts_ms for o in os_))}, "
              f"{len(os_)} sightings)")
    if first_pitch:
        cut = first_pitch - param("close.buffer_s") * 1000
        print(f"\n## Books at first pitch − close.buffer_s = {ms_iso(cut)} (a preview; the close function is PR 3)")
        last_novig = max((b for b in books if b.recv_ts_ms <= cut), key=lambda b: b.recv_ts_ms, default=None)
        if last_novig:
            print(f"- Novig stream seq {last_novig.seq}, received {ms_iso(last_novig.recv_ts_ms)}: "
                  + "; ".join(fmt_book(last_novig, s, names) for s in last_novig.sides()))
        for t in g["kalshi_tickers"]:
            kb = max((b for b in kbooks if b.market == t and b.recv_ts_ms <= cut), key=lambda b: b.recv_ts_ms,
                     default=None)
            if kb:
                print(f"- Kalshi {t}, received {ms_iso(kb.recv_ts_ms)}: {fmt_book(kb, 'yes', {'yes': 'YES'})}")
        last_poll = max((q.recv_ts_ms for q in qs if q.recv_ts_ms <= cut), default=None)
        for q in sorted((q for q in qs if q.recv_ts_ms == last_poll), key=lambda q: (q.bookmaker, q.outcome_name)):
            print(f"- {q.bookmaker} {q.outcome_name} {q.price_american:+d} (book update {ms_iso(q.book_last_update_ms)})")

    # -- Gaps
    print("\n## Gaps")
    found = (gaps.stream_gaps("novig", stream_frames, seq_gaps, resyncs)
             + gaps.poll_gaps("novig", novig_x, {"public_book"})
             + gaps.poll_gaps("kalshi", kx, {"orderbook"})
             + gaps.poll_gaps("odds_api", ox, {"odds"}))
    for gp in found:
        print(f"- {gp.source} {gp.scope} {gp.reason}: {ms_iso(gp.start_ms)} .. "
              f"{ms_iso(gp.end_ms) if gp.end_ms else 'open'}")
    if not found:
        print("- none")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(prog="clv", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("inspect", help="parse one recorded game from the raw archive and summarize it")
    p.add_argument("--game-pk", type=int, required=True)
    p.add_argument("--archive", type=Path, default=REPO / "archive")
    p.add_argument("--games", type=Path, default=GAMES, help="recorder game list (default: %(default)s)")
    args = ap.parse_args(argv)
    if args.cmd == "inspect":
        inspect(args.archive, args.game_pk, args.games)


if __name__ == "__main__":
    main()
