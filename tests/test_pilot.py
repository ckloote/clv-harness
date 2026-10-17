"""The A1 storage pilot report (clv.pilot): raw frames through the replay and the ingest to the
report's numbers.

One NBA market, scheduled to start at S, is recorded from S - 3,650 s. Its book changes at
known moments, goes quiet in between, loses a delta (a sequence gap, closed by a resync), and is
probed. The recorder's probe events and the league's catalog listings are archived beside it.
"""
import json

import pytest
from raw_recorder.archive import ArchiveWriter
from test_ingest_league import Clock, market, rest

from clv import db, lineage, pilot
from clv.ingest import ingest_league_event
from clv.timeutil import ms_iso

S = 1_792_206_000_000                       # the scheduled start
SEC = 1_000
M, E, A, B = "nm-p", "ne-p", "o-home", "o-away"
ORDERS = {A: [{"orderId": "a1", "price": "0.55", "qty": 1000}], B: [{"orderId": "b1", "price": "0.43", "qty": 800}]}
EVENT = {"league": "NBA", "event_id": E, "label": "fixture", "novig_markets": [M],
         "scheduled_start_utc": ms_iso(S), "capture_lead_s": 3_700, "capture_tail_s": 300}
LO, HI = S - 3_700 * SEC, S + 300 * SEC
NBA = "league=NBA&marketType=MONEY&limit=100"


def at(s):
    return int(S + s * SEC)


def snapshot(t, seq, orders, nonce, subscribed=False):
    msg = {"ts": t, "nonce": nonce, "snapshot": {M: {"eventId": E, "book": {"seq": seq, "orders": orders},
                                                     "lifecycle": {"seq": 0, "status": "OPEN"}}}}
    if subscribed:
        msg["subscribed"] = {"markets": {M: "book"}, "events": {}, "private": []}
    return msg


def add(t, seq, oid, price):
    return {"ts": t, "delta": {M: {"eventId": E, "book": {"seq": seq, "deltas": [
        {"kind": "add", "orderId": oid, "outcomeId": A, "price": price, "qty": 10}]}}}}


def book_after(*oids):
    return {A: ORDERS[A] + [{"orderId": o, "price": p, "qty": 10} for o, p in oids], B: ORDERS[B]}


def listing(*mids):
    return {"items": [market(m, E, S, [(A, "HOME"), (B, "AWAY")]) for m in mids]}


def record(root, hi=HI + 200 * SEC):
    clock = Clock()
    clock.t = LO - 60 * SEC
    w = ArchiveWriter(root, session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    disc = w.stream("novig", "discovery-leagues-s1")
    rest(clock, disc, "l0", at(-3_690), pilot.CATALOG_PATH, "league_catalog", listing(M), query=NBA)
    rest(clock, disc, "l1", at(-1_800), pilot.CATALOG_PATH, "league_catalog", listing(M, "late"), query=NBA)
    nv = w.stream("novig", "rest-s1")
    rest(clock, nv, "c0", at(-3_680), f"/v3/public/catalog/markets/{M}", "catalog",
         market(M, E, S, [(A, "HOME"), (B, "AWAY")]), [f"novig:market:{M}"])
    ws = w.stream("novig", "c1")

    def put(s, d, obj):
        clock.t = at(s)
        ws.write(d, "c1", json.dumps(obj), recv_ts_ms=clock.t)

    def event(s, kind, **fields):
        clock.t = at(s)
        ws.event("c1", kind, **fields)

    event(-3_650.001, "ws_connect_attempt", markets=[M], channel="book")
    put(-3_650, "out", {"nonce": 1, "subscribe": {"markets": {M: "book"}}})
    put(-3_649.995, "in", snapshot(at(-3_649.995), 10, ORDERS, 1, subscribed=True))
    put(-3_500, "in", add(at(-3_500), 11, "x1", "0.50"))
    put(-3_000, "in", add(at(-3_000), 12, "x2", "0.49"))
    event(-2_000.01, "probe_sent", nonce=2, channel="book", markets=[M], outside_dense=[M])
    put(-2_000, "in", snapshot(at(-2_000), 12, book_after(("x1", "0.50"), ("x2", "0.49")), 2))
    put(-1_000, "in", add(at(-1_000), 13, "x3", "0.48"))
    put(-700, "in", add(at(-700), 15, "x5", "0.46"))              # seq 14 never arrived
    event(-699.99, "probe_skipped", reason="stream_budget", channel="book", markets=[M], outside_dense=[])
    event(-690.01, "probe_sent", nonce=3, channel="book", markets=[M], outside_dense=[])
    put(-690, "in", snapshot(at(-690), 15, book_after(("x1", "0.50"), ("x2", "0.49"), ("x3", "0.48"),
                                                     ("x4", "0.47"), ("x5", "0.46")), 3))
    put(-100, "in", add(at(-100), 16, "x6", "0.45"))
    put(200, "in", add(at(200), 17, "x7", "0.44"))
    event(250, "ws_disconnected", reason="no_active_markets", close_code=1000, duration_s=1.0)
    clock.t = hi                                     # idle on: everything before this is sealed
    w.close()


@pytest.fixture
def recorded(tmp_path):
    root = tmp_path / "archive"
    record(root)
    conn = db.connect(":memory:")
    db.migrate(conn)
    ingest_league_event(conn, root, E, now_ms=HI + 1_000 * SEC, event=EVENT)
    return conn, root


def test_the_report_counts_probes_inside_and_outside_dense_windows(recorded):
    conn, root = recorded
    rep = pilot.run(conn, root, LO, HI)
    assert [e.event_id for e in rep.events] == [E]
    assert rep.probes.sent == {("book", "quiet"): 1, ("book", "dense"): 1}
    assert rep.probes.skipped == {("book", "dense"): 1}
    assert rep.probes.tokens() == 2 * pilot.PROBE_COST["book"]


def test_quiet_intervals_by_time_to_start_and_a_gap_is_missing_not_quiet(recorded):
    conn, root = recorded
    rep = pilot.run(conn, root, LO, HI)
    by_name = {b.name: b for b in rep.buckets}
    assert list(by_name) == ["more than 24 h before", "24 h to 6 h before", "6 h to 1 h before",
                             "1 h to 15 min before", "dense window (15 min before to 30 min after)",
                             "more than 30 min after"]
    # Activity: the subscribe snapshot (-3,649.995 s), deltas at -3,500, -3,000 and -1,000 s, the resync
    # snapshot (-690 s), deltas at -100 and +200 s. The probe at -2,000 s is evidence, not activity.
    assert by_name["6 h to 1 h before"].quiet_s == pytest.approx([149.995])
    assert by_name["1 h to 15 min before"].quiet_s == pytest.approx([500, 2_000])
    # -1,000 to -690 s spans the sequence gap: missing.
    assert by_name["1 h to 15 min before"].gapped == 1
    assert by_name["dense window (15 min before to 30 min after)"].quiet_s == pytest.approx([590, 300])


def test_cutoffs_take_the_close_functions_verdict(recorded):
    conn, root = recorded
    rep = pilot.run(conn, root, LO, HI)
    c = rep.cutoffs
    assert c["offset 24 h"] == {"no_quotes": 1} and c["offset 6 h"] == {"no_quotes": 1}
    assert c["offset 1 h"] == {"live, bracketed": 1}           # quiet since -3,650 s; the -3,500 s delta vouches
    assert c["offset 15 min"] == {"collection_gap": 1}         # inside the sequence gap
    assert c["cutoff 0 s"] == c["cutoff 1 min"] == {"live, bracketed": 1}  # the +200 s delta vouches
    assert c["cutoff 10 min"] == {"live, bracketed": 1}


def test_listing_lead_is_measured_from_the_first_listing_and_censored_at_the_archives_first(recorded):
    conn, root = recorded
    rep = pilot.run(conn, root, LO, HI)
    assert rep.leads == {M: (3_690 * SEC - 5, True)} and rep.unlisted == []
    leads, unlisted = pilot.listing_leads(root, HI, {M: (S, "NBA"), "late": (S + 3_600 * SEC, "NBA"),
                                                     "never": (S, "NBA")})
    assert leads["late"] == (5_400 * SEC - 5, False) and unlisted == ["never"]


def test_the_report_needs_an_archive_sealed_past_the_period(tmp_path):
    root = tmp_path / "archive"
    record(root, hi=HI - 100 * SEC)
    conn = db.connect(":memory:")
    db.migrate(conn)
    with pytest.raises(pilot.PilotError, match="sealed only through"):
        pilot.run(conn, root, LO, HI)


def test_the_report_writes_nothing(recorded):
    conn, root = recorded
    before = {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in (*lineage.FACT_TABLES, "fact_snapshot")}
    text = pilot.render(pilot.run(conn, root, LO, HI))
    after = {t: conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in before}
    assert after == before
    for heading in ("## Storage", "## Probes", "## Quiet intervals", "## Cutoffs", "## Depth", "## Listing lead"):
        assert heading in text
    assert "| offset 15 min |" in text


def test_probe_costs_and_refill_are_the_recorders():
    from raw_recorder.novig.stream import CHANNEL_WEIGHT, TokenBudget
    assert {ch: CHANNEL_WEIGHT[ch] for ch in pilot.PROBE_COST} == pilot.PROBE_COST
    assert TokenBudget().refill_per_s == pilot.STREAM_REFILL_PER_S


def test_quantiles_are_nearest_rank():
    assert [pilot._rank([500, 2_000], q) for q in (0.5, 0.9, 1.0)] == [500, 2_000, 2_000]
    assert [pilot._rank([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], q) for q in (0.5, 0.9)] == [5, 9]
    assert pilot._q([], 0.5) == "-"


def test_probes_from_a_recorder_older_than_the_policy_are_unclassified(tmp_path):
    clock = Clock()
    clock.t = LO
    w = ArchiveWriter(tmp_path / "archive", session_id="s1", segment_max_s=86_400, fsync_interval_s=5, clock=clock)
    ws = w.stream("novig", "c1")
    ws.event("c1", "probe_sent", nonce=2, channel="book", markets=[M, "other"])     # no outside_dense
    clock.t = HI + 1
    w.close()
    p = pilot.probes(tmp_path / "archive", LO, HI)
    assert p.sent == {("book", "unclassified"): 2} and p.tokens() == 2 * pilot.PROBE_COST["book"]


def discovered(root, *events):
    from raw_recorder import leagues
    from raw_recorder.config import LeagueEvent
    (root / "events.discovered.toml").write_text(leagues.render(
        [LeagueEvent("NBA", e, s, e, (f"m-{e}",), capture_lead_s=lead, capture_tail_s=300) for e, s, lead in events],
        LO, []))


def test_overlapping_events_not_yet_ingested_are_listed_and_the_report_is_incomplete(recorded):
    # Review of #31: sealing past --to does not settle an event starting after the period, which
    # was recorded during it. It must be named, never silently left out.
    conn, root = recorded
    discovered(root, (E, S, 3_700),
               ("open", S + 3_600 * SEC, 3_700),            # overlaps; its window ends past the watermark
               ("settled", S - 1_000 * SEC, 600),            # overlaps and settled, but not loaded
               ("later", S + 10 * 3_600 * SEC, 3_700))       # recorded only after the period
    rep = pilot.run(conn, root, LO, HI)
    assert [(e, why) for e, _, _, why in rep.missing] == [
        ("settled", "settled, not ingested yet: run `clv ingest --league-events`"),
        ("open", (f"the archive is sealed through {ms_iso(HI + 200 * SEC)}, not past its window's end "
                  f"{ms_iso(S + 3_900 * SEC)}"))]
    assert not rep.complete()
    text = pilot.render(rep)
    assert "**Incomplete:** 2 discovered league event(s)" in text and "- open open (starts" in text


def test_the_command_prints_an_incomplete_report_and_exits_1(recorded, tmp_path, capsys):
    from clv import cli
    conn, root = recorded
    path = tmp_path / "clv.sqlite"
    conn.execute("VACUUM INTO ?", (str(path),))
    discovered(root, (E, S, 3_700), ("open", S + 3_600 * SEC, 3_700))
    args = ["pilot", "--from", ms_iso(LO), "--to", ms_iso(HI), "--db", str(path), "--archive", str(root)]
    with pytest.raises(SystemExit, match="incomplete, 1 overlapping league event"):
        cli.main(args)
    assert "**Incomplete:**" in capsys.readouterr().out
    discovered(root, (E, S, 3_700))
    cli.main(args)                                   # complete: no exit
    assert "Every discovered league event whose window overlaps the period is ingested." in capsys.readouterr().out
