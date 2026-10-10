"""Off resolution (DESIGN.md §5.2): start bounds from independent sightings.

The claims below are 849832's: StatsAPI "In Progress" 00:08:04.996, Novig
GOLIVE 00:08:23.244, first pitch 00:08:51.917, each seen more than once, plus a
pregame "Warmup" that is not a start claim.
"""
import json

import pytest

from clv import db, lineage
from clv.off import resolver

T = 1_791_504_000_000            # scheduled start 2026-10-09T00:00Z
WARMUP, IN_PROGRESS, GOLIVE, PITCH = T - 1_604_818, T + 484_996, T + 503_244, T + 531_917


@pytest.fixture
def conn():
    c = db.connect(":memory:")
    db.migrate(c)
    c.execute("INSERT INTO raw_artifact VALUES (1, 'mlb_statsapi', 'mlb_statsapi/d/x.0001.jsonl.gz', ?, 1, 1, NULL,"
              " NULL, 's', '0.1.0', 'r0-v1', 0, 'v0.1', 0)", ("a" * 64,))
    c.execute("INSERT INTO event VALUES (1, 'MLB', '849832', 0)")
    return c


def claim(c, source, kind, detected, observed=T + 5_000_000):
    c.execute("INSERT INTO off_observation (event_id, source, kind, subject, detected_off_ts_ms, observed_ts_ms,"
              " raw_artifact_id, raw_line) VALUES (1, ?, ?, 's', ?, ?, 1, 0)", (source, kind, detected, observed))


def resolve(c):
    lineage.use(c, lineage.take(c, 0))
    return resolver.resolve(c, 1)


def game4(c):
    for observed in (T + 5_000_000, T + 43_000_000):                 # two retrospective sightings each
        claim(c, "mlb_statsapi", "status_warmup", WARMUP, observed)
        claim(c, "mlb_statsapi", "status_in_progress", IN_PROGRESS, observed)
        claim(c, "mlb_statsapi", "first_pitch", PITCH, observed)
    claim(c, "novig_stream", "venue_golive", GOLIVE, GOLIVE + 55)


def test_game4_resolves_to_the_earliest_credible_bound(conn):
    game4(conn)
    r = resolve(conn)
    assert r.trusted
    assert (r.earliest_ms, r.selected_ms, r.latest_ms) == (IN_PROGRESS, PITCH, PITCH)
    assert r.disagreement_ms == PITCH - IN_PROGRESS == 46_921
    assert len(r.source_ids) == 5                    # every sighting of an accepted claim; warmups left out


def test_a_venue_transition_before_every_official_claim_moves_the_earliest_bound(conn):
    game4(conn)
    claim(conn, "novig_stream", "venue_golive", IN_PROGRESS - 30_000)
    r = resolve(conn)
    assert r.trusted and r.earliest_ms == IN_PROGRESS - 30_000 and r.selected_ms == PITCH


def test_without_a_first_pitch_the_official_transition_is_selected(conn):
    claim(conn, "mlb_statsapi", "status_in_progress", IN_PROGRESS)
    claim(conn, "novig_stream", "venue_golive", GOLIVE)
    r = resolve(conn)
    assert r.trusted and (r.earliest_ms, r.selected_ms, r.latest_ms) == (IN_PROGRESS, IN_PROGRESS, IN_PROGRESS)


def test_a_venue_transition_alone_is_not_trusted(conn):
    claim(conn, "novig_stream", "venue_golive", GOLIVE)
    claim(conn, "mlb_statsapi", "status_warmup", WARMUP)
    r = resolve(conn)
    assert (r.reason, r.earliest_ms, r.latest_ms) == ("no_trusted_off", GOLIVE, GOLIVE)


def test_nothing_but_a_schedule_has_no_bounds(conn):
    r = resolve(conn)
    assert (r.reason, r.earliest_ms, r.selected_ms, r.source_ids) == ("no_trusted_off", None, None, ())


def test_sources_too_far_apart_are_untrusted(conn):
    game4(conn)
    claim(conn, "mlb_statsapi", "first_pitch", PITCH + 200_000)       # a corrected first pitch, 200 s later
    r = resolve(conn)
    assert r.reason == "off_disagreement" and r.disagreement_ms == PITCH + 200_000 - IN_PROGRESS
    assert (r.earliest_ms, r.selected_ms, r.latest_ms) == (IN_PROGRESS, PITCH, PITCH + 200_000)


def test_the_resolution_row_is_written_as_resolved(conn):
    game4(conn)
    snap = lineage.take(conn, 0)
    lineage.use(conn, snap)
    rid = resolver.write(conn, snap, resolver.resolve(conn, 1), 7)
    row = conn.execute("SELECT resolver_version, confidence, reason, earliest_plausible_start_ts_ms, source_set"
                       " FROM off_resolution WHERE off_resolution_id = ?", (rid,)).fetchone()
    assert tuple(row)[:4] == ("off-v0.1", "trusted", None, IN_PROGRESS)
    assert len(json.loads(row[4])) == 5


def test_a_later_snapshot_does_not_change_an_earlier_resolution(conn):
    game4(conn)
    first = lineage.take(conn, 0)
    claim(conn, "mlb_statsapi", "first_pitch", PITCH + 200_000)
    lineage.use(conn, first)
    assert resolver.resolve(conn, 1).trusted
    lineage.use(conn, lineage.take(conn, 0))
    assert resolver.resolve(conn, 1).reason == "off_disagreement"
