"""Closes, references and scores (DESIGN.md §5.3, §5.4, §6, §8.3), on the synthetic golden game.

Each test ingests the committed synthetic archive (tests/golden.py) into a
clean database and appends facts -- a gap, a halted or lagging book, a
mapping correction, another entry -- to show each unscoreable reason and
cohort exclusion is emitted for its cause and only for it (DESIGN.md §9.4).

Times are relative to the scheduled start S. The close cutoff is S+240 s: the
earliest claim (StatsAPI "In Progress", S+300 s) less the 60 s buffer.
"""
import json
from fractions import Fraction

import pytest

import golden
from clv import db, entries, identity, ingest, lineage
from clv.close import definitions as close
from clv.score import clv as scoring

S, SEC, NOW = golden.S, golden.SEC, golden.NOW
ARCHIVE, GAMES = golden.SYNTHETIC / "archive", golden.SYNTHETIC / "games.toml"
CUTOFF = S + 240 * SEC


@pytest.fixture
def conn():
    c = db.connect(":memory:")
    db.migrate(c)
    ingest.ingest_game(c, ARCHIVE, golden.SYN_PK, GAMES, now_ms=NOW)
    entries.record(c, ARCHIVE, golden.SYNTHETIC / "entries.toml", NOW, GAMES)
    return c


def run(c):
    r = scoring.run(c, NOW)
    lineage.use(c, r.fact_snapshot_id)
    return r


def closes(c, r, definition="close_live_mid_depth_500"):
    return {(v, o): (p, reason, observed) for v, o, p, reason, observed in (
        (row[0], row[1], None if row[2] is None else Fraction(row[2], row[3]), row[4], row[5]) for row in c.execute("""
        SELECT p.ref_venue, o.team_abbreviation, p.p_close_num, p.p_close_den, p.unscoreable_reason,
               p.book_observed_ts_ms
        FROM close_price p JOIN close_def d USING (close_def_id) JOIN outcome o USING (outcome_id)
        WHERE p.fact_snapshot_id = ? AND d.name = ?""", (r.fact_snapshot_id, definition)))}


def scores(c, r, definition="close_live_mid_depth_500"):
    return [(row[0], row[1], json.loads(row[2])) for row in c.execute("""
        SELECT s.ref_venue, s.entry_id, s.exclusion_reasons FROM clv_score s JOIN close_def d USING (close_def_id)
        WHERE s.scoring_run_id = ? AND d.name = ? ORDER BY s.entry_id, s.ref_venue""",
        (r.scoring_run_id, definition))]


def novig_ref(c):
    lineage.use(c, lineage.take(c, NOW))
    return close.reference(c, identity.current_mappings(c), "novig", outcome(c, "CLE"))


def outcome(c, abbr):
    return c.execute("SELECT outcome_id FROM outcome WHERE team_abbreviation = ? AND event_id = 1", (abbr,)).fetchone()[0]


def novig_tick(c, observed_ms, **over):
    """Append a stream tick for the Novig market, a copy of the S-600 s book with overrides."""
    src = c.execute("SELECT * FROM tick WHERE source = 'stream' AND observed_ts_ms = ?", (S - 600 * SEC,)).fetchone()
    row = dict(src)
    row.pop("tick_id")
    row.update({"observed_ts_ms": observed_ms, "venue_ts_ms": observed_ms - 30, **over})
    tid = c.execute(f"INSERT INTO tick ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                    tuple(row.values())).lastrowid
    c.execute("INSERT INTO tick_level (tick_id, side, rank, price_native, price_e4, qty_native, payout_cents)"
              " SELECT ?, side, rank, price_native, price_e4, qty_native, payout_cents FROM tick_level"
              " WHERE tick_id = ?", (tid, src["tick_id"]))
    return tid


def gap(c, scope, start_ms, end_ms=None):
    gid = c.execute("INSERT INTO collection_gap (kind, source, scope, reason, boundary_ts_ms, observed_ts_ms)"
                    " VALUES ('open', 'novig', ?, 'test', ?, ?)", (scope, start_ms, NOW)).lastrowid
    if end_ms is not None:
        c.execute("INSERT INTO collection_gap (kind, opens_gap_id, source, scope, reason, boundary_ts_ms,"
                  " observed_ts_ms) VALUES ('close', ?, 'novig', ?, 'test', ?, ?)", (gid, scope, end_ms, NOW))


BOOK_SCOPE = f"novig:market:{golden.M}/book"


# -- the close ------------------------------------------------------------------------------------

def test_the_close_is_the_latest_book_received_by_the_cutoff(conn):
    r = run(conn)
    got = closes(conn, r)
    # The in-play delta at S+400 s is after the cutoff and never used (DESIGN.md §9.4 item 4).
    assert got[("novig", "CLE")] == (Fraction("0.505"), None, S - 600 * SEC)
    assert got[("novig", "CWS")] == (Fraction("0.495"), None, S - 600 * SEC)
    cutoffs = {row[0] for row in conn.execute("SELECT cutoff_ts_ms FROM close_price")}
    assert cutoffs == {CUTOFF}


@pytest.mark.parametrize("start, end, reason", [
    (S + 100 * SEC, None, "collection_gap"),             # open after the book, never closed
    (S - 500 * SEC, S + 100 * SEC, "collection_gap"),    # opens after the book, closes before the cutoff
    (S - 700 * SEC, S - 650 * SEC, None),                # over before the book
    (S - 700 * SEC, S - 600 * SEC, None),                # closed by this very book (a resync)
    (CUTOFF, None, None),                                # opens at the cutoff: the book was good until then
])
def test_a_gap_between_the_book_and_the_cutoff_is_unscoreable(conn, start, end, reason):
    gap(conn, BOOK_SCOPE, start, end)
    assert closes(conn, run(conn))[("novig", "CLE")][1] == reason


def test_a_gap_on_another_scope_does_not_count(conn):
    gap(conn, f"novig:market:{golden.M}/trades", S + 100 * SEC)
    gap(conn, f"novig:market:{golden.M}", S + 100 * SEC)               # the public-poll scope
    assert closes(conn, run(conn))[("novig", "CLE")][1] is None


def test_liveness_and_quote_age_are_separate(conn):
    ref = novig_ref(conn)
    d = close.live_defs()[0]
    # The probe at S+230 s vouches for the book until 30 s later; the book itself is from S-600 s.
    ok = close.price_as_of(conn, ref, S + 260 * SEC, d)
    assert ok.reason is None and ok.liveness_age_ms == 29_950 and ok.quote_age_ms == 860_000
    stalled = close.price_as_of(conn, ref, S + 261 * SEC, d)
    assert (stalled.reason, stalled.tick_id) == ("feed_stalled", ok.tick_id)


def test_a_halted_book_is_unscoreable(conn):
    novig_tick(conn, S + 235 * SEC, venue_status="CLOSED")
    assert closes(conn, run(conn))[("novig", "CLE")][1:] == ("halted", S + 235 * SEC)


def test_a_late_arriving_book_is_unscoreable(conn):
    novig_tick(conn, S + 235 * SEC, venue_ts_ms=S + 229 * SEC)        # 6 s behind venue time
    assert closes(conn, run(conn))[("novig", "CLE")][1] == "venue_lag"


def test_an_old_book_on_a_live_feed_is_stale(conn):
    # Kalshi's CWS book never changed after S-3640 s, though every poll confirmed it.
    p, reason, observed = closes(conn, run(conn))[("kalshi", "CWS")]
    assert (p, reason, observed) == (None, "stale_book", S - 3640 * SEC + 40)       # the response's arrival
    liveness = conn.execute("SELECT liveness_age_ms FROM close_price p JOIN outcome o USING (outcome_id)"
                            " WHERE p.ref_venue = 'kalshi' AND o.team_abbreviation = 'CWS'").fetchone()[0]
    assert liveness < 30_000


def test_the_freshest_reference_source_is_used(conn):
    # A Novig public-book poll after the last stream evidence, at other prices, is the book priced.
    art, line = conn.execute("SELECT raw_artifact_id, raw_line FROM tick LIMIT 1").fetchone()
    tid = conn.execute("INSERT INTO tick (venue_instrument_id, source, venue_status, observed_ts_ms, bid0_e4,"
                       " bid1_e4, levels0, levels1, truncated, ladder_complete, raw_artifact_id, raw_line)"
                       " VALUES (1, 'poll', 'OPEN', ?, 4000, 4200, 1, 1, 0, 1, ?, ?)",
                       (S + 236 * SEC, art, line)).lastrowid
    for side, price in ((0, "0.40"), (1, "0.42")):          # side 0 is CWS, side 1 is CLE
        conn.execute("INSERT INTO tick_level (tick_id, side, rank, price_native, price_e4, qty_native, payout_cents)"
                     " VALUES (?, ?, 0, ?, ?, '100000', 100000)", (tid, side, price, int(Fraction(price) * 10000)))
    p, reason, observed = closes(conn, run(conn), "close_live_mid_top")[("novig", "CLE")]
    assert (p, reason, observed) == ((Fraction("0.42") + Fraction("0.60")) / 2, None, S + 236 * SEC)


# -- references and mappings ------------------------------------------------------------------------

def test_both_sides_claiming_one_outcome_is_ambiguous(conn):
    identity.correct_mapping(conn, "novig", golden.M, 0, evidence="test", method="test", now_ms=NOW,
                             outcome_abbreviation="CLE")              # the CWS side now also claims CLE
    got = closes(conn, run(conn))
    assert got[("novig", "CLE")][1] == "ambiguous_reference" and got[("novig", "CWS")][1] == "unmapped"


def test_two_direct_references_for_one_outcome_are_ambiguous(conn):
    identity.correct_mapping(conn, "kalshi", golden.K_CWS, 0, evidence="test", method="test", now_ms=NOW,
                             outcome_abbreviation="CLE")
    assert closes(conn, run(conn))[("kalshi", "CLE")][1] == "ambiguous_reference"


def test_a_complement_side_naming_the_other_outcome_is_inconsistent(conn):
    identity.correct_mapping(conn, "kalshi", golden.K_CLE, 1, evidence="test", method="test", now_ms=NOW,
                             outcome_abbreviation="CWS")              # NO of CLE as the complement of CWS = CLE
    assert closes(conn, run(conn))[("kalshi", "CLE")][1] == "mapping_inconsistent"


def test_no_start_bound_means_no_cutoff(conn):
    conn.execute("INSERT INTO event (league, league_game_id, created_ts_ms) VALUES ('MLB', '900002', 0)")
    for team, abbr in ((114, "CLE"), (145, "CWS")):
        conn.execute("INSERT INTO outcome (event_id, kind, team_id, team_abbreviation, description)"
                     " VALUES (2, 'team_wins', ?, ?, 'x')", (team, abbr))
    r = run(conn)
    rows = conn.execute("SELECT cutoff_ts_ms, unscoreable_reason, book_tick_id FROM close_price p"
                        " JOIN outcome o USING (outcome_id) WHERE o.event_id = 2 AND p.fact_snapshot_id = ?",
                        (r.fact_snapshot_id,)).fetchall()
    assert {tuple(x) for x in rows} == {(None, "no_trusted_off", None)} and len(rows) == 16


# -- scores and their exclusions ----------------------------------------------------------------------

def test_the_score_is_exact(conn):
    r = run(conn)
    row = conn.execute("""SELECT s.d_entry_num, s.d_entry_den, s.clv_ev_num, s.clv_ev_den, s.p_ref_entry_num,
                                 s.p_ref_entry_den, s.null_ev_num, s.null_ev_den, s.clv_residual_num, s.clv_residual_den
                          FROM clv_score s JOIN close_def d USING (close_def_id)
                          WHERE s.scoring_run_id = ? AND d.name = 'close_live_mid_depth_500' AND s.ref_venue = 'novig'""",
                       (r.scoring_run_id,)).fetchone()
    d, clv, p_ref, null, resid = (Fraction(row[i], row[i + 1]) for i in range(0, 10, 2))
    assert d == Fraction(109, 59)                                       # -118
    assert clv == Fraction("0.505") * d - 1 == Fraction(-791, 11800)
    # At the S-1h decision the book was the S-3999 s snapshot: bids (0.38, $300), (0.35, $400).
    assert p_ref == (Fraction("0.368") + Fraction("0.63")) / 2 == Fraction("0.499")
    assert null == p_ref * d - 1 and resid == (Fraction("0.505") - p_ref) * d


def test_cohort_exclusions_sit_beside_the_numbers(conn):
    r = run(conn)
    assert scores(conn, r) == [("kalshi", 1, ["mapping_unverified", "equivalence_pending"]),
                               ("novig", 1, ["mapping_unverified", "equivalence_pending"])]


def spec(tmp_path, decision, operator="draftkings", outcome="CLE"):
    p = tmp_path / "spec.toml"
    p.write_text(f'''game_pk = {golden.SYN_PK}
[[signal]]
outcome = "{outcome}"
kind = "manual"
producer = "test"
decision = "{decision}"
would_bet = 1
policy_version = "v0"
entry = {{ venue = "odds_api", operator = "{operator}" }}
''')
    return p


def test_an_offshore_book_is_excluded_as_unlicensed(conn, tmp_path):
    entries.record(conn, ARCHIVE, spec(tmp_path, "2026-04-01T11:00:00Z", "bovada"), NOW, GAMES)
    got = scores(conn, run(conn))
    assert [x for x in got if x[1] == 2] == [
        ("kalshi", 2, ["mapping_unverified", "unlicensed_book", "equivalence_pending"]),
        ("novig", 2, ["mapping_unverified", "unlicensed_book", "equivalence_pending"])]


def test_an_entry_decided_after_the_cutoff_is_excluded(conn, tmp_path):
    entries.record(conn, ARCHIVE, spec(tmp_path, "2026-04-01T12:04:00Z"), NOW, GAMES)     # the cutoff itself
    got = {(v, e): reasons for v, e, reasons in scores(conn, run(conn))}
    assert "entry_after_cutoff" in got[("novig", 2)] and "entry_after_cutoff" not in got[("novig", 1)]


def test_a_correction_moving_the_entry_side_supersedes_the_entry(conn):
    line = "syn-vend:draftkings:h2h"
    identity.correct_mapping(conn, "odds_api", line, 1, evidence="test", method="test", now_ms=NOW,
                             outcome_abbreviation="CWS")
    assert scores(conn, run(conn))[0][2][0] == "entry_mapping_superseded"


def test_a_rejected_entry_mapping_is_excluded_as_rejected(conn):
    identity.correct_mapping(conn, "odds_api", "syn-vend:draftkings:h2h", 1, evidence="test", method="test",
                             now_ms=NOW, status="rejected")
    assert scores(conn, run(conn))[0][2][0] == "mapping_rejected"


# -- entries --------------------------------------------------------------------------------------

def test_a_quote_received_after_the_decision_is_not_the_decision_quote(conn, tmp_path):
    # The S-3700 s poll's response arrived 40 ms after its request: a decision 20 ms after the
    # request cannot have seen it, so the S-7000 s quote is the decision quote (DESIGN.md §9.4 item 10).
    from clv.timeutil import ms_iso
    entries.record(conn, ARCHIVE, spec(tmp_path, ms_iso(S - 3700 * SEC + 20)), NOW, GAMES)
    observed = conn.execute("SELECT observed_ts_ms FROM entry_quote_observation WHERE entry_id = 2"
                            " AND concept = 'decision_quote'").fetchone()[0]
    assert observed == S - 7000 * SEC + 40


def test_every_entry_quote_cites_its_response_frame(conn):
    rows = conn.execute("""SELECT q.concept, q.price_native, q.observed_ts_ms, a.relpath, q.raw_line
                           FROM entry_quote_observation q JOIN raw_artifact a USING (raw_artifact_id)""").fetchall()
    assert {r[0] for r in rows} == {"observed_quote", "decision_quote", "execution_assumption"}
    assert {tuple(r)[1:] for r in rows} == {("-118", S - 3700 * SEC + 40, "odds_api/2026-04-01/odds-syn.0001.jsonl.gz", 4)}
    assert conn.execute("SELECT feasibility FROM entry_quote_observation WHERE concept = 'execution_assumption'"
                        ).fetchone()[0] == "unavailable"


def test_entries_need_a_bet_and_a_known_outcome(conn, tmp_path):
    p = spec(tmp_path, "2026-04-01T11:00:00Z")
    p.write_text(p.read_text().replace("would_bet = 1", "would_bet = 0"))
    with pytest.raises(entries.EntryError, match="would_bet"):
        entries.record(conn, ARCHIVE, p, NOW, GAMES)
    with pytest.raises(entries.EntryError, match="no outcome"):
        entries.record(conn, ARCHIVE, spec(tmp_path, "2026-04-01T11:00:00Z", outcome="NYY"), NOW, GAMES)
    with pytest.raises(entries.EntryError, match="no draftkings quote"):
        entries.record(conn, ARCHIVE, spec(tmp_path, "2026-04-01T09:00:00Z"), NOW, GAMES)
    assert conn.execute("SELECT count(*) FROM signal").fetchone()[0] == 2         # nothing half-written
