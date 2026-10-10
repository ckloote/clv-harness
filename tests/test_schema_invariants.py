"""The V0 migrations and their DESIGN.md §8.4 tests (the ones that apply at stage V0).

Trade deduplication (stream + CSV) arrives with trade_execution in A1/B1; the
polarity round trip is in test_polarity.py.
"""
import sqlite3

import pytest

from clv import db

FACTS = ["raw_artifact", "event", "event_alias", "outcome", "venue_instrument", "instrument_mapping_observation",
         "tick", "tick_level", "book_snapshot", "book_snapshot_level", "liveness_evidence", "collection_gap",
         "off_observation", "signal", "entry", "entry_quote_observation"]
DERIVED = ["fact_snapshot", "mapping_resolution", "off_resolution", "close_def", "close_price", "scoring_run",
           "clv_score"]
H = "a" * 64
T = 1_791_504_000_000            # scheduled start, 2026-10-09T00:00Z


@pytest.fixture
def conn():
    c = db.connect(":memory:")
    db.migrate(c)
    return c


def ins(c, table, **row):
    return c.execute(f"INSERT INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                     tuple(row.values())).lastrowid


def seed(c) -> dict:
    """One valid row in every V0 table: a game, its outcomes, a Novig and a Kalshi
    book, an entry on a sportsbook line, and a run scoring it against both."""
    ids = {}
    ids["art"] = ins(c, "raw_artifact", source="novig", relpath="novig/2026-10-08/c.0001.jsonl.gz", sha256=H, bytes=10,
                     frame_count=3, first_recv_ts_ms=T, last_recv_ts_ms=T + 5, session_id="s", recorder_version="0.1.0",
                     redaction_policy="r0-v1", unclean_close=0, parser_version="v0.1", ingested_ts_ms=T)
    cite = dict(raw_artifact_id=ids["art"], raw_line=0)
    ids["event"] = add_event(c, "849832")
    ids["alias"] = ins(c, "event_alias", event_id=ids["event"], provider="mlb_statsapi", provider_event_id="849832",
                       status="exact", method="gamePk", evidence="feed", observed_ts_ms=T, **cite)
    ids["cle"] = ins(c, "outcome", event_id=ids["event"], kind="team_wins", team_id=114, team_abbreviation="CLE",
                     description="CLE win")
    ids["cws"] = ins(c, "outcome", event_id=ids["event"], kind="team_wins", team_id=145, team_abbreviation="CWS",
                     description="CWS win")
    ids["novig"] = instrument(c, ids["art"], "novig", "mkt")
    ids["kalshi"] = instrument(c, ids["art"], "kalshi", "KX-CLE")
    ids["dk"] = ins(c, "venue_instrument", venue="odds_api", native_id="ev:draftkings:h2h", native_event_id="ev",
                    operator="draftkings", kind="sportsbook_line", side0_id="Chicago White Sox", side0_label="CWS",
                    side1_id="Cleveland Guardians", side1_label="CLE", payout_currency="USD", first_seen_ts_ms=T, **cite)
    ids["map_novig"] = mapping(c, ids["novig"], 1, ids["cle"])
    ids["map_dk"] = mapping(c, ids["dk"], 1, ids["cle"])
    ids["tick"] = ins(c, "tick", venue_instrument_id=ids["novig"], source="stream", seq=10, venue_status="OPEN",
                      venue_ts_ms=T - 70_000, observed_ts_ms=T - 70_000, bid0_e4=4800, bid1_e4=5150, levels0=1,
                      levels1=1, truncated=0, ladder_complete=1, snapshot_raw_artifact_id=ids["art"],
                      snapshot_raw_line=0, **cite)
    ins(c, "tick_level", tick_id=ids["tick"], side=0, rank=0, price_native="0.48", price_e4=4800, qty_native="100",
        payout_cents=100)
    ids["ktick"] = ins(c, "tick", venue_instrument_id=ids["kalshi"], source="poll", venue_status="active",
                       observed_ts_ms=T - 70_000, bid0_e4=5100, bid1_e4=4800, levels0=0, levels1=0, truncated=0,
                       ladder_complete=1, **cite)
    snap = ins(c, "book_snapshot", venue_instrument_id=ids["novig"], reason="subscribe", source="stream", seq=10,
               venue_status="OPEN", observed_ts_ms=T - 70_000, ladder_complete=1, **cite)
    ins(c, "book_snapshot_level", book_snapshot_id=snap, side=0, rank=0, price_native="0.48", price_e4=4800,
        qty_native="100", payout_cents=100)
    ins(c, "liveness_evidence", venue_instrument_id=ids["novig"], source="stream", kind="probe_confirmed",
        observed_ts_ms=T - 65_000, **cite)
    gap = ins(c, "collection_gap", kind="open", source="kalshi", scope="kalshi:market:KX-CLE", reason="recorder_restart",
              boundary_ts_ms=T - 600_000, observed_ts_ms=T, **cite)
    ins(c, "collection_gap", kind="close", opens_gap_id=gap, source="kalshi", scope="kalshi:market:KX-CLE",
        reason="recorder_restart", boundary_ts_ms=T - 500_000, observed_ts_ms=T, **cite)
    ids["off"] = ins(c, "off_observation", event_id=ids["event"], source="mlb_statsapi", kind="first_pitch",
                     subject="mlb:game:849832", detected_off_ts_ms=T + 531_917, observed_ts_ms=T + 900_000, **cite)
    ids["signal"] = ins(c, "signal", kind="control", event_id=ids["event"], outcome_id=ids["cle"], producer="v0",
                        decision_ts_ms=T - 600_000, emitted_ts_ms=T - 600_000, would_bet=1, policy_version="v0",
                        features_as_of_decision=1, created_ts_ms=T)
    ids["entry"] = ins(c, "entry", kind="signal", signal_id=ids["signal"], event_id=ids["event"], outcome_id=ids["cle"],
                       venue_instrument_id=ids["dk"], side=1, mapping_obs_id=ids["map_dk"], decision_ts_ms=T - 500_000,
                       created_ts_ms=T)
    ids["quote"] = ins(c, "entry_quote_observation", entry_id=ids["entry"], concept="observed_quote", price_native="-118",
                       d_entry_num=109, d_entry_den=59, venue_ts_ms=T - 529_000, observed_ts_ms=T - 528_000, **cite)
    ids["fs"] = ins(c, "fact_snapshot", created_ts_ms=T, high_water_json="{}", manifest_json="[]", manifest_sha256=H)
    ins(c, "mapping_resolution", fact_snapshot_id=ids["fs"], resolver_version="v0", venue_instrument_id=ids["novig"],
        side=1, mapping_obs_id=ids["map_novig"], primary_eligible=0, ineligible_reason="equivalence_pending",
        computed_ts_ms=T)
    ids["offres"] = ins(c, "off_resolution", fact_snapshot_id=ids["fs"], resolver_version="v0", event_id=ids["event"],
                        selected_start_ts_ms=T + 531_917, earliest_plausible_start_ts_ms=T + 484_996,
                        latest_plausible_start_ts_ms=T + 531_917, source_set="[1]", source_disagreement_ms=46_921,
                        confidence="trusted", computed_ts_ms=T)
    ids["cdef"] = ins(c, "close_def", name="close_live_mid_top", family="close_live", params_json="{}",
                      params_sha256=H, created_ts_ms=T)
    ids["close_novig"] = close(c, ids, "novig", ids["tick"])
    ids["close_kalshi"] = close(c, ids, "kalshi", ids["ktick"])
    ids["run"] = ins(c, "scoring_run", fact_snapshot_id=ids["fs"], scorer_version="v0", fee_model_version="none",
                     analysis_spec="v0", created_ts_ms=T)
    ids["score"] = score(c, ids, "novig")
    return ids


def add_event(c, game_pk):
    return ins(c, "event", league="MLB", league_game_id=game_pk, created_ts_ms=T)


def instrument(c, art, venue, native_id):
    return ins(c, "venue_instrument", venue=venue, native_id=native_id, native_event_id="ev", kind="exchange_book",
               side0_id="a", side0_label="A", side1_id="b", side1_label="B", payout_currency="USD",
               payout_cents_per_contract=1, first_seen_ts_ms=T, raw_artifact_id=art, raw_line=0)


def mapping(c, instrument_id, side, outcome_id, polarity="direct", status="manual_verified", supersedes=None):
    return ins(c, "instrument_mapping_observation", venue_instrument_id=instrument_id, side=side, outcome_id=outcome_id,
               polarity=polarity, mapping_status=status, settlement_equivalence="pending", method="m", evidence="e",
               observed_ts_ms=T, supersedes_id=supersedes)


def close(c, ids, venue, tick, **over):
    row = dict(fact_snapshot_id=ids["fs"], close_def_id=ids["cdef"], off_resolution_id=ids["offres"], ref_venue=venue,
               outcome_id=ids["cle"], cutoff_ts_ms=T + 424_996, book_tick_id=tick, book_observed_ts_ms=T - 70_000,
               p_close_num=103, p_close_den=200, computed_ts_ms=T)
    row.update(over)
    return ins(c, "close_price", **row)


def score(c, ids, venue, **over):
    row = dict(scoring_run_id=ids["run"], entry_id=ids["entry"], ref_venue=venue, close_def_id=ids["cdef"],
               close_price_id=ids[f"close_{venue}"], entry_quote_observation_id=ids["quote"], d_entry_num=109,
               d_entry_den=59, computed_ts_ms=T)
    if "ref_entry_reason" in {r[1] for r in c.execute("PRAGMA table_info(clv_score)")}:     # from 0002
        row["ref_entry_reason"] = "no_quotes"
    row.update(over)
    return ins(c, "clv_score", **row)


# -- the migration itself ---------------------------------------------------------------------

def test_migration_applies_cleanly_and_once(conn):
    assert db.migrate(conn) == []                     # already applied
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    assert set(FACTS + DERIVED) <= tables
    sql = {r[0]: r[1] for r in conn.execute("SELECT name, sql FROM sqlite_master WHERE type = 'table'")}
    assert all("WITHOUT ROWID" not in sql[t] for t in FACTS + DERIVED)   # fact_snapshot needs rowids
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_declared_indexes_and_foreign_keys_exist(conn):
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'index'")}
    assert {"tick_instrument_observed", "tick_instrument_venue_ts", "event_alias_provider", "collection_gap_scope",
            "off_observation_event", "liveness_instrument_observed"} <= indexes
    fks = {(t, r["table"]) for t in FACTS + DERIVED for r in conn.execute(f"PRAGMA foreign_key_list({t})")}
    assert {("tick", "venue_instrument"), ("tick", "raw_artifact"), ("entry", "signal"),
            ("clv_score", "close_price"), ("close_price", "off_resolution")} <= fks


def test_changed_applied_migration_is_refused(tmp_path):
    (tmp_path / "0001_v0.sql").write_text("CREATE TABLE a (x INTEGER) STRICT;\n")
    c = db.connect(":memory:")
    db.migrate(c, tmp_path)
    (tmp_path / "0001_v0.sql").write_text("CREATE TABLE a (x INTEGER, y INTEGER) STRICT;\n")
    with pytest.raises(db.MigrationError, match="changed after it was applied"):
        db.migrate(c, tmp_path)


def test_failed_migration_leaves_nothing_behind(tmp_path):
    (tmp_path / "0001_v0.sql").write_text("CREATE TABLE a (x INTEGER) STRICT;\nCREATE TABLE a (x INTEGER) STRICT;\n")
    c = db.connect(":memory:")
    with pytest.raises(sqlite3.OperationalError):
        db.migrate(c, tmp_path)
    assert c.execute("SELECT count(*) FROM sqlite_master WHERE name = 'a'").fetchone()[0] == 0


# -- immutability -------------------------------------------------------------------------------

@pytest.mark.parametrize("table", FACTS + DERIVED)
def test_every_table_refuses_update_and_delete(conn, table):
    seed(conn)
    pk = conn.execute(f"SELECT name FROM pragma_table_info('{table}') WHERE pk = 1").fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="append-only|immutable"):
        conn.execute(f"UPDATE {table} SET {pk} = {pk}")
    with pytest.raises(sqlite3.IntegrityError, match="append-only|immutable"):
        conn.execute(f"DELETE FROM {table}")


def test_a_superseding_observation_is_how_a_mapping_changes(conn):
    ids = seed(conn)
    fixed = mapping(conn, ids["novig"], 1, ids["cws"], supersedes=ids["map_novig"])
    rows = conn.execute("SELECT mapping_obs_id, outcome_id, supersedes_id FROM instrument_mapping_observation "
                        "WHERE venue_instrument_id = ? AND side = 1 ORDER BY 1", (ids["novig"],)).fetchall()
    assert [tuple(r) for r in rows] == [(ids["map_novig"], ids["cle"], None), (fixed, ids["cws"], ids["map_novig"])]
    with pytest.raises(sqlite3.IntegrityError, match="same instrument side"):
        mapping(conn, ids["novig"], 0, ids["cws"], supersedes=ids["map_novig"])


# -- identity -------------------------------------------------------------------------------------

def test_doubleheader_is_two_games(conn):
    ids = seed(conn)
    g2 = add_event(conn, "849900")                    # same teams, same date, game 2
    for event, number in ((ids["event"], 1), (g2, 2)):
        ins(conn, "event_alias", event_id=event, provider="mlb_statsapi", provider_event_id=str(event),
            detail_json=f'{{"double_header": "S", "game_number": {number}}}', status="exact", method="gamePk",
            evidence="feed", observed_ts_ms=T)
    assert conn.execute("SELECT count(*) FROM event").fetchone()[0] == 2
    with pytest.raises(sqlite3.IntegrityError):
        add_event(conn, "849832")                     # one gamePk is one event


def test_rescheduled_game_keeps_its_identity_across_new_provider_ids(conn):
    ids = seed(conn)
    # Novig lists a new event for a rescheduled game (docs/vendor-capabilities.md); both point at one event.
    for provider_event_id, start in (("novig-original", T), ("novig-rescheduled", T + 86_400_000)):
        ins(conn, "event_alias", event_id=ids["event"], provider="novig", provider_event_id=provider_event_id,
            scheduled_start_ms=start, status="manual_verified", method="games.toml", evidence="hand",
            observed_ts_ms=T)
    events = {r[0] for r in conn.execute("SELECT event_id FROM event_alias WHERE provider = 'novig'")}
    assert events == {ids["event"]}


# -- scores ------------------------------------------------------------------------------------------

def test_two_reference_venues_score_one_entry(conn):
    ids = seed(conn)
    score(conn, ids, "kalshi")
    venues = {r[0] for r in conn.execute("SELECT ref_venue FROM clv_score WHERE entry_id = ?", (ids["entry"],))}
    assert venues == {"novig", "kalshi"}
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
        score(conn, ids, "novig")                     # same venue, same run, same definition


def test_score_must_use_its_venues_close(conn):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError, match="does not match its close"):
        score(conn, ids, "kalshi", close_price_id=ids["close_novig"])


# -- numeric and temporal constraints ------------------------------------------------------------------

@pytest.mark.parametrize("table, over", [
    ("tick_level", dict(price_e4=10_001)),                         # invalid probability
    ("tick_level", dict(price_e4=-1)),
    ("tick_level", dict(payout_cents=-1)),                         # negative depth
    ("tick", dict(bid0_e4=5200, bid1_e4=4900)),                    # inverted (crossed) book
    ("tick", dict(source="stream", seq=None)),                     # stream state without a sequence
    ("tick", dict(source="poll", snapshot_raw_artifact_id=1, snapshot_raw_line=0)),   # poll with a snapshot chain
    ("liveness_evidence", dict(source="stream", kind="poll_unchanged")),
    ("collection_gap", dict(kind="close", opens_gap_id=None)),
    ("entry_quote_observation", dict(d_entry_num=1, d_entry_den=2)),
])
def test_invalid_values_are_rejected(conn, table, over):
    ids = seed(conn)
    base = {
        "tick_level": dict(tick_id=ids["tick"], side=1, rank=0, price_native="0.515", price_e4=5150, qty_native="1",
                           payout_cents=1),
        "tick": dict(venue_instrument_id=ids["novig"], source="poll", venue_status="OPEN", observed_ts_ms=T,
                     bid0_e4=4800, bid1_e4=5150, levels0=0, levels1=0, truncated=0, ladder_complete=1,
                     raw_artifact_id=ids["art"], raw_line=1),
        "liveness_evidence": dict(venue_instrument_id=ids["novig"], source="poll", kind="poll_unchanged",
                                  observed_ts_ms=T, raw_artifact_id=ids["art"], raw_line=1),
        "collection_gap": dict(kind="open", source="novig", scope="s", reason="r", boundary_ts_ms=T, observed_ts_ms=T),
        "entry_quote_observation": dict(entry_id=ids["entry"], concept="decision_quote", price_native="-118",
                                        d_entry_num=109, d_entry_den=59, observed_ts_ms=T - 528_000,
                                        raw_artifact_id=ids["art"], raw_line=1),
    }[table]
    ins(conn, table, **base)                          # the base row is valid
    base.update(over)
    with pytest.raises(sqlite3.IntegrityError):
        ins(conn, table, **base)


def test_tick_levels_cannot_exceed_what_the_tick_declares(conn):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError, match="beyond the levels"):
        ins(conn, "tick_level", tick_id=ids["tick"], side=0, rank=1, price_native="0.47", price_e4=4700,
            qty_native="1", payout_cents=1)


def test_post_cutoff_reference_data_is_rejected(conn):
    ids = seed(conn)
    later = ins(conn, "tick", venue_instrument_id=ids["novig"], source="poll", venue_status="OPEN",
                observed_ts_ms=T + 430_000, bid0_e4=4800, bid1_e4=5150, levels0=0, levels1=0, truncated=0,
                ladder_complete=1, raw_artifact_id=ids["art"], raw_line=2)
    with pytest.raises(sqlite3.IntegrityError):       # observed after the cutoff
        close(conn, ids, "novig", later, book_observed_ts_ms=T + 430_000, close_def_id=new_def(conn))
    with pytest.raises(sqlite3.IntegrityError, match="not a pre-cutoff tick of the reference venue"):
        close(conn, ids, "novig", ids["ktick"], close_def_id=new_def(conn))   # another venue's book
    with pytest.raises(sqlite3.IntegrityError, match="not before the earliest plausible start"):
        close(conn, ids, "novig", ids["tick"], cutoff_ts_ms=T + 484_996, close_def_id=new_def(conn))


def test_close_needs_a_price_or_a_reason_not_both(conn):
    ids = seed(conn)
    close(conn, ids, "novig", None, book_observed_ts_ms=None, p_close_num=None, p_close_den=None,
          unscoreable_reason="no_trusted_off", close_def_id=new_def(conn))
    with pytest.raises(sqlite3.IntegrityError):
        close(conn, ids, "novig", None, book_observed_ts_ms=None, p_close_num=None, p_close_den=None,
              close_def_id=new_def(conn))
    with pytest.raises(sqlite3.IntegrityError):
        close(conn, ids, "novig", ids["tick"], unscoreable_reason="stale_book", close_def_id=new_def(conn))


_defs = iter(range(1, 10**6))


def new_def(conn):
    return ins(conn, "close_def", name="d", family="close_live", params_json="{}",
               params_sha256=f"{next(_defs):064x}", created_ts_ms=T)


# -- signals, entries and quotes -------------------------------------------------------------------------

def entry(conn, ids, **over):
    row = dict(kind="signal", signal_id=ids["signal"], event_id=ids["event"], outcome_id=ids["cle"],
               venue_instrument_id=ids["dk"], side=1, mapping_obs_id=ids["map_dk"], decision_ts_ms=T - 500_000,
               created_ts_ms=T)
    row.update(over)
    return ins(conn, "entry", **row)


def test_entry_outcome_must_match_its_mapping(conn):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError, match="does not match its instrument mapping"):
        entry(conn, ids, kind="control", signal_id=None, outcome_id=ids["cws"])
    with pytest.raises(sqlite3.IntegrityError, match="does not match its instrument mapping"):
        entry(conn, ids, kind="control", signal_id=None, side=0)           # the mapping covers side 1


def test_complement_mapping_admits_the_other_outcome(conn):
    ids = seed(conn)
    no_side = mapping(conn, ids["kalshi"], 1, ids["cle"], polarity="complement")
    entry(conn, ids, kind="control", signal_id=None, outcome_id=ids["cws"], venue_instrument_id=ids["kalshi"],
          mapping_obs_id=no_side)
    with pytest.raises(sqlite3.IntegrityError, match="does not match"):
        entry(conn, ids, kind="control", signal_id=None, outcome_id=ids["cle"], venue_instrument_id=ids["kalshi"],
              mapping_obs_id=no_side)


def test_rejected_mapping_cannot_carry_an_entry(conn):
    ids = seed(conn)
    rejected = mapping(conn, ids["dk"], 1, ids["cle"], status="rejected", supersedes=ids["map_dk"])
    with pytest.raises(sqlite3.IntegrityError, match="does not match"):
        entry(conn, ids, kind="control", signal_id=None, mapping_obs_id=rejected)


def test_signal_entry_outcome_mismatch_and_time_travel_are_rejected(conn):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError, match="precedes its signal or names another outcome"):
        entry(conn, ids, decision_ts_ms=T - 700_000)                         # before the signal was emitted
    cws_signal = ins(conn, "signal", kind="control", event_id=ids["event"], outcome_id=ids["cws"], producer="v0",
                     decision_ts_ms=T - 600_000, emitted_ts_ms=T - 600_000, would_bet=1, policy_version="v0",
                     features_as_of_decision=1, created_ts_ms=T)
    with pytest.raises(sqlite3.IntegrityError, match="precedes its signal or names another outcome"):
        entry(conn, ids, signal_id=cws_signal)
    with pytest.raises(sqlite3.IntegrityError):
        ins(conn, "signal", kind="control", event_id=ids["event"], outcome_id=ids["cle"], producer="v0",
            decision_ts_ms=T, emitted_ts_ms=T - 1, would_bet=0, policy_version="v0", features_as_of_decision=1,
            created_ts_ms=T)                                                 # emitted before its decision


def test_0002_rebuilds_close_price_and_clv_score_keeping_their_rows():
    c = db.connect(":memory:")
    db.migrate(c, upto=1)
    ids = seed(c)
    before = [tuple(r) for r in c.execute("SELECT * FROM close_price ORDER BY 1")]
    assert db.migrate(c) == [2]
    assert [tuple(r) for r in c.execute("SELECT * FROM close_price ORDER BY 1")] == before
    assert [tuple(r) for r in c.execute("SELECT clv_score_id, ref_entry_tick_id, ref_entry_reason FROM clv_score")] \
        == [(ids["score"], None, "not_recorded")]
    assert c.execute("PRAGMA foreign_key_check").fetchall() == []
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        c.execute("DELETE FROM clv_score")


def test_a_close_without_a_start_bound_has_no_cutoff_and_no_price(conn):
    ids = seed(conn)
    close(conn, ids, "novig", None, book_observed_ts_ms=None, cutoff_ts_ms=None, p_close_num=None,
          p_close_den=None, unscoreable_reason="no_trusted_off", close_def_id=new_def(conn))
    with pytest.raises(sqlite3.IntegrityError):
        close(conn, ids, "novig", ids["tick"], cutoff_ts_ms=None, close_def_id=new_def(conn))


def test_null_ev_reference_must_be_known_at_the_decision(conn):
    ids = seed(conn)
    cdef = new_def(conn)
    ids["close_novig"] = close(conn, ids, "novig", ids["tick"], close_def_id=cdef)
    early, later = (ins(conn, "tick", venue_instrument_id=ids["novig"], source="poll", venue_status="OPEN",
                        observed_ts_ms=t, bid0_e4=4800, bid1_e4=5150, levels0=0, levels1=0, truncated=0,
                        ladder_complete=1, raw_artifact_id=ids["art"], raw_line=2) for t in (T - 510_000, T - 499_000))
    good = dict(close_def_id=cdef, p_ref_entry_num=103, p_ref_entry_den=200, null_ev_num=1, null_ev_den=11800,
                ref_entry_reason=None)
    with pytest.raises(sqlite3.IntegrityError, match="received after the decision"):
        score(conn, ids, "novig", ref_entry_tick_id=later, **good)             # entry decided at T - 500 s
    with pytest.raises(sqlite3.IntegrityError, match="received after the decision"):
        score(conn, ids, "novig", ref_entry_tick_id=ids["ktick"], **good)      # another venue's book
    with pytest.raises(sqlite3.IntegrityError):                                # a price and a reason
        score(conn, ids, "novig", ref_entry_tick_id=early, **dict(good, ref_entry_reason="stale_book"))
    with pytest.raises(sqlite3.IntegrityError):                                # null EV without its input
        score(conn, ids, "novig", ref_entry_tick_id=early, **dict(good, null_ev_num=None, null_ev_den=None))
    score(conn, ids, "novig", ref_entry_tick_id=early, **good)


def test_quote_received_after_the_decision_is_not_known_at_it(conn):
    ids = seed(conn)
    # DESIGN.md §9.4 item 10: posted before the decision, received after it.
    with pytest.raises(sqlite3.IntegrityError, match="received after the entry decision time"):
        ins(conn, "entry_quote_observation", entry_id=ids["entry"], concept="decision_quote", price_native="-118",
            d_entry_num=109, d_entry_den=59, venue_ts_ms=T - 501_000, observed_ts_ms=T - 499_000,
            raw_artifact_id=ids["art"], raw_line=1)
    # An execution assumption looks forward by design, so it may be later.
    ins(conn, "entry_quote_observation", entry_id=ids["entry"], concept="execution_assumption", price_native="-120",
        d_entry_num=11, d_entry_den=6, observed_ts_ms=T - 485_000, latency_s=15, feasibility="unverified_historical",
        raw_artifact_id=ids["art"], raw_line=1)
