"""The migrations and their DESIGN.md §8.4 tests, from the stage that introduces each table.

The polarity round trip is in test_polarity.py.
"""
import sqlite3
from fractions import Fraction

import pytest

from clv import db
from clv.close.vwap import TradeVwap, trade_vwap

FACTS = ["raw_artifact", "event", "event_alias", "outcome", "venue_instrument", "instrument_mapping_observation",
         "tick", "tick_level", "book_snapshot", "book_snapshot_level", "liveness_evidence", "collection_gap",
         "off_observation", "signal", "entry", "entry_quote_observation", "entry_opposite_quote", "trade_execution",
         "trade_source_observation", "historical_candle", "settlement_observation"]
DERIVED = ["fact_snapshot", "mapping_resolution", "off_resolution", "close_def", "close_price", "scoring_run",
           "clv_score", "calibration_run", "calibration_observation", "calibration_result"]
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
    ids["dquote"] = ins(c, "entry_quote_observation", entry_id=ids["entry"], concept="decision_quote",
                        price_native="-118", d_entry_num=109, d_entry_den=59, venue_ts_ms=T - 529_000,
                        observed_ts_ms=T - 528_000, **cite)
    if has_table(c, "entry_opposite_quote"):                                         # from 0003
        ids["opposite"] = ins(c, "entry_opposite_quote", entry_id=ids["entry"], side=0, price_native="100",
                              d_num=2, d_den=1, venue_ts_ms=T - 529_000, observed_ts_ms=T - 528_000, **cite)
    if has_table(c, "trade_execution"):                                              # from 0004
        ids["exec"] = execution(c, ids)
        ids["maker"] = sighting(c, ids, ids["exec"])
        ids["candle"] = candle(c, ids)
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
    if has_table(c, "settlement_observation"):                                       # from 0005
        ids["settle"] = settlement(c, ids)
        ids["hdef"] = ins(c, "close_def", name="close_hist_candle_mid_top", family="close_hist", params_json="{}",
                          params_sha256="b" * 64, created_ts_ms=T)
        ids["hclose"] = hist_close(c, ids)
        ids["cal"] = calibration_run(c, ids)
        ids["calobs"] = calibration_observation(c, ids)
        ids["calres"] = ins(c, "calibration_result", calibration_run_id=ids["cal"], kind="check1", offset_s=900,
                            component="intercept", n_events=40, n_rows=40, estimate=0.001, se=0.002,
                            lo=-0.0023, hi=0.0043, margin=0.005, outcome="passed", computed_ts_ms=T)
    return ids


def has_table(c, name):
    return c.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (name,)).fetchone() is not None


def add_event(c, game_pk):
    return ins(c, "event", league="MLB", league_game_id=game_pk, created_ts_ms=T)


def instrument(c, art, venue, native_id):
    return ins(c, "venue_instrument", venue=venue, native_id=native_id, native_event_id="ev", kind="exchange_book",
               side0_id="a", side0_label="A", side1_id="b", side1_label="B", payout_currency="USD",
               payout_cents_per_contract=1, first_seen_ts_ms=T, raw_artifact_id=art, raw_line=0)


def execution(c, ids, **over):
    """A Novig maker fill: 156.25 USD of payout on side 1 at 0.68 (cost 106.25)."""
    row = dict(venue_instrument_id=ids["novig"], execution_key="novig:maker:o1:1:106.25:156.25:0", side=1,
               price_native="0.68", price_num=17, price_den=25, qty_native="15625", payout_cents=15625,
               method="m", created_ts_ms=T, raw_artifact_id=ids["art"], raw_line=1, raw_item=0)
    row.update(over)
    return ins(c, "trade_execution", **row)


def sighting(c, ids, execution_id, **over):
    row = dict(venue_instrument_id=ids["novig"], execution_id=execution_id, source="novig_csv", role="maker",
               dual_side_group="mkt@2026-10-08T23:58:00.000Z", side=1, price_native="0.68", qty_native="156.25",
               venue_ts_ms=T - 120_000, observed_ts_ms=T + 3_600_000, reconcile="key", method="m",
               native_json="{}", raw_artifact_id=ids["art"], raw_line=1, raw_item=0)
    row.update(over)
    return ins(c, "trade_source_observation", **row)


def candle(c, ids, **over):
    row = dict(venue_instrument_id=ids["kalshi"], source="kalshi_hist", tier="historical", resolution_s=60,
               interval_start_ms=T - 120_000, interval_end_ms=T - 60_000, side=0, bid_close_e4=5100,
               ask_close_e4=5200, native_json="{}", observed_ts_ms=T, raw_artifact_id=ids["art"], raw_line=2,
               raw_item=0)
    row.update(over)
    return ins(c, "historical_candle", **row)


def settlement(c, ids, **over):
    row = dict(source="mlb_statsapi", subject="mlb:game:849832/team:114", event_id=ids["event"],
               outcome_id=ids["cle"], result="win", observed_ts_ms=T + 4 * 3_600_000, native_json="{}",
               raw_artifact_id=ids["art"], raw_line=3, raw_item=0)
    row.update(over)
    return ins(c, "settlement_observation", **row)


def hist_close(c, ids, **over):
    """A Kalshi candle close: the seed candle ends at T - 60 s, before the cutoff."""
    row = dict(fact_snapshot_id=ids["fs"], close_def_id=ids["hdef"], off_resolution_id=ids["offres"],
               ref_venue="kalshi", outcome_id=ids["cle"], cutoff_ts_ms=T + 424_996, candle_id=ids["candle"],
               cutoff_candle_active=0, quote_age_ms=485_000, p_close_num=103, p_close_den=200, spread_e4=100,
               computed_ts_ms=T)
    row.update(over)
    return ins(c, "close_price", **row)


def calibration_run(c, ids, **over):
    row = dict(fact_snapshot_id=ids["fs"], close_def_id=ids["hdef"], ref_venue="kalshi", mode="check",
               spec_json="{}", calibration_version="cal-v0", verdict="passed", computed_ts_ms=T)
    row.update(over)
    return ins(c, "calibration_run", **row)


def calibration_observation(c, ids, **over):
    """The price 15 min before the earliest plausible start (T + 484.996 s), from an early candle."""
    if "early_candle" not in ids:
        ids["early_candle"] = candle(c, ids, interval_start_ms=T - 3_260_000, interval_end_ms=T - 3_200_000,
                                     raw_item=9)
    row = dict(calibration_run_id=ids["cal"], event_id=ids["event"], outcome_id=ids["cle"], offset_s=900,
               close_price_id=ids["hclose"], p_num=1, p_den=2, candle_id=ids["early_candle"],
               cutoff_candle_active=1, quote_age_ms=0, computed_ts_ms=T)
    row.update(over)
    row.setdefault("as_of_ts_ms", T + 484_996 - row["offset_s"] * 1000)
    return ins(c, "calibration_observation", **row)


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
    columns = {r[1] for r in c.execute("PRAGMA table_info(clv_score)")}
    if "ref_entry_reason" in columns:                                                  # from 0002
        row["ref_entry_reason"] = "no_quotes"
    if "agreement_reason" in columns:                                                  # from 0003
        row["agreement_reason"] = "no_reference_at_entry"
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
        "entry_quote_observation": dict(entry_id=entry(conn, ids), concept="decision_quote", price_native="-118",
                                        d_entry_num=109, d_entry_den=59, observed_ts_ms=T - 528_000,
                                        raw_artifact_id=ids["art"], raw_line=1),
    }[table]
    ins(conn, table, **base)                          # the base row is valid
    base.update(over)
    if table == "entry_quote_observation":            # one quote per entry and concept: use a fresh entry,
        base["entry_id"] = entry(conn, ids)           # so only the invalid value can refuse the row
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


def test_0002_keeps_every_valid_0001_row_exactly_and_marks_it_legacy():
    c = db.connect(":memory:")
    db.migrate(c, upto=1)
    ids = seed(c)
    # Valid under 0001 but not under 0002's new constraints: a close with a book time and no
    # tick, a score with p_ref_entry and null EV but no reference book, and one with CLV but
    # no p_close (review of PR #8).
    close(c, ids, "novig", None, book_observed_ts_ms=T - 70_000, close_def_id=new_def(c))
    score(c, ids, "kalshi", p_ref_entry_num=103, p_ref_entry_den=200, null_ev_num=1, null_ev_den=11800)
    score(c, ids, "novig", **fresh_close(c, ids), clv_ev_num=-791, clv_ev_den=11800)
    old = {t: [tuple(r) for r in c.execute(f"SELECT * FROM {t} ORDER BY 1")] for t in ("close_price", "clv_score")}
    assert db.migrate(c) == [2, 3, 4, 5]
    for t, rows in old.items():
        width = len(rows[0])
        assert [tuple(r)[:width] for r in c.execute(f"SELECT {', '.join(cols(c, t)[:width])} FROM {t} ORDER BY 1")] \
            == rows                                                       # the 0001 columns, unchanged
        assert {r[0] for r in c.execute(f"SELECT legacy_0001 FROM {t}")} == {1}
    assert c.execute("SELECT count(*) FROM clv_score WHERE ref_entry_tick_id IS NOT NULL"
                     " OR ref_entry_reason IS NOT NULL").fetchone()[0] == 0           # nothing invented
    assert c.execute("PRAGMA foreign_key_check").fetchall() == []
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        c.execute("DELETE FROM clv_score")
    # New rows meet 0002 and cannot claim to be legacy.
    with pytest.raises(sqlite3.IntegrityError, match="only rows copied from 0001"):
        score(c, ids, "novig", **fresh_close(c, ids), legacy_0001=1)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):            # p_ref_entry without its book
        score(c, ids, "novig", **fresh_close(c, ids), p_ref_entry_num=103, p_ref_entry_den=200,
              null_ev_num=1, null_ev_den=11800, ref_entry_reason=None)
    with pytest.raises(sqlite3.IntegrityError, match="only rows copied from 0001"):
        close(c, ids, "novig", None, book_observed_ts_ms=None, p_close_num=None, p_close_den=None,
              unscoreable_reason="no_quotes", close_def_id=new_def(c), legacy_0001=1)


def fresh_close(c, ids):
    """A close under a new definition, so another score of the same entry and venue is allowed."""
    d = new_def(c)
    return dict(close_def_id=d, close_price_id=close(c, ids, "novig", ids["tick"], close_def_id=d))


def test_the_opposite_quote_is_the_other_side_of_the_decision_quotes_observation(conn):
    ids = seed(conn)
    e2 = entry(conn, ids)
    ins(conn, "entry_quote_observation", entry_id=e2, concept="decision_quote", price_native="-118",
        d_entry_num=109, d_entry_den=59, observed_ts_ms=T - 528_000, raw_artifact_id=ids["art"], raw_line=0)
    good = dict(entry_id=e2, side=0, price_native="100", d_num=2, d_den=1, observed_ts_ms=T - 528_000,
                raw_artifact_id=ids["art"], raw_line=0)
    for bad in (dict(side=1),                       # the entry's own side
                dict(raw_line=1),                   # another frame
                dict(observed_ts_ms=T - 527_000)):  # another observation
        with pytest.raises(sqlite3.IntegrityError, match="not the other side of the decision quote"):
            ins(conn, "entry_opposite_quote", **dict(good, **bad))
    ins(conn, "entry_opposite_quote", **good)
    with pytest.raises(sqlite3.IntegrityError):     # one per entry
        ins(conn, "entry_opposite_quote", **good)


def test_check_2_columns_must_agree(conn):
    ids = seed(conn)
    early = ins(conn, "tick", venue_instrument_id=ids["novig"], source="poll", venue_status="OPEN",
                observed_ts_ms=T - 510_000, bid0_e4=4800, bid1_e4=5150, levels0=0, levels1=0, truncated=0,
                ladder_complete=1, raw_artifact_id=ids["art"], raw_line=2)
    priced = dict(p_ref_entry_num=103, p_ref_entry_den=200, null_ev_num=1, null_ev_den=11800, ref_entry_reason=None,
                  ref_entry_tick_id=early, q_entry_num=118, q_entry_den=227)
    for bad in (dict(agreement_outlier=None, agreement_reason=None),       # neither a flag nor a reason
                dict(agreement_outlier=0, agreement_reason="x"),           # both
                dict(agreement_outlier=2, agreement_reason=None),
                dict(q_entry_num=None, agreement_outlier=0, agreement_reason=None)):
        with pytest.raises(sqlite3.IntegrityError, match="check 2 columns"):
            score(conn, ids, "novig", **fresh_close(conn, ids), **dict(priced, **bad))
    score(conn, ids, "novig", **fresh_close(conn, ids), **dict(priced, agreement_outlier=0, agreement_reason=None))


def cols(c, table):
    """Column names in 0002's order, which keeps 0001's columns first except clv_score's two new ones."""
    names = [r[1] for r in c.execute(f"PRAGMA table_info({table})")]
    return [n for n in names if n not in ("ref_entry_tick_id", "ref_entry_reason")]


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


# -- B1: executions, sightings and candles (0004) -------------------------------------------------------

def test_0004_adds_the_b1_tables_to_a_populated_database():
    c = db.connect(":memory:")
    db.migrate(c, upto=3)
    ids = seed(c)
    before = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in FACTS[:-4] + DERIVED[:-3]}
    assert db.migrate(c, upto=4) == [4]
    assert {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in before} == before
    assert c.execute("PRAGMA foreign_key_check").fetchall() == []
    sighting(c, ids, execution(c, ids))               # and the new tables take rows
    candle(c, ids)


def test_the_same_trade_via_stream_and_csv_twice_changes_neither_count_nor_vwap(conn):
    # DESIGN.md §8.4: one economic execution, its CSV maker row and its stream trade, imported twice.
    ids = seed(conn)
    stream_art = ins(conn, "raw_artifact", source="novig", relpath="novig/2026-10-08/s.0001.jsonl.gz", sha256=H,
                     bytes=10, frame_count=3, recorder_version="0.1.0", redaction_policy="r0-v1", unclean_close=0,
                     parser_version="v0.1", ingested_ts_ms=T)
    stream = dict(source="novig_stream", role="stream", native_trade_id="t1", dual_side_group=None,
                  venue_ts_ms=T - 125_000, observed_ts_ms=T - 125_000, reconcile="rule_matched",
                  qty_native="15625", raw_artifact_id=stream_art, raw_line=7, raw_item=0)
    sighting(conn, ids, ids["exec"], **stream)
    once = (conn.execute("SELECT count(*) FROM trade_execution").fetchone()[0],
            trade_vwap(conn, ids["novig"], 1, T - 600_000, T))
    for again in (lambda: execution(conn, ids),                     # the same key
                  lambda: sighting(conn, ids, ids["exec"]),         # the same CSV row
                  lambda: sighting(conn, ids, ids["exec"], **stream)):  # the same stream frame item
        with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):
            again()
    assert (conn.execute("SELECT count(*) FROM trade_execution").fetchone()[0],
            trade_vwap(conn, ids["novig"], 1, T - 600_000, T)) == once
    assert once[1] == TradeVwap(Fraction(17, 25), 1, 15625, T - 125_000)
    # The stream's time, 5 s before the file's, decides the window.
    assert trade_vwap(conn, ids["novig"], 0, T - 125_000, T - 120_000) == TradeVwap(Fraction(8, 25), 1, 15625, T - 125_000)


@pytest.mark.parametrize("over", [
    dict(execution_id=None),                          # a maker row must name its execution
    dict(role="taker", reconcile="balanced"),         # a taker row names a group, not an execution
    dict(reconcile="balanced"),                       # balanced and ambiguous are taker outcomes
    dict(dual_side_group=None),                       # a CSV row always has its dual-side group
    dict(source="novig_stream", role="stream"),       # a stream record has none
    dict(native_json="not json"),
])
def test_invalid_sightings_are_rejected(conn, over):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError):
        row = dict(dict(execution_id=ids["exec"], raw_item=1), **over)
        sighting(conn, ids, row.pop("execution_id"), **row)


def test_a_taker_row_names_its_group_only(conn):
    ids = seed(conn)
    sighting(conn, ids, None, role="taker", side=0, price_native="0.32", reconcile="balanced", raw_item=1)
    with pytest.raises(sqlite3.IntegrityError, match="another instrument"):
        sighting(conn, ids, ids["exec"], venue_instrument_id=ids["kalshi"], raw_item=2)


@pytest.mark.parametrize("over", [
    dict(price_num=0),
    dict(price_num=25),                               # a price of 1
    dict(payout_cents=0),
])
def test_invalid_executions_are_rejected(conn, over):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError):
        execution(conn, ids, **dict(dict(execution_key="other", raw_item=3), **over))


@pytest.mark.parametrize("over", [
    dict(interval_start_ms=T - 60_000),               # DESIGN.md §8.3: start < end
    dict(interval_start_ms=T - 180_000),              # not its resolution
    dict(bid_close_e4=5300),                          # bid above ask
    dict(ask_close_e4=10_001),
    dict(bid_low_e4=5000, bid_high_e4=4900),
    dict(tier="other"),
])
def test_invalid_candles_are_rejected(conn, over):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError):
        candle(conn, ids, **dict(dict(raw_item=1), **over))


# -- B2: settlements, historical close lineage and calibration runs (0005) ----------------------------

def test_0005_adds_the_b2_tables_to_a_populated_database():
    c = db.connect(":memory:")
    db.migrate(c, upto=4)
    ids = seed(c)
    before = {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in FACTS[:-1] + DERIVED[:-3]}
    old = [tuple(r) for r in c.execute("SELECT * FROM close_price ORDER BY 1")]
    assert db.migrate(c) == [5]
    assert {t: c.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in before} == before
    width = len(old[0])
    assert [tuple(r)[:width] for r in c.execute("SELECT * FROM close_price ORDER BY 1")] == old
    assert c.execute("PRAGMA foreign_key_check").fetchall() == []
    settlement(c, ids)                                # and the new tables take rows
    ids["hdef"] = ins(c, "close_def", name="h", family="close_hist", params_json="{}", params_sha256="b" * 64,
                      created_ts_ms=T)
    ids["hclose"] = hist_close(c, ids)
    ids["cal"] = calibration_run(c, ids)
    calibration_observation(c, ids)


@pytest.mark.parametrize("over", [
    dict(outcome_id=None, event_id=None),             # StatsAPI names its outcome
    dict(source="kalshi_hist", subject="kalshi:market:KX-CLE/yes"),     # a venue names its side, not an outcome
    dict(result="fair_value"),                        # a fair value carries its price
    dict(result="void"),                              # no source writes it yet
    dict(value_native="0.44", value_e4=4400),         # a win has no price
    dict(native_json="not json"),
])
def test_invalid_settlements_are_rejected(conn, over):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError):
        settlement(conn, ids, **dict(dict(raw_item=1), **over))


def test_a_venue_settlement_names_its_instrument_side(conn):
    ids = seed(conn)
    venue = dict(source="kalshi_hist", subject="kalshi:market:KX-CLE/yes", event_id=None, outcome_id=None,
                 venue_instrument_id=ids["kalshi"], side=0)
    settlement(conn, ids, **venue, result="fair_value", value_native="0.4400", value_e4=4400, raw_item=1)
    with pytest.raises(sqlite3.IntegrityError):
        settlement(conn, ids, **dict(venue, side=None), raw_item=2)
    with pytest.raises(sqlite3.IntegrityError, match="not of its event"):
        settlement(conn, ids, outcome_id=ids["cle"], event_id=add_event(conn, "849833"), raw_item=3)


def test_a_superseding_settlement_is_how_an_answer_changes(conn):
    ids = seed(conn)
    regraded = settlement(conn, ids, result="lose", supersedes_id=ids["settle"], raw_line=4)
    assert conn.execute("SELECT supersedes_id FROM settlement_observation WHERE settlement_obs_id = ?",
                        (regraded,)).fetchone()[0] == ids["settle"]
    with pytest.raises(sqlite3.IntegrityError, match="same source and subject"):
        settlement(conn, ids, subject="mlb:game:849832/team:145", outcome_id=ids["cws"], result="lose",
                   supersedes_id=ids["settle"], raw_line=5)
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE"):     # the same record twice
        settlement(conn, ids)


def test_a_historical_close_candle_ends_by_the_cutoff_on_the_reference_venue(conn):
    ids = seed(conn)
    straddling = candle(conn, ids, interval_start_ms=T + 400_000, interval_end_ms=T + 460_000, raw_item=1)
    with pytest.raises(sqlite3.IntegrityError, match="ending by the cutoff"):
        hist_close(conn, ids, candle_id=straddling)
    with pytest.raises(sqlite3.IntegrityError, match="ending by the cutoff"):
        hist_close(conn, ids, ref_venue="novig")     # the seed candle is Kalshi's
    on_cutoff = candle(conn, ids, interval_start_ms=T + 364_996, interval_end_ms=T + 424_996, raw_item=2)
    hist_close(conn, ids, candle_id=on_cutoff, outcome_id=ids["cws"])     # ending exactly at it is before it


@pytest.mark.parametrize("over, match", [
    (dict(candle_id=None, cutoff_candle_active=None), "exactly one"),          # priced, with no lineage
    (dict(cutoff_candle_active=None), "its flag"),
    (dict(candle_id=None, cutoff_candle_active=None, vwap_lo_ms=T, vwap_hi_ms=T + 1), "all four"),
    (dict(vwap_lo_ms=T, vwap_hi_ms=T + 1, vwap_executions=5, vwap_payout_cents=25_000), "exactly one"),
    (dict(candle_id=None, cutoff_candle_active=None, vwap_lo_ms=T + 400_000, vwap_hi_ms=T + 424_997,
          vwap_executions=5, vwap_payout_cents=25_000), "trade window"),     # past the cutoff
    (dict(candle_id=None, cutoff_candle_active=None, book_tick_id="ktick", book_observed_ts_ms=T - 70_000),
     "a tick for close_live"),
])
def test_historical_close_lineage_is_one_candle_or_one_window(conn, over, match):
    ids = seed(conn)
    if over.get("book_tick_id") == "ktick":
        over = dict(over, book_tick_id=ids["ktick"])
    with pytest.raises(sqlite3.IntegrityError, match=match):
        hist_close(conn, ids, outcome_id=ids["cws"], **over)


def test_a_live_close_cites_no_candle_and_an_unscoreable_window_is_kept(conn):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError, match="close_hist"):
        close(conn, ids, "kalshi", None, book_observed_ts_ms=None, candle_id=ids["candle"], cutoff_candle_active=0,
              close_def_id=new_def(conn))
    # Too few trades: no price, but the window and its counts stay as the evidence.
    hist_close(conn, ids, outcome_id=ids["cws"], candle_id=None, cutoff_candle_active=None, p_close_num=None,
               p_close_den=None, unscoreable_reason="few_trades", vwap_lo_ms=T + 400_000 - 600_000,
               vwap_hi_ms=T + 400_000, vwap_executions=2, vwap_payout_cents=9_000)


@pytest.mark.parametrize("over, match", [
    (dict(offset_s=60, candle_id=None, cutoff_candle_active=None, p_num=None, p_den=None,
          reason="no_quotes"), "the offset before its run's close"),                       # at the cutoff, not before it
    (dict(as_of_ts_ms=T - 3_000_000), "the offset before its run's close"),               # not its offset's as-of time
    (dict(candle_id="candle"), "ending by the as-of time"),                     # the candle ends after it
    (dict(p_num=None, p_den=None), "CHECK"),                                    # no price and no reason
    (dict(candle_id=None, cutoff_candle_active=None), "CHECK"),                 # a price without lineage
    (dict(offset_s=0), "CHECK|offset"),
])
def test_invalid_calibration_observations_are_rejected(conn, over, match):
    ids = seed(conn)
    if over.get("candle_id") == "candle":
        over = dict(over, candle_id=ids["candle"])
    with pytest.raises(sqlite3.IntegrityError, match=match):
        calibration_observation(conn, ids, **dict(dict(offset_s=3600), **over))


@pytest.mark.parametrize("over", [
    dict(verdict=None),                               # a check run has a verdict
    dict(mode="dispersion"),                          # a dispersion run has none
    dict(verdict="failed"),                           # a failure says why
    dict(verdict="passed_ish"),
])
def test_invalid_calibration_runs_are_rejected(conn, over):
    ids = seed(conn)
    with pytest.raises(sqlite3.IntegrityError):
        calibration_run(conn, ids, **over)


def test_calibration_results_fit_their_run(conn):
    ids = seed(conn)
    row = dict(calibration_run_id=ids["cal"], offset_s=900, n_events=40, n_rows=40, estimate=0.01,
               computed_ts_ms=T)
    with pytest.raises(sqlite3.IntegrityError, match="dispersion"):
        ins(conn, "calibration_result", kind="dispersion", component="sd_dp", **row)
    dispersion = calibration_run(conn, ids, mode="dispersion", verdict=None)
    ins(conn, "calibration_result", kind="dispersion", component="sd_dp", **dict(row, calibration_run_id=dispersion))
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):                  # an oracle mean names its buffer
        ins(conn, "calibration_result", kind="oracle", component="all:b60", **row)
    ins(conn, "calibration_result", kind="oracle", component="all:b60", buffer_s=60, **row)
    with pytest.raises(sqlite3.IntegrityError, match="CHECK"):                  # an interval needs its se
        ins(conn, "calibration_result", kind="check1", component="slope", lo=-0.1, hi=0.1, **row)
    ins(conn, "calibration_result", kind="check1", component="curvature", outcome="insufficient_data",
        **dict(row, estimate=None))
