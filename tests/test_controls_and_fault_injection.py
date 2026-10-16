"""Calibration runs (clv.score.calibration) on a synthetic database, with injected faults
(DESIGN.md §9.4): a correct pipeline passes check 1, a close-leg polarity flip and a level
drift fail it, a family below the sample target is `insufficient_data`, the oracle's matched
subset drops events missing at any buffer, and a run over a fact snapshot's replica
reproduces it exactly.

Each game has a Kalshi market per team (YES = that team) with 1-minute candles: one 30 s
before each offset, and the close candle 650 s before the first pitch, so every buffer in
close.buffer_sweep_s sees it. The home market's mid follows a martingale with small steps;
the away market is its complement.
"""
import json
from dataclasses import dataclass, field

import numpy as np
import pytest

from clv import db
from clv.score import calibration

START = 1_791_504_000_000
H = "a" * 64
OFFSETS = (86_400, 21_600, 3_600, 900)
N_GAMES = 200


@dataclass
class Faults:
    level_drift: float = 0.0              # added to every close
    close_flip: bool = False              # the close candle quotes the complement
    late_close: set = field(default_factory=set)      # games whose close candle is at -300 s and with no 15 m candle


def ins(c, table, **row):
    return c.execute(f"INSERT INTO {table} ({', '.join(row)}) VALUES ({', '.join('?' * len(row))})",
                     tuple(row.values())).lastrowid


def build(n=N_GAMES, faults=Faults(), seed=1):
    c = db.connect(":memory:")
    db.migrate(c)
    rng = np.random.default_rng(seed)
    art = ins(c, "raw_artifact", source="kalshi_hist", relpath="kalshi_hist/x.jsonl.gz", sha256=H, bytes=1,
              frame_count=1, recorder_version="0", redaction_policy="r", unclean_close=0, parser_version="v",
              ingested_ts_ms=START)
    cite = dict(raw_artifact_id=art, raw_line=0)
    item = iter(range(10**9))
    for g in range(n):
        start = START + g * 86_400_000
        event = ins(c, "event", league="MLB", league_game_id=str(1000 + g), created_ts_ms=START)
        ins(c, "event_alias", event_id=event, provider="mlb_statsapi", provider_event_id=str(1000 + g), status="exact",
            method="gamePk", evidence="e", observed_ts_ms=START, detail_json=json.dumps({"home": "HOM"}), **cite)
        home = ins(c, "outcome", event_id=event, kind="team_wins", team_id=1, team_abbreviation="HOM", description="h")
        away = ins(c, "outcome", event_id=event, kind="team_wins", team_id=2, team_abbreviation="AWY", description="a")
        ins(c, "off_observation", event_id=event, source="mlb_statsapi", kind="first_pitch", subject=f"mlb:game:{g}",
            detected_off_ts_ms=start, observed_ts_ms=start + 3_600_000, **cite)
        # The home side's path: a martingale from 24 h to the close.
        path = [rng.uniform(0.30, 0.70)]
        for _ in OFFSETS:
            path.append(path[-1] + rng.normal(0, 0.002))
        p_close = path[-1] + faults.level_drift
        if faults.close_flip:
            p_close = 1 - p_close
        home_wins = rng.random() < path[-1]
        for team, outcome, flip in (("HOM", home, False), ("AWY", away, True)):
            iid = ins(c, "venue_instrument", venue="kalshi", native_id=f"KX-{g}-{team}", native_event_id=f"KX-{g}",
                      kind="exchange_book", side0_id="yes", side0_label="YES", side1_id="no", side1_label="NO",
                      payout_currency="USD", payout_cents_per_contract=100, first_seen_ts_ms=START, **cite)
            for side, polarity in ((0, "direct"), (1, "complement")):
                ins(c, "instrument_mapping_observation", venue_instrument_id=iid, side=side, outcome_id=outcome,
                    polarity=polarity, mapping_status="manual_verified", settlement_equivalence="pending",
                    method="m", evidence="e", observed_ts_ms=START)
            mid = (lambda p: 1 - p) if flip else (lambda p: p)
            candles = [(start - k * 1000 - 30_000, path[i]) for i, k in enumerate(OFFSETS)
                       if not (g in faults.late_close and k == 900)]
            candles.append((start - (300_000 if g in faults.late_close else 650_000), p_close))
            for end, p in candles:
                e4 = round(mid(p) * 10_000)
                ins(c, "historical_candle", venue_instrument_id=iid, source="kalshi_hist", tier="historical",
                    resolution_s=60, interval_start_ms=end - 60_000, interval_end_ms=end, side=0,
                    bid_close_e4=e4 - 50, ask_close_e4=e4 + 50, native_json="{}", observed_ts_ms=START,
                    raw_artifact_id=art, raw_line=1, raw_item=next(item))
            won = home_wins != flip
            ins(c, "settlement_observation", source="kalshi_hist", subject=f"kalshi:market:KX-{g}-{team}/yes",
                venue_instrument_id=iid, side=0, result="win" if won else "lose", observed_ts_ms=start + 9e6,
                native_json="{}", raw_artifact_id=art, raw_line=2, raw_item=next(item))
            ins(c, "settlement_observation", source="mlb_statsapi", subject=f"mlb:game:{g}/team:{team}",
                event_id=event, outcome_id=outcome, result="win" if won else "lose", observed_ts_ms=start + 9e6,
                native_json="{}", raw_artifact_id=art, raw_line=3, raw_item=next(item))
    return c


@pytest.fixture
def target(monkeypatch):
    """controls.sample_events, lowered to what the synthetic sample can reach."""
    real = calibration.param

    def set_target(n):
        monkeypatch.setattr(calibration, "param", lambda k: n if k == "controls.sample_events" else real(k))
    return set_target


CANDLE = (("kalshi", "candle_mid_top"),)


def results(c, run_id, kind):
    return {(r[0], r[1]): r[2:] for r in c.execute(
        "SELECT offset_s, component, estimate, lo, hi, outcome, n_events FROM calibration_result"
        " WHERE calibration_run_id = ? AND kind = ? ORDER BY offset_s, component", (run_id, kind))}


def test_a_correct_pipeline_passes_check_1(target):
    target(150)
    c = build()
    snap, (run,) = calibration.run(c, START, families=CANDLE)
    assert (run.verdict, run.verdict_reason) == ("passed", None)
    assert run.n_by_offset == {k: N_GAMES for k in OFFSETS}
    check1 = results(c, run.calibration_run_id, "check1")
    assert {r[3] for r in check1.values()} == {"passed"} and len(check1) == 12
    bins = results(c, run.calibration_run_id, "bin")
    assert len(bins) == 4 * 5 and {r[3] for r in bins.values()} == {"not_flagged"}
    # Every event has its closes at every buffer and its observations at every offset, both outcomes.
    assert c.execute("SELECT count(*) FROM close_price WHERE fact_snapshot_id = ?", (snap,)).fetchone()[0] \
        == N_GAMES * 2 * 5
    assert c.execute("SELECT count(*) FROM calibration_observation WHERE calibration_run_id = ? AND p_num IS NOT NULL",
                     (run.calibration_run_id,)).fetchone()[0] == N_GAMES * 2 * 4
    # The close is as of the candle 650 s before the start: no candle contains any cutoff.
    assert [tuple(r) for r in c.execute("SELECT DISTINCT cutoff_candle_active FROM close_price"
                                        " WHERE fact_snapshot_id = ?", (snap,))] == [(0,)]


def test_a_close_leg_polarity_flip_fails_check_1_with_a_slope_near_minus_two(target):
    target(150)
    c = build(faults=Faults(close_flip=True))
    _, (run,) = calibration.run(c, START, families=CANDLE)
    assert run.verdict == "failed"
    slopes = [v for (k, comp), v in results(c, run.calibration_run_id, "check1").items() if comp == "slope"]
    assert all(abs(s[0] + 2) < 0.05 and s[3] == "failed" for s in slopes)


def test_a_level_drift_fails_check_1_through_the_intercept(target):
    target(150)
    c = build(faults=Faults(level_drift=0.02))
    _, (run,) = calibration.run(c, START, families=CANDLE)
    assert run.verdict == "failed"
    check1 = results(c, run.calibration_run_id, "check1")
    assert all(check1[(k, "intercept")][3] == "failed" and abs(check1[(k, "intercept")][0] - 0.02) < 0.002
               for k in OFFSETS)
    assert all(check1[(k, "slope")][3] == "passed" for k in OFFSETS)


def test_below_the_sample_target_is_insufficient_data_never_passed():
    # The real target, 1,500 events, against 200 games: the statistics are kept as diagnostics.
    c = build()
    _, (run,) = calibration.run(c, START, families=CANDLE)
    assert run.verdict == "insufficient_data"
    assert "fewer than controls.sample_events = 1500" in run.verdict_reason and "would say passed" in run.verdict_reason
    detail = json.loads(c.execute("SELECT detail_json FROM calibration_result WHERE calibration_run_id = ?"
                                  " AND kind = 'check1' LIMIT 1", (run.calibration_run_id,)).fetchone()[0])
    assert detail["below_sample_target"] is True


def test_a_family_without_data_is_insufficient_data(target):
    # No trades and no Novig markets: both VWAP families are empty at every offset.
    target(30)
    c = build(n=40)
    _, runs = calibration.run(c, START)
    by = {(r.venue, r.method): r for r in runs}
    assert by[("kalshi", "candle_mid_top")].verdict == "passed"
    for key in (("kalshi", "trade_vwap"), ("novig", "trade_vwap")):
        assert by[key].verdict == "insufficient_data" and by[key].n_by_offset == {k: 0 for k in OFFSETS}
    reasons = dict(c.execute("SELECT unscoreable_reason, count(*) FROM close_price c JOIN calibration_run r"
                             " ON r.fact_snapshot_id = c.fact_snapshot_id AND r.ref_venue = c.ref_venue"
                             " WHERE r.calibration_run_id = ? AND c.close_def_id IN"
                             " (SELECT close_def_id FROM close_def WHERE json_extract(params_json, '$.method')"
                             " = 'trade_vwap') GROUP BY 1", (by[("novig", "trade_vwap")].calibration_run_id,)))
    assert reasons == {"unmapped": 40 * 2 * 5}


def test_the_oracle_matched_subset_drops_events_missing_at_any_buffer(target):
    # Ten games close late (-300 s) with no 15 m candle: at the 600 s buffer their latest candle
    # is the 1 h one, older than close.max_quote_age_s, so they leave the matched subset.
    target(150)
    late = set(range(10))
    c = build(faults=Faults(late_close=late))
    _, (run,) = calibration.run(c, START, families=CANDLE)
    oracle = results(c, run.calibration_run_id, "oracle")
    n = {comp: r[4] for (k, comp), r in oracle.items() if k == 3_600}
    assert n["all:b60"] == N_GAMES and n["all:b600"] == N_GAMES - 10
    assert {n[f"matched:b{b}"] for b in (0, 30, 60, 120, 600)} == {N_GAMES - 10}
    # At 15 m the late games have no entry price at all.
    assert {r[4] for (k, comp), r in oracle.items() if k == 900} == {N_GAMES - 10}
    detail = json.loads(c.execute("SELECT detail_json FROM calibration_result WHERE calibration_run_id = ?"
                                  " AND component = 'all:b60' AND offset_s = 3600",
                                  (run.calibration_run_id,)).fetchone()[0])
    assert detail == {"excluded": {}}


def test_a_dispersion_run_fits_nothing_and_simulates_item_12(target):
    target(150)
    c = build()
    _, (run,) = calibration.run(c, START, mode="dispersion", families=CANDLE, replicates=20)
    assert run.verdict is None
    kinds = {r[0] for r in c.execute("SELECT kind FROM calibration_result WHERE calibration_run_id = ?",
                                     (run.calibration_run_id,))}
    assert kinds == {"dispersion"}
    d = results(c, run.calibration_run_id, "dispersion")
    assert all(0.001 < d[(k, "sd_dp")][0] < 0.01 for k in OFFSETS)       # the injected steps are 0.2 pp
    oc = {comp: r[0] for (k, comp), r in d.items() if k == 0}
    assert set(oc) == {"oc:correct_pipeline_passes", "oc:level_drift_fails", "oc:curvature_drift_fails"}
    assert all(0 <= v <= 1 for v in oc.values())


def test_a_rerun_over_the_same_facts_reproduces_the_run(target):
    target(150)
    c = build(n=60)
    snap, first = calibration.run(c, START, families=CANDLE)
    again, second = calibration.run(c, START + 1, snapshot_id=snap, families=CANDLE)
    assert again != snap

    def rows(run_id):
        return c.execute("SELECT kind, offset_s, component, buffer_s, n_events, estimate, se, outcome, detail_json"
                         " FROM calibration_result WHERE calibration_run_id = ? ORDER BY 1, 2, 3",
                         (run_id,)).fetchall()
    assert rows(first[0].calibration_run_id) == rows(second[0].calibration_run_id)
    assert (first[0].verdict, first[0].verdict_reason) == (second[0].verdict, second[0].verdict_reason)


def test_oracle_mean_is_event_clustered_and_needs_enough_events():
    from clv.score import controls
    v = np.r_[np.full(40, 0.01), np.full(40, 0.03)]
    est = controls.oracle_mean(v, np.arange(80))
    assert abs(est.value - 0.02) < 1e-12 and est.lo < 0.02 < est.hi
    # Repeating an event's row does not shrink its interval as if it were a new event.
    dup = controls.oracle_mean(np.r_[v, v], np.r_[np.arange(80), np.arange(80)])
    assert abs(dup.value - est.value) < 1e-12 and dup.se > 0.9 * est.se
    with pytest.raises(controls.InsufficientCalibrationData):
        controls.oracle_mean(v[:20], np.arange(20))


def test_the_report_rebuilds_from_the_database(target, tmp_path):
    target(150)
    c = build(faults=Faults(late_close={0}))
    snap, _ = calibration.run(c, START, families=CANDLE)
    md, games = calibration.report(c, snap, tmp_path)
    text = md.read_text()
    assert "**Verdict:** passed" in text and "| 15m | 199 |" in text
    assert "selected candles ending after the earliest plausible start (must be 0) | 0 |" in text
    assert "Excluded from the oracle" not in text                 # every winner is settled
    assert games.read_text().count("\n") == 1 + N_GAMES * 2
    again, _ = calibration.report(c, snap, tmp_path / "again")
    assert again.read_text() == text
    _, (disp,) = calibration.run(c, START + 1, mode="dispersion", snapshot_id=snap, families=CANDLE, replicates=10)
    assert "§9.4 item 12 under the measured series (10 replicates" in calibration.report(c, snap + 1, tmp_path)[0] \
        .read_text()
