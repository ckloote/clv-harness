"""Golden games replayed in CI (DESIGN.md §9.4, §13 Gate V0); see tests/golden.py.

- The committed synthetic archive is exactly what its generator writes.
- From a clean database, raw frames -> facts -> entry -> off resolution ->
  closes -> scores reproduce the committed trace byte for byte, before and
  after the mapping correction.
- The correction leaves the original run intact, and recomputing the original
  run's facts after it reproduces the original trace.
- The synthetic numbers are checked by hand here, independently of the
  committed trace, so regenerating it cannot silently accept a wrong number.
- The recorded 849832 replays to its committed trace where the archive is
  present (its raw frames are not committed).
"""
import json
from fractions import Fraction
from pathlib import Path

import pytest

import golden


@pytest.fixture(scope="module")
def synthetic():
    return golden.replay(golden.SYNTHETIC / "archive", golden.SYNTHETIC / "games.toml", golden.SYNTHETIC,
                         golden.SYN_PK)


def tree(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_the_committed_synthetic_archive_is_what_the_generator_writes(tmp_path):
    golden.generate_synthetic(tmp_path)
    assert tree(tmp_path / "archive") == tree(golden.SYNTHETIC / "archive")
    assert (tmp_path / "games.toml").read_bytes() == (golden.SYNTHETIC / "games.toml").read_bytes()


def test_the_synthetic_game_replays_to_its_committed_trace(synthetic):
    assert golden.expected_text(synthetic) == (golden.SYNTHETIC / "expected.json").read_text()


def test_the_original_run_survives_the_correction(synthetic):
    assert synthetic["before_recomputed"] == synthetic["before"]
    assert synthetic["after"] != synthetic["before"]
    # Every derived row of the original run is still there, unchanged, beside the new run's.
    conn = synthetic["db"]
    assert [r[0] for r in conn.execute("SELECT scoring_run_id FROM scoring_run ORDER BY 1")] == [1, 2, 3]
    assert conn.execute("SELECT count(*) FROM clv_score WHERE scoring_run_id = 1").fetchone()[0] == 8


def q(x):
    return None if x is None else Fraction(x["exact"])


def pick(trace, definition, venue):
    (s,) = [s for s in trace["scores"] if s["definition"] == definition and s["venue"] == venue]
    return s


def test_the_synthetic_numbers_by_hand(synthetic):
    before, after = synthetic["before"], synthetic["after"]
    off = before["off"][0]
    assert (off["earliest"], off["selected"], off["confidence"]) == \
        ("2026-04-01T12:05:00.000Z", "2026-04-01T12:05:30.000Z", "trusted")
    d = Fraction(109, 59)                                       # DraftKings CLE -118
    novig = pick(before, "close_live_mid_depth_500", "novig")
    assert q(novig["d_entry"]) == d and novig["entry"]["decision_quote"]["price_native"] == "-118"
    assert q(novig["p_close"]) == Fraction("0.505")             # the DESIGN.md §2.4 worked fixture
    assert q(novig["clv_ev"]) == Fraction("0.505") * d - 1
    assert q(novig["p_ref_entry"]) == Fraction("0.499")         # the S-1h book: bid VWAP 0.368, ask 0.63
    assert q(novig["null_ev"]) == Fraction("0.499") * d - 1
    assert q(novig["clv_residual"]) == (Fraction("0.505") - Fraction("0.499")) * d
    kalshi = pick(before, "close_live_mid_depth_500", "kalshi")
    # Kalshi CLE: YES bid 0.50; asks 1 - NO bids, $90 a level from 0.52: five levels and $50 of a sixth.
    assert q(kalshi["p_close"]) == (Fraction("0.50") + (90 * Fraction("2.70") + 50 * Fraction("0.57")) / 500) / 2
    closes = {(c["definition"], c["venue"], c["outcome"]): c for c in before["closes"]}
    assert q(closes[("close_live_mid_depth_500", "novig", "CWS")]["p_close"]) == Fraction("0.495")
    assert closes[("close_live_mid_depth_1000", "novig", "CLE")]["unscoreable_reason"] == "insufficient_depth"
    assert closes[("close_live_mid_depth_1000", "kalshi", "CLE")]["unscoreable_reason"] == "truncated_ladder"
    assert closes[("close_live_mid_top", "kalshi", "CWS")]["unscoreable_reason"] == "stale_book"
    # The correction: the DraftKings line verified, the wrong Kalshi ticker rejected.
    assert novig["exclusion_reasons"] == ["mapping_unverified", "equivalence_pending"]
    assert pick(after, "close_live_mid_depth_500", "novig")["exclusion_reasons"] == ["equivalence_pending"]
    assert q(pick(after, "close_live_mid_depth_500", "novig")["clv_ev"]) == q(novig["clv_ev"])
    gone = pick(after, "close_live_mid_depth_500", "kalshi")
    assert (gone["p_close"], gone["clv_ev"], gone["ref_entry_reason"]) == (None, None, "unmapped")
    assert gone["exclusion_reasons"] == ["equivalence_pending", "unmapped"]


def test_every_number_cites_a_hashed_frame(synthetic):
    before = synthetic["before"]
    for s in before["scores"]:
        assert s["entry"]["decision_quote"]["frame"]["sha256"]
        for book in (s["ref_entry_book"],):
            assert book is None or before["books"][book]["frame"]["ref"] == book
    for c in before["closes"]:
        assert c["p_close"] is None or before["books"][c["book"]]["snapshot_frame"] is not None \
            or before["books"][c["book"]]["source"] == "poll"
    assert all(cl["frame"]["sha256"] for cl in before["off"][0]["claims"])


@pytest.mark.skipif(not (golden.REPO / "archive").exists(), reason="the 849832 raw archive is local only")
def test_game_849832_replays_to_its_committed_trace():
    from clv.games import GAMES
    r = golden.replay(golden.REPO / "archive", GAMES, golden.GAME4, 849832, levels=False)
    assert golden.expected_text(r) == (golden.GAME4 / "expected.json").read_text()
    assert r["before_recomputed"] == r["before"]
    doc = json.loads((golden.GAME4 / "expected.json").read_text())
    s = pick(doc["before"], "close_live_mid_depth_500", "novig")
    assert (s["p_close"]["exact"], s["d_entry"]["exact"], s["entry"]["decision_quote"]["price_native"]) == \
        ("207/400", "217/117", "-117")
