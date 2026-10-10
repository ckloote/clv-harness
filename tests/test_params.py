"""config/params.toml mirrors DESIGN.md §10 (DESIGN.md §14).

Every §10 key is present with the same status and revised_by; numeric defaults
match the numbers §10 states; the R0 recorder's copies agree; and loading
rejects malformed entries.
"""
import re
import tomllib
from pathlib import Path

import pytest

from clv.config import PARAMS_PATH, load_params, param
from clv.score.controls import MIN_CLUSTERS

REPO = Path(__file__).resolve().parents[1]
UNIT_S = {"h": 3600, "min": 60}


def design_rows() -> dict[str, tuple[str, str, str]]:
    """§10 table rows: key -> (default, status, revised_by), cell text verbatim."""
    text = (REPO / "DESIGN.md").read_text()
    section = text.split("## 10. Provisional parameters", 1)[1].split("\n## 11.", 1)[0]
    rows = {}
    for line in section.splitlines():
        m = re.match(r"^\| `([a-z0-9_.]+)` \| (.*) \| (fixed|provisional) \| (.*) \|$", line)
        if m:
            rows[m[1]] = (m[2], m[3], m[4])
    assert len(rows) > 50, "the §10 table did not parse"
    return rows


ROWS = design_rows()
PARAMS = load_params()


def stated_numbers(default: str) -> list[float]:
    """Numbers in a §10 default cell, with h/min converted to seconds and % to a fraction.
    Backticked spans are code, not numbers."""
    default = re.sub(r"`[^`]*`", "", default)
    out = []
    for num, suffix in re.findall(r"(\d[\d,]*(?:\.\d+)?)\s*(%|h\b|min\b)?", default):
        x = float(num.replace(",", ""))
        out.append(x / 100 if suffix == "%" else x * UNIT_S.get(suffix, 1))
    return out


def test_every_design_key_is_in_params_and_nothing_else():
    assert set(PARAMS) == set(ROWS)


@pytest.mark.parametrize("key", sorted(ROWS))
def test_status_and_revised_by_match_design(key):
    _, status, revised_by = ROWS[key]
    assert PARAMS[key].status == status
    assert PARAMS[key].revised_by == revised_by


@pytest.mark.parametrize("key", sorted(ROWS))
def test_numeric_default_matches_design(key):
    value = PARAMS[key].value
    stated = stated_numbers(ROWS[key][0])
    if isinstance(value, (bool, str)) or (isinstance(value, list) and all(isinstance(v, str) for v in value)):
        return
    if isinstance(value, (int, float)):
        assert stated and stated[0] == pytest.approx(value), (key, stated)
    elif isinstance(value, list):
        assert stated[: len(value)] == pytest.approx(value), (key, stated)
    elif isinstance(value, dict):          # stream.reconnect_backoff_s
        assert stated[:2] == [value["initial"], value["max"]]
    else:
        pytest.fail(f"{key}: unexpected value type {type(value).__name__}")


def test_min_clusters_matches_code_constant():
    # §10: the code constant is authoritative; the table entry must agree with it.
    assert PARAMS["controls.min_clusters"].value == MIN_CLUSTERS


def test_recorder_copies_match():
    with open(REPO / "tools" / "raw_recorder" / "config.toml", "rb") as f:
        rec = tomllib.load(f)
    for name, value in rec["r0"].items():
        assert param(f"r0.{name}") == value, name
    for name in ("channel_probe_interval_s", "transport_liveness_max_s"):
        assert param(f"stream.{name}") == rec["stream"][name], name
    assert param("stream.reconnect_backoff_s") == rec["stream"]["reconnect_backoff_s"]


def test_unknown_key_is_an_error():
    with pytest.raises(KeyError, match="DESIGN.md §10"):
        param("close.made_up_threshold_s")


@pytest.mark.parametrize("body, match", [
    ('[a.b]\nvalue = 1\nstatus = "fixed"\n', "missing"),
    ('[a.b]\nvalue = 1\nstatus = "tentative"\nrevised_by = "x"\n', "status"),
    ('[a.b]\nvalue = 1\nstatus = "fixed"\nrevised_by = " "\n', "revised_by"),
    ('[a]\nb = 1\n', "expected a table"),
])
def test_malformed_entries_are_rejected(tmp_path, body, match):
    path = tmp_path / "params.toml"
    path.write_text(body)
    with pytest.raises(ValueError, match=match):
        load_params(path)


def test_params_path_is_the_repo_file():
    assert PARAMS_PATH == REPO / "config" / "params.toml"
