"""deploy/: the A1 host's units say what docs/decisions/2026-10-16-a1-host.md decided, and agree
with each other, the runbook (deploy/pi-host.md) and the recorder's config."""
import re
import subprocess
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEPLOY = REPO / "deploy"


def unit(name: str) -> dict[str, list[str]]:
    """Every `Section.Key` of a unit file, with each value it is given, in order."""
    out: dict[str, list[str]] = {}
    section = None
    for line in (DEPLOY / name).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            section = line.strip("[]")
            continue
        key, value = line.split("=", 1)
        out.setdefault(f"{section}.{key}", []).append(value)
    return out


def words(u: dict, key: str) -> list[str]:
    return [w for v in u.get(key, []) for w in v.split()]


def test_the_pi_recorder_waits_for_the_clock_and_the_ssd():
    u = unit("raw-recorder-pi.service")
    assert "time-sync.target" in words(u, "Unit.After") and "time-sync.target" in words(u, "Unit.Wants")
    assert u["Unit.RequiresMountsFor"] == ["/mnt/ssd/archive"]
    assert u["Install.WantedBy"] == ["multi-user.target"]            # a system unit, as the ordering needs
    assert u["Service.User"] == ["recorder"]


def test_the_pi_recorder_runs_the_recorder_from_its_checkout_and_restarts_after_a_kill():
    u = unit("raw-recorder-pi.service")
    checkout = u["Service.WorkingDirectory"][0]
    assert u["Service.ExecStart"] == [f"{checkout}/.venv/bin/raw-recorder run"]
    assert "--package raw-recorder" in u["Service.ExecStartPre"][0]
    assert u["Service.KillSignal"] == ["SIGTERM"]                  # process_stop, then every segment sealed
    assert u["Service.Restart"] == ["on-failure"]                  # the gate's process-kill fault relies on it
    assert u["Unit.StartLimitIntervalSec"] == ["0"]


def test_the_runbook_links_the_checkouts_archive_to_the_mount_the_unit_requires():
    # config.toml's root is the checkout's archive/; on the Pi that must be the SSD.
    u = unit("raw-recorder-pi.service")
    checkout, mount = u["Service.WorkingDirectory"][0], u["Unit.RequiresMountsFor"][0]
    doc = (DEPLOY / "pi-host.md").read_text()
    assert f"ln -s {mount} ~/clv-harness/archive" in doc
    assert checkout == "/home/recorder/clv-harness"
    root = re.search(r'^root = "(.*?)"', (REPO / "tools/raw_recorder/config.toml").read_text(), re.MULTILINE).group(1)
    assert (Path(checkout) / "tools/raw_recorder" / root).resolve() == Path(checkout) / "archive"


def test_the_relay_runs_before_the_recorder_and_is_not_part_of_the_recorder_unit():
    relay, rec = unit("raw-recorder-relay.service"), unit("raw-recorder-pi.service")
    assert relay["Service.ExecStart"] == [rec["Service.WorkingDirectory"][0] + "/.venv/bin/raw-recorder relay"]
    assert "raw-recorder.service" in words(relay, "Unit.Before")
    assert "raw-recorder-relay.service" in words(rec, "Unit.After")
    assert "raw-recorder-relay.service" not in words(rec, "Unit.Wants") + words(rec, "Unit.Requires")
    # Ready once listening, so After= holds the recorder back until the port is open.
    assert relay["Service.Type"] == ["notify"] and relay["Service.NotifyAccess"] == ["main"]


def test_the_workstation_mirror_runs_the_script_every_15_minutes_without_prompting():
    svc, timer = unit("archive-mirror.service"), unit("archive-mirror.timer")
    assert svc["Service.Type"] == ["oneshot"]
    assert svc["Service.ExecStart"] == ["/bin/sh %h/code/clv-harness/deploy/archive-mirror.sh"]
    assert "BatchMode=yes" in svc["Service.Environment"][0]
    assert timer["Timer.OnCalendar"] == ["*:0/15"] and timer["Timer.Persistent"] == ["true"]
    assert timer["Install.WantedBy"] == ["timers.target"]


def test_git_ignores_the_archive_even_as_a_link():
    # On the Pi the checkout's archive/ is a symlink to the SSD; git sees a link as a file.
    for path in ("archive", "archive-paper", "archive-rehearsal"):
        r = subprocess.run(["git", "check-ignore", "-q", "--no-index", path], cwd=REPO, check=False)
        assert r.returncode == 0, path



REHEARSAL = "--config /home/recorder/clv-harness/tools/raw_recorder/config.paper-rehearsal.toml"


def differences(a: dict, b: dict) -> set[str]:
    return {k for k in a.keys() | b.keys() if a.get(k) != b.get(k)}


def test_the_rehearsal_recorder_is_the_pi_recorder_on_the_paper_rehearsal_config():
    # deploy/pi-host.md section 9: everything that protects the Production run protects the rehearsal.
    prod, paper = unit("raw-recorder-pi.service"), unit("raw-recorder-paper.service")
    assert differences(prod, paper) == {"Unit.Description", "Unit.After", "Unit.RequiresMountsFor", "Service.ExecStart"}
    assert paper["Service.ExecStart"] == [prod["Service.ExecStart"][0].replace("raw-recorder run", f"raw-recorder {REHEARSAL} run")]
    assert paper["Unit.RequiresMountsFor"] == ["/mnt/ssd/archive-paper"]
    assert words(paper, "Unit.After") == [w.replace("raw-recorder-relay", "raw-recorder-paper-relay")
                                          for w in words(prod, "Unit.After")]


def test_the_rehearsal_relay_is_the_pi_relay_on_the_paper_rehearsal_config():
    prod, paper = unit("raw-recorder-relay.service"), unit("raw-recorder-paper-relay.service")
    assert differences(prod, paper) == {"Unit.Description", "Unit.Before", "Service.ExecStart"}
    assert paper["Service.ExecStart"] == [prod["Service.ExecStart"][0].replace("raw-recorder relay", f"raw-recorder {REHEARSAL} relay")]
    assert paper["Unit.Before"] == ["raw-recorder-paper.service"]


def test_the_rehearsal_config_and_archive_exist_where_the_units_and_runbook_say():
    assert (REPO / REHEARSAL.split()[1].replace("/home/recorder/clv-harness/", "")).exists()
    doc = (DEPLOY / "pi-host.md").read_text()
    assert "ln -s /mnt/ssd/archive-paper ~/clv-harness/archive-paper" in doc
    assert "rrsync -ro /mnt/ssd/archive-paper" in doc


def test_the_gate_drop_in_runs_the_pi_recorder_on_the_gate_config_with_its_relay():
    # docs/decisions/2026-10-17-a1-gate-plan.md: only the config changes; the unit keeps its clock
    # wait, mount and restart policy, and pulls in the relay it is already ordered after.
    gate, prod = unit("raw-recorder-gate.conf"), unit("raw-recorder-pi.service")
    gate_cfg = "--config /home/recorder/clv-harness/tools/raw_recorder/config.gate.toml"
    assert gate["Service.ExecStart"] == ["", prod["Service.ExecStart"][0].replace("raw-recorder run",
                                                                                    f"raw-recorder {gate_cfg} run")]
    assert set(gate) == {"Unit.Wants", "Service.ExecStart"}
    assert gate["Unit.Wants"] == ["raw-recorder-relay.service"]
    assert "raw-recorder-relay.service" in words(prod, "Unit.After")
    assert (REPO / gate_cfg.split()[1].replace("/home/recorder/clv-harness/", "")).exists()
