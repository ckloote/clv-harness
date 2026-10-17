"""deploy/archive-mirror.sh: the workstation mirror copies sealed segments and journals, never
an active part, and publishes the recorder's seal watermark only after the data it vouches for."""
import json
import os
import shutil
import socket
import subprocess
from pathlib import Path

import pytest
from raw_recorder.archive import WATERMARK as RECORDER_WATERMARK
from raw_recorder.archive import ArchiveWriter

from clv import archive

SCRIPT = Path(__file__).resolve().parents[1] / "deploy" / "archive-mirror.sh"
T0 = 1_791_400_000_000


class Clock:
    t = T0

    def __call__(self):
        return self.t


def test_the_recorder_and_harness_name_the_same_watermark():
    assert archive.WATERMARK == RECORDER_WATERMARK


@pytest.mark.skipif(shutil.which("rsync") is None, reason="rsync is not installed")
def test_the_mirror_copies_sealed_data_and_publishes_the_watermark_last(tmp_path):
    src, dst = tmp_path / "pi", tmp_path / "mirror"
    clock = Clock()
    w = ArchiveWriter(src, session_id="s1", segment_max_s=10, fsync_interval_s=5, clock=clock)
    done, live = w.stream("novig", "done"), w.stream("novig", "live")
    done.write("in", "done", "sealed frame")
    w.release(done)                                  # sealed
    w.drain()
    clock.t = T0 + 5_000
    live.write("in", "live", "still being written")  # an active .part
    w.write_watermark()
    (src / ".recorder.lock").write_text("123")
    control = socket.socket(socket.AF_UNIX)          # the fault harness's socket (Gate A1)
    control.bind(str(src / ".recorder.control"))
    env = dict(os.environ, ARCHIVE_MIRROR_SRC=str(src), ARCHIVE_MIRROR_DST=str(dst))
    run = subprocess.run([str(SCRIPT)], env=env, check=True, capture_output=True, text=True)
    control.close()
    assert "skipping" not in run.stderr + run.stdout
    assert not (dst / ".recorder.control").exists()
    copied = sorted(p.relative_to(dst).as_posix() for p in dst.rglob("*") if p.is_file())
    day = "novig/2026-10-07"
    assert copied == [f"{day}/done.0001.jsonl.gz", f"{day}/manifest.jsonl", "sealed_through.json"]
    assert archive.sealed_through(dst) == T0 + 5_000 == archive.sealed_through(src)
    assert json.loads((dst / "sealed_through.json").read_text())["session_id"] == "s1"
    w.close()


@pytest.mark.skipif(shutil.which("rsync") is None, reason="rsync is not installed")
def test_a_failed_run_publishes_no_new_watermark(tmp_path):
    src, dst = tmp_path / "pi", tmp_path / "mirror"
    dst.mkdir()
    (dst / "sealed_through.json").write_text(json.dumps({"sealed_through_ms": 1}))
    src.mkdir()                                      # no watermark at the source: rsync fails first
    env = dict(os.environ, ARCHIVE_MIRROR_SRC=str(src), ARCHIVE_MIRROR_DST=str(dst))
    assert subprocess.run([str(SCRIPT)], env=env, capture_output=True, check=False).returncode != 0
    assert archive.sealed_through(dst) == 1


@pytest.mark.skipif(shutil.which("rsync") is None or shutil.which("rrsync") is None,
                    reason="rsync or rrsync is not installed")
def test_the_mirror_runs_through_a_read_only_rrsync_key(tmp_path):
    # deploy/pi-host.md: the Pi forces the workstation's key to `rrsync -ro <archive root>`, so
    # ARCHIVE_MIRROR_SRC is just the host alias and paths are relative to the root. An ssh stand-in
    # runs the forced command as sshd would, with the client's command in SSH_ORIGINAL_COMMAND.
    src, dst = tmp_path / "pi", tmp_path / "mirror"
    clock = Clock()
    w = ArchiveWriter(src, session_id="s1", segment_max_s=10, fsync_interval_s=5, clock=clock)
    done = w.stream("novig", "done")
    done.write("in", "done", "sealed frame")
    w.release(done)
    w.drain()
    w.write_watermark()
    ssh = tmp_path / "ssh"
    ssh.write_text('#!/bin/sh\nwhile [ "${1#-}" != "$1" ]; do shift; done\nshift\n'
                   f'SSH_ORIGINAL_COMMAND="$*" exec rrsync -ro {src}\n')
    ssh.chmod(0o755)
    env = dict(os.environ, RSYNC_RSH=str(ssh), ARCHIVE_MIRROR_SRC="pi-archive:", ARCHIVE_MIRROR_DST=str(dst))
    subprocess.run([str(SCRIPT)], env=env, check=True, capture_output=True, text=True)
    assert (dst / "novig/2026-10-07/done.0001.jsonl.gz").exists()
    assert archive.sealed_through(dst) == archive.sealed_through(src)
    # The key can read the archive, never write to it.
    (tmp_path / "stray").write_text("x")
    push = subprocess.run(["rsync", str(tmp_path / "stray"), "pi-archive:"], env=env, capture_output=True, check=False)
    assert push.returncode != 0 and not (src / "stray").exists()
    w.close()
