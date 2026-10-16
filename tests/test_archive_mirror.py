"""deploy/archive-mirror.sh: the workstation mirror copies sealed segments and journals, never
an active part, and publishes the recorder's seal watermark only after the data it vouches for."""
import json
import os
import shutil
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
    env = dict(os.environ, ARCHIVE_MIRROR_SRC=str(src), ARCHIVE_MIRROR_DST=str(dst))
    subprocess.run([str(SCRIPT)], env=env, check=True)
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
