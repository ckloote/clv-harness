"""Recorder session identity, the archive lock, and host evidence.

Every process start gets a new session ID. A killed process cannot emit its
own stop event, so the next session records the previous one as unclean
(DESIGN.md §7.1) after sealing its orphaned segments.
"""
from __future__ import annotations

import fcntl
import os
import shutil
import socket
import subprocess
import uuid
from pathlib import Path

LOCK_NAME = ".recorder.lock"


def new_session_id() -> str:
    return uuid.uuid4().hex


class ArchiveLock:
    """Exclusive flock on <root>/.recorder.lock holding the owner's PID.

    Two writers on one archive would seal each other's active segments, so
    `run` holds this for its lifetime and `seal` refuses to touch `.part`
    files while it is held.
    """

    def __init__(self, root: Path):
        self.path = Path(root) / LOCK_NAME
        self._fd: int | None = None

    def acquire(self) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False
        os.ftruncate(fd, 0)
        os.write(fd, str(os.getpid()).encode())
        self._fd = fd
        return True

    def holder_pid(self) -> int | None:
        try:
            return int(self.path.read_text().strip())
        except (OSError, ValueError):
            return None

    def release(self) -> None:
        if self._fd is not None:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
            os.close(self._fd)
            self._fd = None


def clock_status() -> dict:
    """Best-effort NTP/clock evidence from the host (§12 time health)."""
    for cmd in (["chronyc", "tracking"], ["timedatectl", "timesync-status"],
                ["timedatectl", "show", "--all"]):
        if shutil.which(cmd[0]) is None:
            continue
        try:
            out = subprocess.run(cmd, capture_output=True, text=True, timeout=3)
        except (OSError, subprocess.TimeoutExpired) as exc:
            return {"tool": " ".join(cmd), "error": repr(exc)}
        if out.returncode == 0:
            return {"tool": " ".join(cmd), "output": out.stdout}
    return {"tool": None, "error": "no clock status tool available"}


def host_info() -> dict:
    return {"hostname": socket.gethostname(), "pid": os.getpid()}


def free_disk_mb(path: Path) -> int:
    return shutil.disk_usage(path).free // (1024 * 1024)


class SleepInhibitor:
    """Holds a systemd sleep/idle inhibitor while capture windows are open.

    Runs `systemd-inhibit ... sleep infinity` as a child; killing it releases
    the lock. Failure to inhibit is reported, never fatal: a suspended host
    produces a recorded gap, not corrupt evidence.
    """

    def __init__(self) -> None:
        self._proc: subprocess.Popen | None = None

    @property
    def held(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def acquire(self) -> str | None:
        """Returns an error description, or None on success."""
        if self.held:
            return None
        if shutil.which("systemd-inhibit") is None:
            return "systemd-inhibit not found"
        try:
            self._proc = subprocess.Popen(
                ["systemd-inhibit", "--what=sleep:idle", "--who=raw-recorder",
                 "--why=Recording a capture window", "--mode=block", "sleep", "infinity"],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
            )
        except OSError as exc:
            return repr(exc)
        try:
            self._proc.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            return None  # still running: inhibitor held
        err = self._proc.stderr.read().decode(errors="replace") if self._proc.stderr else ""
        self._proc = None
        return f"systemd-inhibit exited: {err.strip()}"

    def release(self) -> None:
        if self._proc is not None:
            self._proc.terminate()
            try:
                self._proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._proc.kill()
            self._proc = None
