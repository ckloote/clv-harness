"""Recorder tests never depend on the host's clock synchronization: a test that wants an
unsynchronized clock sets `raw_recorder.runtime.clock_synced` itself."""
import pytest

from raw_recorder import runtime


@pytest.fixture(autouse=True)
def synced_clock(monkeypatch):
    monkeypatch.setattr(runtime, "clock_synced", lambda: True)
