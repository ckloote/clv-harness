"""UTC time at the edges: the harness stores integer UTC milliseconds (`_ms`, DESIGN.md §8.3)."""
from __future__ import annotations

from datetime import datetime, timezone


def iso_ms(text: str) -> int:
    """ISO 8601 (with `Z` or an offset; milliseconds optional) -> UTC ms."""
    return round(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def ms_iso(ms: int) -> str:
    """UTC ms -> `YYYY-MM-DDTHH:MM:SS.mmmZ`."""
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
