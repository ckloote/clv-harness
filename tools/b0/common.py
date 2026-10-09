"""Shared paths, sample definition and HTTP helpers for the B0 scripts (docs/feasibility.md).

Everything fetched is cached under the work directory (default `b0-work/`, git-ignored),
so reruns are offline and the inputs keep the sha256 recorded in docs/feasibility.md.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
UA = {"User-Agent": "clv-harness-b0/0.1", "Accept": "application/json"}

NOVIG_DATA = "https://data.novig.com/reporting/trade-data"
NOVIG_API = "https://api.novig.com"
KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
STATSAPI = "https://statsapi.mlb.com/api/v1"

# The B0 sample (docs/feasibility.md §1). Novig files are per Eastern day.
SCHEDULE_RANGE = ("2026-08-03", "2026-10-08")
SAMPLE_DATE = "2026-08-04"                      # every game that day
EDGE_GAMES = (824785, 824784, 824703, 823491, 823489, 824706, 823490, 824705, 849832)
NOVIG_DAYS = ("2026-08-04", "2026-09-22", "2026-09-23", "2026-09-25", "2026-09-26",
              "2026-09-27", "2026-10-08")

# Kalshi edge games, mapped by hand from settlement result and close_time against StatsAPI.
# Ticker date and time do not identify the game (docs/feasibility.md §4).
KALSHI_MANUAL = {
    824785: "KXMLBGAME-26SEP221835TORBAL",    # postponed 09-22, settled on the 09-23 game 1
    824784: "KXMLBGAME-26SEP231835TORBAL",
    824703: "KXMLBGAME-26SEP251305CHCBOSG1",
    824706: "KXMLBGAME-26SEP251910CHCBOS",
    823491: "KXMLBGAME-26SEP261915BALNYY",    # 09-26 ticker settled on the 09-25 game 1
    823489: "KXMLBGAME-26SEP251905BALNYY",
    823490: "KXMLBGAME-26SEP271520BALNYY",    # cancelled: scalar settlement
    824705: "KXMLBGAME-26SEP261915CHCBOS",    # 09-26 ticker settled on the 09-27 game
    849832: "KXMLBGAME-26OCT081700CLECWS",
}

# Novig still uses pre-2025 names and its own codes.
NOVIG_NAME = {"Athletics": "Oakland Athletics"}
NOVIG_ABBR = {"KC": "KAN", "AZ": "ARI", "WSH": "WAS", "ATH": "OAK"}

# DESIGN.md §10 values the coverage table applies.
CLOSE_BUFFER_S = 60                 # close.buffer_s
VWAP_WINDOW_S = 600                 # close.hist_vwap_window_s
VWAP_MIN_EXECUTIONS = 5             # close.hist_vwap_min_executions
VWAP_MIN_NOTIONAL_USD = 250         # close.hist_vwap_min_notional_usd (read as payout USD)


def work_dir(argv: list[str] | None = None, description: str = "") -> tuple[Path, argparse.Namespace]:
    ap = argparse.ArgumentParser(description=description)
    ap.add_argument("--work", type=Path, default=REPO / "b0-work", help="cache and output directory")
    args = ap.parse_args(argv)
    args.work.mkdir(parents=True, exist_ok=True)
    return args.work, args


def http_get(url: str, dest: Path | None = None, timeout: float = 240) -> bytes:
    """GET with a cache file: an existing dest is returned unchanged."""
    if dest is not None and dest.exists() and dest.stat().st_size:
        return dest.read_bytes()
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
    if dest is not None:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
    time.sleep(0.15)
    return body


def get_json(url: str, dest: Path | None = None) -> dict:
    try:
        return json.loads(http_get(url, dest, timeout=30))
    except urllib.error.HTTPError as exc:
        body = {"_error": exc.code, "_body": exc.read()[:300].decode("utf-8", "replace")}
        if dest is not None:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(json.dumps(body))
        return body


def kalshi(work: Path, path: str) -> dict:
    """Cached Kalshi public GET."""
    name = re.sub(r"[^A-Za-z0-9]+", "_", path)[:180]
    return get_json(KALSHI + path, work / "kalshi" / f"cache_{name}.json")


class NovigHistory:
    """Signed GETs to /v3/history with the recorder's trading::read key.

    Uses aiohttp: Novig's edge refuses Python's urllib with an HTML 403.
    Reads NOVIG_READ_KEY_ID / NOVIG_READ_KEY_PATH from the environment, falling back
    to ~/.config/raw-recorder/env.
    """

    def __init__(self) -> None:
        from raw_recorder.novig.signing import NovigSigner
        env = dict(os.environ)
        cfg = Path.home() / ".config/raw-recorder/env"
        if "NOVIG_READ_KEY_ID" not in env and cfg.exists():
            for line in cfg.read_text().splitlines():
                if line.strip().startswith("NOVIG_READ_KEY") and "=" in line:
                    k, v = line.strip().split("=", 1)
                    env[k] = v
        self.signer = NovigSigner.from_pem_file(env["NOVIG_READ_KEY_ID"],
                                                Path(os.path.expanduser(env["NOVIG_READ_KEY_PATH"])))
        self.loop = asyncio.new_event_loop()
        self.http = None

    def get(self, path: str) -> tuple[int, bytes]:
        return self.loop.run_until_complete(self._get(path))

    async def _get(self, path: str) -> tuple[int, bytes]:
        import aiohttp
        if self.http is None:
            self.http = aiohttp.ClientSession()
        async with self.http.get(NOVIG_API + path, headers=self.signer.headers("GET", path, "", b""),
                                 timeout=aiohttp.ClientTimeout(total=15)) as resp:
            return resp.status, await resp.read()

    def close(self) -> None:
        if self.http is not None:
            self.loop.run_until_complete(self.http.close())
        self.loop.close()
