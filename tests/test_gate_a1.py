"""Gate A1 end to end (clv.gate.a1): the real recorder (`raw-recorder run`, in a subprocess),
through the fault relay, against a fake Novig with a live book and trades. Every fault kind
is injected once by the harness, the process kill by a real SIGKILL and a restart. The
archive is ingested as a league event, and the gate must pass. Then the gate must fail
where the facts are wrong: a gap missing, opened late, or closed without a snapshot; a
fault with no result; too few faults; an archive not yet sealed.

The fake venue keeps one book and one trade tape per market, and a seq per connection,
market and channel, as Novig does: a book change every 0.1 s, a trade every 0.5 s."""
import asyncio
import json
import os
import signal
import sqlite3
import sys
import time
from pathlib import Path

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from raw_recorder import faults

from clv import db
from clv.archive import sealed_through
from clv.gate import a1 as gate
from clv.ingest import ingest_league_event
from clv.timeutil import ms_iso

REPO = Path(__file__).resolve().parents[1]
VECTORS = REPO / "tools" / "raw_recorder" / "tests" / "fixtures" / "novig_signature_vectors.json"
EVENT, MARKET, A, B = "nev-gate", "nm-gate", "o-home", "o-away"
# The recorder in a subprocess, with nothing of the host's: no sleep inhibitor, a synced clock.
RUNNER = """
import sys
from raw_recorder import runtime
class NoInhibitor:
    held = False
    def acquire(self):
        self.held = True
    def release(self):
        self.held = False
runtime.SleepInhibitor = NoInhibitor
runtime.clock_synced = lambda: True
from raw_recorder.cli import main
raise SystemExit(main(sys.argv[1:]))
"""


def now_ms():
    return time.time_ns() // 1_000_000


class Venue:
    def __init__(self):
        self.orders = {A: {"a1": ("0.55", 1000)}, B: {"b1": ("0.43", 800)}}
        self.conns: list[dict] = []         # per connection: ws, subs {market: channel}, seq {(m, ch): n}
        self.n = 0

    def app(self):
        app = web.Application()
        app.router.add_get("/v3/ws", self.ws)
        app.router.add_get("/v3/keys/{k}", self.keys)
        app.router.add_get("/v3/limits", self.limits)
        app.router.add_get("/v3/public/catalog/markets/{m}", self.catalog)
        app.on_startup.append(self.start)
        return app

    async def keys(self, request):
        return web.json_response({"code": "FORBIDDEN"}, status=403)       # a read key cannot read keys

    async def limits(self, request):
        return web.json_response({"stream": {"capacity": 512, "refillPerSec": 4}})

    async def catalog(self, request):
        return web.json_response({
            "marketId": MARKET, "description": "HOME", "eventId": EVENT, "marketType": "MONEY", "status": "OPEN",
            "voids": "FMV", "startsTs": now_ms() + 3_600_000, "fee": {},
            "outcomes": [{"outcomeId": A, "name": "HOME", "status": "TBD"},
                         {"outcomeId": B, "name": "AWAY", "status": "TBD"}]})

    async def start(self, app):
        self.feeder = asyncio.create_task(self.feed())

    def body(self, c, channel):
        if channel == "book":
            orders = {o: [{"orderId": k, "price": p, "qty": q} for k, (p, q) in v.items()] for o, v in self.orders.items()}
            inner = {"seq": c["seq"][(MARKET, "book")], "orders": orders}
        else:
            inner = {"seq": c["seq"][(MARKET, "trades")], "trades": []}
        return {"eventId": EVENT, channel: inner, "lifecycle": {"seq": 0, "status": "OPEN"}}

    async def broadcast(self, channel, deltas):
        for c in list(self.conns):
            if c["subs"].get(MARKET) == channel and not c["ws"].closed:
                c["seq"][(MARKET, channel)] += 1
                try:
                    await c["ws"].send_str(json.dumps({"ts": now_ms(), "delta": {MARKET: {
                        "eventId": EVENT, channel: {"seq": c["seq"][(MARKET, channel)], "deltas": deltas}}}}))
                except ConnectionError:
                    pass

    async def feed(self):
        i = 0
        while True:
            await asyncio.sleep(0.1)
            i += 1
            if "x" in self.orders[A]:
                del self.orders[A]["x"]
                await self.broadcast("book", [{"kind": "remove", "orderId": "x", "reason": "cancel"}])
            else:
                self.orders[A]["x"] = ("0.50", 10 + i % 7)
                await self.broadcast("book", [{"kind": "add", "orderId": "x", "outcomeId": A, "price": "0.50",
                                               "qty": self.orders[A]["x"][1]}])
            if i % 5 == 0:
                await self.broadcast("trades", [{"tradeId": f"t{i}", "outcomeId": B, "price": "0.44", "qty": 5,
                                                 "ts": now_ms()}])

    async def ws(self, request):
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)
        c = {"ws": ws, "subs": {}, "seq": {(MARKET, "book"): 100, (MARKET, "trades"): 500}}
        self.conns.append(c)
        try:
            async for msg in ws:
                req = json.loads(msg.data)
                n = req["nonce"]
                if "subscribe" in req:
                    sel = req["subscribe"]["markets"]
                    c["subs"].update(sel)
                    await ws.send_str(json.dumps({"ts": now_ms(), "nonce": n, "subscribed": {
                        "markets": sel, "events": {}, "private": []},
                        "snapshot": {m: self.body(c, ch) for m, ch in sel.items()}}))
                elif "snapshot" in req:
                    sel = req["snapshot"]["markets"]
                    await ws.send_str(json.dumps({"ts": now_ms(), "nonce": n,
                                                  "snapshot": {m: self.body(c, ch) for m, ch in sel.items()}}))
                elif "unsubscribe" in req:
                    for s in req["unsubscribe"]:
                        c["subs"].pop(s.removeprefix("market:"), None)
                elif "status" in req:
                    await ws.send_str(json.dumps({"nonce": n, "status": {"subscriptions": {
                        "markets": dict(c["subs"]), "events": {}, "private": []}}}))
        finally:
            self.conns.remove(c)
        return ws


def write_config(root: Path, venue_url: str, relay_url: str) -> Path:
    cfg = root.parent / "config.toml"
    cfg.write_text(f"""
[archive]
root = "{root}"
[r0]
kalshi_poll_interval_s = 10
odds_poll_interval_s = 1200
odds_quota_floor = 50
capture_lead_s = 10800
capture_tail_s = 5400
odds_window_lead_s = 10800
segment_max_s = 3600
fsync_interval_s = 1
rest_timeout_s = 5
min_free_disk_mb = 1
stream_probe_reserve_fraction = 0.5
novig_public_book_poll_interval_s = 10
[stream]
channel_probe_interval_s = 1
transport_liveness_max_s = 45
probe_timeout_s = 1
subscription_check_interval_s = 60
reconnect_backoff_s = {{ initial = 0.2, max = 1 }}
[a1]
capture_lead_s = 7200
capture_tail_s = 3600
[novig]
host = "{venue_url}"
ws_url = "{relay_url.replace('http://', 'ws://')}/v3/ws"
channels = ["book", "trades"]
key_id_env = "GATE_TEST_KEY_ID"
key_path_env = "GATE_TEST_KEY_PATH"
public_book_poll = "never"
[kalshi]
enabled = false
base_url = "http://unused"
[odds_api]
enabled = false
key_env = "GATE_TEST_UNSET"
[mlb]
enabled = false
base_url = "http://unused"
[faults]
enabled = true
""")
    root.mkdir(parents=True)
    start = now_ms() + 3_600_000
    (root / "events.discovered.toml").write_text(f"""discovered_at_utc = "{ms_iso(now_ms())}"
last_notes = []

[[event]]
league = "NBA"
event_id = "{EVENT}"
label = "Away @ Home, NBA (fixture)"
scheduled_start_utc = "{ms_iso(start)}"
novig_markets = ["{MARKET}"]
""")
    return cfg


@pytest.fixture(scope="module")
def gate_run(tmp_path_factory):
    """The recorded run, ingested: (archive root, database, run start, run end, injection reports)."""
    base = tmp_path_factory.mktemp("gate")
    root = base / "archive"
    return asyncio.run(_record(base, root))


async def _record(base: Path, root: Path):
    venue = Venue()
    vsrv = TestServer(venue.app())
    await vsrv.start_server()
    venue_url = str(vsrv.make_url("")).rstrip("/")
    relay = faults.Relay(venue_url.replace("http://", "ws://"))
    rsrv = TestServer(relay.app())
    await rsrv.start_server()
    relay_url = str(rsrv.make_url("")).rstrip("/")
    cfg = write_config(root, venue_url, relay_url)
    key = base / "key.pem"
    key.write_text(json.loads(VECTORS.read_text())["keypairs"]["ed25519-test-1"]["private_key_pkcs8_pem"])
    key.chmod(0o600)
    env = dict(os.environ, GATE_TEST_KEY_ID="key-1", GATE_TEST_KEY_PATH=str(key))
    log = (base / "recorder.log").open("ab")

    async def start_recorder():
        return await asyncio.create_subprocess_exec(sys.executable, "-c", RUNNER, "run", "--config", str(cfg),
                                                    env=env, stdout=log, stderr=log)

    async def until(cond, timeout=20.0):
        deadline = time.monotonic() + timeout
        while not cond():
            assert time.monotonic() < deadline, f"timed out; see {base / 'recorder.log'}"
            await asyncio.sleep(0.05)

    def subscribed():
        return {ch for c in venue.conns for m, ch in c["subs"].items() if m == MARKET} == {"book", "trades"}

    control = root / ".recorder.control"
    lo = now_ms()
    reports = {}
    proc = await start_recorder()
    try:
        await until(lambda: subscribed() and control.is_socket())
        await asyncio.sleep(3.0)                    # a quiet stretch before the first fault

        async def inject(kind, **target):
            reports[kind] = await faults.inject(kind, target, control_path=control, relay_url=relay_url,
                                                restart_wait_s=30)
            assert reports[kind]["ok"], reports[kind]

        await inject("sequence_skip", market=MARKET, channel="book", timeout_s=5)
        await asyncio.sleep(2.0)
        await inject("channel_stall", market=MARKET, channel="book", seconds=2.5)
        await asyncio.sleep(2.0)
        await inject("resubscribe", market=MARKET)
        await asyncio.sleep(1.5)
        await inject("socket_kill")
        await asyncio.sleep(0.5)
        await until(subscribed)
        await asyncio.sleep(1.5)
        killing = asyncio.create_task(inject("process_kill"))
        await proc.wait()
        assert proc.returncode == -signal.SIGKILL
        await asyncio.sleep(0.5)                     # the service manager's restart delay
        proc = await start_recorder()
        await killing
        await until(subscribed)
        await asyncio.sleep(1.5)
    finally:
        if proc.returncode is None:
            proc.send_signal(signal.SIGTERM)
            await proc.wait()
        log.close()
        await rsrv.close()
        await vsrv.close()
    hi = sealed_through(root) - 1
    # The window ingested and gated is the run itself, ending just inside the seal watermark.
    start = hi - 1_000
    event = {"league": "NBA", "event_id": EVENT, "label": "fixture", "novig_markets": [MARKET],
             "scheduled_start_utc": ms_iso(start), "capture_lead_s": -(-(start - lo) // 1000) + 1,
             "capture_tail_s": 1}
    path = base / "clv.sqlite"
    conn = db.connect(path)
    db.migrate(conn)
    ingest_league_event(conn, root, EVENT, now_ms=hi + 60_000, event=event)
    conn.close()
    return root, path, lo - 1_000, hi, reports


def copy_db(path: Path) -> sqlite3.Connection:
    c = sqlite3.connect(":memory:")
    db.connect(path).backup(c)
    return c


def test_every_fault_yields_the_right_gap_and_the_gate_passes(gate_run):
    root, path, lo, hi, reports = gate_run
    rep = gate.run(db.connect(path), root, lo, hi, min_per_kind=1)
    text = gate.render(rep)
    assert rep.verdict(), text
    assert text.startswith("# Gate A1: PASS")
    assert rep.applied() == {k: 1 for k in gate.KINDS}
    by_kind = {}
    for c in rep.checks:
        by_kind.setdefault(c.kind, set()).add(c.scope)
    book, trades = f"novig:market:{MARKET}/book", f"novig:market:{MARKET}/trades"
    # Connection faults touch both channels; the relay's drops and the resubscribe only what they name.
    assert by_kind["socket_kill"] == by_kind["process_kill"] == {book, trades}
    assert by_kind["sequence_skip"] == by_kind["channel_stall"] == {book}
    assert by_kind["resubscribe"] == {book, trades}
    assert len(reports["channel_stall"]["dropped"]) >= 10
    assert rep.unattributed == [], text
    assert any(src == "novig" and n > 0 for _, src, n, *_ in rep.storage)
    assert rep.depth["snapshots"] > 0 and rep.depth["by_reason"].get("resync", 0) >= 2


def test_a_dropped_delta_no_gap_covers_fails(gate_run):
    root, path, lo, hi, reports = gate_run
    facts = gate._Facts(db.connect(path), root, lo, hi)
    # A drop claimed before any fault, where the book was trusted throughout.
    t = reports["sequence_skip"]["start_ts_ms"] - 1_500   # more than the slack before
    f = gate.Fault("x", "sequence_skip", {}, t, [], "ref", {"dropped": [{"ts_ms": t, "markets": {MARKET: {"book": 1}}}]},
                   True)
    (check,) = [gate.check_scope(facts, f, s, times, set()) for s, times in gate.expectations(f).items()]
    assert not check.ok and "no gap covers" in check.detail


def test_a_gap_opened_after_the_last_observation_fails(gate_run):
    root, path, lo, hi, reports = gate_run
    c = copy_db(path)
    t = reports["sequence_skip"]["start_ts_ms"] - 1_500   # more than the slack before
    last = gate._Facts(c, root, lo, hi).last_evidence(MARKET, t)
    snap = c.execute("SELECT raw_artifact_id, raw_line FROM book_snapshot WHERE observed_ts_ms > ? "
                     "ORDER BY observed_ts_ms LIMIT 1", (t,)).fetchone()
    scope = f"novig:market:{MARKET}/book"
    opened = c.execute("INSERT INTO collection_gap (kind, source, scope, reason, boundary_ts_ms, observed_ts_ms)"
                       " VALUES ('open', 'novig', ?, 'sequence_gap', ?, 0)", (scope, last + 1)).lastrowid
    c.execute("INSERT INTO collection_gap (kind, opens_gap_id, source, scope, reason, boundary_ts_ms, observed_ts_ms,"
              " raw_artifact_id, raw_line) VALUES ('close', ?, 'novig', ?, 'sequence_gap', ?, 0, ?, ?)",
              (opened, scope, t + 60_000, *snap))
    f = gate.Fault("x", "sequence_skip", {}, t, [], "ref", {"dropped": [{"ts_ms": t, "markets": {MARKET: {"book": 1}}}]},
                   True)
    check = gate.check_scope(gate._Facts(c, root, lo, hi), f, scope, [(t, None)], set())
    assert not check.ok and "last trustworthy observation" in check.detail


def insert_gap(c, scope, start, end, close_ref):
    opened = c.execute("INSERT INTO collection_gap (kind, source, scope, reason, boundary_ts_ms, observed_ts_ms)"
                       " VALUES ('open', 'novig', ?, 'sequence_gap', ?, 0)", (scope, start)).lastrowid
    c.execute("INSERT INTO collection_gap (kind, opens_gap_id, source, scope, reason, boundary_ts_ms, observed_ts_ms,"
              " raw_artifact_id, raw_line) VALUES ('close', ?, 'novig', ?, 'sequence_gap', ?, 0, ?, ?)",
              (opened, scope, end, *close_ref))


def test_a_delta_trusted_after_a_dropped_one_fails_though_inside_the_slack(gate_run):
    # The replay's worst failure: after a dropped delta it trusts the next ones, and opens a gap only
    # later. The fault time's slack must not hide it: the boundary is placed by seq.
    root, path, lo, hi, reports = gate_run
    c = copy_db(path)
    t = reports["sequence_skip"]["start_ts_ms"] - 1_500
    seq, a = c.execute("SELECT seq, observed_ts_ms FROM tick WHERE source = 'stream' AND observed_ts_ms <= ?"
                       " ORDER BY observed_ts_ms DESC LIMIT 1", (t,)).fetchone()
    facts = gate._Facts(c, root, lo, hi)
    late = facts.last_evidence(MARKET, a + 1 + 900)               # within the slack after the drop
    assert late > a
    snap = c.execute("SELECT raw_artifact_id, raw_line, observed_ts_ms FROM book_snapshot WHERE observed_ts_ms > ?"
                     " ORDER BY observed_ts_ms LIMIT 1", (late,)).fetchone()
    scope = f"novig:market:{MARKET}/book"
    insert_gap(c, scope, late, snap[2], snap[:2])
    f = gate.Fault("x", "sequence_skip", {}, a, [], "ref",
                   {"dropped": [{"ts_ms": a + 1, "markets": {MARKET: {"book": seq + 1}}}]}, True)
    (check,) = [gate.check_scope(gate._Facts(c, root, lo, hi), f, s, times, set())
                for s, times in gate.expectations(f).items()]
    assert not check.ok and f"seq {seq + 1}" in check.detail and "last trustworthy observation" in check.detail
    assert ms_iso(a) in check.detail


def test_a_gap_closed_without_a_snapshot_fails_the_resync_check(gate_run):
    root, path, lo, hi, _ = gate_run
    c = copy_db(path)
    scope = f"novig:market:{MARKET}/book"
    delta = c.execute("SELECT t.raw_artifact_id, t.raw_line, t.observed_ts_ms FROM tick t "
                      "WHERE t.source = 'stream' AND (t.raw_artifact_id, t.raw_line) NOT IN "
                      "(SELECT raw_artifact_id, raw_line FROM book_snapshot) ORDER BY t.observed_ts_ms LIMIT 1").fetchone()
    opened = c.execute("INSERT INTO collection_gap (kind, source, scope, reason, boundary_ts_ms, observed_ts_ms)"
                       " VALUES ('open', 'novig', ?, 'sequence_gap', ?, 0)", (scope, delta[2] - 10)).lastrowid
    c.execute("INSERT INTO collection_gap (kind, opens_gap_id, source, scope, reason, boundary_ts_ms, observed_ts_ms,"
              " raw_artifact_id, raw_line) VALUES ('close', ?, 'novig', ?, 'sequence_gap', ?, 0, ?, ?)",
              (opened, scope, delta[2], delta[0], delta[1]))
    rep = gate.run(c, root, lo, hi, min_per_kind=1)
    assert not rep.verdict()
    (bad,) = [x for x in rep.checks if not x.ok]
    assert bad.kind == "resync" and "not a snapshot" in bad.detail


def test_a_fault_without_a_result_and_a_short_schedule_fail(gate_run):
    root, path, lo, hi, reports = gate_run
    # Cut the run before the process kill's result, which the restarted recorder archived.
    cut = reports["process_kill"]["killed_ts_ms"]
    rep = gate.run(db.connect(path), root, lo, cut, min_per_kind=1)
    assert not rep.verdict()
    assert [c.detail for c in rep.checks if not c.ok] == ["no fault_applied or fault_failed recorded"]
    rep = gate.run(db.connect(path), root, lo, hi, min_per_kind=2)
    assert all(c.ok for c in rep.checks) and not rep.verdict()
    assert "| socket_kill | 1 (short) |" in gate.render(rep)


def test_the_gate_waits_for_the_archive_to_be_sealed(gate_run):
    root, path, lo, hi, _ = gate_run
    with pytest.raises(gate.GateError, match="sealed only through"):
        gate.run(db.connect(path), root, lo, hi + 10_000)
