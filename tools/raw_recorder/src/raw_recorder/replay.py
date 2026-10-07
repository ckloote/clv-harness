"""Archive-level association, for verification and reporting only.

This reads recorder metadata and the minimum of vendor framing needed to
associate records: REST exchanges by request_id, and probe replies by the
nonce Novig echoes. It does not interpret books, sequences or prices; that is
V0/A1 (DESIGN.md §7.2).
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass
class RestExchange:
    request_id: str
    start: dict | None = None
    request_body: str | None = None
    response_body: str | None = None
    complete: dict | None = None

    @property
    def subjects(self) -> list[str]:
        return (self.start or {}).get("subjects", [])

    @property
    def failure(self) -> str | None:
        return (self.complete or {}).get("failure")


def rest_exchanges(frames: Iterable[dict]) -> dict[str, RestExchange]:
    """Group REST records by request_id (== conn_id), never by adjacency."""
    out: dict[str, RestExchange] = {}
    for f in frames:
        if f["dir"] == "event":
            meta = json.loads(f["frame"])
            kind = meta.get("type")
            if kind == "rest_request":
                out.setdefault(f["conn_id"], RestExchange(f["conn_id"])).start = meta
            elif kind == "rest_complete":
                out.setdefault(f["conn_id"], RestExchange(f["conn_id"])).complete = meta
        elif f["conn_id"] in out:
            ex = out[f["conn_id"]]
            if f["dir"] == "in":
                ex.response_body = f["frame"]
            else:
                ex.request_body = f["frame"]
    return out


@dataclass
class Probe:
    nonce: int
    markets: list[str]
    channel: str | None
    sent_ts_ms: int
    reply_ts_ms: int | None = None
    reply: str | None = None


@dataclass
class WsEvidence:
    """Per connection: transport health and channel evidence, kept apart."""
    conn_id: str
    transport: list[dict] = field(default_factory=list)       # control-frame events
    probes: dict[int, Probe] = field(default_factory=dict)    # nonce -> probe and its reply
    channel_messages: list[dict] = field(default_factory=list)  # in-frames carrying channel data
    events: list[dict] = field(default_factory=list)          # connection lifecycle events


def ws_evidence(frames: Iterable[dict]) -> dict[str, WsEvidence]:
    out: dict[str, WsEvidence] = {}
    for f in frames:
        cid = f["conn_id"]
        if f["dir"] == "event":
            meta = json.loads(f["frame"])
            ev = out.setdefault(cid, WsEvidence(cid))
            if meta.get("type") == "ws_control":
                ev.transport.append({"recv_ts_ms": f["recv_ts_ms"], **meta})
            else:
                ev.events.append({"recv_ts_ms": f["recv_ts_ms"], **meta})
            continue
        try:
            msg = json.loads(f["frame"])
        except ValueError:
            continue
        if not isinstance(msg, dict):
            continue
        ev = out.setdefault(cid, WsEvidence(cid))
        if f["dir"] == "out" and "snapshot" in msg and "nonce" in msg:
            sel = msg["snapshot"].get("markets", {})
            channels = set(sel.values())
            ev.probes[msg["nonce"]] = Probe(msg["nonce"], sorted(sel), channels.pop() if len(channels) == 1 else None,
                                            f["recv_ts_ms"])
        elif f["dir"] == "in":
            nonce = msg.get("nonce")
            if nonce in ev.probes and ev.probes[nonce].reply is None:
                ev.probes[nonce].reply = f["frame"]
                ev.probes[nonce].reply_ts_ms = f["recv_ts_ms"]
            if "snapshot" in msg or "delta" in msg:
                ev.channel_messages.append(f)
    return out
