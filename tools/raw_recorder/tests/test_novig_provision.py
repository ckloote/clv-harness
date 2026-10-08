"""novig-provision against a fake Novig that verifies every NOVIG-V3 signature."""
import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from raw_recorder.novig.signing import NovigSigner, string_to_sign
from raw_recorder.provision import ProvisionError, provision

SPEC = json.loads((Path(__file__).parent / "fixtures" / "novig_signature_vectors.json").read_text())
MGMT_PEM = SPEC["keypairs"]["ed25519-test-1"]["private_key_pkcs8_pem"].encode()
MGMT = NovigSigner.from_pem("mgmt-key", MGMT_PEM)


def fake_novig(state, refuse_issue_with=None):
    """Keys: key ID -> (public key, scope). Rejects any bad signature with 401."""
    mgmt_pub = serialization.load_pem_private_key(MGMT_PEM, None).public_key()
    keys = {"mgmt-key": (mgmt_pub, "management")}

    async def verify(request):
        body = await request.read()
        key_id = request.headers.get("Novig-Key-Id")
        if key_id not in keys:
            raise web.HTTPUnauthorized(text='{"code":"SIGNATURE_REJECTED"}')
        sts = string_to_sign(int(request.headers["Novig-Timestamp"]), request.method,
                             request.path, request.query_string, body)
        try:
            keys[key_id][0].verify(base64.b64decode(request.headers["Novig-Signature"]), sts.encode())
        except Exception:
            raise web.HTTPUnauthorized(text='{"code":"SIGNATURE_REJECTED"}')
        state["calls"].append((request.method, request.path, keys[key_id][1]))
        return keys[key_id][1], body

    def new_key(pem, scope, prefix):
        pub = serialization.load_pem_public_key(pem.encode())
        assert isinstance(pub, Ed25519PublicKey)
        key_id = f"{prefix}-{len(keys)}"
        keys[key_id] = (pub, scope)
        der = pub.public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        return key_id, "sha256:" + hashlib.sha256(der).hexdigest()

    async def echo(request):
        _, body = await verify(request)
        return web.Response(body=body, content_type="application/json")

    async def open_subaccount(request):
        scope, body = await verify(request)
        assert scope == "management"
        req = json.loads(body)
        assert set(req) == {"label", "publicKey", "algorithm"} and req["algorithm"] == "Ed25519"
        key_id, _ = new_key(req["publicKey"], "trading", "trading")
        state["trading_key_ids"].append(key_id)
        return web.json_response({"keyId": key_id, "label": req["label"], "balance": "0.00000"}, status=201)

    async def issue_key(request):
        scope, body = await verify(request)
        assert scope == "management"
        if refuse_issue_with:
            return web.json_response({"code": refuse_issue_with, "message": "no"}, status=403)
        req = json.loads(body)
        assert req["scope"] == "trading::read" and request.match_info["sub"] in state["trading_key_ids"]
        key_id, fp = new_key(req["publicKey"], "trading::read", "read")
        return web.json_response({"keyId": key_id, "fingerprint": fp}, status=201)

    async def get_key(request):
        scope, _ = await verify(request)
        if not scope.startswith("management"):
            return web.json_response({"code": "SCOPE_INSUFFICIENT", "message": "no"}, status=403)
        return web.json_response({"keyId": request.match_info["id"]})

    app = web.Application()
    app.router.add_post("/v3/echo", echo)
    app.router.add_post("/v3/account/subaccounts", open_subaccount)
    app.router.add_post("/v3/account/subaccounts/{sub}/keys", issue_key)
    app.router.add_get("/v3/keys/{id}", get_key)
    return app


async def start(state, **kw):
    state.setdefault("calls", [])
    state.setdefault("trading_key_ids", [])
    srv = TestServer(fake_novig(state, **kw))
    await srv.start_server()
    return srv, str(srv.make_url("")).rstrip("/")


async def test_provision_mints_a_read_key_and_keeps_no_trading_key(tmp_path):
    state = {}
    srv, host = await start(state)
    out = tmp_path / "read.pem"
    try:
        async with aiohttp.ClientSession() as http:
            p = await provision(http, host, MGMT, out)
    finally:
        await srv.close()

    assert p.subaccount_key_id == state["trading_key_ids"][0]
    assert p.read_key_id.startswith("read-") and p.pem_path == out
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o600
    key = serialization.load_pem_private_key(out.read_bytes(), None)
    assert isinstance(key, Ed25519PrivateKey)
    assert list(tmp_path.iterdir()) == [out]            # the trading private key was never written
    assert [c[:2] for c in state["calls"]] == [
        ("POST", "/v3/echo"), ("POST", "/v3/account/subaccounts"),
        ("POST", f"/v3/account/subaccounts/{p.subaccount_key_id}/keys"),
        ("POST", "/v3/echo"), ("GET", f"/v3/keys/{p.read_key_id}")]
    assert state["calls"][-1][2] == "trading::read"      # the checks ran as the new read key


async def test_existing_subaccount_is_reused(tmp_path):
    state = {"trading_key_ids": ["trading-existing"]}
    srv, host = await start(state)
    try:
        async with aiohttp.ClientSession() as http:
            p = await provision(http, host, MGMT, tmp_path / "read.pem", subaccount="trading-existing")
    finally:
        await srv.close()
    assert p.subaccount_key_id == "trading-existing"
    assert ("POST", "/v3/account/subaccounts", "management") not in state["calls"]


async def test_refused_issue_removes_the_key_file_and_explains(tmp_path):
    state = {}
    srv, host = await start(state, refuse_issue_with="NOVIG_API_DISABLED")
    out = tmp_path / "read.pem"
    try:
        async with aiohttp.ClientSession() as http:
            with pytest.raises(ProvisionError) as err:
                await provision(http, host, MGMT, out)
    finally:
        await srv.close()
    assert err.value.code == "NOVIG_API_DISABLED" and "developers@novig.com" in str(err.value)
    assert not out.exists()


async def test_never_overwrites_an_existing_key_file(tmp_path):
    state = {}
    srv, host = await start(state)
    out = tmp_path / "read.pem"
    out.write_text("precious")
    try:
        async with aiohttp.ClientSession() as http:
            with pytest.raises(FileExistsError):
                await provision(http, host, MGMT, out)
    finally:
        await srv.close()
    assert out.read_text() == "precious"


def test_recorder_never_imports_the_management_key_tool():
    code = ("import sys, raw_recorder.cli, raw_recorder.runtime; "
            "sys.exit('raw_recorder.provision' in sys.modules)")
    assert subprocess.run([sys.executable, "-c", code]).returncode == 0
