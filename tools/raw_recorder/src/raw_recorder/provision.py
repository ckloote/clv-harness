"""novig-provision: mint a Novig `trading::read` key with the management key.

DESIGN.md §13 R0 key provisioning. Run it by hand, never as part of the
recorder: it is the only code here that loads a management key, and the
recorder never imports it (tests/test_novig_provision.py checks). For
Production the management private key may be on the recorder host only for
the provisioning session; remove it once the read key works
(docs/decisions/2026-10-08-single-host-provisioning.md). On Paper (play
money) it may stay.

Steps, each signed with the management key:

1. `POST /v3/echo` to prove the management key, host and clock work.
2. Unless `--subaccount` names an existing one, `POST /v3/account/subaccounts`
   opens a subaccount. That call requires the public half of a new `trading`
   keypair; its private half is generated in memory and never written
   anywhere, so no credential that can trade exists afterwards.
3. `POST /v3/account/subaccounts/{keyId}/keys` with scope `trading::read`,
   for a keypair generated here. The private key is written to `--out`
   (created 0600, never overwritten) before the call, and removed if the call
   fails.
4. The new key is checked: its echo must succeed, and `GET /v3/keys/{id}`
   must be refused (a read key cannot read key metadata).

Prints the environment lines the recorder needs. Never prints a private key.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from .novig.signing import NovigSigner

ENVIRONMENTS = {
    "paper": ("https://api.paper.novig.com", "NOVIG_PAPER_READ_KEY"),
    "production": ("https://api.novig.com", "NOVIG_READ_KEY"),
}
HINTS = {
    "NOVIG_API_DISABLED": "the Novig API is not enabled on this account (ask developers@novig.com)",
    "KYC_REQUIRED": "complete identity verification in the Novig app first",
    "REGION_RESTRICTED": "the location check refused this request",
    "ADDRESS_ANONYMIZED": "the request came from a VPN, proxy or Tor address",
    "SIGNATURE_REJECTED": "wrong key ID, key file, host (Paper vs Production) or clock",
    "SCOPE_INSUFFICIENT": "the signing key is not a management key",
}


class ProvisionError(RuntimeError):
    def __init__(self, step: str, status: int, body: bytes):
        self.step, self.status = step, status
        try:
            self.code = json.loads(body).get("code")
        except (ValueError, AttributeError):
            self.code = None
        hint = HINTS.get(self.code or "", "")
        text = body.decode("utf-8", "replace")[:300]
        super().__init__(f"{step}: HTTP {status} {self.code or ''} {text}" + (f"\n  hint: {hint}" if hint else ""))


@dataclass(frozen=True)
class Provisioned:
    subaccount_key_id: str
    read_key_id: str
    fingerprint: str
    pem_path: Path


def _public_pem(key: Ed25519PrivateKey) -> str:
    return key.public_key().public_bytes(serialization.Encoding.PEM,
                                         serialization.PublicFormat.SubjectPublicKeyInfo).decode()


def _fingerprint(key: Ed25519PrivateKey) -> str:
    der = key.public_key().public_bytes(serialization.Encoding.DER,
                                        serialization.PublicFormat.SubjectPublicKeyInfo)
    return "sha256:" + hashlib.sha256(der).hexdigest()


async def _call(http: aiohttp.ClientSession, host: str, signer: NovigSigner, method: str,
                path: str, body: dict | None, step: str, ok: tuple[int, ...]) -> tuple[int, bytes]:
    raw = b"" if body is None else json.dumps(body, separators=(",", ":")).encode()
    headers = signer(method, path, "", raw)
    if body is not None:
        headers["Content-Type"] = "application/json"
    async with http.request(method, host + path, data=raw if body is not None else None,
                            headers=headers, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        data = await resp.read()
        if resp.status not in ok:
            raise ProvisionError(step, resp.status, data)
        return resp.status, data


async def provision(http: aiohttp.ClientSession, host: str, management: NovigSigner, out: Path, *,
                    subaccount: str | None = None, label: str = "clv-r0-recorder",
                    name: str = "clv-r0-recorder-read") -> Provisioned:
    await _call(http, host, management, "POST", "/v3/echo", {"provision": "management"},
                "management echo", (200,))

    if subaccount is None:
        throwaway = Ed25519PrivateKey.generate()   # trading key: never written anywhere
        _, data = await _call(http, host, management, "POST", "/v3/account/subaccounts",
                              {"label": label, "publicKey": _public_pem(throwaway),
                               "algorithm": "Ed25519"}, "open subaccount", (201,))
        del throwaway
        subaccount = json.loads(data)["keyId"]

    read_key = Ed25519PrivateKey.generate()
    pem = read_key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                 serialization.NoEncryption())
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)   # never overwrite a key
    with os.fdopen(fd, "wb") as f:
        f.write(pem)
    try:
        _, data = await _call(http, host, management, "POST",
                              f"/v3/account/subaccounts/{subaccount}/keys",
                              {"name": name, "publicKey": _public_pem(read_key),
                               "algorithm": "Ed25519", "scope": "trading::read"},
                              "issue trading::read key", (201,))
    except BaseException:
        out.unlink()
        raise
    created = json.loads(data)
    expected = _fingerprint(read_key)
    if created.get("fingerprint") != expected:
        raise RuntimeError(f"fingerprint mismatch: Novig {created.get('fingerprint')}, local {expected}")

    reader = NovigSigner(created["keyId"], read_key)
    await _call(http, host, reader, "POST", "/v3/echo", {"provision": "read"}, "read-key echo", (200,))
    status, _ = await _call(http, host, reader, "GET", f"/v3/keys/{created['keyId']}", None,
                            "read-key scope check", (200, 401, 403))
    if status == 200:
        raise RuntimeError("the new key can read /v3/keys: it is not a trading::read key")
    return Provisioned(subaccount, created["keyId"], expected, out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="novig-provision", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", choices=sorted(ENVIRONMENTS), required=True)
    ap.add_argument("--management-key-id", required=True)
    ap.add_argument("--management-key", type=Path, required=True, help="management key PEM (chmod 600)")
    ap.add_argument("--out", type=Path, required=True, help="where to write the new read key PEM")
    ap.add_argument("--subaccount", help="existing subaccount (its trading key ID); default: open a new one")
    ap.add_argument("--label", default="clv-r0-recorder")
    ap.add_argument("--yes", action="store_true", help="skip the Production confirmation")
    args = ap.parse_args(argv)

    host, env_prefix = ENVIRONMENTS[args.env]
    if args.env == "production" and not args.yes:
        print("Production: the management key must not stay on a host that runs the recorder.\n"
              "Remove it once `raw-recorder echo` succeeds with the new read key.", file=sys.stderr)
        if input("Type 'yes' to continue: ").strip() != "yes":
            return 1
    if args.out.exists():
        print(f"{args.out} exists; refusing to overwrite a key file", file=sys.stderr)
        return 2
    management = NovigSigner.from_pem_file(args.management_key_id, args.management_key)

    async def run() -> Provisioned:
        async with aiohttp.ClientSession() as http:
            return await provision(http, host, management, args.out,
                                   subaccount=args.subaccount, label=args.label)
    try:
        p = asyncio.run(run())
    except ProvisionError as exc:
        print(f"novig-provision failed at {exc}", file=sys.stderr)
        return 1

    print(f"subaccount (trading key ID, its address): {p.subaccount_key_id}")
    print(f"trading::read key ID: {p.read_key_id}")
    print(f"fingerprint: {p.fingerprint}  (matches the local key)")
    print(f"private key written to {p.pem_path} (0600); echo ok; key metadata refused, as expected\n")
    print("Recorder environment:")
    print(f"{env_prefix}_ID={p.read_key_id}")
    print(f"{env_prefix}_PATH={p.pem_path.resolve()}")
    if args.env == "production":
        print(f"\nOnce `raw-recorder echo` succeeds, remove {args.management_key} from this host.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
