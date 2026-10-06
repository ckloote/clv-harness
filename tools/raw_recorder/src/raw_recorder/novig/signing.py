"""NOVIG-V3 request signing (docs.novig.com/api/signing, read 2026-10-06).

String to sign: six LF-joined lines, no trailing newline::

    NOVIG-V3
    {unix_millis}
    {METHOD}
    {path}                 verbatim, never normalized or decoded
    {canonical_query}      empty line when there is no query
    {lowercase_hex(sha256(raw_body))}

Headers: Novig-Key-Id, Novig-Timestamp, Novig-Signature (standard padded
base64). Ed25519 signs the string directly; P-256 is ECDSA/SHA-256, DER.
The recorder only ever loads a `trading::read` key.
"""
from __future__ import annotations

import base64
import hashlib
import os
import stat
import time
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

SCHEME = "NOVIG-V3"
_UNRESERVED = frozenset(b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~")
_HEX = "0123456789abcdefABCDEF"


def _decode(part: str) -> bytes:
    """Only valid %XX escapes decode; a bare '+' stays '+'."""
    raw = part.encode("utf-8")
    out = bytearray()
    i = 0
    while i < len(raw):
        if raw[i] == 0x25 and i + 2 < len(raw) and chr(raw[i + 1]) in _HEX and chr(raw[i + 2]) in _HEX:
            out.append(int(raw[i + 1:i + 3], 16))
            i += 3
        else:
            out.append(raw[i])
            i += 1
    return bytes(out)


def _encode(part: str) -> str:
    return "".join(chr(b) if b in _UNRESERVED else f"%{b:02X}" for b in _decode(part))


def canonical_query(raw_query: str) -> str:
    pairs = []
    for pair in raw_query.split("&"):
        if not pair:
            continue
        name, _, value = pair.partition("=")
        pairs.append((_encode(name), _encode(value)))
    pairs.sort()
    return "&".join(f"{k}={v}" for k, v in pairs)


def string_to_sign(ts_ms: int, method: str, path: str, raw_query: str, body: bytes) -> str:
    return "\n".join([SCHEME, str(ts_ms), method.upper(), path,
                      canonical_query(raw_query), hashlib.sha256(body).hexdigest()])


class NovigSigner:
    def __init__(self, key_id: str, private_key):
        if not isinstance(private_key, (Ed25519PrivateKey, ec.EllipticCurvePrivateKey)):
            raise TypeError("Novig keys are Ed25519 or P-256")
        if isinstance(private_key, ec.EllipticCurvePrivateKey) and \
                not isinstance(private_key.curve, ec.SECP256R1):
            raise TypeError("ECDSA Novig keys must use P-256")
        self.key_id = key_id
        self._key = private_key

    @classmethod
    def from_pem(cls, key_id: str, pem: bytes) -> NovigSigner:
        return cls(key_id, serialization.load_pem_private_key(pem, password=None))

    @classmethod
    def from_pem_file(cls, key_id: str, path: Path) -> NovigSigner:
        mode = os.stat(path).st_mode
        if mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise PermissionError(f"{path} is readable by group/other; chmod 600 it")
        return cls.from_pem(key_id, Path(path).read_bytes())

    def sign_bytes(self, data: bytes) -> bytes:
        if isinstance(self._key, Ed25519PrivateKey):
            return self._key.sign(data)
        return self._key.sign(data, ec.ECDSA(hashes.SHA256()))

    def headers(self, method: str, path: str, raw_query: str, body: bytes,
                ts_ms: int | None = None) -> dict[str, str]:
        ts = time.time_ns() // 1_000_000 if ts_ms is None else ts_ms
        sig = self.sign_bytes(string_to_sign(ts, method, path, raw_query, body).encode("utf-8"))
        return {"Novig-Key-Id": self.key_id, "Novig-Timestamp": str(ts),
                "Novig-Signature": base64.b64encode(sig).decode("ascii")}

    __call__ = headers  # usable directly as a rest.Signer
