"""NOVIG-V3 signer against Novig's 30 published vectors (fixtures/, with provenance)."""
import base64
import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec

from raw_recorder.novig.signing import NovigSigner, string_to_sign

SPEC = json.loads((Path(__file__).parent / "fixtures" / "novig_signature_vectors.json").read_text())
VECTORS = SPEC["vectors"]


def test_all_thirty_vectors_present():
    assert len(VECTORS) == 30


@pytest.mark.parametrize("vec", VECTORS, ids=[v["id"] for v in VECTORS])
def test_vector(vec):
    inp = vec["input"]
    body = inp["body"].encode("utf-8")
    sts = string_to_sign(inp["timestamp"], inp["method"], inp["path"], inp["query"], body)
    assert sts == vec["string_to_sign"]

    pair = SPEC["keypairs"][vec["keypair_id"]]
    signer = NovigSigner.from_pem("test-key", pair["private_key_pkcs8_pem"].encode())
    headers = signer.headers(inp["method"], inp["path"], inp["query"], body, ts_ms=inp["timestamp"])
    assert headers["Novig-Timestamp"] == str(inp["timestamp"])
    assert headers["Novig-Key-Id"] == "test-key"
    sig = base64.b64decode(headers["Novig-Signature"], validate=True)

    if vec["algorithm"] == "ed25519":
        # Ed25519 is deterministic: the signature must match byte for byte.
        assert headers["Novig-Signature"] == vec["signature"]
    else:
        # ECDSA is randomized: verify ours and the published one with the public key.
        public = serialization.load_pem_public_key(pair["public_key_spki_pem"].encode())
        for s in (sig, base64.b64decode(vec["signature"])):
            public.verify(s, sts.encode(), ec.ECDSA(hashes.SHA256()))


def test_key_file_must_not_be_group_readable(tmp_path):
    pem = SPEC["keypairs"]["ed25519-test-1"]["private_key_pkcs8_pem"]
    path = tmp_path / "read.pem"
    path.write_text(pem)
    path.chmod(0o640)
    with pytest.raises(PermissionError):
        NovigSigner.from_pem_file("k", path)
    path.chmod(0o600)
    assert NovigSigner.from_pem_file("k", path).key_id == "k"
