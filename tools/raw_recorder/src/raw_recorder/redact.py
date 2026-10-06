"""Redaction applied before request metadata is archived (policy "r0-v1").

Removes credentials, signatures, cookies and API keys from headers and query
strings (DESIGN.md §7.1, §12). Response bodies are archived verbatim; no vendor
used by R0 returns secrets in a body. The Novig key ID is an identifier, not a
credential, and is kept so the archive shows which key collected the data.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from urllib.parse import urlsplit

REDACTED = "[redacted]"
SENSITIVE_HEADERS = frozenset({
    "authorization", "proxy-authorization", "cookie", "set-cookie",
    "novig-signature", "x-api-key", "kalshi-access-signature", "kalshi-access-key",
})
SENSITIVE_QUERY = frozenset({"apikey", "api_key", "key", "token", "signature", "access_token"})


def headers(items: Mapping[str, str] | Iterable[tuple[str, str]]) -> list[list[str]]:
    """Headers as an ordered [name, value] list (repeats kept), secrets redacted."""
    pairs = items.items() if isinstance(items, Mapping) else items
    return [[k, REDACTED if k.lower() in SENSITIVE_HEADERS else v] for k, v in pairs]


def query(raw_query: str) -> str:
    """Redact sensitive parameter values; everything else stays byte-identical."""
    if not raw_query:
        return ""
    out = []
    for pair in raw_query.split("&"):
        name, sep, _ = pair.partition("=")
        out.append(f"{name}{sep}{REDACTED}" if name.lower() in SENSITIVE_QUERY else pair)
    return "&".join(out)


def url_parts(url: str) -> dict:
    parts = urlsplit(url)
    return {"origin": f"{parts.scheme}://{parts.netloc}", "path": parts.path,
            "query": query(parts.query)}
