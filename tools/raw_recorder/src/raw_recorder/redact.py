"""Redaction applied before request metadata is archived (policy "r0-v1").

Removes credentials, signatures, cookies and API keys from headers and query
strings (DESIGN.md §7.1, §12). Response bodies are archived verbatim; no vendor
used by R0 returns secrets in a body. The Novig key ID is an identifier, not a
credential, and is kept so the archive shows which key collected the data.
"""
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from urllib.parse import unquote, urlsplit

REDACTED = "[redacted]"
SENSITIVE_HEADERS = frozenset({
    "authorization", "proxy-authorization", "cookie", "set-cookie",
    "novig-signature", "x-api-key", "kalshi-access-signature", "kalshi-access-key",
})
SENSITIVE_QUERY = frozenset({"apikey", "api_key", "key", "token", "signature", "access_token"})
_URL_RE = re.compile(r"(?:https?|wss?)://[^\s'\"<>]+")
MIN_SECRET_LEN = 4   # shorter values would redact ordinary text


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


def secret_values(raw_query: str, items: Mapping[str, str] | Iterable[tuple[str, str]] = ()) -> set[str]:
    """The secret values carried by one request: sensitive query parameters
    and sensitive headers."""
    secrets = set()
    for pair in raw_query.split("&") if raw_query else ():
        name, _, value = pair.partition("=")
        if name.lower() in SENSITIVE_QUERY and value:
            secrets.update({value, unquote(value)})   # as sent, and as a library may print it
    pairs = items.items() if isinstance(items, Mapping) else items
    secrets.update(v for k, v in pairs if k.lower() in SENSITIVE_HEADERS and v)
    return secrets


def scrub(text: str, secrets: Iterable[str] = ()) -> str:
    """Make free text (exception messages, tracebacks) safe to archive.

    Library exceptions embed request URLs, bypassing header/query redaction.
    Every embedded URL gets its query redacted, then every known secret value
    is replaced wherever it still appears.
    """
    def _url(match: re.Match) -> str:
        url = match.group(0)
        base, sep, q = url.partition("?")
        return f"{base}{sep}{query(q)}" if sep else url

    out = _URL_RE.sub(_url, text)
    for secret in sorted((s for s in secrets if s and len(s) >= MIN_SECRET_LEN), key=len, reverse=True):
        out = out.replace(secret, REDACTED)
    return out
