"""The REST envelope (DESIGN.md §7.1).

Every attempted request, including failures and timeouts, has a unique
request_id used as the conn_id of all its archive records:

1. `event` rest_request: method, sanitized origin/path/query and headers,
   requested subjects, wall-clock start and session-monotonic time.
2. `out` frame: our own request body, if any.
3. `in` frame: the response body verbatim, even when empty, HTML or not JSON.
4. `event` rest_complete: status, sanitized response headers (quota and rate
   headers included), body hash and size, timings, and the failure reason.

Responses are associated by request_id, never by adjacency: several pollers
share nothing, and one poller's requests may interleave.
"""
from __future__ import annotations

import asyncio
import hashlib
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from urllib.parse import quote, urlencode

import aiohttp
from yarl import URL

from . import redact
from .archive import Stream

# (method, path, raw_query, body) -> headers to add (e.g. a NOVIG-V3 signature)
Signer = Callable[[str, str, str, bytes], dict[str, str]]


@dataclass(frozen=True)
class RestResult:
    request_id: str
    status: int | None
    headers: dict[str, str]
    body: bytes | None
    failure: str | None

    @property
    def ok(self) -> bool:
        return self.failure is None and self.status is not None and 200 <= self.status < 300


def build_query(params: dict | Sequence[tuple[str, str]] | None) -> str:
    """Percent-encode with %20 for spaces (never '+'), so the bytes we sign and
    the bytes we send are the same string."""
    if not params:
        return ""
    return urlencode(params, quote_via=quote, safe="")


class RestRecorder:
    def __init__(self, http: aiohttp.ClientSession, stream: Stream, *, timeout_s: float):
        self.http = http
        self.stream = stream
        self.timeout_s = timeout_s

    async def request(self, method: str, base: str, path: str, *,
                      params: dict | Sequence[tuple[str, str]] | None = None,
                      body: bytes | None = None, headers: dict[str, str] | None = None,
                      subjects: Sequence[str] = (), purpose: str = "",
                      signer: Signer | None = None) -> RestResult:
        method = method.upper()
        request_id = str(uuid.uuid4())
        raw_query = build_query(params)
        url_text = f"{base.rstrip('/')}{path}" + (f"?{raw_query}" if raw_query else "")
        req_headers = dict(headers or {})
        payload = body if body is not None else b""
        if body is not None:
            req_headers.setdefault("Content-Type", "application/json")
        if signer is not None:
            req_headers.update(signer(method, path, raw_query, payload))

        secrets = redact.secret_values(raw_query, req_headers)
        parts = redact.url_parts(url_text)
        self.stream.event(
            request_id, "rest_request", request_id=request_id, method=method,
            origin=parts["origin"], path=parts["path"], query=parts["query"],
            request_headers=redact.headers(req_headers), subjects=list(subjects),
            purpose=purpose, start_ts_ms=time.time_ns() // 1_000_000,
            body_sha256=hashlib.sha256(payload).hexdigest() if body is not None else None,
        )
        if body is not None:
            self.stream.write("out", request_id, body.decode("utf-8", "surrogateescape"))

        start_mono = time.monotonic_ns()
        status, resp_headers, data, failure = None, [], None, None
        headers_ts = None
        try:
            async with self.http.request(
                method, URL(url_text, encoded=True), data=body, headers=req_headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout_s), allow_redirects=False,
            ) as resp:
                headers_ts = time.time_ns() // 1_000_000
                status = resp.status
                resp_headers = list(resp.raw_headers)
                data = await resp.read()
        except asyncio.CancelledError:
            # Shutdown cancels in-flight requests: still close the envelope with
            # whatever arrived, then let the cancellation propagate.
            self._complete(request_id, status, resp_headers, None, headers_ts, start_mono, "cancelled")
            raise
        except asyncio.TimeoutError:
            failure = "timeout"
        except aiohttp.ClientResponseError as exc:
            # str() of this error embeds the full request URL: record fields.
            failure = (f"response_error: {type(exc).__name__}: status={exc.status} "
                       f"message={redact.scrub(str(exc.message), secrets)}")
        except aiohttp.ClientConnectionError as exc:
            failure = f"connect_error: {type(exc).__name__}: {redact.scrub(str(exc), secrets)}"
        except aiohttp.ClientError as exc:
            failure = f"client_error: {type(exc).__name__}: {redact.scrub(str(exc), secrets)}"

        if data is not None:
            self.stream.write("in", request_id, data.decode("utf-8", "surrogateescape"))
        decoded = self._complete(request_id, status, resp_headers, data, headers_ts, start_mono, failure)
        return RestResult(request_id, status, {k.lower(): v for k, v in decoded}, data, failure)

    def _complete(self, request_id: str, status: int | None, resp_headers: list,
                  data: bytes | None, headers_ts: int | None, start_mono: int,
                  failure: str | None) -> list[tuple[str, str]]:
        decoded = [(k.decode("latin-1"), v.decode("latin-1")) for k, v in resp_headers]
        self.stream.event(
            request_id, "rest_complete", request_id=request_id, status=status,
            response_headers=redact.headers(decoded),
            body_bytes=None if data is None else len(data),
            body_sha256=None if data is None else hashlib.sha256(data).hexdigest(),
            headers_ts_ms=headers_ts, complete_ts_ms=time.time_ns() // 1_000_000,
            elapsed_mono_ns=time.monotonic_ns() - start_mono, failure=failure,
        )
        return decoded
