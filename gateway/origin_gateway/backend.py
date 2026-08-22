from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
from fastapi import Request
from starlette.background import BackgroundTask
from starlette.responses import StreamingResponse

from .config import GatewaySettings, OperationClass
from .errors import GatewayError, GitClientError
from .metrics import GatewayMetrics

HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
}

SENSITIVE_FORWARD_HEADERS = {
    "authorization",
    "cookie",
    "x-origin-gateway-token",
}


@dataclass
class BackendState:
    url: str
    failures: int = 0
    opened_until: float = 0.0


class BackendPool:
    def __init__(self, settings: GatewaySettings) -> None:
        self._settings = settings
        self._backends = [BackendState(str(url).rstrip("/")) for url in settings.origin_backends]
        self._next = 0
        self._lock = asyncio.Lock()

    async def select(self) -> BackendState:
        now = time.monotonic()
        async with self._lock:
            candidates = [
                backend for backend in self._backends if backend.opened_until <= now
            ] or self._backends
            backend = candidates[self._next % len(candidates)]
            self._next += 1
            return backend

    def record_success(self, backend: BackendState) -> None:
        backend.failures = 0
        backend.opened_until = 0.0

    def record_failure(self, backend: BackendState) -> None:
        backend.failures += 1
        if backend.failures >= self._settings.circuit_breaker_failures:
            backend.opened_until = time.monotonic() + self._settings.circuit_breaker_reset_seconds

    def urls(self) -> list[str]:
        return [backend.url for backend in self._backends]


class OriginProxy:
    def __init__(
        self,
        settings: GatewaySettings,
        metrics: GatewayMetrics,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        timeout = httpx.Timeout(
            connect=settings.backend_connect_timeout_seconds,
            read=settings.backend_read_timeout_seconds,
            write=settings.backend_write_timeout_seconds,
            pool=settings.backend_pool_timeout_seconds,
        )
        self._settings = settings
        self._metrics = metrics
        self._pool = BackendPool(settings)
        self._client = httpx.AsyncClient(
            timeout=timeout, follow_redirects=False, transport=transport
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def readiness(self) -> dict[str, Any]:
        checks: list[dict[str, Any]] = []
        healthy = True
        for url in self._pool.urls():
            status = "unreachable"
            try:
                response = await self._client.get(f"{url}/healthz")
                status = "ready" if response.status_code == 204 else f"http_{response.status_code}"
            except httpx.HTTPError:
                pass
            healthy = healthy and status == "ready"
            checks.append({"url": url, "status": status})
        return {"ready": healthy, "backends": checks}

    async def proxy_git(
        self,
        request: Request,
        *,
        tenant: str,
        repository: str,
        git_path: str,
        operation: OperationClass,
        request_id: str,
        mutating: bool,
    ) -> StreamingResponse:
        content_length = request.headers.get("content-length")
        if content_length:
            try:
                length = int(content_length)
            except ValueError as error:
                raise GitClientError(
                    "invalid_content_length", "content length is malformed", 400
                ) from error
            if length > self._settings.request_body_limit_bytes:
                raise GitClientError("request_too_large", "request body is too large", 413)
        attempts = 1
        if not mutating and request.method.upper() == "GET":
            attempts += self._settings.backend_max_retries
        last_error: Exception | None = None
        for _ in range(attempts):
            backend = await self._pool.select()
            target = f"{backend.url}/{tenant}/{repository}.git/{git_path}"
            if request.url.query:
                target = f"{target}?{request.url.query}"
            try:
                upstream = await self._send_streaming_request(
                    request, target, operation=operation, request_id=request_id
                )
                if upstream.status_code >= 500 and attempts > 1:
                    self._pool.record_failure(backend)
                    self._metrics.backend_errors.labels(backend.url, operation.value).inc()
                    await upstream.aclose()
                    continue
                self._pool.record_success(backend)
                return self._streaming_response(upstream, operation)
            except GitClientError:
                raise
            except httpx.HTTPError as error:
                last_error = error
                self._pool.record_failure(backend)
                self._metrics.backend_errors.labels(backend.url, operation.value).inc()
        detail = str(last_error) if last_error else "origin backend unavailable"
        raise GitClientError("backend_unavailable", detail, 503)

    async def proxy_control_json(
        self,
        request: Request,
        *,
        path: str,
        operation: OperationClass = OperationClass.ADMIN,
    ) -> httpx.Response:
        if self._settings.control_backend_url is None:
            raise GatewayError(
                "unsupported",
                "control backend URL is not configured for this operation",
                501,
            )
        if request.headers.get("content-length"):
            try:
                length = int(request.headers["content-length"])
            except ValueError as error:
                raise GatewayError(
                    "invalid_content_length", "content length is malformed", 400
                ) from error
            if length > self._settings.admin_body_limit_bytes:
                raise GatewayError("request_too_large", "request body is too large", 413)
        base = str(self._settings.control_backend_url).rstrip("/")
        target = f"{base}{path}"
        if request.url.query:
            target = f"{target}?{request.url.query}"
        headers = _forward_headers(request.headers, request_id=request.headers.get("x-request-id"))
        try:
            response = await self._client.request(request.method, target, headers=headers)
        except httpx.HTTPError as error:
            self._metrics.backend_errors.labels(base, operation.value).inc()
            raise GatewayError("backend_unavailable", str(error), 503) from error
        if response.status_code >= 500:
            self._metrics.backend_errors.labels(base, operation.value).inc()
        return response

    async def _send_streaming_request(
        self,
        request: Request,
        target: str,
        *,
        operation: OperationClass,
        request_id: str,
    ) -> httpx.Response:
        headers = _forward_headers(request.headers, request_id=request_id)
        stream = self._limited_body_stream(request, operation)
        upstream_request = self._client.build_request(
            request.method,
            target,
            headers=headers,
            content=stream,
        )
        return await self._client.send(upstream_request, stream=True)

    async def _limited_body_stream(
        self, request: Request, operation: OperationClass
    ) -> AsyncIterator[bytes]:
        seen = 0
        async for chunk in request.stream():
            seen += len(chunk)
            if seen > self._settings.request_body_limit_bytes:
                raise GitClientError("request_too_large", "request body is too large", 413)
            if chunk:
                self._metrics.bytes_proxied.labels(operation.value, "request").inc(len(chunk))
                yield chunk

    def _streaming_response(
        self, upstream: httpx.Response, operation: OperationClass
    ) -> StreamingResponse:
        async def body() -> AsyncIterator[bytes]:
            if upstream.is_stream_consumed:
                if upstream.content:
                    self._metrics.bytes_proxied.labels(operation.value, "response").inc(
                        len(upstream.content)
                    )
                    yield upstream.content
                return
            async for chunk in upstream.aiter_raw():
                if chunk:
                    self._metrics.bytes_proxied.labels(operation.value, "response").inc(len(chunk))
                    yield chunk

        headers = {
            name: value
            for name, value in upstream.headers.items()
            if name.lower() not in HOP_BY_HOP_HEADERS
        }
        return StreamingResponse(
            body(),
            status_code=upstream.status_code,
            headers=headers,
            background=BackgroundTask(upstream.aclose),
        )


def _forward_headers(headers: httpx.Headers | Any, request_id: str | None) -> dict[str, str]:
    forwarded = {
        name: value
        for name, value in headers.items()
        if name.lower() not in HOP_BY_HOP_HEADERS | SENSITIVE_FORWARD_HEADERS
    }
    if request_id:
        forwarded["x-request-id"] = request_id
    return forwarded
