from __future__ import annotations

import logging
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from .audit import audit
from .auth import Authenticator, Authorizer, Principal
from .backend import OriginProxy
from .config import GatewaySettings, OperationClass
from .errors import GatewayError, GitClientError
from .git import classify_git_operation
from .metrics import GatewayMetrics
from .rate_limit import RateLimiter
from .validation import parse_git_route, validate_repo, validate_tenant

logger = logging.getLogger("origin_gateway")


def create_app(
    settings: GatewaySettings | None = None,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    settings = settings or GatewaySettings()
    metrics = GatewayMetrics()
    authenticator = Authenticator(settings.effective_auth_tokens())
    authorizer = Authorizer()
    rate_limiter = RateLimiter(settings.effective_rate_limits())
    proxy = OriginProxy(settings, metrics, transport=transport)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        yield
        await proxy.aclose()

    app = FastAPI(
        title="Origin Gateway API",
        version="0.1.0",
        docs_url="/docs" if settings.environment != "production" else None,
        redoc_url="/redoc" if settings.environment != "production" else None,
        openapi_url="/openapi.json" if settings.environment != "production" else None,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.metrics = metrics
    app.state.proxy = proxy

    @app.middleware("http")
    async def request_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        operation = "unknown"
        started = time.monotonic()
        response: Response | None = None
        try:
            response = await call_next(request)
            return response
        finally:
            status = response.status_code if response is not None else 500
            operation = getattr(request.state, "operation", operation)
            latency = time.monotonic() - started
            metrics.requests.labels(operation, str(status)).inc()
            metrics.latency.labels(operation).observe(latency)
            log_level = (
                logging.ERROR
                if status >= 500
                else logging.WARNING if status >= 400 else logging.INFO
            )
            logger.log(
                log_level,
                "request completed",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "operation": operation,
                    "status": status,
                    "latency_ms": round(latency * 1000, 3),
                    "client_ip": client_ip(request, settings=settings),
                    "tenant": getattr(request.state, "tenant", None),
                    "repo": getattr(request.state, "repo", None),
                    "actor": getattr(request.state, "actor", None),
                },
            )
            if response is not None:
                response.headers["x-request-id"] = request_id

    @app.middleware("http")
    async def authentication_context(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        if is_public_path(request.url.path):
            return await call_next(request)
        try:
            request.state.principal = authenticator.authenticate(
                request, git_client=is_git_request_path(request.url.path)
            )
        except GatewayError as error:
            request.state.auth_error = error
        return await call_next(request)

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, error: GatewayError) -> Response:
        request.state.operation = getattr(request.state, "operation", "unknown")
        headers = dict(error.headers or {})
        if isinstance(error, GitClientError):
            return PlainTextResponse(
                f"{error.message}\n",
                status_code=error.status_code,
                headers=headers,
            )
        return JSONResponse(
            error.json_body(getattr(request.state, "request_id", None)),
            status_code=error.status_code,
            headers=headers,
        )

    @app.get("/healthz", status_code=204)
    async def healthz() -> Response:
        return Response(status_code=204)

    @app.get("/readyz")
    async def readyz() -> Response:
        request_id = "system"
        status = await proxy.readiness()
        code = 200 if status["ready"] else 503
        return JSONResponse({"request_id": request_id, **status}, status_code=code)

    @app.get("/metrics")
    async def prometheus_metrics(request: Request) -> Response:
        request.state.operation = OperationClass.ADMIN.value
        principal = authenticate(request, authenticator, metrics, settings, git_client=False)
        request.state.actor = principal.actor
        ensure_admin(principal)
        return metrics.response()

    @app.get("/admin/repos")
    async def admin_repos(request: Request) -> Response:
        request.state.operation = OperationClass.ADMIN.value
        principal = authenticate(request, authenticator, metrics, settings, git_client=False)
        request.state.actor = principal.actor
        ensure_admin(principal)
        check_rate_limit(
            request, rate_limiter, metrics, settings, OperationClass.ADMIN, "*", "*", principal
        )
        audit("admin_repos_listed", request_id=request.state.request_id, actor=principal.actor)
        upstream = await proxy.proxy_control_json(request, path="/api/repos")
        return relay_repo_listing_response(upstream, principal, settings)

    @app.get("/admin/repos/{tenant}/{repository}/metadata")
    @app.get("/admin/repos/{tenant}/{repository}/refs")
    async def admin_repo_metadata(tenant: str, repository: str, request: Request) -> Response:
        tenant = validate_tenant(tenant)
        repository = validate_repo(repository)
        set_route_context(request, OperationClass.ADMIN, tenant, repository)
        principal = authenticate(request, authenticator, metrics, settings, git_client=False)
        request.state.actor = principal.actor
        authorizer.authorize(
            principal,
            tenant=tenant,
            repository=repository,
            operation=OperationClass.ADMIN,
        )
        enforce_allowed_tenants(settings, tenant)
        check_rate_limit(
            request,
            rate_limiter,
            metrics,
            settings,
            OperationClass.ADMIN,
            tenant,
            repository,
            principal,
        )
        audit(
            "admin_repo_metadata_read",
            request_id=request.state.request_id,
            tenant=tenant,
            repo=repository,
            actor=principal.actor,
        )
        upstream = await proxy.proxy_control_json(
            request, path=f"/api/repos/{tenant}/{repository}/refs"
        )
        return relay_jsonish_response(upstream)

    @app.post("/admin/repos/{tenant}/{repository}/compact")
    async def admin_repo_compact(tenant: str, repository: str, request: Request) -> Response:
        tenant = validate_tenant(tenant)
        repository = validate_repo(repository)
        set_route_context(request, OperationClass.ADMIN, tenant, repository)
        principal = authenticate(request, authenticator, metrics, settings, git_client=False)
        request.state.actor = principal.actor
        authorizer.authorize(
            principal,
            tenant=tenant,
            repository=repository,
            operation=OperationClass.ADMIN,
        )
        enforce_allowed_tenants(settings, tenant)
        check_rate_limit(
            request,
            rate_limiter,
            metrics,
            settings,
            OperationClass.ADMIN,
            tenant,
            repository,
            principal,
        )
        audit(
            "admin_compaction_rejected_unsupported",
            request_id=request.state.request_id,
            tenant=tenant,
            repo=repository,
            actor=principal.actor,
        )
        raise GatewayError(
            "unsupported",
            "Origin does not currently expose an HTTP compaction trigger",
            501,
        )

    @app.api_route("/{tenant}/{repo_and_path:path}", methods=["GET", "POST"])
    async def git_proxy(tenant: str, repo_and_path: str, request: Request) -> Response:
        tenant, repository, git_path = parse_git_route(tenant, repo_and_path)
        operation = classify_git_operation(request.method, git_path, request.url.query)
        set_route_context(request, operation.operation_class, tenant, repository)
        principal = authenticate(request, authenticator, metrics, settings, git_client=True)
        request.state.actor = principal.actor
        authorizer.authorize(
            principal,
            tenant=tenant,
            repository=repository,
            operation=operation.operation_class,
            git_client=True,
        )
        enforce_allowed_tenants(settings, tenant, git_client=True)
        check_rate_limit(
            request,
            rate_limiter,
            metrics,
            settings,
            operation.operation_class,
            tenant,
            repository,
            principal,
            git_client=True,
        )
        audit(
            "git_request_allowed",
            request_id=request.state.request_id,
            tenant=tenant,
            repo=repository,
            actor=principal.actor,
            operation=operation.operation_class.value,
        )
        return await proxy.proxy_git(
            request,
            tenant=tenant,
            repository=repository,
            git_path=git_path,
            operation=operation.operation_class,
            request_id=request.state.request_id,
            mutating=operation.is_mutating,
        )

    return app


def authenticate(
    request: Request,
    authenticator: Authenticator,
    metrics: GatewayMetrics,
    settings: GatewaySettings,
    *,
    git_client: bool,
) -> Principal:
    try:
        if hasattr(request.state, "auth_error"):
            raise request.state.auth_error
        principal = getattr(request.state, "principal", None)
        if principal is None:
            principal = authenticator.authenticate(request, git_client=git_client)
    except GatewayError:
        metrics.auth_failures.labels(getattr(request.state, "operation", "unknown")).inc()
        audit(
            "auth_rejected",
            request_id=request.state.request_id,
            tenant=getattr(request.state, "tenant", None),
            repo=getattr(request.state, "repo", None),
            operation=getattr(request.state, "operation", "unknown"),
            client_ip=client_ip(request, settings=settings),
        )
        raise
    audit(
        "auth_allowed",
        request_id=request.state.request_id,
        actor=principal.actor,
        tenant=getattr(request.state, "tenant", None),
        repo=getattr(request.state, "repo", None),
        operation=getattr(request.state, "operation", "unknown"),
    )
    return principal


def ensure_admin(principal: Principal) -> None:
    if "admin" not in principal.scopes:
        raise GatewayError("forbidden", "operation is not allowed", 403)


def enforce_allowed_tenants(
    settings: GatewaySettings, tenant: str, *, git_client: bool = False
) -> None:
    if settings.allowed_tenants and tenant not in settings.allowed_tenants:
        error_cls = GitClientError if git_client else GatewayError
        raise error_cls("forbidden", "tenant is not served by this gateway", 403)


def check_rate_limit(
    request: Request,
    limiter: RateLimiter,
    metrics: GatewayMetrics,
    settings: GatewaySettings,
    operation: OperationClass,
    tenant: str,
    repository: str,
    principal: Principal,
    *,
    git_client: bool = False,
) -> None:
    decision = limiter.check(
        tenant=tenant,
        actor=principal.actor,
        repository=repository,
        ip_address=client_ip(request, settings=settings),
        operation=operation,
    )
    audit(
        "rate_limit_decision",
        request_id=request.state.request_id,
        tenant=tenant,
        repo=repository,
        actor=principal.actor,
        operation=operation.value,
        allowed=decision.allowed,
        remaining=decision.remaining,
    )
    if not decision.allowed:
        metrics.rate_limit_rejects.labels(operation.value).inc()
        headers = {"Retry-After": str(decision.retry_after_seconds)}
        error_cls = GitClientError if git_client else GatewayError
        raise error_cls("rate_limited", "rate limit exceeded", 429, headers)


def set_route_context(
    request: Request, operation: OperationClass, tenant: str, repository: str
) -> None:
    request.state.operation = operation.value
    request.state.tenant = tenant
    request.state.repo = repository


def client_ip(request: Request, *, settings: GatewaySettings | None = None) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    if settings is not None and settings.trust_forwarded_for and forwarded:
        return forwarded.split(",", maxsplit=1)[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def is_public_path(path: str) -> bool:
    return path in {"/healthz", "/readyz", "/openapi.json"} or path.startswith(("/docs", "/redoc"))


def is_git_request_path(path: str) -> bool:
    return not path.startswith("/admin")


def relay_jsonish_response(upstream: httpx.Response) -> Response:
    content_type = upstream.headers.get("content-type", "application/json")
    headers = {
        name: value
        for name, value in upstream.headers.items()
        if name.lower() not in {"content-length", "connection", "transfer-encoding"}
    }
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=content_type,
        headers=headers,
    )


def relay_repo_listing_response(
    upstream: httpx.Response, principal: Principal, settings: GatewaySettings
) -> Response:
    if upstream.status_code != 200 or "json" not in upstream.headers.get("content-type", ""):
        return relay_jsonish_response(upstream)
    try:
        payload = upstream.json()
    except ValueError:
        return relay_jsonish_response(upstream)
    repositories = payload.get("repositories")
    if not isinstance(repositories, list):
        return relay_jsonish_response(upstream)
    payload["repositories"] = [
        item
        for item in repositories
        if isinstance(item, dict)
        and repo_visible_to_principal(item.get("tenant"), principal, settings)
    ]
    return JSONResponse(payload, status_code=upstream.status_code)


def repo_visible_to_principal(
    tenant: object, principal: Principal, settings: GatewaySettings
) -> bool:
    if not isinstance(tenant, str):
        return False
    if settings.allowed_tenants and tenant not in settings.allowed_tenants:
        return False
    return "*" in principal.tenants or not principal.tenants or tenant in principal.tenants
