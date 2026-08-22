from __future__ import annotations

import json

import httpx
import pytest
from conftest import DEV_TOKEN, ClientFactory
from conftest import test_settings as make_test_settings

from origin_gateway.config import RateLimitRule


@pytest.mark.asyncio
async def test_unauthorized_git_request_is_git_compatible(
    client_factory: ClientFactory,
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.get("/tenant/repo.git/info/refs?service=git-upload-pack")

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == 'Basic realm="Origin Gateway"'
    assert response.headers["content-type"].startswith("text/plain")
    assert "authentication required" in response.text


@pytest.mark.asyncio
async def test_git_request_is_streamed_to_selected_origin_backend(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    seen: list[tuple[str, str, bytes, str | None, str | None, str | None]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.url.host or "",
                request.url.path,
                await request.aread(),
                request.headers.get("x-request-id"),
                request.headers.get("authorization"),
                request.headers.get("x-origin-gateway-token"),
            )
        )
        return httpx.Response(
            200,
            content=b"001e# service=git-upload-pack\n0000",
            headers={"content-type": "application/x-git-upload-pack-advertisement"},
        )

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.post(
            "/tenant/repo.git/git-upload-pack",
            headers={**auth_headers, "x-request-id": "req-1"},
            content=b"0000",
        )

    assert response.status_code == 200
    assert response.content == b"001e# service=git-upload-pack\n0000"
    assert seen == [
        ("origin-one", "/tenant/repo.git/git-upload-pack", b"0000", "req-1", None, None)
    ]


@pytest.mark.asyncio
async def test_unknown_git_post_is_rejected_before_proxying(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    calls = 0

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"ok")

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.post(
            "/tenant/repo.git/not-a-git-rpc",
            headers=auth_headers,
            content=b"payload",
        )

    assert response.status_code == 400
    assert response.headers["content-type"].startswith("text/plain")
    assert calls == 0


@pytest.mark.asyncio
async def test_push_rate_limit_rejection_uses_plain_text_for_git(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    settings = make_test_settings(
        rate_limits={
            "git_receive_pack": RateLimitRule(capacity=1, refill_per_second=0.01),
            "git_upload_pack": RateLimitRule(capacity=10, refill_per_second=1.0),
            "git_metadata": RateLimitRule(capacity=10, refill_per_second=1.0),
            "admin": RateLimitRule(capacity=10, refill_per_second=1.0),
        }
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"ok")

    async for client in client_factory(httpx.MockTransport(handler), settings):
        first = await client.post(
            "/tenant/repo.git/git-receive-pack", headers=auth_headers, content=b"0000"
        )
        second = await client.post(
            "/tenant/repo.git/git-receive-pack", headers=auth_headers, content=b"0000"
        )

    assert first.status_code == 200
    assert second.status_code == 429
    assert second.headers["content-type"].startswith("text/plain")
    assert second.headers["retry-after"]


@pytest.mark.asyncio
async def test_admin_repo_listing_proxies_control_backend(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "origin-control"
        assert request.url.path == "/api/repos"
        assert request.headers.get("authorization") is None
        return httpx.Response(
            200,
            json={"repositories": [{"tenant": "tenant", "name": "repo"}]},
            headers={"content-type": "application/json"},
        )

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.get("/admin/repos", headers=auth_headers)

    assert response.status_code == 200
    assert response.json()["repositories"] == [{"tenant": "tenant", "name": "repo"}]


@pytest.mark.asyncio
async def test_admin_repo_listing_filters_to_authorized_tenants(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "repositories": [
                    {"tenant": "tenant", "name": "repo"},
                    {"tenant": "other-tenant", "name": "repo"},
                ]
            },
            headers={"content-type": "application/json"},
        )

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.get("/admin/repos", headers=auth_headers)

    assert response.status_code == 200
    assert response.json()["repositories"] == [{"tenant": "tenant", "name": "repo"}]


@pytest.mark.asyncio
async def test_body_limit_rejects_git_upload_before_backend_reads(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    calls = 0
    settings = make_test_settings(request_body_limit_bytes=3)

    async def handler(_request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, content=b"ok")

    async for client in client_factory(httpx.MockTransport(handler), settings):
        response = await client.post(
            "/tenant/repo.git/git-upload-pack",
            headers={**auth_headers, "content-length": "4"},
            content=b"0000",
        )

    assert response.status_code == 413
    assert calls == 0


@pytest.mark.asyncio
async def test_rate_limit_ignores_forwarded_for_by_default(
    client_factory: ClientFactory,
    auth_headers: dict[str, str],
) -> None:
    settings = make_test_settings(
        rate_limits={
            "git_receive_pack": RateLimitRule(capacity=10, refill_per_second=1.0),
            "git_upload_pack": RateLimitRule(capacity=1, refill_per_second=0.01),
            "git_metadata": RateLimitRule(capacity=10, refill_per_second=1.0),
            "admin": RateLimitRule(capacity=10, refill_per_second=1.0),
        }
    )

    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            content=b"001e# service=git-upload-pack\n0000",
            headers={"content-type": "application/x-git-upload-pack-advertisement"},
        )

    async for client in client_factory(httpx.MockTransport(handler), settings):
        first = await client.get(
            "/tenant/repo.git/info/refs?service=git-upload-pack",
            headers={**auth_headers, "x-forwarded-for": "198.51.100.10"},
        )
        second = await client.get(
            "/tenant/repo.git/info/refs?service=git-upload-pack",
            headers={**auth_headers, "x-forwarded-for": "198.51.100.11"},
        )

    assert first.status_code == 200
    assert second.status_code == 429


@pytest.mark.asyncio
async def test_readiness_checks_all_origin_backends(client_factory: ClientFactory) -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/healthz"
        status = 204 if request.url.host in {"origin-one", "origin-two"} else 404
        return httpx.Response(status)

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["ready"] is True
    assert len(response.json()["backends"]) == 2


@pytest.mark.asyncio
async def test_normal_api_errors_use_json_envelope(client_factory: ClientFactory) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=json.dumps({}))

    settings = make_test_settings(auth_tokens=[])
    async for client in client_factory(httpx.MockTransport(handler), settings):
        response = await client.get("/admin/repos")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "unauthorized"


@pytest.mark.asyncio
async def test_metrics_endpoint_exposes_gateway_counters(client_factory: ClientFactory) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async for client in client_factory(httpx.MockTransport(handler)):
        await client.get("/healthz")
        response = await client.get("/metrics", headers={"Authorization": f"Bearer {DEV_TOKEN}"})

    assert response.status_code == 200
    assert "origin_gateway_requests_total" in response.text


@pytest.mark.asyncio
async def test_metrics_endpoint_requires_auth(client_factory: ClientFactory) -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    async for client in client_factory(httpx.MockTransport(handler)):
        response = await client.get("/metrics")

    assert response.status_code == 401
