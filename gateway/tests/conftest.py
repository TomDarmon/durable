from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

import httpx
import pytest

from origin_gateway.app import create_app
from origin_gateway.config import GatewaySettings, RateLimitRule, TokenPrincipal

DEV_TOKEN = "dev-token-123456"


class ClientFactory(Protocol):
    def __call__(
        self,
        transport: httpx.AsyncBaseTransport,
        settings: GatewaySettings | None = None,
    ) -> AsyncIterator[httpx.AsyncClient]: ...


def test_settings(**overrides: object) -> GatewaySettings:
    defaults: dict[str, object] = {
        "environment": "test",
        "origin_backends": ["http://origin-one", "http://origin-two"],
        "control_backend_url": "http://origin-control",
        "auth_tokens": [
            TokenPrincipal(
                token=DEV_TOKEN,
                actor="alice",
                tenants=["tenant"],
                repositories=["*"],
                scopes=["git:read", "git:write", "metadata:read", "admin"],
            )
        ],
        "rate_limits": {
            "git_receive_pack": RateLimitRule(capacity=10, refill_per_second=1.0),
            "git_upload_pack": RateLimitRule(capacity=10, refill_per_second=1.0),
            "git_metadata": RateLimitRule(capacity=10, refill_per_second=1.0),
            "admin": RateLimitRule(capacity=10, refill_per_second=1.0),
        },
    }
    defaults.update(overrides)
    return GatewaySettings(**defaults)


@pytest.fixture
def auth_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DEV_TOKEN}"}


@pytest.fixture
def client_factory() -> ClientFactory:
    async def factory(
        transport: httpx.AsyncBaseTransport,
        settings: GatewaySettings | None = None,
    ) -> AsyncIterator[httpx.AsyncClient]:
        app = create_app(settings or test_settings(), transport=transport)
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://gateway",
        ) as client:
            yield client

    return factory
