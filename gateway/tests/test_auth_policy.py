from __future__ import annotations

import base64

import pytest
from starlette.requests import Request

from origin_gateway.auth import Authenticator, Authorizer
from origin_gateway.config import OperationClass, TokenPrincipal
from origin_gateway.errors import GatewayError


def make_request(headers: dict[str, str]) -> Request:
    return Request(
        {"type": "http", "headers": [(k.lower().encode(), v.encode()) for k, v in headers.items()]}
    )


def test_authenticator_accepts_bearer_tokens() -> None:
    authenticator = Authenticator(
        [
            TokenPrincipal(
                token="secret-token-123",
                actor="alice",
                tenants=["tenant"],
                scopes=["git:read"],
            )
        ]
    )

    principal = authenticator.authenticate(
        make_request({"Authorization": "Bearer secret-token-123"})
    )

    assert principal.actor == "alice"
    assert principal.tenants == frozenset({"tenant"})


def test_authenticator_accepts_git_basic_password_token() -> None:
    token = base64.b64encode(b"alice:secret-token-123").decode()
    authenticator = Authenticator(
        [
            TokenPrincipal(
                token="secret-token-123",
                actor="alice",
                tenants=["tenant"],
                scopes=["git:read"],
            )
        ]
    )

    principal = authenticator.authenticate(make_request({"Authorization": f"Basic {token}"}))

    assert principal.actor == "alice"


def test_authorizer_rejects_missing_scope() -> None:
    principal = Authenticator(
        [
            TokenPrincipal(
                token="secret-token-123",
                actor="alice",
                tenants=["tenant"],
                scopes=["git:read"],
            )
        ]
    ).authenticate(make_request({"Authorization": "Bearer secret-token-123"}))

    with pytest.raises(GatewayError):
        Authorizer().authorize(
            principal,
            tenant="tenant",
            repository="repo",
            operation=OperationClass.GIT_RECEIVE_PACK,
        )
