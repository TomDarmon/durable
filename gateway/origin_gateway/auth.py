from __future__ import annotations

import base64
import hmac
from dataclasses import dataclass

from fastapi import Request

from .config import OperationClass, TokenPrincipal
from .errors import GatewayError, GitClientError


@dataclass(frozen=True)
class Principal:
    actor: str
    tenants: frozenset[str]
    repositories: frozenset[str]
    scopes: frozenset[str]


class Authenticator:
    def __init__(self, tokens: list[TokenPrincipal]) -> None:
        self._tokens = {token.token: token for token in tokens}

    def authenticate(self, request: Request, *, git_client: bool = False) -> Principal:
        token = self._extract_token(request)
        if token is None:
            raise self._auth_error("authentication required", git_client)
        for known_token, config in self._tokens.items():
            if hmac.compare_digest(token, known_token):
                return Principal(
                    actor=config.actor,
                    tenants=frozenset(config.tenants),
                    repositories=frozenset(config.repositories),
                    scopes=frozenset(config.scopes),
                )
        raise self._auth_error("invalid credentials", git_client)

    def _extract_token(self, request: Request) -> str | None:
        header = request.headers.get("authorization", "")
        if header.lower().startswith("bearer "):
            return header[7:].strip()
        if header.lower().startswith("basic "):
            encoded = header[6:].strip()
            try:
                decoded = base64.b64decode(encoded, validate=True).decode("utf-8")
            except (ValueError, UnicodeDecodeError):
                return None
            username, separator, password = decoded.partition(":")
            return password if separator else username
        api_key = request.headers.get("x-origin-gateway-token")
        if api_key:
            return api_key.strip()
        return None

    def _auth_error(self, message: str, git_client: bool) -> GatewayError:
        headers = {"WWW-Authenticate": 'Basic realm="Origin Gateway"'}
        if git_client:
            return GitClientError("unauthorized", message, 401, headers)
        return GatewayError("unauthorized", message, 401, headers)


class Authorizer:
    def authorize(
        self,
        principal: Principal,
        *,
        tenant: str,
        repository: str | None,
        operation: OperationClass,
        git_client: bool = False,
    ) -> None:
        required_scope = {
            OperationClass.GIT_RECEIVE_PACK: "git:write",
            OperationClass.GIT_UPLOAD_PACK: "git:read",
            OperationClass.GIT_METADATA: "metadata:read",
            OperationClass.ADMIN: "admin",
        }[operation]
        if required_scope not in principal.scopes:
            raise self._forbidden("operation is not allowed", git_client)
        if principal.tenants and "*" not in principal.tenants and tenant not in principal.tenants:
            raise self._forbidden("tenant is not allowed", git_client)
        if repository is not None:
            repo_key = f"{tenant}/{repository}"
            allowed = principal.repositories
            if (
                allowed
                and "*" not in allowed
                and repository not in allowed
                and repo_key not in allowed
            ):
                raise self._forbidden("repository is not allowed", git_client)

    def _forbidden(self, message: str, git_client: bool) -> GatewayError:
        if git_client:
            return GitClientError("forbidden", message, 403)
        return GatewayError("forbidden", message, 403)
