from __future__ import annotations

import json
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, PositiveInt, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class OperationClass(StrEnum):
    GIT_RECEIVE_PACK = "git_receive_pack"
    GIT_UPLOAD_PACK = "git_upload_pack"
    GIT_METADATA = "git_metadata"
    ADMIN = "admin"


class TokenPrincipal(BaseModel):
    token: str = Field(min_length=12)
    actor: str = Field(min_length=1, max_length=128)
    tenants: list[str] = Field(default_factory=list)
    repositories: list[str] = Field(default_factory=lambda: ["*"])
    scopes: list[str] = Field(default_factory=list)


class RateLimitRule(BaseModel):
    capacity: PositiveInt
    refill_per_second: float = Field(gt=0)


class GatewaySettings(BaseSettings):
    """Centralized gateway configuration.

    Environment variables use the ORIGIN_GATEWAY_ prefix, for example
    ORIGIN_GATEWAY_ORIGIN_BACKENDS=http://origin:9200,http://origin-alt:9200.
    """

    model_config = SettingsConfigDict(
        env_prefix="ORIGIN_GATEWAY_",
        env_file=".env",
        extra="ignore",
    )

    environment: Literal["development", "test", "production"] = "development"
    bind_host: str = "0.0.0.0"
    bind_port: int = 9400
    origin_backends: list[str] = Field(default_factory=lambda: ["http://127.0.0.1:9200"])
    control_backend_url: str | None = None
    auth_tokens: list[TokenPrincipal] = Field(default_factory=list)
    dev_auth_token: str | None = None
    dev_auth_actor: str = "local-dev"
    dev_auth_tenants: list[str] = Field(default_factory=lambda: ["tenant"])
    allowed_tenants: list[str] = Field(default_factory=list)
    trust_forwarded_for: bool = False
    request_body_limit_bytes: PositiveInt = 512 * 1024 * 1024
    admin_body_limit_bytes: PositiveInt = 1024 * 1024
    backend_connect_timeout_seconds: float = Field(default=2.0, gt=0)
    backend_read_timeout_seconds: float = Field(default=120.0, gt=0)
    backend_write_timeout_seconds: float = Field(default=120.0, gt=0)
    backend_pool_timeout_seconds: float = Field(default=5.0, gt=0)
    backend_max_retries: int = Field(default=1, ge=0, le=3)
    circuit_breaker_failures: PositiveInt = 3
    circuit_breaker_reset_seconds: float = Field(default=15.0, gt=0)
    log_level: str = "INFO"
    rate_limits: dict[OperationClass, RateLimitRule] = Field(default_factory=dict)

    @field_validator("origin_backends", mode="before")
    @classmethod
    def parse_backends(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                return json.loads(stripped)
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("origin_backends")
    @classmethod
    def validate_backends(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("at least one Origin backend is required")
        for item in value:
            if not item.startswith(("http://", "https://")):
                raise ValueError("Origin backend URLs must be http(s)")
        return value

    @field_validator("control_backend_url")
    @classmethod
    def validate_control_backend(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("http://", "https://")):
            raise ValueError("control backend URL must be http(s)")
        return value

    @field_validator("allowed_tenants", "dev_auth_tenants", mode="before")
    @classmethod
    def parse_string_list(cls, value: Any) -> Any:
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("["):
                return json.loads(stripped)
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @field_validator("auth_tokens", mode="before")
    @classmethod
    def parse_auth_tokens(cls, value: Any) -> Any:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            return json.loads(value)
        return value

    @field_validator("rate_limits", mode="before")
    @classmethod
    def parse_rate_limits(cls, value: Any) -> Any:
        if value in (None, ""):
            return {}
        raw = json.loads(value) if isinstance(value, str) else value
        return {OperationClass(key): item for key, item in raw.items()}

    def effective_auth_tokens(self) -> list[TokenPrincipal]:
        tokens = list(self.auth_tokens)
        if self.dev_auth_token:
            tokens.append(
                TokenPrincipal(
                    token=self.dev_auth_token,
                    actor=self.dev_auth_actor,
                    tenants=self.dev_auth_tenants,
                    repositories=["*"],
                    scopes=["git:read", "git:write", "metadata:read", "admin"],
                )
            )
        return tokens

    def effective_rate_limits(self) -> dict[OperationClass, RateLimitRule]:
        defaults = {
            OperationClass.GIT_RECEIVE_PACK: RateLimitRule(capacity=20, refill_per_second=0.1),
            OperationClass.GIT_UPLOAD_PACK: RateLimitRule(capacity=120, refill_per_second=2.0),
            OperationClass.GIT_METADATA: RateLimitRule(capacity=240, refill_per_second=4.0),
            OperationClass.ADMIN: RateLimitRule(capacity=60, refill_per_second=1.0),
        }
        defaults.update(self.rate_limits)
        return defaults
