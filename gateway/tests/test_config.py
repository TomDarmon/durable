from __future__ import annotations

from origin_gateway.config import GatewaySettings, OperationClass, RateLimitRule


def test_settings_parse_comma_separated_backends() -> None:
    settings = GatewaySettings(origin_backends="http://one:9200,http://two:9200")

    assert settings.origin_backends == ["http://one:9200", "http://two:9200"]


def test_settings_parse_json_string_lists() -> None:
    settings = GatewaySettings(
        origin_backends='["http://one:9200","http://two:9200"]',
        dev_auth_tenants='["tenant-a","tenant-b"]',
    )

    assert settings.origin_backends == ["http://one:9200", "http://two:9200"]
    assert settings.dev_auth_tenants == ["tenant-a", "tenant-b"]


def test_dev_auth_token_is_isolated_to_explicit_setting() -> None:
    settings = GatewaySettings(dev_auth_token="dev-token-123456", dev_auth_tenants="tenant-a")

    [principal] = settings.effective_auth_tokens()
    assert principal.actor == "local-dev"
    assert principal.tenants == ["tenant-a"]
    assert "admin" in principal.scopes


def test_rate_limit_defaults_can_be_overridden() -> None:
    settings = GatewaySettings(
        rate_limits={
            OperationClass.GIT_RECEIVE_PACK: RateLimitRule(capacity=1, refill_per_second=0.5)
        }
    )

    limits = settings.effective_rate_limits()
    assert limits[OperationClass.GIT_RECEIVE_PACK].capacity == 1
    assert limits[OperationClass.GIT_UPLOAD_PACK].capacity > 1
