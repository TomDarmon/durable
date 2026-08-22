from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    generate_latest,
)
from starlette.responses import Response


class GatewayMetrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.requests = Counter(
            "origin_gateway_requests_total",
            "Gateway requests by operation and outcome.",
            ("operation", "status"),
            registry=self.registry,
        )
        self.latency = Histogram(
            "origin_gateway_request_latency_seconds",
            "Gateway request latency by operation.",
            ("operation",),
            registry=self.registry,
        )
        self.auth_failures = Counter(
            "origin_gateway_auth_failures_total",
            "Authentication or authorization failures.",
            ("operation",),
            registry=self.registry,
        )
        self.rate_limit_rejects = Counter(
            "origin_gateway_rate_limit_rejects_total",
            "Rate-limit rejections.",
            ("operation",),
            registry=self.registry,
        )
        self.backend_errors = Counter(
            "origin_gateway_backend_errors_total",
            "Origin backend errors.",
            ("backend", "operation"),
            registry=self.registry,
        )
        self.bytes_proxied = Counter(
            "origin_gateway_bytes_proxied_total",
            "Bytes proxied between clients and Origin.",
            ("operation", "direction"),
            registry=self.registry,
        )

    def response(self) -> Response:
        return Response(generate_latest(self.registry), media_type=CONTENT_TYPE_LATEST)
