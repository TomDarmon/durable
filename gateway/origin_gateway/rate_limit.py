from __future__ import annotations

import math
import time
from collections.abc import Callable
from dataclasses import dataclass

from .config import OperationClass, RateLimitRule


@dataclass
class _Bucket:
    tokens: float
    updated_at: float


@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    retry_after_seconds: int
    remaining: int


class RateLimiter:
    def __init__(
        self,
        rules: dict[OperationClass, RateLimitRule],
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._rules = rules
        self._clock = clock
        self._buckets: dict[tuple[str, str, str, str, OperationClass], _Bucket] = {}

    def check(
        self,
        *,
        tenant: str,
        actor: str,
        repository: str,
        ip_address: str,
        operation: OperationClass,
    ) -> RateLimitDecision:
        rule = self._rules[operation]
        key = (tenant, actor, repository, ip_address, operation)
        now = self._clock()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=float(rule.capacity), updated_at=now)
            self._buckets[key] = bucket
        elapsed = max(0.0, now - bucket.updated_at)
        bucket.tokens = min(float(rule.capacity), bucket.tokens + elapsed * rule.refill_per_second)
        bucket.updated_at = now
        if bucket.tokens >= 1.0:
            bucket.tokens -= 1.0
            return RateLimitDecision(True, 0, int(bucket.tokens))
        retry_after = max(1, math.ceil((1.0 - bucket.tokens) / rule.refill_per_second))
        return RateLimitDecision(False, retry_after, 0)
