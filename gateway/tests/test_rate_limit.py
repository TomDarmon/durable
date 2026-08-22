from __future__ import annotations

from origin_gateway.config import OperationClass, RateLimitRule
from origin_gateway.rate_limit import RateLimiter


def test_rate_limiter_is_partitioned_by_actor_repo_ip_and_operation() -> None:
    now = 0.0

    def clock() -> float:
        return now

    limiter = RateLimiter(
        {
            OperationClass.GIT_RECEIVE_PACK: RateLimitRule(capacity=1, refill_per_second=1.0),
            OperationClass.GIT_UPLOAD_PACK: RateLimitRule(capacity=1, refill_per_second=1.0),
        },
        clock=clock,
    )

    first = limiter.check(
        tenant="tenant",
        actor="alice",
        repository="repo",
        ip_address="127.0.0.1",
        operation=OperationClass.GIT_RECEIVE_PACK,
    )
    second = limiter.check(
        tenant="tenant",
        actor="alice",
        repository="repo",
        ip_address="127.0.0.1",
        operation=OperationClass.GIT_RECEIVE_PACK,
    )
    other_operation = limiter.check(
        tenant="tenant",
        actor="alice",
        repository="repo",
        ip_address="127.0.0.1",
        operation=OperationClass.GIT_UPLOAD_PACK,
    )

    assert first.allowed
    assert not second.allowed
    assert other_operation.allowed


def test_rate_limiter_retry_after_rounds_up_to_available_token() -> None:
    now = 0.0

    def clock() -> float:
        return now

    limiter = RateLimiter(
        {
            OperationClass.GIT_RECEIVE_PACK: RateLimitRule(capacity=1, refill_per_second=0.25),
        },
        clock=clock,
    )

    assert limiter.check(
        tenant="tenant",
        actor="alice",
        repository="repo",
        ip_address="127.0.0.1",
        operation=OperationClass.GIT_RECEIVE_PACK,
    ).allowed
    decision = limiter.check(
        tenant="tenant",
        actor="alice",
        repository="repo",
        ip_address="127.0.0.1",
        operation=OperationClass.GIT_RECEIVE_PACK,
    )

    assert not decision.allowed
    assert decision.retry_after_seconds == 4
