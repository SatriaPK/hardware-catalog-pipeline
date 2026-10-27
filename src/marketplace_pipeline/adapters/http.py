"""HTTP budget, timeout, pacing and bounded retry shared by both APIs."""

import json
import math
import random
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any
from uuid import uuid4

import httpx


class ContractError(RuntimeError):
    """Safe fixed error code; never carries provider body, URL, or credentials."""


@dataclass(frozen=True)
class Policy:
    request_budget: int = 20
    max_retries: int = 3
    interval: float = 1.0
    max_wait: float = 60.0

    def __post_init__(self) -> None:
        if (
            type(self.request_budget) is not int
            or self.request_budget < 1
            or type(self.max_retries) is not int
            or not 0 <= self.max_retries <= 10
            or not math.isfinite(self.interval)
            or self.interval <= 0
            or not math.isfinite(self.max_wait)
            or self.max_wait < self.interval
        ):
            raise ValueError("Invalid bounded request policy")


class HttpSession:
    def __init__(
        self,
        transport: httpx.BaseTransport | None,
        policy: Policy | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        jitter: Callable[[], float] = random.random,
        before_attempt: Callable[[str, str], None] = lambda method, url: None,
    ):
        self.client = httpx.Client(
            transport=transport,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(30, connect=10),
        )
        self.policy, self.clock, self.sleep, self.now, self.jitter = (
            policy or Policy(),
            clock,
            sleep,
            now,
            jitter,
        )
        self.attempts = 0
        self.next_at = 0.0
        self.events: list[dict[str, Any]] = []
        self.before_attempt = before_attempt

    def close(self) -> None:
        self.client.close()

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        correlation = uuid4().hex
        for retry in range(self.policy.max_retries + 1):
            if self.attempts >= self.policy.request_budget:
                raise ContractError("request_budget_exhausted")
            self.sleep(max(0, self.next_at - self.clock()))
            self.before_attempt(method, url)
            start = self.clock()
            self.next_at = start + self.policy.interval
            self.attempts += 1
            response = None
            try:
                response = self.client.request(method, url, **kwargs)
                status = response.status_code
            except httpx.TransportError:
                status = 0
            self.events.append(
                {
                    "request_id": correlation,
                    "attempt": retry + 1,
                    "status": status,
                    "duration_s": max(0, self.clock() - start),
                }
            )
            if status == 200:
                assert response is not None
                return response
            if status not in {0, 429, 500, 502, 503, 504}:
                raise ContractError("fatal_auth" if status in {401, 403} else "fatal_http")
            if retry == self.policy.max_retries:
                raise ContractError("retry_exhausted")
            delay = 2**retry + min(1, max(0, self.jitter()))
            if response is not None and "Retry-After" in response.headers:
                value = response.headers["Retry-After"]
                try:
                    if value.isascii() and value.isdigit():
                        delay = max(delay, int(value))
                    else:
                        stamp = parsedate_to_datetime(value)
                        if stamp.tzinfo is None:
                            raise ValueError
                        delay = max(delay, (stamp - self.now()).total_seconds())
                except (ValueError, TypeError, OverflowError):
                    raise ContractError("invalid_retry_after") from None
            if delay > self.policy.max_wait:
                raise ContractError("retry_wait_exceeds_policy")
            self.next_at = max(self.next_at, self.clock() + delay)
        raise AssertionError("Unreachable")


def envelope(body: bytes) -> dict:
    try:
        value = json.loads(body)
        if not isinstance(value, dict):
            raise ValueError
        return value
    except (ValueError, UnicodeError):
        raise ContractError("schema_drift") from None
