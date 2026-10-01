"""The decision service: algorithm routing + metrics + the fail-open / fail-closed policy.

Both the HTTP endpoint and the middleware go through here so they behave identically.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from src.algorithms import RateLimiter, RateLimitResult, create_limiters
from src.clock import Clock
from src.metrics import Metrics
from src.storage.base import Storage, StorageError

logger = logging.getLogger(__name__)

_LOG_EVERY = 5.0  # seconds; an outage should not write one log line per request


class RateLimiterUnavailable(Exception):
    """Storage failed and the service is configured to fail closed."""


@dataclass(frozen=True, slots=True)
class Decision:
    result: RateLimitResult
    algorithm: str
    degraded: bool = False
    """True when ``result`` is the fail-open default, not a real decision."""


class RateLimitService:
    def __init__(
        self,
        limiters: dict[str, RateLimiter],
        default_algorithm: str,
        *,
        metrics: Metrics | None = None,
        fail_open: bool = True,
    ) -> None:
        if default_algorithm not in limiters:
            raise ValueError(f"unknown default algorithm {default_algorithm!r}")
        self.limiters = limiters
        self.default_algorithm = default_algorithm
        self.metrics = metrics or Metrics(limiters)
        self.fail_open = fail_open
        self._last_logged = float("-inf")
        self._suppressed = 0

    @classmethod
    def create(
        cls,
        storage: Storage,
        *,
        default_algorithm: str = "token_bucket",
        fail_open: bool = True,
        metrics: Metrics | None = None,
        clock: Clock | None = None,
    ) -> RateLimitService:
        limiters = create_limiters(storage, clock)
        return cls(limiters, default_algorithm, metrics=metrics or Metrics(limiters), fail_open=fail_open)

    async def check(self, key: str, limit: int, window: int, algorithm: str | None = None) -> Decision:
        name = algorithm or self.default_algorithm
        try:
            limiter = self.limiters[name]
        except KeyError:
            raise ValueError(f"unknown algorithm {name!r}; choose from {sorted(self.limiters)}") from None

        started = time.perf_counter()
        try:
            result = await limiter.check(key, limit, window)
        except StorageError as exc:
            self.metrics.record_error(name, fail_open=self.fail_open, seconds=time.perf_counter() - started)
            self._log_storage_error(name, exc)
            if not self.fail_open:
                raise RateLimiterUnavailable(str(exc)) from exc
            return Decision(self._fail_open_result(limit, window), name, degraded=True)

        self.metrics.record(name, allowed=result.allowed, seconds=time.perf_counter() - started)
        return Decision(result, name)

    @staticmethod
    def _fail_open_result(limit: int, window: int) -> RateLimitResult:
        # We know nothing about this key's usage; say so honestly: admit it, claim a full quota.
        return RateLimitResult(allowed=True, remaining=limit, reset_at=time.time() + window, limit=limit)

    def _log_storage_error(self, algorithm: str, exc: StorageError) -> None:
        now = time.monotonic()
        if now - self._last_logged < _LOG_EVERY:
            self._suppressed += 1
            return
        policy = "failing open" if self.fail_open else "failing closed"
        logger.error(
            "storage error in %s check, %s (%d similar errors suppressed): %s",
            algorithm,
            policy,
            self._suppressed,
            exc,
        )
        self._last_logged = now
        self._suppressed = 0
