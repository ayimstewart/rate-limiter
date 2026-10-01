from __future__ import annotations

from src.algorithms.base import RateLimiter, RateLimitResult
from src.algorithms.fixed_window import FixedWindowCounter
from src.algorithms.leaky_bucket import LeakyBucket
from src.algorithms.sliding_window import SlidingWindowLog
from src.algorithms.token_bucket import TokenBucket
from src.clock import Clock
from src.storage.base import Storage

ALGORITHMS: dict[str, type[RateLimiter]] = {
    cls.name: cls for cls in (TokenBucket, SlidingWindowLog, FixedWindowCounter, LeakyBucket)
}


def create_limiters(storage: Storage, clock: Clock | None = None) -> dict[str, RateLimiter]:
    """One limiter per algorithm, all sharing ``storage`` (and ``clock``)."""
    return {name: cls(storage, clock) for name, cls in ALGORITHMS.items()}


__all__ = [
    "ALGORITHMS",
    "FixedWindowCounter",
    "LeakyBucket",
    "RateLimitResult",
    "RateLimiter",
    "SlidingWindowLog",
    "TokenBucket",
    "create_limiters",
]
