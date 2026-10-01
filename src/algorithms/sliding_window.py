from __future__ import annotations

import bisect

from src.algorithms.base import RateLimiter, RateLimitResult
from src.storage.base import State


class SlidingWindowLog(RateLimiter):
    """Exact sliding window: remember every admitted request's timestamp.

    A request is admitted if fewer than ``limit`` admitted requests happened in the last
    ``window`` seconds, measured from *this* request, not from a fixed boundary. There is
    no boundary burst, at the price of O(limit) memory per key.

    State: ``{"ts": [sorted admitted timestamps]}``. Denied requests are not logged, so a
    client hammering a closed gate does not keep it closed.
    ``reset_at``: when the newest logged request leaves the window (the log is empty).
    """

    name = "sliding_window"

    def transition(
        self, state: State | None, now: float, limit: int, window: float
    ) -> tuple[State | None, RateLimitResult]:
        stamps: list[float] = state["ts"] if state else []

        # An entry counts while now - ts < window, so it is evicted once ts <= now - window.
        expired = bisect.bisect_right(stamps, now - window)
        if expired:
            del stamps[:expired]

        allowed = len(stamps) < limit
        if allowed:
            bisect.insort(stamps, now)  # a plain append unless the clock moved backwards
            retry_after = 0.0
        else:
            retry_after = max(0.0, stamps[0] + window - now)

        result = RateLimitResult(
            allowed=allowed,
            remaining=max(0, limit - len(stamps)),
            reset_at=stamps[-1] + window if stamps else now,
            limit=limit,
            retry_after=retry_after,
        )
        return {"ts": stamps}, result
