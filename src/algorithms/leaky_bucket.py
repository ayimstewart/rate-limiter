from __future__ import annotations

import math

from src.algorithms.base import EPS, RateLimiter, RateLimitResult
from src.storage.base import State


class LeakyBucket(RateLimiter):
    """Bucket that holds up to ``limit`` requests and drains at ``limit / window`` per second.

    Each request pours one unit in; if there is no room it is rejected. Unlike the token
    bucket it starts *empty* and measures how far behind the drain is, which yields
    ``delay``: how long to hold an admitted request so the traffic that reaches your backend
    leaves at the constant drain rate instead of arriving as a burst. A caller that ignores
    ``delay`` gets the same admit/deny decisions as a token bucket, because the two are the
    same arithmetic seen from opposite sides (``level == limit - tokens``); one that honours
    it gets smoothed output.

    State: ``{"level": float, "ts": last drain time}``.
    ``reset_at``: when the bucket is empty.
    """

    name = "leaky_bucket"

    def transition(
        self, state: State | None, now: float, limit: int, window: float
    ) -> tuple[State | None, RateLimitResult]:
        rate = limit / window  # drained units per second

        if state is None:
            level = 0.0
        else:
            last = state["ts"]
            if now < last:
                last = now
            level = max(0.0, state["level"] - (now - last) * rate)

        allowed = level + 1 <= limit + EPS
        if allowed:
            delay = level / rate
            level += 1
            retry_after = 0.0
        else:
            delay = 0.0
            retry_after = (level + 1 - limit) / rate

        result = RateLimitResult(
            allowed=allowed,
            remaining=max(0, math.floor(limit - level + EPS)),
            reset_at=now + level / rate,
            limit=limit,
            retry_after=retry_after,
            delay=delay,
        )
        return {"level": level, "ts": now}, result
