from __future__ import annotations

import math

from src.algorithms.base import EPS, RateLimiter, RateLimitResult
from src.storage.base import State


class TokenBucket(RateLimiter):
    """Bucket of ``limit`` tokens that refills at ``limit / window`` tokens per second.

    Each request takes one token. A quiet client accumulates up to a full bucket and may
    then burst ``limit`` requests at once; sustained throughput is capped at the refill rate.

    State: ``{"tokens": float, "ts": last refill time}``.
    ``reset_at``: when the bucket is full again.
    """

    name = "token_bucket"

    def transition(
        self, state: State | None, now: float, limit: int, window: float
    ) -> tuple[State | None, RateLimitResult]:
        rate = limit / window

        if state is None:
            tokens = float(limit)  # first sight of a key: full bucket
        else:
            last = state["ts"]
            if now < last:
                # Clock stepped backwards: re-anchor rather than refill a negative amount.
                last = now
            tokens = min(float(limit), state["tokens"] + (now - last) * rate)

        allowed = tokens >= 1 - EPS
        if allowed:
            tokens = max(0.0, tokens - 1)
            retry_after = 0.0
        else:
            retry_after = (1 - tokens) / rate

        result = RateLimitResult(
            allowed=allowed,
            remaining=math.floor(tokens + EPS),
            reset_at=now + (limit - tokens) / rate,
            limit=limit,
            retry_after=retry_after,
        )
        return {"tokens": tokens, "ts": now}, result
