from __future__ import annotations

import math

from src.algorithms.base import RateLimiter, RateLimitResult
from src.storage.base import State


class FixedWindowCounter(RateLimiter):
    """Count requests per aligned window: ``[0, w)``, ``[w, 2w)``, ...

    O(1) memory and the cheapest operation of the four. The catch is the boundary burst:
    a client can spend ``limit`` requests at the very end of one window and ``limit`` more at
    the very start of the next, so up to ``2 * limit`` requests land inside a single
    ``window`` seconds. Use it where that error is tolerable.

    State: one counter per ``(key, window_start)``; the key is
    ``rl:fixed_window:<key>:<window_start_ms>`` and it expires after one window.
    ``reset_at``: the end of the current window.
    """

    name = "fixed_window"

    def state_key(self, key: str, now: float, window: float) -> str:
        start = math.floor(now / window) * window
        return f"{self.script_key(key)}:{round(start * 1000)}"

    def transition(
        self, state: State | None, now: float, limit: int, window: float
    ) -> tuple[State | None, RateLimitResult]:
        window_end = (math.floor(now / window) + 1) * window
        count = state["count"] if state else 0

        allowed = count < limit
        if allowed:
            count += 1  # only admitted requests are counted, so the counter never exceeds ``limit``

        result = RateLimitResult(
            allowed=allowed,
            remaining=max(0, limit - count),
            reset_at=window_end,
            limit=limit,
            retry_after=0.0 if allowed else window_end - now,
        )
        return {"count": count}, result
