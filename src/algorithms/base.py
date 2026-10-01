"""The strategy interface every rate-limiting algorithm implements."""

from __future__ import annotations

import itertools
import math
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

from src.clock import Clock, MonotonicClock
from src.storage.base import State, Storage

# Absorbs binary-float dust (2.9999999999 tokens is "3"); far below anything observable.
EPS = 1e-9


@dataclass(frozen=True, slots=True)
class RateLimitResult:
    allowed: bool
    remaining: int
    """Requests still admissible right now."""
    reset_at: float
    """Unix time at which this key's quota is fully restored (see each algorithm)."""
    limit: int
    retry_after: float = 0.0
    """Seconds until a request would be admitted. 0 when ``allowed``."""
    delay: float = 0.0
    """Leaky bucket only: seconds to hold an admitted request so output stays at the drain rate."""


class RateLimiter(ABC):
    """One algorithm bound to one storage backend.

    Subclasses supply two things:

    * :meth:`transition` - the algorithm as a *pure function* of (state, now). It runs on
      any backend through ``Storage.atomic_update``.
    * a Lua script named ``<name>.lua`` (``src/storage/lua``) - the same algorithm for
      Redis, executed atomically server-side. Used automatically when the storage
      ``supports_scripts``.

    The two implementations are held to the same behaviour by ``tests/test_algorithms.py``.
    """

    name: ClassVar[str]

    def __init__(self, storage: Storage, clock: Clock | None = None) -> None:
        self.storage = storage
        self.clock: Clock = clock or MonotonicClock()
        # Unique per call across processes; the sliding-window script stores it as a ZSET member.
        self._instance_id = uuid.uuid4().hex[:12]
        self._sequence = itertools.count()

    async def check(self, key: str, limit: int, window: float) -> RateLimitResult:
        """Count one request against ``key``: at most ``limit`` per ``window`` seconds."""
        if limit < 1:
            raise ValueError(f"limit must be >= 1, got {limit}")
        if not (window > 0 and math.isfinite(window)):
            raise ValueError(f"window must be a positive number of seconds, got {window}")

        if self.storage.supports_scripts:
            window_ms = max(1, round(window * 1000))
            request_id = f"{self._instance_id}-{next(self._sequence)}"
            raw = await self.storage.run_script(self.name, self.script_key(key), [limit, window_ms, request_id])
            return self._from_script(raw, limit)

        now = self.clock.now()
        return await self.storage.atomic_update(
            self.state_key(key, now, window),
            lambda state: self.transition(state, now, limit, window),
            self.ttl(window),
        )

    # -- hooks -----------------------------------------------------------

    @abstractmethod
    def transition(
        self, state: State | None, now: float, limit: int, window: float
    ) -> tuple[State | None, RateLimitResult]:
        """Pure function: previous state (``None`` = unseen key) -> (next state, decision)."""

    def script_key(self, key: str) -> str:
        return f"rl:{self.name}:{key}"

    def state_key(self, key: str, now: float, window: float) -> str:
        """Storage key for the generic path. Window-bucketed algorithms override this."""
        return self.script_key(key)

    def ttl(self, window: float) -> float:
        """How long idle state is worth keeping. One window suffices for every algorithm here."""
        return window

    # -- helpers ---------------------------------------------------------

    @staticmethod
    def _from_script(raw: list[int], limit: int) -> RateLimitResult:
        allowed, remaining, reset_ms, retry_ms, delay_ms = raw
        return RateLimitResult(
            allowed=bool(allowed),
            remaining=remaining,
            reset_at=reset_ms / 1000,
            limit=limit,
            retry_after=retry_ms / 1000,
            delay=delay_ms / 1000,
        )
