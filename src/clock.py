"""Time sources.

Everything that needs "now" takes a :class:`Clock` so tests can drive time
deterministically instead of sleeping.
"""

from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now(self) -> float:
        """Seconds on a wall-clock-shaped scale (so ``reset_at`` is a real unix time)."""
        ...


class MonotonicClock:
    """Wall-clock-shaped time that can never run backwards.

    ``time.time()`` is sampled exactly once, at construction, to anchor the scale.
    After that every reading is ``time.monotonic() + offset``, so NTP steps, manual
    clock changes or VM pauses cannot make a bucket refill negatively or make a
    window reopen early. The cost is that the process slowly drifts from true
    wall time by whatever the system clock gets slewed by, which is irrelevant for
    rate limiting and invisible to clients (``reset_at`` is a hint, not a contract).
    """

    def __init__(self) -> None:
        self._offset = time.time() - time.monotonic()

    def now(self) -> float:
        return time.monotonic() + self._offset


class ManualClock:
    """Clock that only moves when told to. For tests and simulations."""

    def __init__(self, start: float = 1_000_000.0) -> None:
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds

    def set(self, value: float) -> None:
        self._now = float(value)
