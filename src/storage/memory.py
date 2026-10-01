"""In-process storage: a dict with TTLs and one ``asyncio.Lock`` per key.

Right for a single instance (or tests). State lives inside the process, so two
replicas each enforce their own private limit - see the README for when that is
and is not acceptable.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
import inspect
import logging

from src.clock import Clock, MonotonicClock
from src.storage.base import State, Storage, T, UpdateFn

logger = logging.getLogger(__name__)


class _Entry:
    __slots__ = ("expires_at", "value")

    def __init__(self, value: State, expires_at: float) -> None:
        self.value = value
        self.expires_at = expires_at


class _LockRef:
    """A lock plus the number of coroutines using or waiting on it.

    Locks are created on demand and dropped when the last user leaves, so the lock
    table is bounded by the number of *in-flight* keys, not the number of keys ever seen.
    """

    __slots__ = ("lock", "users")

    def __init__(self) -> None:
        self.lock = asyncio.Lock()
        self.users = 0


class MemoryStorage(Storage):
    def __init__(self, clock: Clock | None = None, sweep_interval: float = 30.0) -> None:
        self._clock = clock or MonotonicClock()
        self._data: dict[str, _Entry] = {}
        self._locks: dict[str, _LockRef] = {}
        self._sweep_interval = sweep_interval
        self._sweeper: asyncio.Task[None] | None = None

    # -- lifecycle -------------------------------------------------------

    async def connect(self) -> None:
        if self._sweeper is None and self._sweep_interval > 0:
            self._sweeper = asyncio.create_task(self._sweep_loop(), name="memory-storage-sweeper")

    async def close(self) -> None:
        if self._sweeper is not None:
            self._sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweeper
            self._sweeper = None

    async def ping(self) -> bool:
        return True

    # -- expiry ----------------------------------------------------------

    def _live(self, key: str, now: float) -> _Entry | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        if entry.expires_at <= now:
            del self._data[key]
            return None
        return entry

    async def sweep(self) -> int:
        """Drop expired entries nobody has touched. Returns how many were removed.

        Lazy expiry on access keeps correctness; this exists so keys that are never
        seen again (one-off client IPs, say) do not accumulate forever.
        """
        now = self._clock.now()
        expired = [k for k, e in self._data.items() if e.expires_at <= now and k not in self._locks]
        for key in expired:
            self._data.pop(key, None)
        return len(expired)

    async def _sweep_loop(self) -> None:
        while True:
            await asyncio.sleep(self._sweep_interval)
            removed = await self.sweep()
            if removed:
                logger.debug("memory storage swept %d expired keys", removed)

    def __len__(self) -> int:
        return len(self._data)

    # -- Storage interface ----------------------------------------------

    async def get(self, key: str) -> State | None:
        entry = self._live(key, self._clock.now())
        return copy.deepcopy(entry.value) if entry else None

    async def set(self, key: str, value: State, ttl: float) -> None:
        self._data[key] = _Entry(copy.deepcopy(value), self._clock.now() + ttl)

    async def delete(self, key: str) -> None:
        self._data.pop(key, None)

    async def atomic_update(self, key: str, fn: UpdateFn[T], ttl: float) -> T:
        ref = self._locks.get(key)
        if ref is None:
            ref = self._locks[key] = _LockRef()
        ref.users += 1
        try:
            async with ref.lock:
                now = self._clock.now()
                entry = self._live(key, now)
                outcome = fn(entry.value if entry else None)
                if inspect.isawaitable(outcome):
                    outcome = await outcome
                new_state, result = outcome
                if new_state is None:
                    self._data.pop(key, None)
                else:
                    self._data[key] = _Entry(new_state, now + ttl)
                return result
        finally:
            ref.users -= 1
            if ref.users == 0:
                del self._locks[key]
