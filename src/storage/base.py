"""Storage interface shared by the in-memory and Redis backends."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, ClassVar, TypeVar

T = TypeVar("T")

State = dict[str, Any]

# fn(current_state_or_None) -> (new_state_or_None, result). May be sync or async.
# Returning ``None`` as the new state deletes the key.
UpdateFn = Callable[[State | None], tuple[State | None, T] | Awaitable[tuple[State | None, T]]]


class StorageError(Exception):
    """The backend could not complete an operation (unreachable, timed out, ...).

    Backends translate their driver-specific errors into this so the service layer
    has exactly one thing to catch when applying its fail-open / fail-closed policy.
    """


class Storage(ABC):
    """Key -> small dict-of-state store with TTLs and atomic read-modify-write.

    Two ways to run an algorithm against a backend:

    * **Generic path** - ``atomic_update(key, fn, ttl)``. Works on any backend; the
      algorithm's pure ``transition`` function is the ``fn``.
    * **Native path** - ``run_script(name, key, args)``. Backends that can execute
      the whole decision server-side (Redis + Lua) set ``supports_scripts`` and are
      preferred by the algorithms: one round trip, no optimistic-lock retries.
    """

    supports_scripts: ClassVar[bool] = False

    async def connect(self) -> None:  # noqa: B027 - optional hook
        """Acquire resources (connections, background tasks). Idempotent."""

    async def close(self) -> None:  # noqa: B027 - optional hook
        """Release everything acquired in :meth:`connect`. Idempotent."""

    @abstractmethod
    async def ping(self) -> bool:
        """Cheap liveness probe. Never raises; returns ``False`` when unhealthy."""

    @abstractmethod
    async def get(self, key: str) -> State | None:
        """Return a copy of the state at ``key`` or ``None`` if absent/expired."""

    @abstractmethod
    async def set(self, key: str, value: State, ttl: float) -> None:
        """Store ``value`` at ``key``, expiring after ``ttl`` seconds."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Remove ``key`` if present."""

    @abstractmethod
    async def atomic_update(self, key: str, fn: UpdateFn[T], ttl: float) -> T:
        """Apply ``fn`` to the state at ``key`` atomically and return its result.

        No other ``atomic_update`` on the same key may observe or write the state
        between the read and the write.
        """

    async def run_script(self, name: str, key: str, args: Sequence[int | str]) -> list[int]:
        """Execute a named server-side script. Only valid if ``supports_scripts``."""
        raise NotImplementedError(f"{type(self).__name__} does not support server-side scripts")
