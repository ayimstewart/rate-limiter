"""Redis storage: Lua scripts for the built-in algorithms, WATCH/MULTI for everything else.

Why Lua: a rate-limit decision is read-modify-write. Done from the client
(GET ... compute ... SET) two app instances interleave and both admit the
"last" request. Lua scripts run atomically inside Redis, in a single round trip, so
the decision is correct no matter how many instances share the keys.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import math
from collections.abc import Iterator, Sequence
from pathlib import Path

import redis.asyncio as aioredis
from redis.exceptions import RedisError, WatchError

from src.clock import Clock
from src.storage.base import State, Storage, StorageError, T, UpdateFn

logger = logging.getLogger(__name__)

LUA_DIR = Path(__file__).parent / "lua"
_TRANSPORT_ERRORS = (RedisError, OSError, asyncio.TimeoutError)
_MAX_WATCH_RETRIES = 100


@contextlib.contextmanager
def _translate_errors(operation: str) -> Iterator[None]:
    try:
        yield
    except _TRANSPORT_ERRORS as exc:
        detail = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
        raise StorageError(f"redis {operation} failed: {detail}") from exc


def _ttl_ms(ttl: float) -> int:
    return max(1, math.ceil(ttl * 1000))


class RedisStorage(Storage):
    supports_scripts = True

    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client: aioredis.Redis | None = None,
        clock: Clock | None = None,
        socket_timeout: float = 0.25,
        max_connections: int = 100,
    ) -> None:
        """
        :param client: use this connection instead of dialling ``url`` (tests, shared pools).
        :param max_connections: pool size. Callers beyond it wait up to ``socket_timeout`` for a free
            connection (back-pressure) and only then fail; the default redis-py pool would instead
            raise immediately, turning every burst past its limit into a fail-open.
        :param clock: override the time source the scripts use. Leave ``None`` in production:
            the scripts then read ``TIME`` from the Redis server, so every app instance
            agrees on "now" no matter how skewed their own clocks are.
        """
        self._url = url
        self._client = client
        self._owns_client = client is None
        self._clock = clock
        self._socket_timeout = socket_timeout
        self._max_connections = max_connections
        self._scripts: dict[str, aioredis.client.AsyncScript] = {}

    # -- lifecycle -------------------------------------------------------

    async def connect(self) -> None:
        if self._client is None:
            pool = aioredis.BlockingConnectionPool.from_url(
                self._url,
                max_connections=self._max_connections,
                timeout=self._socket_timeout,
                socket_timeout=self._socket_timeout,
                socket_connect_timeout=self._socket_timeout,
                health_check_interval=15,
            )
            self._client = aioredis.Redis(connection_pool=pool)
        for path in sorted(LUA_DIR.glob("*.lua")):
            self._scripts[path.stem] = self._client.register_script(path.read_text(encoding="utf-8"))
        # Warm the script cache so the hot path is pure EVALSHA. If Redis is down right
        # now that is fine: the service starts anyway and the first successful call loads
        # the scripts lazily (a NOSCRIPT reply triggers SCRIPT LOAD + retry).
        try:
            with _translate_errors("script load"):
                for script in self._scripts.values():
                    await self._client.script_load(script.script)
            logger.info("loaded %d lua scripts into redis", len(self._scripts))
        except StorageError as exc:
            logger.warning("redis unavailable at startup, scripts will load lazily: %s", exc)

    async def close(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose(close_connection_pool=True)
            self._client = None

    async def ping(self) -> bool:
        if self._client is None:
            return False
        try:
            return bool(await self._client.ping())
        except _TRANSPORT_ERRORS:
            return False

    def _conn(self) -> aioredis.Redis:
        if self._client is None:
            raise StorageError("redis storage used before connect()")
        return self._client

    # -- native path -----------------------------------------------------

    async def run_script(self, name: str, key: str, args: Sequence[int | str]) -> list[int]:
        if not self._scripts:
            raise StorageError("redis storage used before connect()")
        try:
            script = self._scripts[name]
        except KeyError:
            raise StorageError(f"no lua script named {name!r}") from None
        now_ms = round(self._clock.now() * 1000) if self._clock is not None else -1
        with _translate_errors(f"eval {name}"):
            # AsyncScript.__call__ tries EVALSHA and falls back to SCRIPT LOAD on NOSCRIPT,
            # which is what happens after a Redis restart or SCRIPT FLUSH.
            raw = await script(keys=[key], args=[*args, now_ms], client=self._conn())
        return [int(v) for v in raw]

    # -- generic path ----------------------------------------------------

    async def get(self, key: str) -> State | None:
        with _translate_errors("get"):
            raw = await self._conn().get(key)
        return json.loads(raw) if raw is not None else None

    async def set(self, key: str, value: State, ttl: float) -> None:
        with _translate_errors("set"):
            await self._conn().set(key, json.dumps(value), px=_ttl_ms(ttl))

    async def delete(self, key: str) -> None:
        with _translate_errors("delete"):
            await self._conn().delete(key)

    async def atomic_update(self, key: str, fn: UpdateFn[T], ttl: float) -> T:
        """Optimistic transaction: WATCH, read, compute, MULTI/EXEC; retry if someone wrote first.

        ``fn`` may therefore run more than once and must be free of side effects.
        """
        with _translate_errors("atomic_update"):
            async with self._conn().pipeline(transaction=True) as pipe:
                for _ in range(_MAX_WATCH_RETRIES):
                    try:
                        await pipe.watch(key)
                        raw = await pipe.get(key)
                        outcome = fn(json.loads(raw) if raw is not None else None)
                        if inspect.isawaitable(outcome):
                            outcome = await outcome
                        new_state, result = outcome
                        pipe.multi()
                        if new_state is None:
                            pipe.delete(key)
                        else:
                            pipe.set(key, json.dumps(new_state), px=_ttl_ms(ttl))
                        await pipe.execute()
                        return result
                    except WatchError:
                        continue
        raise StorageError(f"atomic_update on {key!r} lost {_MAX_WATCH_RETRIES} optimistic-lock races")
