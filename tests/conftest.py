"""Shared fixtures.

``storage`` is parametrized over every backend, so any test that takes it runs against all
of them. That is how "both backends implement the same interface" is enforced rather than
merely claimed.

    memory       MemoryStorage
    fakeredis    RedisStorage on fakeredis (real Lua engine via lupa): fast, no server needed
    real_redis   RedisStorage on a real server; only when TEST_REDIS_URL is set, e.g.
                 TEST_REDIS_URL=redis://127.0.0.1:6379/15 pytest
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator

import fakeredis
import pytest

from src.algorithms import RateLimiter, create_limiters
from src.clock import ManualClock
from src.storage import MemoryStorage, RedisStorage, Storage

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL")

BACKENDS = [
    "memory",
    "fakeredis",
    pytest.param(
        "real_redis",
        marks=[
            pytest.mark.real_redis,
            pytest.mark.skipif(not TEST_REDIS_URL, reason="set TEST_REDIS_URL to run against a real Redis"),
        ],
    ),
]


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(1_000_000.0)


@pytest.fixture
def key() -> str:
    """A key no other test (or earlier run against a real Redis) has used."""
    return f"test-{uuid.uuid4().hex[:12]}"


@pytest.fixture(params=BACKENDS)
async def storage(request: pytest.FixtureRequest, clock: ManualClock) -> AsyncIterator[Storage]:
    backend = request.param
    if backend == "memory":
        store: Storage = MemoryStorage(clock=clock, sweep_interval=0)
    elif backend == "fakeredis":
        store = RedisStorage(client=fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer()), clock=clock)
    else:
        assert TEST_REDIS_URL
        store = RedisStorage(TEST_REDIS_URL, clock=clock, socket_timeout=2.0)
    await store.connect()
    yield store
    await store.close()


@pytest.fixture
def limiters(storage: Storage, clock: ManualClock) -> dict[str, RateLimiter]:
    return create_limiters(storage, clock)
