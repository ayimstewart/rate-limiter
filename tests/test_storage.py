"""Storage backends.

``TestStorageContract`` takes the parametrized ``storage`` fixture, so the same assertions
run against every backend. The classes below it cover what is specific to one backend.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time

import fakeredis
import pytest

from src.algorithms import ALGORITHMS
from src.clock import ManualClock
from src.storage import MemoryStorage, RedisStorage, Storage, StorageError
from src.storage.redis import LUA_DIR

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL")


async def increment(state):
    """An update function that yields to the event loop between read and write."""
    await asyncio.sleep(0)
    return {"n": (state or {"n": 0})["n"] + 1}, None


async def increment_with_result(state):
    new_state, _ = await increment(state)
    return new_state, new_state["n"]


class TestStorageContract:
    async def test_ping(self, storage):
        assert await storage.ping() is True

    async def test_set_get_delete_roundtrip(self, storage, key):
        assert await storage.get(key) is None

        await storage.set(key, {"a": 1, "nested": {"b": [1, 2]}}, ttl=30)
        assert await storage.get(key) == {"a": 1, "nested": {"b": [1, 2]}}

        await storage.delete(key)
        assert await storage.get(key) is None
        await storage.delete(key)  # deleting a missing key is not an error

    async def test_values_expire_after_their_ttl(self, storage, clock, key):
        await storage.set(key, {"a": 1}, ttl=0.05)
        assert await storage.get(key) == {"a": 1}

        clock.advance(1)  # memory backend runs on the manual clock...
        await asyncio.sleep(0.12)  # ...redis on its own
        assert await storage.get(key) is None

    async def test_atomic_update_creates_updates_and_deletes(self, storage, key):
        assert await storage.atomic_update(key, lambda s: ({"n": 1}, "created" if s is None else "updated"), 30) == "created"
        assert await storage.atomic_update(key, lambda s: ({"n": s["n"] + 1}, s["n"] + 1), 30) == 2
        assert await storage.get(key) == {"n": 2}

        assert await storage.atomic_update(key, lambda s: (None, "gone"), 30) == "gone"
        assert await storage.get(key) is None

    async def test_atomic_update_accepts_async_functions(self, storage, key):
        assert await storage.atomic_update(key, increment_with_result, 30) == 1

    async def test_atomic_update_is_atomic_under_concurrency(self, storage, key):
        """20 coroutines each yield between read and write. Without atomicity most increments are lost."""
        await asyncio.gather(*(storage.atomic_update(key, increment, 30) for _ in range(20)))
        assert await storage.get(key) == {"n": 20}

    async def test_atomic_update_propagates_errors_without_writing(self, storage, key):
        def boom(state):
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await storage.atomic_update(key, boom, 30)
        assert await storage.get(key) is None

    async def test_keys_do_not_interfere(self, storage, key):
        await storage.set(f"{key}-a", {"v": "a"}, 30)
        await storage.set(f"{key}-b", {"v": "b"}, 30)
        assert (await storage.get(f"{key}-a"))["v"] == "a"
        assert (await storage.get(f"{key}-b"))["v"] == "b"

    async def test_connect_and_close_are_idempotent(self, storage):
        await storage.connect()
        await storage.close()
        await storage.close()


async def test_read_modify_write_without_atomic_update_loses_updates():
    """Why the primitive exists: the same 20 increments, done as a naive get -> set, collapse to 1."""
    store = MemoryStorage(sweep_interval=0)

    async def naive_increment():
        state = await store.get("counter") or {"n": 0}
        await asyncio.sleep(0)  # any await between read and write is a window for a race
        await store.set("counter", {"n": state["n"] + 1}, ttl=30)

    await asyncio.gather(*(naive_increment() for _ in range(20)))
    assert (await store.get("counter"))["n"] == 1


class TestMemoryStorage:
    async def test_get_returns_a_copy(self):
        store = MemoryStorage(sweep_interval=0)
        await store.set("k", {"xs": [1]}, ttl=30)

        (await store.get("k"))["xs"].append(2)
        assert await store.get("k") == {"xs": [1]}

    async def test_set_stores_a_copy(self):
        store = MemoryStorage(sweep_interval=0)
        value = {"xs": [1]}
        await store.set("k", value, ttl=30)
        value["xs"].append(2)
        assert await store.get("k") == {"xs": [1]}

    async def test_lock_table_is_empty_when_idle(self):
        """Locks exist only while someone is using the key, so the table cannot grow without bound."""
        store = MemoryStorage(sweep_interval=0)
        await asyncio.gather(*(store.atomic_update(f"k{i % 5}", increment, 30) for i in range(50)))
        assert store._locks == {}

    async def test_lock_is_released_even_when_the_update_raises(self):
        store = MemoryStorage(sweep_interval=0)
        with pytest.raises(ValueError):
            await store.atomic_update("k", lambda s: (_ for _ in ()).throw(ValueError("x")), 30)
        assert store._locks == {}
        assert await store.atomic_update("k", increment_with_result, 30) == 1  # not deadlocked

    async def test_sweep_removes_only_expired_entries(self):
        clock = ManualClock(100)
        store = MemoryStorage(clock=clock, sweep_interval=0)
        await store.set("short", {"a": 1}, ttl=5)
        await store.set("long", {"a": 1}, ttl=50)

        clock.advance(10)
        assert await store.sweep() == 1
        assert len(store) == 1
        assert await store.get("long") == {"a": 1}

    async def test_background_sweeper_runs_and_stops_cleanly(self):
        store = MemoryStorage(sweep_interval=0.01)
        await store.connect()
        await store.set("k", {"a": 1}, ttl=0.01)
        await asyncio.sleep(0.15)
        assert len(store) == 0  # swept without anyone reading the key
        await store.close()
        assert store._sweeper is None

    async def test_uses_the_real_monotonic_clock_by_default(self):
        store = MemoryStorage(sweep_interval=0)
        await store.set("k", {"a": 1}, ttl=0.05)
        assert await store.get("k") == {"a": 1}
        await asyncio.sleep(0.1)
        assert await store.get("k") is None

    async def test_does_not_support_scripts(self):
        store = MemoryStorage(sweep_interval=0)
        assert store.supports_scripts is False
        with pytest.raises(NotImplementedError):
            await store.run_script("token_bucket", "k", [])


def fake_store(**kwargs) -> tuple[RedisStorage, fakeredis.FakeServer]:
    server = fakeredis.FakeServer()
    return RedisStorage(client=fakeredis.FakeAsyncRedis(server=server), **kwargs), server


class TestRedisStorage:
    async def test_every_algorithm_has_a_lua_script_and_it_is_loaded_on_connect(self):
        store, server = fake_store()
        await store.connect()

        assert {p.stem for p in LUA_DIR.glob("*.lua")} == set(ALGORITHMS)
        assert set(store._scripts) == set(ALGORITHMS)
        for script in store._scripts.values():
            assert await store._client.script_exists(script.sha) == [True]  # EVALSHA will hit, not fall back

    async def test_recovers_when_the_script_cache_is_flushed(self):
        """After a Redis restart / SCRIPT FLUSH, EVALSHA says NOSCRIPT; the storage must reload and carry on."""
        store, _ = fake_store()
        await store.connect()
        assert (await store.run_script("fixed_window", "rl:t", [5, 10_000, "id"]))[0] == 1

        await store._client.script_flush()
        assert (await store.run_script("fixed_window", "rl:t", [5, 10_000, "id"]))[0] == 1

    async def test_uses_the_redis_server_clock_unless_given_one(self):
        store, _ = fake_store()  # no clock override
        await store.connect()
        reset_ms = (await store.run_script("fixed_window", "rl:t", [5, 60_000, "id"]))[2]

        now_ms = time.time() * 1000
        assert -1_000 <= reset_ms - now_ms <= 60_000  # the end of the window containing "now"

    async def test_clock_override_is_passed_to_the_script(self):
        store, _ = fake_store(clock=ManualClock(1_000_000))
        await store.connect()
        reset_ms = (await store.run_script("sliding_window", "rl:t", [5, 10_000, "id"]))[2]
        assert reset_ms == 1_000_010_000  # 1,000,000s * 1000 + the 10s window

    async def test_unknown_script_is_a_storage_error(self):
        store, _ = fake_store()
        await store.connect()
        with pytest.raises(StorageError, match="no lua script"):
            await store.run_script("nope", "k", [])

    async def test_use_before_connect_is_a_storage_error(self):
        store = RedisStorage("redis://127.0.0.1:1")
        with pytest.raises(StorageError, match="before connect"):
            await store.get("k")
        with pytest.raises(StorageError, match="before connect"):
            await store.run_script("token_bucket", "k", [1, 1000, "id"])
        assert await store.ping() is False

    async def test_connection_failures_become_storage_errors(self):
        store, server = fake_store()
        await store.connect()
        server.connected = False

        assert await store.ping() is False
        for call in (
            store.get("k"),
            store.set("k", {"a": 1}, 1),
            store.delete("k"),
            store.atomic_update("k", increment, 1),
            store.run_script("token_bucket", "k", [1, 1000, "id"]),
        ):
            with pytest.raises(StorageError):
                await call

    async def test_starts_even_if_redis_is_down_then_recovers(self, caplog):
        store, server = fake_store()
        server.connected = False
        with caplog.at_level(logging.WARNING):
            await store.connect()  # must not raise: fail-open needs a service that boots during an outage
        assert "redis unavailable at startup" in caplog.text

        with pytest.raises(StorageError):
            await store.run_script("token_bucket", "k", [1, 1000, "id"])

        server.connected = True  # Redis comes back; scripts load lazily on first use
        assert (await store.run_script("token_bucket", "k", [1, 1000, "id"]))[0] == 1

    async def test_atomic_update_gives_up_after_too_many_lost_races(self, monkeypatch):
        import src.storage.redis as module

        store, _ = fake_store()
        await store.connect()
        monkeypatch.setattr(module, "_MAX_WATCH_RETRIES", 3)

        async def interfering(state):
            await store._client.set("hot", b'{"n": 0}')  # another writer sneaks in on every attempt
            return {"n": 1}, None

        with pytest.raises(StorageError, match="optimistic-lock"):
            await store.atomic_update("hot", interfering, 30)

    async def test_non_default_url_builds_a_client_with_tight_timeouts(self):
        store = RedisStorage("redis://127.0.0.1:1/0", socket_timeout=0.05)
        await store.connect()  # nothing listening on port 1: must degrade, not hang or raise
        assert await store.ping() is False
        await store.close()
        assert store._client is None


@pytest.mark.real_redis
@pytest.mark.skipif(not TEST_REDIS_URL, reason="set TEST_REDIS_URL to run against a real Redis")
async def test_bursts_beyond_the_pool_size_queue_instead_of_failing(key):
    """Regression: with redis-py's default pool, the 6th concurrent call raised MaxConnectionsError,
    which the service then turned into a silent fail-open."""
    store = RedisStorage(TEST_REDIS_URL, socket_timeout=2.0, max_connections=5)
    await store.connect()
    try:
        results = await asyncio.gather(*(store.run_script("fixed_window", f"rl:{key}", [1000, 60_000, "id"]) for _ in range(200)))
        assert sum(r[0] for r in results) == 200
    finally:
        await store.close()


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
async def test_storage_abstraction_is_satisfiable_by_a_third_party_backend(name):
    """A minimal backend with only the abstract methods is enough to run every algorithm."""
    from src.algorithms import create_limiters

    class Dict(Storage):
        def __init__(self):
            self.d = {}

        async def ping(self):
            return True

        async def get(self, key):
            return self.d.get(key)

        async def set(self, key, value, ttl):
            self.d[key] = value

        async def delete(self, key):
            self.d.pop(key, None)

        async def atomic_update(self, key, fn, ttl):
            new_state, result = fn(self.d.get(key))
            self.d[key] = new_state
            return result

    limiter = create_limiters(Dict(), ManualClock(1_000_000))[name]
    allowed = [(await limiter.check("k", 2, 10)).allowed for _ in range(3)]
    assert allowed == [True, True, False]
