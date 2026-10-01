"""Algorithm behaviour.

Every test takes ``limiters`` (see conftest), so each runs on the pure-Python path
(MemoryStorage) *and* on the Lua path (fakeredis, and real Redis when configured).
Time is a ManualClock: no sleeps, no flakiness.
"""

from __future__ import annotations

import asyncio
import math
import random

import fakeredis
import pytest

from src.algorithms import ALGORITHMS, RateLimiter, RateLimitResult, create_limiters
from src.clock import ManualClock
from src.storage import MemoryStorage, RedisStorage

ALL = sorted(ALGORITHMS)
T0 = 1_000_000.0  # divisible by 10, so fixed windows of 10s start exactly at T0


def approx(value: float) -> object:
    return pytest.approx(value, abs=2e-3)  # scripts work in whole milliseconds


async def burst(limiter: RateLimiter, key: str, n: int, limit: int, window: float) -> list[RateLimitResult]:
    return [await limiter.check(key, limit, window) for _ in range(n)]


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------


class TestTokenBucket:
    LIMIT, WINDOW = 10, 10  # refills 1 token per second

    async def test_full_bucket_allows_a_burst_of_limit_then_empty_bucket_denies(self, limiters, key):
        tb = limiters["token_bucket"]
        results = await burst(tb, key, 11, self.LIMIT, self.WINDOW)

        assert [r.allowed for r in results] == [True] * 10 + [False]
        assert [r.remaining for r in results] == [9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 0]
        assert results[-1].retry_after == approx(1.0)

    async def test_refills_at_limit_over_window_tokens_per_second(self, limiters, clock, key):
        tb = limiters["token_bucket"]
        await burst(tb, key, 10, self.LIMIT, self.WINDOW)

        clock.advance(3)
        results = await burst(tb, key, 4, self.LIMIT, self.WINDOW)
        assert [r.allowed for r in results] == [True, True, True, False]

    async def test_partial_tokens_accumulate_across_checks(self, limiters, clock, key):
        tb = limiters["token_bucket"]
        await burst(tb, key, 10, self.LIMIT, self.WINDOW)

        clock.advance(0.5)
        r = await tb.check(key, self.LIMIT, self.WINDOW)
        assert not r.allowed
        assert r.retry_after == approx(0.5)

        clock.advance(0.5)
        assert (await tb.check(key, self.LIMIT, self.WINDOW)).allowed

    async def test_idle_time_never_banks_more_than_a_full_bucket(self, limiters, clock, key):
        tb = limiters["token_bucket"]
        await tb.check(key, self.LIMIT, self.WINDOW)

        clock.advance(1_000)  # 100 windows of idling
        results = await burst(tb, key, 11, self.LIMIT, self.WINDOW)
        assert sum(r.allowed for r in results) == 10

    async def test_reset_at_is_when_the_bucket_is_full_again(self, limiters, key):
        tb = limiters["token_bucket"]
        r = await tb.check(key, self.LIMIT, self.WINDOW)  # 9 tokens left, 1 missing = 1s to refill
        assert r.reset_at == approx(T0 + 1)

        r = await tb.check(key, self.LIMIT, self.WINDOW)
        assert r.reset_at == approx(T0 + 2)

    async def test_clock_stepping_backwards_mints_no_tokens(self, limiters, clock, key):
        tb = limiters["token_bucket"]
        await burst(tb, key, 10, self.LIMIT, self.WINDOW)

        clock.advance(-3_600)  # NTP step, VM restore, ...
        assert not (await tb.check(key, self.LIMIT, self.WINDOW)).allowed

        # ...and it re-anchors instead of stalling for an hour: refill resumes from the new "now".
        clock.advance(2)
        results = await burst(tb, key, 3, self.LIMIT, self.WINDOW)
        assert [r.allowed for r in results] == [True, True, False]

    async def test_limit_of_one(self, limiters, clock, key):
        tb = limiters["token_bucket"]
        assert (await tb.check(key, 1, 5)).allowed
        assert not (await tb.check(key, 1, 5)).allowed
        clock.advance(5)
        assert (await tb.check(key, 1, 5)).allowed


# ---------------------------------------------------------------------------
# Sliding window log
# ---------------------------------------------------------------------------


class TestSlidingWindow:
    async def test_denies_at_limit_and_recovers_when_the_oldest_entry_leaves(self, limiters, clock, key):
        sw = limiters["sliding_window"]
        for _ in range(3):  # admitted at T0, T0+1, T0+2
            assert (await sw.check(key, 3, 10)).allowed
            clock.advance(1)

        denied = await sw.check(key, 3, 10)  # now T0+3
        assert not denied.allowed
        assert denied.retry_after == approx(7)  # oldest (T0) + 10 - (T0+3)

        clock.advance(7)  # T0+10: the entry at T0 is exactly `window` old, so it has left
        assert (await sw.check(key, 3, 10)).allowed
        r = await sw.check(key, 3, 10)
        assert not r.allowed
        assert r.retry_after == approx(1)  # next to leave: T0+1

    async def test_has_no_boundary_burst(self, limiters, clock, key):
        sw = limiters["sliding_window"]
        clock.advance(9)  # last second of what a fixed window would call window [T0, T0+10)
        assert sum(r.allowed for r in await burst(sw, key, 5, 5, 10)) == 5

        clock.advance(2)  # a fixed window would have reset by now; a sliding one remembers
        assert sum(r.allowed for r in await burst(sw, key, 5, 5, 10)) == 0

    async def test_rejected_requests_do_not_extend_the_lockout(self, limiters, clock, key):
        sw = limiters["sliding_window"]
        assert (await sw.check(key, 2, 10)).allowed  # T0
        clock.advance(1)
        assert (await sw.check(key, 2, 10)).allowed  # T0+1

        for _ in range(100):  # hammer the closed gate
            clock.advance(0.05)
            assert not (await sw.check(key, 2, 10)).allowed

        clock.set(T0 + 10)  # only the two admitted entries count, and T0's has left
        assert (await sw.check(key, 2, 10)).allowed

    async def test_remaining_and_reset_at(self, limiters, clock, key):
        sw = limiters["sliding_window"]
        r1 = await sw.check(key, 3, 10)
        clock.advance(4)
        r2 = await sw.check(key, 3, 10)

        assert (r1.remaining, r2.remaining) == (2, 1)
        assert r1.reset_at == approx(T0 + 10)
        assert r2.reset_at == approx(T0 + 14)  # when the newest entry leaves

    async def test_two_requests_in_the_same_instant_both_count(self, limiters, key):
        sw = limiters["sliding_window"]
        results = await burst(sw, key, 3, 2, 10)  # clock never moves
        assert [r.allowed for r in results] == [True, True, False]

    async def test_clock_stepping_backwards_keeps_the_log_consistent(self, limiters, clock, key):
        sw = limiters["sliding_window"]
        await burst(sw, key, 3, 3, 10)

        clock.advance(-5)
        assert not (await sw.check(key, 3, 10)).allowed
        clock.set(T0 + 10)
        assert (await sw.check(key, 3, 10)).allowed


# ---------------------------------------------------------------------------
# Fixed window counter
# ---------------------------------------------------------------------------


class TestFixedWindow:
    async def test_counts_down_then_denies_until_the_window_ends(self, limiters, clock, key):
        fw = limiters["fixed_window"]
        results = await burst(fw, key, 4, 3, 10)

        assert [r.allowed for r in results] == [True, True, True, False]
        assert [r.remaining for r in results] == [2, 1, 0, 0]
        assert results[0].reset_at == approx(T0 + 10)  # aligned window end, not now + window
        assert results[-1].retry_after == approx(10)

        clock.advance(9.999)
        assert not (await fw.check(key, 3, 10)).allowed
        clock.advance(0.001)  # exactly the next window
        assert (await fw.check(key, 3, 10)).allowed

    async def test_windows_are_aligned_to_multiples_of_the_window(self, limiters, clock, key):
        fw = limiters["fixed_window"]
        clock.advance(7)
        r = await fw.check(key, 3, 10)
        assert r.reset_at == approx(T0 + 10)
        assert r.retry_after == 0  # allowed

        await burst(fw, key, 2, 3, 10)
        denied = await fw.check(key, 3, 10)
        assert denied.retry_after == approx(3)  # 7s into the window, 3 left

    async def test_boundary_burst_admits_twice_the_limit_within_one_window_length(self, limiters, clock, key):
        """The known flaw of this algorithm, pinned down so nobody is surprised by it."""
        fw = limiters["fixed_window"]
        clock.advance(9.9)
        end_of_window = await burst(fw, key, 5, 5, 10)
        clock.advance(0.2)  # 0.2s later, but across the boundary
        start_of_next = await burst(fw, key, 5, 5, 10)

        assert all(r.allowed for r in end_of_window + start_of_next)  # 10 admitted in 0.2s on a limit of 5/10s

    async def test_old_windows_do_not_leak_into_new_ones(self, limiters, clock, key):
        fw = limiters["fixed_window"]
        await burst(fw, key, 10, 3, 10)  # 3 admitted, 7 denied; the counter must not run past `limit`
        clock.advance(10)
        assert sum(r.allowed for r in await burst(fw, key, 5, 3, 10)) == 3


# ---------------------------------------------------------------------------
# Leaky bucket
# ---------------------------------------------------------------------------


class TestLeakyBucket:
    LIMIT, WINDOW = 4, 4  # drains 1 request per second

    async def test_admitted_requests_are_spread_at_the_drain_rate(self, limiters, key):
        lb = limiters["leaky_bucket"]
        results = await burst(lb, key, 5, self.LIMIT, self.WINDOW)

        assert [r.allowed for r in results] == [True, True, True, True, False]
        assert [r.delay for r in results[:4]] == [approx(0), approx(1), approx(2), approx(3)]
        assert results[4].retry_after == approx(1)
        assert results[4].delay == 0

    async def test_drains_while_idle(self, limiters, clock, key):
        lb = limiters["leaky_bucket"]
        await burst(lb, key, 4, self.LIMIT, self.WINDOW)

        clock.advance(2)  # two requests have leaked out
        results = await burst(lb, key, 3, self.LIMIT, self.WINDOW)
        assert [r.allowed for r in results] == [True, True, False]
        assert results[0].delay == approx(2)  # two still ahead of it in the bucket

    async def test_reset_at_is_when_the_bucket_is_empty(self, limiters, key):
        lb = limiters["leaky_bucket"]
        r = await burst(lb, key, 3, self.LIMIT, self.WINDOW)
        assert r[-1].reset_at == approx(T0 + 3)
        assert r[-1].remaining == 1

    async def test_clock_stepping_backwards_does_not_drain_extra(self, limiters, clock, key):
        lb = limiters["leaky_bucket"]
        await burst(lb, key, 4, self.LIMIT, self.WINDOW)
        clock.advance(-100)
        assert not (await lb.check(key, self.LIMIT, self.WINDOW)).allowed

    async def test_makes_the_same_admit_deny_decisions_as_a_token_bucket(self, limiters, clock, key):
        """Leaky bucket (as a meter) and token bucket are the same arithmetic seen from opposite sides."""
        rng = random.Random(7)
        tb, lb = limiters["token_bucket"], limiters["leaky_bucket"]
        for _ in range(300):
            clock.advance(rng.choice([0, 0, 0.25, 0.5, 1, 2]))
            a = await tb.check(key, 5, 10)
            b = await lb.check(key, 5, 10)
            assert a.allowed == b.allowed


# ---------------------------------------------------------------------------
# Properties every algorithm shares
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ALL)
class TestCommon:
    async def test_keys_are_independent(self, limiters, key, name):
        limiter = limiters[name]
        await burst(limiter, f"{key}-a", 5, 2, 10)
        assert (await limiter.check(f"{key}-b", 2, 10)).allowed

    @pytest.mark.parametrize(
        ("limit", "window"),
        [(0, 10), (-1, 10), (5, 0), (5, -3), (5, math.nan), (5, math.inf)],
    )
    async def test_rejects_nonsensical_parameters(self, limiters, key, name, limit, window):
        with pytest.raises(ValueError):
            await limiters[name].check(key, limit, window)

    async def test_result_is_internally_consistent(self, limiters, key, name):
        limiter = limiters[name]
        for _ in range(8):
            r = await limiter.check(key, 5, 10)
            assert 0 <= r.remaining <= 5
            assert r.limit == 5
            assert r.reset_at >= T0 - 1e-6
            assert (r.retry_after == 0) == r.allowed
            if not r.allowed:
                assert r.remaining == 0

    async def test_concurrent_checks_never_over_admit(self, limiters, key, name):
        """200 simultaneous requests against a limit of 50: exactly 50 get in, never 51.

        On Redis this is the Lua-atomicity guarantee; on memory it is the per-key lock.
        """
        limiter = limiters[name]
        results = await asyncio.gather(*(limiter.check(key, 50, 60) for _ in range(200)))
        assert sum(r.allowed for r in results) == 50

    async def test_never_exceeds_the_theoretical_maximum_under_random_traffic(self, limiters, clock, key, name):
        limit, window = 5, 10.0
        limiter = limiters[name]
        rng = random.Random(1234)

        admitted: list[float] = []
        for _ in range(400):
            # Steps are exact binary fractions on purpose: 0.1 accumulates float dust, and the Lua
            # scripts round to whole milliseconds, so 9.9999999999s would count as a full 10s.
            clock.advance(rng.choice([0, 0, 0.125, 0.5, 1, 2, 5]))
            if (await limiter.check(key, limit, window)).allowed:
                admitted.append(clock.now())

        assert len(admitted) > limit  # the test is vacuous if almost nothing was admitted
        for i, start in enumerate(admitted):
            for j in range(i, len(admitted)):
                span = admitted[j] - start
                count = j - i + 1
                if name == "sliding_window":
                    # every window of length `window` holds at most `limit`
                    if span < window:
                        assert count <= limit
                elif name == "fixed_window":
                    pass  # checked per aligned window below
                else:
                    # buckets: a full burst plus whatever refilled/drained since
                    assert count <= limit + (limit / window) * span + 1e-6

        if name == "fixed_window":
            per_window: dict[int, int] = {}
            for t in admitted:
                bucket = int(t // window)
                per_window[bucket] = per_window.get(bucket, 0) + 1
            assert max(per_window.values()) <= limit


# ---------------------------------------------------------------------------
# The two implementations of each algorithm (Python and Lua) must agree
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", ALL)
async def test_python_and_lua_implementations_agree_on_random_traffic(name):
    rng = random.Random(99)
    py_clock, lua_clock = ManualClock(T0), ManualClock(T0)
    py = create_limiters(MemoryStorage(clock=py_clock, sweep_interval=0), py_clock)[name]
    lua_store = RedisStorage(client=fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer()), clock=lua_clock)
    await lua_store.connect()
    lua = create_limiters(lua_store, lua_clock)[name]

    # Each key keeps one (limit, window) for its whole life. The Lua keys carry a real-time TTL
    # of one window while the clock here is manual, so windows must outlast the test run.
    params = {"k0": (1, 10), "k1": (3, 10), "k2": (7, 30), "k3": (20, 60)}

    for step in range(500):
        dt = rng.choice([0, 0, 0.25, 0.5, 1, 3, 11])
        py_clock.advance(dt)
        lua_clock.advance(dt)
        key = rng.choice(sorted(params))
        limit, window = params[key]

        a = await py.check(key, limit, window)
        b = await lua.check(key, limit, window)

        where = f"step {step} ({name}, key={key}, limit={limit}, window={window})"
        assert a.allowed == b.allowed, where
        assert a.remaining == b.remaining, where
        assert a.reset_at == approx(b.reset_at), where
        assert a.retry_after == approx(b.retry_after), where
        assert a.delay == approx(b.delay), where

    await lua_store.close()
