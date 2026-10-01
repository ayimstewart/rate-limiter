"""The point of the Redis backend: separate processes, one shared limit.

Needs a real Redis (``TEST_REDIS_URL``); fakeredis lives inside one process so it cannot prove this.
Uses no clock override, so these also exercise the Redis-server-clock path end to end.
"""

from __future__ import annotations

import asyncio
import os
import sys
import textwrap
from pathlib import Path

import pytest

from src.algorithms import ALGORITHMS

TEST_REDIS_URL = os.environ.get("TEST_REDIS_URL")
ROOT = Path(__file__).resolve().parent.parent

pytestmark = [
    pytest.mark.real_redis,
    pytest.mark.skipif(not TEST_REDIS_URL, reason="set TEST_REDIS_URL to run against a real Redis"),
]

WORKER = textwrap.dedent(
    """
    import asyncio, sys
    from src.algorithms import create_limiters
    from src.storage import RedisStorage

    async def main(url, algorithm, key, limit, window, requests):
        store = RedisStorage(url, socket_timeout=5.0)
        await store.connect()
        limiter = create_limiters(store)[algorithm]
        results = await asyncio.gather(*(limiter.check(key, limit, window) for _ in range(requests)))
        print(sum(r.allowed for r in results))
        await store.close()

    url, algorithm, key, limit, window, requests = sys.argv[1:7]
    asyncio.run(main(url, algorithm, key, int(limit), int(window), int(requests)))
    """
)


@pytest.mark.parametrize("name", sorted(ALGORITHMS))
async def test_independent_processes_never_over_admit(name, key):
    """4 processes x 100 concurrent requests = 400 attempts against a limit of 150: exactly 150 get in.

    The window is a day so refill/drain/rollover during the run is negligible and the answer is exact.
    """
    processes, limit, per_process = 4, 150, 100
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    workers = [
        await asyncio.create_subprocess_exec(
            sys.executable, "-c", WORKER, TEST_REDIS_URL, name, key, str(limit), "86400", str(per_process),
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, cwd=ROOT, env=env,
        )
        for _ in range(processes)
    ]
    outputs = await asyncio.gather(*(w.communicate() for w in workers))

    for worker, (_, stderr) in zip(workers, outputs, strict=True):
        assert worker.returncode == 0, stderr.decode()
    admitted = [int(stdout) for stdout, _ in outputs]
    assert sum(admitted) == limit, f"per-process admits: {admitted}"
