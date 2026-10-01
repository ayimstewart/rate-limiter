"""Latency of one rate limit decision on an otherwise idle system: no queueing, no concurrency.

``compare.py`` drives many workers at once, so its latencies include waiting for the (single)
Python event loop. This one issues checks strictly one after another and shows the floor: what a
single decision costs by itself.

    python benchmarks/idle_latency.py --backend both --redis-url redis://redis:6379/0
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.algorithms import ALGORITHMS, create_limiters  # noqa: E402
from src.storage import MemoryStorage, RedisStorage, Storage  # noqa: E402


async def measure(storage: Storage, n: int) -> dict[str, list[float]]:
    out: dict[str, list[float]] = {}
    run = uuid.uuid4().hex[:8]
    for name, limiter in create_limiters(storage).items():
        for i in range(200):  # warm up: script cache, connection, branch predictors
            await limiter.check(f"idle:{run}:{name}:{i % 50}", 10_000, 60)
        samples = []
        for i in range(n):
            t = time.perf_counter()
            await limiter.check(f"idle:{run}:{name}:{i % 50}", 10_000, 60)
            samples.append((time.perf_counter() - t) * 1e6)
        out[name] = sorted(samples)
    return out


async def main(args: argparse.Namespace) -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    backends = ["memory", "redis"] if args.backend == "both" else [args.backend]
    for backend in backends:
        storage: Storage = RedisStorage(args.redis_url, socket_timeout=5.0) if backend == "redis" else MemoryStorage(sweep_interval=0)
        await storage.connect()
        results = await measure(storage, args.requests)
        await storage.close()
        print(f"### {backend} backend, {args.requests} sequential checks each\n")
        print("| Algorithm | p50 (µs) | p95 (µs) | p99 (µs) |\n|---|---:|---:|---:|")
        for name in sorted(ALGORITHMS):
            s = results[name]
            print(f"| `{name}` | {s[len(s) // 2]:,.0f} | {s[int(len(s) * 0.95)]:,.0f} | {s[int(len(s) * 0.99)]:,.0f} |")
        print()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--backend", choices=["memory", "redis", "both"], default="both")
    ap.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    ap.add_argument("--requests", type=int, default=5000)
    asyncio.run(main(ap.parse_args()))
