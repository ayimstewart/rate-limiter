"""Compare the four algorithms head to head and print a markdown report.

Runs in-process (no HTTP), so the numbers isolate the algorithm + storage cost from web
framework overhead. Use ``locustfile.py`` for the end-to-end picture.

    python benchmarks/compare.py                              # memory backend, 60s per algorithm
    python benchmarks/compare.py --backend redis --redis-url redis://127.0.0.1:6379/0
    python benchmarks/compare.py --backend both --duration 10 --output benchmarks/results/compare.md

Each algorithm gets the same traffic: ``--concurrency`` workers issue back-to-back checks
over ``--keys`` uniformly random keys for ``--duration`` seconds (after a warm-up whose
samples are discarded). Latency is measured around ``limiter.check()`` only.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import platform
import random
import sys
import time
import tracemalloc
import uuid
from array import array
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.algorithms import ALGORITHMS, create_limiters  # noqa: E402
from src.storage import MemoryStorage, RedisStorage, Storage  # noqa: E402


@dataclass
class Result:
    algorithm: str
    requests: int
    seconds: float
    allowed: int
    latencies_us: array

    def percentile(self, p: float) -> float:
        ordered = sorted(self.latencies_us)
        return ordered[min(len(ordered) - 1, int(len(ordered) * p))] if ordered else float("nan")


async def run_algorithm(
    storage: Storage, name: str, *, duration: float, warmup: float, concurrency: int, keys: int, limit: int, window: int
) -> Result:
    limiter = create_limiters(storage)[name]
    run_id = uuid.uuid4().hex[:8]  # fresh key space per run; nothing to clean up, TTLs do it
    latencies = array("d")
    counters = {"requests": 0, "allowed": 0}
    start = time.perf_counter()
    measure_from = start + warmup
    stop_at = measure_from + duration

    async def worker(seed: int) -> None:
        rng = random.Random(seed)
        while True:
            t0 = time.perf_counter()
            if t0 >= stop_at:
                return
            result = await limiter.check(f"bench:{run_id}:{rng.randrange(keys)}", limit, window)
            t1 = time.perf_counter()
            if t0 >= measure_from:
                latencies.append((t1 - t0) * 1e6)
                counters["requests"] += 1
                counters["allowed"] += result.allowed

    await asyncio.gather(*(worker(i) for i in range(concurrency)))
    return Result(name, counters["requests"], duration, counters["allowed"], latencies)


async def probe_state_size(storage: Storage, backend: str, limit: int) -> dict[str, float]:
    """Bytes of state one key costs once it has absorbed ``limit`` requests."""
    sizes: dict[str, float] = {}
    for name, limiter in create_limiters(storage).items():
        if backend == "redis":
            assert isinstance(storage, RedisStorage) and storage._client is not None
            probe = f"sizeprobe:{uuid.uuid4().hex[:8]}"
            for _ in range(limit):
                await limiter.check(probe, limit, 600)
            found = [k async for k in storage._client.scan_iter(match=f"rl:{name}:{probe}*")]
            sizes[name] = float(sum([await storage._client.memory_usage(k) or 0 for k in found]))
        else:
            n = 200
            tracemalloc.start()
            before = tracemalloc.take_snapshot()
            for i in range(n):
                for _ in range(limit):
                    await limiter.check(f"sizeprobe:{i}", limit, 600)
            after = tracemalloc.take_snapshot()
            tracemalloc.stop()
            sizes[name] = sum(s.size_diff for s in after.compare_to(before, "filename")) / n
    return sizes


def format_report(backend: str, results: list[Result], sizes: dict[str, float] | None, args: argparse.Namespace) -> str:
    lines = [
        f"### {backend} backend",
        "",
        f"`{args.concurrency}` concurrent workers, `{args.keys}` keys, limit `{args.limit}` per `{args.window}`s, "
        f"`{args.duration:g}`s measured per algorithm.",
        "",
        "| Algorithm | Throughput (checks/s) | p50 (µs) | p95 (µs) | p99 (µs) | Allowed |"
        + (" State per key at limit |" if sizes else ""),
        "|---|---:|---:|---:|---:|---:|" + ("---:|" if sizes else ""),
    ]
    for r in results:
        row = (
            f"| `{r.algorithm}` | {r.requests / r.seconds:,.0f} | {r.percentile(0.50):,.0f} | "
            f"{r.percentile(0.95):,.0f} | {r.percentile(0.99):,.0f} | {100 * r.allowed / max(1, r.requests):.1f}% |"
        )
        if sizes:
            row += f" {sizes[r.algorithm]:,.0f} B |"
        lines.append(row)
    return "\n".join(lines)


def run(coro):
    """asyncio.run, on uvloop when installed (as uvicorn[standard] does on Linux/macOS)."""
    try:
        import uvloop
    except ImportError:
        return asyncio.run(coro)
    return asyncio.run(coro, loop_factory=uvloop.new_event_loop)


async def run_backend(backend: str, args: argparse.Namespace) -> tuple[str, str]:
    storage: Storage = (
        RedisStorage(args.redis_url, socket_timeout=5.0) if backend == "redis" else MemoryStorage(sweep_interval=5.0)
    )
    await storage.connect()
    env = f"{type(asyncio.get_running_loop()).__module__.split('.')[0]} event loop"
    try:
        if backend == "redis":
            info = await storage._client.info("server")  # type: ignore[union-attr]
            env = f"Redis {info['redis_version']}"
        results = []
        for name in args.algorithms:
            print(f"  [{backend}] {name} ...", file=sys.stderr, flush=True)
            results.append(
                await run_algorithm(
                    storage,
                    name,
                    duration=args.duration,
                    warmup=args.warmup,
                    concurrency=args.concurrency,
                    keys=args.keys,
                    limit=args.limit,
                    window=args.window,
                )
            )
        sizes = None if args.skip_probe else await probe_state_size(storage, backend, args.probe_limit)
        return env, format_report(backend, results, sizes, args)
    finally:
        await storage.close()


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")  # the report contains µ; Windows consoles default to cp1252
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--backend", choices=["memory", "redis", "both"], default="memory")
    p.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    p.add_argument("--duration", type=float, default=60.0, help="measured seconds per algorithm (default 60)")
    p.add_argument("--warmup", type=float, default=2.0, help="discarded seconds before measuring (default 2)")
    p.add_argument("--concurrency", type=int, default=100)
    p.add_argument("--keys", type=int, default=1_000)
    p.add_argument("--limit", type=int, default=50)
    p.add_argument("--window", type=int, default=10)
    p.add_argument("--probe-limit", type=int, default=1000, help="limit used when measuring state size per key")
    p.add_argument("--skip-probe", action="store_true", help="skip the state-size measurement")
    p.add_argument("--algorithms", nargs="+", default=sorted(ALGORITHMS), choices=sorted(ALGORITHMS))
    p.add_argument("--output", type=Path, help="also write the markdown report to this file")
    args = p.parse_args()

    backends = ["memory", "redis"] if args.backend == "both" else [args.backend]
    sections, envs = [], []
    for backend in backends:
        env, section = run(run_backend(backend, args))
        sections.append(section)
        envs.append(env)

    header = [
        "## Algorithm comparison",
        "",
        f"Python {platform.python_version()} on {platform.system()} {platform.release()} ({platform.machine()}), "
        f"{os.cpu_count()} logical CPUs, " + ", ".join(dict.fromkeys(envs)) + ".",
        "",
    ]
    report = "\n".join(header) + "\n" + "\n\n".join(sections) + "\n"
    print(report)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
