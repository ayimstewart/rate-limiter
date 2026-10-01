"""Regenerate the charts in docs/images.

The behaviour charts are produced by running the *real* algorithm implementations on a
manual clock, so they cannot drift from the code. The latency charts are drawn from the
saved benchmark output in docs/benchmarks.

    pip install -r requirements-docs.txt
    python docs/generate_figures.py
"""

from __future__ import annotations

import asyncio
import re
import sys
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.algorithms import create_limiters  # noqa: E402
from src.clock import ManualClock  # noqa: E402
from src.storage import MemoryStorage  # noqa: E402

IMAGES = ROOT / "docs" / "images"
BENCH = ROOT / "docs" / "benchmarks"

T0 = 1_000_000.0  # a multiple of the 10 s window, so fixed windows start at t=0
LIMIT, WINDOW = 5, 10
ORDER = ["token_bucket", "sliding_window", "fixed_window", "leaky_bucket"]
TITLES = {
    "token_bucket": "Token bucket",
    "sliding_window": "Sliding window log",
    "fixed_window": "Fixed window counter",
    "leaky_bucket": "Leaky bucket",
}
GREEN, RED, BLUE, GREY, INK = "#1a9850", "#d73027", "#3b6fb6", "#9aa3ad", "#222b36"

# One traffic pattern, chosen to separate the algorithms:
#   a burst at the end of window 1, an identical burst right after the boundary, then a steady 1 req/s
#   trickle that is twice the sustainable rate (limit / window = 0.5 req/s).
EVENTS = (
    [9.0 + 0.1 * i for i in range(6)]
    + [10.0 + 0.1 * i for i in range(6)]
    + [16.0 + 1.0 * i for i in range(19)]
)


def style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 10,
            "axes.edgecolor": GREY,
            "axes.labelcolor": INK,
            "text.color": INK,
            "xtick.color": INK,
            "ytick.color": INK,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "figure.facecolor": "white",
        }
    )


async def simulate() -> dict[str, list[tuple[float, bool, int]]]:
    out: dict[str, list[tuple[float, bool, int]]] = {}
    for name in ORDER:
        clock = ManualClock(T0)
        limiter = create_limiters(MemoryStorage(clock=clock, sweep_interval=0), clock)[name]
        rows = []
        for t in EVENTS:
            clock.set(T0 + t)
            r = await limiter.check("user", LIMIT, WINDOW)
            rows.append((t, r.allowed, r.remaining))
        out[name] = rows
    return out


def behaviour(sim: dict[str, list[tuple[float, bool, int]]]) -> None:
    fig, axes = plt.subplots(4, 1, figsize=(11, 10), sharex=True, sharey=True)
    for ax, name in zip(axes, ORDER, strict=True):
        rows = sim[name]
        ts = [t for t, _, _ in rows]
        remaining = [rem for _, _, rem in rows]
        ax.step(ts, remaining, where="post", color=BLUE, lw=1.6, alpha=0.55, label="quota remaining")
        ax.scatter([t for t, ok, _ in rows if ok], [rem for _, ok, rem in rows if ok], color=GREEN, s=34, zorder=3, label="allowed")
        ax.scatter([t for t, ok, _ in rows if not ok], [rem for _, ok, rem in rows if not ok], color=RED, marker="x", s=36, zorder=3, label="denied")
        admitted = sum(ok for _, ok, _ in rows)
        ax.set_title(f"{TITLES[name]}   -   admitted {admitted} of {len(rows)}", loc="left", fontsize=11, fontweight="bold")
        ax.set_ylabel("remaining")
        ax.set_ylim(-0.6, LIMIT + 0.8)
        ax.set_yticks(range(LIMIT + 1))
        ax.axvspan(9.0, 10.5, color="#f6c85f", alpha=0.18, lw=0)
        if name == "fixed_window":
            for boundary in (10, 20, 30):
                ax.axvline(boundary, color=INK, ls="--", lw=0.9, alpha=0.6)
            ax.text(10.15, LIMIT + 0.35, "window boundary: counter resets", fontsize=8.5, color=INK, va="center")
        if name == "token_bucket":
            ax.text(10.7, LIMIT + 0.35, "burst budget used up, refills at 0.5/s", fontsize=8.5, va="center")
        if name == "sliding_window":
            ax.text(10.7, LIMIT + 0.35, "still remembers the first burst", fontsize=8.5, va="center")
        if name == "leaky_bucket":
            ax.text(10.7, LIMIT + 0.35, "same decisions as the token bucket", fontsize=8.5, va="center")
    axes[0].legend(loc="upper right", ncols=3, frameon=False, fontsize=9)
    axes[-1].set_xlabel("time (s)     limit = 5 requests per 10 s     shaded: burst across the t = 10 s boundary")
    fig.suptitle("Same traffic, four algorithms", x=0.01, ha="left", fontsize=14, fontweight="bold")
    fig.tight_layout(rect=(0, 0, 1, 0.97))
    fig.savefig(IMAGES / "algorithm-behavior.png", dpi=150)
    plt.close(fig)


def busiest_window(admitted: list[float]) -> int:
    return max(sum(1 for u in admitted if t <= u < t + WINDOW) for t in admitted)


def burst_chart(sim: dict[str, list[tuple[float, bool, int]]]) -> None:
    values = [busiest_window([t for t, ok, _ in sim[n] if ok]) for n in ORDER]
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    colors = [RED if v > LIMIT else BLUE for v in values]
    bars = ax.bar([TITLES[n] for n in ORDER], values, color=colors, width=0.55)
    ax.axhline(LIMIT, color=INK, ls="--", lw=1)
    ax.text(3.45, LIMIT + 0.15, f"configured limit = {LIMIT} per {WINDOW} s", ha="right", fontsize=9)
    for bar, v in zip(bars, values, strict=True):
        ax.text(bar.get_x() + bar.get_width() / 2, v + 0.15, str(v), ha="center", fontweight="bold")
    ax.set_ylabel(f"most requests admitted in any {WINDOW} s span")
    ax.set_ylim(0, max(values) + 2)
    ax.set_title("What each algorithm lets through, worst case on the traffic above", loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMAGES / "worst-case-window.png", dpi=150)
    plt.close(fig)


def parse_compare(path: Path) -> dict[str, dict[str, tuple[float, float, float, float]]]:
    """{backend: {algorithm: (throughput, p50, p95, p99)}} from compare.py's markdown."""
    result: dict[str, dict[str, tuple[float, float, float, float]]] = {}
    backend = ""
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("### "):
            backend = line.split()[1]
            result[backend] = {}
        m = re.match(r"^\| `(\w+)` \| ([\d,]+) \| ([\d,]+) \| ([\d,]+) \| ([\d,]+) \|", line)
        if m and backend:
            thr, p50, p95, p99 = (float(g.replace(",", "")) for g in m.groups()[1:])
            result[backend][m.group(1)] = (thr, p50, p95, p99)
    return result


def latency_chart(data: dict[str, dict[str, tuple[float, float, float, float]]]) -> None:
    backends = [b for b in ("memory", "redis") if b in data]
    fig, axes = plt.subplots(1, len(backends), figsize=(6.2 * len(backends), 4.6))
    axes = [axes] if len(backends) == 1 else list(axes)
    width = 0.26
    for ax, backend in zip(axes, backends, strict=True):
        scale, unit, fmt = (1, "µs", "{:.0f}") if backend == "memory" else (1000, "ms", "{:.1f}")
        for i, (label, color) in enumerate((("p50", "#8fb8de"), ("p95", BLUE), ("p99", "#1f3f73"))):
            vals = [data[backend][n][i + 1] / scale for n in ORDER]
            xs = [x + (i - 1) * width for x in range(len(ORDER))]
            bars = ax.bar(xs, vals, width=width, color=color, label=label)
            for bar, v in zip(bars, vals, strict=True):
                ax.text(bar.get_x() + bar.get_width() / 2, v * 1.01, fmt.format(v), ha="center", va="bottom", fontsize=8)
        ax.set_xticks(range(len(ORDER)))
        ax.set_xticklabels([TITLES[n].replace(" ", "\n", 1) for n in ORDER], fontsize=8.5)
        ax.set_ylabel(f"latency per check ({unit})")
        top = max(data[backend][n][3] for n in ORDER) / scale
        ax.set_ylim(0, top * 1.18)
        ax.set_title(f"{backend} backend", loc="left", fontweight="bold")
        ax.grid(axis="y", alpha=0.25)
    axes[0].legend(frameon=False, loc="upper left", ncols=3, bbox_to_anchor=(0, 1.0))
    fig.suptitle(
        "Closed-loop latency: 100 concurrent workers on one Python process (lower is better)",
        x=0.01, ha="left", fontweight="bold",
    )
    fig.text(
        0.01, 0.005,
        "Redis latency here is mostly queueing for the single event loop (100 workers / ~8k checks/s = ~12 ms); "
        "a lone check takes ~0.36 ms (see idle latency).",
        fontsize=8.5, color="#5b6675",
    )
    fig.tight_layout(rect=(0, 0.04, 1, 0.94))
    fig.savefig(IMAGES / "latency-by-algorithm.png", dpi=150)
    plt.close(fig)


def throughput_chart(data: dict[str, dict[str, tuple[float, float, float, float]]]) -> None:
    backends = [b for b in ("memory", "redis") if b in data]
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    width = 0.36
    for i, (backend, color) in enumerate(zip(backends, (BLUE, "#e08a1e"), strict=False)):
        vals = [data[backend][n][0] for n in ORDER]
        xs = [x + (i - 0.5) * width for x in range(len(ORDER))]
        bars = ax.bar(xs, vals, width=width, color=color, label=backend)
        for bar, v in zip(bars, vals, strict=True):
            ax.text(bar.get_x() + bar.get_width() / 2, v * 1.02, f"{v / 1000:.1f}k", ha="center", fontsize=8.5)
    ax.set_xticks(range(len(ORDER)))
    ax.set_xticklabels([TITLES[n] for n in ORDER], fontsize=9)
    ax.set_ylabel("checks per second (one Python process)")
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}k"))
    ax.legend(frameon=False, loc="upper right")
    ax.set_title("Throughput of one process, in-memory vs Redis", loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMAGES / "throughput.png", dpi=150)
    plt.close(fig)


def parse_locust(path: Path) -> dict[str, dict[str, int]]:
    text = path.read_text(encoding="utf-8")
    text = text[text.index("Response time percentiles") :]
    cols = ["50", "66", "75", "80", "90", "95", "98", "99", "99.9", "99.99", "100"]
    out: dict[str, dict[str, int]] = {}
    for m in re.finditer(r"POST /check \[(\w+)\]\s+((?:\d+\s+){11})\d+", text):
        out[m.group(1)] = dict(zip(cols, map(int, m.group(2).split()), strict=True))
    return out


def locust_chart(data: dict[str, dict[str, int]]) -> None:
    fig, ax = plt.subplots(figsize=(8.5, 4.2))
    width = 0.26
    for i, (label, key, color) in enumerate((("p50", "50", "#8fb8de"), ("p95", "95", BLUE), ("p99", "99", "#1f3f73"))):
        vals = [data[n][key] for n in ORDER]
        xs = [x + (i - 1) * width for x in range(len(ORDER))]
        bars = ax.bar(xs, vals, width=width, color=color, label=label)
        for bar, v in zip(bars, vals, strict=True):
            ax.text(bar.get_x() + bar.get_width() / 2, v + 0.6, str(v), ha="center", fontsize=8.5)
    ax.set_xticks(range(len(ORDER)))
    ax.set_xticklabels([TITLES[n] for n in ORDER], fontsize=9)
    ax.set_ylabel("end-to-end HTTP latency (ms)")
    ax.legend(frameon=False)
    ax.set_title("1000 concurrent users, ~1000 req/s, Redis backend", loc="left", fontweight="bold")
    fig.tight_layout()
    fig.savefig(IMAGES / "locust-latency.png", dpi=150)
    plt.close(fig)


def main() -> None:
    IMAGES.mkdir(parents=True, exist_ok=True)
    style()
    sim = asyncio.run(simulate())
    behaviour(sim)
    burst_chart(sim)
    print("wrote algorithm-behavior.png, worst-case-window.png")

    compare = BENCH / "compare.md"
    if compare.exists():
        data = parse_compare(compare)
        latency_chart(data)
        throughput_chart(data)
        print("wrote latency-by-algorithm.png, throughput.png")
    locust = BENCH / "locust-1000-users.txt"
    if locust.exists():
        locust_chart(parse_locust(locust))
        print("wrote locust-latency.png")


if __name__ == "__main__":
    main()
