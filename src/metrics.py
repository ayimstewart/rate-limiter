"""Prometheus metrics.

Each ``Metrics`` owns its own registry, so several apps (or tests) in one process never
collide on metric names and ``/metrics`` exports exactly this service's series.

Labels are deliberately low-cardinality: algorithm only. Never label by rate-limit key;
one series per user would take Prometheus down long before it took the limiter down.
"""

from __future__ import annotations

from collections.abc import Iterable

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Histogram,
    disable_created_metrics,
    generate_latest,
)

# Decisions are sub-millisecond in memory and ~1ms against local Redis; the default
# Prometheus buckets (5ms and up) would put everything in the first bucket.
LATENCY_BUCKETS = (0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1.0)

CONTENT_TYPE = CONTENT_TYPE_LATEST

# prometheus_client adds a `<name>_created` timestamp series next to every counter and histogram.
# Nothing here queries them and they double the series count, so don't export them.
disable_created_metrics()


class Metrics:
    def __init__(self, algorithms: Iterable[str] = (), registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        self.checks = Counter(
            "rate_limit_checks_total", "Rate limit checks performed.", ["algorithm"], registry=self.registry
        )
        self.allowed = Counter(
            "rate_limit_allowed_total", "Checks that admitted the request.", ["algorithm"], registry=self.registry
        )
        self.denied = Counter(
            "rate_limit_denied_total", "Checks that rejected the request.", ["algorithm"], registry=self.registry
        )
        self.errors = Counter(
            "rate_limit_errors_total",
            "Checks that failed because the storage backend errored, by the policy applied.",
            ["algorithm", "policy"],
            registry=self.registry,
        )
        self.latency = Histogram(
            "rate_limit_latency_seconds",
            "Time spent making one rate limit decision, storage round trip included.",
            ["algorithm"],
            buckets=LATENCY_BUCKETS,
            registry=self.registry,
        )
        # Materialise every series at 0 so dashboards and rate() queries work before the first request.
        for algorithm in algorithms:
            for counter in (self.checks, self.allowed, self.denied):
                counter.labels(algorithm)
            self.latency.labels(algorithm)
            for policy in ("open", "closed"):
                self.errors.labels(algorithm, policy)

    def record(self, algorithm: str, *, allowed: bool, seconds: float) -> None:
        self.checks.labels(algorithm).inc()
        (self.allowed if allowed else self.denied).labels(algorithm).inc()
        self.latency.labels(algorithm).observe(seconds)

    def record_error(self, algorithm: str, *, fail_open: bool, seconds: float) -> None:
        self.errors.labels(algorithm, "open" if fail_open else "closed").inc()
        self.latency.labels(algorithm).observe(seconds)

    def render(self) -> bytes:
        return generate_latest(self.registry)
