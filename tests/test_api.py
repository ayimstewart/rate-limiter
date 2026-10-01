"""HTTP API, metrics, middleware and the fail-open / fail-closed policy."""

from __future__ import annotations

import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import fakeredis
import httpx
import pytest
from asgi_lifespan import LifespanManager
from fastapi import FastAPI
from pydantic import ValidationError

from src.algorithms import ALGORITHMS
from src.clock import ManualClock
from src.config import Settings
from src.main import app as default_app
from src.main import build_storage, create_app
from src.middleware import RateLimitMiddleware, client_ip_key, header_key
from src.models import Algorithm
from src.service import RateLimiterUnavailable, RateLimitService
from src.storage import MemoryStorage, RedisStorage, StorageError

T0 = 1_000_000.0


class BrokenStorage(MemoryStorage):
    """Every operation fails, like a Redis that fell over."""

    async def atomic_update(self, key, fn, ttl):
        raise StorageError("connection refused")

    async def ping(self):
        return False


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with LifespanManager(app) as manager:
        transport = httpx.ASGITransport(app=manager.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


def make_app(storage=None, **settings) -> FastAPI:
    return create_app(Settings(_env_file=None, **settings), storage if storage is not None else MemoryStorage(sweep_interval=0))


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with running(make_app()) as c:
        yield c


def check_body(**overrides) -> dict:
    return {"key": "user:123", "algorithm": "token_bucket", "limit": 3, "window": 60} | overrides


def metric(text: str, name: str, **labels: str) -> float:
    selector = ",".join(f'{k}="{v}"' for k, v in labels.items())
    match = re.search(rf"^{name}\{{{re.escape(selector)}\}} ([0-9.e+-]+)$", text, re.MULTILINE)
    assert match, f"{name}{{{selector}}} not found in:\n{text}"
    return float(match.group(1))


# ---------------------------------------------------------------------------
# POST /check
# ---------------------------------------------------------------------------


class TestCheck:
    async def test_documented_example(self, client):
        response = await client.post("/check", json={"key": "user:123", "algorithm": "token_bucket", "limit": 100, "window": 60})

        assert response.status_code == 200
        body = response.json()
        assert body["allowed"] is True
        assert body["remaining"] == 99
        assert isinstance(body["reset_at"], int)
        assert body["algorithm"] == "token_bucket"
        assert body["retry_after"] == 0
        assert body["degraded"] is False

    async def test_allows_up_to_the_limit_then_denies_with_retry_after(self, client):
        responses = [(await client.post("/check", json=check_body())).json() for _ in range(4)]

        assert [r["allowed"] for r in responses] == [True, True, True, False]
        assert [r["remaining"] for r in responses] == [2, 1, 0, 0]
        assert responses[-1]["retry_after"] > 0

    @pytest.mark.parametrize("name", sorted(ALGORITHMS))
    async def test_every_algorithm_is_reachable(self, client, name):
        body = (await client.post("/check", json=check_body(algorithm=name, limit=1))).json()
        assert body["algorithm"] == name
        assert body["allowed"] is True
        assert (await client.post("/check", json=check_body(algorithm=name, limit=1))).json()["allowed"] is False

    async def test_algorithm_enum_matches_the_implemented_algorithms(self):
        assert {a.value for a in Algorithm} == set(ALGORITHMS)

    async def test_omitted_algorithm_uses_the_configured_default(self):
        async with running(make_app(default_algorithm="fixed_window")) as client:
            body = (await client.post("/check", json={"key": "k", "limit": 5, "window": 60})).json()
        assert body["algorithm"] == "fixed_window"

    async def test_delay_is_reported_for_leaky_bucket_only(self, client):
        leaky = (await client.post("/check", json=check_body(algorithm="leaky_bucket"))).json()
        token = (await client.post("/check", json=check_body(algorithm="token_bucket"))).json()
        assert "delay" in leaky
        assert "delay" not in token

    async def test_keys_and_algorithms_do_not_share_state(self, client):
        await client.post("/check", json=check_body(limit=1))
        assert (await client.post("/check", json=check_body(limit=1))).json()["allowed"] is False
        assert (await client.post("/check", json=check_body(limit=1, key="user:456"))).json()["allowed"] is True
        assert (await client.post("/check", json=check_body(limit=1, algorithm="fixed_window"))).json()["allowed"] is True

    @pytest.mark.parametrize(
        "bad",
        [
            {"limit": 0},
            {"limit": -5},
            {"limit": 10**9},
            {"window": 0},
            {"window": -1},
            {"window": 10**9},
            {"key": ""},
            {"key": "x" * 257},
            {"algorithm": "round_robin"},
            {"limit": "lots"},
            {"windows": 60},  # typo'd extra field
        ],
    )
    async def test_invalid_requests_are_rejected_with_422(self, client, bad):
        response = await client.post("/check", json=check_body(**bad))
        assert response.status_code == 422

    async def test_missing_required_fields_are_rejected(self, client):
        assert (await client.post("/check", json={"key": "k"})).status_code == 422
        assert (await client.post("/check", content=b"not json")).status_code == 422


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


class TestHealth:
    async def test_memory_backend(self, client):
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "storage": "memory", "redis": "not_configured"}

    async def test_redis_connected(self):
        server = fakeredis.FakeServer()
        storage = RedisStorage(client=fakeredis.FakeAsyncRedis(server=server))
        async with running(make_app(storage, storage_backend="redis")) as client:
            response = await client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "storage": "redis", "redis": "connected"}

    async def test_redis_down_reports_503(self):
        server = fakeredis.FakeServer()
        storage = RedisStorage(client=fakeredis.FakeAsyncRedis(server=server))
        async with running(make_app(storage, storage_backend="redis")) as client:
            server.connected = False
            response = await client.get("/health")
        assert response.status_code == 503
        assert response.json() == {"status": "degraded", "storage": "redis", "redis": "disconnected"}


# ---------------------------------------------------------------------------
# GET /metrics
# ---------------------------------------------------------------------------


class TestMetrics:
    async def test_exposes_the_required_series_in_prometheus_format(self, client):
        response = await client.get("/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        for name in (
            "rate_limit_checks_total",
            "rate_limit_allowed_total",
            "rate_limit_denied_total",
            "rate_limit_latency_seconds_bucket",
        ):
            assert name in response.text

    async def test_series_exist_at_zero_before_any_traffic(self, client):
        text = (await client.get("/metrics")).text
        for name in ALGORITHMS:
            assert metric(text, "rate_limit_checks_total", algorithm=name) == 0

    async def test_counts_checks_allowed_and_denied_per_algorithm(self, client):
        for _ in range(5):  # limit 3: 3 allowed, 2 denied
            await client.post("/check", json=check_body())
        await client.post("/check", json=check_body(algorithm="fixed_window"))

        text = (await client.get("/metrics")).text
        assert metric(text, "rate_limit_checks_total", algorithm="token_bucket") == 5
        assert metric(text, "rate_limit_allowed_total", algorithm="token_bucket") == 3
        assert metric(text, "rate_limit_denied_total", algorithm="token_bucket") == 2
        assert metric(text, "rate_limit_checks_total", algorithm="fixed_window") == 1
        assert metric(text, "rate_limit_latency_seconds_count", algorithm="token_bucket") == 5
        assert metric(text, "rate_limit_latency_seconds_sum", algorithm="token_bucket") > 0

    async def test_rejected_requests_are_not_counted_as_checks(self, client):
        await client.post("/check", json=check_body(limit=0))
        text = (await client.get("/metrics")).text
        assert metric(text, "rate_limit_checks_total", algorithm="token_bucket") == 0

    async def test_apps_do_not_share_metrics(self):
        async with running(make_app()) as a, running(make_app()) as b:
            await a.post("/check", json=check_body())
            assert metric((await a.get("/metrics")).text, "rate_limit_checks_total", algorithm="token_bucket") == 1
            assert metric((await b.get("/metrics")).text, "rate_limit_checks_total", algorithm="token_bucket") == 0


# ---------------------------------------------------------------------------
# Fail open / fail closed
# ---------------------------------------------------------------------------


class TestStorageFailure:
    async def test_fail_open_admits_the_request_and_says_so(self):
        async with running(make_app(BrokenStorage(), fail_open=True)) as client:
            response = await client.post("/check", json=check_body())
            metrics = (await client.get("/metrics")).text

        assert response.status_code == 200
        body = response.json()
        assert body["allowed"] is True
        assert body["degraded"] is True
        assert body["remaining"] == 3
        assert metric(metrics, "rate_limit_errors_total", algorithm="token_bucket", policy="open") == 1
        assert metric(metrics, "rate_limit_checks_total", algorithm="token_bucket") == 0  # not a real decision

    async def test_fail_closed_answers_503(self):
        async with running(make_app(BrokenStorage(), fail_open=False)) as client:
            response = await client.post("/check", json=check_body())
            metrics = (await client.get("/metrics")).text

        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert metric(metrics, "rate_limit_errors_total", algorithm="token_bucket", policy="closed") == 1

    async def test_redis_outage_mid_flight_fails_open_then_recovers(self):
        server = fakeredis.FakeServer()
        storage = RedisStorage(client=fakeredis.FakeAsyncRedis(server=server), socket_timeout=0.05)
        async with running(make_app(storage, storage_backend="redis")) as client:
            assert (await client.post("/check", json=check_body())).json()["degraded"] is False

            server.connected = False
            during = (await client.post("/check", json=check_body())).json()
            assert (during["allowed"], during["degraded"]) == (True, True)

            server.connected = True
            after = (await client.post("/check", json=check_body())).json()
            assert after["degraded"] is False
            assert after["remaining"] == 1  # the first request survived the outage; the degraded one was never counted

    async def test_error_logging_is_throttled(self, caplog):
        service = RateLimitService.create(BrokenStorage(), fail_open=True)
        for _ in range(50):
            await service.check("k", 1, 60)
        assert caplog.text.count("storage error") == 1


# ---------------------------------------------------------------------------
# Service, settings, wiring
# ---------------------------------------------------------------------------


class TestService:
    async def test_unknown_algorithm_is_a_value_error(self):
        service = RateLimitService.create(MemoryStorage(sweep_interval=0))
        with pytest.raises(ValueError, match="unknown algorithm"):
            await service.check("k", 1, 60, algorithm="nope")

    async def test_unknown_default_algorithm_is_rejected_at_construction(self):
        with pytest.raises(ValueError, match="default algorithm"):
            RateLimitService.create(MemoryStorage(sweep_interval=0), default_algorithm="nope")

    async def test_fail_closed_raises(self):
        service = RateLimitService.create(BrokenStorage(), fail_open=False)
        with pytest.raises(RateLimiterUnavailable):
            await service.check("k", 1, 60)


class TestSettings:
    def test_defaults(self):
        s = Settings(_env_file=None)
        assert (s.storage_backend, s.default_algorithm, s.log_level, s.fail_open) == (
            "memory",
            Algorithm.TOKEN_BUCKET,
            "INFO",
            True,
        )

    def test_reads_the_documented_environment_variables(self, monkeypatch):
        monkeypatch.setenv("REDIS_URL", "redis://cache:6380/2")
        monkeypatch.setenv("STORAGE_BACKEND", "redis")
        monkeypatch.setenv("DEFAULT_ALGORITHM", "sliding_window")
        monkeypatch.setenv("LOG_LEVEL", "debug")
        monkeypatch.setenv("FAIL_OPEN", "false")

        s = Settings(_env_file=None)
        assert s.redis_url == "redis://cache:6380/2"
        assert s.storage_backend == "redis"
        assert s.default_algorithm is Algorithm.SLIDING_WINDOW
        assert s.log_level == "DEBUG"
        assert s.fail_open is False

    @pytest.mark.parametrize(
        ("name", "value"),
        [("STORAGE_BACKEND", "postgres"), ("DEFAULT_ALGORITHM", "nope"), ("LOG_LEVEL", "loud")],
    )
    def test_rejects_invalid_values(self, monkeypatch, name, value):
        monkeypatch.setenv(name, value)
        with pytest.raises(ValidationError):
            Settings(_env_file=None)

    def test_build_storage_selects_the_backend(self):
        assert isinstance(build_storage(Settings(_env_file=None)), MemoryStorage)
        redis_storage = build_storage(Settings(_env_file=None, storage_backend="redis"))
        assert isinstance(redis_storage, RedisStorage)

    def test_module_level_app_exists_for_uvicorn(self):
        assert isinstance(default_app, FastAPI)

    async def test_lifespan_closes_the_storage(self):
        closed = []

        class Tracking(MemoryStorage):
            async def close(self):
                closed.append(True)

        async with running(make_app(Tracking(sweep_interval=0))):
            assert closed == []
        assert closed == [True]


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------


def protected_app(
    clock: ManualClock,
    *,
    storage=None,
    fail_open: bool = True,
    **middleware,
) -> FastAPI:
    storage = storage if storage is not None else MemoryStorage(clock=clock, sweep_interval=0)
    service = RateLimitService.create(storage, fail_open=fail_open, clock=clock)
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, service=service, **{"limit": 2, "window": 10} | middleware)

    @app.get("/hello")
    async def hello():
        return {"hello": "world"}

    @app.get("/health")
    async def health():
        return {"ok": True}

    return app


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(T0)


class TestMiddleware:
    async def test_admits_up_to_the_limit_with_rate_limit_headers(self, clock):
        async with running(protected_app(clock)) as client:
            first = await client.get("/hello")
            second = await client.get("/hello")

        assert first.status_code == second.status_code == 200
        assert first.json() == {"hello": "world"}
        assert first.headers["x-ratelimit-limit"] == "2"
        assert first.headers["x-ratelimit-remaining"] == "1"
        assert second.headers["x-ratelimit-remaining"] == "0"
        assert int(first.headers["x-ratelimit-reset"]) > T0
        assert "retry-after" not in first.headers

    async def test_rejects_over_the_limit_with_429_and_retry_after(self, clock):
        async with running(protected_app(clock, algorithm="fixed_window")) as client:
            for _ in range(2):
                await client.get("/hello")
            denied = await client.get("/hello")

            assert denied.status_code == 429
            assert denied.json() == {"detail": "rate limit exceeded"}
            assert denied.headers["retry-after"] == "10"
            assert denied.headers["x-ratelimit-remaining"] == "0"

            clock.advance(10)
            assert (await client.get("/hello")).status_code == 200

    async def test_excluded_paths_are_never_limited(self, clock):
        async with running(protected_app(clock)) as client:
            for _ in range(10):
                assert (await client.get("/health")).status_code == 200

    async def test_clients_are_limited_independently_by_key_func(self, clock):
        app = protected_app(clock, limit=1, key_func=header_key("x-api-key"))
        async with running(app) as client:
            assert (await client.get("/hello", headers={"x-api-key": "alice"})).status_code == 200
            assert (await client.get("/hello", headers={"x-api-key": "alice"})).status_code == 429
            assert (await client.get("/hello", headers={"x-api-key": "bob"})).status_code == 200
            # no header: falls back to the peer address
            assert (await client.get("/hello")).status_code == 200
            assert (await client.get("/hello")).status_code == 429

    async def test_key_func_can_exempt_a_request(self, clock):
        app = protected_app(clock, limit=1, key_func=lambda r: None if r.headers.get("x-internal") else "public")
        async with running(app) as client:
            for _ in range(5):
                assert (await client.get("/hello", headers={"x-internal": "1"})).status_code == 200
            assert (await client.get("/hello")).status_code == 200
            assert (await client.get("/hello")).status_code == 429

    async def test_fail_open_passes_requests_through_and_flags_them(self, clock):
        async with running(protected_app(clock, storage=BrokenStorage())) as client:
            response = await client.get("/hello")
        assert response.status_code == 200
        assert response.headers["x-ratelimit-degraded"] == "true"

    async def test_fail_closed_returns_503(self, clock):
        async with running(protected_app(clock, storage=BrokenStorage(), fail_open=False)) as client:
            response = await client.get("/hello")
        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"

    async def test_unknown_algorithm_is_rejected_when_the_app_is_built(self, clock):
        service = RateLimitService.create(MemoryStorage(sweep_interval=0))
        with pytest.raises(ValueError, match="unknown algorithm"):
            RateLimitMiddleware(FastAPI(), service, limit=1, window=1, algorithm="nope")

    async def test_works_on_a_redis_backend(self, clock):
        storage = RedisStorage(client=fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer()), clock=clock)
        await storage.connect()  # a bare middleware has no lifespan of its own: the host app connects the storage
        async with running(protected_app(clock, storage=storage, limit=1)) as client:
            assert (await client.get("/hello")).status_code == 200
            assert (await client.get("/hello")).status_code == 429

    async def test_non_http_scopes_pass_through(self, clock):
        seen = []

        async def inner(scope, receive, send):
            seen.append(scope["type"])

        service = RateLimitService.create(MemoryStorage(clock=clock, sweep_interval=0), clock=clock)
        middleware = RateLimitMiddleware(inner, service, limit=1, window=1)
        await middleware({"type": "websocket", "path": "/ws"}, None, None)
        assert seen == ["websocket"]

    def test_client_ip_key(self):
        class Req:
            def __init__(self, client):
                self.client = client

        assert client_ip_key(Req(type("C", (), {"host": "10.0.0.7"})())) == "10.0.0.7"
        assert client_ip_key(Req(None)) == "unknown"

