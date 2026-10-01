"""FastAPI app: ``uvicorn src.main:app``."""

from __future__ import annotations

import logging
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse

from src.config import Settings
from src.metrics import CONTENT_TYPE, Metrics
from src.models import Algorithm, CheckRequest, CheckResponse, HealthResponse
from src.service import RateLimiterUnavailable, RateLimitService
from src.storage import MemoryStorage, RedisStorage, Storage

logger = logging.getLogger(__name__)


def build_storage(settings: Settings) -> Storage:
    if settings.storage_backend == "redis":
        return RedisStorage(
            settings.redis_url,
            socket_timeout=settings.redis_socket_timeout,
            max_connections=settings.redis_max_connections,
        )
    return MemoryStorage()


async def get_service(request: Request) -> RateLimitService:
    # `async def` on purpose: FastAPI runs a plain `def` dependency in a worker thread, and that
    # thread hop (plus GIL contention) cost more per request than the rate limit decision itself.
    return request.app.state.service


def create_app(settings: Settings | None = None, storage: Storage | None = None) -> FastAPI:
    """Build the app. Pass ``storage`` to inject a backend (tests); otherwise one is built from settings."""
    settings = settings or Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        # `is not None`, not `or`: an empty MemoryStorage is falsy (it defines __len__).
        store = storage if storage is not None else build_storage(settings)
        await store.connect()
        metrics = Metrics(a.value for a in Algorithm)
        app.state.storage = store
        app.state.settings = settings
        app.state.service = RateLimitService.create(
            store,
            default_algorithm=settings.default_algorithm.value,
            fail_open=settings.fail_open,
            metrics=metrics,
        )
        logger.info(
            "rate limiter up: backend=%s default_algorithm=%s fail_open=%s",
            settings.storage_backend,
            settings.default_algorithm.value,
            settings.fail_open,
        )
        try:
            yield
        finally:
            await store.close()

    app = FastAPI(
        title="Rate Limiter Service",
        version="1.0.0",
        description="Token bucket, sliding window, fixed window and leaky bucket rate limiting "
        "over in-memory or Redis state.",
        lifespan=lifespan,
    )

    @app.post("/check", response_model=CheckResponse, response_model_exclude_none=True)
    async def check(
        body: CheckRequest, service: Annotated[RateLimitService, Depends(get_service)]
    ) -> CheckResponse:
        """Count one request against `key`. Always answers 200: `allowed` carries the verdict."""
        try:
            decision = await service.check(
                body.key, body.limit, body.window, body.algorithm.value if body.algorithm else None
            )
        except RateLimiterUnavailable:
            raise HTTPException(
                status_code=503, detail="rate limiter storage unavailable", headers={"Retry-After": "1"}
            ) from None

        result = decision.result
        return CheckResponse(
            allowed=result.allowed,
            remaining=result.remaining,
            reset_at=math.ceil(result.reset_at),
            limit=result.limit,
            algorithm=Algorithm(decision.algorithm),
            retry_after=round(result.retry_after, 3),
            delay=round(result.delay, 3) if decision.algorithm == Algorithm.LEAKY_BUCKET else None,
            degraded=decision.degraded,
        )

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint(service: Annotated[RateLimitService, Depends(get_service)]) -> Response:
        return Response(service.metrics.render(), media_type=CONTENT_TYPE)

    @app.get("/health", response_model=HealthResponse)
    async def health(request: Request) -> JSONResponse:
        """200 when the storage backend answers, 503 when it does not."""
        store: Storage = request.app.state.storage
        backend = request.app.state.settings.storage_backend
        up = await store.ping()
        body = HealthResponse(
            status="ok" if up else "degraded",
            storage=backend,
            redis=("connected" if up else "disconnected") if backend == "redis" else "not_configured",
        )
        return JSONResponse(body.model_dump(), status_code=200 if up else 503)

    return app


app = create_app()
