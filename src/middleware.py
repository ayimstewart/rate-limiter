"""ASGI middleware: rate limit any FastAPI / Starlette app with this library.

    storage = MemoryStorage()                      # or RedisStorage(url)
    service = RateLimitService.create(storage)
    app.add_middleware(RateLimitMiddleware, service=service, limit=100, window=60)

Written as plain ASGI rather than ``BaseHTTPMiddleware`` so it adds no extra task per
request and does not interfere with streaming responses.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Collection

from starlette.datastructures import MutableHeaders
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from src.service import Decision, RateLimiterUnavailable, RateLimitService

KeyFunc = Callable[[Request], str | None]


def client_ip_key(request: Request) -> str | None:
    """Key by the TCP peer address.

    Behind a reverse proxy or load balancer this is the *proxy's* address, so every user
    shares one bucket. In that setup use :func:`header_key` with a header your proxy sets
    (and strips from client input), never a header the client controls.
    """
    return request.client.host if request.client else "unknown"


def header_key(name: str, *, fallback: KeyFunc = client_ip_key) -> KeyFunc:
    """Key by a request header (``X-API-Key``, ``X-User-Id`` ...), falling back if it is absent."""

    def key_func(request: Request) -> str | None:
        return request.headers.get(name) or fallback(request)

    return key_func


def _headers(decision: Decision) -> dict[str, str]:
    result = decision.result
    headers = {
        "X-RateLimit-Limit": str(result.limit),
        "X-RateLimit-Remaining": str(result.remaining),
        "X-RateLimit-Reset": str(math.ceil(result.reset_at)),
    }
    if decision.degraded:
        headers["X-RateLimit-Degraded"] = "true"
    if not result.allowed:
        headers["Retry-After"] = str(max(1, math.ceil(result.retry_after)))
    return headers


class RateLimitMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        service: RateLimitService,
        *,
        limit: int,
        window: int,
        algorithm: str | None = None,
        key_func: KeyFunc = client_ip_key,
        key_prefix: str = "http:",
        exclude_paths: Collection[str] = ("/health", "/metrics"),
    ) -> None:
        if algorithm is not None and algorithm not in service.limiters:
            raise ValueError(f"unknown algorithm {algorithm!r}; choose from {sorted(service.limiters)}")
        self.app = app
        self.service = service
        self.limit = limit
        self.window = window
        self.algorithm = algorithm
        self.key_func = key_func
        self.key_prefix = key_prefix
        self.exclude_paths = frozenset(exclude_paths)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"] in self.exclude_paths:
            await self.app(scope, receive, send)
            return

        key = self.key_func(Request(scope))
        if key is None:  # key_func opted this request out of limiting
            await self.app(scope, receive, send)
            return

        try:
            decision = await self.service.check(self.key_prefix + key, self.limit, self.window, self.algorithm)
        except RateLimiterUnavailable:
            response = JSONResponse(
                {"detail": "rate limiter unavailable"}, status_code=503, headers={"Retry-After": "1"}
            )
            await response(scope, receive, send)
            return

        headers = _headers(decision)
        if not decision.result.allowed:
            response = JSONResponse({"detail": "rate limit exceeded"}, status_code=429, headers=headers)
            await response(scope, receive, send)
            return

        async def send_with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                MutableHeaders(scope=message).update(headers)
            await send(message)

        await self.app(scope, receive, send_with_headers)
