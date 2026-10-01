# Rate Limiter Service

> The original project brief, kept as a record of the process. The finished project is
> described in the top-level [README](../README.md); where the implementation deliberately
> departs from this brief, the README's *Design decisions* section says so.

A production-grade rate limiting service implementing multiple algorithms with Redis-backed
distributed state, comprehensive benchmarks, and observability.

## Project goal

Build a standalone rate limiter that can be used as middleware or a microservice.
Demonstrate understanding of distributed systems, concurrency, and performance trade-offs.

## Core requirements

### Algorithms to implement

1. **Token Bucket** - allows bursts up to bucket size, refills at constant rate
2. **Sliding Window Log** - precise but memory-heavy, stores timestamps
3. **Fixed Window Counter** - simple, but has boundary burst problem
4. **Leaky Bucket** - smooths traffic at constant rate

Each algorithm must be implemented as a separate strategy class with a common interface.

### Storage backends

- **In-memory** - for single-instance deployments (Python dict with locks)
- **Redis** - for distributed deployments (atomic Lua scripts for correctness)

Both backends must implement the same interface. Document trade-offs in the README.

### API endpoints

```
POST /check
Body: { "key": "user:123", "algorithm": "token_bucket", "limit": 100, "window": 60 }
Response: { "allowed": true, "remaining": 87, "reset_at": 1234567890 }

GET /metrics
Response: Prometheus-format metrics

GET /health
Response: { "status": "ok", "redis": "connected" }
```

### Configuration (environment variables)

- `REDIS_URL` - Redis connection string
- `STORAGE_BACKEND` - `memory` or `redis`
- `DEFAULT_ALGORITHM` - fallback algorithm
- `LOG_LEVEL` - logging verbosity

## Project structure

```
rate-limiter/
├── src/
│   ├── __init__.py
│   ├── main.py              # FastAPI app
│   ├── config.py            # Settings via pydantic
│   ├── algorithms/
│   │   ├── __init__.py
│   │   ├── base.py          # Abstract RateLimiter
│   │   ├── token_bucket.py
│   │   ├── sliding_window.py
│   │   ├── fixed_window.py
│   │   └── leaky_bucket.py
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── base.py          # Abstract Storage
│   │   ├── memory.py
│   │   └── redis.py
│   ├── middleware.py        # FastAPI middleware
│   └── metrics.py           # Prometheus metrics
├── tests/
│   ├── test_algorithms.py
│   ├── test_storage.py
│   └── test_api.py
├── benchmarks/
│   ├── locustfile.py        # Load test
│   └── compare.py           # Algorithm comparison script
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Implementation steps

### Phase 1: core algorithms (days 1-2)

1. Define the `RateLimiter` abstract base class:

   ```python
   class RateLimiter(ABC):
       @abstractmethod
       async def check(self, key: str, limit: int, window: int) -> RateLimitResult:
           pass
   ```

2. `TokenBucket`: store `(tokens, last_refill)` per key; refill rate = limit / window; use a
   monotonic clock for time.
3. `SlidingWindowLog`: store a sorted list of timestamps per key; remove timestamps older than
   the window; allow if count < limit.
4. `FixedWindowCounter`: key = `{user}:{window_start}`; increment the counter with a TTL; reset
   on the window boundary.
5. `LeakyBucket`: similar to token bucket but enforces a constant drain rate.

### Phase 2: storage backends (days 3-4)

1. Define the `Storage` abstract base:

   ```python
   class Storage(ABC):
       @abstractmethod
       async def get(self, key: str) -> dict: ...
       @abstractmethod
       async def set(self, key: str, value: dict, ttl: int) -> None: ...
       @abstractmethod
       async def atomic_update(self, key: str, fn: Callable) -> dict: ...
   ```

2. In-memory: use an `asyncio.Lock` per key to prevent race conditions.
3. Redis: write Lua scripts for atomic operations (`token_bucket.lua`, `sliding_window.lua`,
   etc.). Load scripts on startup, use `EVALSHA` for performance.

### Phase 3: FastAPI service (days 5-6)

1. Pydantic models for request/response.
2. `/check` endpoint that routes to the configured algorithm.
3. Middleware that can be added to any FastAPI app.
4. Prometheus metrics: `rate_limit_checks_total`, `rate_limit_allowed_total`,
   `rate_limit_denied_total`, `rate_limit_latency_seconds`.

### Phase 4: benchmarks (day 7)

1. `benchmarks/locustfile.py`: simulate 1000 concurrent users, test each algorithm, record
   p50, p95, p99 latencies.
2. `benchmarks/compare.py`: run each algorithm for 60 seconds, generate a comparison table,
   output as markdown for the README.

### Phase 5: tests (day 8)

- Unit tests for each algorithm (edge cases: empty bucket, full bucket, clock skew).
- Integration tests with Redis (use `fakeredis` or testcontainers).
- API tests with `httpx.AsyncClient`.
- Target: 80%+ coverage.

### Phase 6: documentation (day 9)

The README must include an architecture diagram, an algorithm comparison table with benchmark
results, a "Design decisions" section (why Lua scripts for Redis, why a monotonic clock,
fail-open vs fail-closed), setup instructions, and example curl commands.

## Testing requirements

```bash
pytest tests/ -v --cov=src

docker-compose up -d redis
locust -f benchmarks/locustfile.py --headless -u 1000 -r 100 -t 60s

python benchmarks/compare.py
```

## Success criteria

- [ ] All 4 algorithms implemented and tested
- [ ] Both storage backends work with the same interface
- [ ] Redis implementation uses atomic Lua scripts
- [ ] Benchmarks show latency for each algorithm
- [ ] README has architecture diagram and comparison table
- [ ] Docker Compose brings up the full stack with one command
- [ ] Test coverage > 80%
- [ ] Prometheus metrics exposed

## What this proves

Distributed systems fundamentals; understanding of concurrency and atomicity; the ability to
benchmark and make data-driven decisions; knowledge of production concerns (failover, clock
skew); clear technical communication.

## References

- [Stripe: Scaling your API with rate limiters](https://stripe.com/blog/rate-limiters)
- [Cloudflare: How we built rate limiting capable of scaling to millions of domains](https://blog.cloudflare.com/counting-things-a-lot-of-different-things/)
- Redis Lua scripting docs

## Stretch goals

- gRPC interface
- Sliding window counter (hybrid approach)
- Tiered limits (free vs pro users)
- Distributed coordination with Redis pub/sub for cache invalidation
- Deploy to AWS/GCP with Terraform
