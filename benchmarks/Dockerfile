# Linux load generator that lives on the compose network next to the service and Redis.
# Benchmarking from a Windows/macOS host measures Docker Desktop's port proxy and the host's
# asyncio loop more than the limiter; run it here instead:
#
#   docker compose --profile bench run --rm bench                                  # compare.py, both backends
#   docker compose --profile bench run --rm bench locust -f benchmarks/locustfile.py \
#       --headless -u 1000 -r 100 -t 60s --host http://rate-limiter:8000
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt locust

COPY src ./src
COPY benchmarks ./benchmarks

CMD ["python", "benchmarks/compare.py", "--backend", "both", "--redis-url", "redis://redis:6379/0"]
