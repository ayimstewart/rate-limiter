"""Load test for POST /check.

    # all four algorithms interleaved, 1000 concurrent users, per-algorithm percentiles
    locust -f benchmarks/locustfile.py --headless -u 1000 -r 100 -t 60s \
        --host http://localhost:8000 --csv benchmarks/results/locust

    # one algorithm
    ALGORITHM=sliding_window locust -f benchmarks/locustfile.py ...

    # no think time: push the service to saturation instead of simulating users
    THINK_TIME=0 locust ...

Environment:
    ALGORITHM    token_bucket | sliding_window | fixed_window | leaky_bucket | all   (default: all)
    LIMIT        requests allowed per window                                        (default: 100)
    WINDOW       window in seconds                                                  (default: 10)
    KEYS         distinct rate-limit keys the users spread over                     (default: 10000)
    THINK_TIME   mean seconds a user pauses between requests; actual wait is
                 uniform in [0.5x, 1.5x]. 0 disables it.                            (default: 1.0)

With ALGORITHM=all each request is tagged ``POST /check [<algorithm>]`` so Locust reports
p50/p95/p99 per algorithm from one run under identical conditions.
"""

from __future__ import annotations

import os
import random

from locust import FastHttpUser, between, constant, task

ALGORITHMS = ["token_bucket", "sliding_window", "fixed_window", "leaky_bucket"]

ALGORITHM = os.environ.get("ALGORITHM", "all")
LIMIT = int(os.environ.get("LIMIT", "100"))
WINDOW = int(os.environ.get("WINDOW", "10"))
KEYS = int(os.environ.get("KEYS", "10000"))
THINK_TIME = float(os.environ.get("THINK_TIME", "1.0"))

if ALGORITHM != "all" and ALGORITHM not in ALGORITHMS:
    raise SystemExit(f"ALGORITHM must be 'all' or one of {ALGORITHMS}, got {ALGORITHM!r}")


class CheckUser(FastHttpUser):
    wait_time = between(THINK_TIME * 0.5, THINK_TIME * 1.5) if THINK_TIME > 0 else constant(0)

    @task
    def check(self) -> None:
        algorithm = random.choice(ALGORITHMS) if ALGORITHM == "all" else ALGORITHM
        payload = {
            "key": f"user:{random.randrange(KEYS)}",
            "algorithm": algorithm,
            "limit": LIMIT,
            "window": WINDOW,
        }
        # A denied request is a correct answer (HTTP 200, allowed=false), not a failure.
        with self.client.post("/check", json=payload, name=f"POST /check [{algorithm}]", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"HTTP {response.status_code}")
            elif "allowed" not in response.json():
                response.failure("response has no 'allowed' field")
            elif response.json().get("degraded"):
                response.failure("degraded: storage backend failed and the service failed open")
