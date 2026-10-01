<!-- docker compose --profile bench run --rm bench python benchmarks/compare.py --backend both --redis-url redis://redis:6379/0 --duration 60 -->
## Algorithm comparison

Python 3.12.14 on Linux 6.18.33.2-microsoft-standard-WSL2 (x86_64), 32 logical CPUs. uvloop event loop, Redis 7.4.11.

### memory backend

`100` concurrent workers, `1000` keys, limit `50` per `10`s, `60`s measured per algorithm.

| Algorithm | Throughput (checks/s) | p50 (µs) | p95 (µs) | p99 (µs) | Allowed | State per key at limit |
|---|---:|---:|---:|---:|---:|---:|
| `fixed_window` | 99,378 | 8 | 14 | 31 | 5.0% | 374 B |
| `leaky_bucket` | 102,640 | 8 | 13 | 29 | 4.9% | 376 B |
| `sliding_window` | 103,970 | 8 | 13 | 29 | 4.8% | 33,165 B |
| `token_bucket` | 107,787 | 8 | 13 | 27 | 4.6% | 294 B |

### redis backend

`100` concurrent workers, `1000` keys, limit `50` per `10`s, `60`s measured per algorithm.

| Algorithm | Throughput (checks/s) | p50 (µs) | p95 (µs) | p99 (µs) | Allowed | State per key at limit |
|---|---:|---:|---:|---:|---:|---:|
| `fixed_window` | 8,026 | 13,574 | 19,403 | 23,646 | 62.6% | 96 B |
| `leaky_bucket` | 8,052 | 13,411 | 18,383 | 22,923 | 70.6% | 128 B |
| `sliding_window` | 8,084 | 13,407 | 18,180 | 21,965 | 59.5% | 89,016 B |
| `token_bucket` | 8,018 | 13,389 | 18,717 | 23,174 | 71.0% | 128 B |

