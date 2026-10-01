### memory backend, 5000 sequential checks each

| Algorithm | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---:|---:|---:|
| `fixed_window` | 7 | 12 | 24 |
| `leaky_bucket` | 8 | 22 | 35 |
| `sliding_window` | 7 | 9 | 21 |
| `token_bucket` | 7 | 12 | 27 |

### redis backend, 5000 sequential checks each

| Algorithm | p50 (µs) | p95 (µs) | p99 (µs) |
|---|---:|---:|---:|
| `fixed_window` | 352 | 580 | 820 |
| `leaky_bucket` | 371 | 598 | 813 |
| `sliding_window` | 363 | 589 | 814 |
| `token_bucket` | 359 | 527 | 759 |

