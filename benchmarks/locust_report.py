"""Turn Locust's ``--csv`` output into a markdown table.

    python benchmarks/locust_report.py benchmarks/results/locust_stats.csv
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path


def main(path: str) -> None:
    with Path(path).open(newline="", encoding="utf-8") as fh:
        everything = list(csv.DictReader(fh))
    rows = sorted((r for r in everything if r["Name"] != "Aggregated"), key=lambda r: r["Name"])
    aggregated = next(r for r in everything if r["Name"] == "Aggregated")

    print("| Endpoint | Requests | Failures | req/s | p50 (ms) | p95 (ms) | p99 (ms) | max (ms) |")
    print("|---|---:|---:|---:|---:|---:|---:|---:|")
    for r in rows + [aggregated]:
        print(
            f"| {r['Name'] if r['Name'] != 'Aggregated' else '**all**'} | {r['Request Count']} | {r['Failure Count']} "
            f"| {float(r['Requests/s']):.0f} | {r['50%']} | {r['95%']} | {r['99%']} | {float(r['Max Response Time']):.0f} |"
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(sys.argv[1])
