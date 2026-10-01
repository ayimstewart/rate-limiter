"""Capture the screenshots in docs/images from the running stack.

Prerequisites: the full stack and Prometheus are up and have seen some traffic, e.g.

    docker compose --profile monitoring up -d --build
    docker compose --profile bench run --rm -e THINK_TIME=1 bench locust -f benchmarks/locustfile.py \
        --headless -u 800 -r 20 -t 150s --host http://rate-limiter:8000      # in another terminal
    pip install -r requirements-docs.txt
    python docs/generate_screenshots.py

Uses the Microsoft Edge (or, with --channel chrome, Google Chrome) already installed on the
machine through Playwright, so no browser is downloaded.

Terminal screenshots are rendered from real captured output stored next to the benchmark
results (docs/benchmarks/*.txt), not typed by hand.
"""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

ROOT = Path(__file__).resolve().parent.parent
IMAGES = ROOT / "docs" / "images"
BENCH = ROOT / "docs" / "benchmarks"

TERMINAL_CSS = """
body { margin:0; background:#eef1f5; font-family:'Segoe UI',Arial,sans-serif; padding:28px; }
.win { width: 1040px; border-radius:10px; overflow:hidden; box-shadow:0 8px 30px rgba(20,30,50,.25); }
.bar { background:#2b313b; padding:10px 14px; display:flex; align-items:center; gap:8px; }
.dot { width:12px; height:12px; border-radius:50%; }
.title { color:#aab4c3; font-size:13px; margin-left:10px; }
pre { margin:0; background:#161b22; color:#d6dde8; padding:18px 20px; font:13px/1.45 Consolas,'Cascadia Mono','Courier New',monospace; white-space:pre-wrap; }
.g { color:#56d364 } .r { color:#ff7b72 } .y { color:#e3b341 } .b { color:#79c0ff } .d { color:#7d8590 }
"""


def colour(line: str) -> str:
    safe = html.escape(line)
    if line.startswith("$ "):
        return f'<span class="b">{safe}</span>'
    for token, cls in (("passed", "g"), ("PASSED", "g"), ("healthy", "g"), ("(healthy)", "g"), ("failed", "r"), ("FAILED", "r")):
        safe = safe.replace(token, f'<span class="{cls}">{token}</span>')
    if "100%" in line:
        safe = safe.replace("100%", '<span class="g">100%</span>')
    return safe


def terminal(page: Page, title: str, text: str, out: Path) -> None:
    body = "\n".join(colour(line) for line in text.rstrip().splitlines())
    page.set_content(
        f"<style>{TERMINAL_CSS}</style><div class='win'><div class='bar'>"
        "<span class='dot' style='background:#ff5f57'></span><span class='dot' style='background:#febc2e'></span>"
        f"<span class='dot' style='background:#28c840'></span><span class='title'>{html.escape(title)}</span></div>"
        f"<pre>{body}</pre></div>"
    )
    page.locator(".win").screenshot(path=str(out))


def swagger(page: Page, base: str) -> None:
    page.goto(f"{base}/docs")
    page.wait_for_selector(".opblock-post")
    page.screenshot(path=str(IMAGES / "swagger-ui.png"))

    page.locator(".opblock-post .opblock-summary-control").click()
    page.get_by_role("button", name="Try it out").click()
    body = {"key": "user:123", "algorithm": "token_bucket", "limit": 3, "window": 60}
    page.locator("textarea.body-param__text").fill(json.dumps(body, indent=2))
    for _ in range(4):  # 3 allowed, then the 4th is denied: the last response shown is the interesting one
        page.get_by_role("button", name="Execute").click()
        page.wait_for_selector(".live-responses-table .response-col_status")
        page.wait_for_timeout(400)
    # Crop to the request and the live server response; the schema examples below it add 1500px of noise.
    op = page.locator(".opblock-post").bounding_box()
    live = page.locator(".live-responses-table").bounding_box()
    scroll_y = page.evaluate("window.scrollY")
    top = op["y"] + scroll_y
    page.screenshot(
        path=str(IMAGES / "swagger-check.png"),
        full_page=True,
        clip={"x": op["x"], "y": top, "width": op["width"], "height": live["y"] + scroll_y + live["height"] - top + 12},
    )


def metrics(page: Page, base: str) -> None:
    page.goto(f"{base}/metrics")
    page.screenshot(path=str(IMAGES / "metrics-endpoint.png"), clip={"x": 0, "y": 0, "width": 1100, "height": 620})


def prometheus(page: Page, prom: str) -> None:
    graphs = [
        ("prometheus-throughput.png", "sum by (algorithm) (rate(rate_limit_checks_total[30s]))"),
        (
            "prometheus-allowed-denied.png",
            'label_replace(sum(rate(rate_limit_allowed_total[30s])), "result", "allowed", "", "")'
            ' or label_replace(sum(rate(rate_limit_denied_total[30s])), "result", "denied", "", "")',
        ),
        (
            "prometheus-latency.png",
            "histogram_quantile(0.99, sum by (le, algorithm) (rate(rate_limit_latency_seconds_bucket[30s])))",
        ),
    ]
    for filename, expr in graphs:
        page.goto(f"{prom}/graph?g0.expr={quote(expr)}&g0.tab=0&g0.range_input=3m&g0.display_mode=lines")
        page.wait_for_selector("canvas, .uplot, svg", timeout=15000)
        page.wait_for_timeout(2500)
        page.screenshot(path=str(IMAGES / filename))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--channel", default="msedge", help="msedge | chrome")
    ap.add_argument("--only", choices=["web", "prometheus", "terminal"], help="capture just one group")
    args = ap.parse_args()

    IMAGES.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch(channel=args.channel)
        page = browser.new_page(viewport={"width": 1280, "height": 860}, device_scale_factor=1.5)
        if args.only in (None, "web"):
            swagger(page, args.base)
            metrics(page, args.base)
        if args.only in (None, "prometheus"):
            prometheus(page, args.prometheus)
        if args.only in (None, "terminal"):
            term = browser.new_page(viewport={"width": 1120, "height": 900}, device_scale_factor=1.5)
            for src, title, out in (
                ("quickstart-session.txt", "docker compose up --build   -   then talk to it", "quickstart.png"),
                ("test-run.txt", "pytest tests --cov=src   (against a real Redis 7)", "tests.png"),
            ):
                path = BENCH / src
                if path.exists():
                    terminal(term, title, path.read_text(encoding="utf-8"), IMAGES / out)
        browser.close()
    print("screenshots written to", IMAGES)


if __name__ == "__main__":
    main()
