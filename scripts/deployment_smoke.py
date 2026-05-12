#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _join_url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}/{path.lstrip('/')}"


def _read_response(response: object) -> tuple[int, bytes]:
    status = int(getattr(response, "status", 200))
    data = response.read()
    return status, data


def fetch_json(
    url: str,
    timeout: float = 10.0,
    opener: Callable[..., object] = urlopen,
) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    status, data = _read_response(opener(request, timeout=timeout))
    if status >= 400:
        raise RuntimeError(f"{url} returned HTTP {status}")
    return json.loads(data.decode("utf-8"))


def fetch_text(
    url: str,
    timeout: float = 10.0,
    opener: Callable[..., object] = urlopen,
) -> str:
    request = Request(url, headers={"Accept": "text/html,*/*"})
    status, data = _read_response(opener(request, timeout=timeout))
    if status >= 400:
        raise RuntimeError(f"{url} returned HTTP {status}")
    return data.decode("utf-8", errors="replace")


def run_smoke(
    api_url: str,
    ui_url: str,
    timeout: float = 10.0,
    require_watchlist: bool = False,
    opener: Callable[..., object] = urlopen,
) -> list[str]:
    messages: list[str] = []

    health = fetch_json(_join_url(api_url, "/health"), timeout=timeout, opener=opener)
    if health.get("ok") is not True:
        raise RuntimeError(f"API health check failed: {health}")
    messages.append(f"api ok: {health.get('as_of', '')}")

    watchlist = fetch_json(_join_url(api_url, "/api/watchlists/default"), timeout=timeout, opener=opener)
    count = int(watchlist.get("count", 0))
    if require_watchlist and count <= 0:
        raise RuntimeError("默认股票池为空")
    messages.append(f"watchlist count={count}")

    ui_html = fetch_text(ui_url, timeout=timeout, opener=opener)
    if not ui_html.strip():
        raise RuntimeError("UI returned empty response")
    messages.append("ui ok")

    return messages


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Smoke test deployed ai-finance API and UI.")
    parser.add_argument("--api-url", default="http://127.0.0.1:8000")
    parser.add_argument("--ui-url", default="http://127.0.0.1:8501")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--require-watchlist",
        action="store_true",
        help="Fail if the default watchlist is empty.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        for message in run_smoke(
            api_url=args.api_url,
            ui_url=args.ui_url,
            timeout=float(args.timeout),
            require_watchlist=bool(args.require_watchlist),
        ):
            print(message)
    except (HTTPError, URLError, TimeoutError, RuntimeError, ValueError, json.JSONDecodeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
