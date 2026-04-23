#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from thsdk import THS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch thsdk kline data as JSON.")
    parser.add_argument("--symbol", type=str, required=True, help="THS symbol, e.g. USZA300033")
    parser.add_argument("--count", type=int, default=100, help="Number of bars to request")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with THS() as ths:
        response = ths.klines(args.symbol, count=args.count)

    payload: dict[str, Any] = {
        "ok": response.success,
        "error": response.error,
        "data": response.data if isinstance(response.data, (list, dict)) else [],
        "extra": response.extra,
    }
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return 0 if response.success else 2


if __name__ == "__main__":
    raise SystemExit(main())
