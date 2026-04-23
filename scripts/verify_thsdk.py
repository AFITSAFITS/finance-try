#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from thsdk import THS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify thsdk connectivity with a sample kline request.")
    parser.add_argument("--symbol", type=str, default="USZA300033", help="THS symbol, e.g. USZA300033")
    parser.add_argument("--count", type=int, default=3, help="Number of bars to request")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    with THS() as ths:
        response = ths.klines(args.symbol, count=args.count)

    payload: dict[str, Any] = {
        "ok": response.success,
        "project": "thsdk",
        "symbol": args.symbol,
        "count": args.count,
        "error": response.error,
        "rows": len(response.data) if isinstance(response.data, list) else 0,
        "columns": list(response.data[0].keys()) if isinstance(response.data, list) and response.data else [],
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2, default=str))
    return 0 if response.success else 2


if __name__ == "__main__":
    raise SystemExit(main())
