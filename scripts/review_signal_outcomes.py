#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import review_service


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill and summarize signal review snapshots.")
    parser.add_argument("--trade-date", type=str, default="", help="Filter signal events by trade date")
    parser.add_argument("--code", type=str, default="", help="Filter signal events by stock code")
    parser.add_argument("--horizons", type=str, default="1,3,5", help="Comma-separated trading-day horizons")
    parser.add_argument("--adjust", type=str, default="qfq", help="qfq / hfq / empty string")
    parser.add_argument("--summary-horizon", type=str, default="T+3", help="Summary horizon label, e.g. T+3")
    return parser.parse_args()


def parse_horizon_args(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in values]


def main() -> int:
    args = parse_args()
    try:
        result = review_service.backfill_review_snapshots(
            trade_date=args.trade_date.strip() or None,
            code=args.code.strip() or None,
            horizons=parse_horizon_args(args.horizons),
            adjust=args.adjust,
        )
        stats = review_service.summarize_review_stats(
            horizon=args.summary_horizon.strip() or "T+3",
            trade_date=args.trade_date.strip() or None,
            code=args.code.strip() or None,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"review_snapshots={result['count']}")
    for error in result["errors"]:
        print(f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)

    if stats:
        print("\nsummary_stats")
        for item in stats:
            print(
                f"{item['summary']} | horizon={item['horizon']} | samples={item['sample_count']} | "
                f"avg_return={item['avg_return']} | win_rate={item['win_rate']} | "
                f"avg_max_drawdown={item['avg_max_drawdown']}"
            )
    else:
        print("没有可用的复盘统计结果。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
