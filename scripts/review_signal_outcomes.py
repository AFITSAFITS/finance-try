#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import limit_up_service
from app import review_service


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill and summarize signal review snapshots.")
    parser.add_argument(
        "--target",
        type=str,
        default="signals",
        choices=["signals", "limit-up", "both"],
        help="Review target",
    )
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
        selected_horizons = parse_horizon_args(args.horizons)
        signal_result = {"count": 0, "errors": []}
        signal_stats = []
        limit_result = {"count": 0, "errors": []}
        limit_stats = []

        if args.target in {"signals", "both"}:
            signal_result = review_service.backfill_review_snapshots(
                trade_date=args.trade_date.strip() or None,
                code=args.code.strip() or None,
                horizons=selected_horizons,
                adjust=args.adjust,
            )
            signal_stats = review_service.summarize_review_stats(
                horizon=args.summary_horizon.strip() or "T+3",
                trade_date=args.trade_date.strip() or None,
                code=args.code.strip() or None,
            )

        if args.target in {"limit-up", "both"}:
            limit_result = limit_up_service.backfill_limit_up_review_snapshots(
                trade_date=args.trade_date.strip() or None,
                code=args.code.strip() or None,
                horizons=selected_horizons,
                adjust=args.adjust,
            )
            limit_stats = limit_up_service.summarize_limit_up_review_stats(
                horizon=args.summary_horizon.strip() or "T+3",
                trade_date=args.trade_date.strip() or None,
                code=args.code.strip() or None,
            )
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"review_snapshots={signal_result['count']}")
    for error in signal_result["errors"]:
        print(f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)

    if signal_stats:
        print("\nsummary_stats")
        for item in signal_stats:
            print(
                f"score_bucket={item['score_bucket']} | {item['summary']} | horizon={item['horizon']} | "
                f"samples={item['sample_count']} | avg_return={item['avg_return']} | win_rate={item['win_rate']} | "
                f"avg_max_drawdown={item['avg_max_drawdown']}"
            )
    elif args.target in {"signals", "both"}:
        print("没有可用的复盘统计结果。")

    print(f"limit_up_review_snapshots={limit_result['count']}")
    for error in limit_result["errors"]:
        print(f"WARNING [limit-up {error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)

    if limit_stats:
        print("\nlimit_up_summary_stats")
        for item in limit_stats:
            print(
                f"score_bucket={item['score_bucket']} | horizon={item['horizon']} | "
                f"samples={item['sample_count']} | avg_return={item['avg_return']} | "
                f"win_rate={item['win_rate']} | avg_max_drawdown={item['avg_max_drawdown']} | "
                f"avg_sector_limit_up_count={item['avg_sector_limit_up_count']}"
            )
    elif args.target in {"limit-up", "both"}:
        print("没有可用的涨停候选复盘统计结果。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
