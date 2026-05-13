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
from app import strategy_summary_service


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
    parser.add_argument(
        "--stats-only",
        action="store_true",
        help="Only summarize existing review snapshots; do not backfill from external market data",
    )
    parser.add_argument(
        "--strategy-summary",
        action="store_true",
        help="Print a unified strategy decision summary across signal and limit-up reviews",
    )
    parser.add_argument("--strategy-limit", type=int, default=20, help="Maximum unified strategy rows to print")
    parser.add_argument("--strategy-min-samples", type=int, default=1, help="Minimum samples for unified strategy rows")
    parser.add_argument("--strategy-data-source", type=str, default="", help="Filter unified strategy rows by data source")
    parser.add_argument(
        "--strategy-actionable-only",
        action="store_true",
        help="Only print unified strategy rows that are actionable",
    )
    return parser.parse_args()


def parse_horizon_args(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in values]


def main() -> int:
    args = parse_args()
    try:
        selected_horizons = parse_horizon_args(args.horizons)
        signal_result = {"count": "skipped" if args.stats_only else 0, "errors": []}
        signal_stats = []
        limit_result = {"count": "skipped" if args.stats_only else 0, "errors": []}
        limit_stats = []
        strategy_summary: dict[str, object] | None = None

        if args.target in {"signals", "both"}:
            if not args.stats_only:
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
            if not args.stats_only:
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

        if args.strategy_summary:
            strategy_summary = strategy_summary_service.summarize_strategy_decisions(
                horizon=args.summary_horizon.strip() or "T+3",
                trade_date=args.trade_date.strip() or None,
                code=args.code.strip() or None,
                limit=int(args.strategy_limit),
                min_samples=int(args.strategy_min_samples),
                actionable_only=bool(args.strategy_actionable_only),
                data_source=args.strategy_data_source.strip() or None,
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
                f"score_bucket={item['score_bucket']} | direction={item['signal_direction']} | "
                f"conclusion={item['observation_conclusion']} | data_freshness={item['data_freshness']} | "
                f"data_source={item['data_source']} | "
                f"risk={item['risk_bucket']} | "
                f"{item['summary']} | horizon={item['horizon']} | "
                f"samples={item['sample_count']} | avg_return={item['avg_return']} | win_rate={item['win_rate']} | "
                f"avg_max_drawdown={item['avg_max_drawdown']} | avg_position_60d={item['avg_position_60d']} | "
                f"avg_volume_ratio={item['avg_volume_ratio']} | risk_plan={item['risk_plan_bucket']} | "
                f"avg_stop_distance_pct={item['avg_stop_distance_pct']} | avg_risk_reward_ratio={item['avg_risk_reward_ratio']} | "
                f"stop_hit_rate={item['stop_hit_rate']} | target_hit_rate={item['target_hit_rate']} | "
                f"stop_first_rate={item['stop_first_rate']} | target_first_rate={item['target_first_rate']} | "
                f"same_day_hit_rate={item['same_day_hit_rate']} | "
                f"verdict={item['strategy_verdict']} | "
                f"confidence={item['strategy_confidence']} | actionable={item['strategy_actionable']} | "
                f"samples_to_actionable={item.get('samples_to_actionable', '')} | "
                f"note={item['strategy_note']}"
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
                f"avg_sector_limit_up_count={item['avg_sector_limit_up_count']} | "
                f"verdict={item['strategy_verdict']} | confidence={item['strategy_confidence']} | "
                f"actionable={item['strategy_actionable']} | samples_to_actionable={item.get('samples_to_actionable', '')} | "
                f"note={item['strategy_note']}"
            )
    elif args.target in {"limit-up", "both"}:
        print("没有可用的涨停候选复盘统计结果。")

    if strategy_summary is not None:
        print("\nstrategy_summary")
        print(
            f"horizon={strategy_summary['horizon']} | total={strategy_summary['total_count']} | "
            f"filtered={strategy_summary['filtered_count']} | actionable={strategy_summary['actionable_count']} | "
            f"min_samples={strategy_summary['min_samples']} | actionable_only={strategy_summary['actionable_only']} | "
            f"data_source={strategy_summary['data_source']} | "
            f"verdicts={strategy_summary['verdict_counts']} | confidence={strategy_summary['confidence_counts']}"
        )
        items = strategy_summary.get("items", [])
        if not items:
            print("没有可用的统一策略结论。")
        for item in items:
            print(
                f"type={item['strategy_type']} | name={item['strategy_name']} | horizon={item['horizon']} | "
                f"data_source={item['data_source']} | samples={item['sample_count']} | "
                f"avg_return={item['avg_return']} | win_rate={item['win_rate']} | "
                f"avg_max_drawdown={item['avg_max_drawdown']} | verdict={item['strategy_verdict']} | "
                f"confidence={item['strategy_confidence']} | actionable={item['strategy_actionable']} | "
                f"samples_to_actionable={item.get('samples_to_actionable', '')} | "
                f"note={item['strategy_note']}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
