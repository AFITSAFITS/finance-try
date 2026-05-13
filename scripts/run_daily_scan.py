#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import notification_service
from app.api import select_newly_delivered_events
from app import review_service
from app import scan_run_service
from app import scan_workflow


def parse_horizon_args(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in values]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the default watchlist daily scan workflow.")
    parser.add_argument("--lookback-days", type=int, default=180, help="Calendar days to fetch")
    parser.add_argument("--adjust", type=str, default="qfq", help="qfq / hfq / empty string")
    parser.add_argument(
        "--channel",
        type=str,
        default="stdout",
        help="Notification channel: stdout / feishu_webhook",
    )
    parser.add_argument("--max-workers", type=int, default=8, help="Parallel fetch workers")
    parser.add_argument(
        "--min-score",
        type=float,
        default=float(os.getenv("AI_FINANCE_DAILY_MIN_SCORE", "60")),
        help="Minimum signal score to persist and notify; use 0 to keep all signals",
    )
    parser.add_argument(
        "--review-after-scan",
        action="store_true",
        help="Backfill matured signal review snapshots after the daily scan",
    )
    parser.add_argument(
        "--review-trade-date",
        type=str,
        default="",
        help="Optional trade date filter for review backfill, e.g. 2026-05-12",
    )
    parser.add_argument(
        "--review-horizons",
        type=str,
        default="1,3,5",
        help="Comma-separated review horizons for --review-after-scan",
    )
    parser.add_argument(
        "--review-summary-horizon",
        type=str,
        default="T+3",
        help="Summary horizon label after review backfill, e.g. T+3",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = scan_workflow.run_default_watchlist_scan(
            lookback_days=int(args.lookback_days),
            adjust=args.adjust,
            channel=args.channel,
            max_workers=int(args.max_workers),
            min_score=float(args.min_score),
        )
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    watchlist = result["watchlist"]
    print(
        f"watchlist={watchlist.get('name', '')} "
        f"count={watchlist.get('count', 0)} "
        f"source={result.get('watchlist_source', 'existing')} "
        f"min_score={result.get('min_score', '')} "
        f"events={len(result['persisted_events'])} "
        f"notification_events={len(result.get('notification_events', []))} "
        f"deliveries={len(result['delivery_results'])}"
    )
    if result.get("scan_run"):
        print(
            f"scan_run_id={result['scan_run'].get('id')} "
            f"run_at={result['scan_run'].get('run_at')} "
            f"status={result['scan_run'].get('status', '')} "
            f"note={result['scan_run'].get('note', '')}"
        )
    if result.get("watchlist_message"):
        print(result["watchlist_message"])
    if result.get("watchlist_warning"):
        print(f"WARNING [watchlist]: {result['watchlist_warning']}", file=sys.stderr)
    summary = result.get("signal_summary", {})
    if summary:
        print(
            "signal_summary "
            f"signals={summary.get('signals', 0)} "
            f"errors={summary.get('error_count', 0)} "
            f"max_score={summary.get('max_score', '-')} "
            f"stale_signals={summary.get('stale_signals', 0)} "
            f"cache_fallback_signals={summary.get('cache_fallback_signals', 0)} "
            f"observations={summary.get('observation_counts', {})} "
            f"freshness={summary.get('freshness_counts', {})} "
            f"data_sources={summary.get('data_source_counts', {})}"
        )

    new_events = select_newly_delivered_events(
        result.get("notification_events", result["persisted_events"]),
        result["delivery_results"],
    )
    for message in notification_service.build_stdout_messages(new_events):
        print(message)

    for error in result["errors"]:
        print(f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)

    if args.review_after_scan:
        try:
            review_result = review_service.backfill_review_snapshots(
                trade_date=args.review_trade_date.strip() or None,
                horizons=parse_horizon_args(args.review_horizons),
                adjust=args.adjust,
            )
            review_stats = review_service.summarize_review_stats(
                horizon=args.review_summary_horizon.strip() or "T+3",
                trade_date=args.review_trade_date.strip() or None,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"ERROR [review]: {exc}", file=sys.stderr)
            scan_run = result.get("scan_run") if isinstance(result.get("scan_run"), dict) else {}
            updated_scan_run = scan_run_service.update_scan_run_review(
                scan_run.get("id"),
                review_after_scan=True,
                review_error=str(exc),
            )
            if updated_scan_run is not None:
                result["scan_run"] = updated_scan_run
            return 1

        scan_run = result.get("scan_run") if isinstance(result.get("scan_run"), dict) else {}
        updated_scan_run = scan_run_service.update_scan_run_review(
            scan_run.get("id"),
            review_after_scan=True,
            review_snapshot_count=int(review_result.get("count") or 0),
            review_stats_count=len(review_stats),
            review_error="",
        )
        if updated_scan_run is not None:
            result["scan_run"] = updated_scan_run
        print(f"review_snapshots={review_result['count']}")
        print(
            "scan_run_review "
            f"enabled={result['scan_run'].get('review_after_scan', True)} "
            f"snapshots={result['scan_run'].get('review_snapshot_count', review_result['count'])} "
            f"stats={result['scan_run'].get('review_stats_count', len(review_stats))} "
            f"error={result['scan_run'].get('review_error', '')}"
        )
        for error in review_result["errors"]:
            print(f"WARNING [review {error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)
        if review_stats:
            for item in review_stats:
                print(
                    "review_summary "
                    f"horizon={item['horizon']} "
                    f"samples={item['sample_count']} "
                    f"avg_return={item['avg_return']} "
                    f"win_rate={item['win_rate']} "
                    f"verdict={item['strategy_verdict']} "
                    f"confidence={item['strategy_confidence']} "
                    f"actionable={item['strategy_actionable']}"
                )
        else:
            print("没有可用的复盘统计结果。")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
