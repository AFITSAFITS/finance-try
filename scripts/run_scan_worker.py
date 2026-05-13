#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import worker_service


def env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def parse_horizon_args(raw: str) -> list[int]:
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return [int(item) for item in values]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the scheduled scan worker.")
    parser.add_argument("--channel", type=str, default=os.getenv("AI_FINANCE_NOTIFICATION_CHANNEL", "stdout"))
    parser.add_argument("--lookback-days", type=int, default=int(os.getenv("AI_FINANCE_LOOKBACK_DAYS", "180")))
    parser.add_argument("--adjust", type=str, default=os.getenv("AI_FINANCE_ADJUST", "qfq"))
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("AI_FINANCE_MAX_WORKERS", "8")))
    parser.add_argument("--min-score", type=float, default=float(os.getenv("AI_FINANCE_DAILY_MIN_SCORE", "60")))
    parser.add_argument(
        "--review-after-scan",
        action="store_true",
        default=env_flag("AI_FINANCE_REVIEW_AFTER_SCAN", False),
        help="Backfill signal review snapshots after each worker scan",
    )
    parser.add_argument(
        "--review-trade-date",
        type=str,
        default=os.getenv("AI_FINANCE_REVIEW_TRADE_DATE", ""),
        help="Optional trade date filter for worker review backfill",
    )
    parser.add_argument(
        "--review-horizons",
        type=str,
        default=os.getenv("AI_FINANCE_REVIEW_HORIZONS", "1,3,5"),
        help="Comma-separated review horizons for --review-after-scan",
    )
    parser.add_argument(
        "--review-summary-horizon",
        type=str,
        default=os.getenv("AI_FINANCE_REVIEW_SUMMARY_HORIZON", "T+3"),
        help="Summary horizon label after review backfill",
    )
    parser.add_argument(
        "--review-due-only",
        action=argparse.BooleanOptionalAction,
        default=env_flag("AI_FINANCE_REVIEW_DUE_ONLY", True),
        help="Only backfill matured review snapshots after worker scans",
    )
    parser.add_argument(
        "--strategy-guard-horizon",
        type=str,
        default=os.getenv("AI_FINANCE_STRATEGY_GUARD_HORIZON", "T+1"),
        help="Review horizon used to attach strategy conclusions to worker notifications",
    )
    parser.add_argument(
        "--schedule-time",
        type=str,
        default=os.getenv("AI_FINANCE_WORKER_SCHEDULE_TIME", "15:05"),
        help="Daily run time in HH:MM",
    )
    parser.add_argument(
        "--timezone",
        type=str,
        default=os.getenv("AI_FINANCE_TIMEZONE", "Asia/Shanghai"),
    )
    parser.add_argument(
        "--poll-seconds",
        type=int,
        default=int(os.getenv("AI_FINANCE_WORKER_POLL_SECONDS", "30")),
    )
    parser.add_argument("--run-once", action="store_true", help="Run once and exit")
    parser.add_argument("--all-days", action="store_true", help="Allow weekend execution")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.run_once:
            result = worker_service.run_single_scan_job(
                channel=args.channel,
                lookback_days=int(args.lookback_days),
                adjust=args.adjust,
                max_workers=int(args.max_workers),
                min_score=float(args.min_score),
                review_after_scan=bool(args.review_after_scan),
                review_trade_date=args.review_trade_date,
                review_horizons=parse_horizon_args(args.review_horizons),
                review_summary_horizon=args.review_summary_horizon,
                review_due_only=bool(args.review_due_only),
                strategy_guard_horizon=args.strategy_guard_horizon,
            )
            print(
                f"watchlist={result['watchlist'].get('name', '')} "
                f"count={result.get('requested_count', 0)} "
                f"min_score={result.get('min_score', '')} "
                f"events={len(result.get('persisted_events', []))} "
                f"notification_events={len(result.get('notification_events', []))} "
                f"strategy_matched={(result.get('strategy_guard') or {}).get('matched_count', 0)} "
                f"errors={len(result.get('errors', []))}"
            )
            if args.review_after_scan:
                review_result = result.get("review_result") or {}
                review_stats = result.get("review_stats") or []
                print(
                    f"review_snapshots={review_result.get('count', 0)} "
                    f"review_due_only={bool(args.review_due_only)} "
                    f"review_stats={len(review_stats)} "
                    f"review_error={result.get('review_error', '')}"
                )
            return 0

        worker_service.run_worker_loop(
            channel=args.channel,
            lookback_days=int(args.lookback_days),
            adjust=args.adjust,
            max_workers=int(args.max_workers),
            min_score=float(args.min_score),
            review_after_scan=bool(args.review_after_scan),
            review_trade_date=args.review_trade_date,
            review_horizons=parse_horizon_args(args.review_horizons),
            review_summary_horizon=args.review_summary_horizon,
            review_due_only=bool(args.review_due_only),
            strategy_guard_horizon=args.strategy_guard_horizon,
            schedule_time=args.schedule_time,
            timezone_name=args.timezone,
            poll_seconds=int(args.poll_seconds),
            weekdays_only=not bool(args.all_days),
        )
        return 0
    except ValueError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
