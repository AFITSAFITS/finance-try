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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the scheduled scan worker.")
    parser.add_argument("--channel", type=str, default=os.getenv("AI_FINANCE_NOTIFICATION_CHANNEL", "stdout"))
    parser.add_argument("--lookback-days", type=int, default=int(os.getenv("AI_FINANCE_LOOKBACK_DAYS", "180")))
    parser.add_argument("--adjust", type=str, default=os.getenv("AI_FINANCE_ADJUST", "qfq"))
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("AI_FINANCE_MAX_WORKERS", "8")))
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
            )
            print(
                f"watchlist={result['watchlist'].get('name', '')} "
                f"count={result.get('requested_count', 0)} "
                f"events={len(result.get('persisted_events', []))} "
                f"errors={len(result.get('errors', []))}"
            )
            return 0

        worker_service.run_worker_loop(
            channel=args.channel,
            lookback_days=int(args.lookback_days),
            adjust=args.adjust,
            max_workers=int(args.max_workers),
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
