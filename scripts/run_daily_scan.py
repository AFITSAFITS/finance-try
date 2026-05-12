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
from app import scan_workflow


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
    if result.get("watchlist_message"):
        print(result["watchlist_message"])
    if result.get("watchlist_warning"):
        print(f"WARNING [watchlist]: {result['watchlist_warning']}", file=sys.stderr)

    new_events = select_newly_delivered_events(
        result.get("notification_events", result["persisted_events"]),
        result["delivery_results"],
    )
    for message in notification_service.build_stdout_messages(new_events):
        print(message)

    for error in result["errors"]:
        print(f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
