from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app import scan_workflow


def parse_schedule_time(value: str) -> tuple[int, int]:
    raw = str(value).strip()
    hour_str, minute_str = raw.split(":", maxsplit=1)
    hour = int(hour_str)
    minute = int(minute_str)
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        raise ValueError("schedule_time 格式应为 HH:MM")
    return hour, minute


def should_run_daily_job(
    now: datetime,
    schedule_time: str,
    last_run_date: str | None,
    weekdays_only: bool = True,
) -> bool:
    if weekdays_only and now.weekday() >= 5:
        return False

    hour, minute = parse_schedule_time(schedule_time)
    if (now.hour, now.minute) < (hour, minute):
        return False

    current_date = now.strftime("%Y-%m-%d")
    if last_run_date == current_date:
        return False
    return True


def run_single_scan_job(
    channel: str = "stdout",
    lookback_days: int = 180,
    adjust: str = "qfq",
    max_workers: int = 8,
    min_score: float = 60.0,
) -> dict[str, Any]:
    return scan_workflow.run_default_watchlist_scan(
        lookback_days=int(lookback_days),
        adjust=adjust,
        channel=channel,
        max_workers=int(max_workers),
        min_score=float(min_score),
    )


def run_worker_loop(
    channel: str = "stdout",
    lookback_days: int = 180,
    adjust: str = "qfq",
    max_workers: int = 8,
    min_score: float = 60.0,
    schedule_time: str = "15:05",
    timezone_name: str = "Asia/Shanghai",
    poll_seconds: int = 30,
    weekdays_only: bool = True,
) -> None:
    zone = ZoneInfo(timezone_name)
    last_run_date: str | None = None
    while True:
        now = datetime.now(zone)
        if should_run_daily_job(now, schedule_time, last_run_date, weekdays_only=weekdays_only):
            result = run_single_scan_job(
                channel=channel,
                lookback_days=int(lookback_days),
                adjust=adjust,
                max_workers=int(max_workers),
                min_score=float(min_score),
            )
            last_run_date = now.strftime("%Y-%m-%d")
            summary = result.get("signal_summary", {})
            print(
                f"[worker] trade_date={last_run_date} "
                f"watchlist={result['watchlist'].get('name', '')} "
                f"count={result.get('requested_count', 0)} "
                f"events={len(result.get('persisted_events', []))} "
                f"notification_events={len(result.get('notification_events', []))} "
                f"scan_run_id={(result.get('scan_run') or {}).get('id', '')} "
                f"status={(result.get('scan_run') or {}).get('status', '')} "
                f"min_score={result.get('min_score', '')} "
                f"stale_signals={summary.get('stale_signals', 0) if isinstance(summary, dict) else 0} "
                f"errors={len(result.get('errors', []))}"
            )
        time.sleep(max(1, int(poll_seconds)))
