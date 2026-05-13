from __future__ import annotations

import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from app import review_service
from app import scan_run_service
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
    review_after_scan: bool = False,
    review_trade_date: str = "",
    review_horizons: list[int] | tuple[int, ...] | None = None,
    review_summary_horizon: str = "T+3",
) -> dict[str, Any]:
    result = scan_workflow.run_default_watchlist_scan(
        lookback_days=int(lookback_days),
        adjust=adjust,
        channel=channel,
        max_workers=int(max_workers),
        min_score=float(min_score),
    )
    if not review_after_scan:
        return result

    try:
        selected_horizons = review_service.parse_horizons(review_horizons)
        review_result = review_service.backfill_review_snapshots(
            trade_date=review_trade_date.strip() or None,
            horizons=selected_horizons,
            adjust=adjust,
        )
        review_stats = review_service.summarize_review_stats(
            horizon=review_summary_horizon.strip() or "T+3",
            trade_date=review_trade_date.strip() or None,
        )
        result["review_result"] = review_result
        result["review_stats"] = review_stats
    except Exception as exc:  # noqa: BLE001
        result["review_error"] = str(exc)
    scan_run = result.get("scan_run") if isinstance(result.get("scan_run"), dict) else {}
    result["scan_run"] = scan_run
    review_result = result.get("review_result") if isinstance(result.get("review_result"), dict) else {}
    review_stats = result.get("review_stats") if isinstance(result.get("review_stats"), list) else []
    review_error = str(result.get("review_error", "") or "")
    updated_scan_run = scan_run_service.update_scan_run_review(
        scan_run.get("id"),
        review_after_scan=True,
        review_snapshot_count=int(review_result.get("count") or 0),
        review_stats_count=len(review_stats),
        review_error=review_error,
    )
    if updated_scan_run is not None:
        result["scan_run"] = updated_scan_run
    elif isinstance(scan_run, dict):
        scan_run.update(
            {
                "review_after_scan": True,
                "review_snapshot_count": int(review_result.get("count") or 0),
                "review_stats_count": len(review_stats),
                "review_error": review_error,
            }
        )
    return result


def run_worker_loop(
    channel: str = "stdout",
    lookback_days: int = 180,
    adjust: str = "qfq",
    max_workers: int = 8,
    min_score: float = 60.0,
    review_after_scan: bool = False,
    review_trade_date: str = "",
    review_horizons: list[int] | tuple[int, ...] | None = None,
    review_summary_horizon: str = "T+3",
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
                review_after_scan=review_after_scan,
                review_trade_date=review_trade_date,
                review_horizons=review_horizons,
                review_summary_horizon=review_summary_horizon,
            )
            last_run_date = now.strftime("%Y-%m-%d")
            summary = result.get("signal_summary", {})
            review_result = result.get("review_result") or {}
            review_stats = result.get("review_stats") or []
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
                f"errors={len(result.get('errors', []))} "
                f"review_snapshots={review_result.get('count', '') if review_after_scan else ''} "
                f"review_stats={len(review_stats) if review_after_scan else ''} "
                f"review_error={result.get('review_error', '') if review_after_scan else ''}"
            )
        time.sleep(max(1, int(poll_seconds)))
