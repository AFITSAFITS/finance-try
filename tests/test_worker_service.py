from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app import worker_service


def test_should_run_daily_job_once_per_day() -> None:
    now = datetime(2026, 4, 23, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert worker_service.should_run_daily_job(now, "15:05", None, weekdays_only=True) is True
    assert worker_service.should_run_daily_job(now, "15:05", "2026-04-23", weekdays_only=True) is False


def test_should_not_run_on_weekend_when_weekdays_only() -> None:
    weekend = datetime(2026, 4, 25, 15, 10, tzinfo=ZoneInfo("Asia/Shanghai"))

    assert worker_service.should_run_daily_job(weekend, "15:05", None, weekdays_only=True) is False


def test_run_single_scan_job_invokes_scan_workflow(monkeypatch) -> None:
    called: dict[str, object] = {}

    def fake_run_default_watchlist_scan(**kwargs):
        called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 2},
            "requested_count": 2,
            "elapsed_seconds": 3.5,
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
        }

    monkeypatch.setattr(worker_service.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)

    result = worker_service.run_single_scan_job(
        channel="feishu_webhook",
        lookback_days=180,
        adjust="qfq",
        max_workers=8,
        min_score=70,
    )

    assert called["channel"] == "feishu_webhook"
    assert called["min_score"] == 70.0
    assert result["watchlist"]["name"] == "默认股票池"
