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


def test_run_single_scan_job_can_review_after_scan(monkeypatch) -> None:
    scan_called: dict[str, object] = {}
    review_called: dict[str, object] = {}

    def fake_run_default_watchlist_scan(**kwargs):
        scan_called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "requested_count": 1,
            "elapsed_seconds": 1.2,
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
        }

    def fake_backfill_review_snapshots(**kwargs):
        review_called["backfill"] = kwargs
        return {"count": 3, "errors": []}

    def fake_summarize_review_stats(**kwargs):
        review_called["stats"] = kwargs
        return [{"sample_count": 6}]

    monkeypatch.setattr(worker_service.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)
    monkeypatch.setattr(worker_service.review_service, "backfill_review_snapshots", fake_backfill_review_snapshots)
    monkeypatch.setattr(worker_service.review_service, "summarize_review_stats", fake_summarize_review_stats)

    result = worker_service.run_single_scan_job(
        review_after_scan=True,
        review_trade_date="2026-05-01",
        review_horizons=[1, 3],
        review_summary_horizon="T+3",
    )

    assert scan_called["min_score"] == 60.0
    assert review_called["backfill"]["trade_date"] == "2026-05-01"
    assert review_called["backfill"]["horizons"] == [1, 3]
    assert review_called["backfill"]["due_only"] is True
    assert review_called["stats"]["horizon"] == "T+3"
    assert result["review_result"]["count"] == 3
    assert len(result["review_stats"]) == 1
    assert result["scan_run"]["review_after_scan"] is True
    assert result["scan_run"]["review_snapshot_count"] == 3
    assert result["scan_run"]["review_stats_count"] == 1
    assert result["scan_run"]["review_error"] == ""
