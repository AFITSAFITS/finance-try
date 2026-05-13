from __future__ import annotations

from app import scan_run_service


def test_persist_and_list_scan_runs(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    saved = scan_run_service.persist_scan_run(
        channel="stdout",
        watchlist={"name": "默认股票池"},
        watchlist_source="existing",
        requested_count=1,
        event_count=1,
        notification_count=1,
        error_count=0,
        elapsed_seconds=1.23,
        min_score=50,
        signal_summary={"signals": 1, "observation_counts": {"谨慎观察": 1}},
    )
    items = scan_run_service.list_scan_runs()

    assert saved["id"] == 1
    assert saved["status"] == "正常"
    assert saved["note"] == "扫描完成并生成信号"
    assert saved["summary"]["signals"] == 1
    assert len(items) == 1
    assert items[0]["event_count"] == 1
    assert items[0]["summary"]["observation_counts"] == {"谨慎观察": 1}


def test_build_scan_run_health_statuses() -> None:
    assert scan_run_service.build_scan_run_health(
        requested_count=2,
        event_count=0,
        error_count=2,
        signal_summary={"signals": 0, "stale_signals": 0},
    )["status"] == "失败"
    assert scan_run_service.build_scan_run_health(
        requested_count=2,
        event_count=1,
        error_count=1,
        signal_summary={"signals": 1, "stale_signals": 0},
    )["status"] == "部分失败"
    assert scan_run_service.build_scan_run_health(
        requested_count=2,
        event_count=1,
        error_count=0,
        signal_summary={"signals": 1, "stale_signals": 1},
    )["status"] == "数据滞后"
    assert scan_run_service.build_scan_run_health(
        requested_count=2,
        event_count=0,
        error_count=0,
        signal_summary={"signals": 0, "stale_signals": 0},
    )["status"] == "无信号"


def test_list_scan_runs_backfills_blank_legacy_status(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    scan_run_service.persist_scan_run(
        channel="stdout",
        watchlist={"name": "默认股票池"},
        watchlist_source="existing",
        requested_count=1,
        event_count=1,
        notification_count=1,
        error_count=0,
        elapsed_seconds=1.23,
        min_score=50,
        signal_summary={"signals": 1, "stale_signals": 0},
    )
    from app import db

    with db.get_connection() as conn:
        conn.execute("UPDATE scan_runs SET status = '', note = ''")

    items = scan_run_service.list_scan_runs()

    assert items[0]["status"] == "正常"
    assert items[0]["note"] == "扫描完成并生成信号"
