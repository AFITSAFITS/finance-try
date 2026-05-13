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
    assert saved["summary"]["signals"] == 1
    assert len(items) == 1
    assert items[0]["event_count"] == 1
    assert items[0]["summary"]["observation_counts"] == {"谨慎观察": 1}
