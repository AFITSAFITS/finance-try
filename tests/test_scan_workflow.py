from __future__ import annotations

import pandas as pd

from app import scan_workflow


def test_run_default_watchlist_scan_bootstraps_empty_watchlist(monkeypatch) -> None:
    def fake_ensure_default_watchlist():
        return {
            "name": "默认股票池",
            "count": 1,
            "items": [{"code": "600519"}],
            "source": "seed",
            "message": "已使用内置种子股票池",
        }

    def fake_scan_stock_signal_events(**kwargs):
        assert kwargs["codes"] == ["600519"]
        return pd.DataFrame(), []

    monkeypatch.setattr(scan_workflow.watchlist_service, "ensure_default_watchlist", fake_ensure_default_watchlist)
    monkeypatch.setattr(scan_workflow.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)
    monkeypatch.setattr(scan_workflow.event_service, "persist_signal_rows", lambda df: [])
    monkeypatch.setattr(scan_workflow.notification_service, "deliver_signal_events", lambda events, channel: [])

    result = scan_workflow.run_default_watchlist_scan()

    assert result["requested_count"] == 1
    assert result["watchlist_source"] == "seed"
    assert result["watchlist_message"] == "已使用内置种子股票池"
