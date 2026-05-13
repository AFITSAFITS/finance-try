from __future__ import annotations

import pandas as pd

from app import scan_workflow


def test_select_representative_notification_events_keeps_one_per_stock_date() -> None:
    events = [
        {
            "id": 1,
            "trade_date": "2026-05-12",
            "code": "600001",
            "severity": "normal",
            "event_type": "ma5_cross_up_ma20",
            "payload": {"signal_score": 70},
        },
        {
            "id": 2,
            "trade_date": "2026-05-12",
            "code": "600001",
            "severity": "high",
            "event_type": "secondary_golden_cross_above_zero",
            "payload": {"signal_score": 85},
        },
        {
            "id": 3,
            "trade_date": "2026-05-12",
            "code": "600002",
            "severity": "normal",
            "event_type": "golden_cross",
            "payload": {"signal_score": 75},
        },
    ]

    selected = scan_workflow.select_representative_notification_events(events)

    assert [item["id"] for item in selected] == [2, 3]


def test_select_representative_notification_events_prefers_better_strategy_when_strength_ties() -> None:
    events = [
        {
            "id": 1,
            "trade_date": "2026-05-12",
            "code": "600001",
            "severity": "normal",
            "event_type": "golden_cross",
            "payload": {"signal_score": 80, "strategy_verdict": "降权"},
        },
        {
            "id": 2,
            "trade_date": "2026-05-12",
            "code": "600001",
            "severity": "normal",
            "event_type": "golden_cross",
            "payload": {"signal_score": 80, "strategy_verdict": "保留"},
        },
    ]

    selected = scan_workflow.select_representative_notification_events(events)

    assert [item["id"] for item in selected] == [2]


def test_run_default_watchlist_scan_bootstraps_empty_watchlist(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
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
        assert kwargs["min_score"] == 60.0
        return pd.DataFrame(
            [
                {
                    "股票代码": "600519",
                    "观察结论": "重点观察",
                    "数据时效": "最近交易日",
                    "信号方向": "偏多",
                    "信号评分": 85,
                }
            ]
        ), []

    monkeypatch.setattr(scan_workflow.watchlist_service, "ensure_default_watchlist", fake_ensure_default_watchlist)
    monkeypatch.setattr(scan_workflow.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)
    saved_events = [
        {
            "id": 1,
            "trade_date": "2026-05-12",
            "code": "600519",
            "severity": "normal",
            "event_type": "golden_cross",
            "payload": {"signal_score": 70},
        },
        {
            "id": 2,
            "trade_date": "2026-05-12",
            "code": "600519",
            "severity": "high",
            "event_type": "secondary_golden_cross_above_zero",
            "payload": {"signal_score": 85},
        },
    ]
    delivered: dict[str, object] = {}

    def fake_deliver_signal_events(events, channel):
        delivered["ids"] = [item["id"] for item in events]
        delivered["strategy_verdicts"] = [item.get("payload", {}).get("strategy_verdict") for item in events]
        return [{"signal_event_id": item["id"], "created": True} for item in events]

    monkeypatch.setattr(scan_workflow.event_service, "persist_signal_rows", lambda df: saved_events)
    monkeypatch.setattr(
        scan_workflow.strategy_guard_service,
        "annotate_signal_events_with_strategy_decisions",
        lambda events, horizon: ([{**item, "payload": {**item.get("payload", {}), "strategy_verdict": "保留"}} for item in events], {"enabled": True, "horizon": horizon, "matched_count": 2, "total_count": 2}),
    )
    monkeypatch.setattr(scan_workflow.notification_service, "deliver_signal_events", fake_deliver_signal_events)

    result = scan_workflow.run_default_watchlist_scan()

    assert result["requested_count"] == 1
    assert result["min_score"] == 60.0
    assert [item["id"] for item in result["persisted_events"]] == [1, 2]
    assert result["signal_summary"]["observation_counts"] == {"重点观察": 1}
    assert result["signal_summary"]["freshness_counts"] == {"最近交易日": 1}
    assert result["scan_run"]["event_count"] == 2
    assert result["scan_run"]["status"] == "正常"
    assert result["scan_run"]["summary"]["signals"] == 1
    assert [item["id"] for item in result["notification_events"]] == [2]
    assert delivered["ids"] == [2]
    assert delivered["strategy_verdicts"] == ["保留"]
    assert result["strategy_guard"]["matched_count"] == 2
    assert result["watchlist_source"] == "seed"
    assert result["watchlist_message"] == "已使用内置种子股票池"
