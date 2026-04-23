from __future__ import annotations

import pandas as pd

from app import event_service


def test_persist_signal_rows_dedupes_events(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    df = pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "日期": "2026-04-08",
                "收盘": 1530.25,
                "涨跌幅": 1.82,
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MACD形态": "水下金叉后水上再次金叉",
                "MA5": 1520.1,
                "MA20": 1498.3,
                "均线信号": "MA5上穿MA20",
                "信号": "MACD金叉, 水下金叉后水上再次金叉, MA5上穿MA20",
            }
        ]
    )

    first_saved = event_service.persist_signal_rows(df)
    second_saved = event_service.persist_signal_rows(df)
    history = event_service.list_signal_events(trade_date="2026-04-08")

    assert len(first_saved) == 3
    assert len(second_saved) == 3
    assert len(history) == 3
    assert {(item["indicator"], item["event_type"]) for item in history} == {
        ("MACD", "golden_cross"),
        ("MACD", "secondary_golden_cross_above_zero"),
        ("MA", "ma5_cross_up_ma20"),
    }
    assert all(item["severity"] == "high" for item in history)
    assert history[0]["payload"]["close"] == 1530.25
