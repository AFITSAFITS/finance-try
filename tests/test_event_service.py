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
                "数据时效": "最近交易日",
                "数据滞后天数": 1,
                "数据来源": "旧缓存兜底",
                "缓存获取时间": "2026-01-01 00:00:00",
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MACD形态": "水下金叉后水上再次金叉",
                "MA5": 1520.1,
                "MA20": 1498.3,
                "均线信号": "MA5上穿MA20",
                "信号": "MACD金叉, 水下金叉后水上再次金叉, MA5上穿MA20",
                "信号评分": 88,
                "信号方向": "偏多",
                "信号级别": "重点",
                "观察结论": "重点观察",
                "观察仓位": "≤30%",
                "执行提示": "优先观察，跌破参考止损退出",
                "评分原因": "多指标共振",
                "相对强度": 92.0,
                "相对强度分层": "强势",
                "主力净流入(亿)": 0.3,
                "资金流确认": "资金支持",
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
    assert history[0]["payload"]["data_freshness"] == "最近交易日"
    assert history[0]["payload"]["data_lag_days"] == 1.0
    assert history[0]["payload"]["data_source"] == "旧缓存兜底"
    assert history[0]["payload"]["cache_fetched_at"] == "2026-01-01 00:00:00"
    assert history[0]["payload"]["signal_score"] == 88.0
    assert history[0]["payload"]["signal_direction"] == "偏多"
    assert history[0]["payload"]["observation_position_size"] == "≤30%"
    assert history[0]["payload"]["execution_hint"] == "优先观察，跌破参考止损退出"
    assert history[0]["payload"]["relative_strength"] == 92.0
    assert history[0]["payload"]["relative_strength_bucket"] == "强势"
    assert history[0]["payload"]["main_net_inflow_yi"] == 0.3
    assert history[0]["payload"]["flow_confirmation"] == "资金支持"

    updated_df = df.copy()
    updated_df.loc[0, "信号评分"] = 72
    updated_df.loc[0, "信号级别"] = "观察"
    event_service.persist_signal_rows(updated_df)
    updated_history = event_service.list_signal_events(trade_date="2026-04-08")
    assert len(updated_history) == 3
    assert updated_history[0]["payload"]["signal_score"] == 72.0
    assert updated_history[0]["payload"]["signal_level"] == "观察"
