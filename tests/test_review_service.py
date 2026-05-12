from __future__ import annotations

import pandas as pd

from app import event_service
from app import review_service


def make_history(code: str, closes: list[float]) -> pd.DataFrame:
    rows = []
    for index, close in enumerate(closes):
        rows.append(
            {
                "日期": pd.Timestamp("2026-04-08") + pd.Timedelta(days=index),
                "股票代码": code,
                "开盘": close,
                "收盘": close,
                "最高": close,
                "最低": close,
                "成交量": 1000 + index,
                "成交额": 10000 + index,
                "涨跌幅": 0.0,
                "换手率": 1.0,
            }
        )
    return pd.DataFrame(rows)


def seed_events() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "日期": "2026-04-08",
                "收盘": 10.0,
                "涨跌幅": 1.82,
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MA5": 9.8,
                "MA20": 9.5,
                "均线信号": "MA5上穿MA20",
                "信号": "MACD金叉, MA5上穿MA20",
                "信号评分": 75,
                "信号方向": "偏多",
                "信号级别": "观察",
                "评分原因": "金叉叠加均线转强",
                "风险提示": "接近60日高位",
                "60日位置": 0.92,
                "量能比": 1.35,
            }
        ]
    )


def test_backfill_review_snapshots_and_stats(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    event_service.persist_signal_rows(seed_events())

    def fake_fetcher(code: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        assert code == "600519"
        assert start_date == "2026-04-08"
        assert adjust == "qfq"
        return make_history(code, [10.0, 11.0, 9.0, 12.0, 13.0, 8.0])

    first = review_service.backfill_review_snapshots(fetcher=fake_fetcher)
    second = review_service.backfill_review_snapshots(fetcher=fake_fetcher)
    stats = review_service.summarize_review_stats(horizon="T+3")

    assert first["count"] == 6
    assert second["count"] == 6
    assert first["errors"] == []
    assert len(first["items"]) == 6
    assert {item["horizon"] for item in first["items"]} == {"T+1", "T+3", "T+5"}

    t3_rows = [item for item in first["items"] if item["horizon"] == "T+3"]
    assert len(t3_rows) == 2
    assert t3_rows[0]["pct_return"] == 20.0
    assert t3_rows[0]["max_drawdown"] == -10.0
    assert t3_rows[0]["signal_score"] == 75.0
    assert t3_rows[0]["signal_direction"] == "偏多"
    assert t3_rows[0]["risk_note"] == "接近60日高位"
    assert t3_rows[0]["position_60d"] == 0.92
    assert t3_rows[0]["volume_ratio"] == 1.35

    assert len(stats) == 2
    macd_stats = next(item for item in stats if item["summary"] == "MACD金叉")
    assert macd_stats["score_bucket"] == "60-80"
    assert macd_stats["signal_direction"] == "偏多"
    assert macd_stats["risk_bucket"] == "有风险提示"
    assert macd_stats["sample_count"] == 1
    assert macd_stats["avg_return"] == 20.0
    assert macd_stats["win_rate"] == 1.0
    assert macd_stats["avg_position_60d"] == 0.92
    assert macd_stats["avg_volume_ratio"] == 1.35
    assert macd_stats["strategy_verdict"] == "样本不足"
    assert "继续积累" in macd_stats["strategy_note"]
