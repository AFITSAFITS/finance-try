from __future__ import annotations

import pandas as pd

from app import bar_service


def make_history(code: str, rows: int = 20) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "日期": pd.Timestamp("2026-04-20") + pd.Timedelta(days=index),
                "股票代码": code,
                "开盘": 10 + index,
                "收盘": 10 + index,
                "最高": 10 + index,
                "最低": 10 + index,
                "成交量": 1000 + index,
                "成交额": 10000 + index,
                "涨跌幅": 1.0,
                "换手率": 2.0,
            }
            for index in range(rows)
        ]
    )


def test_fetch_daily_history_cached_reuses_fresh_cache(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    calls = {"count": 0}

    def fake_fetcher(code: str, start_date: str, end_date: str, adjust: str) -> pd.DataFrame:
        calls["count"] += 1
        return make_history(code)

    monkeypatch.setattr(bar_service, "fetch_daily_history_range_akshare", fake_fetcher)

    first = bar_service.fetch_daily_history_cached("600001", lookback_days=30)
    second = bar_service.fetch_daily_history_cached("600001", lookback_days=30)

    assert calls["count"] == 1
    assert len(first) == 20
    assert len(second) == 20
    assert list(second["股票代码"].unique()) == ["600001"]
