from __future__ import annotations

import pandas as pd

from app import limit_up_service


def make_history(code: str) -> pd.DataFrame:
    closes = [10.0] * 30 + [10.5, 10.8, 11.0, 11.5, 12.2]
    return pd.DataFrame(
        {
            "日期": pd.date_range("2026-04-01", periods=len(closes), freq="D"),
            "股票代码": [code] * len(closes),
            "收盘": closes,
            "涨跌幅": [0.0] * (len(closes) - 1) + [10.0],
            "数据来源": ["旧缓存兜底"] * len(closes),
            "缓存获取时间": ["2026-01-01 00:00:00"] * len(closes),
        }
    )


def test_scan_limit_up_breakthroughs_scores_and_filters() -> None:
    def fake_pool_fetcher(trade_date: str | None = None) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "代码": "600001",
                    "名称": "样例股份",
                    "最新价": 12.2,
                    "涨跌幅": 10.0,
                    "换手率": 8.5,
                    "所属行业": "软件服务",
                    "连板数": 2,
                    "炸板次数": 0,
                },
                {
                    "代码": "600002",
                    "名称": "同板块样例",
                    "最新价": 8.8,
                    "涨跌幅": 10.0,
                    "换手率": 6.1,
                    "所属行业": "软件服务",
                    "连板数": 1,
                    "炸板次数": 1,
                }
            ]
        )

    def fake_history_fetcher(code: str, lookback_days: int, adjust: str) -> pd.DataFrame:
        assert lookback_days == 120
        assert adjust == "qfq"
        return make_history(code)

    items, errors = limit_up_service.scan_limit_up_breakthroughs(
        trade_date="2026-05-12",
        pool_fetcher=fake_pool_fetcher,
        history_fetcher=fake_history_fetcher,
        min_score=50,
    )

    assert errors == []
    assert len(items) == 2
    first = next(item for item in items if item["code"] == "600001")
    assert first["score"] >= 50
    assert "涨停" in first["reason"]
    assert first["sector_limit_up_count"] == 2
    assert first["sector_heat_rank"] == 1
    assert first["payload"]["data_source"] == "旧缓存兜底"
    assert first["payload"]["cache_fetched_at"] == "2026-01-01 00:00:00"
    assert "共振" in first["reason"]


def test_persist_and_list_limit_up_candidates(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    saved = limit_up_service.persist_limit_up_candidates(
        [
            {
                "trade_date": "2026-05-12",
                "code": "600001",
                "name": "样例股份",
                "sector": "软件服务",
                "close_price": 12.2,
                "pct_change": 10.0,
                "turnover_rate": 8.5,
                "consecutive_boards": 2,
                "sector_limit_up_count": 4,
                "sector_heat_rank": 1,
                "first_limit_time": "09:35:00",
                "last_limit_time": "14:50:00",
                "open_board_count": 0,
                "score": 85,
                "reason": "突破近60日收盘高点",
                "payload": {
                    "breakout_ratio": 1.05,
                    "data_source": "旧缓存兜底",
                    "cache_fetched_at": "2026-01-01 00:00:00",
                },
            }
        ]
    )

    assert saved[0]["code"] == "600001"
    items = limit_up_service.list_limit_up_candidates(trade_date="2026-05-12")
    assert len(items) == 1
    assert items[0]["payload"]["breakout_ratio"] == 1.05
    assert items[0]["sector_limit_up_count"] == 4
    assert items[0]["data_source"] == "旧缓存兜底"
    assert items[0]["cache_fetched_at"] == "2026-01-01 00:00:00"


def test_build_sector_heat_map_counts_limit_up_clusters() -> None:
    heat_map = limit_up_service.build_sector_heat_map(
        pd.DataFrame(
            [
                {"sector": "软件服务"},
                {"sector": "软件服务"},
                {"sector": "半导体"},
                {"sector": ""},
            ]
        )
    )

    assert heat_map["软件服务"] == {"sector_limit_up_count": 2, "sector_heat_rank": 1}
    assert heat_map["半导体"]["sector_limit_up_count"] == 1
    assert heat_map["未分类"]["sector_limit_up_count"] == 1


def test_scan_limit_up_breakthroughs_reports_pool_error() -> None:
    def broken_pool_fetcher(trade_date: str | None = None) -> pd.DataFrame:
        raise RuntimeError("remote disconnected")

    items, errors = limit_up_service.scan_limit_up_breakthroughs(
        trade_date="2026-05-12",
        pool_fetcher=broken_pool_fetcher,
    )

    assert items == []
    assert errors == [{"股票代码": "全部", "error": "remote disconnected"}]


def make_review_history(code: str) -> pd.DataFrame:
    closes = [10.0, 11.0, 9.0, 12.0, 13.0, 8.0]
    return pd.DataFrame(
        {
            "日期": pd.date_range("2026-04-08", periods=len(closes), freq="D"),
            "股票代码": [code] * len(closes),
            "开盘": closes,
            "收盘": closes,
            "最高": closes,
            "最低": closes,
            "成交量": [1000] * len(closes),
            "成交额": [10000] * len(closes),
            "涨跌幅": [0.0] * len(closes),
            "换手率": [1.0] * len(closes),
        }
    )


def test_backfill_limit_up_review_snapshots_and_stats(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    limit_up_service.persist_limit_up_candidates(
        [
            {
                "trade_date": "2026-04-08",
                "code": "600001",
                "name": "样例股份",
                "sector": "软件服务",
                "close_price": 10.0,
                "pct_change": 10.0,
                "turnover_rate": 8.5,
                "consecutive_boards": 2,
                "sector_limit_up_count": 4,
                "sector_heat_rank": 1,
                "first_limit_time": "09:35:00",
                "last_limit_time": "14:50:00",
                "open_board_count": 0,
                "score": 85,
                "reason": "突破近60日收盘高点",
                "payload": {
                    "breakout_ratio": 1.05,
                    "data_source": "旧缓存兜底",
                    "cache_fetched_at": "2026-01-01 00:00:00",
                },
            }
        ]
    )

    def fake_fetcher(code: str, start_date: str, end_date: str, adjust: str = "qfq") -> pd.DataFrame:
        assert code == "600001"
        assert start_date == "2026-04-08"
        assert adjust == "qfq"
        return make_review_history(code)

    first = limit_up_service.backfill_limit_up_review_snapshots(fetcher=fake_fetcher)
    second = limit_up_service.backfill_limit_up_review_snapshots(fetcher=fake_fetcher)
    snapshots = limit_up_service.list_limit_up_review_snapshots(horizon="T+3")
    stats = limit_up_service.summarize_limit_up_review_stats(horizon="T+3")

    assert first["count"] == 3
    assert second["count"] == 3
    assert first["errors"] == []
    assert {item["horizon"] for item in first["items"]} == {"T+1", "T+3", "T+5"}
    assert len(snapshots) == 1
    assert snapshots[0]["pct_return"] == 20.0
    assert snapshots[0]["max_drawdown"] == -10.0
    assert snapshots[0]["data_source"] == "旧缓存兜底"
    assert snapshots[0]["cache_fetched_at"] == "2026-01-01 00:00:00"
    assert stats == [
        {
            "score_bucket": "80+",
            "data_source": "旧缓存兜底",
            "sample_count": 1,
            "avg_return": 20.0,
            "win_rate": 1.0,
            "avg_max_drawdown": -10.0,
            "avg_sector_limit_up_count": 4.0,
            "strategy_verdict": "样本不足",
            "strategy_note": "样本数少于5，先继续积累",
            "strategy_confidence": "低",
            "strategy_actionable": False,
            "min_actionable_samples": 5,
            "samples_to_actionable": 4,
            "horizon": "T+3",
        }
    ]


def test_limit_up_review_uses_cached_daily_bars(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_pool_fetcher(trade_date: str | None = None) -> pd.DataFrame:
        return pd.DataFrame(
            [
                {
                    "代码": "600001",
                    "名称": "样例股份",
                    "最新价": 10.0,
                    "涨跌幅": 10.0,
                    "所属行业": "软件服务",
                    "连板数": 1,
                    "炸板次数": 0,
                }
            ]
        )

    limit_up_service.scan_and_save_limit_up_breakthroughs(
        trade_date="2026-04-08",
        min_score=0,
        pool_fetcher=fake_pool_fetcher,
        history_fetcher=lambda code, lookback_days, adjust: make_review_history(code),
    )

    result = limit_up_service.backfill_limit_up_review_snapshots(
        trade_date="2026-04-08",
        fetcher=lambda code, start_date, end_date, adjust: limit_up_service.fetch_daily_history_range_with_cache(
            code,
            start_date,
            end_date,
            adjust,
        ),
    )

    assert result["errors"] == []
    assert result["count"] == 3
    assert next(item for item in result["items"] if item["horizon"] == "T+3")["pct_return"] == 20.0
