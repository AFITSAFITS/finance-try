from __future__ import annotations

import pandas as pd

from app import sector_rotation_service


def make_sector_history() -> pd.DataFrame:
    closes = [100.0] * 50 + [92.0, 94.0, 96.0, 99.0, 102.0]
    return pd.DataFrame(
        {
            "日期": pd.date_range("2026-03-01", periods=len(closes), freq="D"),
            "收盘": closes,
            "涨跌幅": [0.0] * (len(closes) - 1) + [3.0],
        }
    )


def test_scan_sector_rotation_finds_active_low_position_sector() -> None:
    def fake_spot_fetcher(sector_type: str) -> pd.DataFrame:
        assert sector_type == "industry"
        return pd.DataFrame(
            [
                {"板块名称": "软件服务", "涨跌幅": 3.0},
                {"板块名称": "银行", "涨跌幅": 0.2},
            ]
        )

    def fake_history_fetcher(
        sector_name: str,
        sector_type: str,
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame:
        assert sector_type == "industry"
        assert end_date == "20260512"
        return make_sector_history()

    items, errors = sector_rotation_service.scan_sector_rotation(
        trade_date="2026-05-12",
        sector_type="industry",
        top_n=1,
        spot_fetcher=fake_spot_fetcher,
        history_fetcher=fake_history_fetcher,
    )

    assert errors == []
    assert len(items) == 1
    assert items[0]["sector_name"] == "软件服务"
    assert items[0]["rotation_score"] > 0
    assert items[0]["signal"] in {"活跃低位", "活跃偏高", "低位观察", "普通观察"}


def test_persist_and_list_sector_rotation_snapshots(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    saved = sector_rotation_service.persist_sector_rotation_snapshots(
        [
            {
                "trade_date": "2026-05-12",
                "sector_type": "industry",
                "sector_name": "软件服务",
                "latest_close": 102.0,
                "latest_pct_change": 3.0,
                "return_5d": 10.0,
                "return_10d": 8.0,
                "position_60d": 0.35,
                "activity_score": 60.0,
                "rotation_score": 86.0,
                "signal": "活跃低位",
                "payload": {"history_count": 55},
            }
        ]
    )

    assert saved[0]["sector_name"] == "软件服务"
    items = sector_rotation_service.list_sector_rotation_snapshots(
        trade_date="2026-05-12",
        sector_type="industry",
    )
    assert len(items) == 1
    assert items[0]["signal"] == "活跃低位"


def test_scan_sector_rotation_reports_spot_error() -> None:
    def broken_spot_fetcher(sector_type: str) -> pd.DataFrame:
        raise RuntimeError("remote disconnected")

    items, errors = sector_rotation_service.scan_sector_rotation(
        trade_date="2026-05-12",
        spot_fetcher=broken_spot_fetcher,
    )

    assert items == []
    assert errors == [{"板块": "全部", "error": "remote disconnected"}]
