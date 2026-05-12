from __future__ import annotations

import sys
import types

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


def test_list_sector_rotation_trends_filters_date_and_name(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    sector_rotation_service.persist_sector_rotation_snapshots(
        [
            {
                "trade_date": "2026-05-10",
                "sector_type": "industry",
                "sector_name": "软件服务",
                "latest_close": 100.0,
                "latest_pct_change": 1.0,
                "return_5d": 4.0,
                "return_10d": 6.0,
                "position_60d": 0.4,
                "activity_score": 45.0,
                "rotation_score": 69.0,
                "signal": "活跃低位",
                "payload": {},
            },
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
                "payload": {},
            },
            {
                "trade_date": "2026-05-12",
                "sector_type": "industry",
                "sector_name": "银行",
                "latest_close": 90.0,
                "latest_pct_change": 0.5,
                "return_5d": 1.0,
                "return_10d": 2.0,
                "position_60d": 0.7,
                "activity_score": 10.0,
                "rotation_score": 22.0,
                "signal": "普通观察",
                "payload": {},
            },
        ]
    )

    items = sector_rotation_service.list_sector_rotation_trends(
        sector_type="industry",
        sector_names=["软件服务"],
        start_date="2026-05-11",
        end_date="2026-05-12",
    )

    assert [item["trade_date"] for item in items] == ["2026-05-12"]
    assert items[0]["sector_name"] == "软件服务"
    assert items[0]["rotation_score"] == 86.0


def test_fetch_sector_spot_falls_back_to_ths(monkeypatch) -> None:
    def broken_em() -> pd.DataFrame:
        raise RuntimeError("remote disconnected")

    fake_ak = types.SimpleNamespace(
        stock_board_industry_name_em=broken_em,
        stock_board_industry_name_ths=lambda: pd.DataFrame([{"name": "软件服务", "code": "881001"}]),
        stock_board_concept_name_em=broken_em,
        stock_board_concept_name_ths=lambda: pd.DataFrame([{"name": "AI概念", "code": "301001"}]),
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    df = sector_rotation_service.fetch_sector_spot("industry")

    assert df.iloc[0]["name"] == "软件服务"


def test_normalize_sector_spot_accepts_ths_name_column() -> None:
    df = sector_rotation_service.normalize_sector_spot(
        pd.DataFrame([{"name": "软件服务", "code": "881001"}]),
        "industry",
    )

    assert df.iloc[0]["sector_name"] == "软件服务"
    assert df.iloc[0]["spot_pct_change"] is None


def test_fetch_sector_history_falls_back_to_ths(monkeypatch) -> None:
    def broken_em(**kwargs) -> pd.DataFrame:
        raise RuntimeError("remote disconnected")

    def fake_ths(**kwargs) -> pd.DataFrame:
        assert kwargs["symbol"] == "软件服务"
        assert kwargs["start_date"] == "20260501"
        assert kwargs["end_date"] == "20260512"
        return pd.DataFrame(
            [
                {"日期": "2026-05-12", "收盘价": 102.0},
            ]
        )

    fake_ak = types.SimpleNamespace(
        stock_board_industry_hist_em=broken_em,
        stock_board_concept_hist_em=broken_em,
        stock_board_industry_index_ths=fake_ths,
        stock_board_concept_index_ths=fake_ths,
    )
    monkeypatch.setitem(sys.modules, "akshare", fake_ak)

    df = sector_rotation_service.fetch_sector_history(
        "软件服务",
        "industry",
        start_date="20260501",
        end_date="20260512",
    )

    assert df.iloc[0]["收盘价"] == 102.0


def test_scan_sector_rotation_reports_spot_error() -> None:
    def broken_spot_fetcher(sector_type: str) -> pd.DataFrame:
        raise RuntimeError("remote disconnected")

    items, errors = sector_rotation_service.scan_sector_rotation(
        trade_date="2026-05-12",
        spot_fetcher=broken_spot_fetcher,
    )

    assert items == []
    assert errors == [{"板块": "全部", "error": "remote disconnected"}]
