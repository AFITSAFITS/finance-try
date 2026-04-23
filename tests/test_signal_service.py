from __future__ import annotations

import pandas as pd
import requests

from app import signal_service


def make_history(code: str, closes: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "日期": pd.date_range("2026-03-01", periods=len(closes), freq="D"),
            "股票代码": [code] * len(closes),
            "收盘": closes,
            "涨跌幅": [0.0] * len(closes),
        }
    )


def test_scan_stock_signal_events_detects_latest_crosses() -> None:
    sample_map = {
        "600001": make_history("600001", [10.0] * 34 + [20.0]),
        "600002": make_history("600002", [10.0] * 34 + [0.0]),
        "600003": make_history("600003", [10.0] * 35),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        assert lookback_days == 180
        assert adjust == "qfq"
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002", "600003"],
        fetcher=fake_fetcher,
    )

    assert errors == []
    assert list(df["股票代码"]) == ["600001", "600002"]

    up_row = df[df["股票代码"] == "600001"].iloc[0]
    assert up_row["MACD信号"] == "MACD金叉"
    assert up_row["均线信号"] == "MA5上穿MA20"
    assert up_row["信号"] == "MACD金叉, MA5上穿MA20"

    down_row = df[df["股票代码"] == "600002"].iloc[0]
    assert down_row["MACD信号"] == "MACD死叉"
    assert down_row["均线信号"] == "MA5下穿MA20"
    assert down_row["信号"] == "MACD死叉, MA5下穿MA20"


def test_scan_stock_signal_events_collects_fetch_errors() -> None:
    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        if code == "600002":
            raise RuntimeError("network timeout")
        return make_history(code, [10.0] * 35)

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002"],
        fetcher=fake_fetcher,
    )

    assert df.empty
    assert errors == [{"股票代码": "600002", "error": "network timeout"}]


def test_scan_stock_signal_events_supports_parallel_fetch() -> None:
    sample_map = {
        "600001": make_history("600001", [10.0] * 34 + [20.0]),
        "600002": make_history("600002", [10.0] * 34 + [0.0]),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002"],
        fetcher=fake_fetcher,
        max_workers=4,
    )

    assert errors == []
    assert set(df["股票代码"]) == {"600001", "600002"}


def test_detects_macd_secondary_golden_cross_above_zero() -> None:
    history_df = pd.DataFrame(
        {
            "日期": pd.date_range("2026-03-01", periods=6, freq="D"),
            "股票代码": ["600001"] * 6,
            "收盘": [10.0] * 6,
            "涨跌幅": [0.0] * 6,
            "DIF": [-1.2, -0.4, 0.8, 1.0, 0.1, 0.7],
            "DEA": [-1.0, -0.6, 0.9, 1.1, 0.2, 0.5],
            "MA5": [10.0] * 6,
            "MA20": [10.0] * 6,
        }
    )

    assert signal_service.detect_macd_secondary_golden_cross_above_zero(history_df) is True


def test_scan_stock_signal_events_filters_secondary_golden_cross_only() -> None:
    sample_map = {
        "600001": pd.DataFrame(
            {
                "日期": pd.date_range("2026-03-01", periods=6, freq="D"),
                "股票代码": ["600001"] * 6,
                "收盘": [10.0] * 6,
                "涨跌幅": [0.0] * 6,
                "DIF": [-1.2, -0.4, 0.8, 1.0, 0.1, 0.7],
                "DEA": [-1.0, -0.6, 0.9, 1.1, 0.2, 0.5],
                "MA5": [10.0] * 6,
                "MA20": [10.0] * 6,
            }
        ),
        "600002": make_history("600002", [10.0] * 34 + [20.0]),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002"],
        fetcher=fake_fetcher,
        only_secondary_golden_cross=True,
    )

    assert errors == []
    assert list(df["股票代码"]) == ["600001"]
    assert df.iloc[0]["MACD形态"] == "水下金叉后水上再次金叉"


class _FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


def test_fetch_daily_history_eastmoney_retries_transient_errors() -> None:
    attempts = {"count": 0}

    def fake_requester(url: str, params: dict[str, str], timeout: float, headers: dict[str, str]):
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise requests.ConnectionError("remote disconnected")
        return _FakeResponse(
            {
                "data": {
                    "klines": [
                        "2026-04-07,10,10,10,10,100,1000,0,0,0,1",
                        "2026-04-08,10,11,11,10,120,1200,0,10,1,1.2",
                    ]
                }
            }
        )

    df = signal_service.fetch_daily_history_eastmoney(
        code="600001",
        start_date="20260101",
        end_date="20260409",
        adjust="qfq",
        requester=fake_requester,
        retries=2,
    )

    assert attempts["count"] == 2
    assert list(df["股票代码"]) == ["600001", "600001"]
    assert list(df["收盘"]) == [10.0, 11.0]


def test_fetch_daily_history_akshare_falls_back_when_direct_fetch_fails(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_eastmoney",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_akshare_provider",
        lambda code, start_date, end_date, adjust: make_history(code, [10.0] * 35),
    )

    df = signal_service.fetch_daily_history_akshare("600001", lookback_days=180, adjust="qfq")

    assert len(df) == 35
    assert list(df["股票代码"].unique()) == ["600001"]
