from __future__ import annotations

import time

import pandas as pd
import requests

from app import signal_service


def make_history(code: str, closes: list[float]) -> pd.DataFrame:
    dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=len(closes), freq="D")
    return pd.DataFrame(
        {
            "日期": dates,
            "股票代码": [code] * len(closes),
            "开盘": closes,
            "收盘": closes,
            "最高": closes,
            "最低": closes,
            "成交量": [1000.0] * len(closes),
            "涨跌幅": [0.0] * len(closes),
        }
    )


def make_ohlc_history(
    code: str,
    closes: list[float],
    *,
    latest_open: float,
    latest_high: float,
    latest_low: float,
) -> pd.DataFrame:
    df = make_history(code, closes)
    df.loc[df.index[-1], "开盘"] = latest_open
    df.loc[df.index[-1], "最高"] = latest_high
    df.loc[df.index[-1], "最低"] = latest_low
    return df


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
    assert up_row["信号评分"] >= 80
    assert up_row["信号方向"] == "偏多"
    assert up_row["60日位置"] == 1.0
    assert up_row["量能比"] == 1.0
    assert "接近60日高位" in up_row["风险提示"]

    down_row = df[df["股票代码"] == "600002"].iloc[0]
    assert down_row["MACD信号"] == "MACD死叉"
    assert down_row["均线信号"] == "MA5下穿MA20"
    assert down_row["信号"] == "MACD死叉, MA5下穿MA20"
    assert down_row["信号评分"] <= 30
    assert down_row["信号方向"] == "偏空"


def test_score_signal_row_accounts_for_position_and_volume() -> None:
    strong_row = signal_service.score_signal_row(
        {
            "MACD信号": "MACD金叉",
            "均线信号": "MA5上穿MA20",
            "涨跌幅": 2.0,
            "60日位置": 0.35,
            "量能比": 1.8,
        }
    )
    high_weak_row = signal_service.score_signal_row(
        {
            "MACD信号": "MACD金叉",
            "均线信号": "MA5上穿MA20",
            "涨跌幅": 2.0,
            "60日位置": 0.95,
            "量能比": 0.5,
        }
    )

    assert strong_row["信号评分"] > high_weak_row["信号评分"]
    assert "价格位置不高" in strong_row["评分原因"]
    assert "量能放大" in strong_row["评分原因"]
    assert "接近60日高位" in high_weak_row["风险提示"]
    assert "量能不足" in high_weak_row["风险提示"]


def test_data_freshness_marks_stale_signal_risk() -> None:
    recent = signal_service.extract_data_freshness("2026-05-12", now=pd.Timestamp("2026-05-13").to_pydatetime())
    stale = signal_service.score_signal_row(
        {
            "MACD信号": "MACD金叉",
            "均线信号": "MA5上穿MA20",
            "涨跌幅": 2.0,
            "数据时效": "数据明显滞后",
        }
    )

    assert recent == {"数据时效": "最近交易日", "数据滞后天数": 1}
    assert "数据明显滞后" in stale["风险提示"]
    assert stale["信号评分"] < 90


def test_extract_candlestick_profile_detects_strong_and_upper_shadow() -> None:
    strong = signal_service.extract_candlestick_profile(
        pd.Series({"开盘": 10.0, "收盘": 10.5, "最高": 10.55, "最低": 9.95})
    )
    upper_shadow = signal_service.extract_candlestick_profile(
        pd.Series({"开盘": 10.0, "收盘": 10.2, "最高": 11.2, "最低": 9.9})
    )

    assert strong["K线形态"] == "强势收盘"
    assert upper_shadow["K线形态"] == "长上影线"
    assert upper_shadow["K线提示"] == "冲高回落"


def test_candlestick_profile_adjusts_bullish_signal_score() -> None:
    strong_row = signal_service.score_signal_row(
        {
            "MACD信号": "MACD金叉",
            "均线信号": "MA5上穿MA20",
            "涨跌幅": 2.0,
            "60日位置": 0.5,
            "量能比": 1.0,
            "K线形态": "强势收盘",
        }
    )
    upper_shadow_row = signal_service.score_signal_row(
        {
            "MACD信号": "MACD金叉",
            "均线信号": "MA5上穿MA20",
            "涨跌幅": 2.0,
            "60日位置": 0.5,
            "量能比": 1.0,
            "K线形态": "长上影线",
        }
    )

    assert strong_row["信号评分"] > upper_shadow_row["信号评分"]
    assert "K线收盘较强" in strong_row["评分原因"]
    assert "冲高回落" in upper_shadow_row["风险提示"]


def test_extract_bullish_trade_plan_uses_nearby_support() -> None:
    history_df = make_history("600001", [10.0] * 24 + [12.0])
    enriched = signal_service.add_indicator_columns(signal_service.normalize_history_df(history_df, "600001"))
    row = {"收盘": 12.0, "信号方向": "偏多"}

    plan = signal_service.extract_bullish_trade_plan(enriched, row)

    assert plan["参考止损"] is not None
    assert plan["参考止损"] < 12.0
    assert plan["参考目标"] > 12.0
    assert plan["风险收益比"] == 2.0


def test_trade_plan_risk_lowers_score_when_stop_is_too_far() -> None:
    row = {
        "收盘": 10.0,
        "信号方向": "偏多",
        "信号评分": 82,
        "信号级别": "重点观察",
        "风险提示": "无明显风险",
        "参考止损": 9.0,
    }

    signal_service.apply_trade_plan_risk(row)

    assert row["信号评分"] == 77
    assert row["信号级别"] == "观察"
    assert "止损距离偏大" in row["风险提示"]


def test_apply_observation_conclusion_uses_score_risk_and_direction() -> None:
    key_row = {
        "信号方向": "偏多",
        "信号评分": 85,
        "风险提示": "无明显风险",
        "收盘": 10.0,
        "参考止损": 9.5,
    }
    caution_row = {
        "信号方向": "偏多",
        "信号评分": 75,
        "风险提示": "止损距离偏大",
        "收盘": 10.0,
        "参考止损": 9.0,
    }
    bearish_row = {"信号方向": "偏空", "信号评分": 20, "风险提示": "跌幅偏大"}

    signal_service.apply_observation_conclusion(key_row)
    signal_service.apply_observation_conclusion(caution_row)
    signal_service.apply_observation_conclusion(bearish_row)

    assert key_row["观察结论"] == "重点观察"
    assert caution_row["观察结论"] == "谨慎观察"
    assert bearish_row["观察结论"] == "风险回避"


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
    assert df.iloc[0]["信号级别"] == "重点观察"


def test_scan_stock_signal_events_filters_by_min_score() -> None:
    sample_map = {
        "600001": make_history("600001", [10.0] * 34 + [20.0]),
        "600002": make_history("600002", [10.0] * 34 + [0.0]),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002"],
        fetcher=fake_fetcher,
        min_score=60,
    )

    assert errors == []
    assert list(df["股票代码"]) == ["600001"]


def test_scan_stock_signal_events_adds_relative_strength() -> None:
    sample_map = {
        "600001": make_history("600001", [10.0] * 64 + [20.0]),
        "600002": make_history("600002", [10.0] * 64 + [11.0]),
        "600003": make_history("600003", [10.0] * 65),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002", "600003"],
        fetcher=fake_fetcher,
        max_workers=1,
    )

    assert errors == []
    strong_row = df[df["股票代码"] == "600001"].iloc[0]
    normal_row = df[df["股票代码"] == "600002"].iloc[0]
    assert strong_row["60日涨幅"] == 100.0
    assert strong_row["相对强度"] == 100.0
    assert "股票池内强势" in strong_row["评分原因"]
    assert normal_row["20日涨幅"] == 10.0
    assert normal_row["相对强度"] > 50.0


def test_relative_strength_can_filter_weak_pool_signal() -> None:
    sample_map = {
        "600001": make_history("600001", [10.0] * 64 + [10.5]),
        "600002": make_history("600002", [10.0] * 64 + [20.0]),
        "600003": make_history("600003", [10.0] * 64 + [18.0]),
        "600004": make_history("600004", [10.0] * 64 + [16.0]),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002", "600003", "600004"],
        fetcher=fake_fetcher,
        min_score=80,
        max_workers=1,
    )

    assert errors == []
    assert "600001" not in set(df["股票代码"])


def test_scan_stock_signal_events_outputs_candlestick_profile() -> None:
    sample_map = {
        "600001": make_ohlc_history("600001", [10.0] * 64 + [20.0], latest_open=19.0, latest_high=20.1, latest_low=18.8),
        "600002": make_ohlc_history("600002", [10.0] * 64 + [20.0], latest_open=19.8, latest_high=23.0, latest_low=19.5),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001", "600002"],
        fetcher=fake_fetcher,
        max_workers=1,
    )

    assert errors == []
    strong_row = df[df["股票代码"] == "600001"].iloc[0]
    upper_shadow_row = df[df["股票代码"] == "600002"].iloc[0]
    assert strong_row["K线形态"] == "强势收盘"
    assert upper_shadow_row["K线形态"] == "长上影线"
    assert "冲高回落" in upper_shadow_row["风险提示"]


def test_scan_stock_signal_events_outputs_trade_plan() -> None:
    sample_map = {
        "600001": make_ohlc_history("600001", [10.0] * 64 + [12.0], latest_open=11.5, latest_high=12.1, latest_low=11.4),
    }

    def fake_fetcher(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
        return sample_map[code].copy()

    df, errors = signal_service.scan_stock_signal_events(
        codes=["600001"],
        fetcher=fake_fetcher,
        max_workers=1,
    )

    assert errors == []
    row = df.iloc[0]
    assert row["数据时效"] in {"当日数据", "最近交易日", "数据可能滞后", "数据明显滞后"}
    assert row["数据滞后天数"] >= 0
    assert row["参考止损"] < row["收盘"]
    assert row["参考目标"] > row["收盘"]
    assert row["风险收益比"] == 2.0
    assert row["观察结论"] in {"正常观察", "谨慎观察", "重点观察"}


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


def test_fetch_daily_history_best_effort_falls_back_to_yahoo(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_eastmoney",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_akshare_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("akshare down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_tx_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("tencent down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_sina_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("sina down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_baostock_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("baostock down")),
    )
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_yahoo",
        lambda code, start_date, end_date: make_history(code, [10.0, 11.0]),
    )

    df = signal_service.fetch_daily_history_best_effort(
        code="600001",
        start_date="20260401",
        end_date="20260408",
        adjust="qfq",
    )

    assert list(df["收盘"]) == [10.0, 11.0]


def test_fetch_daily_history_best_effort_falls_back_to_tencent(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_eastmoney",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_akshare_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("akshare down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_tx_provider",
        lambda code, start_date, end_date, adjust: make_history(code, [10.0, 11.0]),
    )

    df = signal_service.fetch_daily_history_best_effort(
        code="600001",
        start_date="20260401",
        end_date="20260408",
        adjust="qfq",
    )

    assert list(df["收盘"]) == [10.0, 11.0]


def test_fetch_daily_history_best_effort_skips_timed_out_provider(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_eastmoney",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )

    def slow_akshare(**kwargs):
        time.sleep(0.3)
        return make_history(kwargs["code"], [9.0, 9.5])

    monkeypatch.setattr(signal_service, "_fetch_daily_history_akshare_provider", slow_akshare)
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_tx_provider",
        lambda code, start_date, end_date, adjust: make_history(code, [10.0, 11.0]),
    )

    started_at = time.perf_counter()
    df = signal_service.fetch_daily_history_best_effort(
        code="600001",
        start_date="20260401",
        end_date="20260408",
        adjust="qfq",
        provider_timeout=0.1,
    )

    assert time.perf_counter() - started_at < 0.25
    assert list(df["收盘"]) == [10.0, 11.0]


def test_fetch_daily_history_best_effort_falls_back_to_baostock(monkeypatch) -> None:
    monkeypatch.setattr(
        signal_service,
        "fetch_daily_history_eastmoney",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_akshare_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("akshare down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_tx_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("tencent down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_sina_provider",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("sina down")),
    )
    monkeypatch.setattr(
        signal_service,
        "_fetch_daily_history_baostock_provider",
        lambda code, start_date, end_date, adjust: make_history(code, [10.0, 11.0]),
    )

    df = signal_service.fetch_daily_history_best_effort(
        code="600001",
        start_date="20260401",
        end_date="20260408",
        adjust="qfq",
    )

    assert list(df["收盘"]) == [10.0, 11.0]


def test_fetch_daily_history_yahoo_parses_chart_response() -> None:
    def fake_requester(url: str, params: dict[str, int | str], timeout: float, headers: dict[str, str]):
        return _FakeResponse(
            {
                "chart": {
                    "result": [
                        {
                            "timestamp": [1775520000, 1775606400],
                            "indicators": {
                                "quote": [
                                    {
                                        "open": [10.0, 10.5],
                                        "high": [10.2, 11.2],
                                        "low": [9.8, 10.4],
                                        "close": [10.0, 11.0],
                                        "volume": [1000, 1200],
                                    }
                                ]
                            },
                        }
                    ]
                }
            }
        )

    df = signal_service.fetch_daily_history_yahoo(
        code="600001",
        start_date="2026-04-07",
        end_date="2026-04-08",
        requester=fake_requester,
    )

    assert list(df["股票代码"]) == ["600001", "600001"]
    assert list(df["收盘"]) == [10.0, 11.0]
    assert pd.isna(df.iloc[0]["涨跌幅"])
    assert round(float(df.iloc[1]["涨跌幅"]), 4) == 10.0
