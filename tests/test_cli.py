from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pandas as pd

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "get_stock_data.py"


def load_cli_module():
    spec = importlib.util.spec_from_file_location("get_stock_data_cli", SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daily_signals_cli_success(monkeypatch, capsys, tmp_path: Path) -> None:
    module = load_cli_module()
    codes_file = tmp_path / "codes.txt"
    codes_file.write_text("600592\n600487\n", encoding="utf-8")
    called: dict[str, object] = {}

    def fake_scan_stock_signal_events(**kwargs):
        called.update(kwargs)
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "MACD信号": "MACD金叉",
                        "MACD形态": "水下金叉后水上再次金叉",
                        "均线信号": "MA5上穿MA20",
                        "信号": "MACD金叉, 水下金叉后水上再次金叉, MA5上穿MA20",
                    }
                ]
            ),
            [{"股票代码": "600487", "error": "network timeout"}],
        )

    monkeypatch.setattr(module.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "daily-signals",
            "--codes-file",
            str(codes_file),
        ],
    )

    result = module.main()
    output = capsys.readouterr()

    assert result == 0
    assert called["codes"] == ["600592", "600487"]
    assert called["only_secondary_golden_cross"] is False
    assert "MACD金叉" in output.out
    assert "WARNING [600487]: network timeout" in output.err


def test_daily_signals_cli_secondary_golden_cross_flag(monkeypatch, capsys, tmp_path: Path) -> None:
    module = load_cli_module()
    codes_file = tmp_path / "codes.txt"
    codes_file.write_text("600592\n", encoding="utf-8")
    called: dict[str, object] = {}

    def fake_scan_stock_signal_events(**kwargs):
        called.update(kwargs)
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "MACD信号": "MACD金叉",
                        "MACD形态": "水下金叉后水上再次金叉",
                        "均线信号": None,
                        "信号": "MACD金叉, 水下金叉后水上再次金叉",
                    }
                ]
            ),
            [],
        )

    monkeypatch.setattr(module.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "daily-signals",
            "--codes-file",
            str(codes_file),
            "--only-secondary-golden-cross",
        ],
    )

    result = module.main()
    output = capsys.readouterr()

    assert result == 0
    assert called["only_secondary_golden_cross"] is True
    assert "水下金叉后水上再次金叉" in output.out


def test_realtime_quotes_cli_success(monkeypatch, capsys) -> None:
    module = load_cli_module()

    def fake_fetch_realtime_quotes_best_effort(codes):
        assert codes == ["600519", "000001"]
        return (
            [
                {
                    "code": "600519",
                    "name": "贵州茅台",
                    "latest_price": 1354.55,
                    "pct_change": -0.5,
                    "source": "eastmoney",
                }
            ],
            [{"股票代码": "000001", "error": "未返回实时行情"}],
            "eastmoney",
        )

    monkeypatch.setattr(
        module.realtime_quote_service,
        "fetch_realtime_quotes_best_effort",
        fake_fetch_realtime_quotes_best_effort,
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "realtime-quotes",
            "--codes",
            "600519,000001",
        ],
    )

    result = module.main()
    output = capsys.readouterr()

    assert result == 0
    assert "source=eastmoney" in output.out
    assert "贵州茅台" in output.out
    assert "WARNING [000001]: 未返回实时行情" in output.err
