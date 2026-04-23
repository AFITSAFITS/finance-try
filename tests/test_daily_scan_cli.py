from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

DAILY_SCAN_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_daily_scan.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_daily_scan_cli", DAILY_SCAN_SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_daily_scan_cli_success(monkeypatch, capsys) -> None:
    module = load_module()

    monkeypatch.setattr(
        module.scan_workflow,
        "run_default_watchlist_scan",
        lambda **kwargs: {
            "watchlist": {"name": "默认股票池", "count": 2},
            "persisted_events": [
                {"summary": "MACD金叉", "code": "600519"},
                {"summary": "MA5上穿MA20", "code": "600519"},
            ],
            "delivery_results": [
                {"channel": "stdout", "status": "delivered", "created": True},
                {"channel": "stdout", "status": "delivered", "created": True},
            ],
            "errors": [{"股票代码": "000001", "error": "network timeout"}],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [str(DAILY_SCAN_SCRIPT), "--channel", "stdout"],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert "默认股票池" in captured.out
    assert "MACD金叉" in captured.out
    assert "WARNING [000001]: network timeout" in captured.err
