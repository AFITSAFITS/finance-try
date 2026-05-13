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
    called: dict[str, object] = {}

    def fake_run_default_watchlist_scan(**kwargs):
        called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 2},
            "min_score": kwargs["min_score"],
            "persisted_events": [
                {"summary": "MACD金叉", "code": "600519"},
                {"summary": "MA5上穿MA20", "code": "600519"},
            ],
            "delivery_results": [
                {"channel": "stdout", "status": "delivered", "created": True},
                {"channel": "stdout", "status": "delivered", "created": True},
            ],
            "signal_summary": {
                "signals": 2,
                "error_count": 1,
                "max_score": 85,
                "stale_signals": 0,
                "observation_counts": {"重点观察": 1, "谨慎观察": 1},
                "freshness_counts": {"最近交易日": 2},
            },
            "errors": [{"股票代码": "000001", "error": "network timeout"}],
        }

    monkeypatch.setattr(module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(DAILY_SCAN_SCRIPT), "--channel", "stdout", "--min-score", "70"],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert called["min_score"] == 70.0
    assert "min_score=70.0" in captured.out
    assert "signal_summary" in captured.out
    assert "stale_signals=0" in captured.out
    assert "默认股票池" in captured.out
    assert "MACD金叉" in captured.out
    assert "WARNING [000001]: network timeout" in captured.err
