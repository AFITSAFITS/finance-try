from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

WORKER_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "run_scan_worker.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_scan_worker_cli", WORKER_SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_scan_worker_cli_run_once(monkeypatch, capsys) -> None:
    module = load_module()
    called: dict[str, object] = {}

    def fake_run_single_scan_job(**kwargs):
        called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 2},
            "requested_count": 2,
            "elapsed_seconds": 1.2,
            "min_score": kwargs["min_score"],
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
        }

    monkeypatch.setattr(module.worker_service, "run_single_scan_job", fake_run_single_scan_job)
    monkeypatch.setattr(
        sys,
        "argv",
        [str(WORKER_SCRIPT), "--run-once", "--channel", "feishu_webhook", "--min-score", "70"],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert called["channel"] == "feishu_webhook"
    assert called["min_score"] == 70.0
    assert "min_score=70.0" in captured.out
    assert "默认股票池" in captured.out
