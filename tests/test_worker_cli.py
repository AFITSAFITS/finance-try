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
        [str(WORKER_SCRIPT), "--run-once", "--channel", "feishu_webhook", "--min-score", "70", "--mute-downgraded-strategies"],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert called["channel"] == "feishu_webhook"
    assert called["min_score"] == 70.0
    assert called["mute_downgraded_strategies"] is True
    assert "min_score=70.0" in captured.out
    assert "默认股票池" in captured.out


def test_run_scan_worker_cli_run_once_with_review(monkeypatch, capsys) -> None:
    module = load_module()
    called: dict[str, object] = {}

    def fake_run_single_scan_job(**kwargs):
        called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "requested_count": 1,
            "elapsed_seconds": 1.2,
            "min_score": kwargs["min_score"],
            "persisted_events": [],
            "notification_events": [],
            "delivery_results": [],
            "errors": [],
            "review_result": {"count": 2, "errors": []},
            "review_stats": [{"sample_count": 6}],
        }

    monkeypatch.setattr(module.worker_service, "run_single_scan_job", fake_run_single_scan_job)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(WORKER_SCRIPT),
            "--run-once",
            "--review-after-scan",
            "--review-trade-date",
            "2026-05-01",
            "--review-horizons",
            "1,3",
        ],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert called["review_after_scan"] is True
    assert called["review_trade_date"] == "2026-05-01"
    assert called["review_horizons"] == [1, 3]
    assert called["review_due_only"] is True
    assert "review_snapshots=2" in captured.out
    assert "review_due_only=True" in captured.out
    assert "review_stats=1" in captured.out
