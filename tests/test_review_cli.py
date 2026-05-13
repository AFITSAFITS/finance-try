from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


REVIEW_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "review_signal_outcomes.py"


def load_module():
    spec = importlib.util.spec_from_file_location("review_signal_outcomes_cli", REVIEW_SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_review_cli_stats_only_skips_signal_backfill(monkeypatch, capsys) -> None:
    module = load_module()
    called = {"backfill": False, "stats": False}

    def fake_backfill_review_snapshots(**kwargs):
        called["backfill"] = True
        raise AssertionError("stats-only must not backfill")

    def fake_summarize_review_stats(**kwargs):
        called["stats"] = True
        assert kwargs["horizon"] == "T+3"
        return [
            {
                "score_bucket": "60-80",
                "signal_direction": "偏多",
                "observation_conclusion": "谨慎观察",
                "risk_bucket": "有风险提示",
                "risk_plan_bucket": "8%+",
                "summary": "MA5上穿MA20",
                "horizon": "T+3",
                "sample_count": 1,
                "avg_return": 1.2,
                "win_rate": 1.0,
                "avg_max_drawdown": -2.1,
                "avg_position_60d": 0.8,
                "avg_volume_ratio": 1.3,
                "avg_stop_distance_pct": 8.7,
                "avg_risk_reward_ratio": 2.0,
                "strategy_verdict": "样本不足",
                "strategy_note": "继续积累样本",
            }
        ]

    monkeypatch.setattr(module.review_service, "backfill_review_snapshots", fake_backfill_review_snapshots)
    monkeypatch.setattr(module.review_service, "summarize_review_stats", fake_summarize_review_stats)
    monkeypatch.setattr(sys, "argv", [str(REVIEW_SCRIPT), "--stats-only", "--summary-horizon", "T+3"])

    assert module.main() == 0
    captured = capsys.readouterr()
    assert called == {"backfill": False, "stats": True}
    assert "review_snapshots=skipped" in captured.out
    assert "conclusion=谨慎观察" in captured.out
    assert "risk_plan=8%+" in captured.out
    assert "avg_stop_distance_pct=8.7" in captured.out


def test_review_cli_stats_only_skips_limit_up_backfill(monkeypatch, capsys) -> None:
    module = load_module()
    called = {"backfill": False, "stats": False}

    def fake_backfill_limit_up_review_snapshots(**kwargs):
        called["backfill"] = True
        raise AssertionError("stats-only must not backfill")

    def fake_summarize_limit_up_review_stats(**kwargs):
        called["stats"] = True
        return []

    monkeypatch.setattr(module.limit_up_service, "backfill_limit_up_review_snapshots", fake_backfill_limit_up_review_snapshots)
    monkeypatch.setattr(module.limit_up_service, "summarize_limit_up_review_stats", fake_summarize_limit_up_review_stats)
    monkeypatch.setattr(sys, "argv", [str(REVIEW_SCRIPT), "--target", "limit-up", "--stats-only"])

    assert module.main() == 0
    captured = capsys.readouterr()
    assert called == {"backfill": False, "stats": True}
    assert "limit_up_review_snapshots=skipped" in captured.out
