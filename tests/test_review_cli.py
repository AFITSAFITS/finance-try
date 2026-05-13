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
                "data_freshness": "最近交易日",
                "data_source": "旧缓存兜底",
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
                "stop_hit_rate": 0.25,
                "target_hit_rate": 0.5,
                "stop_first_rate": 0.25,
                "target_first_rate": 0.5,
                "same_day_hit_rate": 0.0,
                "strategy_verdict": "样本不足",
                "strategy_confidence": "低",
                "strategy_actionable": False,
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
    assert "data_freshness=最近交易日" in captured.out
    assert "data_source=旧缓存兜底" in captured.out
    assert "risk_plan=8%+" in captured.out
    assert "avg_stop_distance_pct=8.7" in captured.out
    assert "stop_hit_rate=0.25" in captured.out
    assert "target_hit_rate=0.5" in captured.out
    assert "stop_first_rate=0.25" in captured.out
    assert "target_first_rate=0.5" in captured.out
    assert "same_day_hit_rate=0.0" in captured.out
    assert "confidence=低" in captured.out
    assert "actionable=False" in captured.out


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


def test_review_cli_prints_strategy_summary(monkeypatch, capsys) -> None:
    module = load_module()
    called = {"strategy": False}

    monkeypatch.setattr(module.review_service, "summarize_review_stats", lambda **kwargs: [])
    monkeypatch.setattr(module.limit_up_service, "summarize_limit_up_review_stats", lambda **kwargs: [])

    def fake_summarize_strategy_decisions(**kwargs):
        called["strategy"] = True
        assert kwargs["horizon"] == "T+3"
        assert kwargs["limit"] == 5
        assert kwargs["min_samples"] == 5
        assert kwargs["actionable_only"] is True
        assert kwargs["data_source"] == "本地缓存"
        return {
            "horizon": "T+3",
            "total_count": 1,
            "filtered_count": 1,
            "actionable_count": 1,
            "min_samples": 5,
            "actionable_only": True,
            "data_source": "本地缓存",
            "verdict_counts": {"保留": 1},
            "confidence_counts": {"中": 1},
            "strategy_type_counts": {"日线信号": 1},
            "data_source_counts": {"本地缓存": 1},
            "items": [
                {
                    "strategy_type": "日线信号",
                    "strategy_name": "60-80 / 偏多 / MACD金叉",
                    "horizon": "T+3",
                    "data_source": "本地缓存",
                    "sample_count": 6,
                    "avg_return": 2.1,
                    "win_rate": 0.6,
                    "avg_max_drawdown": -3.2,
                    "strategy_verdict": "保留",
                    "strategy_confidence": "中",
                    "strategy_actionable": True,
                    "strategy_note": "表现可接受",
                }
            ],
        }

    monkeypatch.setattr(module.strategy_summary_service, "summarize_strategy_decisions", fake_summarize_strategy_decisions)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(REVIEW_SCRIPT),
            "--stats-only",
            "--strategy-summary",
            "--strategy-limit",
            "5",
            "--strategy-min-samples",
            "5",
            "--strategy-actionable-only",
            "--strategy-data-source",
            "本地缓存",
        ],
    )

    assert module.main() == 0
    captured = capsys.readouterr()
    assert called == {"strategy": True}
    assert "strategy_summary" in captured.out
    assert "total=1" in captured.out
    assert "filtered=1" in captured.out
    assert "actionable=1" in captured.out
    assert "min_samples=5" in captured.out
    assert "actionable_only=True" in captured.out
    assert "data_source=本地缓存" in captured.out
    assert "types={'日线信号': 1}" in captured.out
    assert "sources={'本地缓存': 1}" in captured.out
    assert "type=日线信号" in captured.out
    assert "verdict=保留" in captured.out
