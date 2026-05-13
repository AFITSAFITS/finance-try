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
    review_called = {"backfill": False, "stats": False}

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
                "actionable_signals": 2,
                "no_action_signals": 0,
                "cautious_signals": 1,
                "stale_signals": 0,
                "cache_fallback_signals": 1,
                "observation_counts": {"重点观察": 1, "谨慎观察": 1},
                "freshness_counts": {"最近交易日": 2},
                "data_source_counts": {"旧缓存兜底": 1, "外部行情源": 1},
                "relative_strength_bucket_counts": {"强势": 1, "偏弱": 1},
                "flow_confirmation_counts": {"资金支持": 1, "资金背离": 1},
                "position_size_counts": {"≤30%": 1, "≤10%": 1},
            },
            "strategy_guard": {"horizon": "T+1", "matched_count": 1, "total_count": 2, "mute_downgraded": True, "muted_count": 1},
            "scan_run": {"id": 3, "run_at": "2026-05-13 11:35:00", "status": "正常", "note": "扫描完成并生成信号"},
            "errors": [{"股票代码": "000001", "error": "network timeout"}],
        }

    monkeypatch.setattr(module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)
    monkeypatch.setattr(module.review_service, "backfill_review_snapshots", lambda **kwargs: review_called.update(backfill=True))
    monkeypatch.setattr(module.review_service, "summarize_review_stats", lambda **kwargs: review_called.update(stats=True))
    monkeypatch.setattr(
        sys,
        "argv",
        [str(DAILY_SCAN_SCRIPT), "--channel", "stdout", "--min-score", "70", "--mute-downgraded-strategies"],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert called["min_score"] == 70.0
    assert called["mute_downgraded_strategies"] is True
    assert review_called == {"backfill": False, "stats": False}
    assert "min_score=70.0" in captured.out
    assert "signal_summary" in captured.out
    assert "scan_run_id=3" in captured.out
    assert "status=正常" in captured.out
    assert "stale_signals=0" in captured.out
    assert "actionable=2" in captured.out
    assert "no_action=0" in captured.out
    assert "cautious=1" in captured.out
    assert "cache_fallback_signals=1" in captured.out
    assert "data_sources={'旧缓存兜底': 1, '外部行情源': 1}" in captured.out
    assert "strength={'强势': 1, '偏弱': 1}" in captured.out
    assert "flow={'资金支持': 1, '资金背离': 1}" in captured.out
    assert "positions={'≤30%': 1, '≤10%': 1}" in captured.out
    assert "strategy_guard horizon=T+1 matched=1 total=2 mute_downgraded=True muted=1" in captured.out
    assert "默认股票池" in captured.out
    assert "MACD金叉" in captured.out
    assert "WARNING [000001]: network timeout" in captured.err


def test_run_daily_scan_cli_can_review_after_scan(monkeypatch, capsys) -> None:
    module = load_module()
    scan_called: dict[str, object] = {}
    review_called: dict[str, object] = {}

    def fake_run_default_watchlist_scan(**kwargs):
        scan_called.update(kwargs)
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "min_score": kwargs["min_score"],
            "persisted_events": [],
            "notification_events": [],
            "delivery_results": [],
            "signal_summary": {},
            "scan_run": {"id": 4, "run_at": "2026-05-13 12:00:00", "status": "无信号", "note": "没有新信号"},
            "errors": [],
        }

    def fake_backfill_review_snapshots(**kwargs):
        review_called["backfill"] = kwargs
        return {"count": 2, "errors": []}

    def fake_summarize_review_stats(**kwargs):
        review_called["stats"] = kwargs
        return [
            {
                "horizon": "T+3",
                "sample_count": 6,
                "avg_return": 3.2,
                "win_rate": 0.66,
                "strategy_verdict": "保留",
                "strategy_confidence": "中",
                "strategy_actionable": True,
            }
        ]

    monkeypatch.setattr(module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)
    monkeypatch.setattr(module.review_service, "backfill_review_snapshots", fake_backfill_review_snapshots)
    monkeypatch.setattr(module.review_service, "summarize_review_stats", fake_summarize_review_stats)
    monkeypatch.setattr(
        module.scan_run_service,
        "update_scan_run_review",
        lambda scan_run_id, **kwargs: {
            "id": scan_run_id,
            "review_after_scan": kwargs["review_after_scan"],
            "review_snapshot_count": kwargs["review_snapshot_count"],
            "review_stats_count": kwargs["review_stats_count"],
            "review_error": kwargs["review_error"],
        },
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(DAILY_SCAN_SCRIPT),
            "--review-after-scan",
            "--review-trade-date",
            "2026-05-01",
            "--review-horizons",
            "1,3",
            "--review-summary-horizon",
            "T+3",
        ],
    )

    result = module.main()
    captured = capsys.readouterr()

    assert result == 0
    assert scan_called["min_score"] == 60.0
    assert review_called["backfill"]["trade_date"] == "2026-05-01"
    assert review_called["backfill"]["horizons"] == [1, 3]
    assert review_called["backfill"]["due_only"] is True
    assert review_called["stats"]["horizon"] == "T+3"
    assert "review_snapshots=2" in captured.out
    assert "scan_run_review enabled=True due_only=True snapshots=2 stats=1 error=" in captured.out
    assert "review_summary" in captured.out
    assert "confidence=中" in captured.out
    assert "actionable=True" in captured.out
