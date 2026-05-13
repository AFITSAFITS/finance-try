from __future__ import annotations

from app import strategy_guard_service


def test_annotate_signal_events_with_strategy_decisions(monkeypatch) -> None:
    def fake_summarize_review_stats(horizon: str):
        assert horizon == "T+3"
        return [
            {
                "score_bucket": "60-80",
                "signal_direction": "偏多",
                "observation_conclusion": "谨慎观察",
                "data_freshness": "最近交易日",
                "data_source": "旧缓存兜底",
                "risk_bucket": "有风险提示",
                "risk_plan_bucket": "5-8%",
                "summary": "MACD金叉",
                "indicator": "MACD",
                "event_type": "golden_cross",
                "horizon": "T+3",
                "sample_count": 6,
                "avg_return": 3.2,
                "win_rate": 0.6667,
                "avg_max_drawdown": -2.1,
                "strategy_verdict": "保留",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "strategy_next_action": "保留该分组，继续跟踪表现",
                "strategy_note": "表现可接受",
                "samples_to_actionable": 0,
            }
        ]

    monkeypatch.setattr(strategy_guard_service.review_service, "summarize_review_stats", fake_summarize_review_stats)

    annotated, summary = strategy_guard_service.annotate_signal_events_with_strategy_decisions(
        [
            {
                "id": 1,
                "trade_date": "2026-05-13",
                "code": "600519",
                "summary": "MACD金叉",
                "indicator": "MACD",
                "event_type": "golden_cross",
                "close_price": 10.0,
                "payload": {
                    "signal_score": 75,
                    "signal_direction": "偏多",
                    "observation_conclusion": "谨慎观察",
                    "data_freshness": "最近交易日",
                    "data_source": "旧缓存兜底",
                    "risk_note": "接近60日高位",
                    "stop_loss_price": 9.2,
                },
            }
        ],
        horizon="T+3",
    )

    assert summary == {"enabled": True, "horizon": "T+3", "matched_count": 1, "total_count": 1}
    payload = annotated[0]["payload"]
    assert payload["strategy_verdict"] == "保留"
    assert payload["strategy_confidence"] == "中"
    assert payload["strategy_actionable"] is True
    assert payload["strategy_sample_count"] == 6
    assert payload["strategy_next_action"] == "保留该分组，继续跟踪表现"


def test_annotate_signal_events_keeps_unmatched_events(monkeypatch) -> None:
    monkeypatch.setattr(strategy_guard_service.review_service, "summarize_review_stats", lambda horizon: [])

    annotated, summary = strategy_guard_service.annotate_signal_events_with_strategy_decisions(
        [{"id": 1, "payload": {"signal_score": 90}}],
        horizon="T+1",
    )

    assert summary == {"enabled": True, "horizon": "T+1", "matched_count": 0, "total_count": 1}
    assert annotated == [{"id": 1, "payload": {"signal_score": 90}}]
