from __future__ import annotations

from app import review_decision


def test_review_decision_marks_low_confidence_when_samples_are_not_actionable() -> None:
    decision = review_decision.build_review_decision(
        sample_count=4,
        avg_return=8.0,
        win_rate=0.8,
        avg_max_drawdown=-2.0,
    )

    assert decision["strategy_verdict"] == "样本不足"
    assert decision["strategy_confidence"] == "低"
    assert decision["strategy_actionable"] is False


def test_review_decision_marks_mid_and_high_confidence_by_sample_count() -> None:
    mid = review_decision.build_review_decision(
        sample_count=5,
        avg_return=8.0,
        win_rate=0.8,
        avg_max_drawdown=-2.0,
    )
    high = review_decision.build_review_decision(
        sample_count=20,
        avg_return=8.0,
        win_rate=0.8,
        avg_max_drawdown=-2.0,
    )

    assert mid["strategy_verdict"] == "保留"
    assert mid["strategy_confidence"] == "中"
    assert mid["strategy_actionable"] is True
    assert high["strategy_verdict"] == "保留"
    assert high["strategy_confidence"] == "高"
    assert high["strategy_actionable"] is True
