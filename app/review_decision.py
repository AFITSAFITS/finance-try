from __future__ import annotations


MIN_ACTIONABLE_SAMPLES = 5
HIGH_CONFIDENCE_SAMPLES = 20


def build_sample_confidence(sample_count: int) -> dict[str, object]:
    samples_to_actionable = max(0, MIN_ACTIONABLE_SAMPLES - int(sample_count))
    if sample_count < MIN_ACTIONABLE_SAMPLES:
        return {
            "strategy_confidence": "低",
            "strategy_actionable": False,
            "min_actionable_samples": MIN_ACTIONABLE_SAMPLES,
            "samples_to_actionable": samples_to_actionable,
        }
    if sample_count < HIGH_CONFIDENCE_SAMPLES:
        return {
            "strategy_confidence": "中",
            "strategy_actionable": True,
            "min_actionable_samples": MIN_ACTIONABLE_SAMPLES,
            "samples_to_actionable": samples_to_actionable,
        }
    return {
        "strategy_confidence": "高",
        "strategy_actionable": True,
        "min_actionable_samples": MIN_ACTIONABLE_SAMPLES,
        "samples_to_actionable": samples_to_actionable,
    }


def build_review_decision(
    sample_count: int,
    avg_return: float,
    win_rate: float,
    avg_max_drawdown: float,
) -> dict[str, object]:
    confidence = build_sample_confidence(sample_count)
    if sample_count < MIN_ACTIONABLE_SAMPLES:
        return {
            "strategy_verdict": "样本不足",
            "strategy_note": f"样本数少于{MIN_ACTIONABLE_SAMPLES}，先继续积累",
            **confidence,
        }

    if avg_return > 0 and win_rate >= 0.55 and avg_max_drawdown > -8:
        return {
            "strategy_verdict": "保留",
            "strategy_note": "收益、胜率和回撤都处于可接受区间",
            **confidence,
        }

    if avg_return < 0 or win_rate < 0.45 or avg_max_drawdown <= -10:
        return {
            "strategy_verdict": "降权",
            "strategy_note": "收益、胜率或回撤有明显拖累",
            **confidence,
        }

    return {
        "strategy_verdict": "继续观察",
        "strategy_note": "表现尚未稳定，需要更多样本确认",
        **confidence,
    }
