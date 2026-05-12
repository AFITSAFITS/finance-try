from __future__ import annotations


MIN_ACTIONABLE_SAMPLES = 5


def build_review_decision(
    sample_count: int,
    avg_return: float,
    win_rate: float,
    avg_max_drawdown: float,
) -> dict[str, str]:
    if sample_count < MIN_ACTIONABLE_SAMPLES:
        return {
            "strategy_verdict": "样本不足",
            "strategy_note": f"样本数少于{MIN_ACTIONABLE_SAMPLES}，先继续积累",
        }

    if avg_return > 0 and win_rate >= 0.55 and avg_max_drawdown > -8:
        return {
            "strategy_verdict": "保留",
            "strategy_note": "收益、胜率和回撤都处于可接受区间",
        }

    if avg_return < 0 or win_rate < 0.45 or avg_max_drawdown <= -10:
        return {
            "strategy_verdict": "降权",
            "strategy_note": "收益、胜率或回撤有明显拖累",
        }

    return {
        "strategy_verdict": "继续观察",
        "strategy_note": "表现尚未稳定，需要更多样本确认",
    }
