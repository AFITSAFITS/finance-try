from __future__ import annotations

from typing import Any

from app import limit_up_service
from app import review_decision
from app import review_service

VERDICT_PRIORITY = {
    "保留": 0,
    "降权": 1,
    "继续观察": 2,
    "样本不足": 3,
}
CONFIDENCE_PRIORITY = {
    "高": 0,
    "中": 1,
    "低": 2,
}


def _signal_strategy_name(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("score_bucket", "") or "").strip(),
        str(item.get("signal_direction", "") or "").strip(),
        str(item.get("observation_conclusion", "") or "").strip(),
        str(item.get("summary", "") or "").strip(),
    ]
    return " / ".join(part for part in parts if part)


def _limit_up_strategy_name(item: dict[str, Any]) -> str:
    parts = [
        str(item.get("score_bucket", "") or "").strip(),
        str(item.get("data_source", "") or "").strip(),
    ]
    return " / ".join(part for part in parts if part)


def _normalize_signal_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_type": "日线信号",
        "strategy_name": _signal_strategy_name(item),
        "horizon": item.get("horizon", ""),
        "data_source": item.get("data_source", ""),
        "sample_count": int(item.get("sample_count", 0) or 0),
        "avg_return": item.get("avg_return"),
        "win_rate": item.get("win_rate"),
        "avg_max_drawdown": item.get("avg_max_drawdown"),
        "strategy_verdict": item.get("strategy_verdict", ""),
        "strategy_confidence": item.get("strategy_confidence", ""),
        "strategy_actionable": bool(item.get("strategy_actionable", False)),
        "min_actionable_samples": item.get("min_actionable_samples", review_decision.MIN_ACTIONABLE_SAMPLES),
        "samples_to_actionable": item.get(
            "samples_to_actionable",
            max(0, review_decision.MIN_ACTIONABLE_SAMPLES - int(item.get("sample_count", 0) or 0)),
        ),
        "strategy_note": item.get("strategy_note", ""),
    }


def _normalize_limit_up_item(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_type": "涨停策略",
        "strategy_name": _limit_up_strategy_name(item),
        "horizon": item.get("horizon", ""),
        "data_source": item.get("data_source", ""),
        "sample_count": int(item.get("sample_count", 0) or 0),
        "avg_return": item.get("avg_return"),
        "win_rate": item.get("win_rate"),
        "avg_max_drawdown": item.get("avg_max_drawdown"),
        "strategy_verdict": item.get("strategy_verdict", ""),
        "strategy_confidence": item.get("strategy_confidence", ""),
        "strategy_actionable": bool(item.get("strategy_actionable", False)),
        "min_actionable_samples": item.get("min_actionable_samples", review_decision.MIN_ACTIONABLE_SAMPLES),
        "samples_to_actionable": item.get(
            "samples_to_actionable",
            max(0, review_decision.MIN_ACTIONABLE_SAMPLES - int(item.get("sample_count", 0) or 0)),
        ),
        "strategy_note": item.get("strategy_note", ""),
    }


def _sort_key(item: dict[str, Any]) -> tuple[object, ...]:
    return (
        not item["strategy_actionable"],
        VERDICT_PRIORITY.get(str(item.get("strategy_verdict", "")), 9),
        CONFIDENCE_PRIORITY.get(str(item.get("strategy_confidence", "")), 9),
        -int(item.get("sample_count", 0) or 0),
        str(item.get("strategy_type", "")),
        str(item.get("strategy_name", "")),
    )


def _count_by(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = str(item.get(key, "") or "未标记")
        counts[value] = counts.get(value, 0) + 1
    return counts


def summarize_strategy_decisions(
    horizon: str = "T+3",
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 50,
    min_samples: int = 1,
    actionable_only: bool = False,
    data_source: str | None = None,
) -> dict[str, Any]:
    signal_items = [
        _normalize_signal_item(item)
        for item in review_service.summarize_review_stats(
            horizon=horizon,
            trade_date=trade_date,
            code=code,
        )
    ]
    limit_up_items = [
        _normalize_limit_up_item(item)
        for item in limit_up_service.summarize_limit_up_review_stats(
            horizon=horizon,
            trade_date=trade_date,
            code=code,
        )
    ]
    min_samples = max(1, int(min_samples))
    all_items = [*signal_items, *limit_up_items]
    actionable_count = sum(1 for item in all_items if item["strategy_actionable"])
    items = [item for item in all_items if int(item.get("sample_count", 0) or 0) >= min_samples]
    data_source_filter = str(data_source or "").strip()
    if data_source_filter:
        items = [item for item in items if str(item.get("data_source", "") or "") == data_source_filter]
    if actionable_only:
        items = [item for item in items if item["strategy_actionable"]]
    items.sort(key=_sort_key)
    limited = items[: max(1, int(limit))]
    return {
        "horizon": horizon,
        "total_count": len(all_items),
        "filtered_count": len(items),
        "actionable_count": actionable_count,
        "min_samples": min_samples,
        "actionable_only": bool(actionable_only),
        "data_source": data_source_filter,
        "verdict_counts": _count_by(items, "strategy_verdict"),
        "confidence_counts": _count_by(items, "strategy_confidence"),
        "items": limited,
    }
