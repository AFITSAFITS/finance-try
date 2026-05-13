from __future__ import annotations

from typing import Any, Iterable

import pandas as pd

from app import review_service


def _score_bucket(value: object) -> str:
    if value is None or pd.isna(value):
        return "未评分"
    score = float(value)
    if score <= 40:
        return "0-40"
    if score <= 60:
        return "40-60"
    if score <= 80:
        return "60-80"
    return "80+"


def _risk_bucket(value: object) -> str:
    raw_value = str(value or "").strip()
    if not raw_value or raw_value in {"-", "无明显风险", "None", "nan"}:
        return "无明显风险"
    return "有风险提示"


def _stop_distance_bucket(close_price: object, stop_loss_price: object) -> str:
    if close_price is None or stop_loss_price is None:
        return "无风险计划"
    try:
        close = float(close_price)
        stop = float(stop_loss_price)
    except (TypeError, ValueError):
        return "无风险计划"
    if close <= 0 or stop <= 0 or stop >= close:
        return "无风险计划"
    distance = round((close - stop) / close * 100, 4)
    if distance <= 5:
        return "0-5%"
    if distance <= 8:
        return "5-8%"
    return "8%+"


def _clean_text(value: object, default: str) -> str:
    raw = str(value or "").strip()
    return raw if raw and raw.lower() != "nan" else default


def _event_key(event: dict[str, Any]) -> tuple[str, ...]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    return (
        _score_bucket(payload.get("signal_score")),
        _clean_text(payload.get("signal_direction"), "未知"),
        _clean_text(payload.get("observation_conclusion"), "未标记"),
        _clean_text(payload.get("data_freshness"), "未知"),
        _clean_text(payload.get("data_source"), "未知"),
        _risk_bucket(payload.get("risk_note")),
        _stop_distance_bucket(event.get("close_price"), payload.get("stop_loss_price")),
        _clean_text(event.get("summary"), ""),
        _clean_text(event.get("indicator"), ""),
        _clean_text(event.get("event_type"), ""),
    )


def _stats_key(item: dict[str, Any]) -> tuple[str, ...]:
    return (
        _clean_text(item.get("score_bucket"), "未评分"),
        _clean_text(item.get("signal_direction"), "未知"),
        _clean_text(item.get("observation_conclusion"), "未标记"),
        _clean_text(item.get("data_freshness"), "未知"),
        _clean_text(item.get("data_source"), "未知"),
        _clean_text(item.get("risk_bucket"), "无明显风险"),
        _clean_text(item.get("risk_plan_bucket"), "无风险计划"),
        _clean_text(item.get("summary"), ""),
        _clean_text(item.get("indicator"), ""),
        _clean_text(item.get("event_type"), ""),
    )


def _decision_payload(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "strategy_horizon": item.get("horizon", ""),
        "strategy_verdict": item.get("strategy_verdict", ""),
        "strategy_confidence": item.get("strategy_confidence", ""),
        "strategy_actionable": bool(item.get("strategy_actionable", False)),
        "strategy_next_action": item.get("strategy_next_action", ""),
        "strategy_note": item.get("strategy_note", ""),
        "strategy_sample_count": int(item.get("sample_count", 0) or 0),
        "strategy_avg_return": item.get("avg_return"),
        "strategy_win_rate": item.get("win_rate"),
        "strategy_avg_max_drawdown": item.get("avg_max_drawdown"),
        "strategy_samples_to_actionable": int(item.get("samples_to_actionable", 0) or 0),
    }


def annotate_signal_events_with_strategy_decisions(
    events: Iterable[dict[str, Any]],
    horizon: str = "T+3",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    event_list = [dict(event) for event in events]
    if not event_list:
        return [], {"enabled": True, "horizon": horizon, "matched_count": 0, "total_count": 0}

    stats = review_service.summarize_review_stats(horizon=horizon)
    stats_index = {_stats_key(item): item for item in stats}
    matched_count = 0
    annotated: list[dict[str, Any]] = []
    for event in event_list:
        copied = dict(event)
        payload = dict(copied.get("payload") or {})
        decision = stats_index.get(_event_key(copied))
        if decision:
            payload.update(_decision_payload(decision))
            copied["payload"] = payload
            matched_count += 1
        annotated.append(copied)

    return annotated, {
        "enabled": True,
        "horizon": horizon,
        "matched_count": matched_count,
        "total_count": len(event_list),
    }
