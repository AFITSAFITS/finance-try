from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from app import db
from app import limit_up_service
from app import review_decision
from app import review_service
from app import tdx_service

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


def _horizon_days(horizon: str) -> int:
    raw_value = str(horizon or "").strip().upper()
    if raw_value.startswith("T+"):
        raw_value = raw_value[2:]
    try:
        return max(1, int(raw_value))
    except ValueError:
        return review_service.DEFAULT_HORIZONS[1]


def _review_due_cutoff(horizon: str) -> str:
    return (datetime.now() - timedelta(days=_horizon_days(horizon))).strftime("%Y-%m-%d")


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
        "strategy_next_action": item.get("strategy_next_action", ""),
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
        "strategy_next_action": item.get("strategy_next_action", ""),
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


def _build_sample_gap_summary(items: list[dict[str, Any]], limit: int = 3) -> dict[str, Any]:
    gap_items = [item for item in items if int(item.get("samples_to_actionable", 0) or 0) > 0]
    gap_items.sort(
        key=lambda item: (
            int(item.get("samples_to_actionable", 0) or 0),
            -int(item.get("sample_count", 0) or 0),
            str(item.get("strategy_type", "")),
            str(item.get("strategy_name", "")),
        )
    )
    nearest = [
        {
            "strategy_type": item.get("strategy_type", ""),
            "strategy_name": item.get("strategy_name", ""),
            "horizon": item.get("horizon", ""),
            "data_source": item.get("data_source", ""),
            "sample_count": int(item.get("sample_count", 0) or 0),
            "samples_to_actionable": int(item.get("samples_to_actionable", 0) or 0),
            "strategy_next_action": item.get("strategy_next_action", ""),
        }
        for item in gap_items[: max(1, int(limit))]
    ]
    return {
        "needs_more_samples_count": len(gap_items),
        "total_samples_to_actionable": sum(int(item.get("samples_to_actionable", 0) or 0) for item in gap_items),
        "nearest_to_actionable": nearest,
    }


def _build_where(
    date_column: str,
    code_column: str,
    trade_date: str | None = None,
    code: str | None = None,
    normalize_date: bool = False,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append(f"{date_column} = ?")
        params.append(limit_up_service.normalize_trade_date(trade_date) if normalize_date else trade_date)
    if code:
        clauses.append(f"{code_column} = ?")
        params.append(tdx_service.format_code(code))
    return (f"WHERE {' AND '.join(clauses)}" if clauses else "", params)


def _summarize_signal_review_backlog(
    horizon: str,
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    due_cutoff = _review_due_cutoff(horizon)
    where_sql, params = _build_where("e.trade_date", "e.code", trade_date=trade_date, code=code)
    join_sql = "LEFT JOIN review_snapshots r ON r.signal_event_id = e.id AND r.horizon = ?"
    with db.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS total_count,
                   COUNT(r.id) AS reviewed_count,
                   SUM(CASE WHEN r.id IS NULL THEN 1 ELSE 0 END) AS missing_count,
                   SUM(CASE WHEN r.id IS NULL AND e.trade_date <= ? THEN 1 ELSE 0 END) AS due_missing_count,
                   SUM(CASE WHEN r.id IS NULL AND e.trade_date > ? THEN 1 ELSE 0 END) AS not_due_count
            FROM signal_events e
            {join_sql}
            {where_sql}
            """,
            (due_cutoff, due_cutoff, horizon, *params),
        ).fetchone()
        missing_rows = conn.execute(
            f"""
            SELECT e.trade_date, e.code, e.summary
            FROM signal_events e
            {join_sql}
            {where_sql}
              {"AND" if where_sql else "WHERE"} r.id IS NULL
            ORDER BY e.trade_date DESC, e.code ASC, e.id DESC
            LIMIT ?
            """,
            (horizon, *params, max(1, int(limit))),
        ).fetchall()
    total = int(row["total_count"] or 0) if row else 0
    reviewed = int(row["reviewed_count"] or 0) if row else 0
    missing = int(row["missing_count"] or 0) if row else 0
    due_missing = int(row["due_missing_count"] or 0) if row else 0
    not_due = int(row["not_due_count"] or 0) if row else 0
    return {
        "total_count": total,
        "reviewed_count": reviewed,
        "missing_count": missing,
        "due_missing_count": due_missing,
        "not_due_count": not_due,
        "due_cutoff": due_cutoff,
        "reviewed_ratio": round(reviewed / total, 4) if total else None,
        "latest_missing": [dict(item) for item in missing_rows],
    }


def _summarize_limit_up_review_backlog(
    horizon: str,
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    due_cutoff = _review_due_cutoff(horizon)
    where_sql, params = _build_where(
        "c.trade_date",
        "c.code",
        trade_date=trade_date,
        code=code,
        normalize_date=True,
    )
    join_sql = "LEFT JOIN limit_up_review_snapshots r ON r.limit_up_candidate_id = c.id AND r.horizon = ?"
    with db.get_connection() as conn:
        row = conn.execute(
            f"""
            SELECT COUNT(*) AS total_count,
                   COUNT(r.id) AS reviewed_count,
                   SUM(CASE WHEN r.id IS NULL THEN 1 ELSE 0 END) AS missing_count,
                   SUM(CASE WHEN r.id IS NULL AND c.trade_date <= ? THEN 1 ELSE 0 END) AS due_missing_count,
                   SUM(CASE WHEN r.id IS NULL AND c.trade_date > ? THEN 1 ELSE 0 END) AS not_due_count
            FROM limit_up_candidates c
            {join_sql}
            {where_sql}
            """,
            (due_cutoff, due_cutoff, horizon, *params),
        ).fetchone()
        missing_rows = conn.execute(
            f"""
            SELECT c.trade_date, c.code, c.name, c.score
            FROM limit_up_candidates c
            {join_sql}
            {where_sql}
              {"AND" if where_sql else "WHERE"} r.id IS NULL
            ORDER BY c.trade_date DESC, c.score DESC, c.code ASC, c.id DESC
            LIMIT ?
            """,
            (horizon, *params, max(1, int(limit))),
        ).fetchall()
    total = int(row["total_count"] or 0) if row else 0
    reviewed = int(row["reviewed_count"] or 0) if row else 0
    missing = int(row["missing_count"] or 0) if row else 0
    due_missing = int(row["due_missing_count"] or 0) if row else 0
    not_due = int(row["not_due_count"] or 0) if row else 0
    return {
        "total_count": total,
        "reviewed_count": reviewed,
        "missing_count": missing,
        "due_missing_count": due_missing,
        "not_due_count": not_due,
        "due_cutoff": due_cutoff,
        "reviewed_ratio": round(reviewed / total, 4) if total else None,
        "latest_missing": [dict(item) for item in missing_rows],
    }


def summarize_review_backlog(
    horizon: str = "T+3",
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    signal_backlog = _summarize_signal_review_backlog(horizon, trade_date=trade_date, code=code, limit=limit)
    limit_up_backlog = _summarize_limit_up_review_backlog(horizon, trade_date=trade_date, code=code, limit=limit)
    total_count = int(signal_backlog["total_count"]) + int(limit_up_backlog["total_count"])
    reviewed_count = int(signal_backlog["reviewed_count"]) + int(limit_up_backlog["reviewed_count"])
    missing_count = int(signal_backlog["missing_count"]) + int(limit_up_backlog["missing_count"])
    due_missing_count = int(signal_backlog["due_missing_count"]) + int(limit_up_backlog["due_missing_count"])
    not_due_count = int(signal_backlog["not_due_count"]) + int(limit_up_backlog["not_due_count"])
    return {
        "horizon": horizon,
        "total_count": total_count,
        "reviewed_count": reviewed_count,
        "missing_count": missing_count,
        "due_missing_count": due_missing_count,
        "not_due_count": not_due_count,
        "due_cutoff": _review_due_cutoff(horizon),
        "reviewed_ratio": round(reviewed_count / total_count, 4) if total_count else None,
        "signals": signal_backlog,
        "limit_up": limit_up_backlog,
    }


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
    filtered_actionable_count = sum(1 for item in items if item["strategy_actionable"])
    items.sort(key=_sort_key)
    limited = items[: max(1, int(limit))]
    return {
        "horizon": horizon,
        "total_count": len(all_items),
        "filtered_count": len(items),
        "actionable_count": actionable_count,
        "filtered_actionable_count": filtered_actionable_count,
        "min_samples": min_samples,
        "actionable_only": bool(actionable_only),
        "data_source": data_source_filter,
        "verdict_counts": _count_by(items, "strategy_verdict"),
        "confidence_counts": _count_by(items, "strategy_confidence"),
        "strategy_type_counts": _count_by(items, "strategy_type"),
        "data_source_counts": _count_by(items, "data_source"),
        "next_action_counts": _count_by(items, "strategy_next_action"),
        "sample_gap_summary": _build_sample_gap_summary(items),
        "review_backlog": summarize_review_backlog(
            horizon=horizon,
            trade_date=trade_date,
            code=code,
        ),
        "items": limited,
    }
