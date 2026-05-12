from __future__ import annotations

import time
from typing import Any

from app import bar_service
from app import event_service
from app import notification_service
from app import signal_service
from app import watchlist_service


def _event_priority(event: dict[str, Any]) -> tuple[float, float, int]:
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    severity_score = 1.0 if str(event.get("severity", "")).lower() in {"high", "critical"} else 0.0
    try:
        signal_score = float(payload.get("signal_score") or 0)
    except (TypeError, ValueError):
        signal_score = 0.0
    event_type_rank = {
        "secondary_golden_cross_above_zero": 5,
        "golden_cross": 4,
        "ma5_cross_up_ma20": 3,
        "death_cross": 2,
        "ma5_cross_down_ma20": 1,
    }.get(str(event.get("event_type", "")), 0)
    return severity_score, signal_score, event_type_rank


def select_representative_notification_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[tuple[str, str], dict[str, Any]] = {}
    order: list[tuple[str, str]] = []
    for event in events:
        key = (str(event.get("trade_date", "")), str(event.get("code", "")))
        if key not in selected:
            selected[key] = event
            order.append(key)
            continue
        if _event_priority(event) > _event_priority(selected[key]):
            selected[key] = event
    return [selected[key] for key in order]


def run_default_watchlist_scan(
    lookback_days: int = 180,
    adjust: str = "qfq",
    channel: str = "stdout",
    max_workers: int = 8,
    bootstrap_if_empty: bool = True,
    min_score: float | None = 60.0,
) -> dict[str, Any]:
    if bootstrap_if_empty:
        watchlist = watchlist_service.ensure_default_watchlist()
    else:
        watchlist = watchlist_service.get_default_watchlist()
    codes = [str(item["code"]) for item in watchlist["items"]]
    if not codes:
        raise ValueError("默认股票池为空，请先保存股票代码")

    started_at = time.perf_counter()
    signal_rows, errors = signal_service.scan_stock_signal_events(
        codes=codes,
        lookback_days=int(lookback_days),
        adjust=adjust.strip(),
        fetcher=bar_service.fetch_daily_history_cached,
        max_workers=int(max_workers),
        min_score=min_score,
    )
    persisted_events = event_service.persist_signal_rows(signal_rows)
    notification_events = select_representative_notification_events(persisted_events)
    delivery_results = notification_service.deliver_signal_events(
        notification_events,
        channel=channel,
    )
    return {
        "watchlist": watchlist,
        "requested_count": len(codes),
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "min_score": min_score,
        "persisted_events": persisted_events,
        "notification_events": notification_events,
        "delivery_results": delivery_results,
        "errors": errors,
        "watchlist_source": watchlist.get("source", "existing"),
        "watchlist_message": watchlist.get("message", ""),
        "watchlist_warning": watchlist.get("warning", ""),
    }
