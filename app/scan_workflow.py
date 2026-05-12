from __future__ import annotations

import time
from typing import Any

from app import bar_service
from app import event_service
from app import notification_service
from app import signal_service
from app import watchlist_service


def run_default_watchlist_scan(
    lookback_days: int = 180,
    adjust: str = "qfq",
    channel: str = "stdout",
    max_workers: int = 8,
    bootstrap_if_empty: bool = True,
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
    )
    persisted_events = event_service.persist_signal_rows(signal_rows)
    delivery_results = notification_service.deliver_signal_events(
        persisted_events,
        channel=channel,
    )
    return {
        "watchlist": watchlist,
        "requested_count": len(codes),
        "elapsed_seconds": round(time.perf_counter() - started_at, 3),
        "persisted_events": persisted_events,
        "delivery_results": delivery_results,
        "errors": errors,
        "watchlist_source": watchlist.get("source", "existing"),
        "watchlist_message": watchlist.get("message", ""),
        "watchlist_warning": watchlist.get("warning", ""),
    }
