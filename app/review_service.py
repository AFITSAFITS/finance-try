from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Callable, Iterable

import pandas as pd

from app import bar_service
from app import db
from app import signal_service
from app import tdx_service

DEFAULT_HORIZONS = (1, 3, 5)


def horizon_label(days: int) -> str:
    return f"T+{int(days)}"


def parse_horizons(values: Iterable[int] | None) -> list[int]:
    raw_values = list(values or DEFAULT_HORIZONS)
    normalized = sorted({int(value) for value in raw_values if int(value) > 0})
    return normalized or list(DEFAULT_HORIZONS)


def _load_signal_events(
    trade_date: str | None = None,
    code: str | None = None,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append("trade_date = ?")
        params.append(trade_date)
    if code:
        clauses.append("code = ?")
        params.append(tdx_service.format_code(code))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, trade_date, code, indicator, event_type, summary,
                   close_price, pct_change
            FROM signal_events
            {where_sql}
            ORDER BY trade_date ASC, code ASC, id ASC
            """,
            tuple(params),
        ).fetchall()
    return [dict(row) for row in rows]


def _row_to_snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "signal_event_id": row["signal_event_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "indicator": row["indicator"],
        "event_type": row["event_type"],
        "summary": row["summary"],
        "horizon": row["horizon"],
        "future_trade_date": row["future_trade_date"],
        "future_close_price": row["future_close_price"],
        "pct_return": row["pct_return"],
        "max_drawdown": row["max_drawdown"],
        "updated_at": row["updated_at"],
    }


def list_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizon: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append("e.trade_date = ?")
        params.append(trade_date)
    if code:
        clauses.append("e.code = ?")
        params.append(tdx_service.format_code(code))
    if horizon:
        clauses.append("r.horizon = ?")
        params.append(horizon)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT r.id, r.signal_event_id, e.trade_date, e.code, e.indicator, e.event_type, e.summary,
                   r.horizon, r.future_trade_date, r.future_close_price, r.pct_return,
                   r.max_drawdown, r.updated_at
            FROM review_snapshots r
            JOIN signal_events e ON e.id = r.signal_event_id
            {where_sql}
            ORDER BY e.trade_date DESC, e.code ASC, r.horizon ASC, r.id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_snapshot(dict(row)) for row in rows]


def backfill_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizons: Iterable[int] | None = None,
    adjust: str = "qfq",
    fetcher: Callable[[str, str, str, str], pd.DataFrame] = bar_service.fetch_daily_history_range_akshare,
) -> dict[str, Any]:
    selected_horizons = parse_horizons(horizons)
    events = _load_signal_events(trade_date=trade_date, code=code)
    if not events:
        return {"count": 0, "items": [], "errors": []}

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[str(event["code"])].append(event)

    snapshots: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []
    end_date = datetime.now().strftime("%Y-%m-%d")

    with db.get_connection() as conn:
        for stock_code, code_events in grouped.items():
            start_date = min(str(event["trade_date"]) for event in code_events)
            try:
                history_df = fetcher(stock_code, start_date, end_date, adjust)
                normalized = signal_service.normalize_history_df(history_df, stock_code)
                if normalized.empty:
                    continue

                bar_service.upsert_daily_bars(stock_code, normalized, adjust=adjust)
                indexed = normalized.copy()
                indexed["日期字符串"] = indexed["日期"].map(signal_service.format_trade_date)
                date_to_index = {
                    str(row["日期字符串"]): idx
                    for idx, row in indexed.iterrows()
                }

                for event in code_events:
                    event_date = str(event["trade_date"])
                    base_index = date_to_index.get(event_date)
                    if base_index is None:
                        continue

                    base_close = float(event["close_price"]) if event.get("close_price") is not None else float(indexed.iloc[base_index]["收盘"])
                    for horizon_days in selected_horizons:
                        future_index = base_index + int(horizon_days)
                        if future_index >= len(indexed.index):
                            continue

                        future_row = indexed.iloc[future_index]
                        window = indexed.iloc[base_index + 1 : future_index + 1]
                        if window.empty:
                            continue

                        future_close = float(future_row["收盘"])
                        pct_return = round(((future_close / base_close) - 1.0) * 100.0, 4)
                        max_drawdown = round((((window["收盘"] / base_close) - 1.0).min()) * 100.0, 4)
                        label = horizon_label(horizon_days)
                        now = tdx_service.now_ts()
                        conn.execute(
                            """
                            INSERT INTO review_snapshots (
                                signal_event_id, horizon, future_trade_date, future_close_price,
                                pct_return, max_drawdown, updated_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(signal_event_id, horizon)
                            DO UPDATE SET
                                future_trade_date=excluded.future_trade_date,
                                future_close_price=excluded.future_close_price,
                                pct_return=excluded.pct_return,
                                max_drawdown=excluded.max_drawdown,
                                updated_at=excluded.updated_at
                            """,
                            (
                                int(event["id"]),
                                label,
                                signal_service.format_trade_date(future_row["日期"]),
                                future_close,
                                pct_return,
                                max_drawdown,
                                now,
                            ),
                        )
                        saved = conn.execute(
                            """
                            SELECT r.id, r.signal_event_id, e.trade_date, e.code, e.indicator, e.event_type, e.summary,
                                   r.horizon, r.future_trade_date, r.future_close_price, r.pct_return,
                                   r.max_drawdown, r.updated_at
                            FROM review_snapshots r
                            JOIN signal_events e ON e.id = r.signal_event_id
                            WHERE r.signal_event_id = ? AND r.horizon = ?
                            """,
                            (int(event["id"]), label),
                        ).fetchone()
                        assert saved is not None
                        snapshots.append(_row_to_snapshot(dict(saved)))
            except Exception as exc:  # noqa: BLE001
                errors.append({"股票代码": stock_code, "error": str(exc)})

    snapshots.sort(key=lambda item: (item["trade_date"], item["code"], item["summary"], item["horizon"]))
    return {
        "count": len(snapshots),
        "items": snapshots,
        "errors": errors,
    }


def summarize_review_stats(
    horizon: str = "T+3",
    trade_date: str | None = None,
    code: str | None = None,
) -> list[dict[str, Any]]:
    snapshots = list_review_snapshots(
        trade_date=trade_date,
        code=code,
        horizon=horizon,
        limit=5000,
    )
    if not snapshots:
        return []

    df = pd.DataFrame(snapshots)
    grouped = (
        df.groupby(["summary", "indicator", "event_type"], dropna=False)
        .agg(
            sample_count=("pct_return", "count"),
            avg_return=("pct_return", "mean"),
            win_rate=("pct_return", lambda s: float((s > 0).mean())),
            avg_max_drawdown=("max_drawdown", "mean"),
        )
        .reset_index()
    )

    items: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        items.append(
            {
                "summary": row["summary"],
                "indicator": row["indicator"],
                "event_type": row["event_type"],
                "sample_count": int(row["sample_count"]),
                "avg_return": round(float(row["avg_return"]), 4),
                "win_rate": round(float(row["win_rate"]), 4),
                "avg_max_drawdown": round(float(row["avg_max_drawdown"]), 4),
                "horizon": horizon,
            }
        )
    items.sort(key=lambda item: (-item["sample_count"], item["summary"]))
    return items
