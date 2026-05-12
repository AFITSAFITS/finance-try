from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from app import db
from app import signal_service
from app import tdx_service


def fetch_daily_history_range_akshare(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
) -> pd.DataFrame:
    begin = pd.to_datetime(start_date).strftime("%Y%m%d")
    finish = pd.to_datetime(end_date).strftime("%Y%m%d")
    return signal_service.fetch_daily_history_best_effort(
        code=code,
        start_date=begin,
        end_date=finish,
        adjust=adjust,
    )


def _to_number(row: pd.Series, column: str) -> float | None:
    value = row.get(column)
    if value is None or pd.isna(value):
        return None
    return float(value)


def upsert_daily_bars(
    code: str,
    history_df: pd.DataFrame,
    adjust: str = "qfq",
    source: str = "akshare",
) -> int:
    normalized = signal_service.normalize_history_df(history_df, code)
    if normalized.empty:
        return 0

    fetched_at = tdx_service.now_ts()
    count = 0
    with db.get_connection() as conn:
        for _, row in normalized.iterrows():
            conn.execute(
                """
                INSERT INTO daily_bars (
                    code, trade_date, open, close, high, low, volume, amount,
                    pct_change, turnover_rate, adjust, source, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date, adjust, source)
                DO UPDATE SET
                    open=excluded.open,
                    close=excluded.close,
                    high=excluded.high,
                    low=excluded.low,
                    volume=excluded.volume,
                    amount=excluded.amount,
                    pct_change=excluded.pct_change,
                    turnover_rate=excluded.turnover_rate,
                    fetched_at=excluded.fetched_at
                """,
                (
                    tdx_service.format_code(code),
                    signal_service.format_trade_date(row["日期"]),
                    _to_number(row, "开盘"),
                    _to_number(row, "收盘"),
                    _to_number(row, "最高"),
                    _to_number(row, "最低"),
                    _to_number(row, "成交量"),
                    _to_number(row, "成交额"),
                    _to_number(row, "涨跌幅"),
                    _to_number(row, "换手率"),
                    adjust,
                    source,
                    fetched_at,
                ),
            )
            count += 1
    return count


def list_daily_bars(
    code: str,
    adjust: str = "qfq",
    source: str = "akshare",
) -> list[dict[str, Any]]:
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover_rate, adjust, source, fetched_at
            FROM daily_bars
            WHERE code = ? AND adjust = ? AND source = ?
            ORDER BY trade_date ASC
            """,
            (tdx_service.format_code(code), adjust, source),
        ).fetchall()
    return [dict(row) for row in rows]
