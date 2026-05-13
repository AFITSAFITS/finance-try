from __future__ import annotations

from datetime import datetime, timedelta
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


def cached_daily_bars_to_history_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "日期": row["trade_date"],
                "股票代码": row["code"],
                "开盘": row["open"],
                "收盘": row["close"],
                "最高": row["high"],
                "最低": row["low"],
                "成交量": row["volume"],
                "成交额": row["amount"],
                "涨跌幅": row["pct_change"],
                "换手率": row["turnover_rate"],
                "数据来源": "本地缓存",
                "缓存获取时间": row.get("fetched_at", ""),
            }
            for row in rows
        ]
    )


def _cached_history_df(rows: list[dict[str, Any]], source_label: str) -> pd.DataFrame:
    history_df = cached_daily_bars_to_history_df(rows)
    if not history_df.empty:
        history_df["数据来源"] = source_label
    return history_df


def _cached_rows_are_usable(rows: list[dict[str, Any]], lookback_days: int) -> bool:
    if not _cached_rows_have_enough_history(rows, lookback_days):
        return False
    today = datetime.now().strftime("%Y-%m-%d")
    return any(str(row.get("fetched_at", "")).startswith(today) for row in rows)


def _cached_rows_have_enough_history(rows: list[dict[str, Any]], lookback_days: int) -> bool:
    if not rows:
        return False
    min_rows = 35 if int(lookback_days) >= 60 else 15
    return len(rows) >= min_rows


def fetch_daily_history_cached(
    code: str,
    lookback_days: int = 180,
    adjust: str = "qfq",
) -> pd.DataFrame:
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=int(lookback_days))
    start = start_dt.strftime("%Y-%m-%d")
    end = end_dt.strftime("%Y-%m-%d")
    cached_rows = list_daily_bars_range(code, start, end, adjust=adjust)
    if _cached_rows_are_usable(cached_rows, lookback_days):
        return signal_service.normalize_history_df(
            _cached_history_df(cached_rows, "本地缓存"),
            code,
        )

    try:
        fetched = fetch_daily_history_range_akshare(code, start, end, adjust)
    except Exception:
        if _cached_rows_have_enough_history(cached_rows, lookback_days):
            return signal_service.normalize_history_df(
                _cached_history_df(cached_rows, "旧缓存兜底"),
                code,
            )
        raise
    upsert_daily_bars(code, fetched, adjust=adjust)
    fetched = fetched.copy()
    fetched["数据来源"] = "外部行情源"
    fetched["缓存获取时间"] = ""
    return fetched


def fetch_daily_history_range_cached(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
) -> pd.DataFrame:
    start = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    cached_rows = list_daily_bars_range(code, start, end, adjust=adjust)
    if cached_rows:
        return signal_service.normalize_history_df(
            _cached_history_df(cached_rows, "本地缓存"),
            code,
        )

    fetched = fetch_daily_history_range_akshare(code, start, end, adjust)
    upsert_daily_bars(code, fetched, adjust=adjust)
    fetched = signal_service.normalize_history_df(fetched, code)
    if not fetched.empty:
        fetched = fetched.copy()
        fetched["数据来源"] = "外部行情源"
        fetched["缓存获取时间"] = ""
    return fetched


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


def list_daily_bars_range(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    source: str = "akshare",
) -> list[dict[str, Any]]:
    start = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT code, trade_date, open, close, high, low, volume, amount,
                   pct_change, turnover_rate, adjust, source, fetched_at
            FROM daily_bars
            WHERE code = ? AND adjust = ? AND source = ? AND trade_date BETWEEN ? AND ?
            ORDER BY trade_date ASC
            """,
            (tdx_service.format_code(code), adjust, source, start, end),
        ).fetchall()
    return [dict(row) for row in rows]
