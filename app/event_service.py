from __future__ import annotations

import json
import sqlite3
from typing import Any

import pandas as pd

from app import db
from app import signal_service
from app import tdx_service


def _clean_number(value: object) -> float | None:
    if pd.isna(value):
        return None
    return float(value)


def _normalize_trade_date(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    return str(value)


def _signal_payload(row: dict[str, object]) -> dict[str, object]:
    return {
        "close": _clean_number(row.get("收盘")),
        "pct_change": _clean_number(row.get("涨跌幅")),
        "dif": _clean_number(row.get("DIF")),
        "dea": _clean_number(row.get("DEA")),
        "macd_pattern": row.get("MACD形态"),
        "ma5": _clean_number(row.get("MA5")),
        "ma20": _clean_number(row.get("MA20")),
        "signal": row.get("信号"),
    }


def _events_from_row(row: dict[str, object]) -> list[dict[str, object]]:
    code = tdx_service.format_code(row.get("股票代码", ""))
    trade_date = _normalize_trade_date(row.get("日期", ""))
    payload = _signal_payload(row)
    severity = "high" if row.get("MACD形态") or (row.get("MACD信号") and row.get("均线信号")) else "normal"
    events: list[dict[str, object]] = []

    macd_signal = row.get("MACD信号")
    if macd_signal == "MACD金叉":
        events.append(
            {
                "trade_date": trade_date,
                "code": code,
                "indicator": "MACD",
                "event_type": "golden_cross",
                "severity": severity,
                "summary": "MACD金叉",
                "close_price": _clean_number(row.get("收盘")),
                "pct_change": _clean_number(row.get("涨跌幅")),
                "payload": payload,
            }
        )
    elif macd_signal == "MACD死叉":
        events.append(
            {
                "trade_date": trade_date,
                "code": code,
                "indicator": "MACD",
                "event_type": "death_cross",
                "severity": severity,
                "summary": "MACD死叉",
                "close_price": _clean_number(row.get("收盘")),
                "pct_change": _clean_number(row.get("涨跌幅")),
                "payload": payload,
            }
        )

    macd_pattern = row.get("MACD形态")
    if macd_pattern == signal_service.SECONDARY_GOLDEN_CROSS_PATTERN:
        events.append(
            {
                "trade_date": trade_date,
                "code": code,
                "indicator": "MACD",
                "event_type": "secondary_golden_cross_above_zero",
                "severity": "high",
                "summary": signal_service.SECONDARY_GOLDEN_CROSS_PATTERN,
                "close_price": _clean_number(row.get("收盘")),
                "pct_change": _clean_number(row.get("涨跌幅")),
                "payload": payload,
            }
        )

    ma_signal = row.get("均线信号")
    if ma_signal == "MA5上穿MA20":
        events.append(
            {
                "trade_date": trade_date,
                "code": code,
                "indicator": "MA",
                "event_type": "ma5_cross_up_ma20",
                "severity": severity,
                "summary": "MA5上穿MA20",
                "close_price": _clean_number(row.get("收盘")),
                "pct_change": _clean_number(row.get("涨跌幅")),
                "payload": payload,
            }
        )
    elif ma_signal == "MA5下穿MA20":
        events.append(
            {
                "trade_date": trade_date,
                "code": code,
                "indicator": "MA",
                "event_type": "ma5_cross_down_ma20",
                "severity": severity,
                "summary": "MA5下穿MA20",
                "close_price": _clean_number(row.get("收盘")),
                "pct_change": _clean_number(row.get("涨跌幅")),
                "payload": payload,
            }
        )
    return events


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "indicator": row["indicator"],
        "event_type": row["event_type"],
        "severity": row["severity"],
        "summary": row["summary"],
        "close_price": row["close_price"],
        "pct_change": row["pct_change"],
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


def persist_signal_rows(df: pd.DataFrame) -> list[dict[str, Any]]:
    if df.empty:
        return []

    saved_events: list[dict[str, Any]] = []
    with db.get_connection() as conn:
        for _, series in df.iterrows():
            row = series.to_dict()
            for event in _events_from_row(row):
                payload_json = json.dumps(event["payload"], ensure_ascii=False, sort_keys=True)
                conn.execute(
                    """
                    INSERT OR IGNORE INTO signal_events (
                        trade_date,
                        code,
                        indicator,
                        event_type,
                        severity,
                        summary,
                        close_price,
                        pct_change,
                        payload_json,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event["trade_date"],
                        event["code"],
                        event["indicator"],
                        event["event_type"],
                        event["severity"],
                        event["summary"],
                        event["close_price"],
                        event["pct_change"],
                        payload_json,
                        tdx_service.now_ts(),
                    ),
                )
                saved_row = conn.execute(
                    """
                    SELECT id, trade_date, code, indicator, event_type, severity, summary,
                           close_price, pct_change, payload_json, created_at
                    FROM signal_events
                    WHERE trade_date = ? AND code = ? AND indicator = ? AND event_type = ?
                    """,
                    (
                        event["trade_date"],
                        event["code"],
                        event["indicator"],
                        event["event_type"],
                    ),
                ).fetchone()
                assert saved_row is not None
                saved_events.append(_row_to_event(saved_row))
    return saved_events


def list_signal_events(
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 100,
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
    params.append(int(limit))
    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, trade_date, code, indicator, event_type, severity, summary,
                   close_price, pct_change, payload_json, created_at
            FROM signal_events
            {where_sql}
            ORDER BY trade_date DESC, code ASC, indicator ASC, event_type ASC, id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_event(row) for row in rows]
