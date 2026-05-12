from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Callable

import pandas as pd

from app import db
from app import tdx_service


SECTOR_TYPES = {"industry", "concept"}


def normalize_trade_date(value: str | None = None) -> str:
    raw = (value or "").strip()
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def compact_trade_date(value: str | None = None) -> str:
    return normalize_trade_date(value).replace("-", "")


def _clean_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    raw = str(value).strip().replace("%", "").replace(",", "")
    if raw in {"", "-", "None", "nan"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _col(df: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def fetch_sector_spot(sector_type: str = "industry") -> pd.DataFrame:
    import akshare as ak

    if sector_type == "industry":
        return ak.stock_board_industry_name_em()
    if sector_type == "concept":
        return ak.stock_board_concept_name_em()
    raise ValueError("sector_type 只能是 industry 或 concept")


def fetch_sector_history(
    sector_name: str,
    sector_type: str = "industry",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    import akshare as ak

    end = compact_trade_date(end_date)
    start = start_date or (datetime.strptime(end, "%Y%m%d") - timedelta(days=120)).strftime("%Y%m%d")
    if sector_type == "industry":
        return ak.stock_board_industry_hist_em(
            symbol=sector_name,
            start_date=start,
            end_date=end,
            period="日k",
            adjust="",
        )
    if sector_type == "concept":
        return ak.stock_board_concept_hist_em(
            symbol=sector_name,
            start_date=start,
            end_date=end,
            period="daily",
            adjust="",
        )
    raise ValueError("sector_type 只能是 industry 或 concept")


def normalize_sector_spot(df: pd.DataFrame, sector_type: str) -> pd.DataFrame:
    name_col = _col(df, ["板块名称", "名称", "行业名称", "概念名称"])
    pct_col = _col(df, ["涨跌幅", "涨跌幅%", "涨幅"])
    if name_col is None:
        raise ValueError("板块数据缺少名称字段")

    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        name = str(row[name_col]).strip()
        if not name:
            continue
        rows.append(
            {
                "sector_type": sector_type,
                "sector_name": name,
                "spot_pct_change": _clean_float(row[pct_col]) if pct_col else None,
            }
        )
    return pd.DataFrame(rows)


def normalize_sector_history(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    date_col = _col(df, ["日期", "时间"])
    close_col = _col(df, ["收盘", "收盘价"])
    pct_col = _col(df, ["涨跌幅", "涨跌幅%"])
    if date_col is None or close_col is None:
        raise ValueError("板块历史数据缺少日期或收盘字段")

    normalized = pd.DataFrame(
        {
            "日期": pd.to_datetime(df[date_col], errors="coerce"),
            "收盘": pd.to_numeric(df[close_col], errors="coerce"),
            "涨跌幅": pd.to_numeric(df[pct_col], errors="coerce") if pct_col else None,
        }
    )
    return normalized.dropna(subset=["日期", "收盘"]).sort_values("日期").reset_index(drop=True)


def analyze_sector_rotation(
    sector_name: str,
    sector_type: str,
    trade_date: str,
    history_fetcher: Callable[[str, str, str | None, str | None], pd.DataFrame] = fetch_sector_history,
) -> dict[str, object]:
    history = normalize_sector_history(
        history_fetcher(sector_name, sector_type, None, compact_trade_date(trade_date))
    )
    if history.empty:
        raise ValueError(f"{sector_name} 缺少历史行情")

    latest = history.iloc[-1]
    latest_close = float(latest["收盘"])
    latest_pct_change = _clean_float(latest.get("涨跌幅"))

    def calc_return(days: int) -> float | None:
        if len(history.index) <= days:
            return None
        base = float(history.iloc[-days - 1]["收盘"])
        if base <= 0:
            return None
        return round((latest_close / base - 1) * 100, 4)

    return_5d = calc_return(5)
    return_10d = calc_return(10)
    window = history.tail(60)
    low = float(window["收盘"].min())
    high = float(window["收盘"].max())
    position_60d = 1.0 if high <= low else (latest_close - low) / (high - low)

    activity_score = 0.0
    if latest_pct_change is not None:
        activity_score += max(0.0, min(40.0, latest_pct_change * 8))
    if return_5d is not None:
        activity_score += max(0.0, min(35.0, return_5d * 3))
    if return_10d is not None and return_10d > 0:
        activity_score += min(25.0, return_10d * 1.5)

    low_position_bonus = max(0.0, 1 - position_60d) * 40
    rotation_score = round(activity_score + low_position_bonus, 2)
    if activity_score >= 35 and position_60d <= 0.45:
        signal = "活跃低位"
    elif activity_score >= 35:
        signal = "活跃偏高"
    elif position_60d <= 0.35:
        signal = "低位观察"
    else:
        signal = "普通观察"

    return {
        "trade_date": normalize_trade_date(trade_date),
        "sector_type": sector_type,
        "sector_name": sector_name,
        "latest_close": round(latest_close, 4),
        "latest_pct_change": latest_pct_change,
        "return_5d": return_5d,
        "return_10d": return_10d,
        "position_60d": round(position_60d, 4),
        "activity_score": round(activity_score, 2),
        "rotation_score": rotation_score,
        "signal": signal,
        "payload": {
            "history_count": len(history.index),
            "latest_date": str(latest["日期"].date()),
        },
    }


def scan_sector_rotation(
    trade_date: str | None = None,
    sector_type: str = "industry",
    top_n: int = 30,
    max_items: int = 20,
    spot_fetcher: Callable[[str], pd.DataFrame] = fetch_sector_spot,
    history_fetcher: Callable[[str, str, str | None, str | None], pd.DataFrame] = fetch_sector_history,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    normalized_type = sector_type.strip().lower()
    if normalized_type not in SECTOR_TYPES:
        raise ValueError("sector_type 只能是 industry 或 concept")
    normalized_date = normalize_trade_date(trade_date)
    try:
        spot_df = normalize_sector_spot(spot_fetcher(normalized_type), normalized_type)
    except Exception as exc:  # noqa: BLE001
        return [], [{"板块": "全部", "error": str(exc)}]
    if "spot_pct_change" in spot_df.columns:
        spot_df = spot_df.sort_values("spot_pct_change", ascending=False, na_position="last")

    items: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    for _, row in spot_df.head(int(top_n)).iterrows():
        sector_name = str(row["sector_name"])
        try:
            item = analyze_sector_rotation(
                sector_name=sector_name,
                sector_type=normalized_type,
                trade_date=normalized_date,
                history_fetcher=history_fetcher,
            )
            if item.get("latest_pct_change") is None:
                item["latest_pct_change"] = row.get("spot_pct_change")
            items.append(item)
        except Exception as exc:  # noqa: BLE001
            errors.append({"板块": sector_name, "error": str(exc)})

    items.sort(key=lambda item: float(item.get("rotation_score", 0)), reverse=True)
    return items[: int(max_items)], errors


def _row_to_snapshot(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "trade_date": row["trade_date"],
        "sector_type": row["sector_type"],
        "sector_name": row["sector_name"],
        "latest_close": row["latest_close"],
        "latest_pct_change": row["latest_pct_change"],
        "return_5d": row["return_5d"],
        "return_10d": row["return_10d"],
        "position_60d": row["position_60d"],
        "activity_score": row["activity_score"],
        "rotation_score": row["rotation_score"],
        "signal": row["signal"],
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


def persist_sector_rotation_snapshots(items: list[dict[str, object]]) -> list[dict[str, Any]]:
    saved: list[dict[str, Any]] = []
    with db.get_connection() as conn:
        for item in items:
            payload_json = json.dumps(item.get("payload", {}), ensure_ascii=False, sort_keys=True)
            conn.execute(
                """
                INSERT INTO sector_rotation_snapshots (
                    trade_date, sector_type, sector_name, latest_close, latest_pct_change,
                    return_5d, return_10d, position_60d, activity_score, rotation_score,
                    signal, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, sector_type, sector_name) DO UPDATE SET
                    latest_close = excluded.latest_close,
                    latest_pct_change = excluded.latest_pct_change,
                    return_5d = excluded.return_5d,
                    return_10d = excluded.return_10d,
                    position_60d = excluded.position_60d,
                    activity_score = excluded.activity_score,
                    rotation_score = excluded.rotation_score,
                    signal = excluded.signal,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (
                    item["trade_date"],
                    item["sector_type"],
                    item["sector_name"],
                    item.get("latest_close"),
                    item.get("latest_pct_change"),
                    item.get("return_5d"),
                    item.get("return_10d"),
                    item.get("position_60d"),
                    item.get("activity_score"),
                    item.get("rotation_score"),
                    item.get("signal", ""),
                    payload_json,
                    tdx_service.now_ts(),
                ),
            )
            row = conn.execute(
                """
                SELECT id, trade_date, sector_type, sector_name, latest_close, latest_pct_change,
                       return_5d, return_10d, position_60d, activity_score, rotation_score,
                       signal, payload_json, created_at
                FROM sector_rotation_snapshots
                WHERE trade_date = ? AND sector_type = ? AND sector_name = ?
                """,
                (item["trade_date"], item["sector_type"], item["sector_name"]),
            ).fetchone()
            assert row is not None
            saved.append(_row_to_snapshot(row))
    return saved


def scan_and_save_sector_rotation(**kwargs: object) -> dict[str, object]:
    items, errors = scan_sector_rotation(**kwargs)
    saved = persist_sector_rotation_snapshots(items)
    return {
        "trade_date": normalize_trade_date(kwargs.get("trade_date") if kwargs else None),
        "count": len(saved),
        "items": saved,
        "errors": errors,
    }


def list_sector_rotation_snapshots(
    trade_date: str | None = None,
    sector_type: str | None = None,
    signal: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append("trade_date = ?")
        params.append(normalize_trade_date(trade_date))
    if sector_type:
        clauses.append("sector_type = ?")
        params.append(sector_type.strip().lower())
    if signal:
        clauses.append("signal = ?")
        params.append(signal.strip())
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, trade_date, sector_type, sector_name, latest_close, latest_pct_change,
                   return_5d, return_10d, position_60d, activity_score, rotation_score,
                   signal, payload_json, created_at
            FROM sector_rotation_snapshots
            {where_sql}
            ORDER BY trade_date DESC, rotation_score DESC, sector_name ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_snapshot(row) for row in rows]
