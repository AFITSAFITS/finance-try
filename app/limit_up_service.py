from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from app import db
from app import signal_service
from app import tdx_service


LIMIT_UP_OUTPUT_COLUMNS = [
    "trade_date",
    "code",
    "name",
    "sector",
    "close_price",
    "pct_change",
    "turnover_rate",
    "consecutive_boards",
    "sector_limit_up_count",
    "sector_heat_rank",
    "score",
    "reason",
]


def normalize_trade_date(value: str | None = None) -> str:
    raw = (value or "").strip()
    if not raw:
        return datetime.now().strftime("%Y-%m-%d")
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def compact_trade_date(value: str | None = None) -> str:
    return normalize_trade_date(value).replace("-", "")


def _first_existing(row: pd.Series, names: list[str], default: object = None) -> object:
    for name in names:
        if name in row and not pd.isna(row[name]):
            return row[name]
    return default


def _clean_float(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    raw = str(value).strip().replace("%", "").replace(",", "")
    if raw in {"", "-", "None", "nan"}:
        return None
    multiplier = 1.0
    if raw.endswith("亿"):
        multiplier = 1e8
        raw = raw[:-1]
    elif raw.endswith("万"):
        multiplier = 1e4
        raw = raw[:-1]
    try:
        return float(raw) * multiplier
    except ValueError:
        return None


def _clean_int(value: object) -> int | None:
    number = _clean_float(value)
    return None if number is None else int(number)


def fetch_limit_up_pool(trade_date: str | None = None) -> pd.DataFrame:
    import akshare as ak

    return ak.stock_zt_pool_em(date=compact_trade_date(trade_date))


def normalize_limit_up_pool(df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for _, row in df.iterrows():
        code = tdx_service.format_code(_first_existing(row, ["代码", "股票代码", "证券代码"]))
        rows.append(
            {
                "trade_date": normalize_trade_date(trade_date),
                "code": code,
                "name": str(_first_existing(row, ["名称", "股票简称", "简称"], "") or ""),
                "sector": str(_first_existing(row, ["所属行业", "行业", "板块"], "") or ""),
                "close_price": _clean_float(_first_existing(row, ["最新价", "收盘", "现价"])),
                "pct_change": _clean_float(_first_existing(row, ["涨跌幅", "涨幅"])),
                "turnover_rate": _clean_float(_first_existing(row, ["换手率", "实际换手率"])),
                "consecutive_boards": _clean_int(_first_existing(row, ["连板数", "连续涨停天数"], 1)),
                "first_limit_time": str(_first_existing(row, ["首次封板时间"], "") or ""),
                "last_limit_time": str(_first_existing(row, ["最后封板时间"], "") or ""),
                "open_board_count": _clean_int(_first_existing(row, ["炸板次数", "开板次数"], 0)),
            }
        )
    return pd.DataFrame(rows)


def build_sector_heat_map(pool_df: pd.DataFrame) -> dict[str, dict[str, int]]:
    if pool_df.empty or "sector" not in pool_df.columns:
        return {}
    counts = (
        pool_df["sector"]
        .fillna("")
        .map(lambda value: str(value).strip() or "未分类")
        .value_counts()
    )
    heat_map: dict[str, dict[str, int]] = {}
    for rank, (sector, count) in enumerate(counts.items(), start=1):
        heat_map[sector] = {
            "sector_limit_up_count": int(count),
            "sector_heat_rank": int(rank),
        }
    return heat_map


def analyze_limit_up_candidate(
    row: dict[str, object],
    lookback_days: int = 120,
    sector_heat: dict[str, int] | None = None,
    history_fetcher: Callable[[str, int, str], pd.DataFrame] = signal_service.fetch_daily_history_akshare,
) -> dict[str, object]:
    code = str(row["code"])
    score = 0.0
    reasons: list[str] = []
    payload: dict[str, object] = {}

    try:
        history = signal_service.add_indicator_columns(
            signal_service.normalize_history_df(
                history_fetcher(code, lookback_days, "qfq"),
                code,
            )
        )
    except Exception as exc:  # noqa: BLE001
        history = pd.DataFrame()
        payload["history_error"] = str(exc)

    if not history.empty:
        latest = history.iloc[-1]
        close = float(latest["收盘"])
        latest_pct = _clean_float(latest.get("涨跌幅"))
        if row.get("close_price") is None or float(row.get("close_price") or 0) <= 0:
            row["close_price"] = round(close, 4)
        if latest_pct is not None:
            current_pct = row.get("pct_change")
            if current_pct is None or abs(float(current_pct)) > 30:
                row["pct_change"] = latest_pct
        prev_high = history["收盘"].iloc[:-1].tail(60).max() if len(history.index) > 1 else close
        if pd.notna(prev_high) and prev_high > 0:
            breakout_ratio = close / float(prev_high)
            payload["breakout_ratio"] = round(breakout_ratio, 4)
            if breakout_ratio >= 1:
                score += 35
                reasons.append("突破近60日收盘高点")
            elif breakout_ratio >= 0.97:
                score += 22
                reasons.append("接近近60日高位")

        ma5 = latest.get("MA5")
        ma20 = latest.get("MA20")
        if pd.notna(ma5) and pd.notna(ma20) and float(ma5) >= float(ma20):
            score += 20
            reasons.append("短期均线强于中期均线")

        recent_low = history["收盘"].tail(20).min()
        if pd.notna(recent_low) and recent_low > 0:
            rebound_ratio = close / float(recent_low) - 1
            payload["rebound_20d"] = round(rebound_ratio, 4)
            if 0.05 <= rebound_ratio <= 0.45:
                score += 15
                reasons.append("近期走势有抬升但未过热")

    pct_change = row.get("pct_change")
    if pct_change is not None and float(pct_change) >= 9.5:
        score += 15
        reasons.append("当日涨停强度确认")

    consecutive_boards = row.get("consecutive_boards")
    if consecutive_boards is not None and int(consecutive_boards) >= 2:
        score += 10
        reasons.append("连板增强关注度")

    open_board_count = row.get("open_board_count")
    if open_board_count is not None and int(open_board_count) == 0:
        score += 5
        reasons.append("封板过程中未明显开板")

    if sector_heat:
        sector_count = int(sector_heat.get("sector_limit_up_count", 0))
        sector_rank = int(sector_heat.get("sector_heat_rank", 0))
        row["sector_limit_up_count"] = sector_count
        row["sector_heat_rank"] = sector_rank
        payload["sector_limit_up_count"] = sector_count
        payload["sector_heat_rank"] = sector_rank
        if sector_count >= 5:
            score += 15
            reasons.append(f"所属板块当日{sector_count}只涨停，共振明显")
        elif sector_count >= 3:
            score += 10
            reasons.append(f"所属板块当日{sector_count}只涨停")
        elif sector_count >= 2:
            score += 5
            reasons.append("所属板块有涨停共振")
        if 0 < sector_rank <= 3:
            score += 5
            reasons.append("所属板块涨停热度靠前")

    enriched = dict(row)
    enriched["score"] = round(score, 2)
    enriched["reason"] = "；".join(reasons) if reasons else "涨停入池，K线数据不足"
    enriched["payload"] = payload
    return enriched


def scan_limit_up_breakthroughs(
    trade_date: str | None = None,
    lookback_days: int = 120,
    min_score: float = 50,
    max_items: int = 100,
    pool_limit: int = 200,
    pool_fetcher: Callable[[str | None], pd.DataFrame] = fetch_limit_up_pool,
    history_fetcher: Callable[[str, int, str], pd.DataFrame] = signal_service.fetch_daily_history_akshare,
) -> tuple[list[dict[str, object]], list[dict[str, str]]]:
    normalized_date = normalize_trade_date(trade_date)
    try:
        pool_df = normalize_limit_up_pool(pool_fetcher(normalized_date), normalized_date)
    except Exception as exc:  # noqa: BLE001
        return [], [{"股票代码": "全部", "error": str(exc)}]
    sector_heat_map = build_sector_heat_map(pool_df)
    candidates: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []

    for _, series in pool_df.head(int(pool_limit)).iterrows():
        row = series.to_dict()
        try:
            candidate = analyze_limit_up_candidate(
                row,
                lookback_days=int(lookback_days),
                sector_heat=sector_heat_map.get(str(row.get("sector", "")).strip() or "未分类"),
                history_fetcher=history_fetcher,
            )
            if float(candidate["score"]) >= float(min_score):
                candidates.append(candidate)
        except Exception as exc:  # noqa: BLE001
            errors.append({"股票代码": str(row.get("code", "")), "error": str(exc)})

    candidates.sort(key=lambda item: float(item.get("score", 0)), reverse=True)
    return candidates[: int(max_items)], errors


def _row_to_candidate(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "sector": row["sector"],
        "close_price": row["close_price"],
        "pct_change": row["pct_change"],
        "turnover_rate": row["turnover_rate"],
        "consecutive_boards": row["consecutive_boards"],
        "sector_limit_up_count": row["sector_limit_up_count"],
        "sector_heat_rank": row["sector_heat_rank"],
        "first_limit_time": row["first_limit_time"],
        "last_limit_time": row["last_limit_time"],
        "open_board_count": row["open_board_count"],
        "score": row["score"],
        "reason": row["reason"],
        "payload": json.loads(row["payload_json"]),
        "created_at": row["created_at"],
    }


def persist_limit_up_candidates(candidates: list[dict[str, object]]) -> list[dict[str, Any]]:
    saved: list[dict[str, Any]] = []
    with db.get_connection() as conn:
        for item in candidates:
            payload_json = json.dumps(item.get("payload", {}), ensure_ascii=False, sort_keys=True)
            conn.execute(
                """
                INSERT INTO limit_up_candidates (
                    trade_date, code, name, sector, close_price, pct_change, turnover_rate,
                    consecutive_boards, sector_limit_up_count, sector_heat_rank,
                    first_limit_time, last_limit_time, open_board_count,
                    score, reason, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date, code) DO UPDATE SET
                    name = excluded.name,
                    sector = excluded.sector,
                    close_price = excluded.close_price,
                    pct_change = excluded.pct_change,
                    turnover_rate = excluded.turnover_rate,
                    consecutive_boards = excluded.consecutive_boards,
                    sector_limit_up_count = excluded.sector_limit_up_count,
                    sector_heat_rank = excluded.sector_heat_rank,
                    first_limit_time = excluded.first_limit_time,
                    last_limit_time = excluded.last_limit_time,
                    open_board_count = excluded.open_board_count,
                    score = excluded.score,
                    reason = excluded.reason,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (
                    item["trade_date"],
                    item["code"],
                    item.get("name", ""),
                    item.get("sector", ""),
                    item.get("close_price"),
                    item.get("pct_change"),
                    item.get("turnover_rate"),
                    item.get("consecutive_boards"),
                    item.get("sector_limit_up_count"),
                    item.get("sector_heat_rank"),
                    item.get("first_limit_time", ""),
                    item.get("last_limit_time", ""),
                    item.get("open_board_count"),
                    item.get("score", 0),
                    item.get("reason", ""),
                    payload_json,
                    tdx_service.now_ts(),
                ),
            )
            row = conn.execute(
                """
                SELECT id, trade_date, code, name, sector, close_price, pct_change, turnover_rate,
                       consecutive_boards, sector_limit_up_count, sector_heat_rank,
                       first_limit_time, last_limit_time, open_board_count,
                       score, reason, payload_json, created_at
                FROM limit_up_candidates
                WHERE trade_date = ? AND code = ?
                """,
                (item["trade_date"], item["code"]),
            ).fetchone()
            assert row is not None
            saved.append(_row_to_candidate(row))
    return saved


def scan_and_save_limit_up_breakthroughs(**kwargs: object) -> dict[str, object]:
    candidates, errors = scan_limit_up_breakthroughs(**kwargs)
    saved = persist_limit_up_candidates(candidates)
    return {
        "trade_date": normalize_trade_date(kwargs.get("trade_date") if kwargs else None),
        "count": len(saved),
        "items": saved,
        "errors": errors,
    }


def list_limit_up_candidates(
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = 200,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append("trade_date = ?")
        params.append(normalize_trade_date(trade_date))
    if code:
        clauses.append("code = ?")
        params.append(tdx_service.format_code(code))
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, trade_date, code, name, sector, close_price, pct_change, turnover_rate,
                   consecutive_boards, sector_limit_up_count, sector_heat_rank,
                   first_limit_time, last_limit_time, open_board_count,
                   score, reason, payload_json, created_at
            FROM limit_up_candidates
            {where_sql}
            ORDER BY trade_date DESC, score DESC, code ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_candidate(row) for row in rows]
