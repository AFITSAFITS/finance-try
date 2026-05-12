from __future__ import annotations

from collections import defaultdict
import json
import sqlite3
from datetime import datetime
from typing import Any, Callable

import pandas as pd

from app import db
from app import bar_service
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
        bar_service.upsert_daily_bars(code, history, adjust="qfq")
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


def parse_horizons(values: list[int] | tuple[int, ...] | None) -> list[int]:
    raw_values = list(values or [1, 3, 5])
    normalized = sorted({int(value) for value in raw_values if int(value) > 0})
    return normalized or [1, 3, 5]


def horizon_label(days: int) -> str:
    return f"T+{int(days)}"


def _load_limit_up_candidates(
    trade_date: str | None = None,
    code: str | None = None,
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
    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, trade_date, code, name, sector, close_price, score,
                   sector_limit_up_count, sector_heat_rank, reason
            FROM limit_up_candidates
            {where_sql}
            ORDER BY trade_date ASC, code ASC, id ASC
            """,
            tuple(params),
        ).fetchall()
    return [dict(row) for row in rows]


def _cached_daily_bars_to_history_df(rows: list[dict[str, Any]]) -> pd.DataFrame:
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
            }
            for row in rows
        ]
    )


def fetch_daily_history_range_with_cache(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
) -> pd.DataFrame:
    start = normalize_trade_date(start_date)
    end = normalize_trade_date(end_date)
    cached_rows = [
        row
        for row in bar_service.list_daily_bars(code, adjust=adjust)
        if start <= str(row["trade_date"]) <= end
    ]
    if cached_rows:
        return signal_service.normalize_history_df(
            _cached_daily_bars_to_history_df(cached_rows),
            code,
        )
    return bar_service.fetch_daily_history_range_akshare(code, start, end, adjust)


def _row_to_review(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "limit_up_candidate_id": row["limit_up_candidate_id"],
        "trade_date": row["trade_date"],
        "code": row["code"],
        "name": row["name"],
        "sector": row["sector"],
        "score": row["score"],
        "sector_limit_up_count": row["sector_limit_up_count"],
        "sector_heat_rank": row["sector_heat_rank"],
        "horizon": row["horizon"],
        "future_trade_date": row["future_trade_date"],
        "future_close_price": row["future_close_price"],
        "pct_return": row["pct_return"],
        "max_drawdown": row["max_drawdown"],
        "updated_at": row["updated_at"],
    }


def list_limit_up_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizon: str | None = None,
    limit: int = 500,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if trade_date:
        clauses.append("c.trade_date = ?")
        params.append(normalize_trade_date(trade_date))
    if code:
        clauses.append("c.code = ?")
        params.append(tdx_service.format_code(code))
    if horizon:
        clauses.append("r.horizon = ?")
        params.append(horizon)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT r.id, r.limit_up_candidate_id, c.trade_date, c.code, c.name, c.sector,
                   c.score, c.sector_limit_up_count, c.sector_heat_rank,
                   r.horizon, r.future_trade_date, r.future_close_price,
                   r.pct_return, r.max_drawdown, r.updated_at
            FROM limit_up_review_snapshots r
            JOIN limit_up_candidates c ON c.id = r.limit_up_candidate_id
            {where_sql}
            ORDER BY c.trade_date DESC, c.score DESC, c.code ASC, r.horizon ASC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_review(row) for row in rows]


def backfill_limit_up_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizons: list[int] | tuple[int, ...] | None = None,
    adjust: str = "qfq",
    fetcher: Callable[[str, str, str, str], pd.DataFrame] = fetch_daily_history_range_with_cache,
) -> dict[str, Any]:
    selected_horizons = parse_horizons(horizons)
    candidates = _load_limit_up_candidates(trade_date=trade_date, code=code)
    if not candidates:
        return {"count": 0, "items": [], "errors": []}

    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        grouped[str(candidate["code"])].append(candidate)

    end_date = datetime.now().strftime("%Y-%m-%d")
    snapshots: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    with db.get_connection() as conn:
        for stock_code, code_candidates in grouped.items():
            start_date = min(str(item["trade_date"]) for item in code_candidates)
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

                for candidate in code_candidates:
                    candidate_date = str(candidate["trade_date"])
                    base_index = date_to_index.get(candidate_date)
                    if base_index is None:
                        continue

                    base_close = (
                        float(candidate["close_price"])
                        if candidate.get("close_price") is not None and float(candidate["close_price"]) > 0
                        else float(indexed.iloc[base_index]["收盘"])
                    )
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
                            INSERT INTO limit_up_review_snapshots (
                                limit_up_candidate_id, horizon, future_trade_date,
                                future_close_price, pct_return, max_drawdown, updated_at
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(limit_up_candidate_id, horizon)
                            DO UPDATE SET
                                future_trade_date=excluded.future_trade_date,
                                future_close_price=excluded.future_close_price,
                                pct_return=excluded.pct_return,
                                max_drawdown=excluded.max_drawdown,
                                updated_at=excluded.updated_at
                            """,
                            (
                                int(candidate["id"]),
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
                            SELECT r.id, r.limit_up_candidate_id, c.trade_date, c.code, c.name, c.sector,
                                   c.score, c.sector_limit_up_count, c.sector_heat_rank,
                                   r.horizon, r.future_trade_date, r.future_close_price,
                                   r.pct_return, r.max_drawdown, r.updated_at
                            FROM limit_up_review_snapshots r
                            JOIN limit_up_candidates c ON c.id = r.limit_up_candidate_id
                            WHERE r.limit_up_candidate_id = ? AND r.horizon = ?
                            """,
                            (int(candidate["id"]), label),
                        ).fetchone()
                        assert saved is not None
                        snapshots.append(_row_to_review(saved))
            except Exception as exc:  # noqa: BLE001
                errors.append({"股票代码": stock_code, "error": str(exc)})

    snapshots.sort(key=lambda item: (item["trade_date"], item["code"], item["horizon"]))
    return {"count": len(snapshots), "items": snapshots, "errors": errors}


def summarize_limit_up_review_stats(
    horizon: str = "T+3",
    trade_date: str | None = None,
    code: str | None = None,
) -> list[dict[str, Any]]:
    snapshots = list_limit_up_review_snapshots(
        trade_date=trade_date,
        code=code,
        horizon=horizon,
        limit=5000,
    )
    if not snapshots:
        return []

    df = pd.DataFrame(snapshots)
    df["score_bucket"] = pd.cut(
        df["score"].fillna(0),
        bins=[-1, 40, 60, 80, 101],
        labels=["0-40", "40-60", "60-80", "80+"],
    )
    grouped = (
        df.groupby("score_bucket", observed=True)
        .agg(
            sample_count=("pct_return", "count"),
            avg_return=("pct_return", "mean"),
            win_rate=("pct_return", lambda s: float((s > 0).mean())),
            avg_max_drawdown=("max_drawdown", "mean"),
            avg_sector_limit_up_count=("sector_limit_up_count", "mean"),
        )
        .reset_index()
    )

    items: list[dict[str, Any]] = []
    for _, row in grouped.iterrows():
        items.append(
            {
                "score_bucket": str(row["score_bucket"]),
                "sample_count": int(row["sample_count"]),
                "avg_return": round(float(row["avg_return"]), 4),
                "win_rate": round(float(row["win_rate"]), 4),
                "avg_max_drawdown": round(float(row["avg_max_drawdown"]), 4),
                "avg_sector_limit_up_count": round(float(row["avg_sector_limit_up_count"]), 4)
                if not pd.isna(row["avg_sector_limit_up_count"])
                else None,
                "horizon": horizon,
            }
        )
    return items
