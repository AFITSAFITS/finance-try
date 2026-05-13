from __future__ import annotations

import json
from typing import Any

from app import db
from app import tdx_service


def _row_to_scan_run(row: Any) -> dict[str, Any]:
    summary = json.loads(row["summary_json"])
    status = str(row["status"] or "")
    note = str(row["note"] or "")
    if not status:
        health = build_scan_run_health(
            requested_count=int(row["requested_count"]),
            event_count=int(row["event_count"]),
            error_count=int(row["error_count"]),
            signal_summary=summary,
        )
        status = health["status"]
        note = health["note"]
    return {
        "id": row["id"],
        "run_at": row["run_at"],
        "channel": row["channel"],
        "watchlist_name": row["watchlist_name"],
        "watchlist_source": row["watchlist_source"],
        "requested_count": row["requested_count"],
        "event_count": row["event_count"],
        "notification_count": row["notification_count"],
        "error_count": row["error_count"],
        "elapsed_seconds": row["elapsed_seconds"],
        "min_score": row["min_score"],
        "status": status,
        "note": note,
        "summary": summary,
    }


def build_scan_run_health(
    *,
    requested_count: int,
    event_count: int,
    error_count: int,
    signal_summary: dict[str, Any],
) -> dict[str, str]:
    signals = int(signal_summary.get("signals") or 0)
    stale_signals = int(signal_summary.get("stale_signals") or 0)
    cache_fallback_signals = int(signal_summary.get("cache_fallback_signals") or 0)
    if requested_count > 0 and error_count >= requested_count:
        return {"status": "失败", "note": "全部股票扫描失败"}
    if error_count > 0:
        return {"status": "部分失败", "note": f"{error_count} 只股票扫描失败"}
    if stale_signals > 0:
        return {"status": "数据滞后", "note": f"{stale_signals} 条信号数据可能滞后"}
    if cache_fallback_signals > 0:
        return {"status": "缓存兜底", "note": f"{cache_fallback_signals} 条信号使用旧缓存兜底"}
    if signals == 0 or event_count == 0:
        return {"status": "无信号", "note": "本次没有命中可保存信号"}
    return {"status": "正常", "note": "扫描完成并生成信号"}


def persist_scan_run(
    *,
    channel: str,
    watchlist: dict[str, Any],
    watchlist_source: str,
    requested_count: int,
    event_count: int,
    notification_count: int,
    error_count: int,
    elapsed_seconds: float | None,
    min_score: float | None,
    signal_summary: dict[str, Any],
) -> dict[str, Any]:
    run_at = tdx_service.now_ts()
    health = build_scan_run_health(
        requested_count=requested_count,
        event_count=event_count,
        error_count=error_count,
        signal_summary=signal_summary,
    )
    summary_json = json.dumps(signal_summary, ensure_ascii=False, sort_keys=True)
    with db.get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO scan_runs (
                run_at, channel, watchlist_name, watchlist_source, requested_count,
                event_count, notification_count, error_count, elapsed_seconds,
                min_score, status, note, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_at,
                channel,
                str(watchlist.get("name", "")),
                watchlist_source,
                int(requested_count),
                int(event_count),
                int(notification_count),
                int(error_count),
                elapsed_seconds,
                min_score,
                health["status"],
                health["note"],
                summary_json,
            ),
        )
        row = conn.execute(
            """
            SELECT id, run_at, channel, watchlist_name, watchlist_source,
                   requested_count, event_count, notification_count, error_count,
                   elapsed_seconds, min_score, status, note, summary_json
            FROM scan_runs
            WHERE id = ?
            """,
            (cursor.lastrowid,),
        ).fetchone()
        assert row is not None
        return _row_to_scan_run(row)


def list_scan_runs(limit: int = 50) -> list[dict[str, Any]]:
    with db.get_connection() as conn:
        rows = conn.execute(
            """
            SELECT id, run_at, channel, watchlist_name, watchlist_source,
                   requested_count, event_count, notification_count, error_count,
                   elapsed_seconds, min_score, status, note, summary_json
            FROM scan_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_row_to_scan_run(row) for row in rows]
