from __future__ import annotations

import json
from typing import Any

from app import db
from app import tdx_service


def _row_to_scan_run(row: Any) -> dict[str, Any]:
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
        "summary": json.loads(row["summary_json"]),
    }


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
    summary_json = json.dumps(signal_summary, ensure_ascii=False, sort_keys=True)
    with db.get_connection() as conn:
        cursor = conn.execute(
            """
            INSERT INTO scan_runs (
                run_at, channel, watchlist_name, watchlist_source, requested_count,
                event_count, notification_count, error_count, elapsed_seconds,
                min_score, summary_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                summary_json,
            ),
        )
        row = conn.execute(
            """
            SELECT id, run_at, channel, watchlist_name, watchlist_source,
                   requested_count, event_count, notification_count, error_count,
                   elapsed_seconds, min_score, summary_json
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
                   elapsed_seconds, min_score, summary_json
            FROM scan_runs
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_row_to_scan_run(row) for row in rows]
