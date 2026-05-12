from __future__ import annotations

import sqlite3
from typing import Iterable

from app import db
from app import market_service
from app import tdx_service

DEFAULT_WATCHLIST_NAME = "默认股票池"
DEFAULT_SEED_CODES = [
    "600519",
    "000001",
    "600036",
    "300750",
    "601318",
    "000858",
    "600900",
    "601899",
    "002594",
    "000333",
    "600276",
    "600030",
    "601012",
    "601088",
    "600309",
    "002415",
    "300760",
    "601166",
    "600887",
    "002475",
]


def _row_to_item(row: sqlite3.Row) -> dict[str, object]:
    return {
        "id": row["id"],
        "code": row["code"],
        "name": row["name"],
        "market": row["market"],
        "enabled": bool(row["enabled"]),
        "tags": row["tags"],
        "created_at": row["created_at"],
    }


def _load_items(conn: sqlite3.Connection, watchlist_id: int) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT id, code, name, market, enabled, tags, created_at
        FROM watchlist_items
        WHERE watchlist_id = ?
        ORDER BY id ASC
        """,
        (watchlist_id,),
    ).fetchall()
    return [_row_to_item(row) for row in rows]


def _get_or_create_default_watchlist(conn: sqlite3.Connection) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, name, description, is_default, created_at, updated_at
        FROM watchlists
        WHERE is_default = 1
        ORDER BY id ASC
        LIMIT 1
        """
    ).fetchone()
    if row is not None:
        return row

    now = tdx_service.now_ts()
    cursor = conn.execute(
        """
        INSERT INTO watchlists (name, description, is_default, created_at, updated_at)
        VALUES (?, ?, 1, ?, ?)
        """,
        (DEFAULT_WATCHLIST_NAME, "默认关注股票列表", now, now),
    )
    created_id = int(cursor.lastrowid)
    created = conn.execute(
        """
        SELECT id, name, description, is_default, created_at, updated_at
        FROM watchlists
        WHERE id = ?
        """,
        (created_id,),
    ).fetchone()
    assert created is not None
    return created


def _watchlist_payload(conn: sqlite3.Connection, row: sqlite3.Row) -> dict[str, object]:
    items = _load_items(conn, int(row["id"]))
    return {
        "id": row["id"],
        "name": row["name"],
        "description": row["description"],
        "is_default": bool(row["is_default"]),
        "count": len(items),
        "items": items,
        "updated_at": row["updated_at"],
    }


def get_default_watchlist() -> dict[str, object]:
    with db.get_connection() as conn:
        row = _get_or_create_default_watchlist(conn)
        return _watchlist_payload(conn, row)


def list_default_watchlist_codes() -> list[str]:
    watchlist = get_default_watchlist()
    return [str(item["code"]) for item in watchlist["items"]]


def replace_default_watchlist_items(codes: Iterable[str]) -> dict[str, object]:
    normalized_codes = tdx_service.dedupe_keep_order(tdx_service.normalize_codes(codes))
    now = tdx_service.now_ts()
    with db.get_connection() as conn:
        watchlist = _get_or_create_default_watchlist(conn)
        conn.execute("DELETE FROM watchlist_items WHERE watchlist_id = ?", (watchlist["id"],))
        for code in normalized_codes:
            conn.execute(
                """
                INSERT INTO watchlist_items (watchlist_id, code, name, market, enabled, tags, created_at)
                VALUES (?, ?, '', 'CN', 1, '', ?)
                """,
                (watchlist["id"], code, now),
            )
        conn.execute(
            "UPDATE watchlists SET updated_at = ? WHERE id = ?",
            (now, watchlist["id"]),
        )
        refreshed = conn.execute(
            """
            SELECT id, name, description, is_default, created_at, updated_at
            FROM watchlists
            WHERE id = ?
            """,
            (watchlist["id"],),
        ).fetchone()
        assert refreshed is not None
        return _watchlist_payload(conn, refreshed)


def import_default_watchlist_from_index(
    index_code: str = "000300",
    constituent_fetcher=market_service.fetch_index_constituent_codes,
) -> dict[str, object]:
    normalized_index_code = market_service.normalize_index_code(index_code)
    codes = constituent_fetcher(normalized_index_code)
    payload = replace_default_watchlist_items(codes)
    payload["index_code"] = normalized_index_code
    return payload


def bootstrap_default_watchlist(
    index_code: str = "000300",
    fallback_to_seed: bool = True,
    constituent_fetcher=market_service.fetch_index_constituent_codes,
) -> dict[str, object]:
    normalized_index_code = market_service.normalize_index_code(index_code)
    try:
        payload = import_default_watchlist_from_index(
            index_code=normalized_index_code,
            constituent_fetcher=constituent_fetcher,
        )
        payload["source"] = "index"
        payload["message"] = f"已导入 {normalized_index_code} 指数成分股"
        return payload
    except Exception as exc:  # noqa: BLE001
        if not fallback_to_seed:
            raise
        payload = replace_default_watchlist_items(DEFAULT_SEED_CODES)
        payload["index_code"] = normalized_index_code
        payload["source"] = "seed"
        payload["warning"] = str(exc)
        payload["message"] = "指数成分股导入失败，已使用内置种子股票池"
        return payload


def ensure_default_watchlist(
    index_code: str = "000300",
    fallback_to_seed: bool = True,
    constituent_fetcher=market_service.fetch_index_constituent_codes,
) -> dict[str, object]:
    watchlist = get_default_watchlist()
    if int(watchlist.get("count", 0)) > 0:
        watchlist["source"] = "existing"
        return watchlist
    return bootstrap_default_watchlist(
        index_code=index_code,
        fallback_to_seed=fallback_to_seed,
        constituent_fetcher=constituent_fetcher,
    )
