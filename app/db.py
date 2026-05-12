from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "app.db"

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS watchlists (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    is_default INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS watchlist_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    watchlist_id INTEGER NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    market TEXT NOT NULL DEFAULT 'CN',
    enabled INTEGER NOT NULL DEFAULT 1,
    tags TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    FOREIGN KEY (watchlist_id) REFERENCES watchlists(id) ON DELETE CASCADE,
    UNIQUE (watchlist_id, code)
);

CREATE TABLE IF NOT EXISTS signal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    code TEXT NOT NULL,
    indicator TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'normal',
    summary TEXT NOT NULL,
    close_price REAL,
    pct_change REAL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (trade_date, code, indicator, event_type)
);

CREATE INDEX IF NOT EXISTS idx_signal_events_trade_date ON signal_events(trade_date);
CREATE INDEX IF NOT EXISTS idx_signal_events_code ON signal_events(code);

CREATE TABLE IF NOT EXISTS daily_bars (
    code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    open REAL,
    close REAL,
    high REAL,
    low REAL,
    volume REAL,
    amount REAL,
    pct_change REAL,
    turnover_rate REAL,
    adjust TEXT NOT NULL DEFAULT 'qfq',
    source TEXT NOT NULL DEFAULT 'akshare',
    fetched_at TEXT NOT NULL,
    UNIQUE (code, trade_date, adjust, source)
);

CREATE INDEX IF NOT EXISTS idx_daily_bars_code_trade_date ON daily_bars(code, trade_date);

CREATE TABLE IF NOT EXISTS notification_deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_event_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'delivered',
    delivered_at TEXT NOT NULL,
    message_id TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    FOREIGN KEY (signal_event_id) REFERENCES signal_events(id) ON DELETE CASCADE,
    UNIQUE (signal_event_id, channel)
);

CREATE INDEX IF NOT EXISTS idx_notification_deliveries_channel ON notification_deliveries(channel);

CREATE TABLE IF NOT EXISTS review_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_event_id INTEGER NOT NULL,
    horizon TEXT NOT NULL,
    future_trade_date TEXT NOT NULL,
    future_close_price REAL,
    pct_return REAL,
    max_drawdown REAL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (signal_event_id) REFERENCES signal_events(id) ON DELETE CASCADE,
    UNIQUE (signal_event_id, horizon)
);

CREATE INDEX IF NOT EXISTS idx_review_snapshots_horizon ON review_snapshots(horizon);

CREATE TABLE IF NOT EXISTS limit_up_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    code TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    sector TEXT NOT NULL DEFAULT '',
    close_price REAL,
    pct_change REAL,
    turnover_rate REAL,
    consecutive_boards INTEGER,
    first_limit_time TEXT NOT NULL DEFAULT '',
    last_limit_time TEXT NOT NULL DEFAULT '',
    open_board_count INTEGER,
    score REAL NOT NULL DEFAULT 0,
    reason TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (trade_date, code)
);

CREATE INDEX IF NOT EXISTS idx_limit_up_candidates_trade_date ON limit_up_candidates(trade_date);
CREATE INDEX IF NOT EXISTS idx_limit_up_candidates_score ON limit_up_candidates(score);

CREATE TABLE IF NOT EXISTS sector_rotation_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    sector_type TEXT NOT NULL,
    sector_name TEXT NOT NULL,
    latest_close REAL,
    latest_pct_change REAL,
    return_5d REAL,
    return_10d REAL,
    position_60d REAL,
    activity_score REAL,
    rotation_score REAL,
    signal TEXT NOT NULL DEFAULT '',
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (trade_date, sector_type, sector_name)
);

CREATE INDEX IF NOT EXISTS idx_sector_rotation_trade_date ON sector_rotation_snapshots(trade_date);
CREATE INDEX IF NOT EXISTS idx_sector_rotation_score ON sector_rotation_snapshots(rotation_score);
"""


def get_db_path() -> Path:
    override = os.getenv("AI_FINANCE_DB_PATH", "").strip()
    return Path(override) if override else DEFAULT_DB_PATH


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_SQL)


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    db_path = get_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    _configure_connection(conn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
