from __future__ import annotations

import base64
import hashlib
import hmac
import os
import sqlite3
import time
from typing import Any, Iterable

import requests

from app import db
from app import tdx_service


def format_event_message(event: dict[str, Any]) -> str:
    code = str(event.get("code", ""))
    trade_date = str(event.get("trade_date", ""))
    summary = str(event.get("summary", ""))
    severity = str(event.get("severity", ""))
    close_price = event.get("close_price")
    pct_change = event.get("pct_change")
    return (
        f"[{trade_date}] {code} {summary} | severity={severity} | "
        f"close={close_price} | pct_change={pct_change}"
    )


def build_stdout_messages(events: Iterable[dict[str, Any]]) -> list[str]:
    return [format_event_message(event) for event in events]


def build_feishu_webhook_payload(text: str, secret: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": text},
    }
    normalized_secret = secret.strip()
    if normalized_secret:
        timestamp = str(int(time.time()))
        string_to_sign = f"{timestamp}\n{normalized_secret}".encode("utf-8")
        sign = base64.b64encode(
            hmac.new(string_to_sign, b"", digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        payload["timestamp"] = timestamp
        payload["sign"] = sign
    return payload


def send_feishu_webhook_message(
    event: dict[str, Any],
    webhook_url: str,
    secret: str = "",
    requester=None,
) -> str:
    normalized_webhook = webhook_url.strip()
    if not normalized_webhook:
        raise ValueError("AI_FINANCE_FEISHU_WEBHOOK 未配置")

    request_fn = requester or requests.post
    payload = build_feishu_webhook_payload(format_event_message(event), secret=secret)
    response = request_fn(
        normalized_webhook,
        json=payload,
        timeout=10,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("StatusCode", 0) not in {0, None}:
        raise RuntimeError(body.get("StatusMessage", "feishu webhook send failed"))
    if body.get("code", 0) not in {0, None}:
        raise RuntimeError(body.get("msg", "feishu webhook send failed"))
    return str(body.get("msg", ""))


def _row_to_delivery(row: sqlite3.Row, created: bool) -> dict[str, Any]:
    return {
        "id": row["id"],
        "signal_event_id": row["signal_event_id"],
        "channel": row["channel"],
        "status": row["status"],
        "delivered_at": row["delivered_at"],
        "message_id": row["message_id"],
        "error_message": row["error_message"],
        "created": created,
    }


def deliver_signal_events(
    events: Iterable[dict[str, Any]],
    channel: str = "stdout",
) -> list[dict[str, Any]]:
    normalized_channel = channel.strip() or "stdout"
    results: list[dict[str, Any]] = []

    with db.get_connection() as conn:
        for event in events:
            event_id = int(event["id"])
            delivered_at = tdx_service.now_ts()
            existing = conn.execute(
                """
                SELECT id, signal_event_id, channel, status, delivered_at, message_id, error_message
                FROM notification_deliveries
                WHERE signal_event_id = ? AND channel = ?
                """,
                (event_id, normalized_channel),
            ).fetchone()

            if existing is not None and existing["status"] == "delivered":
                results.append(_row_to_delivery(existing, created=False))
                continue

            try:
                message_id = ""
                if normalized_channel == "stdout":
                    status = "delivered"
                    error_message = ""
                elif normalized_channel == "feishu_webhook":
                    webhook_url = os.getenv("AI_FINANCE_FEISHU_WEBHOOK", "")
                    secret = os.getenv("AI_FINANCE_FEISHU_SECRET", "")
                    message_id = send_feishu_webhook_message(
                        event,
                        webhook_url=webhook_url,
                        secret=secret,
                    )
                    status = "delivered"
                    error_message = ""
                else:
                    raise ValueError(f"不支持的通知渠道: {normalized_channel}")
            except Exception as exc:  # noqa: BLE001
                status = "failed"
                message_id = ""
                error_message = str(exc)

            if existing is None:
                cursor = conn.execute(
                    """
                    INSERT INTO notification_deliveries (
                        signal_event_id,
                        channel,
                        status,
                        delivered_at,
                        message_id,
                        error_message
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (event_id, normalized_channel, status, delivered_at, message_id, error_message),
                )
                row_id = int(cursor.lastrowid)
                row = conn.execute(
                    """
                    SELECT id, signal_event_id, channel, status, delivered_at, message_id, error_message
                    FROM notification_deliveries
                    WHERE id = ?
                    """,
                    (row_id,),
                ).fetchone()
                assert row is not None
                results.append(_row_to_delivery(row, created=True))
            else:
                conn.execute(
                    """
                    UPDATE notification_deliveries
                    SET status = ?, delivered_at = ?, message_id = ?, error_message = ?
                    WHERE id = ?
                    """,
                    (status, delivered_at, message_id, error_message, existing["id"]),
                )
                row = conn.execute(
                    """
                    SELECT id, signal_event_id, channel, status, delivered_at, message_id, error_message
                    FROM notification_deliveries
                    WHERE id = ?
                    """,
                    (existing["id"],),
                ).fetchone()
                assert row is not None
                results.append(_row_to_delivery(row, created=False))
    return results


def list_notification_deliveries(channel: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[object] = []
    if channel:
        clauses.append("channel = ?")
        params.append(channel)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(int(limit))

    with db.get_connection() as conn:
        rows = conn.execute(
            f"""
            SELECT id, signal_event_id, channel, status, delivered_at, message_id, error_message
            FROM notification_deliveries
            {where_sql}
            ORDER BY delivered_at DESC, id DESC
            LIMIT ?
            """,
            tuple(params),
        ).fetchall()
    return [_row_to_delivery(row, created=False) for row in rows]
