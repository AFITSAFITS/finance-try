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
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    signal_score = _clean_display(payload.get("signal_score"))
    observation_conclusion = _clean_display(payload.get("observation_conclusion"))
    data_freshness = _clean_display(payload.get("data_freshness"))
    data_source = _clean_display(payload.get("data_source"))
    risk_note = _clean_display(payload.get("risk_note"))
    position_60d = _format_number(payload.get("position_60d"))
    volume_ratio = _format_number(payload.get("volume_ratio"))
    relative_strength = _format_number(payload.get("relative_strength"))
    candlestick_pattern = _clean_display(payload.get("candlestick_pattern"))
    stop_loss_price = _format_number(payload.get("stop_loss_price"))
    target_price = _format_number(payload.get("target_price"))
    strategy_verdict = _clean_display(payload.get("strategy_verdict"))
    strategy_confidence = _clean_display(payload.get("strategy_confidence"))
    strategy_next_action = _clean_display(payload.get("strategy_next_action"))
    strategy_sample_count = _clean_display(payload.get("strategy_sample_count"))
    return (
        f"[{trade_date}] {code} {summary} | severity={severity} | "
        f"close={close_price} | pct_change={pct_change} | score={signal_score} | conclusion={observation_conclusion} | "
        f"data_freshness={data_freshness} | data_source={data_source} | "
        f"position_60d={position_60d} | volume_ratio={volume_ratio} | "
        f"relative_strength={relative_strength} | candlestick={candlestick_pattern} | "
        f"stop_loss={stop_loss_price} | target={target_price} | risk={risk_note} | "
        f"strategy={strategy_verdict} | strategy_confidence={strategy_confidence} | "
        f"strategy_samples={strategy_sample_count} | next_action={strategy_next_action}"
    )


def _clean_display(value: object, default: str = "-") -> str:
    if value is None:
        return default
    raw = str(value).strip()
    return raw if raw and raw.lower() != "nan" else default


def _format_number(value: object, suffix: str = "") -> str:
    if value is None:
        return "-"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return _clean_display(value)
    return f"{number:.2f}{suffix}"


def _feishu_severity_template(event: dict[str, Any]) -> str:
    severity = str(event.get("severity", "")).strip().lower()
    pct_change = event.get("pct_change")
    try:
        pct = float(pct_change)
    except (TypeError, ValueError):
        pct = 0.0
    if severity in {"high", "critical"} or pct >= 3:
        return "red"
    if pct > 0:
        return "green"
    if pct < 0:
        return "orange"
    return "blue"


def build_feishu_event_card_payload(event: dict[str, Any], secret: str = "") -> dict[str, Any]:
    code = _clean_display(event.get("code"))
    summary = _clean_display(event.get("summary"), "交易提醒")
    trade_date = _clean_display(event.get("trade_date"))
    indicator = _clean_display(event.get("indicator"))
    event_type = _clean_display(event.get("event_type"))
    severity = _clean_display(event.get("severity"))
    close_price = _format_number(event.get("close_price"))
    pct_change = _format_number(event.get("pct_change"), "%")
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
    signal_text = _clean_display(payload.get("signal"), summary)
    signal_score = _format_number(payload.get("signal_score"))
    signal_level = _clean_display(payload.get("signal_level"))
    observation_conclusion = _clean_display(payload.get("observation_conclusion"))
    data_freshness = _clean_display(payload.get("data_freshness"))
    data_lag_days = _format_number(payload.get("data_lag_days"))
    data_source = _clean_display(payload.get("data_source"))
    risk_note = _clean_display(payload.get("risk_note"))
    position_60d = _format_number(payload.get("position_60d"))
    volume_ratio = _format_number(payload.get("volume_ratio"))
    relative_strength = _format_number(payload.get("relative_strength"))
    candlestick_pattern = _clean_display(payload.get("candlestick_pattern"))
    stop_loss_price = _format_number(payload.get("stop_loss_price"))
    target_price = _format_number(payload.get("target_price"))
    risk_reward_ratio = _format_number(payload.get("risk_reward_ratio"))
    strategy_verdict = _clean_display(payload.get("strategy_verdict"))
    strategy_confidence = _clean_display(payload.get("strategy_confidence"))
    strategy_sample_count = _clean_display(payload.get("strategy_sample_count"))
    strategy_next_action = _clean_display(payload.get("strategy_next_action"))

    card_payload: dict[str, Any] = {
        "msg_type": "interactive",
        "card": {
            "config": {
                "wide_screen_mode": True,
                "enable_forward": True,
            },
            "header": {
                "template": _feishu_severity_template(event),
                "title": {
                    "tag": "plain_text",
                    "content": f"{code} {summary}",
                },
            },
            "elements": [
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**{signal_text}**",
                    },
                },
                {
                    "tag": "div",
                    "fields": [
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**日期**\n{trade_date}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**收盘价**\n{close_price}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**涨跌幅**\n{pct_change}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**强度**\n{severity}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**评分**\n{signal_score}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**级别**\n{signal_level}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**观察结论**\n{observation_conclusion}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**数据时效**\n{data_freshness}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**数据来源**\n{data_source}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**滞后天数**\n{data_lag_days}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**60日位置**\n{position_60d}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**量能比**\n{volume_ratio}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**相对强度**\n{relative_strength}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**K线形态**\n{candlestick_pattern}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**参考止损**\n{stop_loss_price}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**参考目标**\n{target_price}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**风险收益比**\n{risk_reward_ratio}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**策略结论**\n{strategy_verdict}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**结论可信度**\n{strategy_confidence}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**复盘样本**\n{strategy_sample_count}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**指标**\n{indicator}"}},
                        {"is_short": True, "text": {"tag": "lark_md", "content": f"**类型**\n{event_type}"}},
                    ],
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**风险提示**\n{risk_note}",
                    },
                },
                {
                    "tag": "div",
                    "text": {
                        "tag": "lark_md",
                        "content": f"**下一步动作**\n{strategy_next_action}",
                    },
                },
                {
                    "tag": "hr",
                },
                {
                    "tag": "note",
                    "elements": [
                        {
                            "tag": "plain_text",
                            "content": "来自 ai-finance 自动扫描，仅作候选观察。",
                        }
                    ],
                },
            ],
        },
    }
    return _sign_feishu_payload(card_payload, secret=secret)


def build_stdout_messages(events: Iterable[dict[str, Any]]) -> list[str]:
    return [format_event_message(event) for event in events]


def _sign_feishu_payload(payload: dict[str, Any], secret: str = "") -> dict[str, Any]:
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


def build_feishu_webhook_payload(text: str, secret: str = "") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "msg_type": "text",
        "content": {"text": text},
    }
    return _sign_feishu_payload(payload, secret=secret)


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
    payload = build_feishu_event_card_payload(event, secret=secret)
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
