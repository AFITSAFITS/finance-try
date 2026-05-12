from __future__ import annotations

import requests
import pandas as pd

from app import event_service
from app import notification_service


def test_deliver_signal_events_dedupes_by_channel(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    df = pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "日期": "2026-04-08",
                "收盘": 1530.25,
                "涨跌幅": 1.82,
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MA5": 1520.1,
                "MA20": 1498.3,
                "均线信号": None,
                "信号": "MACD金叉",
            }
        ]
    )
    events = event_service.persist_signal_rows(df)

    first = notification_service.deliver_signal_events(events, channel="stdout")
    second = notification_service.deliver_signal_events(events, channel="stdout")

    assert len(first) == 1
    assert {item["created"] for item in first} == {True}
    assert {item["status"] for item in first} == {"delivered"}

    assert len(second) == 1
    assert {item["created"] for item in second} == {False}
    assert {item["status"] for item in second} == {"delivered"}

    deliveries = notification_service.list_notification_deliveries(channel="stdout")
    assert len(deliveries) == 1


def test_build_stdout_messages_formats_events() -> None:
    messages = notification_service.build_stdout_messages(
        [
            {
                "id": 1,
                "trade_date": "2026-04-08",
                "code": "600519",
                "summary": "MACD金叉",
                "severity": "high",
                "close_price": 1530.25,
                "pct_change": 1.82,
                "payload": {
                    "signal_score": 75,
                    "position_60d": 0.42,
                    "volume_ratio": 1.6,
                    "stop_loss_price": 1450.0,
                    "target_price": 1690.0,
                    "risk_note": "无明显风险",
                },
            }
        ]
    )

    assert len(messages) == 1
    assert "600519" in messages[0]
    assert "MACD金叉" in messages[0]
    assert "score=75" in messages[0]
    assert "position_60d=0.42" in messages[0]
    assert "stop_loss=1450.00" in messages[0]
    assert "target=1690.00" in messages[0]
    assert "risk=无明显风险" in messages[0]


def test_build_feishu_webhook_payload_adds_signature(monkeypatch) -> None:
    monkeypatch.setattr(notification_service.time, "time", lambda: 1_710_000_000)

    payload = notification_service.build_feishu_webhook_payload("hello", secret="secret")

    assert payload["msg_type"] == "text"
    assert payload["content"]["text"] == "hello"
    assert payload["timestamp"] == "1710000000"
    assert payload["sign"] == "jWsBkWnzlRKtaP+iZgwraSojMWik4cJR7aysApQZuoA="


def test_build_feishu_event_card_payload_formats_event(monkeypatch) -> None:
    monkeypatch.setattr(notification_service.time, "time", lambda: 1_710_000_000)

    payload = notification_service.build_feishu_event_card_payload(
        {
            "trade_date": "2026-05-12",
            "code": "600519",
            "summary": "MACD金叉",
            "severity": "high",
            "indicator": "MACD",
            "event_type": "golden_cross",
            "close_price": 1530.25,
            "pct_change": 3.2,
            "payload": {
                "signal": "MACD金叉, MA5上穿MA20",
                "signal_score": 88,
                "signal_level": "重点观察",
                "position_60d": 0.35,
                "volume_ratio": 1.8,
                "stop_loss_price": 1450.0,
                "target_price": 1690.0,
                "risk_reward_ratio": 2.0,
                "risk_note": "无明显风险",
            },
        },
        secret="secret",
    )

    assert payload["msg_type"] == "interactive"
    assert payload["card"]["header"]["template"] == "red"
    assert payload["card"]["header"]["title"]["content"] == "600519 MACD金叉"
    assert payload["card"]["elements"][0]["text"]["content"] == "**MACD金叉, MA5上穿MA20**"
    fields = payload["card"]["elements"][1]["fields"]
    field_text = "\n".join(str(item["text"]["content"]) for item in fields)
    assert "**评分**\n88.00" in field_text
    assert "**级别**\n重点观察" in field_text
    assert "**60日位置**\n0.35" in field_text
    assert "**量能比**\n1.80" in field_text
    assert "**参考止损**\n1450.00" in field_text
    assert "**参考目标**\n1690.00" in field_text
    assert "**风险收益比**\n2.00" in field_text
    assert payload["card"]["elements"][2]["text"]["content"] == "**风险提示**\n无明显风险"
    assert payload["timestamp"] == "1710000000"
    assert payload["sign"] == "jWsBkWnzlRKtaP+iZgwraSojMWik4cJR7aysApQZuoA="


def test_deliver_signal_events_posts_to_feishu_webhook(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    monkeypatch.setenv("AI_FINANCE_FEISHU_WEBHOOK", "https://open.feishu.cn/open-apis/bot/v2/hook/test")
    df = pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "日期": "2026-04-08",
                "收盘": 1530.25,
                "涨跌幅": 1.82,
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MA5": 1520.1,
                "MA20": 1498.3,
                "均线信号": None,
                "信号": "MACD金叉",
            }
        ]
    )
    events = event_service.persist_signal_rows(df)
    calls: list[dict[str, object]] = []

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"StatusCode": 0}

    def fake_post(url: str, json: dict[str, object], timeout: int):
        calls.append({"url": url, "json": json, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr(notification_service.requests, "post", fake_post)

    deliveries = notification_service.deliver_signal_events(events, channel="feishu_webhook")

    assert len(deliveries) == 1
    assert {item["status"] for item in deliveries} == {"delivered"}
    assert len(calls) == 1
    assert calls[0]["url"] == "https://open.feishu.cn/open-apis/bot/v2/hook/test"
    assert calls[0]["json"]["msg_type"] == "interactive"
    assert "600519" in calls[0]["json"]["card"]["header"]["title"]["content"]


def test_deliver_signal_events_retries_failed_feishu_webhook(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    monkeypatch.setenv("AI_FINANCE_FEISHU_WEBHOOK", "https://open.feishu.cn/open-apis/bot/v2/hook/test")
    df = pd.DataFrame(
        [
            {
                "股票代码": "600519",
                "日期": "2026-04-08",
                "收盘": 1530.25,
                "涨跌幅": 1.82,
                "DIF": 1.2034,
                "DEA": 1.1028,
                "MACD信号": "MACD金叉",
                "MA5": 1520.1,
                "MA20": 1498.3,
                "均线信号": None,
                "信号": "MACD金叉",
            }
        ]
    )
    events = event_service.persist_signal_rows(df[:1])
    attempts = {"count": 0}

    class FakeResponse:
        def __init__(self, should_fail: bool) -> None:
            self.should_fail = should_fail

        def raise_for_status(self) -> None:
            if self.should_fail:
                raise requests.HTTPError("bad gateway")

        def json(self) -> dict[str, object]:
            return {"StatusCode": 0}

    def fake_post(url: str, json: dict[str, object], timeout: int):
        attempts["count"] += 1
        return FakeResponse(should_fail=attempts["count"] == 1)

    monkeypatch.setattr(notification_service.requests, "post", fake_post)

    first = notification_service.deliver_signal_events(events, channel="feishu_webhook")
    second = notification_service.deliver_signal_events(events, channel="feishu_webhook")

    assert first[0]["status"] == "failed"
    assert second[0]["status"] == "delivered"
    assert attempts["count"] == 2
