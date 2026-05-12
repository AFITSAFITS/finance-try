from __future__ import annotations

import pandas as pd
from fastapi.testclient import TestClient

from app import api as api_module
from app import thsdk_service
from app.tdx_service import TdxUnavailableError

client = TestClient(api_module.app)


def test_health() -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_flow_rank_success(monkeypatch) -> None:
    def fake_flow_rank_tdx(**kwargs):
        assert kwargs["codes"] == ["600592", "600487"]
        return pd.DataFrame(
            [
                {
                    "股票代码": "600592",
                    "symbol": "600592.SH",
                    "HqDate": "2026-03-31",
                    "Zjl_HB": "19.60亿",
                    "主力净流入_元": 1_960_000_000,
                    "主力净流入(亿)": 19.6,
                }
            ]
        )

    monkeypatch.setattr(api_module.tdx_service, "flow_rank_tdx", fake_flow_rank_tdx)

    resp = client.post(
        "/api/tdx/flow-rank",
        json={
            "codes_text": "600592\n600487",
            "min_net_inflow": 0,
            "limit": 20,
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["股票代码"] == "600592"


def test_flow_rank_no_codes() -> None:
    resp = client.post("/api/tdx/flow-rank", json={"codes": [], "codes_text": ""})
    assert resp.status_code == 400
    assert "至少提供一个股票代码" in resp.json()["detail"]


def test_flow_rank_tdx_unavailable_fallback_to_akshare(monkeypatch) -> None:
    def fake_flow_rank_tdx(**kwargs):
        raise TdxUnavailableError("未安装 tqcenter")

    def fake_flow_rank_akshare_for_codes(**kwargs):
        return pd.DataFrame(
            [
                {
                    "股票代码": "600592",
                    "股票简称": "平潭发展",
                    "净额": "19.60亿",
                    "主力净流入_元": 1_960_000_000,
                    "主力净流入(亿)": 19.6,
                }
            ]
        )

    monkeypatch.setattr(api_module.tdx_service, "flow_rank_tdx", fake_flow_rank_tdx)
    monkeypatch.setattr(
        api_module.tdx_service,
        "flow_rank_akshare_for_codes",
        fake_flow_rank_akshare_for_codes,
    )

    resp = client.post(
        "/api/tdx/flow-rank",
        json={"codes": ["600592"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "akshare_fallback"
    assert body["count"] == 1
    assert body["items"][0]["股票代码"] == "600592"


def test_flow_rank_tdx_unavailable_no_fallback(monkeypatch) -> None:
    def fake_flow_rank_tdx(**kwargs):
        raise TdxUnavailableError("未安装 tqcenter")

    monkeypatch.setattr(api_module.tdx_service, "flow_rank_tdx", fake_flow_rank_tdx)

    resp = client.post(
        "/api/tdx/flow-rank",
        json={"codes": ["600592"], "fallback_to_akshare": False},
    )
    assert resp.status_code == 503
    assert "tqcenter" in resp.json()["detail"]


def test_thsdk_klines_success(monkeypatch) -> None:
    def fake_klines_thsdk(**kwargs):
        assert kwargs["symbol"] == "USZA300033"
        assert kwargs["count"] == 3
        return pd.DataFrame(
            [
                {"时间": "2026-03-31 00:00:00", "收盘价": 12.3},
                {"时间": "2026-04-01 00:00:00", "收盘价": 12.5},
            ]
        )

    monkeypatch.setattr(api_module.thsdk_service, "klines_thsdk", fake_klines_thsdk)

    resp = client.post(
        "/api/thsdk/klines",
        json={"symbol": "USZA300033", "count": 3},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "thsdk"
    assert body["count"] == 2
    assert body["items"][0]["收盘价"] == 12.3


def test_thsdk_klines_unavailable(monkeypatch) -> None:
    def fake_klines_thsdk(**kwargs):
        raise thsdk_service.ThsdkUnavailableError("thsdk python runtime not found")

    monkeypatch.setattr(api_module.thsdk_service, "klines_thsdk", fake_klines_thsdk)

    resp = client.post(
        "/api/thsdk/klines",
        json={"symbol": "USZA300033", "count": 3},
    )
    assert resp.status_code == 503
    assert "thsdk python" in resp.json()["detail"]


def test_thsdk_klines_empty_symbol() -> None:
    resp = client.post(
        "/api/thsdk/klines",
        json={"symbol": "   ", "count": 3},
    )
    assert resp.status_code == 400
    assert "symbol 不能为空" in resp.json()["detail"]


def test_daily_signals_success(monkeypatch) -> None:
    def fake_scan_stock_signal_events(**kwargs):
        assert kwargs["codes"] == ["600592", "600487"]
        assert kwargs["only_secondary_golden_cross"] is False
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "MACD信号": "MACD金叉",
                        "MACD形态": "水下金叉后水上再次金叉",
                        "均线信号": "MA5上穿MA20",
                        "信号": "MACD金叉, 水下金叉后水上再次金叉, MA5上穿MA20",
                    }
                ]
            ),
            [{"股票代码": "600487", "error": "network timeout"}],
        )

    monkeypatch.setattr(
        api_module.signal_service,
        "scan_stock_signal_events",
        fake_scan_stock_signal_events,
    )

    resp = client.post(
        "/api/signals/daily",
        json={
            "codes_text": "600592\n600487",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "akshare"
    assert body["count"] == 1
    assert body["requested_count"] == 2
    assert body["error_count"] == 1
    assert "elapsed_seconds" in body
    assert body["items"][0]["股票代码"] == "600592"
    assert body["errors"][0]["股票代码"] == "600487"


def test_daily_signals_secondary_golden_cross_filter(monkeypatch) -> None:
    def fake_scan_stock_signal_events(**kwargs):
        assert kwargs["only_secondary_golden_cross"] is True
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "MACD信号": "MACD金叉",
                        "MACD形态": "水下金叉后水上再次金叉",
                        "均线信号": None,
                        "信号": "MACD金叉, 水下金叉后水上再次金叉",
                    }
                ]
            ),
            [],
        )

    monkeypatch.setattr(api_module.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)

    resp = client.post(
        "/api/signals/daily",
        json={"codes": ["600592"], "only_secondary_golden_cross": True},
    )

    assert resp.status_code == 200
    assert resp.json()["count"] == 1


def test_daily_signals_no_codes() -> None:
    resp = client.post("/api/signals/daily", json={"codes": [], "codes_text": ""})
    assert resp.status_code == 400
    assert "至少提供一个股票代码" in resp.json()["detail"]


def test_default_watchlist_api_roundtrip(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    save_resp = client.post(
        "/api/watchlists/default",
        json={"codes_text": "600519\n000001"},
    )
    assert save_resp.status_code == 200
    assert save_resp.json()["count"] == 2

    load_resp = client.get("/api/watchlists/default")
    assert load_resp.status_code == 200
    assert [item["code"] for item in load_resp.json()["items"]] == ["600519", "000001"]


def test_scan_default_watchlist_persists_events(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    client.post(
        "/api/watchlists/default",
        json={"codes_text": "600592\n600487"},
    )

    def fake_scan_stock_signal_events(**kwargs):
        assert kwargs["codes"] == ["600592", "600487"]
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "涨跌幅": 1.1,
                        "DIF": 0.12,
                        "DEA": 0.08,
                        "MACD信号": "MACD金叉",
                        "MA5": 12.1,
                        "MA20": 11.6,
                        "均线信号": "MA5上穿MA20",
                        "信号": "MACD金叉, MA5上穿MA20",
                    }
                ]
            ),
            [{"股票代码": "600487", "error": "network timeout"}],
        )

    monkeypatch.setattr(
        api_module.signal_service,
        "scan_stock_signal_events",
        fake_scan_stock_signal_events,
    )

    scan_resp = client.post("/api/signals/scan-default", json={})
    assert scan_resp.status_code == 200
    scan_body = scan_resp.json()
    assert scan_body["count"] == 2
    assert scan_body["requested_count"] == 2
    assert scan_body["error_count"] == 1
    assert "elapsed_seconds" in scan_body
    assert scan_body["errors"][0]["股票代码"] == "600487"
    assert {item["summary"] for item in scan_body["items"]} == {"MACD金叉", "MA5上穿MA20"}

    history_resp = client.get("/api/signals/events", params={"trade_date": "2026-04-08"})
    assert history_resp.status_code == 200
    history_body = history_resp.json()
    assert history_body["count"] == 2
    assert history_body["items"][0]["payload"]["close"] == 12.3


def test_scan_default_watchlist_requires_items(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    resp = client.post("/api/signals/scan-default", json={})
    assert resp.status_code == 400
    assert "默认股票池为空" in resp.json()["detail"]


def test_run_daily_job_returns_deliveries(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_run_default_watchlist_scan(**kwargs):
        assert kwargs["channel"] == "stdout"
        return {
            "watchlist": {"name": "默认股票池", "count": 2},
            "persisted_events": [{"summary": "MACD金叉", "code": "600519"}],
            "delivery_results": [{"channel": "stdout", "status": "delivered", "created": True}],
            "errors": [{"股票代码": "000001", "error": "network timeout"}],
            "requested_count": 2,
            "elapsed_seconds": 12.5,
        }

    monkeypatch.setattr(api_module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)

    resp = client.post(
        "/api/signals/run-daily-job",
        json={"channel": "stdout"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["requested_count"] == 2
    assert body["error_count"] == 1
    assert body["elapsed_seconds"] == 12.5
    assert body["deliveries"][0]["channel"] == "stdout"
    assert body["errors"][0]["股票代码"] == "000001"


def test_run_daily_job_passes_feishu_channel(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_run_default_watchlist_scan(**kwargs):
        assert kwargs["channel"] == "feishu_webhook"
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
            "requested_count": 1,
            "elapsed_seconds": 1.5,
        }

    monkeypatch.setattr(api_module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)

    resp = client.post(
        "/api/signals/run-daily-job",
        json={"channel": "feishu_webhook"},
    )

    assert resp.status_code == 200
    assert resp.json()["requested_count"] == 1


def test_import_default_watchlist_index_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_import_default_watchlist_from_index(**kwargs):
        assert kwargs["index_code"] == "000300"
        return {
            "id": 1,
            "name": "默认股票池",
            "description": "默认关注股票列表",
            "is_default": True,
            "count": 2,
            "items": [{"code": "600519"}, {"code": "000001"}],
            "updated_at": "2026-04-09 12:00:00",
            "index_code": "000300",
        }

    monkeypatch.setattr(
        api_module.watchlist_service,
        "import_default_watchlist_from_index",
        fake_import_default_watchlist_from_index,
    )

    resp = client.post(
        "/api/watchlists/default/import-index",
        json={"index_code": "000300"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert body["index_code"] == "000300"


def test_review_backfill_api(monkeypatch) -> None:
    def fake_backfill_review_snapshots(**kwargs):
        assert kwargs["horizons"] == [1, 3, 5]
        return {
            "count": 2,
            "items": [
                {
                    "signal_event_id": 1,
                    "trade_date": "2026-04-08",
                    "code": "600519",
                    "summary": "MACD金叉",
                    "horizon": "T+3",
                    "pct_return": 20.0,
                    "max_drawdown": -10.0,
                }
            ],
            "errors": [],
        }

    monkeypatch.setattr(api_module.review_service, "backfill_review_snapshots", fake_backfill_review_snapshots)

    resp = client.post(
        "/api/reviews/backfill",
        json={"horizons": [1, 3, 5]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 2
    assert body["items"][0]["horizon"] == "T+3"


def test_review_stats_api(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module.review_service,
        "summarize_review_stats",
        lambda **kwargs: [
            {
                "summary": "MACD金叉",
                "indicator": "MACD",
                "event_type": "golden_cross",
                "sample_count": 3,
                "avg_return": 4.2,
                "win_rate": 0.67,
                "avg_max_drawdown": -2.5,
            }
        ],
    )

    resp = client.get("/api/reviews/stats", params={"horizon": "T+3"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["summary"] == "MACD金叉"


def test_limit_up_breakthrough_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_scan_and_save_limit_up_breakthroughs(**kwargs):
        assert kwargs["trade_date"] == "2026-05-12"
        assert kwargs["min_score"] == 50
        return {
            "trade_date": "2026-05-12",
            "count": 1,
            "items": [{"code": "600001", "score": 88, "reason": "突破近60日收盘高点"}],
            "errors": [],
        }

    monkeypatch.setattr(
        api_module.limit_up_service,
        "scan_and_save_limit_up_breakthroughs",
        fake_scan_and_save_limit_up_breakthroughs,
    )

    resp = client.post(
        "/api/limit-up/breakthroughs",
        json={"trade_date": "2026-05-12", "min_score": 50},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["code"] == "600001"


def test_sector_rotation_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_scan_and_save_sector_rotation(**kwargs):
        assert kwargs["trade_date"] == "2026-05-12"
        assert kwargs["sector_type"] == "industry"
        return {
            "trade_date": "2026-05-12",
            "count": 1,
            "items": [{"sector_name": "软件服务", "signal": "活跃低位"}],
            "errors": [],
        }

    monkeypatch.setattr(
        api_module.sector_rotation_service,
        "scan_and_save_sector_rotation",
        fake_scan_and_save_sector_rotation,
    )

    resp = client.post(
        "/api/sectors/rotation",
        json={"trade_date": "2026-05-12", "sector_type": "industry"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["signal"] == "活跃低位"


def test_sector_rotation_trends_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_list_sector_rotation_trends(**kwargs):
        assert kwargs["sector_type"] == "industry"
        assert kwargs["sector_names"] == ["软件服务", "半导体"]
        assert kwargs["start_date"] == "2026-05-01"
        assert kwargs["end_date"] == "2026-05-12"
        return [
            {
                "trade_date": "2026-05-12",
                "sector_name": "软件服务",
                "rotation_score": 86.0,
            }
        ]

    monkeypatch.setattr(
        api_module.sector_rotation_service,
        "list_sector_rotation_trends",
        fake_list_sector_rotation_trends,
    )

    resp = client.get(
        "/api/sectors/rotation/trends",
        params={
            "sector_type": "industry",
            "sector_names": "软件服务,半导体",
            "start_date": "2026-05-01",
            "end_date": "2026-05-12",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["rotation_score"] == 86.0


def test_limit_up_review_backfill_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_backfill_limit_up_review_snapshots(**kwargs):
        assert kwargs["trade_date"] == "2026-05-12"
        assert kwargs["horizons"] == [1, 3, 5]
        return {
            "count": 1,
            "items": [{"code": "600001", "horizon": "T+3", "pct_return": 12.5}],
            "errors": [],
        }

    monkeypatch.setattr(
        api_module.limit_up_service,
        "backfill_limit_up_review_snapshots",
        fake_backfill_limit_up_review_snapshots,
    )

    resp = client.post(
        "/api/limit-up/reviews/backfill",
        json={"trade_date": "2026-05-12", "horizons": [1, 3, 5]},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["pct_return"] == 12.5


def test_limit_up_review_stats_api(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module.limit_up_service,
        "summarize_limit_up_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "80+",
                "sample_count": 3,
                "avg_return": 8.2,
                "win_rate": 0.67,
                "avg_max_drawdown": -3.1,
                "avg_sector_limit_up_count": 4.0,
                "horizon": "T+3",
            }
        ],
    )

    resp = client.get("/api/limit-up/reviews/stats", params={"horizon": "T+3"})

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["score_bucket"] == "80+"
