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


def test_realtime_quotes_api(monkeypatch) -> None:
    def fake_fetch_realtime_quotes_best_effort(codes):
        assert codes == ["600519", "000001"]
        return (
            [
                {
                    "code": "600519",
                    "name": "贵州茅台",
                    "latest_price": 1354.55,
                    "pct_change": -0.5,
                    "source": "eastmoney",
                }
            ],
            [{"股票代码": "000001", "error": "未返回实时行情"}],
            "eastmoney",
        )

    monkeypatch.setattr(
        api_module.realtime_quote_service,
        "fetch_realtime_quotes_best_effort",
        fake_fetch_realtime_quotes_best_effort,
    )

    resp = client.post(
        "/api/market/realtime-quotes",
        json={"codes_text": "600519\n000001"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "eastmoney"
    assert body["count"] == 1
    assert body["requested_count"] == 2
    assert body["error_count"] == 1
    assert body["items"][0]["latest_price"] == 1354.55


def test_realtime_quotes_api_requires_codes() -> None:
    resp = client.post("/api/market/realtime-quotes", json={"codes": [], "codes_text": ""})
    assert resp.status_code == 400
    assert "至少提供一个股票代码" in resp.json()["detail"]


def test_default_watchlist_realtime_quotes_api(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module.watchlist_service,
        "get_default_watchlist",
        lambda: {
            "id": 1,
            "name": "默认股票池",
            "count": 2,
            "items": [{"code": "600519"}, {"code": "000001"}],
        },
    )

    def fake_fetch_realtime_quotes_best_effort(codes):
        assert codes == ["600519", "000001"]
        return (
            [{"code": "600519", "latest_price": 1354.55, "source": "eastmoney"}],
            [{"股票代码": "000001", "error": "未返回实时行情"}],
            "eastmoney",
        )

    monkeypatch.setattr(
        api_module.realtime_quote_service,
        "fetch_realtime_quotes_best_effort",
        fake_fetch_realtime_quotes_best_effort,
    )

    resp = client.get("/api/market/realtime-quotes/default")

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["requested_count"] == 2
    assert body["error_count"] == 1
    assert body["watchlist"]["name"] == "默认股票池"


def test_default_watchlist_realtime_quotes_requires_items(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module.watchlist_service,
        "get_default_watchlist",
        lambda: {"id": 1, "name": "默认股票池", "count": 0, "items": []},
    )

    resp = client.get("/api/market/realtime-quotes/default")

    assert resp.status_code == 400
    assert "默认股票池为空" in resp.json()["detail"]


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
        assert kwargs["min_score"] == 60
        assert kwargs["flow_fetcher"] is None
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "信号评分": 95,
                        "信号方向": "偏多",
                        "信号级别": "重点观察",
                        "观察结论": "重点观察",
                        "数据时效": "最近交易日",
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
            "min_score": 60,
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
    assert body["items"][0]["信号评分"] == 95
    assert body["signal_summary"]["observation_counts"] == {"重点观察": 1}
    assert body["signal_summary"]["freshness_counts"] == {"最近交易日": 1}
    assert body["errors"][0]["股票代码"] == "600487"


def test_daily_signals_can_include_flow(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_flow_rank_akshare_for_codes(codes, min_net_inflow, limit):
        captured["flow_codes"] = codes
        captured["min_net_inflow"] = min_net_inflow
        captured["limit"] = limit
        return pd.DataFrame([{"股票代码": "600592", "主力净流入_元": 30_000_000}])

    def fake_scan_stock_signal_events(**kwargs):
        captured["flow_fetcher"] = kwargs["flow_fetcher"]
        flow_df = kwargs["flow_fetcher"](["600592"])
        assert list(flow_df["股票代码"]) == ["600592"]
        return (
            pd.DataFrame(
                [
                    {
                        "股票代码": "600592",
                        "日期": "2026-04-08",
                        "收盘": 12.3,
                        "信号评分": 95,
                        "观察结论": "重点观察",
                        "资金流确认": "资金支持",
                    }
                ]
            ),
            [],
        )

    monkeypatch.setattr(api_module.tdx_service, "flow_rank_akshare_for_codes", fake_flow_rank_akshare_for_codes)
    monkeypatch.setattr(api_module.signal_service, "scan_stock_signal_events", fake_scan_stock_signal_events)

    resp = client.post(
        "/api/signals/daily",
        json={"codes_text": "600592", "include_flow": True},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["items"][0]["资金流确认"] == "资金支持"
    assert callable(captured["flow_fetcher"])
    assert captured["flow_codes"] == ["600592"]
    assert captured["min_net_inflow"] == float("-inf")
    assert captured["limit"] == 1


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
        assert kwargs["min_score"] == 70.0
        assert kwargs["mute_downgraded_strategies"] is True
        return {
            "watchlist": {"name": "默认股票池", "count": 2},
            "persisted_events": [{"summary": "MACD金叉", "code": "600519"}],
            "delivery_results": [{"channel": "stdout", "status": "delivered", "created": True}],
            "errors": [{"股票代码": "000001", "error": "network timeout"}],
            "requested_count": 2,
            "elapsed_seconds": 12.5,
            "min_score": kwargs["min_score"],
            "signal_summary": {"signals": 1, "observation_counts": {"重点观察": 1}},
            "strategy_guard": {"horizon": "T+1", "matched_count": 1, "total_count": 1, "mute_downgraded": True, "muted_count": 1},
            "scan_run": {"id": 7, "run_at": "2026-05-13 11:35:00", "status": "正常", "note": "扫描完成并生成信号"},
        }

    monkeypatch.setattr(api_module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)

    resp = client.post(
        "/api/signals/run-daily-job",
        json={"channel": "stdout", "min_score": 70, "mute_downgraded_strategies": True},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["requested_count"] == 2
    assert body["error_count"] == 1
    assert body["elapsed_seconds"] == 12.5
    assert body["min_score"] == 70.0
    assert body["signal_summary"]["signals"] == 1
    assert body["strategy_guard"]["muted_count"] == 1
    assert body["scan_run"]["id"] == 7
    assert body["scan_run"]["status"] == "正常"
    assert body["deliveries"][0]["channel"] == "stdout"
    assert body["errors"][0]["股票代码"] == "000001"
    assert body["review_after_scan"] is False


def test_run_daily_job_can_review_after_scan(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    review_called: dict[str, object] = {}

    def fake_run_default_watchlist_scan(**kwargs):
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
            "requested_count": 1,
            "elapsed_seconds": 1.2,
            "min_score": kwargs["min_score"],
            "signal_summary": {},
            "scan_run": {"id": 8, "run_at": "2026-05-13 11:35:00", "status": "无信号", "note": "没有新信号"},
        }

    def fake_backfill_review_snapshots(**kwargs):
        review_called["backfill"] = kwargs
        return {"count": 2, "errors": []}

    def fake_summarize_review_stats(**kwargs):
        review_called["stats"] = kwargs
        return [{"sample_count": 6, "strategy_confidence": "中"}]

    monkeypatch.setattr(api_module.scan_workflow, "run_default_watchlist_scan", fake_run_default_watchlist_scan)
    monkeypatch.setattr(api_module.review_service, "backfill_review_snapshots", fake_backfill_review_snapshots)
    monkeypatch.setattr(api_module.review_service, "summarize_review_stats", fake_summarize_review_stats)

    resp = client.post(
        "/api/signals/run-daily-job",
        json={
            "channel": "stdout",
            "review_after_scan": True,
            "review_trade_date": "2026-05-01",
            "review_horizons": [1, 3],
            "review_summary_horizon": "T+3",
        },
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["review_after_scan"] is True
    assert body["review_due_only"] is True
    assert body["review_result"]["count"] == 2
    assert body["review_stats"][0]["strategy_confidence"] == "中"
    assert body["review_error"] == ""
    assert body["scan_run"]["review_after_scan"] is True
    assert body["scan_run"]["review_snapshot_count"] == 2
    assert body["scan_run"]["review_stats_count"] == 1
    assert body["scan_run"]["review_error"] == ""
    assert review_called["backfill"]["trade_date"] == "2026-05-01"
    assert review_called["backfill"]["horizons"] == [1, 3]
    assert review_called["backfill"]["due_only"] is True
    assert review_called["stats"]["horizon"] == "T+3"


def test_scan_runs_api(monkeypatch) -> None:
    monkeypatch.setattr(
        api_module.scan_run_service,
        "list_scan_runs",
        lambda limit=50: [{"id": 1, "summary": {"signals": 1}}],
    )

    resp = client.get("/api/signals/scan-runs", params={"limit": 10})

    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["items"][0]["id"] == 1


def test_run_daily_job_passes_feishu_channel(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_run_default_watchlist_scan(**kwargs):
        assert kwargs["channel"] == "feishu_webhook"
        assert kwargs["min_score"] == 60.0
        return {
            "watchlist": {"name": "默认股票池", "count": 1},
            "persisted_events": [],
            "delivery_results": [],
            "errors": [],
            "requested_count": 1,
            "elapsed_seconds": 1.5,
            "min_score": kwargs["min_score"],
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


def test_bootstrap_default_watchlist_api(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def fake_bootstrap_default_watchlist(**kwargs):
        assert kwargs["index_code"] == "000300"
        return {
            "id": 1,
            "name": "默认股票池",
            "description": "默认关注股票列表",
            "is_default": True,
            "count": 1,
            "items": [{"code": "600519"}],
            "updated_at": "2026-04-09 12:00:00",
            "index_code": "000300",
            "source": "seed",
            "message": "指数成分股导入失败，已使用内置种子股票池",
        }

    monkeypatch.setattr(
        api_module.watchlist_service,
        "bootstrap_default_watchlist",
        fake_bootstrap_default_watchlist,
    )

    resp = client.post(
        "/api/watchlists/default/bootstrap",
        json={"index_code": "000300"},
    )
    assert resp.status_code == 200
    assert resp.json()["source"] == "seed"


def test_review_backfill_api(monkeypatch) -> None:
    def fake_backfill_review_snapshots(**kwargs):
        assert kwargs["horizons"] == [1, 3, 5]
        assert kwargs["due_only"] is True
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
        json={"horizons": [1, 3, 5], "due_only": True},
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


def test_strategy_summary_api(monkeypatch) -> None:
    def fake_summary(**kwargs):
        assert kwargs["horizon"] == "T+3"
        assert kwargs["limit"] == 20
        assert kwargs["min_samples"] == 5
        assert kwargs["actionable_only"] is True
        assert kwargs["data_source"] == "本地缓存"
        return {
            "horizon": "T+3",
            "total_count": 1,
            "filtered_count": 1,
            "actionable_count": 1,
            "filtered_actionable_count": 1,
            "min_samples": 5,
            "actionable_only": True,
            "data_source": "本地缓存",
            "verdict_counts": {"保留": 1},
            "confidence_counts": {"中": 1},
            "strategy_type_counts": {"日线信号": 1},
            "data_source_counts": {"本地缓存": 1},
            "next_action_counts": {"保留该分组": 1},
            "sample_gap_summary": {"needs_more_samples_count": 0, "total_samples_to_actionable": 0, "nearest_to_actionable": []},
            "review_backlog": {
                "total_count": 1,
                "reviewed_count": 1,
                "missing_count": 0,
                "due_missing_count": 0,
                "not_due_count": 0,
                "next_due_date": "",
                "review_now": False,
                "reviewed_ratio": 1.0,
            },
            "items": [
                {
                    "strategy_type": "日线信号",
                    "strategy_name": "60-80 / 偏多 / MACD金叉",
                    "strategy_verdict": "保留",
                    "strategy_confidence": "中",
                    "strategy_actionable": True,
                }
            ],
        }

    monkeypatch.setattr(api_module.strategy_summary_service, "summarize_strategy_decisions", fake_summary)

    resp = client.get(
        "/api/strategy/summary",
        params={
            "horizon": "T+3",
            "limit": 20,
            "min_samples": 5,
            "actionable_only": True,
            "data_source": "本地缓存",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 1
    assert body["filtered_count"] == 1
    assert body["actionable_count"] == 1
    assert body["filtered_actionable_count"] == 1
    assert body["min_samples"] == 5
    assert body["actionable_only"] is True
    assert body["data_source"] == "本地缓存"
    assert body["strategy_type_counts"] == {"日线信号": 1}
    assert body["data_source_counts"] == {"本地缓存": 1}
    assert body["next_action_counts"] == {"保留该分组": 1}
    assert body["sample_gap_summary"]["needs_more_samples_count"] == 0
    assert body["review_backlog"]["missing_count"] == 0
    assert body["review_backlog"]["due_missing_count"] == 0
    assert body["review_backlog"]["next_due_date"] == ""
    assert body["review_backlog"]["review_now"] is False
    assert body["items"][0]["strategy_verdict"] == "保留"


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
        assert kwargs["due_only"] is True
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
        json={"trade_date": "2026-05-12", "horizons": [1, 3, 5], "due_only": True},
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
