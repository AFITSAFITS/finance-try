from __future__ import annotations

import time
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from app import bar_service
from app import event_service
from app import limit_up_service
from app import notification_service
from app import realtime_quote_service
from app import review_service
from app import scan_run_service
from app import scan_workflow
from app import sector_rotation_service
from app import signal_service
from app import strategy_summary_service
from app import thsdk_service
from app import tdx_service
from app import watchlist_service

app = FastAPI(title="AI Finance TDX API", version="0.1.0")


class FlowRankRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    codes_text: str = ""
    fields: list[str] = Field(default_factory=lambda: list(tdx_service.DEFAULT_FLOW_FIELDS))
    inflow_field: str = "Zjl_HB"
    min_net_inflow: float = 0.0
    limit: int = Field(default=20, ge=1, le=500)
    fallback_to_akshare: bool = True


class MoreInfoRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    codes_text: str = ""
    fields: list[str] = Field(default_factory=lambda: ["HqDate", "Zjl", "Zjl_HB"])


class RealtimeQuotesRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    codes_text: str = ""


class ThsdkKlinesRequest(BaseModel):
    symbol: str
    count: int = Field(default=100, ge=1, le=5000)


class DailySignalsRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    codes_text: str = ""
    lookback_days: int = Field(default=180, ge=30, le=2000)
    adjust: str = "qfq"
    max_workers: int = Field(default=8, ge=1, le=32)
    only_secondary_golden_cross: bool = False
    min_score: float | None = Field(default=None, ge=0, le=100)


class DefaultWatchlistRequest(BaseModel):
    codes: list[str] = Field(default_factory=list)
    codes_text: str = ""


class ImportIndexWatchlistRequest(BaseModel):
    index_code: str = "000300"


class ScanDefaultSignalsRequest(BaseModel):
    lookback_days: int = Field(default=180, ge=30, le=2000)
    adjust: str = "qfq"
    max_workers: int = Field(default=8, ge=1, le=32)


class RunDailyJobRequest(BaseModel):
    lookback_days: int = Field(default=180, ge=30, le=2000)
    adjust: str = "qfq"
    channel: str = "stdout"
    max_workers: int = Field(default=8, ge=1, le=32)
    min_score: float = Field(default=60, ge=0, le=100)
    review_after_scan: bool = False
    review_trade_date: str = ""
    review_horizons: list[int] = Field(default_factory=lambda: [1, 3, 5])
    review_summary_horizon: str = "T+3"
    review_due_only: bool = True
    strategy_guard_horizon: str = "T+1"


class BackfillReviewsRequest(BaseModel):
    trade_date: str = ""
    code: str = ""
    horizons: list[int] = Field(default_factory=lambda: [1, 3, 5])
    adjust: str = "qfq"
    due_only: bool = False


class LimitUpBreakthroughRequest(BaseModel):
    trade_date: str = ""
    lookback_days: int = Field(default=120, ge=30, le=1000)
    min_score: float = Field(default=50, ge=0, le=100)
    max_items: int = Field(default=100, ge=1, le=500)
    pool_limit: int = Field(default=200, ge=1, le=1000)


class LimitUpReviewRequest(BaseModel):
    trade_date: str = ""
    code: str = ""
    horizons: list[int] = Field(default_factory=lambda: [1, 3, 5])
    adjust: str = "qfq"
    due_only: bool = False


class SectorRotationRequest(BaseModel):
    trade_date: str = ""
    sector_type: str = "industry"
    top_n: int = Field(default=30, ge=1, le=200)
    max_items: int = Field(default=20, ge=1, le=100)


def merge_codes(codes: list[str], codes_text: str) -> list[str]:
    combined = []
    combined.extend(codes)
    combined.extend(tdx_service.parse_codes_text(codes_text))
    return tdx_service.dedupe_keep_order(tdx_service.normalize_codes(combined))


def select_newly_delivered_events(
    events: list[dict[str, Any]],
    deliveries: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    created_deliveries = [item for item in deliveries if item.get("created")]
    if not created_deliveries:
        return []

    created_ids = {
        int(item["signal_event_id"])
        for item in created_deliveries
        if item.get("signal_event_id") is not None
    }
    if created_ids:
        return [event for event in events if event.get("id") is not None and int(event["id"]) in created_ids]
    return list(events)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "provider": "TongDaXin",
        "as_of": tdx_service.now_ts(),
    }


@app.post("/api/tdx/flow-rank")
def api_flow_rank(req: FlowRankRequest) -> dict[str, Any]:
    try:
        codes = merge_codes(req.codes, req.codes_text)
        if not codes:
            raise ValueError("至少提供一个股票代码")

        try:
            df = tdx_service.flow_rank_tdx(
                codes=codes,
                fields=req.fields,
                inflow_field=req.inflow_field.strip(),
                min_net_inflow=float(req.min_net_inflow),
                limit=int(req.limit),
            )
            items = tdx_service.dataframe_to_records(df)
            return {
                "as_of": tdx_service.now_ts(),
                "count": len(items),
                "items": items,
                "source": "tdx",
            }
        except tdx_service.TdxUnavailableError as tdx_exc:
            if not req.fallback_to_akshare:
                raise HTTPException(status_code=503, detail=str(tdx_exc)) from tdx_exc

            try:
                fallback_df = tdx_service.flow_rank_akshare_for_codes(
                    codes=codes,
                    min_net_inflow=float(req.min_net_inflow),
                    limit=int(req.limit),
                )
                items = tdx_service.dataframe_to_records(fallback_df)
                return {
                    "as_of": tdx_service.now_ts(),
                    "count": len(items),
                    "items": items,
                    "source": "akshare_fallback",
                    "warning": str(tdx_exc),
                }
            except Exception as fallback_exc:  # noqa: BLE001
                raise HTTPException(
                    status_code=503,
                    detail=f"{tdx_exc}；且 AkShare 兜底失败: {fallback_exc}",
                ) from fallback_exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/tdx/more-info")
def api_more_info(req: MoreInfoRequest) -> dict[str, Any]:
    try:
        codes = merge_codes(req.codes, req.codes_text)
        if not codes:
            raise ValueError("至少提供一个股票代码")

        df = tdx_service.more_info_tdx(codes=codes, fields=req.fields)
        items = tdx_service.dataframe_to_records(df)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except tdx_service.TdxUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/market/realtime-quotes")
def api_realtime_quotes(req: RealtimeQuotesRequest) -> dict[str, Any]:
    try:
        codes = merge_codes(req.codes, req.codes_text)
        if not codes:
            raise ValueError("至少提供一个股票代码")
        items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(codes)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "requested_count": len(codes),
            "error_count": len(errors),
            "items": items,
            "errors": errors,
            "source": source,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/market/realtime-quotes/default")
def api_default_watchlist_realtime_quotes() -> dict[str, Any]:
    try:
        watchlist = watchlist_service.get_default_watchlist()
        codes = [str(item["code"]) for item in watchlist["items"]]
        if not codes:
            raise ValueError("默认股票池为空，请先保存股票代码")
        items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(codes)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "requested_count": len(codes),
            "error_count": len(errors),
            "items": items,
            "errors": errors,
            "source": source,
            "watchlist": {
                "id": watchlist["id"],
                "name": watchlist["name"],
                "count": watchlist["count"],
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/thsdk/klines")
def api_thsdk_klines(req: ThsdkKlinesRequest) -> dict[str, Any]:
    try:
        symbol = req.symbol.strip()
        if not symbol:
            raise ValueError("symbol 不能为空")

        df = thsdk_service.klines_thsdk(symbol=symbol, count=int(req.count))
        items = tdx_service.dataframe_to_records(df)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
            "source": "thsdk",
        }
    except thsdk_service.ThsdkUnavailableError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/signals/daily")
def api_daily_signals(req: DailySignalsRequest) -> dict[str, Any]:
    try:
        codes = merge_codes(req.codes, req.codes_text)
        if not codes:
            raise ValueError("至少提供一个股票代码")

        started_at = time.perf_counter()
        df, errors = signal_service.scan_stock_signal_events(
            codes=codes,
            lookback_days=int(req.lookback_days),
            adjust=req.adjust.strip(),
            fetcher=bar_service.fetch_daily_history_cached,
            max_workers=int(req.max_workers),
            only_secondary_golden_cross=bool(req.only_secondary_golden_cross),
            min_score=req.min_score,
        )
        items = tdx_service.dataframe_to_records(df)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "requested_count": len(codes),
            "error_count": len(errors),
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "signal_summary": signal_service.summarize_signal_rows(df, errors),
            "items": items,
            "errors": errors,
            "source": "akshare",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/watchlists/default")
def api_get_default_watchlist() -> dict[str, Any]:
    return watchlist_service.get_default_watchlist()


@app.post("/api/watchlists/default")
def api_update_default_watchlist(req: DefaultWatchlistRequest) -> dict[str, Any]:
    codes = merge_codes(req.codes, req.codes_text)
    return watchlist_service.replace_default_watchlist_items(codes)


@app.post("/api/watchlists/default/import-index")
def api_import_default_watchlist(req: ImportIndexWatchlistRequest) -> dict[str, Any]:
    try:
        return watchlist_service.import_default_watchlist_from_index(
            index_code=req.index_code.strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/watchlists/default/bootstrap")
def api_bootstrap_default_watchlist(req: ImportIndexWatchlistRequest) -> dict[str, Any]:
    try:
        return watchlist_service.bootstrap_default_watchlist(
            index_code=req.index_code.strip(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/signals/scan-default")
def api_scan_default_signals(req: ScanDefaultSignalsRequest) -> dict[str, Any]:
    try:
        watchlist = watchlist_service.get_default_watchlist()
        codes = [str(item["code"]) for item in watchlist["items"]]
        if not codes:
            raise ValueError("默认股票池为空，请先保存股票代码")

        started_at = time.perf_counter()
        df, errors = signal_service.scan_stock_signal_events(
            codes=codes,
            lookback_days=int(req.lookback_days),
            adjust=req.adjust.strip(),
            fetcher=bar_service.fetch_daily_history_cached,
            max_workers=int(req.max_workers),
        )
        items = event_service.persist_signal_rows(df)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "requested_count": len(codes),
            "error_count": len(errors),
            "elapsed_seconds": round(time.perf_counter() - started_at, 3),
            "items": items,
            "errors": errors,
            "source": "akshare",
            "watchlist": {
                "id": watchlist["id"],
                "name": watchlist["name"],
                "count": watchlist["count"],
            },
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/signals/events")
def api_list_signal_events(
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = Query(default=100, ge=1, le=500),
) -> dict[str, Any]:
    try:
        items = event_service.list_signal_events(
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/signals/run-daily-job")
def api_run_daily_job(req: RunDailyJobRequest) -> dict[str, Any]:
    try:
        result = scan_workflow.run_default_watchlist_scan(
            lookback_days=int(req.lookback_days),
            adjust=req.adjust.strip(),
            channel=req.channel.strip() or "stdout",
            max_workers=int(req.max_workers),
            min_score=float(req.min_score),
            strategy_guard_horizon=req.strategy_guard_horizon.strip() or "T+1",
        )
        review_result: dict[str, Any] | None = None
        review_stats: list[dict[str, Any]] = []
        review_error = ""
        if req.review_after_scan:
            try:
                review_result = review_service.backfill_review_snapshots(
                    trade_date=req.review_trade_date.strip() or None,
                    horizons=review_service.parse_horizons(req.review_horizons),
                    adjust=req.adjust.strip(),
                    due_only=bool(req.review_due_only),
                )
                review_stats = review_service.summarize_review_stats(
                    horizon=req.review_summary_horizon.strip() or "T+3",
                    trade_date=req.review_trade_date.strip() or None,
                )
            except Exception as exc:  # noqa: BLE001
                review_error = str(exc)
            review_snapshot_count = int((review_result or {}).get("count") or 0)
            review_stats_count = len(review_stats)
            scan_run = result.get("scan_run") if isinstance(result.get("scan_run"), dict) else {}
            result["scan_run"] = scan_run
            updated_scan_run = scan_run_service.update_scan_run_review(
                scan_run.get("id"),
                review_after_scan=True,
                review_snapshot_count=review_snapshot_count,
                review_stats_count=review_stats_count,
                review_error=review_error,
            )
            if updated_scan_run is not None:
                result["scan_run"] = updated_scan_run
            elif isinstance(scan_run, dict):
                scan_run.update(
                    {
                        "review_after_scan": True,
                        "review_snapshot_count": review_snapshot_count,
                        "review_stats_count": review_stats_count,
                        "review_error": review_error,
                    }
                )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(result["persisted_events"]),
            "requested_count": int(result.get("requested_count", result["watchlist"].get("count", 0))),
            "error_count": len(result["errors"]),
            "elapsed_seconds": result.get("elapsed_seconds"),
            "min_score": result.get("min_score"),
            "signal_summary": result.get("signal_summary", {}),
            "strategy_guard": result.get("strategy_guard", {}),
            "scan_run": result.get("scan_run", {}),
            "notification_count": len(result.get("notification_events", [])),
            "items": result["persisted_events"],
            "deliveries": result["delivery_results"],
            "errors": result["errors"],
            "review_after_scan": req.review_after_scan,
            "review_due_only": req.review_due_only,
            "review_result": review_result or {},
            "review_stats": review_stats,
            "review_error": review_error,
            "source": "akshare",
            "watchlist": {
                "id": result["watchlist"].get("id"),
                "name": result["watchlist"].get("name", ""),
                "count": result["watchlist"].get("count", 0),
                "source": result.get("watchlist_source", "existing"),
                "message": result.get("watchlist_message", ""),
                "warning": result.get("watchlist_warning", ""),
            },
            "messages": notification_service.build_stdout_messages(
                select_newly_delivered_events(
                    result.get("notification_events", result["persisted_events"]),
                    result["delivery_results"],
                )
            ),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/signals/scan-runs")
def api_list_scan_runs(limit: int = 50) -> dict[str, Any]:
    try:
        items = scan_run_service.list_scan_runs(limit=limit)
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/reviews/backfill")
def api_backfill_reviews(req: BackfillReviewsRequest) -> dict[str, Any]:
    try:
        result = review_service.backfill_review_snapshots(
            trade_date=req.trade_date.strip() or None,
            code=req.code.strip() or None,
            horizons=req.horizons,
            adjust=req.adjust.strip(),
            due_only=bool(req.due_only),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": result["count"],
            "items": result["items"],
            "errors": result["errors"],
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/reviews/stats")
def api_review_stats(
    horizon: str = Query(default="T+3"),
    trade_date: str | None = None,
    code: str | None = None,
) -> dict[str, Any]:
    try:
        items = review_service.summarize_review_stats(
            horizon=horizon.strip() or "T+3",
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/reviews/snapshots")
def api_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizon: str | None = None,
    limit: int = Query(default=200, ge=1, le=1000),
) -> dict[str, Any]:
    try:
        items = review_service.list_review_snapshots(
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
            horizon=horizon.strip() if horizon else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/strategy/summary")
def api_strategy_summary(
    horizon: str = Query(default="T+3"),
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    min_samples: int = Query(default=1, ge=1, le=10000),
    actionable_only: bool = Query(default=False),
    data_source: str | None = None,
) -> dict[str, Any]:
    try:
        result = strategy_summary_service.summarize_strategy_decisions(
            horizon=horizon.strip() or "T+3",
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
            limit=int(limit),
            min_samples=int(min_samples),
            actionable_only=bool(actionable_only),
            data_source=data_source.strip() if data_source else None,
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(result["items"]),
            **result,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/limit-up/breakthroughs")
def api_scan_limit_up_breakthroughs(req: LimitUpBreakthroughRequest) -> dict[str, Any]:
    try:
        result = limit_up_service.scan_and_save_limit_up_breakthroughs(
            trade_date=req.trade_date.strip() or None,
            lookback_days=int(req.lookback_days),
            min_score=float(req.min_score),
            max_items=int(req.max_items),
            pool_limit=int(req.pool_limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "trade_date": result["trade_date"],
            "count": result["count"],
            "items": result["items"],
            "errors": result["errors"],
            "source": "akshare",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/limit-up/breakthroughs")
def api_list_limit_up_breakthroughs(
    trade_date: str | None = None,
    code: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    try:
        items = limit_up_service.list_limit_up_candidates(
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/limit-up/reviews/backfill")
def api_backfill_limit_up_reviews(req: LimitUpReviewRequest) -> dict[str, Any]:
    try:
        result = limit_up_service.backfill_limit_up_review_snapshots(
            trade_date=req.trade_date.strip() or None,
            code=req.code.strip() or None,
            horizons=req.horizons,
            adjust=req.adjust.strip(),
            due_only=bool(req.due_only),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": result["count"],
            "items": result["items"],
            "errors": result["errors"],
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/limit-up/reviews/snapshots")
def api_list_limit_up_review_snapshots(
    trade_date: str | None = None,
    code: str | None = None,
    horizon: str | None = None,
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict[str, Any]:
    try:
        items = limit_up_service.list_limit_up_review_snapshots(
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
            horizon=horizon.strip() if horizon else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/limit-up/reviews/stats")
def api_limit_up_review_stats(
    horizon: str = Query(default="T+3"),
    trade_date: str | None = None,
    code: str | None = None,
) -> dict[str, Any]:
    try:
        items = limit_up_service.summarize_limit_up_review_stats(
            horizon=horizon.strip() or "T+3",
            trade_date=trade_date.strip() if trade_date else None,
            code=code.strip() if code else None,
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.post("/api/sectors/rotation")
def api_scan_sector_rotation(req: SectorRotationRequest) -> dict[str, Any]:
    try:
        result = sector_rotation_service.scan_and_save_sector_rotation(
            trade_date=req.trade_date.strip() or None,
            sector_type=req.sector_type.strip().lower(),
            top_n=int(req.top_n),
            max_items=int(req.max_items),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "trade_date": result["trade_date"],
            "count": result["count"],
            "items": result["items"],
            "errors": result["errors"],
            "source": "akshare",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/sectors/rotation")
def api_list_sector_rotation(
    trade_date: str | None = None,
    sector_type: str | None = None,
    signal: str | None = None,
    limit: int = Query(default=200, ge=1, le=500),
) -> dict[str, Any]:
    try:
        items = sector_rotation_service.list_sector_rotation_snapshots(
            trade_date=trade_date.strip() if trade_date else None,
            sector_type=sector_type.strip() if sector_type else None,
            signal=signal.strip() if signal else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc


@app.get("/api/sectors/rotation/trends")
def api_list_sector_rotation_trends(
    sector_type: str | None = None,
    sector_names: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = Query(default=2000, ge=1, le=10000),
) -> dict[str, Any]:
    try:
        names = [item.strip() for item in (sector_names or "").split(",") if item.strip()]
        items = sector_rotation_service.list_sector_rotation_trends(
            sector_type=sector_type.strip() if sector_type else None,
            sector_names=names,
            start_date=start_date.strip() if start_date else None,
            end_date=end_date.strip() if end_date else None,
            limit=int(limit),
        )
        return {
            "as_of": tdx_service.now_ts(),
            "count": len(items),
            "items": items,
        }
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=500, detail=f"服务内部错误: {exc}") from exc
