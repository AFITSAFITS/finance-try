from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from typing import Any, Callable

import requests

from app import signal_service
from app import tdx_service

EASTMONEY_QUOTE_URL = "https://push2.eastmoney.com/api/qt/ulist.np/get"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q={symbols}"
MAX_REASONABLE_PCT_CHANGE = 20.0
MAX_PCT_CHANGE_DIFF = 0.5
HOT_PCT_CHANGE = 7.0
WEAK_PCT_CHANGE = -5.0
STRONG_VOLUME_RATIO = 1.5
REALTIME_QUOTE_BATCH_SIZE = 80


def _clean_float(value: object) -> float | None:
    if value is None:
        return None
    raw = str(value).strip().replace(",", "")
    if raw in {"", "-", "None", "nan"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _batched(values: list[str], batch_size: int = REALTIME_QUOTE_BATCH_SIZE) -> list[list[str]]:
    size = max(1, int(batch_size))
    return [values[index : index + size] for index in range(0, len(values), size)]


def _enrich_quote_quality(item: dict[str, Any]) -> dict[str, Any]:
    issues: list[str] = []
    latest_price = _clean_float(item.get("latest_price"))
    prev_close = _clean_float(item.get("prev_close"))
    pct_change = _clean_float(item.get("pct_change"))
    volume = _clean_float(item.get("volume"))
    amount = _clean_float(item.get("amount"))

    if latest_price is None or latest_price <= 0:
        issues.append("当前价缺失")
    if prev_close is None or prev_close <= 0:
        issues.append("昨收价缺失")
    if latest_price is not None and prev_close is not None and prev_close > 0 and pct_change is not None:
        computed_pct = round(((latest_price / prev_close) - 1.0) * 100.0, 4)
        if abs(computed_pct - pct_change) > MAX_PCT_CHANGE_DIFF:
            issues.append("涨跌幅与价格不一致")
    if pct_change is not None and abs(pct_change) > MAX_REASONABLE_PCT_CHANGE:
        issues.append("涨跌幅异常")
    if volume is not None and volume <= 0:
        issues.append("成交量为0")
    if amount is not None and amount <= 0:
        issues.append("成交额为0")

    item["quality_status"] = "需确认" if issues else "正常"
    item["quality_note"] = "；".join(issues) if issues else "数据字段完整"
    return item


def _enrich_quote_signal(item: dict[str, Any]) -> dict[str, Any]:
    if item.get("quality_status") != "正常":
        item["quote_signal"] = "暂不参考"
        item["quote_note"] = str(item.get("quality_note") or "行情数据需确认")
        return item

    pct_change = _clean_float(item.get("pct_change"))
    volume_ratio = _clean_float(item.get("volume_ratio"))
    if pct_change is None:
        item["quote_signal"] = "继续观察"
        item["quote_note"] = "缺少涨跌幅，先看价格和成交额"
    elif pct_change >= HOT_PCT_CHANGE:
        item["quote_signal"] = "谨慎追高"
        item["quote_note"] = "当日涨幅偏高，避免直接追入"
    elif pct_change <= WEAK_PCT_CHANGE:
        item["quote_signal"] = "弱势回避"
        item["quote_note"] = "当日跌幅偏大，先观察承接"
    elif pct_change > 0 and volume_ratio is not None and volume_ratio >= STRONG_VOLUME_RATIO:
        item["quote_signal"] = "放量走强"
        item["quote_note"] = "价格上涨且量能放大"
    else:
        item["quote_signal"] = "正常观察"
        item["quote_note"] = "价格和成交字段正常"
    return item


def _enrich_quote_item(item: dict[str, Any]) -> dict[str, Any]:
    return _enrich_quote_signal(_enrich_quote_quality(item))


def _market_prefix(code: str) -> str:
    formatted = tdx_service.format_code(code)
    return "sh" if formatted.startswith(("6", "9")) else "sz"


def _eastmoney_secid(code: str) -> str:
    formatted = tdx_service.format_code(code)
    market = "1" if formatted.startswith(("6", "9")) else "0"
    return f"{market}.{formatted}"


def _tencent_symbol(code: str) -> str:
    formatted = tdx_service.format_code(code)
    return f"{_market_prefix(formatted)}{formatted}"


def _call_provider_with_timeout(
    provider_name: str,
    provider: Callable[[], list[dict[str, Any]]],
    timeout_seconds: float,
) -> list[dict[str, Any]]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(provider)
    try:
        return future.result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise TimeoutError(f"{provider_name} timeout after {timeout_seconds:g}s") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def fetch_realtime_quotes_eastmoney(
    codes: list[str],
    timeout: float = 10.0,
    requester: Callable[..., object] = requests.get,
    batch_size: int = REALTIME_QUOTE_BATCH_SIZE,
) -> list[dict[str, Any]]:
    normalized_codes = tdx_service.validate_codes(codes)
    if not normalized_codes:
        return []

    by_code: dict[str, dict[str, Any]] = {}
    for code_batch in _batched(normalized_codes, batch_size=batch_size):
        response = requester(
            EASTMONEY_QUOTE_URL,
            params={
                "fltt": "2",
                "secids": ",".join(_eastmoney_secid(code) for code in code_batch),
                "fields": "f12,f14,f2,f3,f4,f5,f6,f15,f16,f17,f18,f8,f10",
            },
            timeout=timeout,
            headers=signal_service.EASTMONEY_HISTORY_HEADERS,
        )
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()

        payload = response.json()
        rows = ((payload.get("data") or {}).get("diff") or []) if isinstance(payload, dict) else []
        for row in rows:
            code = tdx_service.format_code(row.get("f12"))
            by_code[code] = _enrich_quote_item({
                "code": code,
                "name": str(row.get("f14") or ""),
                "latest_price": _clean_float(row.get("f2")),
                "pct_change": _clean_float(row.get("f3")),
                "change_amount": _clean_float(row.get("f4")),
                "volume": _clean_float(row.get("f5")),
                "amount": _clean_float(row.get("f6")),
                "high": _clean_float(row.get("f15")),
                "low": _clean_float(row.get("f16")),
                "open": _clean_float(row.get("f17")),
                "prev_close": _clean_float(row.get("f18")),
                "turnover_rate": _clean_float(row.get("f8")),
                "volume_ratio": _clean_float(row.get("f10")),
                "source": "eastmoney",
            })
    return [by_code[code] for code in normalized_codes if code in by_code]


def _split_tencent_records(text: str) -> list[list[str]]:
    records: list[list[str]] = []
    for raw_record in text.split(";"):
        if "=" not in raw_record:
            continue
        _, raw_value = raw_record.split("=", 1)
        value = raw_value.strip().strip('"')
        if value:
            records.append(value.split("~"))
    return records


def fetch_realtime_quotes_tencent(
    codes: list[str],
    timeout: float = 10.0,
    requester: Callable[..., object] = requests.get,
    batch_size: int = REALTIME_QUOTE_BATCH_SIZE,
) -> list[dict[str, Any]]:
    normalized_codes = tdx_service.validate_codes(codes)
    if not normalized_codes:
        return []

    by_code: dict[str, dict[str, Any]] = {}
    for code_batch in _batched(normalized_codes, batch_size=batch_size):
        response = requester(
            TENCENT_QUOTE_URL.format(symbols=",".join(_tencent_symbol(code) for code in code_batch)),
            timeout=timeout,
            headers=signal_service.EASTMONEY_HISTORY_HEADERS,
        )
        raise_for_status = getattr(response, "raise_for_status", None)
        if callable(raise_for_status):
            raise_for_status()
        text = getattr(response, "text", "")
        if not text and hasattr(response, "content"):
            text = response.content.decode("gbk", errors="ignore")

        for parts in _split_tencent_records(str(text)):
            if len(parts) < 38:
                continue
            code = tdx_service.format_code(parts[2])
            by_code[code] = _enrich_quote_item({
                "code": code,
                "name": parts[1],
                "latest_price": _clean_float(parts[3]),
                "pct_change": _clean_float(parts[32]),
                "change_amount": _clean_float(parts[31]),
                "volume": _clean_float(parts[36]),
                "amount": _clean_float(parts[37]),
                "high": _clean_float(parts[33]),
                "low": _clean_float(parts[34]),
                "open": _clean_float(parts[5]),
                "prev_close": _clean_float(parts[4]),
                "turnover_rate": _clean_float(parts[38]) if len(parts) > 38 else None,
                "volume_ratio": _clean_float(parts[49]) if len(parts) > 49 else None,
                "source": "tencent",
            })
    return [by_code[code] for code in normalized_codes if code in by_code]


def fetch_realtime_quotes_best_effort(
    codes: list[str],
    provider_timeout: float | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, str]], str]:
    normalized_codes = tdx_service.validate_codes(codes)
    timeout_seconds = signal_service.provider_timeout_seconds() if provider_timeout is None else max(0.05, float(provider_timeout))
    errors: list[dict[str, str]] = []
    items_by_code: dict[str, dict[str, Any]] = {}
    used_sources: list[str] = []
    providers: list[tuple[str, Callable[[], list[dict[str, Any]]]]] = [
        ("eastmoney", lambda: fetch_realtime_quotes_eastmoney([code for code in normalized_codes if code not in items_by_code])),
        ("tencent", lambda: fetch_realtime_quotes_tencent([code for code in normalized_codes if code not in items_by_code])),
    ]

    for provider_name, provider in providers:
        missing_before_provider = [code for code in normalized_codes if code not in items_by_code]
        if not missing_before_provider:
            break
        try:
            items = _call_provider_with_timeout(provider_name, provider, timeout_seconds)
            if items:
                used_sources.append(provider_name)
                for item in items:
                    code = str(item.get("code", ""))
                    if code in missing_before_provider:
                        items_by_code[code] = item
                continue
            errors.append({"股票代码": "全部", "error": f"{provider_name}: empty"})
        except Exception as exc:  # noqa: BLE001
            errors.append({"股票代码": "全部", "error": f"{provider_name}: {exc}"})

    missing = [code for code in normalized_codes if code not in items_by_code]
    item_errors = [{"股票代码": code, "error": "未返回实时行情"} for code in missing]
    items = [items_by_code[code] for code in normalized_codes if code in items_by_code]
    source = "+".join(used_sources) if used_sources else "none"
    if items:
        return items, item_errors, source
    return [], errors + item_errors, source
