from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from datetime import datetime, timedelta, timezone
import os
from typing import Callable, Iterable

import pandas as pd
import requests

from app import tdx_service

EASTMONEY_HISTORY_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
EASTMONEY_HISTORY_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/133.0.0.0 Safari/537.36"
    ),
    "Referer": "https://quote.eastmoney.com/",
    "Connection": "close",
}
SECONDARY_GOLDEN_CROSS_PATTERN = "水下金叉后水上再次金叉"
DEFAULT_PROVIDER_TIMEOUT_SECONDS = 12.0

SIGNAL_OUTPUT_COLUMNS = [
    "股票代码",
    "日期",
    "收盘",
    "涨跌幅",
    "信号评分",
    "信号方向",
    "信号级别",
    "评分原因",
    "DIF",
    "DEA",
    "MACD信号",
    "MACD形态",
    "MA5",
    "MA20",
    "60日位置",
    "量能比",
    "20日涨幅",
    "60日涨幅",
    "相对强度",
    "K线形态",
    "K线提示",
    "参考止损",
    "参考目标",
    "风险收益比",
    "风险提示",
    "观察结论",
    "均线信号",
    "信号",
]


def _suppress_akshare_progress() -> None:
    import akshare.stock.stock_zh_a_sina as stock_zh_a_sina
    import akshare.stock_feature.stock_hist_tx as stock_hist_tx

    silent_tqdm = lambda iterable, *args, **kwargs: iterable
    for module in (stock_zh_a_sina, stock_hist_tx):
        if hasattr(module, "get_tqdm"):
            module.get_tqdm = lambda enable=True, _silent_tqdm=silent_tqdm: _silent_tqdm


def provider_timeout_seconds() -> float:
    raw_value = os.getenv("AI_FINANCE_PROVIDER_TIMEOUT_SECONDS", "").strip()
    if not raw_value:
        return DEFAULT_PROVIDER_TIMEOUT_SECONDS
    try:
        value = float(raw_value)
    except ValueError:
        return DEFAULT_PROVIDER_TIMEOUT_SECONDS
    return max(1.0, value)


def _call_provider_with_timeout(
    provider_name: str,
    provider: Callable[[], pd.DataFrame],
    timeout_seconds: float,
) -> pd.DataFrame:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(provider)
    try:
        return future.result(timeout=timeout_seconds)
    except FuturesTimeoutError as exc:
        future.cancel()
        raise TimeoutError(f"{provider_name} timeout after {timeout_seconds:g}s") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def format_trade_date(value: object) -> str:
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    return str(value)


def normalize_history_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    if "日期" not in df.columns or "收盘" not in df.columns:
        raise ValueError(f"{code} 日线数据缺少必要字段: 日期/收盘")

    normalized = df.copy()
    normalized["日期"] = pd.to_datetime(normalized["日期"], errors="coerce")
    for column in ["开盘", "收盘", "最高", "最低", "成交量", "成交额", "涨跌幅", "换手率"]:
        if column in normalized.columns:
            normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    if "涨跌幅" not in normalized.columns:
        normalized["涨跌幅"] = None
    if "股票代码" not in normalized.columns:
        normalized["股票代码"] = code
    normalized = normalized.dropna(subset=["日期", "收盘"]).sort_values("日期").reset_index(drop=True)
    return normalized


def add_indicator_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    enriched = df.copy()
    if "EMA12" not in enriched.columns:
        enriched["EMA12"] = enriched["收盘"].ewm(span=12, adjust=False).mean()
    if "EMA26" not in enriched.columns:
        enriched["EMA26"] = enriched["收盘"].ewm(span=26, adjust=False).mean()
    if "DIF" not in enriched.columns:
        enriched["DIF"] = enriched["EMA12"] - enriched["EMA26"]
    if "DEA" not in enriched.columns:
        enriched["DEA"] = enriched["DIF"].ewm(span=9, adjust=False).mean()
    if "MA5" not in enriched.columns:
        enriched["MA5"] = enriched["收盘"].rolling(window=5).mean()
    if "MA20" not in enriched.columns:
        enriched["MA20"] = enriched["收盘"].rolling(window=20).mean()
    if "成交量" in enriched.columns and "VOL20" not in enriched.columns:
        enriched["VOL20"] = enriched["成交量"].rolling(window=20).mean()
    if "最高" in enriched.columns and "最低" in enriched.columns:
        rolling_high = enriched["最高"].rolling(window=60, min_periods=20).max()
        rolling_low = enriched["最低"].rolling(window=60, min_periods=20).min()
    else:
        rolling_high = enriched["收盘"].rolling(window=60, min_periods=20).max()
        rolling_low = enriched["收盘"].rolling(window=60, min_periods=20).min()
    price_range = rolling_high - rolling_low
    enriched["POSITION_60D"] = (enriched["收盘"] - rolling_low) / price_range.where(price_range != 0)
    return enriched


def crosses_up(prev_left: object, curr_left: object, prev_right: object, curr_right: object) -> bool:
    values = [prev_left, curr_left, prev_right, curr_right]
    if any(pd.isna(value) for value in values):
        return False
    return float(prev_left) <= float(prev_right) and float(curr_left) > float(curr_right)


def crosses_down(prev_left: object, curr_left: object, prev_right: object, curr_right: object) -> bool:
    values = [prev_left, curr_left, prev_right, curr_right]
    if any(pd.isna(value) for value in values):
        return False
    return float(prev_left) >= float(prev_right) and float(curr_left) < float(curr_right)


def detect_macd_secondary_golden_cross_above_zero(enriched_df: pd.DataFrame) -> bool:
    if len(enriched_df.index) < 3:
        return False

    latest_prev = enriched_df.iloc[-2]
    latest_curr = enriched_df.iloc[-1]
    if not crosses_up(latest_prev["DIF"], latest_curr["DIF"], latest_prev["DEA"], latest_curr["DEA"]):
        return False
    if pd.isna(latest_curr["DIF"]) or pd.isna(latest_curr["DEA"]):
        return False
    if float(latest_curr["DIF"]) <= 0 or float(latest_curr["DEA"]) <= 0:
        return False

    for idx in range(1, len(enriched_df.index) - 1):
        prev_row = enriched_df.iloc[idx - 1]
        curr_row = enriched_df.iloc[idx]
        if not crosses_up(prev_row["DIF"], curr_row["DIF"], prev_row["DEA"], curr_row["DEA"]):
            continue
        if pd.isna(curr_row["DIF"]) or pd.isna(curr_row["DEA"]):
            continue
        if float(curr_row["DIF"]) < 0 and float(curr_row["DEA"]) < 0:
            return True
    return False


def extract_candlestick_profile(row: pd.Series) -> dict[str, object]:
    close = row.get("收盘")
    if pd.isna(close):
        return {"K线形态": None, "K线提示": ""}

    close_price = float(close)
    open_price = close_price if pd.isna(row.get("开盘")) else float(row.get("开盘"))
    high_price = max(open_price, close_price) if pd.isna(row.get("最高")) else float(row.get("最高"))
    low_price = min(open_price, close_price) if pd.isna(row.get("最低")) else float(row.get("最低"))

    price_range = high_price - low_price
    if price_range <= 0 or open_price <= 0:
        return {"K线形态": "平稳K线", "K线提示": ""}

    body_pct = (close_price / open_price - 1) * 100
    close_position = (close_price - low_price) / price_range
    upper_shadow_ratio = (high_price - max(open_price, close_price)) / price_range

    if close_position >= 0.75 and body_pct >= 2:
        return {"K线形态": "强势收盘", "K线提示": "收盘接近日高"}
    if upper_shadow_ratio >= 0.45 and close_position <= 0.65:
        return {"K线形态": "长上影线", "K线提示": "冲高回落"}
    if close_position <= 0.25 and body_pct <= -2:
        return {"K线形态": "弱势收盘", "K线提示": "收盘接近日低"}
    return {"K线形态": "普通K线", "K线提示": ""}


def extract_bullish_trade_plan(enriched_df: pd.DataFrame, row: dict[str, object]) -> dict[str, object]:
    if row.get("信号方向") != "偏多" or enriched_df.empty:
        return {"参考止损": None, "参考目标": None, "风险收益比": None}

    close_price = row.get("收盘")
    try:
        close = float(close_price)
    except (TypeError, ValueError):
        close = float("nan")
    if pd.isna(close) or close <= 0:
        return {"参考止损": None, "参考目标": None, "风险收益比": None}

    latest = enriched_df.iloc[-1]
    support_candidates: list[float] = []
    if not pd.isna(latest.get("MA20")) and float(latest["MA20"]) < close:
        support_candidates.append(float(latest["MA20"]))
    if "最低" in enriched_df.columns:
        recent_low = enriched_df["最低"].tail(10).min()
        if not pd.isna(recent_low) and float(recent_low) < close:
            support_candidates.append(float(recent_low))

    support = max(support_candidates) if support_candidates else close * 0.94
    stop_price = min(support * 0.98, close * 0.98)
    stop_price = max(stop_price, close * 0.9)
    if stop_price >= close:
        stop_price = close * 0.95

    risk = close - stop_price
    if risk <= 0:
        return {"参考止损": None, "参考目标": None, "风险收益比": None}
    target_price = close + risk * 2
    return {
        "参考止损": round(stop_price, 4),
        "参考目标": round(target_price, 4),
        "风险收益比": 2.0,
    }


def apply_trade_plan_risk(row: dict[str, object]) -> None:
    if row.get("信号方向") != "偏多":
        return
    try:
        close = float(row.get("收盘"))
        stop_price = float(row.get("参考止损"))
    except (TypeError, ValueError):
        return
    if close <= 0 or stop_price <= 0 or stop_price >= close:
        return

    stop_distance_pct = (close - stop_price) / close * 100
    if stop_distance_pct <= 8:
        return

    score = max(0.0, float(row.get("信号评分", 0) or 0) - 5)
    risks = [part for part in str(row.get("风险提示") or "").split("；") if part and part != "无明显风险"]
    risks.append("止损距离偏大")
    row["信号评分"] = round(score, 2)
    row["信号级别"] = _signal_level(score)
    row["风险提示"] = "；".join(dict.fromkeys(risks)) if risks else "无明显风险"


def apply_observation_conclusion(row: dict[str, object]) -> None:
    direction = str(row.get("信号方向") or "")
    risk_note = str(row.get("风险提示") or "")
    risk_parts = [part for part in risk_note.split("；") if part and part != "无明显风险"]
    try:
        score = float(row.get("信号评分", 0) or 0)
    except (TypeError, ValueError):
        score = 0.0
    try:
        close = float(row.get("收盘"))
        stop_price = float(row.get("参考止损"))
        stop_distance_pct = (close - stop_price) / close * 100 if close > 0 and 0 < stop_price < close else None
    except (TypeError, ValueError):
        stop_distance_pct = None

    if direction == "偏空":
        conclusion = "风险回避"
    elif score < 60:
        conclusion = "暂不参考"
    elif score >= 80 and not risk_parts:
        conclusion = "重点观察"
    elif risk_parts or (stop_distance_pct is not None and stop_distance_pct > 8):
        conclusion = "谨慎观察"
    else:
        conclusion = "正常观察"
    row["观察结论"] = conclusion


def score_signal_row(row: dict[str, object]) -> dict[str, object]:
    score = 50.0
    reasons: list[str] = []
    risks: list[str] = []
    direction = "中性"

    macd_signal = str(row.get("MACD信号") or "")
    ma_signal = str(row.get("均线信号") or "")
    macd_pattern = str(row.get("MACD形态") or "")
    pct_change = row.get("涨跌幅")
    position_60d = row.get("60日位置")
    volume_ratio = row.get("量能比")
    candlestick_pattern = str(row.get("K线形态") or "")

    if macd_signal == "MACD金叉":
        score += 20
        direction = "偏多"
        reasons.append("MACD金叉")
    elif macd_signal == "MACD死叉":
        score -= 20
        direction = "偏空"
        reasons.append("MACD死叉")

    if ma_signal == "MA5上穿MA20":
        score += 20
        direction = "偏多"
        reasons.append("均线转强")
    elif ma_signal == "MA5下穿MA20":
        score -= 20
        direction = "偏空"
        reasons.append("均线转弱")

    if macd_pattern == SECONDARY_GOLDEN_CROSS_PATTERN:
        score += 20
        direction = "偏多"
        reasons.append("水下金叉后水上再次金叉")

    try:
        pct = float(pct_change)
    except (TypeError, ValueError):
        pct = 0.0
    if direction == "偏多":
        if pct >= 7:
            score -= 10
            risks.append("当日涨幅偏高")
        elif 0 <= pct <= 5:
            score += 5
            reasons.append("涨幅未明显过热")
    elif direction == "偏空" and pct <= -5:
        score -= 5
        risks.append("跌幅偏大")

    try:
        position = float(position_60d)
    except (TypeError, ValueError):
        position = float("nan")
    if not pd.isna(position):
        if direction == "偏多":
            if position >= 0.9:
                score -= 10
                risks.append("接近60日高位")
            elif position <= 0.45:
                score += 5
                reasons.append("价格位置不高")
        elif direction == "偏空" and position <= 0.2:
            risks.append("接近60日低位")

    try:
        ratio = float(volume_ratio)
    except (TypeError, ValueError):
        ratio = float("nan")
    if not pd.isna(ratio) and direction == "偏多":
        if ratio >= 1.5:
            score += 5
            reasons.append("量能放大")
        elif ratio < 0.7:
            score -= 5
            risks.append("量能不足")

    if direction == "偏多":
        if candlestick_pattern == "强势收盘":
            score += 5
            reasons.append("K线收盘较强")
        elif candlestick_pattern == "长上影线":
            score -= 8
            risks.append("冲高回落")
        elif candlestick_pattern == "弱势收盘":
            score -= 10
            risks.append("收盘偏弱")

    bounded_score = max(0.0, min(100.0, score))
    if bounded_score >= 80:
        level = "重点观察"
    elif bounded_score >= 60:
        level = "观察"
    elif bounded_score <= 30:
        level = "风险"
    else:
        level = "普通"

    return {
        "信号评分": round(bounded_score, 2),
        "信号方向": direction,
        "信号级别": level,
        "评分原因": "；".join(reasons),
        "风险提示": "；".join(risks) if risks else "无明显风险",
    }


def _signal_level(score: float) -> str:
    if score >= 80:
        return "重点观察"
    if score >= 60:
        return "观察"
    if score <= 30:
        return "风险"
    return "普通"


def extract_strength_metrics(code: str, history_df: pd.DataFrame) -> dict[str, object] | None:
    normalized = normalize_history_df(history_df, code)
    if len(normalized.index) < 21:
        return None

    latest = normalized.iloc[-1]
    close = float(latest["收盘"])
    if close <= 0:
        return None

    def pct_return(days: int) -> float | None:
        if len(normalized.index) <= days:
            return None
        base = normalized.iloc[-days - 1]["收盘"]
        if pd.isna(base) or float(base) <= 0:
            return None
        return round((close / float(base) - 1) * 100, 4)

    return {
        "股票代码": code,
        "20日涨幅": pct_return(20),
        "60日涨幅": pct_return(60),
    }


def apply_relative_strength(
    rows_by_code: dict[str, dict[str, object]],
    metrics_by_code: dict[str, dict[str, object]],
) -> None:
    if not rows_by_code or not metrics_by_code:
        return

    strength_values = {
        code: metrics["60日涨幅"] if metrics.get("60日涨幅") is not None else metrics.get("20日涨幅")
        for code, metrics in metrics_by_code.items()
    }
    strength_series = pd.Series(strength_values, dtype="float64").dropna()
    if strength_series.empty:
        return

    rank_series = strength_series.rank(method="average", pct=True) * 100
    for code, row in rows_by_code.items():
        metrics = metrics_by_code.get(code) or {}
        row["20日涨幅"] = metrics.get("20日涨幅")
        row["60日涨幅"] = metrics.get("60日涨幅")
        if code not in rank_series:
            row["相对强度"] = None
            continue

        relative_strength = round(float(rank_series[code]), 2)
        row["相对强度"] = relative_strength
        if row.get("信号方向") != "偏多":
            continue

        score = float(row.get("信号评分", 0) or 0)
        reasons = [part for part in str(row.get("评分原因") or "").split("；") if part]
        risks = [part for part in str(row.get("风险提示") or "").split("；") if part and part != "无明显风险"]
        if relative_strength >= 80:
            score += 8
            reasons.append("股票池内强势")
        elif relative_strength >= 60:
            score += 3
            reasons.append("股票池内偏强")
        elif relative_strength < 30:
            score -= 8
            risks.append("股票池内偏弱")

        bounded_score = max(0.0, min(100.0, score))
        row["信号评分"] = round(bounded_score, 2)
        row["信号级别"] = _signal_level(bounded_score)
        row["评分原因"] = "；".join(dict.fromkeys(reasons))
        row["风险提示"] = "；".join(dict.fromkeys(risks)) if risks else "无明显风险"


def extract_latest_signal_row(code: str, history_df: pd.DataFrame) -> dict[str, object] | None:
    normalized = normalize_history_df(history_df, code)
    if len(normalized.index) < 2:
        return None

    enriched = add_indicator_columns(normalized)
    prev_row = enriched.iloc[-2]
    curr_row = enriched.iloc[-1]

    macd_signal = None
    if crosses_up(prev_row["DIF"], curr_row["DIF"], prev_row["DEA"], curr_row["DEA"]):
        macd_signal = "MACD金叉"
    elif crosses_down(prev_row["DIF"], curr_row["DIF"], prev_row["DEA"], curr_row["DEA"]):
        macd_signal = "MACD死叉"

    macd_pattern = None
    if macd_signal == "MACD金叉" and detect_macd_secondary_golden_cross_above_zero(enriched):
        macd_pattern = SECONDARY_GOLDEN_CROSS_PATTERN

    ma_signal = None
    if crosses_up(prev_row["MA5"], curr_row["MA5"], prev_row["MA20"], curr_row["MA20"]):
        ma_signal = "MA5上穿MA20"
    elif crosses_down(prev_row["MA5"], curr_row["MA5"], prev_row["MA20"], curr_row["MA20"]):
        ma_signal = "MA5下穿MA20"

    signals = [signal for signal in [macd_signal, macd_pattern, ma_signal] if signal]
    if not signals:
        return None

    row = {
        "股票代码": code,
        "日期": format_trade_date(curr_row["日期"]),
        "收盘": round(float(curr_row["收盘"]), 4),
        "涨跌幅": None if pd.isna(curr_row["涨跌幅"]) else round(float(curr_row["涨跌幅"]), 4),
        "DIF": round(float(curr_row["DIF"]), 6),
        "DEA": round(float(curr_row["DEA"]), 6),
        "MACD信号": macd_signal,
        "MACD形态": macd_pattern,
        "MA5": None if pd.isna(curr_row["MA5"]) else round(float(curr_row["MA5"]), 4),
        "MA20": None if pd.isna(curr_row["MA20"]) else round(float(curr_row["MA20"]), 4),
        "60日位置": None if pd.isna(curr_row["POSITION_60D"]) else round(float(curr_row["POSITION_60D"]), 4),
        "量能比": None
        if "VOL20" not in curr_row or pd.isna(curr_row["VOL20"]) or pd.isna(curr_row.get("成交量")) or float(curr_row["VOL20"]) == 0
        else round(float(curr_row["成交量"]) / float(curr_row["VOL20"]), 4),
        "均线信号": ma_signal,
        "信号": ", ".join(signals),
    }
    row.update(extract_candlestick_profile(curr_row))
    row.update(score_signal_row(row))
    row.update(extract_bullish_trade_plan(enriched, row))
    apply_trade_plan_risk(row)
    apply_observation_conclusion(row)
    return row


def fetch_daily_history_eastmoney(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    timeout: float = 10.0,
    retries: int = 3,
    requester: Callable[..., object] = requests.get,
) -> pd.DataFrame:
    formatted_code = tdx_service.format_code(code)
    market_code = "1" if formatted_code.startswith(("6", "9")) else "0"
    adjust_map = {"qfq": "1", "hfq": "2", "": "0"}
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "101",
        "fqt": adjust_map.get(adjust, "1"),
        "secid": f"{market_code}.{formatted_code}",
        "beg": start_date,
        "end": end_date,
    }

    last_exc: Exception | None = None
    for _ in range(max(1, int(retries))):
        try:
            response = requester(
                EASTMONEY_HISTORY_URL,
                params=params,
                timeout=timeout,
                headers=EASTMONEY_HISTORY_HEADERS,
            )
            raise_for_status = getattr(response, "raise_for_status", None)
            if callable(raise_for_status):
                raise_for_status()
            data_json = response.json()
            data = data_json.get("data") or {}
            klines = data.get("klines") or []
            if not klines:
                return pd.DataFrame()

            temp_df = pd.DataFrame([item.split(",") for item in klines])
            temp_df["股票代码"] = formatted_code
            temp_df.columns = [
                "日期",
                "开盘",
                "收盘",
                "最高",
                "最低",
                "成交量",
                "成交额",
                "振幅",
                "涨跌幅",
                "涨跌额",
                "换手率",
                "股票代码",
            ]
            return normalize_history_df(temp_df, formatted_code)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc

    assert last_exc is not None
    raise last_exc


def _fetch_daily_history_akshare_provider(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_zh_a_hist(
        symbol=code,
        period="daily",
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        timeout=10,
    )
    return normalize_history_df(df, code)


def _to_yahoo_symbol(code: str) -> str:
    formatted_code = tdx_service.format_code(code)
    suffix = ".SS" if formatted_code.startswith(("6", "9")) else ".SZ"
    return f"{formatted_code}{suffix}"


def _to_ak_market_symbol(code: str) -> str:
    formatted_code = tdx_service.format_code(code)
    prefix = "sh" if formatted_code.startswith(("6", "9")) else "sz"
    return f"{prefix}{formatted_code}"


def _to_baostock_symbol(code: str) -> str:
    formatted_code = tdx_service.format_code(code)
    prefix = "sh" if formatted_code.startswith(("6", "9")) else "sz"
    return f"{prefix}.{formatted_code}"


def normalize_provider_history_df(df: pd.DataFrame, code: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    rename_map = {
        "date": "日期",
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
        "turnover": "换手率",
        "turn": "换手率",
        "pctChg": "涨跌幅",
    }
    normalized = df.rename(columns={key: value for key, value in rename_map.items() if key in df.columns}).copy()
    if "日期" not in normalized.columns and normalized.index.name:
        normalized = normalized.reset_index().rename(columns={normalized.index.name: "日期"})
    if "日期" not in normalized.columns and not isinstance(normalized.index, pd.RangeIndex):
        normalized = normalized.reset_index().rename(columns={"index": "日期"})
    if "股票代码" not in normalized.columns:
        normalized["股票代码"] = tdx_service.format_code(code)
    if "涨跌幅" not in normalized.columns and "收盘" in normalized.columns:
        normalized["涨跌幅"] = pd.to_numeric(normalized["收盘"], errors="coerce").pct_change() * 100
    return normalize_history_df(normalized, code)


def _fetch_daily_history_tx_provider(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> pd.DataFrame:
    import akshare as ak

    _suppress_akshare_progress()
    df = ak.stock_zh_a_hist_tx(
        symbol=_to_ak_market_symbol(code),
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
        timeout=10,
    )
    return normalize_provider_history_df(df, code)


def _fetch_daily_history_sina_provider(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> pd.DataFrame:
    import akshare as ak

    _suppress_akshare_progress()
    df = ak.stock_zh_a_daily(
        symbol=_to_ak_market_symbol(code),
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )
    return normalize_provider_history_df(df, code)


def _fetch_daily_history_baostock_provider(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str,
) -> pd.DataFrame:
    import baostock as bs

    adjust_map = {"qfq": "2", "hfq": "1", "": "3"}
    lg = bs.login()
    try:
        if getattr(lg, "error_code", "0") != "0":
            raise RuntimeError(getattr(lg, "error_msg", "baostock login failed"))
        rs = bs.query_history_k_data_plus(
            _to_baostock_symbol(code),
            "date,code,open,high,low,close,volume,amount,turn,pctChg",
            start_date=pd.to_datetime(start_date).strftime("%Y-%m-%d"),
            end_date=pd.to_datetime(end_date).strftime("%Y-%m-%d"),
            frequency="d",
            adjustflag=adjust_map.get(adjust, "2"),
        )
        if getattr(rs, "error_code", "0") != "0":
            raise RuntimeError(getattr(rs, "error_msg", "baostock query failed"))

        rows: list[list[str]] = []
        while rs.next():
            rows.append(rs.get_row_data())
        df = pd.DataFrame(rows, columns=rs.fields)
        return normalize_provider_history_df(df, code)
    finally:
        bs.logout()


def fetch_daily_history_yahoo(
    code: str,
    start_date: str,
    end_date: str,
    timeout: float = 10.0,
    requester: Callable[..., object] = requests.get,
) -> pd.DataFrame:
    formatted_code = tdx_service.format_code(code)
    start_dt = pd.to_datetime(start_date).to_pydatetime().replace(tzinfo=timezone.utc)
    end_dt = pd.to_datetime(end_date).to_pydatetime().replace(tzinfo=timezone.utc) + timedelta(days=1)
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{_to_yahoo_symbol(formatted_code)}"
    params = {
        "period1": int(start_dt.timestamp()),
        "period2": int(end_dt.timestamp()),
        "interval": "1d",
        "events": "history",
        "includeAdjustedClose": "true",
    }
    response = requester(url, params=params, timeout=timeout, headers=EASTMONEY_HISTORY_HEADERS)
    raise_for_status = getattr(response, "raise_for_status", None)
    if callable(raise_for_status):
        raise_for_status()

    payload = response.json()
    result = ((payload.get("chart") or {}).get("result") or [None])[0]
    if not result:
        return pd.DataFrame()

    timestamps = result.get("timestamp") or []
    quote = (((result.get("indicators") or {}).get("quote") or [{}])[0]) or {}
    rows: list[dict[str, object]] = []
    for idx, ts in enumerate(timestamps):
        close = (quote.get("close") or [None] * len(timestamps))[idx]
        if close is None:
            continue
        open_price = (quote.get("open") or [None] * len(timestamps))[idx]
        high = (quote.get("high") or [None] * len(timestamps))[idx]
        low = (quote.get("low") or [None] * len(timestamps))[idx]
        volume = (quote.get("volume") or [None] * len(timestamps))[idx]
        rows.append(
            {
                "日期": datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%d"),
                "股票代码": formatted_code,
                "开盘": open_price,
                "收盘": close,
                "最高": high,
                "最低": low,
                "成交量": volume,
                "成交额": None,
                "涨跌幅": None,
                "换手率": None,
            }
        )

    normalized = normalize_history_df(pd.DataFrame(rows), formatted_code)
    if normalized.empty:
        return normalized
    normalized["涨跌幅"] = normalized["收盘"].pct_change() * 100
    return normalized


def fetch_daily_history_best_effort(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    provider_timeout: float | None = None,
) -> pd.DataFrame:
    errors: list[str] = []
    timeout_seconds = provider_timeout_seconds() if provider_timeout is None else max(0.05, float(provider_timeout))
    providers: list[tuple[str, Callable[[], pd.DataFrame]]] = [
        (
            "eastmoney",
            lambda: fetch_daily_history_eastmoney(
                code=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
        ),
        (
            "akshare",
            lambda: _fetch_daily_history_akshare_provider(
                code=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
        ),
        (
            "tencent",
            lambda: _fetch_daily_history_tx_provider(
                code=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
        ),
        (
            "sina",
            lambda: _fetch_daily_history_sina_provider(
                code=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
        ),
        (
            "baostock",
            lambda: _fetch_daily_history_baostock_provider(
                code=code,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            ),
        ),
        (
            "yahoo",
            lambda: fetch_daily_history_yahoo(
                code=code,
                start_date=start_date,
                end_date=end_date,
            ),
        ),
    ]

    for provider_name, provider in providers:
        try:
            df = _call_provider_with_timeout(provider_name, provider, timeout_seconds)
            if not df.empty:
                return df
            errors.append(f"{provider_name}: empty")
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{provider_name}: {exc}")
    raise RuntimeError("；".join(errors))


def fetch_daily_history_akshare(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")
    return fetch_daily_history_best_effort(
        code=code,
        start_date=start_date,
        end_date=end_date,
        adjust=adjust,
    )


def scan_stock_signal_events(
    codes: Iterable[str],
    lookback_days: int = 180,
    adjust: str = "qfq",
    fetcher: Callable[[str, int, str], pd.DataFrame] = fetch_daily_history_akshare,
    max_workers: int = 8,
    only_secondary_golden_cross: bool = False,
    min_score: float | None = None,
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    normalized_codes = tdx_service.validate_codes(codes)

    def run_single(code: str) -> tuple[str, dict[str, object] | None, dict[str, object] | None, str | None]:
        try:
            history_df = fetcher(code, int(lookback_days), adjust)
            signal_row = extract_latest_signal_row(code, history_df)
            strength_metrics = extract_strength_metrics(code, history_df)
            return code, signal_row, strength_metrics, None
        except Exception as exc:  # noqa: BLE001
            return code, None, None, str(exc)

    rows_by_code: dict[str, dict[str, object]] = {}
    metrics_by_code: dict[str, dict[str, object]] = {}
    errors_by_code: dict[str, str] = {}
    worker_count = max(1, min(int(max_workers), len(normalized_codes)))

    if worker_count == 1:
        for code in normalized_codes:
            fetched_code, signal_row, strength_metrics, error = run_single(code)
            if only_secondary_golden_cross and signal_row:
                if signal_row.get("MACD形态") != SECONDARY_GOLDEN_CROSS_PATTERN:
                    signal_row = None
            if strength_metrics:
                metrics_by_code[fetched_code] = strength_metrics
            if signal_row:
                rows_by_code[fetched_code] = signal_row
            if error:
                errors_by_code[fetched_code] = error
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(run_single, code): code for code in normalized_codes}
            for future in as_completed(future_map):
                fetched_code, signal_row, strength_metrics, error = future.result()
                if only_secondary_golden_cross and signal_row:
                    if signal_row.get("MACD形态") != SECONDARY_GOLDEN_CROSS_PATTERN:
                        signal_row = None
                if strength_metrics:
                    metrics_by_code[fetched_code] = strength_metrics
                if signal_row:
                    rows_by_code[fetched_code] = signal_row
                if error:
                    errors_by_code[fetched_code] = error

    apply_relative_strength(rows_by_code, metrics_by_code)
    if min_score is not None:
        rows_by_code = {
            code: row
            for code, row in rows_by_code.items()
            if float(row.get("信号评分", 0)) >= float(min_score)
        }

    rows: list[dict[str, object]] = [rows_by_code[code] for code in normalized_codes if code in rows_by_code]
    errors: list[dict[str, str]] = [
        {"股票代码": code, "error": errors_by_code[code]}
        for code in normalized_codes
        if code in errors_by_code
    ]

    if not rows:
        return pd.DataFrame(columns=SIGNAL_OUTPUT_COLUMNS), errors
    return pd.DataFrame(rows, columns=SIGNAL_OUTPUT_COLUMNS), errors
