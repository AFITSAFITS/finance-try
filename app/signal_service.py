from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
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

SIGNAL_OUTPUT_COLUMNS = [
    "股票代码",
    "日期",
    "收盘",
    "涨跌幅",
    "DIF",
    "DEA",
    "MACD信号",
    "MACD形态",
    "MA5",
    "MA20",
    "均线信号",
    "信号",
]


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
    normalized["收盘"] = pd.to_numeric(normalized["收盘"], errors="coerce")
    if "涨跌幅" in normalized.columns:
        normalized["涨跌幅"] = pd.to_numeric(normalized["涨跌幅"], errors="coerce")
    else:
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

    return {
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
        "均线信号": ma_signal,
        "信号": ", ".join(signals),
    }


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


def fetch_daily_history_akshare(code: str, lookback_days: int = 180, adjust: str = "qfq") -> pd.DataFrame:
    end_date = datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=int(lookback_days))).strftime("%Y%m%d")
    try:
        return fetch_daily_history_eastmoney(
            code=code,
            start_date=start_date,
            end_date=end_date,
            adjust=adjust,
        )
    except Exception:  # noqa: BLE001
        return _fetch_daily_history_akshare_provider(
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
) -> tuple[pd.DataFrame, list[dict[str, str]]]:
    normalized_codes = tdx_service.validate_codes(codes)

    def run_single(code: str) -> tuple[str, dict[str, object] | None, str | None]:
        try:
            history_df = fetcher(code, int(lookback_days), adjust)
            signal_row = extract_latest_signal_row(code, history_df)
            return code, signal_row, None
        except Exception as exc:  # noqa: BLE001
            return code, None, str(exc)

    rows_by_code: dict[str, dict[str, object]] = {}
    errors_by_code: dict[str, str] = {}
    worker_count = max(1, min(int(max_workers), len(normalized_codes)))

    if worker_count == 1:
        for code in normalized_codes:
            fetched_code, signal_row, error = run_single(code)
            if only_secondary_golden_cross and signal_row:
                if signal_row.get("MACD形态") != SECONDARY_GOLDEN_CROSS_PATTERN:
                    signal_row = None
            if signal_row:
                rows_by_code[fetched_code] = signal_row
            if error:
                errors_by_code[fetched_code] = error
    else:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            future_map = {executor.submit(run_single, code): code for code in normalized_codes}
            for future in as_completed(future_map):
                fetched_code, signal_row, error = future.result()
                if only_secondary_golden_cross and signal_row:
                    if signal_row.get("MACD形态") != SECONDARY_GOLDEN_CROSS_PATTERN:
                        signal_row = None
                if signal_row:
                    rows_by_code[fetched_code] = signal_row
                if error:
                    errors_by_code[fetched_code] = error

    rows: list[dict[str, object]] = [rows_by_code[code] for code in normalized_codes if code in rows_by_code]
    errors: list[dict[str, str]] = [
        {"股票代码": code, "error": errors_by_code[code]}
        for code in normalized_codes
        if code in errors_by_code
    ]

    if not rows:
        return pd.DataFrame(columns=SIGNAL_OUTPUT_COLUMNS), errors
    return pd.DataFrame(rows, columns=SIGNAL_OUTPUT_COLUMNS), errors
