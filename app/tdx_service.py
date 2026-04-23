from __future__ import annotations

from datetime import datetime
from typing import Callable, Iterable

import pandas as pd

DEFAULT_FLOW_FIELDS = ["HqDate", "ZAF", "Zjl", "Zjl_HB"]


class TdxUnavailableError(RuntimeError):
    """Raised when TongDaXin client/runtime is unavailable."""


def format_code(code: object) -> str:
    return str(code).strip().zfill(6)


def to_tdx_symbol(code: str) -> str:
    value = code.strip()
    if "." in value:
        return value.upper()
    if value.startswith(("6", "9")):
        return f"{value}.SH"
    if value.startswith(("0", "3")):
        return f"{value}.SZ"
    return value


def parse_china_number(value: object) -> float:
    """Parse values like '1.23亿' / '4567万' to CNY."""
    raw = str(value).strip().replace(",", "")
    if raw in {"", "-", "nan", "None"}:
        return float("nan")

    sign = -1.0 if raw.startswith("-") else 1.0
    raw = raw.lstrip("+-")

    multiplier = 1.0
    if raw.endswith("亿"):
        multiplier = 1e8
        raw = raw[:-1]
    elif raw.endswith("万"):
        multiplier = 1e4
        raw = raw[:-1]

    try:
        return sign * float(raw) * multiplier
    except ValueError:
        return float("nan")


def parse_money(value: object) -> float:
    """Parse values like '1.23亿', '4567.89万' into CNY."""
    return parse_china_number(value)


def normalize_codes(codes: Iterable[str]) -> list[str]:
    result: list[str] = []
    for value in codes:
        code = str(value).strip()
        if not code:
            continue
        if "." in code:
            code = code.split(".")[0]
        result.append(format_code(code))
    return result


def parse_codes_text(text: str) -> list[str]:
    values: list[str] = []
    for line in (text or "").splitlines():
        item = line.strip()
        if not item or item.startswith("#"):
            continue
        values.append(item.split(",")[0].strip())
    return normalize_codes(values)


def dedupe_keep_order(codes: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for code in codes:
        if code in seen:
            continue
        seen.add(code)
        result.append(code)
    return result


def validate_codes(codes: Iterable[str]) -> list[str]:
    normalized = dedupe_keep_order(normalize_codes(codes))
    if not normalized:
        raise ValueError("至少提供一个股票代码")
    return normalized


def more_info_tdx(codes: Iterable[str], fields: list[str]) -> pd.DataFrame:
    try:
        from tqcenter import tq
    except ImportError as exc:
        raise TdxUnavailableError(
            "未安装 tqcenter。请先安装并启动通达信客户端，再使用 TdxQuant 模式。"
        ) from exc

    tq.initialize(__file__)
    rows: list[dict[str, object]] = []
    for raw_code in validate_codes(codes):
        symbol = to_tdx_symbol(raw_code)
        data = tq.get_more_info(stock_code=symbol, field_list=fields)
        row = {"symbol": symbol}
        if isinstance(data, dict):
            row.update(data)
        else:
            row["raw"] = data
        rows.append(row)
    return pd.DataFrame(rows)


def flow_rank_tdx(
    codes: Iterable[str],
    fields: list[str] | None = None,
    inflow_field: str = "Zjl_HB",
    min_net_inflow: float = 0.0,
    limit: int = 20,
    fetcher: Callable[[list[str], list[str]], pd.DataFrame] = more_info_tdx,
) -> pd.DataFrame:
    """Return TongDaXin inflow ranking for the given codes."""
    normalized_codes = validate_codes(codes)

    selected_fields = list(fields or DEFAULT_FLOW_FIELDS)
    if inflow_field not in selected_fields:
        selected_fields.append(inflow_field)

    raw_df = fetcher(normalized_codes, selected_fields)
    if raw_df.empty:
        return raw_df
    if inflow_field not in raw_df.columns:
        raise ValueError(f"字段缺失: {inflow_field}")

    df = raw_df.copy()
    df["股票代码"] = df["symbol"].map(lambda x: str(x).split(".")[0])
    df["主力净流入_元"] = df[inflow_field].map(parse_china_number)
    df = df.dropna(subset=["主力净流入_元"])
    df = df[df["主力净流入_元"] >= float(min_net_inflow)]
    df["主力净流入(亿)"] = (df["主力净流入_元"] / 1e8).round(4)
    df = df.sort_values("主力净流入_元", ascending=False).head(int(limit)).reset_index(drop=True)
    return df


def fetch_akshare_flow_snapshot() -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_fund_flow_individual(symbol="即时")
    df["股票代码"] = df["股票代码"].map(format_code)
    df["主力净流入_元"] = df["净额"].map(parse_money)
    df["主力净流入(亿)"] = (df["主力净流入_元"] / 1e8).round(4)
    return df


def flow_rank_akshare_for_codes(
    codes: Iterable[str],
    min_net_inflow: float = 0.0,
    limit: int = 20,
) -> pd.DataFrame:
    selected_codes = set(validate_codes(codes))
    df = fetch_akshare_flow_snapshot()
    if "主力净流入_元" not in df.columns:
        if "净额" not in df.columns:
            raise ValueError("AkShare 数据缺少字段: 净额")
        df["主力净流入_元"] = df["净额"].map(parse_money)
    if "主力净流入(亿)" not in df.columns:
        df["主力净流入(亿)"] = (df["主力净流入_元"] / 1e8).round(4)
    result = df[df["股票代码"].isin(selected_codes)].copy()
    result = result.dropna(subset=["主力净流入_元"])
    result = result[result["主力净流入_元"] >= float(min_net_inflow)]
    result = result.sort_values("主力净流入_元", ascending=False).head(int(limit)).reset_index(drop=True)
    return result


def dataframe_to_records(df: pd.DataFrame) -> list[dict[str, object]]:
    if df.empty:
        return []
    safe_df = df.where(pd.notna(df), None)
    return safe_df.to_dict(orient="records")


def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
