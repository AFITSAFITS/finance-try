#!/usr/bin/env python3
"""Stock data CLI.

Primary provider: TongDaXin TdxQuant (requires local client + tqcenter)
Fallback provider: AkShare (public web source)
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app import bar_service
from app import limit_up_service
from app import realtime_quote_service
from app import sector_rotation_service
from app import signal_service


def parse_money(value: object) -> float:
    """Parse values like '1.23亿', '4567.89万' into CNY."""
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


def parse_pct(value: object) -> float:
    raw = str(value).strip().replace("%", "").replace(",", "")
    try:
        return float(raw)
    except ValueError:
        return float("nan")


def format_code(code: object) -> str:
    return str(code).strip().zfill(6)


def to_tdx_symbol(code: str) -> str:
    code = code.strip()
    if "." in code:
        return code.upper()
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("0", "3")):
        return f"{code}.SZ"
    return code


def parse_china_number(value: object) -> float:
    """Parse '1.23亿' / '4567万' / '12345' into float CNY."""
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


def normalize_codes(codes: Iterable[str]) -> list[str]:
    result: list[str] = []
    for c in codes:
        s = c.strip()
        if not s:
            continue
        if "." in s:
            s = s.split(".")[0]
        result.append(format_code(s))
    return result


def load_codes(codes_arg: str, codes_file: str) -> list[str]:
    codes: list[str] = []
    if codes_arg:
        codes.extend(x.strip() for x in codes_arg.split(",") if x.strip())
    if codes_file:
        text = Path(codes_file).read_text(encoding="utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            codes.append(line.split(",")[0].strip())
    return normalize_codes(codes)


def fetch_akshare_flow_snapshot() -> pd.DataFrame:
    import akshare as ak

    df = ak.stock_fund_flow_individual(symbol="即时")
    df["股票代码"] = df["股票代码"].map(format_code)
    df["净额_元"] = df["净额"].map(parse_money)
    df["涨跌幅pct"] = df["涨跌幅"].map(parse_pct)
    return df


def flow_rank_akshare(min_net_inflow: float, min_pct: float, limit: int) -> pd.DataFrame:
    df = fetch_akshare_flow_snapshot()
    base = df.dropna(subset=["净额_元", "涨跌幅pct"]).copy()
    mask = (base["净额_元"] >= min_net_inflow) & (base["涨跌幅pct"] >= min_pct)
    result = base.loc[mask].sort_values("净额_元", ascending=False).head(limit).copy()
    result["净流入(亿)"] = (result["净额_元"] / 1e8).round(4)
    return result


def flow_for_codes_akshare(codes: Iterable[str]) -> pd.DataFrame:
    code_set = {format_code(c) for c in codes}
    df = fetch_akshare_flow_snapshot()
    result = df[df["股票代码"].isin(code_set)].copy()
    result["净流入(亿)"] = (result["净额_元"] / 1e8).round(4)
    return result.sort_values("净额_元", ascending=False)


def more_info_tdx(codes: Iterable[str], fields: list[str]) -> pd.DataFrame:
    try:
        from tqcenter import tq
    except ImportError as exc:
        raise RuntimeError(
            "未安装 tqcenter。请先安装并启动通达信客户端，再使用 TdxQuant 模式。"
        ) from exc

    tq.initialize(__file__)
    rows: list[dict[str, object]] = []
    for raw_code in codes:
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
) -> pd.DataFrame:
    fields = fields or ["HqDate", "ZAF", "Zjl", "Zjl_HB"]
    if inflow_field not in fields:
        fields.append(inflow_field)

    raw_df = more_info_tdx(codes, fields)
    if raw_df.empty:
        return raw_df

    raw_df["股票代码"] = raw_df["symbol"].map(lambda x: str(x).split(".")[0])
    raw_df["主力净流入_元"] = raw_df[inflow_field].map(parse_china_number)
    raw_df = raw_df.dropna(subset=["主力净流入_元"]).copy()
    raw_df = raw_df[raw_df["主力净流入_元"] >= min_net_inflow]
    raw_df["主力净流入(亿)"] = (raw_df["主力净流入_元"] / 1e8).round(4)
    raw_df = raw_df.sort_values("主力净流入_元", ascending=False).head(limit)
    return raw_df


def print_df(df: pd.DataFrame, output: str | None) -> None:
    if df.empty:
        print("没有命中数据。")
        return

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)
    print(df.to_string(index=False))

    if output:
        df.to_csv(output, index=False, encoding="utf-8-sig")
        print(f"\n已写出: {output}")


def format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "-"
    return ",".join(f"{key}:{value}" for key, value in counts.items())


def print_signal_summary(summary: dict[str, object]) -> None:
    print(
        "signal_summary "
        f"signals={summary.get('signals', 0)} "
        f"errors={summary.get('error_count', 0)} "
        f"max_score={summary.get('max_score', '-')} "
        f"stale_signals={summary.get('stale_signals', 0)} "
        f"observations={format_counts(summary.get('observation_counts', {}))} "
        f"freshness={format_counts(summary.get('freshness_counts', {}))} "
        f"directions={format_counts(summary.get('direction_counts', {}))}"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch stock data.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_tdx_rank = sub.add_parser("tdx-flow-rank", help="Main inflow ranking from TdxQuant.")
    p_tdx_rank.add_argument("--codes", type=str, default="", help="Comma-separated codes")
    p_tdx_rank.add_argument("--codes-file", type=str, default="", help="One code per line")
    p_tdx_rank.add_argument("--min-net-inflow", type=float, default=0.0, help="CNY")
    p_tdx_rank.add_argument("--limit", type=int, default=20)
    p_tdx_rank.add_argument("--inflow-field", type=str, default="Zjl_HB")
    p_tdx_rank.add_argument(
        "--fields",
        type=str,
        default="HqDate,ZAF,Zjl,Zjl_HB",
        help="Comma-separated fields for get_more_info",
    )
    p_tdx_rank.add_argument("--output", type=str, default="")

    p_rank = sub.add_parser("ak-flow-rank", help="Main inflow ranking from AkShare.")
    p_rank.add_argument("--min-net-inflow", type=float, default=20_000_000, help="CNY")
    p_rank.add_argument("--min-pct", type=float, default=0.0, help="min change pct")
    p_rank.add_argument("--limit", type=int, default=20)
    p_rank.add_argument("--output", type=str, default="")

    p_codes = sub.add_parser("ak-flow-by-codes", help="Main inflow for given codes from AkShare.")
    p_codes.add_argument("--codes", type=str, required=True, help="Comma-separated 6-digit codes")
    p_codes.add_argument("--output", type=str, default="")

    p_quotes = sub.add_parser("realtime-quotes", help="Realtime quote snapshot from public providers.")
    p_quotes.add_argument("--codes", type=str, default="", help="Comma-separated codes")
    p_quotes.add_argument("--codes-file", type=str, default="", help="One code per line")
    p_quotes.add_argument("--output", type=str, default="")

    p_tdx = sub.add_parser("tdx-more-info", help="TongDaXin TdxQuant get_more_info.")
    p_tdx.add_argument("--codes", type=str, required=True, help="Comma-separated codes")
    p_tdx.add_argument(
        "--fields",
        type=str,
        default="HqDate,Zjl,Zjl_HB",
        help="Comma-separated field list",
    )
    p_tdx.add_argument("--output", type=str, default="")

    p_signal = sub.add_parser("daily-signals", help="Daily MACD/MA cross alerts from AkShare.")
    p_signal.add_argument("--codes", type=str, default="", help="Comma-separated codes")
    p_signal.add_argument("--codes-file", type=str, default="", help="One code per line")
    p_signal.add_argument("--lookback-days", type=int, default=180, help="Calendar days to fetch")
    p_signal.add_argument("--adjust", type=str, default="qfq", help="qfq / hfq / empty string")
    p_signal.add_argument("--max-workers", type=int, default=8, help="Parallel fetch workers")
    p_signal.add_argument("--min-score", type=float, default=0, help="Only keep signals with score >= this value")
    p_signal.add_argument(
        "--only-secondary-golden-cross",
        action="store_true",
        help="Only keep stocks with 水下金叉后水上再次金叉",
    )
    p_signal.add_argument("--output", type=str, default="")

    p_limit = sub.add_parser("limit-up-breakthroughs", help="Scan and save daily limit-up breakthrough candidates.")
    p_limit.add_argument("--trade-date", type=str, default="", help="YYYY-MM-DD or YYYYMMDD; default today")
    p_limit.add_argument("--lookback-days", type=int, default=120)
    p_limit.add_argument("--min-score", type=float, default=50)
    p_limit.add_argument("--max-items", type=int, default=100)
    p_limit.add_argument("--pool-limit", type=int, default=200)
    p_limit.add_argument("--output", type=str, default="")

    p_sector = sub.add_parser("sector-rotation", help="Scan and save active low-position sectors.")
    p_sector.add_argument("--trade-date", type=str, default="", help="YYYY-MM-DD or YYYYMMDD; default today")
    p_sector.add_argument("--sector-type", type=str, default="industry", choices=["industry", "concept"])
    p_sector.add_argument("--top-n", type=int, default=30)
    p_sector.add_argument("--max-items", type=int, default=20)
    p_sector.add_argument("--output", type=str, default="")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"as_of={now}")

    try:
        if args.cmd == "tdx-flow-rank":
            codes = load_codes(args.codes, args.codes_file)
            if not codes:
                raise SystemExit("请至少提供 --codes 或 --codes-file")
            fields = [x.strip() for x in args.fields.split(",") if x.strip()]
            df = flow_rank_tdx(
                codes=codes,
                fields=fields,
                inflow_field=args.inflow_field.strip(),
                min_net_inflow=float(args.min_net_inflow),
                limit=int(args.limit),
            )
            preferred_cols = [
                "股票代码",
                "symbol",
                "HqDate",
                "ZAF",
                "Zjl",
                "Zjl_HB",
                "主力净流入(亿)",
            ]
            cols = [c for c in preferred_cols if c in df.columns]
            if cols:
                print_df(df[cols], args.output or None)
            else:
                print_df(df, args.output or None)
            return 0

        if args.cmd == "ak-flow-rank":
            df = flow_rank_akshare(
                min_net_inflow=float(args.min_net_inflow),
                min_pct=float(args.min_pct),
                limit=int(args.limit),
            )
            cols = ["股票代码", "股票简称", "最新价", "涨跌幅", "净额", "净流入(亿)", "流入资金", "流出资金"]
            print_df(df[cols], args.output or None)
            return 0

        if args.cmd == "ak-flow-by-codes":
            codes = [x.strip() for x in args.codes.split(",") if x.strip()]
            df = flow_for_codes_akshare(codes)
            cols = ["股票代码", "股票简称", "最新价", "涨跌幅", "净额", "净流入(亿)"]
            print_df(df[cols], args.output or None)
            return 0

        if args.cmd == "realtime-quotes":
            codes = load_codes(args.codes, args.codes_file)
            if not codes:
                raise SystemExit("请至少提供 --codes 或 --codes-file")
            items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(codes)
            for error in errors:
                print(
                    f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}",
                    file=sys.stderr,
                )
            print(f"source={source}")
            df = pd.DataFrame(items)
            cols = [
                "code",
                "name",
                "latest_price",
                "pct_change",
                "change_amount",
                "open",
                "high",
                "low",
                "prev_close",
                "turnover_rate",
                "volume_ratio",
                "amount",
                "quality_status",
                "quality_note",
                "quote_signal",
                "quote_note",
                "source",
            ]
            selected_cols = [col for col in cols if col in df.columns]
            print_df(df[selected_cols] if selected_cols else df, args.output or None)
            return 0

        if args.cmd == "tdx-more-info":
            codes = [x.strip() for x in args.codes.split(",") if x.strip()]
            fields = [x.strip() for x in args.fields.split(",") if x.strip()]
            df = more_info_tdx(codes, fields)
            print_df(df, args.output or None)
            return 0

        if args.cmd == "daily-signals":
            codes = load_codes(args.codes, args.codes_file)
            if not codes:
                raise SystemExit("请至少提供 --codes 或 --codes-file")
            df, errors = signal_service.scan_stock_signal_events(
                codes=codes,
                lookback_days=int(args.lookback_days),
                adjust=args.adjust,
                fetcher=bar_service.fetch_daily_history_cached,
                max_workers=int(args.max_workers),
                only_secondary_golden_cross=bool(args.only_secondary_golden_cross),
                min_score=float(args.min_score),
            )
            for error in errors:
                print(
                    f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}",
                    file=sys.stderr,
                )
            print_signal_summary(signal_service.summarize_signal_rows(df, errors))
            cols = [
                "股票代码",
                "日期",
                "数据时效",
                "数据滞后天数",
                "收盘",
                "涨跌幅",
                "信号评分",
                "信号方向",
                "信号级别",
                "观察结论",
                "评分原因",
                "风险提示",
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
                "MACD信号",
                "MACD形态",
                "均线信号",
                "信号",
                "DIF",
                "DEA",
                "MA5",
                "MA20",
            ]
            selected_cols = [col for col in cols if col in df.columns]
            selected_df = df[selected_cols] if selected_cols else df
            print_df(selected_df, args.output or None)
            return 0

        if args.cmd == "limit-up-breakthroughs":
            result = limit_up_service.scan_and_save_limit_up_breakthroughs(
                trade_date=args.trade_date or None,
                lookback_days=int(args.lookback_days),
                min_score=float(args.min_score),
                max_items=int(args.max_items),
                pool_limit=int(args.pool_limit),
            )
            for error in result["errors"]:
                print(
                    f"WARNING [{error.get('股票代码', '')}]: {error.get('error', '')}",
                    file=sys.stderr,
                )
            df = pd.DataFrame(result["items"])
            cols = [
                "trade_date",
                "code",
                "name",
                "sector",
                "close_price",
                "pct_change",
                "turnover_rate",
                "consecutive_boards",
                "sector_limit_up_count",
                "sector_heat_rank",
                "score",
                "reason",
            ]
            selected_cols = [col for col in cols if col in df.columns]
            print_df(df[selected_cols] if selected_cols else df, args.output or None)
            return 0

        if args.cmd == "sector-rotation":
            result = sector_rotation_service.scan_and_save_sector_rotation(
                trade_date=args.trade_date or None,
                sector_type=args.sector_type,
                top_n=int(args.top_n),
                max_items=int(args.max_items),
            )
            for error in result["errors"]:
                print(
                    f"WARNING [{error.get('板块', '')}]: {error.get('error', '')}",
                    file=sys.stderr,
                )
            df = pd.DataFrame(result["items"])
            cols = [
                "trade_date",
                "sector_type",
                "sector_name",
                "latest_pct_change",
                "return_5d",
                "return_10d",
                "position_60d",
                "activity_score",
                "rotation_score",
                "signal",
            ]
            selected_cols = [col for col in cols if col in df.columns]
            print_df(df[selected_cols] if selected_cols else df, args.output or None)
            return 0
    except RuntimeError as exc:
        print(f"ERROR: {exc}")
        return 2

    return 1


if __name__ == "__main__":
    sys.exit(main())
