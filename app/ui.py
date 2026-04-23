from __future__ import annotations

import io
import os

import pandas as pd
import requests
import streamlit as st

DEFAULT_CODES = """600592
600487
601105
002309
600343
""".strip()
DEFAULT_THSDK_SYMBOL = "USZA300033"
DEFAULT_NOTIFICATION_CHANNEL = os.getenv("AI_FINANCE_NOTIFICATION_CHANNEL", "stdout").strip() or "stdout"


def request_api(
    base_url: str,
    path: str,
    method: str = "POST",
    payload: dict | None = None,
    params: dict | None = None,
    timeout_seconds: int | float = 30,
) -> dict:
    url = f"{base_url.rstrip('/')}{path}"
    resp = requests.request(
        method=method.upper(),
        url=url,
        json=payload,
        params=params,
        timeout=timeout_seconds,
    )
    if resp.status_code >= 400:
        detail = ""
        try:
            detail = resp.json().get("detail", "")
        except Exception:  # noqa: BLE001
            detail = resp.text
        raise RuntimeError(f"{resp.status_code}: {detail}")
    return resp.json()


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8-sig")


def show_downloadable_table(df: pd.DataFrame, file_name: str) -> None:
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "下载 CSV",
        data=df_to_csv_bytes(df),
        file_name=file_name,
        mime="text/csv",
    )


def show_event_table(items: list[dict], file_name: str) -> None:
    df = pd.DataFrame(items)
    if df.empty:
        st.warning("没有命中数据。")
        return

    preferred_cols = [
        "trade_date",
        "code",
        "summary",
        "severity",
        "indicator",
        "event_type",
        "close_price",
        "pct_change",
        "created_at",
    ]
    cols = [c for c in preferred_cols if c in df.columns]
    if cols:
        df = df[cols]
    show_downloadable_table(df, file_name)


def main() -> None:
    st.set_page_config(page_title="TDX 主力净流入看板", layout="wide")
    st.title("股票数据查询")

    api_base = st.sidebar.text_input(
        "API 地址",
        value=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"),
    )

    with st.expander("说明", expanded=False):
        st.markdown(
            "- 先启动 FastAPI: `uvicorn app.api:app --reload`\n"
            "- 再打开本页面: `streamlit run app/ui.py`\n"
            "- 本页面调用 `/api/tdx/flow-rank`、`/api/signals/daily`、`/api/watchlists/default`、`/api/signals/scan-default`、`/api/signals/events` 与 `/api/thsdk/klines`。"
        )
    flow_tab, signal_tab, alerts_tab, history_tab, review_tab, watchlist_tab, kline_tab = st.tabs(
        ["主力净流入", "日线信号扫描", "今日提醒", "历史事件", "复盘统计", "股票池", "THSDK K线"]
    )

    with flow_tab:
        codes_text = st.text_area("股票代码（每行一个）", value=DEFAULT_CODES, height=160)
        c1, c2, c3 = st.columns(3)
        min_inflow = c1.number_input("最小主力净流入（元）", min_value=0.0, value=20_000_000.0, step=10_000_000.0)
        limit = int(c2.number_input("返回条数", min_value=1, max_value=200, value=20, step=1))
        inflow_field = c3.text_input("净流入字段", value="Zjl_HB")

        if st.button("查询主力净流入排名", type="primary"):
            payload = {
                "codes_text": codes_text,
                "min_net_inflow": float(min_inflow),
                "limit": int(limit),
                "inflow_field": inflow_field.strip() or "Zjl_HB",
            }
            try:
                data = request_api(api_base, "/api/tdx/flow-rank", payload=payload)
            except Exception as exc:  # noqa: BLE001
                st.error(f"查询失败: {exc}")
                st.stop()

            items = data.get("items", [])
            source = data.get("source", "unknown")
            st.caption(f"as_of={data.get('as_of', '')} | source={source} | count={len(items)}")
            if data.get("warning"):
                st.warning(data["warning"])
            if not items:
                st.warning("没有命中数据。")
                st.stop()

            df = pd.DataFrame(items)
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
                df = df[cols]

            show_downloadable_table(df, "tdx_flow_rank.csv")

    with signal_tab:
        st.caption("这里是临时扫描输入股票；如果要沉淀进复盘事件库，请使用“今日提醒”里的默认股票池扫描。")
        codes_text = st.text_area("股票代码（每行一个）", value=DEFAULT_CODES, height=160, key="signal_codes")
        if st.button("从默认股票池载入", key="signal_load_default_watchlist"):
            try:
                watchlist = request_api(
                    api_base,
                    "/api/watchlists/default",
                    method="GET",
                    timeout_seconds=60,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载默认股票池失败: {exc}")
                st.stop()

            st.session_state["signal_codes"] = "\n".join(
                str(item.get("code", ""))
                for item in watchlist.get("items", [])
            )
            st.rerun()
        c1, c2, c3 = st.columns(3)
        lookback_days = int(c1.number_input("回看天数", min_value=30, max_value=2000, value=180, step=10))
        adjust = c2.selectbox("复权方式", options=["qfq", "hfq", ""], format_func=lambda x: x or "不复权")
        max_workers = int(c3.number_input("并发数", min_value=1, max_value=32, value=8, step=1))
        only_secondary_golden_cross = st.checkbox("仅保留“水下金叉后水上再次金叉”", value=False)

        if st.button("扫描今日新信号", type="primary"):
            payload = {
                "codes_text": codes_text,
                "lookback_days": lookback_days,
                "adjust": adjust,
                "max_workers": max_workers,
                "only_secondary_golden_cross": only_secondary_golden_cross,
            }
            try:
                data = request_api(
                    api_base,
                    "/api/signals/daily",
                    payload=payload,
                    timeout_seconds=600,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"查询失败: {exc}")
                st.stop()

            items = data.get("items", [])
            errors = data.get("errors", [])
            st.caption(f"as_of={data.get('as_of', '')} | source={data.get('source', 'unknown')} | count={len(items)}")
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("扫描股票数", data.get("requested_count", len(items) + len(errors)))
            m2.metric("命中信号数", len(items))
            m3.metric("错误数", data.get("error_count", len(errors)))
            m4.metric("耗时(秒)", data.get("elapsed_seconds", ""))
            for error in errors:
                st.warning(f"{error.get('股票代码', '')}: {error.get('error', '')}")
            if errors and not items and data.get("error_count", 0) == data.get("requested_count", 0):
                st.error("当前行情源连接不稳定，本次扫描未取回有效日线数据。建议稍后重试，或先缩小扫描范围验证。")
            if not items:
                st.warning("今天没有发现新的金叉/死叉或均线上穿/下穿信号。")
                st.stop()

            df = pd.DataFrame(items)
            preferred_cols = ["股票代码", "日期", "收盘", "涨跌幅", "MACD信号", "MACD形态", "均线信号", "信号", "DIF", "DEA", "MA5", "MA20"]
            cols = [c for c in preferred_cols if c in df.columns]
            if cols:
                df = df[cols]

            show_downloadable_table(df, "daily_signal_alerts.csv")

    with alerts_tab:
        st.caption("执行每日任务后，新的信号事件会写入 SQLite，并按通知渠道去重，便于后续自动化调度。")
        event_date = st.date_input("交易日", value=pd.Timestamp.now().date(), key="event_date")
        c1, c2, c3 = st.columns(3)
        notification_channel = c1.selectbox(
            "通知渠道",
            options=["stdout", "feishu_webhook"],
            index=0 if DEFAULT_NOTIFICATION_CHANNEL == "stdout" else 1,
            key="job_notification_channel",
        )
        job_workers = int(c3.number_input("任务并发数", min_value=1, max_value=32, value=8, step=1, key="job_workers"))
        if notification_channel == "feishu_webhook":
            st.info("会向飞书机器人地址推送新事件。需要先在运行环境里配置 webhook。")

        if c2.button("执行每日任务", type="primary"):
            try:
                data = request_api(
                    api_base,
                    "/api/signals/run-daily-job",
                    payload={"channel": notification_channel, "max_workers": job_workers},
                    timeout_seconds=600,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"扫描失败: {exc}")
                st.stop()

            st.caption(
                f"as_of={data.get('as_of', '')} | source={data.get('source', 'unknown')} | "
                f"watchlist={data.get('watchlist', {}).get('name', '')} | count={data.get('count', 0)}"
            )
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("扫描股票数", data.get("requested_count", 0))
            m2.metric("新增事件数", data.get("count", 0))
            m3.metric("错误数", data.get("error_count", len(data.get("errors", []))))
            m4.metric("耗时(秒)", data.get("elapsed_seconds", ""))
            for message in data.get("messages", []):
                st.info(message)
            for error in data.get("errors", []):
                st.warning(f"{error.get('股票代码', '')}: {error.get('error', '')}")
            if data.get("errors") and not data.get("items") and data.get("error_count", 0) == data.get("requested_count", 0):
                st.error("当前行情源连接不稳定，本次默认股票池扫描未取回有效日线数据。建议稍后重试。")
            if not data.get("items"):
                st.warning("这次扫描没有产生新的入库事件。")
            else:
                show_event_table(data["items"], "today_signal_events.csv")

        if c1.button("刷新当日事件"):
            try:
                data = request_api(
                    api_base,
                    "/api/signals/events",
                    method="GET",
                    params={"trade_date": str(event_date), "limit": 200},
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            st.caption(f"as_of={data.get('as_of', '')} | count={data.get('count', 0)}")
            show_event_table(data.get("items", []), "today_signal_events.csv")

    with history_tab:
        c1, c2 = st.columns([2, 1])
        code_filter = c1.text_input("股票代码（可选）", value="")
        limit = int(c2.number_input("返回条数", min_value=1, max_value=500, value=100, step=10, key="history_limit"))

        if st.button("加载历史事件"):
            params = {"limit": limit}
            if code_filter.strip():
                params["code"] = code_filter.strip()
            try:
                data = request_api(
                    api_base,
                    "/api/signals/events",
                    method="GET",
                    params=params,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            st.caption(f"as_of={data.get('as_of', '')} | count={data.get('count', 0)}")
            show_event_table(data.get("items", []), "signal_event_history.csv")

    with review_tab:
        st.caption("先回填事件后的未来表现，再按指定 horizon 聚合成复盘统计。")
        c1, c2, c3 = st.columns(3)
        review_code = c1.text_input("股票代码过滤（可选）", value="", key="review_code")
        review_date = c2.text_input("交易日过滤（可选）", value="", key="review_date")
        horizon = c3.selectbox("统计 horizon", options=["T+1", "T+3", "T+5"], index=1)

        if st.button("回填复盘快照", type="primary"):
            payload = {
                "code": review_code.strip(),
                "trade_date": review_date.strip(),
                "horizons": [1, 3, 5],
                "adjust": "qfq",
            }
            try:
                data = request_api(
                    api_base,
                    "/api/reviews/backfill",
                    payload=payload,
                    timeout_seconds=600,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"回填失败: {exc}")
                st.stop()

            st.caption(f"as_of={data.get('as_of', '')} | count={data.get('count', 0)}")
            for error in data.get("errors", []):
                st.warning(f"{error.get('股票代码', '')}: {error.get('error', '')}")
            snapshots = pd.DataFrame(data.get("items", []))
            if snapshots.empty:
                st.warning("没有生成新的复盘快照。")
            else:
                preferred_cols = [
                    "trade_date",
                    "code",
                    "summary",
                    "horizon",
                    "future_trade_date",
                    "future_close_price",
                    "pct_return",
                    "max_drawdown",
                ]
                cols = [c for c in preferred_cols if c in snapshots.columns]
                if cols:
                    snapshots = snapshots[cols]
                show_downloadable_table(snapshots, "review_snapshots.csv")

        if st.button("加载复盘统计"):
            params = {"horizon": horizon}
            if review_code.strip():
                params["code"] = review_code.strip()
            if review_date.strip():
                params["trade_date"] = review_date.strip()
            try:
                data = request_api(
                    api_base,
                    "/api/reviews/stats",
                    method="GET",
                    params=params,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            stats_df = pd.DataFrame(data.get("items", []))
            st.caption(f"as_of={data.get('as_of', '')} | count={data.get('count', 0)}")
            if stats_df.empty:
                st.warning("当前条件下没有复盘统计数据。")
            else:
                preferred_cols = [
                    "summary",
                    "indicator",
                    "event_type",
                    "sample_count",
                    "avg_return",
                    "win_rate",
                    "avg_max_drawdown",
                    "horizon",
                ]
                cols = [c for c in preferred_cols if c in stats_df.columns]
                if cols:
                    stats_df = stats_df[cols]
                show_downloadable_table(stats_df, "review_stats.csv")

    with watchlist_tab:
        try:
            watchlist = request_api(api_base, "/api/watchlists/default", method="GET")
            current_codes_text = "\n".join(str(item.get("code", "")) for item in watchlist.get("items", []))
        except Exception as exc:  # noqa: BLE001
            watchlist = None
            current_codes_text = ""
            st.error(f"读取股票池失败: {exc}")

        if "watchlist_codes_text" not in st.session_state:
            st.session_state["watchlist_codes_text"] = current_codes_text

        if watchlist is not None:
            st.caption(
                f"名称={watchlist.get('name', '')} | count={watchlist.get('count', 0)} | "
                f"updated_at={watchlist.get('updated_at', '')}"
            )

        st.text_area(
            "默认股票池代码（每行一个）",
            key="watchlist_codes_text",
            height=180,
            placeholder=DEFAULT_CODES,
        )

        c1, c2, c3 = st.columns(3)
        if c1.button("从服务端刷新股票池"):
            st.session_state["watchlist_codes_text"] = current_codes_text
            st.rerun()

        if c2.button("保存默认股票池", type="primary"):
            payload = {"codes_text": st.session_state.get("watchlist_codes_text", "")}
            try:
                data = request_api(api_base, "/api/watchlists/default", payload=payload)
            except Exception as exc:  # noqa: BLE001
                st.error(f"保存失败: {exc}")
                st.stop()

            st.success(f"已保存默认股票池，共 {data.get('count', 0)} 只股票。")

        if c3.button("导入沪深300成分股"):
            try:
                data = request_api(
                    api_base,
                    "/api/watchlists/default/import-index",
                    payload={"index_code": "000300"},
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"导入失败: {exc}")
                st.stop()

            st.session_state["watchlist_codes_text"] = "\n".join(str(item.get("code", "")) for item in data.get("items", []))
            st.success(f"已导入沪深300成分股，共 {data.get('count', 0)} 只股票。")

    with kline_tab:
        c1, c2 = st.columns([2, 1])
        symbol = c1.text_input("THSDK 代码", value=DEFAULT_THSDK_SYMBOL)
        count = int(c2.number_input("K线数量", min_value=1, max_value=2000, value=20, step=1))

        if st.button("查询 THSDK K线"):
            payload = {
                "symbol": symbol.strip(),
                "count": count,
            }
            try:
                data = request_api(api_base, "/api/thsdk/klines", payload=payload)
            except Exception as exc:  # noqa: BLE001
                st.error(f"查询失败: {exc}")
                st.stop()

            items = data.get("items", [])
            st.caption(f"as_of={data.get('as_of', '')} | source={data.get('source', 'unknown')} | count={len(items)}")
            if not items:
                st.warning("没有命中数据。")
                st.stop()

            df = pd.DataFrame(items)
            show_downloadable_table(df, "thsdk_klines.csv")


if __name__ == "__main__":
    main()
