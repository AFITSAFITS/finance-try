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


def inject_page_style() -> None:
    st.markdown(
        """
        <style>
        :root {
            --surface: #f6f7f2;
            --surface-strong: #e8ece2;
            --panel: #fffef9;
            --ink: #20241f;
            --muted: #626b60;
            --line: #d4dbc9;
            --accent: #245f47;
            --accent-soft: #e0eadf;
            --blue-soft: #dde8ef;
            --warn-soft: #f5e3c7;
            --bad-soft: #eed5d1;
        }
        .stApp {
            background: var(--surface);
            color: var(--ink);
        }
        section[data-testid="stSidebar"] {
            background: #e9ede4;
            border-right: 1px solid var(--line);
        }
        .block-container {
            padding-top: 1.8rem;
            padding-bottom: 4rem;
            max-width: 1500px;
        }
        h1, h2, h3 {
            color: var(--ink);
            letter-spacing: 0;
        }
        div[data-testid="stMetric"] {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 12px 14px;
        }
        div[data-testid="stMetricLabel"] p {
            color: var(--muted);
            font-size: 0.82rem;
        }
        div[data-testid="stMetricValue"] {
            color: var(--ink);
            font-size: 1.36rem;
        }
        div[data-testid="stButton"] button {
            border-radius: 8px;
            min-height: 2.4rem;
        }
        .stDownloadButton button {
            border-radius: 8px;
        }
        div[data-testid="stTabs"] button {
            border-radius: 0;
            color: var(--muted);
            font-weight: 600;
        }
        div[data-testid="stTabs"] button[aria-selected="true"] {
            color: var(--accent);
            border-bottom-color: var(--accent);
        }
        .workbench-hero {
            display: grid;
            grid-template-columns: minmax(0, 1fr);
            gap: 10px;
            align-items: stretch;
            margin-bottom: 14px;
        }
        .workbench-panel {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 18px 20px;
        }
        .workbench-title {
            margin: 0;
            font-size: clamp(1.65rem, 2.6vw, 2.35rem);
            line-height: 1.12;
            font-weight: 740;
            letter-spacing: 0;
        }
        .workbench-subtitle {
            margin: 8px 0 0;
            max-width: 960px;
            color: var(--muted);
            line-height: 1.55;
            font-size: 0.94rem;
        }
        .status-strip {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-top: 18px;
        }
        .status-pill {
            display: inline-flex;
            align-items: center;
            min-height: 28px;
            padding: 3px 10px;
            border-radius: 999px;
            border: 1px solid var(--line);
            background: var(--accent-soft);
            color: #244b3b;
            font-size: 0.82rem;
            font-weight: 650;
        }
        .status-pill.info {
            background: var(--blue-soft);
            color: #25495c;
        }
        .status-pill.warn {
            background: var(--warn-soft);
            color: #755018;
        }
        .status-pill.bad {
            background: var(--bad-soft);
            color: #78362e;
        }
        .workflow-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 10px;
            margin: 8px 0 18px;
        }
        .workflow-step {
            background: var(--panel);
            border: 1px solid var(--line);
            border-radius: 8px;
            padding: 12px 13px;
            min-height: 86px;
        }
        .workflow-step span {
            display: block;
            color: var(--muted);
            font-size: 0.75rem;
            font-weight: 700;
            margin-bottom: 7px;
        }
        .workflow-step strong {
            display: block;
            color: var(--ink);
            font-size: 0.96rem;
            margin-bottom: 5px;
        }
        .workflow-step p {
            margin: 0;
            color: var(--muted);
            line-height: 1.45;
            font-size: 0.83rem;
        }
        .section-heading {
            margin: 18px 0 12px;
            padding: 14px 16px;
            border-left: 4px solid var(--accent);
            background: rgba(255, 254, 249, 0.72);
            border-top: 1px solid var(--line);
            border-right: 1px solid var(--line);
            border-bottom: 1px solid var(--line);
            border-radius: 8px;
        }
        .section-heading h2 {
            margin: 0;
            font-size: 1.18rem;
            line-height: 1.3;
        }
        .section-heading p {
            margin: 5px 0 0;
            color: var(--muted);
            font-size: 0.9rem;
            line-height: 1.5;
        }
        .result-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin: 12px 0 8px;
        }
        .result-meta span {
            display: inline-flex;
            align-items: center;
            min-height: 27px;
            padding: 3px 9px;
            border-radius: 8px;
            border: 1px solid var(--line);
            background: var(--panel);
            color: var(--muted);
            font-size: 0.8rem;
            font-weight: 650;
        }
        .side-note {
            color: var(--muted);
            font-size: 0.86rem;
            line-height: 1.55;
        }
        .side-note strong {
            color: var(--ink);
        }
        @media (max-width: 900px) {
            .block-container {
                padding-left: 1rem;
                padding-right: 1rem;
            }
            .workflow-grid {
                grid-template-columns: 1fr 1fr;
            }
        }
        @media (max-width: 560px) {
            .workflow-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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
    st.dataframe(df, width="stretch")
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


def render_status_pill(label: str, tone: str = "ok") -> str:
    cls = "status-pill"
    if tone in {"info", "warn", "bad"}:
        cls = f"{cls} {tone}"
    return f'<span class="{cls}">{label}</span>'


def render_section_header(title: str, description: str) -> None:
    st.markdown(
        f"""
        <div class="section-heading">
          <h2>{title}</h2>
          <p>{description}</p>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_result_meta(items: dict[str, object]) -> None:
    chips = "".join(f"<span>{key}={value}</span>" for key, value in items.items() if value not in {None, ""})
    if chips:
        st.markdown(f'<div class="result-meta">{chips}</div>', unsafe_allow_html=True)


def show_api_errors(errors: list[dict], primary_key: str = "股票代码") -> None:
    for error in errors:
        st.warning(f"{error.get(primary_key, '')}: {error.get('error', '')}")


def render_workbench_header(api_base: str) -> None:
    health: dict | None = None
    watchlist: dict | None = None
    latest_run: dict | None = None
    status_notes: list[str] = []

    try:
        health = request_api(api_base, "/health", method="GET", timeout_seconds=5)
    except Exception as exc:  # noqa: BLE001
        status_notes.append(f"API 暂不可用：{exc}")

    try:
        watchlist = request_api(api_base, "/api/watchlists/default", method="GET", timeout_seconds=10)
    except Exception as exc:  # noqa: BLE001
        status_notes.append(f"股票池读取失败：{exc}")

    try:
        runs = request_api(api_base, "/api/signals/scan-runs", method="GET", params={"limit": 1}, timeout_seconds=10)
        run_items = runs.get("items", [])
        if run_items:
            latest_run = run_items[0]
    except Exception as exc:  # noqa: BLE001
        status_notes.append(f"运行记录读取失败：{exc}")

    api_tone = "ok" if health and health.get("ok") else "bad"
    watchlist_count = int(watchlist.get("count", 0)) if watchlist else 0
    watchlist_tone = "ok" if watchlist_count > 0 else "warn"
    run_status = str(latest_run.get("status", "暂无记录")) if latest_run else "暂无记录"
    run_tone = "ok" if run_status == "正常" else "warn"
    if run_status in {"失败", "部分失败"}:
        run_tone = "bad"

    st.markdown(
        f"""
        <div class="workbench-hero">
          <section class="workbench-panel">
            <h1 class="workbench-title">AI Finance 工作台</h1>
            <p class="workbench-subtitle">
              日常看盘从这里开始：先看服务状态和股票池，再做实时行情、默认池扫描、复盘统计和策略观察。
            </p>
            <div class="status-strip">
              {render_status_pill("API 正常" if api_tone == "ok" else "API 异常", api_tone)}
              {render_status_pill(f"默认股票池 {watchlist_count} 只", watchlist_tone)}
              {render_status_pill(f"最近运行：{run_status}", run_tone)}
              {render_status_pill(f"数据源：{health.get('provider', '-') if health else '-'}", "info" if health else "warn")}
            </div>
          </section>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="workflow-grid">
          <div class="workflow-step"><span>01 盘中</span><strong>实时行情</strong><p>先看价格、涨跌、成交和盘中提示。</p></div>
          <div class="workflow-step"><span>02 收盘后</span><strong>今日提醒</strong><p>扫描默认股票池，保留可复盘事件。</p></div>
          <div class="workflow-step"><span>03 复盘</span><strong>结果验证</strong><p>查看后续表现、止损目标和策略结论。</p></div>
          <div class="workflow-step"><span>04 维护</span><strong>基础数据</strong><p>维护股票池、板块轮动和涨停候选。</p></div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("服务时间", health.get("as_of", "-") if health else "-")
    c2.metric("默认股票池", watchlist_count)
    c3.metric("最近扫描事件", latest_run.get("event_count", "-") if latest_run else "-")
    c4.metric("最近扫描错误", latest_run.get("error_count", "-") if latest_run else "-")

    if latest_run and latest_run.get("note"):
        st.caption(f"最近扫描说明：{latest_run.get('note')}")
    for note in status_notes:
        st.warning(note)


def main() -> None:
    st.set_page_config(page_title="AI Finance 工作台", layout="wide")
    inject_page_style()

    st.sidebar.title("AI Finance")
    api_base = st.sidebar.text_input(
        "API 地址",
        value=os.getenv("API_BASE_URL", "http://127.0.0.1:8000"),
    )
    st.sidebar.caption("改地址后，页面所有操作都会请求新的 API。")
    st.sidebar.divider()
    st.sidebar.markdown(
        """
        <div class="side-note">
        <strong>建议顺序</strong><br>
        实时行情 -> 今日提醒 -> 复盘统计 -> 股票池维护
        </div>
        """,
        unsafe_allow_html=True,
    )

    render_workbench_header(api_base)

    with st.expander("说明", expanded=False):
        st.markdown(
            "- 先启动 FastAPI: `uvicorn app.api:app --reload`\n"
            "- 再打开本页面: `streamlit run app/ui.py`\n"
            "- 本页面调用 `/api/tdx/flow-rank`、`/api/market/realtime-quotes`、`/api/signals/daily`、`/api/watchlists/default`、`/api/signals/scan-default`、`/api/signals/events` 与 `/api/thsdk/klines`。"
        )
    (
        flow_tab,
        quote_tab,
        signal_tab,
        limit_up_tab,
        sector_tab,
        alerts_tab,
        history_tab,
        review_tab,
        strategy_tab,
        watchlist_tab,
        kline_tab,
    ) = st.tabs(
        [
            "资金流",
            "实时行情",
            "临时信号",
            "涨停策略",
            "板块轮动",
            "每日任务",
            "历史事件",
            "复盘验证",
            "策略结论",
            "股票池",
            "K线工具",
        ]
    )

    with flow_tab:
        render_section_header("资金流", "按主力净流入做快速排序，用来补充盘中热度判断。")
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
            render_result_meta({"时间": data.get("as_of", ""), "来源": source, "数量": len(items)})
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

    with quote_tab:
        render_section_header("实时行情", "快速查看当前价格、涨跌和盘中观察提示；优先使用东方财富，失败时自动尝试腾讯。")
        c1, c2, c3 = st.columns(3)
        if c1.button("从默认股票池载入", key="quote_load_default_watchlist"):
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
            st.session_state["quote_codes"] = "\n".join(str(item.get("code", "")) for item in watchlist.get("items", []))
            st.rerun()
        codes_text = st.text_area("股票代码（每行一个）", value=DEFAULT_CODES, height=160, key="quote_codes")

        quote_data = None
        if c2.button("查询实时行情", type="primary"):
            try:
                quote_data = request_api(
                    api_base,
                    "/api/market/realtime-quotes",
                    payload={"codes_text": codes_text},
                    timeout_seconds=60,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"查询失败: {exc}")
                st.stop()

        if c3.button("查询默认股票池实时行情"):
            try:
                quote_data = request_api(
                    api_base,
                    "/api/market/realtime-quotes/default",
                    method="GET",
                    timeout_seconds=120,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"查询失败: {exc}")
                st.stop()

        if quote_data is not None:
            render_result_meta(
                {
                    "时间": quote_data.get("as_of", ""),
                    "来源": quote_data.get("source", "unknown"),
                    "数量": quote_data.get("count", 0),
                    "错误": quote_data.get("error_count", 0),
                }
            )
            watchlist_info = quote_data.get("watchlist") or {}
            if watchlist_info:
                st.caption(f"watchlist={watchlist_info.get('name', '')} | requested={quote_data.get('requested_count', 0)}")
            show_api_errors(quote_data.get("errors", []))
            df = pd.DataFrame(quote_data.get("items", []))
            if df.empty:
                st.warning("没有取到实时行情。")
            else:
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
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "realtime_quotes.csv")

    with signal_tab:
        render_section_header("临时信号", "临时输入股票做日线信号扫描；要沉淀进复盘事件库，请使用“每日任务”。")
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
        codes_text = st.text_area("股票代码（每行一个）", value=DEFAULT_CODES, height=160, key="signal_codes")
        c1, c2, c3, c4 = st.columns(4)
        lookback_days = int(c1.number_input("回看天数", min_value=30, max_value=2000, value=180, step=10))
        adjust = c2.selectbox("复权方式", options=["qfq", "hfq", ""], format_func=lambda x: x or "不复权")
        max_workers = int(c3.number_input("并发数", min_value=1, max_value=32, value=8, step=1))
        min_signal_score = float(c4.number_input("最低评分", min_value=0.0, max_value=100.0, value=0.0, step=5.0))
        only_secondary_golden_cross = st.checkbox("仅保留“水下金叉后水上再次金叉”", value=False)

        if st.button("扫描今日新信号", type="primary"):
            payload = {
                "codes_text": codes_text,
                "lookback_days": lookback_days,
                "adjust": adjust,
                "max_workers": max_workers,
                "only_secondary_golden_cross": only_secondary_golden_cross,
                "min_score": min_signal_score,
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
            render_result_meta({"时间": data.get("as_of", ""), "来源": data.get("source", "unknown"), "数量": len(items)})
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("扫描股票数", data.get("requested_count", len(items) + len(errors)))
            m2.metric("命中信号数", len(items))
            m3.metric("错误数", data.get("error_count", len(errors)))
            m4.metric("耗时(秒)", data.get("elapsed_seconds", ""))
            summary = data.get("signal_summary", {}) if isinstance(data.get("signal_summary"), dict) else {}
            if summary:
                st.caption(
                    f"可观察={summary.get('actionable_signals', 0)} | "
                    f"建议跳过={summary.get('no_action_signals', 0)} | "
                    f"观察结论={summary.get('observation_counts', {})} | "
                    f"观察仓位={summary.get('position_size_counts', {})} | "
                    f"数据时效={summary.get('freshness_counts', {})} | "
                    f"最高评分={summary.get('max_score', '-')}"
                )
            show_api_errors(errors)
            if errors and not items and data.get("error_count", 0) == data.get("requested_count", 0):
                st.error("当前行情源连接不稳定，本次扫描未取回有效日线数据。建议稍后重试，或先缩小扫描范围验证。")
            if not items:
                st.warning("今天没有发现新的金叉/死叉或均线上穿/下穿信号。")
                st.stop()

            df = pd.DataFrame(items)
            preferred_cols = [
                "股票代码",
                "日期",
                "数据时效",
                "数据滞后天数",
                "数据来源",
                "缓存获取时间",
                "收盘",
                "涨跌幅",
                "信号评分",
                "信号方向",
                "信号级别",
                "观察结论",
                "观察仓位",
                "执行提示",
                "评分原因",
                "风险提示",
                "60日位置",
                "量能比",
                "20日涨幅",
                "60日涨幅",
                "相对强度",
                "相对强度分层",
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
            cols = [c for c in preferred_cols if c in df.columns]
            if cols:
                df = df[cols]

            show_downloadable_table(df, "daily_signal_alerts.csv")

    with limit_up_tab:
        render_section_header("涨停策略", "扫描每日涨停池，按突破、均线、连板和封板稳定性筛出候选，并保存到本地库。")
        c1, c2, c3, c4, c5 = st.columns(5)
        limit_trade_date = c1.date_input("交易日", value=pd.Timestamp.now().date(), key="limit_trade_date")
        limit_lookback = int(c2.number_input("回看天数", min_value=30, max_value=1000, value=120, step=10, key="limit_lookback"))
        limit_min_score = float(c3.number_input("最低评分", min_value=0.0, max_value=100.0, value=50.0, step=5.0, key="limit_min_score"))
        limit_max_items = int(c4.number_input("最多保存", min_value=1, max_value=500, value=100, step=10, key="limit_max_items"))
        limit_pool_limit = int(c5.number_input("最多分析涨停股", min_value=1, max_value=1000, value=200, step=20, key="limit_pool_limit"))

        if st.button("扫描并保存涨停突破", type="primary"):
            try:
                data = request_api(
                    api_base,
                    "/api/limit-up/breakthroughs",
                    payload={
                        "trade_date": str(limit_trade_date),
                        "lookback_days": limit_lookback,
                        "min_score": limit_min_score,
                        "max_items": limit_max_items,
                        "pool_limit": limit_pool_limit,
                    },
                    timeout_seconds=900,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"扫描失败: {exc}")
                st.stop()

            render_result_meta({"时间": data.get("as_of", ""), "交易日": data.get("trade_date", ""), "数量": data.get("count", 0)})
            show_api_errors(data.get("errors", []))
            df = pd.DataFrame(data.get("items", []))
            if df.empty:
                st.warning("当前条件下没有筛出涨停突破候选。")
            else:
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
                    "data_source",
                    "cache_fetched_at",
                    "score",
                    "reason",
                ]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "limit_up_breakthroughs.csv")

        if st.button("加载已保存涨停突破"):
            try:
                data = request_api(
                    api_base,
                    "/api/limit-up/breakthroughs",
                    method="GET",
                    params={"trade_date": str(limit_trade_date), "limit": limit_max_items},
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            df = pd.DataFrame(data.get("items", []))
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if df.empty:
                st.warning("没有已保存的涨停突破候选。")
            else:
                cols = [
                    "trade_date",
                    "code",
                    "name",
                    "sector",
                    "close_price",
                    "pct_change",
                    "sector_limit_up_count",
                    "sector_heat_rank",
                    "data_source",
                    "cache_fetched_at",
                    "score",
                    "reason",
                    "created_at",
                ]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "limit_up_breakthrough_history.csv")

        st.divider()
        st.caption("对已保存的涨停候选做后续表现复盘，用来判断评分分层是否真的有效。")
        c1, c2 = st.columns(2)
        review_horizon = c1.selectbox("复盘统计周期", options=["T+1", "T+3", "T+5"], index=1, key="limit_review_horizon")
        review_code = c2.text_input("股票代码过滤（可选）", value="", key="limit_review_code")
        limit_due_only = st.checkbox("只回填已到期涨停候选", value=True, key="limit_review_due_only")

        if st.button("回填涨停候选复盘"):
            try:
                data = request_api(
                    api_base,
                    "/api/limit-up/reviews/backfill",
                    payload={
                        "trade_date": str(limit_trade_date),
                        "code": review_code.strip(),
                        "horizons": [1, 3, 5],
                        "adjust": "qfq",
                        "due_only": limit_due_only,
                    },
                    timeout_seconds=900,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"回填失败: {exc}")
                st.stop()

            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            show_api_errors(data.get("errors", []))
            df = pd.DataFrame(data.get("items", []))
            if df.empty:
                st.warning("没有生成新的涨停候选复盘。")
            else:
                cols = [
                    "trade_date",
                    "code",
                    "name",
                    "score",
                    "sector_limit_up_count",
                    "data_source",
                    "cache_fetched_at",
                    "horizon",
                    "future_trade_date",
                    "pct_return",
                    "max_drawdown",
                ]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "limit_up_review_snapshots.csv")

        if st.button("加载涨停候选复盘统计"):
            try:
                data = request_api(
                    api_base,
                    "/api/limit-up/reviews/stats",
                    method="GET",
                    params={
                        "trade_date": str(limit_trade_date),
                        "code": review_code.strip(),
                        "horizon": review_horizon,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            df = pd.DataFrame(data.get("items", []))
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if df.empty:
                st.warning("当前条件下没有涨停候选复盘统计。")
            else:
                cols = [
                    "score_bucket",
                    "data_source",
                    "sample_count",
                    "avg_return",
                    "win_rate",
                    "avg_max_drawdown",
                    "avg_sector_limit_up_count",
                    "strategy_verdict",
                    "strategy_confidence",
                    "strategy_actionable",
                    "samples_to_actionable",
                    "strategy_next_action",
                    "strategy_note",
                    "horizon",
                ]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "limit_up_review_stats.csv")

    with sector_tab:
        render_section_header("板块轮动", "扫描行业或概念板块，优先找近期活跃、但仍处于相对低位的方向。")
        c1, c2, c3, c4 = st.columns(4)
        sector_trade_date = c1.date_input("交易日", value=pd.Timestamp.now().date(), key="sector_trade_date")
        sector_type = c2.selectbox("板块类型", options=["industry", "concept"], format_func=lambda x: "行业" if x == "industry" else "概念")
        sector_top_n = int(c3.number_input("扫描前 N 个活跃板块", min_value=1, max_value=200, value=30, step=5, key="sector_top_n"))
        sector_max_items = int(c4.number_input("最多保存", min_value=1, max_value=100, value=20, step=5, key="sector_max_items"))

        if st.button("扫描并保存板块轮动", type="primary"):
            try:
                data = request_api(
                    api_base,
                    "/api/sectors/rotation",
                    payload={
                        "trade_date": str(sector_trade_date),
                        "sector_type": sector_type,
                        "top_n": sector_top_n,
                        "max_items": sector_max_items,
                    },
                    timeout_seconds=900,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"扫描失败: {exc}")
                st.stop()

            render_result_meta({"时间": data.get("as_of", ""), "交易日": data.get("trade_date", ""), "数量": data.get("count", 0)})
            show_api_errors(data.get("errors", []), primary_key="板块")
            df = pd.DataFrame(data.get("items", []))
            if df.empty:
                st.warning("当前条件下没有生成板块轮动快照。")
            else:
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
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "sector_rotation.csv")

        if st.button("加载已保存板块轮动"):
            try:
                data = request_api(
                    api_base,
                    "/api/sectors/rotation",
                    method="GET",
                    params={"trade_date": str(sector_trade_date), "sector_type": sector_type, "limit": sector_max_items},
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            df = pd.DataFrame(data.get("items", []))
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if df.empty:
                st.warning("没有已保存的板块轮动快照。")
            else:
                cols = ["trade_date", "sector_name", "latest_pct_change", "return_5d", "position_60d", "rotation_score", "signal", "created_at"]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "sector_rotation_history.csv")

        st.divider()
        st.caption("查看已保存板块的轮动强弱变化，用来判断热点是持续走强、轮动扩散，还是只有单日异动。")
        c1, c2 = st.columns(2)
        trend_days = int(c1.number_input("趋势天数", min_value=5, max_value=250, value=60, step=5, key="sector_trend_days"))
        trend_names = c2.text_input("指定板块（逗号分隔，可选）", value="", key="sector_trend_names")

        if st.button("加载板块轮动趋势"):
            end_date = pd.Timestamp(sector_trade_date)
            start_date = end_date - pd.Timedelta(days=trend_days)
            try:
                data = request_api(
                    api_base,
                    "/api/sectors/rotation/trends",
                    method="GET",
                    params={
                        "sector_type": sector_type,
                        "sector_names": trend_names.strip(),
                        "start_date": str(start_date.date()),
                        "end_date": str(end_date.date()),
                        "limit": 5000,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            df = pd.DataFrame(data.get("items", []))
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if df.empty:
                st.warning("没有可展示的板块轮动趋势。请先扫描并保存几个交易日的板块快照。")
            else:
                chart_df = (
                    df.assign(trade_date=pd.to_datetime(df["trade_date"]))
                    .pivot_table(
                        index="trade_date",
                        columns="sector_name",
                        values="rotation_score",
                        aggfunc="last",
                    )
                    .sort_index()
                )
                st.line_chart(chart_df, width="stretch")
                cols = ["trade_date", "sector_name", "rotation_score", "activity_score", "position_60d", "signal", "created_at"]
                show_downloadable_table(df[[c for c in cols if c in df.columns]], "sector_rotation_trends.csv")

    with alerts_tab:
        render_section_header("每日任务", "正式扫描默认股票池，写入事件库，并按通知渠道去重，适合收盘后固定执行。")
        event_date = st.date_input("交易日", value=pd.Timestamp.now().date(), key="event_date")
        c1, c2, c3, c4 = st.columns(4)
        notification_channel = c1.selectbox(
            "通知渠道",
            options=["stdout", "feishu_webhook"],
            index=0 if DEFAULT_NOTIFICATION_CHANNEL == "stdout" else 1,
            key="job_notification_channel",
        )
        job_workers = int(c3.number_input("任务并发数", min_value=1, max_value=32, value=8, step=1, key="job_workers"))
        job_min_score = float(c4.number_input("最低评分", min_value=0.0, max_value=100.0, value=60.0, step=5.0, key="job_min_score"))
        review_after_scan = st.checkbox("扫描后复盘已到期信号", value=False, key="job_review_after_scan")
        review_trade_date = ""
        review_horizons = "1,3,5"
        review_summary_horizon = "T+3"
        review_due_only = True
        if review_after_scan:
            r1, r2, r3 = st.columns(3)
            review_trade_date = r1.text_input("复盘交易日过滤（可选）", value="", key="job_review_trade_date")
            review_horizons = r2.text_input("复盘周期", value="1,3,5", key="job_review_horizons")
            review_summary_horizon = r3.selectbox("复盘统计周期", options=["T+1", "T+3", "T+5"], index=1, key="job_review_summary_horizon")
            review_due_only = st.checkbox("只回填已到期样本", value=True, key="job_review_due_only")
        if notification_channel == "feishu_webhook":
            st.info("会向飞书机器人地址推送新事件。需要先在运行环境里配置 webhook。")

        if c2.button("执行每日任务", type="primary"):
            payload = {
                "channel": notification_channel,
                "max_workers": job_workers,
                "min_score": job_min_score,
                "review_after_scan": review_after_scan,
                "review_trade_date": review_trade_date.strip(),
                "review_horizons": [int(item.strip()) for item in review_horizons.split(",") if item.strip()],
                "review_summary_horizon": review_summary_horizon,
                "review_due_only": review_due_only,
            }
            try:
                data = request_api(
                    api_base,
                    "/api/signals/run-daily-job",
                    payload=payload,
                    timeout_seconds=900,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"扫描失败: {exc}")
                st.stop()

            render_result_meta(
                {
                    "时间": data.get("as_of", ""),
                    "来源": data.get("source", "unknown"),
                    "股票池": data.get("watchlist", {}).get("name", ""),
                    "最低评分": data.get("min_score", ""),
                    "数量": data.get("count", 0),
                }
            )
            if data.get("scan_run"):
                st.caption(
                    f"运行记录 ID={data['scan_run'].get('id', '')} | "
                    f"记录时间={data['scan_run'].get('run_at', '')} | "
                    f"状态={data['scan_run'].get('status', '')} | "
                    f"{data['scan_run'].get('note', '')}"
                )
                if data["scan_run"].get("review_after_scan"):
                    st.caption(
                        f"运行记录复盘：快照 {data['scan_run'].get('review_snapshot_count', 0)} 条 | "
                        f"统计 {data['scan_run'].get('review_stats_count', 0)} 组 | "
                        f"错误 {data['scan_run'].get('review_error', '') or '无'}"
                    )
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("扫描股票数", data.get("requested_count", 0))
            m2.metric("新增事件数", data.get("count", 0))
            m3.metric("错误数", data.get("error_count", len(data.get("errors", []))))
            m4.metric("耗时(秒)", data.get("elapsed_seconds", ""))
            st.caption(f"本次代表通知数={data.get('notification_count', len(data.get('deliveries', [])))}")
            strategy_guard = data.get("strategy_guard") if isinstance(data.get("strategy_guard"), dict) else {}
            if strategy_guard:
                st.caption(
                    f"策略结论匹配：周期={strategy_guard.get('horizon', '')} | "
                    f"已匹配={strategy_guard.get('matched_count', 0)}/{strategy_guard.get('total_count', 0)} | "
                    f"降权静音={strategy_guard.get('mute_downgraded', False)} | "
                    f"静音={strategy_guard.get('muted_count', 0)}"
                )
            if data.get("review_after_scan"):
                review_result = data.get("review_result") or {}
                review_stats = data.get("review_stats") or []
                if data.get("review_error"):
                    st.warning(f"复盘失败: {data.get('review_error')}")
                else:
                    st.caption(f"复盘快照={review_result.get('count', 0)} | 复盘统计={len(review_stats)}")
                    if review_stats:
                        show_downloadable_table(pd.DataFrame(review_stats), "daily_job_review_stats.csv")
            summary = data.get("signal_summary", {}) if isinstance(data.get("signal_summary"), dict) else {}
            if summary:
                st.caption(
                    f"可观察={summary.get('actionable_signals', 0)} | "
                    f"建议跳过={summary.get('no_action_signals', 0)} | "
                    f"观察结论={summary.get('observation_counts', {})} | "
                    f"观察仓位={summary.get('position_size_counts', {})} | "
                    f"数据时效={summary.get('freshness_counts', {})} | "
                    f"最高评分={summary.get('max_score', '-')}"
                )
            for message in data.get("messages", []):
                st.info(message)
            show_api_errors(data.get("errors", []))
            if data.get("errors") and not data.get("items") and data.get("error_count", 0) == data.get("requested_count", 0):
                st.error("当前行情源连接不稳定，本次默认股票池扫描未取回有效日线数据。建议稍后重试。")
            if not data.get("items"):
                st.warning("这次扫描没有产生新的入库事件。")
            else:
                show_event_table(data["items"], "today_signal_events.csv")

        if st.button("加载最近扫描记录"):
            try:
                data = request_api(api_base, "/api/signals/scan-runs", method="GET", params={"limit": 20})
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()
            runs = pd.DataFrame(data.get("items", []))
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if runs.empty:
                st.warning("还没有扫描运行记录。")
            else:
                if "summary" in runs.columns:
                    runs["策略匹配"] = runs["summary"].map(
                        lambda item: (item or {}).get("strategy_guard", {}).get("matched_count", "")
                        if isinstance(item, dict)
                        else ""
                    )
                    runs["降权静音数"] = runs["summary"].map(
                        lambda item: (item or {}).get("strategy_guard", {}).get("muted_count", "")
                        if isinstance(item, dict)
                        else ""
                    )
                show_downloadable_table(runs, "scan_runs.csv")

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

            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            show_event_table(data.get("items", []), "today_signal_events.csv")

    with history_tab:
        render_section_header("历史事件", "回看已经入库的日线信号事件，用于追踪同一股票或最近扫描结果。")
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

            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            show_event_table(data.get("items", []), "signal_event_history.csv")

    with review_tab:
        render_section_header("复盘验证", "先回填事件后的未来表现，再按周期汇总，用来判断策略是否值得继续保留。")
        c1, c2, c3 = st.columns(3)
        review_code = c1.text_input("股票代码过滤（可选）", value="", key="review_code")
        review_date = c2.text_input("交易日过滤（可选）", value="", key="review_date")
        horizon = c3.selectbox("统计 horizon", options=["T+1", "T+3", "T+5"], index=1)
        due_only = st.checkbox("只回填已到期信号", value=True, key="review_due_only")

        if st.button("回填复盘快照", type="primary"):
            payload = {
                "code": review_code.strip(),
                "trade_date": review_date.strip(),
                "horizons": [1, 3, 5],
                "adjust": "qfq",
                "due_only": due_only,
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

            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            show_api_errors(data.get("errors", []))
            snapshots = pd.DataFrame(data.get("items", []))
            if snapshots.empty:
                st.warning("没有生成新的复盘快照。")
            else:
                preferred_cols = [
                    "trade_date",
                    "code",
                    "summary",
                    "signal_score",
                    "signal_direction",
                    "signal_level",
                    "observation_conclusion",
                    "data_freshness",
                    "data_lag_days",
                    "data_source",
                    "cache_fetched_at",
                    "risk_note",
                    "position_60d",
                    "volume_ratio",
                    "stop_loss_price",
                    "target_price",
                    "risk_reward_ratio",
                    "stop_distance_pct",
                    "stop_hit",
                    "target_hit",
                    "risk_plan_outcome",
                    "risk_plan_hit_date",
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
            render_result_meta({"时间": data.get("as_of", ""), "数量": data.get("count", 0)})
            if stats_df.empty:
                st.warning("当前条件下没有复盘统计数据。")
            else:
                preferred_cols = [
                    "score_bucket",
                    "signal_direction",
                    "observation_conclusion",
                    "data_freshness",
                    "data_source",
                    "risk_bucket",
                    "risk_plan_bucket",
                    "summary",
                    "indicator",
                    "event_type",
                    "sample_count",
                    "avg_return",
                    "win_rate",
                    "avg_max_drawdown",
                    "avg_position_60d",
                    "avg_volume_ratio",
                    "avg_stop_distance_pct",
                    "avg_risk_reward_ratio",
                    "stop_hit_rate",
                    "target_hit_rate",
                    "stop_first_rate",
                    "target_first_rate",
                    "same_day_hit_rate",
                    "strategy_verdict",
                    "strategy_confidence",
                    "strategy_actionable",
                    "samples_to_actionable",
                    "strategy_next_action",
                    "strategy_note",
                    "horizon",
                ]
                cols = [c for c in preferred_cols if c in stats_df.columns]
                if cols:
                    stats_df = stats_df[cols]
                show_downloadable_table(stats_df, "review_stats.csv")

    with strategy_tab:
        render_section_header("策略结论", "汇总日线信号和涨停策略的复盘结果，优先展示可执行的保留、降权和观察结论。")
        c1, c2, c3, c4 = st.columns(4)
        strategy_horizon = c1.selectbox("统计周期", options=["T+1", "T+3", "T+5"], index=1, key="strategy_horizon")
        strategy_date = c2.text_input("交易日过滤（可选）", value="", key="strategy_date")
        strategy_code = c3.text_input("股票代码过滤（可选）", value="", key="strategy_code")
        strategy_limit = int(c4.number_input("最多展示", min_value=1, max_value=200, value=50, step=10, key="strategy_limit"))
        c1, c2, c3 = st.columns(3)
        strategy_min_samples = int(c1.number_input("最低样本数", min_value=1, max_value=10000, value=1, step=1, key="strategy_min_samples"))
        strategy_data_source = c2.text_input("数据来源过滤（可选）", value="", key="strategy_data_source")
        strategy_actionable_only = c3.checkbox("只看可行动结论", value=False, key="strategy_actionable_only")

        if st.button("加载策略结论", type="primary"):
            params = {
                "horizon": strategy_horizon,
                "limit": strategy_limit,
                "min_samples": strategy_min_samples,
                "actionable_only": strategy_actionable_only,
            }
            if strategy_data_source.strip():
                params["data_source"] = strategy_data_source.strip()
            if strategy_date.strip():
                params["trade_date"] = strategy_date.strip()
            if strategy_code.strip():
                params["code"] = strategy_code.strip()
            try:
                data = request_api(
                    api_base,
                    "/api/strategy/summary",
                    method="GET",
                    params=params,
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"加载失败: {exc}")
                st.stop()

            render_result_meta(
                {
                    "时间": data.get("as_of", ""),
                    "总分组": data.get("total_count", 0),
                    "过滤后": data.get("filtered_count", 0),
                    "可执行": data.get("actionable_count", 0),
                    "过滤后可执行": data.get("filtered_actionable_count", 0),
                    "展示": data.get("count", 0),
                }
            )
            verdict_counts = data.get("verdict_counts") or {}
            confidence_counts = data.get("confidence_counts") or {}
            type_counts = data.get("strategy_type_counts") or {}
            source_counts = data.get("data_source_counts") or {}
            action_counts = data.get("next_action_counts") or {}
            if verdict_counts or confidence_counts or type_counts or source_counts or action_counts:
                st.caption(
                    f"结论分布={verdict_counts} | 可信度={confidence_counts} | "
                    f"策略类型={type_counts} | 数据来源={source_counts} | 下一步动作={action_counts}"
                )
            sample_gap_summary = data.get("sample_gap_summary") or {}
            if sample_gap_summary:
                st.caption(
                    f"待积累分组={sample_gap_summary.get('needs_more_samples_count', 0)} | "
                    f"合计还差样本={sample_gap_summary.get('total_samples_to_actionable', 0)}"
                )
                gap_df = pd.DataFrame(sample_gap_summary.get("nearest_to_actionable", []))
                if not gap_df.empty:
                    gap_cols = [
                        "strategy_type",
                        "strategy_name",
                        "sample_count",
                        "samples_to_actionable",
                        "data_source",
                        "strategy_next_action",
                    ]
                    st.dataframe(gap_df[[c for c in gap_cols if c in gap_df.columns]], use_container_width=True)
            review_backlog = data.get("review_backlog") or {}
            if review_backlog:
                st.caption(
                    f"复盘缺口：总样本={review_backlog.get('total_count', 0)} | "
                    f"已复盘={review_backlog.get('reviewed_count', 0)} | "
                    f"未复盘={review_backlog.get('missing_count', 0)} | "
                    f"已到期未复盘={review_backlog.get('due_missing_count', 0)} | "
                    f"未到期={review_backlog.get('not_due_count', 0)} | "
                    f"下次复盘日期={review_backlog.get('next_due_date', '') or '-'} | "
                    f"现在需要复盘={review_backlog.get('review_now', False)} | "
                    f"覆盖率={review_backlog.get('reviewed_ratio')}"
                )
                due_review_df = pd.DataFrame(review_backlog.get("due_missing_items", []))
                if not due_review_df.empty:
                    due_cols = [
                        "strategy_type",
                        "trade_date",
                        "review_due_date",
                        "code",
                        "name",
                        "summary",
                        "score",
                    ]
                    st.dataframe(due_review_df[[c for c in due_cols if c in due_review_df.columns]], use_container_width=True)
            strategy_df = pd.DataFrame(data.get("items", []))
            if strategy_df.empty:
                st.warning("当前还没有可展示的策略结论。请先回填复盘结果。")
            else:
                cols = [
                    "strategy_type",
                    "strategy_name",
                    "strategy_verdict",
                    "strategy_confidence",
                    "strategy_actionable",
                    "samples_to_actionable",
                    "strategy_next_action",
                    "sample_count",
                    "avg_return",
                    "win_rate",
                    "avg_max_drawdown",
                    "data_source",
                    "strategy_note",
                    "horizon",
                ]
                show_downloadable_table(strategy_df[[c for c in cols if c in strategy_df.columns]], "strategy_summary.csv")

    with watchlist_tab:
        render_section_header("股票池", "维护默认股票池；每日任务和默认池实时行情都会使用这里保存的列表。")
        try:
            watchlist = request_api(api_base, "/api/watchlists/default", method="GET")
            current_codes_text = "\n".join(str(item.get("code", "")) for item in watchlist.get("items", []))
        except Exception as exc:  # noqa: BLE001
            watchlist = None
            current_codes_text = ""
            st.error(f"读取股票池失败: {exc}")

        if "watchlist_codes_text" not in st.session_state:
            st.session_state["watchlist_codes_text"] = current_codes_text
        if "watchlist_codes_text_pending" in st.session_state:
            st.session_state["watchlist_codes_text"] = st.session_state.pop("watchlist_codes_text_pending")
        if "watchlist_pending_message" in st.session_state:
            pending_message = st.session_state.pop("watchlist_pending_message")
            if pending_message.get("tone") == "warning":
                st.warning(pending_message.get("text", ""))
            else:
                st.success(pending_message.get("text", ""))

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

        c1, c2, c3, c4 = st.columns(4)
        if c1.button("从服务端刷新股票池"):
            st.session_state["watchlist_codes_text_pending"] = current_codes_text
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

            st.session_state["watchlist_codes_text_pending"] = "\n".join(str(item.get("code", "")) for item in data.get("items", []))
            st.session_state["watchlist_pending_message"] = {
                "tone": "success",
                "text": f"已导入沪深300成分股，共 {data.get('count', 0)} 只股票。",
            }
            st.rerun()

        if c4.button("初始化股票池"):
            try:
                data = request_api(
                    api_base,
                    "/api/watchlists/default/bootstrap",
                    payload={"index_code": "000300"},
                )
            except Exception as exc:  # noqa: BLE001
                st.error(f"初始化失败: {exc}")
                st.stop()

            st.session_state["watchlist_codes_text_pending"] = "\n".join(str(item.get("code", "")) for item in data.get("items", []))
            if data.get("warning"):
                st.session_state["watchlist_pending_message"] = {
                    "tone": "warning",
                    "text": f"{data.get('message', '已初始化默认股票池')}：{data.get('warning')}",
                }
            else:
                st.session_state["watchlist_pending_message"] = {
                    "tone": "success",
                    "text": f"已初始化默认股票池，共 {data.get('count', 0)} 只股票。",
                }
            st.rerun()

    with kline_tab:
        render_section_header("K线工具", "直接查询 THSDK K 线，用于核对底层行情连接和单只股票历史数据。")
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
            render_result_meta({"时间": data.get("as_of", ""), "来源": data.get("source", "unknown"), "数量": len(items)})
            if not items:
                st.warning("没有命中数据。")
                st.stop()

            df = pd.DataFrame(items)
            show_downloadable_table(df, "thsdk_klines.csv")


if __name__ == "__main__":
    main()
