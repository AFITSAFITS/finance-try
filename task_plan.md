# Task Plan

## Goal
Build a self-verifiable TongDaXin-first data toolchain with:
- reusable data service layer
- FastAPI endpoints
- Streamlit page
- automated tests
- clear run/verify docs
- extensible signal event architecture for alerting and review

## Phases
- [x] Phase 1: Inspect existing CLI and dependencies
- [x] Phase 2: Add/adjust tests for service and API behavior
- [x] Phase 3: Implement shared service module (reuse current parser/business logic)
- [x] Phase 4: Implement FastAPI app endpoints
- [x] Phase 5: Implement Streamlit UI page
- [x] Phase 6: Update CLI integration if needed and README docs
- [x] Phase 7: Run local verification (tests + smoke commands)
- [x] Phase 8: Wire `thsdk` and `easytrader` deployment into project scripts
- [x] Phase 9: Add runnable verification helpers for both third-party projects
- [x] Phase 10: Update docs for third-party setup and usage
- [x] Phase 11: Run end-to-end verification for both integrations
- [x] Phase 12: Add `thsdk` service integration to main app
- [x] Phase 13: Expose `thsdk` API endpoint and UI entry
- [x] Phase 14: Verify `thsdk` integration end-to-end
- [x] Phase 15: Add daily technical signal scanning (`MACD` / `MA5-MA20`)
- [x] Phase 16: Introduce watchlists + SQLite-backed signal event storage
- [x] Phase 17: Add notification delivery tracking and daily scan entrypoint
- [x] Phase 18: Add historical event timeline and review statistics

## Decisions
- Prioritize TongDaXin (`tqcenter`) as first provider; keep AkShare fallback via existing script.
- API exposes normalized JSON for UI and external integration.
- UI calls API via HTTP (simple deployment and manual verification).
- Keep third-party dependencies isolated under `third_party/*/.venv` instead of forcing them into the main project environment.
- Treat `thsdk` repo source as incomplete on macOS when bundled dylib is absent; use the published `thsdk` package in the repo-local venv for a working runtime.
- Treat `easytrader` as partially platform-dependent; verify a safe, non-Windows code path (`xq`) locally and document broker client limitations.
- Treat signal scanning as an event system, not a one-off query result, so later notification and review flows can build on the same core records.
- Treat review metrics as trading-day based (`T+1`, `T+3`, `T+5`) rather than calendar-day based to better match market workflows.

## Errors Encountered
| Time | Error | Attempt | Resolution |
|------|-------|---------|------------|
| 2026-03-31 01:35 CST | `pytest` 无法导入 `app` 包 | 1 | 新增 `tests/conftest.py` 注入项目根路径到 `sys.path` |
| 2026-04-01 11:10 CST | `thsdk` 集成测试在收集阶段失败，缺少 `app.thsdk_service` 模块 | 1 | 先写红灯测试，再补 `app/thsdk_service.py`、`scripts/fetch_thsdk_klines.py` 和 API 路由 |
