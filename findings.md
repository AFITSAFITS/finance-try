# Findings

## 2026-03-31
- Existing core logic already supports TongDaXin-first ranking via `tdx-flow-rank` in `scripts/get_stock_data.py`.
- `more_info_tdx` returns RuntimeError with clear message if `tqcenter` missing; keep this behavior for API.
- `requirements.txt` already includes FastAPI/Streamlit/pytest.
- No existing app package or tests yet.
- Added `app/tdx_service.py` as reusable TDX service layer for API/UI.
- Added FastAPI endpoints in `app/api.py`:
  - `GET /health`
  - `POST /api/tdx/flow-rank`
  - `POST /api/tdx/more-info`
- Added Streamlit UI in `app/ui.py`, connected to API, with CSV download.
- Added tests for service and API in `tests/`, all passing.

## 2026-04-01
- `easytrader` repo can be installed in a repo-local Python 3.11 venv and verified with the non-Windows `xq` path (`XueQiuTrader`).
- `easytrader` broker client automation paths depend on `pywinauto` and Windows GUI clients; local macOS verification should avoid those code paths.
- `thsdk` repo source imports cleanly with `PYTHONPATH=src`, but fails at runtime on macOS because `src/thsdk/libs/darwin/arm64/hq.dylib` is not present in the cloned repo.
- Installing the published `thsdk` package inside the repo-local venv provides the missing runtime artifacts and works on this machine.
- Verified `thsdk` published package with guest account login and a successful `klines("USZA300033", count=3)` request.
- The reproducible path is now project-owned: bootstrap both repos with `scripts/setup_third_party.sh`, then verify via `scripts/run_easytrader_check.sh` and `scripts/run_thsdk_check.sh`.
- Main app now integrates `thsdk` through a subprocess bridge to `third_party/thsdk/.venv/bin/python`, which avoids adding `thsdk` to the main project environment.
- New FastAPI route `/api/thsdk/klines` returns real K-line rows from `thsdk`.
- Streamlit page now exposes a dedicated `THSDK K线` tab backed by the new API route.

## 2026-04-08
- The current `MACD` / `MA5-MA20` scanner is suitable as a first event producer, but not yet sufficient for long-term alerting because scan results are not persisted.
- Future extensibility depends more on introducing a stable event model and SQLite storage than on adding more indicator-specific columns.
- The most leverage comes from storing standardized signal events with payload snapshots, then layering notifications and review metrics on top.
- Added SQLite-backed storage with `watchlists`, `watchlist_items`, and `signal_events`.
- Standardized persisted events around `trade_date + code + indicator + event_type`, which makes re-scans idempotent.
- Added API routes for default watchlist management and historical event retrieval.
- Streamlit now exposes default watchlist management plus “今日提醒 / 历史事件” views backed by the event store.
- Added `notification_deliveries` for channel-level idempotency, keyed by `(signal_event_id, channel)`.
- Added a shared daily scan workflow so API, CLI script, and future schedulers all reuse the same scan → persist → deliver chain.
- Added `daily_bars` caching and `review_snapshots` so post-signal performance can be measured without mixing it into the event table.
- Review snapshots are computed on trading-day horizons (`T+1`, `T+3`, `T+5`) using future close prices relative to the signal event close.
- Summary stats are now available by `summary + indicator + event_type`, with `sample_count`, `avg_return`, `win_rate`, and `avg_max_drawdown`.
