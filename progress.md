# Progress Log

## 2026-03-31 01:32 CST
- Initialized planning files.
- Reviewed current CLI code and README.
- Next: write tests first, then implement app modules.

## 2026-03-31 01:39 CST
- Added tests (`tests/test_tdx_service.py`, `tests/test_api.py`, `tests/conftest.py`).
- Implemented shared service module: `app/tdx_service.py`.
- Implemented FastAPI app: `app/api.py`.
- Implemented Streamlit page: `app/ui.py`.
- Updated docs: `README.md`.
- Updated dependencies: added `httpx`.
- Verification:
  - `pytest -q` => `7 passed`
  - `python scripts/get_stock_data.py --help` works
  - `uvicorn app.api:app` smoke:
    - `/health` returns OK
    - `/api/tdx/flow-rank` returns expected `tqcenter` unavailable message when local TDX runtime not installed
  - `streamlit run app/ui.py` starts successfully

## 2026-04-01 00:12 CST
- Cloned `easytrader` and `thsdk` under `../third_party/`.
- Created repo-local Python 3.11 virtual environments for both third-party projects.
- Verified `easytrader` install and basic object creation (`use("xq")`).
- Verified cloned `thsdk` source tree is incomplete for macOS runtime because bundled dylib is missing.
- Installed published `thsdk` package into the repo-local venv and verified connect + sample `klines` request succeed.
- Next: add project-owned bootstrap and verification scripts so this setup is reproducible from the main repo.

## 2026-04-01 00:36 CST
- Added one-shot bootstrap script: `scripts/setup_third_party.sh`.
- Added verification helpers:
  - `scripts/verify_easytrader.py`
  - `scripts/verify_thsdk.py`
  - `scripts/run_easytrader_check.sh`
  - `scripts/run_thsdk_check.sh`
  - `scripts/run_vendor_checks.sh`
- Updated `README.md` with setup and check commands.
- Verification:
  - `bash scripts/setup_third_party.sh` => success
  - `bash scripts/run_easytrader_check.sh` => success
  - `bash scripts/run_thsdk_check.sh --symbol USZA300033 --count 3` => success
  - `bash scripts/run_vendor_checks.sh --symbol USZA300033 --count 2` => success
  - `source .venv/bin/activate && pytest -q` => `9 passed`

## 2026-04-01 11:17 CST
- Added `app/thsdk_service.py` as a subprocess bridge to the repo-local `thsdk` runtime.
- Added `scripts/fetch_thsdk_klines.py` for JSON-based K-line retrieval through the `thsdk` venv.
- Added `/api/thsdk/klines` to `app/api.py`.
- Extended `app/ui.py` with a `THSDK K线` tab.
- Added tests for `thsdk` service and API route.
- Verification:
  - `source .venv/bin/activate && pytest -q` => `15 passed`
  - `curl -s -X POST http://127.0.0.1:8004/api/thsdk/klines ...` => returned live K-line data
  - `streamlit run app/ui.py` on port `8504` => health check OK

## 2026-04-08 21:20 CST
- Added daily technical signal scanning for `MACD` crosses and `MA5/MA20` crosses.
- Exposed the scanner through CLI, FastAPI, and Streamlit.
- Wrote architecture spec for the next phase under `docs/superpowers/specs/`.
- Defined the next system shape as `watchlists + signal_events + notification_deliveries + review_snapshots`.
- Verification:
  - `.venv/bin/pytest -q` => `20 passed`

## 2026-04-08 22:05 CST
- Implemented Phase 1 event persistence with SQLite storage in `data/app.db` by default.
- Added default watchlist management through API and Streamlit.
- Added `signal_events` persistence with idempotent inserts keyed by trade date, code, indicator, and event type.
- Added API routes:
  - `GET /api/watchlists/default`
  - `POST /api/watchlists/default`
  - `POST /api/signals/scan-default`
  - `GET /api/signals/events`
- Extended Streamlit with:
  - `今日提醒`
  - `历史事件`
  - `股票池`
- Verification:
  - `.venv/bin/pytest -q` => `25 passed`
  - `.venv/bin/python -m py_compile app/*.py scripts/get_stock_data.py` => success

## 2026-04-08 22:35 CST
- Implemented Phase 2 notification delivery tracking with `notification_deliveries`.
- Added shared daily workflow module: default watchlist scan → event persistence → channel delivery dedupe.
- Added runnable entrypoint: `scripts/run_daily_scan.py`.
- Added API route:
  - `POST /api/signals/run-daily-job`
- Updated Streamlit “今日提醒” tab to execute the same daily workflow as the script/API path.
- Verification:
  - `.venv/bin/pytest -q` => `29 passed`
  - `.venv/bin/python -m py_compile app/*.py scripts/get_stock_data.py scripts/run_daily_scan.py` => success

## 2026-04-08 23:05 CST
- Implemented Phase 3 review infrastructure with:
  - `daily_bars`
  - `review_snapshots`
  - review summary aggregation
- Added API routes:
  - `POST /api/reviews/backfill`
  - `GET /api/reviews/stats`
  - `GET /api/reviews/snapshots`
- Added CLI helper:
  - `scripts/review_signal_outcomes.py`
- Extended Streamlit with a `复盘统计` tab for snapshot backfill and horizon-based summary views.
- Verification:
  - `.venv/bin/pytest -q` => `32 passed`
  - `.venv/bin/python -m py_compile app/*.py scripts/get_stock_data.py scripts/run_daily_scan.py scripts/review_signal_outcomes.py` => success
  - `.venv/bin/python scripts/review_signal_outcomes.py --help` => success
