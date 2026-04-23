from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Callable

import pandas as pd


class ThsdkUnavailableError(RuntimeError):
    """Raised when the thsdk runtime is not available."""


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = PROJECT_ROOT.parent
DEFAULT_THSDK_PYTHON = WORKSPACE_ROOT / "third_party" / "thsdk" / ".venv" / "bin" / "python"
DEFAULT_FETCH_SCRIPT = PROJECT_ROOT / "scripts" / "fetch_thsdk_klines.py"


def get_thsdk_python_path() -> Path:
    override = os.getenv("THSDK_PYTHON")
    return Path(override) if override else DEFAULT_THSDK_PYTHON


def get_thsdk_fetch_script_path() -> Path:
    override = os.getenv("THSDK_FETCH_SCRIPT")
    return Path(override) if override else DEFAULT_FETCH_SCRIPT


def klines_thsdk(
    symbol: str,
    count: int = 100,
    python_bin: Path | None = None,
    script_path: Path | None = None,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> pd.DataFrame:
    runtime_python = python_bin or get_thsdk_python_path()
    fetch_script = script_path or get_thsdk_fetch_script_path()

    if not runtime_python.is_file():
        raise ThsdkUnavailableError(f"thsdk python runtime not found: {runtime_python}")
    if not fetch_script.is_file():
        raise ThsdkUnavailableError(f"thsdk fetch script not found: {fetch_script}")

    completed = runner(
        [
            str(runtime_python),
            str(fetch_script),
            "--symbol",
            symbol,
            "--count",
            str(count),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip() or "thsdk query failed"
        raise RuntimeError(detail)

    payload = json.loads(completed.stdout)
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error") or "thsdk query failed")

    data = payload.get("data", [])
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        return pd.DataFrame([data])
    return pd.DataFrame()
