from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from app import thsdk_service


class FakeCompletedProcess:
    def __init__(self, returncode: int, stdout: str = "", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_klines_thsdk_success(tmp_path: Path) -> None:
    python_bin = tmp_path / "python"
    script_path = tmp_path / "fetch.py"
    python_bin.write_text("", encoding="utf-8")
    script_path.write_text("", encoding="utf-8")

    def fake_runner(cmd: list[str], capture_output: bool, text: bool, check: bool):
        assert cmd == [
            str(python_bin),
            str(script_path),
            "--symbol",
            "USZA300033",
            "--count",
            "3",
        ]
        assert capture_output is True
        assert text is True
        assert check is False
        return FakeCompletedProcess(
            returncode=0,
            stdout=json.dumps(
                {
                    "ok": True,
                    "data": [
                        {"时间": "2026-03-31 00:00:00", "收盘价": 12.3},
                        {"时间": "2026-04-01 00:00:00", "收盘价": 12.5},
                    ],
                    "extra": {"代码": "USZA300033"},
                },
                ensure_ascii=False,
            ),
        )

    df = thsdk_service.klines_thsdk(
        symbol="USZA300033",
        count=3,
        python_bin=python_bin,
        script_path=script_path,
        runner=fake_runner,
    )

    assert isinstance(df, pd.DataFrame)
    assert list(df["收盘价"]) == [12.3, 12.5]


def test_klines_thsdk_missing_runtime(tmp_path: Path) -> None:
    script_path = tmp_path / "fetch.py"
    script_path.write_text("", encoding="utf-8")

    with pytest.raises(thsdk_service.ThsdkUnavailableError, match="thsdk python"):
        thsdk_service.klines_thsdk(
            symbol="USZA300033",
            count=3,
            python_bin=tmp_path / "missing-python",
            script_path=script_path,
        )


def test_klines_thsdk_runtime_error(tmp_path: Path) -> None:
    python_bin = tmp_path / "python"
    script_path = tmp_path / "fetch.py"
    python_bin.write_text("", encoding="utf-8")
    script_path.write_text("", encoding="utf-8")

    def fake_runner(cmd: list[str], capture_output: bool, text: bool, check: bool):
        return FakeCompletedProcess(returncode=2, stderr="boom")

    with pytest.raises(RuntimeError, match="boom"):
        thsdk_service.klines_thsdk(
            symbol="USZA300033",
            count=3,
            python_bin=python_bin,
            script_path=script_path,
            runner=fake_runner,
        )
