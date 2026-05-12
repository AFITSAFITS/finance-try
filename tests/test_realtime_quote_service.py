from __future__ import annotations

import time

from app import realtime_quote_service


class _JsonResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self._payload


class _TextResponse:
    def __init__(self, text: str) -> None:
        self.text = text

    def raise_for_status(self) -> None:
        return None


def test_fetch_realtime_quotes_eastmoney_parses_rows() -> None:
    def fake_requester(url: str, params: dict[str, str], timeout: float, headers: dict[str, str]):
        assert params["secids"] == "1.600519,0.000001"
        return _JsonResponse(
            {
                "data": {
                    "diff": [
                        {
                            "f12": "600519",
                            "f14": "贵州茅台",
                            "f2": 1354.55,
                            "f3": -0.5,
                            "f4": -6.78,
                            "f5": 50837,
                            "f6": 6886304982.0,
                            "f15": 1363.58,
                            "f16": 1350.5,
                            "f17": 1362.0,
                            "f18": 1361.33,
                            "f8": 0.41,
                            "f10": 1.1,
                        },
                        {
                            "f12": "000001",
                            "f14": "平安银行",
                            "f2": 11.25,
                            "f3": -0.27,
                            "f4": -0.03,
                            "f5": 1029038,
                            "f6": 1158241865.91,
                            "f15": 11.36,
                            "f16": 11.21,
                            "f17": 11.28,
                            "f18": 11.28,
                            "f8": 0.53,
                            "f10": 1.02,
                        },
                    ]
                }
            }
        )

    items = realtime_quote_service.fetch_realtime_quotes_eastmoney(
        ["600519", "000001"],
        requester=fake_requester,
    )

    assert [item["code"] for item in items] == ["600519", "000001"]
    assert items[0]["name"] == "贵州茅台"
    assert items[0]["latest_price"] == 1354.55
    assert items[0]["pct_change"] == -0.5
    assert items[0]["quality_status"] == "正常"
    assert items[0]["source"] == "eastmoney"


def test_fetch_realtime_quotes_tencent_parses_rows() -> None:
    text = (
        'v_sh600519="1~贵州茅台~600519~1354.55~1361.33~1362.00~50837~0~0~'
        '0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~0~~20260512161416~'
        '-6.78~-0.50~1363.58~1350.50~1354.55/50837/6886304982~50837~688630~'
        '0.41~20.51~~1363.58~1350.50~0.96~16962.63~16962.63~6.26~1497.46~'
        '1225.20~1.10~35";'
    )

    def fake_requester(url: str, timeout: float, headers: dict[str, str]):
        assert "sh600519" in url
        return _TextResponse(text)

    items = realtime_quote_service.fetch_realtime_quotes_tencent(
        ["600519"],
        requester=fake_requester,
    )

    assert len(items) == 1
    assert items[0]["code"] == "600519"
    assert items[0]["name"] == "贵州茅台"
    assert items[0]["latest_price"] == 1354.55
    assert items[0]["pct_change"] == -0.5
    assert items[0]["quality_status"] == "正常"
    assert items[0]["source"] == "tencent"


def test_quote_quality_marks_suspicious_rows() -> None:
    item = realtime_quote_service._enrich_quote_quality(
        {
            "latest_price": 0,
            "prev_close": 10,
            "pct_change": 50,
            "volume": 0,
            "amount": 0,
        }
    )

    assert item["quality_status"] == "需确认"
    assert "当前价缺失" in item["quality_note"]
    assert "涨跌幅异常" in item["quality_note"]
    assert "成交量为0" in item["quality_note"]


def test_fetch_realtime_quotes_best_effort_falls_back_to_tencent(monkeypatch) -> None:
    monkeypatch.setattr(
        realtime_quote_service,
        "fetch_realtime_quotes_eastmoney",
        lambda codes: (_ for _ in ()).throw(RuntimeError("eastmoney down")),
    )
    monkeypatch.setattr(
        realtime_quote_service,
        "fetch_realtime_quotes_tencent",
        lambda codes: [{"code": "600519", "name": "贵州茅台", "latest_price": 1354.55, "source": "tencent"}],
    )

    items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(["600519"])

    assert source == "tencent"
    assert errors == []
    assert items[0]["latest_price"] == 1354.55


def test_fetch_realtime_quotes_best_effort_reports_missing_codes(monkeypatch) -> None:
    monkeypatch.setattr(
        realtime_quote_service,
        "fetch_realtime_quotes_eastmoney",
        lambda codes: [{"code": "600519", "name": "贵州茅台", "latest_price": 1354.55, "source": "eastmoney"}],
    )
    monkeypatch.setattr(realtime_quote_service, "fetch_realtime_quotes_tencent", lambda codes: [])

    items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(["600519", "000001"])

    assert source == "eastmoney"
    assert len(items) == 1
    assert errors == [{"股票代码": "000001", "error": "未返回实时行情"}]


def test_fetch_realtime_quotes_best_effort_fills_missing_codes_from_tencent(monkeypatch) -> None:
    def fake_eastmoney(codes):
        assert codes == ["600519", "000001"]
        return [{"code": "600519", "name": "贵州茅台", "latest_price": 1354.55, "source": "eastmoney"}]

    def fake_tencent(codes):
        assert codes == ["000001"]
        return [{"code": "000001", "name": "平安银行", "latest_price": 11.25, "source": "tencent"}]

    monkeypatch.setattr(realtime_quote_service, "fetch_realtime_quotes_eastmoney", fake_eastmoney)
    monkeypatch.setattr(realtime_quote_service, "fetch_realtime_quotes_tencent", fake_tencent)

    items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(["600519", "000001"])

    assert source == "eastmoney+tencent"
    assert errors == []
    assert [item["code"] for item in items] == ["600519", "000001"]


def test_fetch_realtime_quotes_best_effort_skips_timed_out_provider(monkeypatch) -> None:
    def slow_eastmoney(codes):
        time.sleep(0.3)
        return [{"code": "600519", "latest_price": 1}]

    monkeypatch.setattr(realtime_quote_service, "fetch_realtime_quotes_eastmoney", slow_eastmoney)
    monkeypatch.setattr(
        realtime_quote_service,
        "fetch_realtime_quotes_tencent",
        lambda codes: [{"code": "600519", "name": "贵州茅台", "latest_price": 1354.55, "source": "tencent"}],
    )

    started_at = time.perf_counter()
    items, errors, source = realtime_quote_service.fetch_realtime_quotes_best_effort(
        ["600519"],
        provider_timeout=0.1,
    )

    assert time.perf_counter() - started_at < 0.25
    assert source == "tencent"
    assert errors == []
    assert items[0]["source"] == "tencent"
