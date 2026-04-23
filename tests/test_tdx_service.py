from __future__ import annotations

import pandas as pd
import pytest

from app import tdx_service


def test_parse_china_number() -> None:
    assert tdx_service.parse_china_number("1.23亿") == pytest.approx(123_000_000)
    assert tdx_service.parse_china_number("4567万") == pytest.approx(45_670_000)
    assert tdx_service.parse_china_number("-1.5万") == pytest.approx(-15_000)


def test_flow_rank_tdx_sorts_and_filters() -> None:
    def fake_fetcher(codes: list[str], fields: list[str]) -> pd.DataFrame:
        assert codes == ["600592", "600487", "601105"]
        assert "Zjl_HB" in fields
        return pd.DataFrame(
            [
                {"symbol": "600592.SH", "HqDate": "2026-03-31", "Zjl_HB": "19.60亿"},
                {"symbol": "600487.SH", "HqDate": "2026-03-31", "Zjl_HB": "10.12亿"},
                {"symbol": "601105.SH", "HqDate": "2026-03-31", "Zjl_HB": "7.55亿"},
            ]
        )

    df = tdx_service.flow_rank_tdx(
        codes=["600592", "600487", "601105"],
        min_net_inflow=800_000_000,
        limit=2,
        fetcher=fake_fetcher,
    )

    assert list(df["股票代码"]) == ["600592", "600487"]
    assert list(df["主力净流入(亿)"]) == [19.6, 10.12]


def test_flow_rank_tdx_requires_codes() -> None:
    with pytest.raises(ValueError, match="至少提供一个股票代码"):
        tdx_service.flow_rank_tdx(codes=[], fetcher=lambda *_: pd.DataFrame())


def test_flow_rank_akshare_for_codes(monkeypatch) -> None:
    sample = pd.DataFrame(
        [
            {"股票代码": "600592", "净额": "19.60亿"},
            {"股票代码": "600487", "净额": "10.12亿"},
            {"股票代码": "001111", "净额": "0.10亿"},
        ]
    )

    monkeypatch.setattr(tdx_service, "fetch_akshare_flow_snapshot", lambda: sample.copy())

    df = tdx_service.flow_rank_akshare_for_codes(
        codes=["600592", "600487"],
        min_net_inflow=1_100_000_000,
        limit=10,
    )
    assert list(df["股票代码"]) == ["600592"]
    assert list(df["主力净流入(亿)"]) == [19.6]
