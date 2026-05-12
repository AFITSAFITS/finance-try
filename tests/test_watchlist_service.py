from __future__ import annotations

from app import watchlist_service


def test_default_watchlist_roundtrip(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    initial = watchlist_service.get_default_watchlist()
    assert initial["name"] == "默认股票池"
    assert initial["count"] == 0
    assert initial["items"] == []

    updated = watchlist_service.replace_default_watchlist_items(["600519", "000001", "600519"])
    assert updated["count"] == 2
    assert [item["code"] for item in updated["items"]] == ["600519", "000001"]

    loaded = watchlist_service.get_default_watchlist()
    assert loaded["count"] == 2
    assert [item["code"] for item in loaded["items"]] == ["600519", "000001"]


def test_import_default_watchlist_from_index(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    imported = watchlist_service.import_default_watchlist_from_index(
        index_code="000300",
        constituent_fetcher=lambda index_code: ["600519", "000001", "600519"],
    )

    assert imported["index_code"] == "000300"
    assert imported["count"] == 2
    assert [item["code"] for item in imported["items"]] == ["600519", "000001"]


def test_bootstrap_default_watchlist_falls_back_to_seed(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))

    def broken_fetcher(index_code: str) -> list[str]:
        raise RuntimeError("remote disconnected")

    bootstrapped = watchlist_service.bootstrap_default_watchlist(
        index_code="000300",
        constituent_fetcher=broken_fetcher,
    )

    assert bootstrapped["source"] == "seed"
    assert bootstrapped["count"] == len(watchlist_service.DEFAULT_SEED_CODES)
    assert bootstrapped["items"][0]["code"] == watchlist_service.DEFAULT_SEED_CODES[0]
    assert "remote disconnected" in str(bootstrapped["warning"])


def test_ensure_default_watchlist_keeps_existing_items(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AI_FINANCE_DB_PATH", str(tmp_path / "app.db"))
    watchlist_service.replace_default_watchlist_items(["600519"])

    ensured = watchlist_service.ensure_default_watchlist(
        constituent_fetcher=lambda index_code: ["000001"],
    )

    assert ensured["source"] == "existing"
    assert ensured["count"] == 1
    assert ensured["items"][0]["code"] == "600519"
