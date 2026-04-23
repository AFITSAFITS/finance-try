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
