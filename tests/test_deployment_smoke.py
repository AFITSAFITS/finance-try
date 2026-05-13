from __future__ import annotations

import json
from urllib.request import Request

from scripts import deployment_smoke


class FakeResponse:
    def __init__(self, status: int, body: object) -> None:
        self.status = status
        self.body = body

    def read(self) -> bytes:
        if isinstance(self.body, bytes):
            return self.body
        if isinstance(self.body, str):
            return self.body.encode("utf-8")
        return json.dumps(self.body).encode("utf-8")


def test_run_smoke_checks_api_watchlist_and_ui() -> None:
    def fake_open(request: Request, timeout: float) -> FakeResponse:
        url = request.full_url
        if url.endswith("/health"):
            return FakeResponse(200, {"ok": True, "as_of": "2026-05-12 20:00:00"})
        if url.endswith("/api/watchlists/default"):
            return FakeResponse(200, {"count": 2})
        if "/api/strategy/summary" in url:
            return FakeResponse(200, {"total_count": 1, "filtered_count": 1, "items": []})
        return FakeResponse(200, "<html>streamlit</html>")

    messages = deployment_smoke.run_smoke(
        api_url="http://api:8000",
        ui_url="http://ui:8501",
        require_watchlist=True,
        opener=fake_open,
    )

    assert messages == [
        "api ok: 2026-05-12 20:00:00",
        "watchlist count=2",
        "strategy summary total=1 filtered=1",
        "ui ok",
    ]


def test_run_smoke_can_require_watchlist() -> None:
    def fake_open(request: Request, timeout: float) -> FakeResponse:
        url = request.full_url
        if url.endswith("/health"):
            return FakeResponse(200, {"ok": True})
        if url.endswith("/api/watchlists/default"):
            return FakeResponse(200, {"count": 0})
        if "/api/strategy/summary" in url:
            return FakeResponse(200, {"total_count": 0, "filtered_count": 0, "items": []})
        return FakeResponse(200, "<html>streamlit</html>")

    try:
        deployment_smoke.run_smoke(
            api_url="http://api:8000",
            ui_url="http://ui:8501",
            require_watchlist=True,
            opener=fake_open,
        )
    except RuntimeError as exc:
        assert "默认股票池为空" in str(exc)
    else:
        raise AssertionError("run_smoke should fail when watchlist is required")


def test_run_smoke_can_skip_strategy_check() -> None:
    seen_urls: list[str] = []

    def fake_open(request: Request, timeout: float) -> FakeResponse:
        url = request.full_url
        seen_urls.append(url)
        if url.endswith("/health"):
            return FakeResponse(200, {"ok": True})
        if url.endswith("/api/watchlists/default"):
            return FakeResponse(200, {"count": 1})
        return FakeResponse(200, "<html>streamlit</html>")

    messages = deployment_smoke.run_smoke(
        api_url="http://api:8000",
        ui_url="http://ui:8501",
        check_strategy=False,
        opener=fake_open,
    )

    assert messages == ["api ok: ", "watchlist count=1", "ui ok"]
    assert not any("/api/strategy/summary" in url for url in seen_urls)
