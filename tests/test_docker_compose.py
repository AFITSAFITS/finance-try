from __future__ import annotations

from pathlib import Path


def test_api_healthcheck_covers_strategy_summary() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert "http://127.0.0.1:8000/health" in compose
    assert "http://127.0.0.1:8000/api/strategy/summary?horizon=T%2B3&limit=1" in compose
    assert "'total_count' in strategy" in compose
    assert "AI_FINANCE_REVIEW_DUE_ONLY" in compose
