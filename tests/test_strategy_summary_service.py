from __future__ import annotations

from app import strategy_summary_service


def test_summarize_strategy_decisions_combines_and_prioritizes(monkeypatch) -> None:
    monkeypatch.setattr(
        strategy_summary_service.review_service,
        "summarize_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "60-80",
                "signal_direction": "偏多",
                "observation_conclusion": "谨慎观察",
                "summary": "MACD金叉",
                "data_source": "本地缓存",
                "sample_count": 6,
                "avg_return": 2.1,
                "win_rate": 0.6,
                "avg_max_drawdown": -3.2,
                "strategy_verdict": "保留",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "strategy_note": "表现可接受",
                "strategy_next_action": "保留该分组",
                "horizon": kwargs["horizon"],
            },
            {
                "score_bucket": "40-60",
                "signal_direction": "偏空",
                "observation_conclusion": "风险回避",
                "summary": "MACD死叉",
                "data_source": "旧缓存兜底",
                "sample_count": 2,
                "avg_return": -1.0,
                "win_rate": 0.0,
                "avg_max_drawdown": -4.0,
                "strategy_verdict": "样本不足",
                "strategy_confidence": "低",
                "strategy_actionable": False,
                "strategy_note": "继续积累",
                "strategy_next_action": "继续积累样本",
                "horizon": kwargs["horizon"],
            },
        ],
    )
    monkeypatch.setattr(
        strategy_summary_service.limit_up_service,
        "summarize_limit_up_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "60-80",
                "data_source": "本地缓存",
                "sample_count": 8,
                "avg_return": -2.4,
                "win_rate": 0.25,
                "avg_max_drawdown": -11.0,
                "strategy_verdict": "降权",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "strategy_note": "回撤偏大",
                "strategy_next_action": "降低权重",
                "horizon": kwargs["horizon"],
            }
        ],
    )

    result = strategy_summary_service.summarize_strategy_decisions(horizon="T+3", limit=10)

    assert result["total_count"] == 3
    assert result["filtered_count"] == 3
    assert result["actionable_count"] == 2
    assert result["filtered_actionable_count"] == 2
    assert result["min_samples"] == 1
    assert result["actionable_only"] is False
    assert result["verdict_counts"] == {"保留": 1, "样本不足": 1, "降权": 1}
    assert result["confidence_counts"] == {"中": 2, "低": 1}
    assert result["strategy_type_counts"] == {"日线信号": 2, "涨停策略": 1}
    assert result["data_source_counts"] == {"本地缓存": 2, "旧缓存兜底": 1}
    assert result["next_action_counts"] == {"保留该分组": 1, "继续积累样本": 1, "降低权重": 1}
    assert [item["strategy_verdict"] for item in result["items"]] == ["保留", "降权", "样本不足"]
    assert result["items"][0]["strategy_type"] == "日线信号"
    assert result["items"][0]["strategy_name"] == "60-80 / 偏多 / 谨慎观察 / MACD金叉"
    assert result["items"][0]["strategy_next_action"] == "保留该分组"
    assert result["items"][0]["samples_to_actionable"] == 0
    assert result["items"][1]["strategy_type"] == "涨停策略"
    assert result["items"][2]["samples_to_actionable"] == 3


def test_summarize_strategy_decisions_applies_limit(monkeypatch) -> None:
    monkeypatch.setattr(
        strategy_summary_service.review_service,
        "summarize_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "60-80",
                "signal_direction": "偏多",
                "observation_conclusion": "正常观察",
                "summary": "信号A",
                "sample_count": 6,
                "strategy_verdict": "保留",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "horizon": kwargs["horizon"],
            }
        ],
    )
    monkeypatch.setattr(
        strategy_summary_service.limit_up_service,
        "summarize_limit_up_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "60-80",
                "data_source": "本地缓存",
                "sample_count": 8,
                "strategy_verdict": "降权",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "horizon": kwargs["horizon"],
            }
        ],
    )

    result = strategy_summary_service.summarize_strategy_decisions(horizon="T+1", limit=1)

    assert result["total_count"] == 2
    assert len(result["items"]) == 1


def test_summarize_strategy_decisions_filters_samples_and_actionable(monkeypatch) -> None:
    monkeypatch.setattr(
        strategy_summary_service.review_service,
        "summarize_review_stats",
        lambda **kwargs: [
            {
                "score_bucket": "60-80",
                "signal_direction": "偏多",
                "observation_conclusion": "正常观察",
                "summary": "信号A",
                "data_source": "本地缓存",
                "sample_count": 6,
                "strategy_verdict": "保留",
                "strategy_confidence": "中",
                "strategy_actionable": True,
                "horizon": kwargs["horizon"],
            },
            {
                "score_bucket": "40-60",
                "signal_direction": "偏空",
                "observation_conclusion": "风险回避",
                "summary": "信号B",
                "data_source": "旧缓存兜底",
                "sample_count": 3,
                "strategy_verdict": "样本不足",
                "strategy_confidence": "低",
                "strategy_actionable": False,
                "horizon": kwargs["horizon"],
            },
        ],
    )
    monkeypatch.setattr(strategy_summary_service.limit_up_service, "summarize_limit_up_review_stats", lambda **kwargs: [])

    result = strategy_summary_service.summarize_strategy_decisions(
        horizon="T+3",
        min_samples=5,
        actionable_only=True,
        data_source="本地缓存",
    )

    assert result["total_count"] == 2
    assert result["filtered_count"] == 1
    assert result["actionable_count"] == 1
    assert result["filtered_actionable_count"] == 1
    assert result["min_samples"] == 5
    assert result["actionable_only"] is True
    assert result["data_source"] == "本地缓存"
    assert result["strategy_type_counts"] == {"日线信号": 1}
    assert result["data_source_counts"] == {"本地缓存": 1}
    assert result["next_action_counts"] == {"未标记": 1}
    assert [item["strategy_name"] for item in result["items"]] == ["60-80 / 偏多 / 正常观察 / 信号A"]
