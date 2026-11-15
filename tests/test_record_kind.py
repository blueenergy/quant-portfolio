"""Mirrored strategy-decision predicates (see quantFinance portfolio_record_kind)."""

import pytest

from portfolio.record_kind import (
    is_strategy_decision,
    strategy_decision_mongo_filter,
    strategy_generated_record_mongo_filter,
    strategy_rebalance_mongo_filter,
)


def test_is_strategy_decision_matches_quantfinance_semantics():
    assert is_strategy_decision(
        {"record_kind": "trade_plan", "origin": "strategy", "plan_type": "rebalance"}
    )
    assert not is_strategy_decision(
        {
            "record_kind": "trade_plan",
            "origin": "manual",
            "plan_type": "rebalance",
            "manual_action": "rebalance",
        }
    )
    assert not is_strategy_decision(
        {"record_kind": "observation", "origin": "strategy", "plan_type": "monitor"}
    )


def test_is_strategy_decision_preserves_ledger_overrides():
    assert not is_strategy_decision(
        {
            "record_kind": "trade_plan",
            "origin": "strategy",
            "manual_action": "external_manual",
        }
    )
    assert not is_strategy_decision(
        {
            "record_kind": "trade_plan",
            "origin": "strategy",
            "plan_id": "lineage_extman_20260903",
        }
    )


def test_missing_record_kind_fails_loud_like_quantfinance():
    with pytest.raises(ValueError, match="missing/invalid record_kind"):
        is_strategy_decision({"origin": "strategy", "plan_type": "rebalance"})
    assert not is_strategy_decision({"origin": "manual", "plan_type": "rebalance"})


def test_mongo_filters_match_quantfinance_shape():
    assert strategy_generated_record_mongo_filter(base_date="20250101") == {
        "origin": {"$ne": "manual"},
        "base_date": "20250101",
    }
    assert strategy_decision_mongo_filter(base_date="20250101") == {
        "origin": {"$ne": "manual"},
        "$or": [
            {"record_kind": "trade_plan"},
            {"record_kind": {"$exists": False}},
        ],
        "base_date": "20250101",
    }
    assert strategy_rebalance_mongo_filter(user_id="u1") == {
        "origin": {"$ne": "manual"},
        "$or": [
            {"record_kind": "trade_plan"},
            {"record_kind": {"$exists": False}},
        ],
        "plan_type": "rebalance",
        "user_id": "u1",
    }
