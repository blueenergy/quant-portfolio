"""Strategy template / preset loading and lineage param hashing."""

from __future__ import annotations

from typing import Any, Dict

from pymongo import ASCENDING

from portfolio.db import TradingCollections
from shared.strategies import (
    PLAN_PARAM_KEYS,
    PLAN_SNAPSHOT_PARAM_KEYS,
    build_user_plan_id,
    effective_strategy_config,
    normalized_lineage_params,
    normalized_plan_params,
    plan_params_hash,
    selected_preset_id_for_plan,
    strategy_display_name,
)

__all__ = [
    "PLAN_PARAM_KEYS",
    "PLAN_SNAPSHOT_PARAM_KEYS",
    "build_user_plan_id",
    "effective_strategy_config",
    "load_default_param_preset",
    "load_strategy_config",
    "normalized_lineage_params",
    "normalized_plan_params",
    "plan_params_hash",
    "selected_preset_id_for_plan",
    "strategy_display_name",
]


def load_strategy_config(collections: TradingCollections, strategy_template_id: str) -> Dict[str, Any]:
    query: Dict[str, Any] = {"strategy_template_id": strategy_template_id, "status": {"$ne": "disabled"}}
    config = collections.strategy_configs.find_one(query, {"_id": 0})
    if not config:
        raise ValueError(f"Strategy template not found or disabled: {strategy_template_id}")
    return config


def load_default_param_preset(collections: TradingCollections, strategy_template_id: str) -> Dict[str, Any]:
    query: Dict[str, Any] = {
        "strategy_template_id": strategy_template_id,
        "user_id": None,
        "status": {"$in": ["recommended", "enabled"]},
    }
    default_preset = collections.param_presets.find_one(
        {**query, "is_default": True}, {"_id": 0}, sort=[("priority", ASCENDING), ("created_at", ASCENDING)]
    )
    if default_preset:
        return default_preset
    return collections.param_presets.find_one(query, {"_id": 0}, sort=[("priority", ASCENDING), ("created_at", ASCENDING)]) or {}
