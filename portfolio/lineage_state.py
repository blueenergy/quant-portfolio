"""Materialized portfolio/lineage summary sync for the plan generator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

EXECUTION_VENUE_LIVE = "live"
EXECUTION_VENUE_PAPER = "paper"


def _lineage_has_live_signals(app_db, plan_meta: Dict[str, Any]) -> bool:
    plan_ids = list(
        app_db["portfolio_trade_plans"].find(
            {
                "user_id": plan_meta.get("user_id"),
                "strategy_template_id": plan_meta.get("strategy_template_id"),
                "params_hash": plan_meta.get("params_hash"),
            },
            {"_id": 0, "plan_id": 1},
        )
    )
    ids = [row.get("plan_id") for row in plan_ids if row.get("plan_id")]
    if not ids:
        return False
    return bool(
        app_db["trade_signals"].find_one(
            {"plan_id": {"$in": ids}, "mode": "live"},
            {"_id": 1},
        )
    )


def _param_summary(params: Dict[str, Any]) -> str:
    parts = []
    universe = params.get("universe_index")
    if universe:
        parts.append(str(universe))
    top_n = params.get("top_n")
    if top_n is not None:
        parts.append(f"Top{int(top_n)}")
    rebalance_days = params.get("rebalance_days")
    if rebalance_days is not None:
        parts.append(f"{int(rebalance_days)}d")
    mode = params.get("construction_mode")
    if mode:
        parts.append(str(mode))
    return " · ".join(parts)


def sync_lineage_summary_after_plan(
    lineage_controls_coll,
    app_db,
    plan_doc: Dict[str, Any],
    *,
    all_lineage: Optional[list] = None,
) -> None:
    if lineage_controls_coll is None:
        return
    user_id = str(plan_doc.get("user_id") or "")
    strategy_template_id = str(plan_doc.get("strategy_template_id") or "")
    params_hash = str(plan_doc.get("params_hash") or "")
    if not user_id or not strategy_template_id or not params_hash:
        return
    if all_lineage is None:
        all_lineage = list(
            app_db["portfolio_trade_plans"].find(
                {
                    "user_id": user_id,
                    "strategy_template_id": strategy_template_id,
                    "params_hash": params_hash,
                },
                {
                    "_id": 0,
                    "plan_id": 1,
                    "base_date": 1,
                    "strategy_name": 1,
                    "status": 1,
                    "plan_type": 1,
                    "record_kind": 1,
                    "params_snapshot": 1,
                },
            )
        )
    if not all_lineage:
        return
    latest = max(all_lineage, key=lambda row: str(row.get("base_date") or ""))
    earliest = min(all_lineage, key=lambda row: str(row.get("base_date") or ""))
    params = dict(latest.get("params_snapshot") or {})
    existing = lineage_controls_coll.find_one(
        {
            "user_id": user_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
        },
        {"_id": 0, "execution_venue": 1},
    ) or {}
    execution_venue = existing.get("execution_venue")
    if execution_venue not in (EXECUTION_VENUE_LIVE, EXECUTION_VENUE_PAPER):
        capital_basis = str(
            plan_doc.get("capital_basis")
            or (plan_doc.get("summary") or {}).get("capital_basis")
            or ""
        )
        execution_venue = (
            EXECUTION_VENUE_PAPER if capital_basis == "rolling_paper" else EXECUTION_VENUE_LIVE
        )
    now = datetime.utcnow()
    lineage_controls_coll.update_one(
        {
            "user_id": user_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
        },
        {
            "$set": {
                "user_id": user_id,
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "execution_venue": execution_venue,
                "latest_plan_id": latest.get("plan_id"),
                "latest_plan_status": latest.get("status"),
                "latest_plan_type": latest.get("plan_type"),
                "first_base_date": earliest.get("base_date"),
                "last_base_date": latest.get("base_date"),
                "plan_count": len(all_lineage),
                "strategy_name": latest.get("strategy_name") or earliest.get("strategy_name") or "",
                "param_summary": _param_summary(params),
                "universe_index": params.get("universe_index"),
                "top_n": params.get("top_n"),
                "rebalance_days": params.get("rebalance_days"),
                "construction_mode": params.get("construction_mode"),
                "initial_capital": params.get("initial_capital"),
                "updated_at": now,
            },
            "$setOnInsert": {"paused": False, "excluded_symbols": []},
        },
        upsert=True,
    )
