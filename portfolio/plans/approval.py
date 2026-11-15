"""Paper plan approval resolution and post-save finalization."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from portfolio.constants import (
    AUTO_PAPER_APPROVE_REVIEWER,
    PAPER_EXECUTION_MODE_AUTO_SHADOW,
    PAPER_EXECUTION_MODE_MANUAL_REVIEW,
    PAPER_EXECUTION_MODES,
)
from portfolio.db import TradingCollections
from portfolio.lineage import _lineage_has_live_signals
from portfolio.lineage_state import sync_lineage_summary_after_plan
from portfolio.reviews import approve_plan

log = logging.getLogger(__name__)


def resolve_paper_execution_mode(
    config: Dict[str, Any],
    app_db,
    cols: TradingCollections,
    plan_meta: Dict[str, Any],
) -> str:
    """Resolve how paper rebalance plans should enter the approval gate.

  默认需人工审批；仅当策略显式配置 ``paper_execution_mode=auto_shadow`` 时才跳过审核。
    """
    explicit = config.get("paper_execution_mode")
    if explicit in PAPER_EXECUTION_MODES:
        return str(explicit)
    return PAPER_EXECUTION_MODE_MANUAL_REVIEW


def maybe_auto_approve_paper_plan(
    cols: TradingCollections,
    app_db,
    plan_doc: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Auto-approve rebalance plans for paper-only ``auto_shadow`` lineages."""
    if plan_doc.get("status") != "needs_review":
        return None
    if not plan_doc.get("is_rebalance_day"):
        return None
    plan_meta = {
        "plan_id": plan_doc.get("plan_id"),
        "strategy_template_id": plan_doc.get("strategy_template_id"),
        "params_hash": plan_doc.get("params_hash"),
        "user_id": plan_doc.get("user_id"),
    }
    if _lineage_has_live_signals(app_db, cols, plan_meta):
        return None
    effective_config = dict(config or plan_doc.get("params_snapshot") or {})
    for key in ("monitor_can_trade", "capital_basis", "paper_execution_mode"):
        if key in plan_doc and key not in effective_config:
            effective_config[key] = plan_doc[key]
    mode = resolve_paper_execution_mode(effective_config, app_db, cols, plan_meta)
    if mode != PAPER_EXECUTION_MODE_AUTO_SHADOW:
        return None
    return approve_plan(
        cols,
        str(plan_doc["plan_id"]),
        reviewer=AUTO_PAPER_APPROVE_REVIEWER,
        comment="auto-approved for paper-only shadow lineage",
    )


def _finalize_saved_plan(
    cols: TradingCollections,
    app_db,
    plan_id: str,
    plan_doc: Dict[str, Any],
    *,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    maybe_auto_approve_paper_plan(cols, app_db, plan_doc, config=config)
    refreshed = cols.trade_plans.find_one({"plan_id": plan_id}, {"_id": 0})
    if refreshed:
        plan_doc = refreshed
    try:
        sync_lineage_summary_after_plan(cols.lineage_controls, app_db, plan_doc)
    except Exception:
        # the plan itself is saved; only the lineage rollup is stale, which is
        # invisible to the reader unless it is said out loud
        log.warning("lineage summary sync failed after saving plan %s", plan_doc.get("plan_id"), exc_info=True)
    return plan_doc
