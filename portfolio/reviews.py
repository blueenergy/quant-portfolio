"""Plan approval / rejection helpers."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict

from portfolio.constants import TERMINAL_REVIEW_STATUSES
from portfolio.db import TradingCollections


def approve_plan(collections: TradingCollections, plan_id: str, reviewer: str = "system", comment: str = "") -> Dict[str, Any]:
    return review_plan(collections, plan_id, "approved", reviewer, comment)


def reject_plan(collections: TradingCollections, plan_id: str, reviewer: str = "system", comment: str = "") -> Dict[str, Any]:
    return review_plan(collections, plan_id, "rejected", reviewer, comment)


def review_plan(collections: TradingCollections, plan_id: str, decision: str, reviewer: str, comment: str = "") -> Dict[str, Any]:
    if decision not in TERMINAL_REVIEW_STATUSES:
        raise ValueError("decision must be approved or rejected")
    plan = collections.trade_plans.find_one({"plan_id": plan_id}, {"_id": 0})
    if not plan:
        raise ValueError(f"Plan not found: {plan_id}")
    if plan.get("status") not in {"needs_review", "approved", "rejected"}:
        raise ValueError(f"Plan {plan_id} is not reviewable in status {plan.get('status')}")
    now = datetime.utcnow()
    review_doc = {
        "review_id": str(uuid.uuid4()),
        "plan_id": plan_id,
        "strategy_template_id": plan.get("strategy_template_id"),
        "user_id": plan.get("user_id"),
        "decision": decision,
        "reviewer": reviewer,
        "comment": comment,
        "created_at": now,
    }
    collections.plan_reviews.insert_one(review_doc)
    collections.trade_plans.update_one({"plan_id": plan_id}, {"$set": {"status": decision, "reviewed_at": now, "reviewer": reviewer}})
    return review_doc
