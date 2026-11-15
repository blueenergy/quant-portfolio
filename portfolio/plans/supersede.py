"""Plan supersede and same-day reuse selection."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from pymongo import ASCENDING

from portfolio.constants import (
    ACTIVE_PLAN_STATUSES,
    RECORD_KIND_TRADE_PLAN,
    SAME_DAY_PLAN_REUSE_STATUSES,
    TERMINAL_LIVE_SIGNAL_STATUSES,
)
from portfolio.db import TradingCollections
from portfolio.lineage import _execution_fill_quantity, _filled_quantity_from_signal
from portfolio.plans.signals import cancel_open_live_signals_for_plan
from portfolio.record_kind import strategy_decision_mongo_filter


def supersede_stale_active_strategy_plans(
    collections: "TradingCollections",
    *,
    app_db=None,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    base_date: str,
    replacement_plan_id: str,
    now: Optional[datetime] = None,
) -> List[str]:
    """Mark older active strategy plans as replaced by a new rebalance plan."""
    now = now or datetime.utcnow()
    query: Dict[str, Any] = strategy_decision_mongo_filter(
        strategy_template_id=strategy_template_id,
        params_hash=params_hash,
        base_date={"$lt": base_date},
        status={"$in": sorted(ACTIVE_PLAN_STATUSES)},
    )
    if user_id:
        query["user_id"] = user_id
    rows = list(collections.trade_plans.find(query, {"_id": 0, "plan_id": 1, "user_id": 1}).sort("base_date", ASCENDING))
    superseded: List[str] = []
    for row in rows:
        plan_id = str(row.get("plan_id") or "")
        if not plan_id:
            continue
        cancelled_signal_count = 0
        if app_db is not None:
            cancelled_signal_count, has_live_fills = cancel_open_live_signals_for_plan(
                app_db,
                plan_id=plan_id,
                user_id=row.get("user_id"),
                reviewer="system:supersede",
                reason=f"superseded by {replacement_plan_id}",
                now=now,
            )
            if has_live_fills:
                collections.operation_logs.insert_one(
                    {
                        "log_id": f"supersede-skip-{plan_id}-{uuid.uuid4().hex[:8]}",
                        "plan_id": plan_id,
                        "user_id": row.get("user_id"),
                        "operation_type": "supersede_plan",
                        "status": "blocked",
                        "message": "plan has live fills; cannot supersede safely",
                        "payload": {"replacement_plan_id": replacement_plan_id},
                        "created_at": now,
                    }
                )
                continue
        update_query = {"plan_id": plan_id}
        if row.get("user_id"):
            update_query["user_id"] = row.get("user_id")
        collections.trade_plans.update_one(
            update_query,
            {
                "$set": {
                    "status": "superseded",
                    "superseded_at": now,
                    "superseded_by_plan_id": replacement_plan_id,
                    "supersede_cancelled_signal_count": cancelled_signal_count,
                    "updated_at": now,
                }
            },
        )
        collections.operation_logs.insert_one(
            {
                "log_id": f"supersede-{plan_id}-{uuid.uuid4().hex[:8]}",
                "plan_id": plan_id,
                "user_id": row.get("user_id"),
                "operation_type": "supersede_plan",
                "status": "success",
                "message": f"superseded by {replacement_plan_id}",
                "payload": {
                    "replacement_plan_id": replacement_plan_id,
                    "cancelled_signal_count": cancelled_signal_count,
                },
                "created_at": now,
            }
        )
        superseded.append(plan_id)
    return superseded


def sweep_stale_zero_fill_plans(
    collections: "TradingCollections",
    *,
    app_db=None,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str] = None,
    now: Optional[datetime] = None,
) -> List[str]:
    """Expire published live trade_plans that settled with zero fills.

    A live plan sits at ``approved`` after its signals are published until they
    settle. When every live signal has reached a terminal status with *zero*
    shares filled, the plan never executed and never will — yet nothing flips it
    off ``approved``: ``executed_live`` requires a fill and
    ``supersede_stale_active_strategy_plans`` deliberately skips manual-origin
    plans (``origin != manual``). Such plans linger as action-required forever
    (e.g. a manual live rebalance whose only order was cancelled before filling).

    Mark them ``expired`` so they leave the active set. Unlike supersede this is
    origin-agnostic and not date-gated; it only ever touches plans whose live
    signals have all settled with no fill, so it can never race a plan that is
    still executing. Mirrors the read-path ``_maybe_stamp_plan_expired_no_fill``
    in quantFinance so both entry points converge on the same terminal.
    """
    if app_db is None:
        return []
    now = now or datetime.utcnow()
    query: Dict[str, Any] = {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "record_kind": RECORD_KIND_TRADE_PLAN,
        "status": "approved",
        "live_signals_published_at": {"$ne": None},
    }
    if user_id:
        query["user_id"] = user_id
    rows = list(collections.trade_plans.find(query, {"_id": 0, "plan_id": 1, "user_id": 1}))
    expired: List[str] = []
    for row in rows:
        plan_id = str(row.get("plan_id") or "")
        if not plan_id:
            continue
        owner = row.get("user_id")
        sig_query: Dict[str, Any] = {"plan_id": plan_id, "mode": "live"}
        if owner:
            sig_query["user_id"] = owner
        signals = list(app_db["trade_signals"].find(sig_query))
        if not signals:
            continue
        # Any signal still in flight -> the plan may yet fill; leave it alone.
        if any(str(s.get("status") or "").lower() not in TERMINAL_LIVE_SIGNAL_STATUSES for s in signals):
            continue
        order_ids = [s.get("order_id") for s in signals if s.get("order_id")]
        executions = list(app_db["trade_executions"].find({"order_id": {"$in": order_ids}})) if order_ids else []
        if owner:
            executions = [e for e in executions if e.get("user_id") in (None, owner)]
        has_fills = any(_filled_quantity_from_signal(s) > 0 for s in signals) or any(
            _execution_fill_quantity(e) > 0 for e in executions
        )
        if has_fills:
            continue
        update_query: Dict[str, Any] = {"plan_id": plan_id, "status": "approved"}
        if owner:
            update_query["user_id"] = owner
        result = collections.trade_plans.update_one(
            update_query,
            {"$set": {"status": "expired", "expired_at": now, "updated_at": now}},
        )
        if int(getattr(result, "modified_count", 0) or 0) == 0:
            continue
        collections.operation_logs.insert_one(
            {
                "log_id": f"expire-zero-fill-{plan_id}-{uuid.uuid4().hex[:8]}",
                "plan_id": plan_id,
                "user_id": owner,
                "operation_type": "expire_plan",
                "status": "success",
                "message": "published live plan settled with zero fills; marked expired",
                "payload": {},
                "created_at": now,
            }
        )
        expired.append(plan_id)
    return expired


def select_same_day_strategy_plan_for_generation(rows: Iterable[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Pick the same-day plan generation should reuse, if any.

    Active/mutable plans are reusable and must win over terminal history. When
    only terminal rows exist (e.g. user cancelled and wants to rebuild), return a
    terminal row so the caller can create a unique force plan id.
    """
    values = list(rows)
    for row in values:
        if str(row.get("status") or "") in SAME_DAY_PLAN_REUSE_STATUSES:
            return row
    return values[0] if values else None
