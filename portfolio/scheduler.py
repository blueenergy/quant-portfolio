"""Auto plan-generation subscriptions and enqueue helpers."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from portfolio.calendar import _shift_date_days, normalize_date
from portfolio.db import TradingCollections
from portfolio.lineage import is_lineage_paused, pause_lineage
from portfolio.record_kind import strategy_generated_record_mongo_filter

SUBSCRIBED_PLAN_STATUSES = {"executed_paper", "partially_executed"}


AUTO_TASK_DEDUP_STATUSES = ("pending", "waiting_for_scores", "running", "completed")


def _plan_is_committed(plan: Dict[str, Any]) -> bool:
    if str(plan.get("status")) in SUBSCRIBED_PLAN_STATUSES:
        return True
    return bool(plan.get("live_signals_published_at"))


def find_active_plan_subscriptions(
    collections: TradingCollections,
    as_of_date: Optional[str] = None,
    lookback_days: int = 120,
) -> List[Dict[str, Any]]:
    """Derive the set of (strategy, params_hash, user) combos to auto-maintain.

    A combo is "subscribed" once the user has turned it into a real paper/live
    position (executed paper plan, or one with published live signals). We use *existence* of any
    recent committed plan rather than the latest plan's status, because between
    rebalances the latest plan is an advisory monitor plan.
    """
    query: Dict[str, Any] = {}
    cutoff = _shift_date_days(as_of_date, -abs(int(lookback_days))) if as_of_date and lookback_days else None
    if cutoff:
        query["base_date"] = {"$gte": cutoff}
    projection = {
        "_id": 0,
        "strategy_template_id": 1,
        "params_hash": 1,
        "user_id": 1,
        "preset_id": 1,
        "params_override": 1,
        "base_date": 1,
        "status": 1,
        "live_signals_published_at": 1,
    }
    latest_by_combo: Dict[Tuple[Any, Any, Any], Dict[str, Any]] = {}
    for plan in collections.trade_plans.find(query, projection):
        if not _plan_is_committed(plan):
            continue
        key = (plan.get("strategy_template_id"), plan.get("params_hash"), plan.get("user_id"))
        existing = latest_by_combo.get(key)
        if existing is None or str(plan.get("base_date") or "") > str(existing.get("base_date") or ""):
            latest_by_combo[key] = plan
    subscriptions: List[Dict[str, Any]] = []
    for (strategy_template_id, params_hash, user_id), plan in latest_by_combo.items():
        if is_lineage_paused(collections, strategy_template_id, params_hash, user_id):
            continue
        subscriptions.append(
            {
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "user_id": user_id,
                "preset_id": plan.get("preset_id"),
                "params_override": plan.get("params_override") or {},
                "source_base_date": plan.get("base_date"),
            }
        )
    return subscriptions


def apply_due_activations(
    collections: TradingCollections,
    base_date: str,
    *,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Switch scheduled research activations that have reached their date.

    A scheduled activation waits for the next rebalance date so the old book
    keeps its monitoring cadence until the switch. On the due date this pauses
    the replaced lineages and enqueues the new lineage's first (rebalance) plan.
    """
    base_date = normalize_date(base_date) or base_date
    now = now or datetime.utcnow()
    due = list(
        collections.strategy_activations.find(
            {"status": "scheduled", "effective_date": {"$lte": base_date}}
        )
    )
    applied: List[Dict[str, Any]] = []
    for activation in due:
        strategy_template_id = str(activation.get("strategy_template_id") or "")
        params_hash = str(activation.get("params_hash") or "")
        user_id = activation.get("user_id")
        if not strategy_template_id or not params_hash or not user_id:
            collections.strategy_activations.update_one(
                {"activation_id": activation.get("activation_id")},
                {
                    "$set": {
                        "status": "failed",
                        "error_message": "activation is missing strategy_template_id/params_hash/user_id",
                        "updated_at": now,
                    }
                },
            )
            continue
        paused = []
        for replaced in activation.get("replaces") or []:
            replaced_template = str(replaced.get("strategy_template_id") or "")
            replaced_hash = str(replaced.get("params_hash") or "")
            if not replaced_template or not replaced_hash:
                continue
            if replaced_template == strategy_template_id and replaced_hash == params_hash:
                continue
            pause_lineage(
                collections,
                replaced_template,
                replaced_hash,
                user_id,
                reason="replaced_by_activation",
                paused_by=activation.get("requested_by") or user_id,
            )
            paused.append(
                {"strategy_template_id": replaced_template, "params_hash": replaced_hash}
            )
        execution_venue = str(activation.get("execution_venue") or "paper")
        collections.lineage_controls.update_one(
            {
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "user_id": user_id,
            },
            {
                "$set": {
                    "strategy_template_id": strategy_template_id,
                    "params_hash": params_hash,
                    "user_id": user_id,
                    "execution_venue": execution_venue,
                    "activation_id": activation.get("activation_id"),
                    "updated_at": now,
                },
                "$setOnInsert": {"paused": False, "excluded_symbols": []},
            },
            upsert=True,
        )
        params_override = dict(activation.get("params_override") or {})
        params_override["capital_basis"] = (
            "rolling_paper" if execution_venue == "paper" else "account_equity"
        )
        existing_task = collections.plan_generation_tasks.find_one(
            {
                "task_type": "generate_portfolio_plan",
                "activation_id": activation.get("activation_id"),
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "user_id": user_id,
                "base_date": base_date,
                "status": {"$in": list(AUTO_TASK_DEDUP_STATUSES)},
            },
            {"_id": 0, "task_id": 1},
        )
        if existing_task:
            task_id = existing_task.get("task_id")
        else:
            task_id = str(uuid.uuid4())
            collections.plan_generation_tasks.insert_one(
                {
                    "task_id": task_id,
                    "task_type": "generate_portfolio_plan",
                    "user_id": user_id,
                    "strategy_template_id": strategy_template_id,
                    "params_hash": params_hash,
                    "base_date": base_date,
                    "mode": "rebalance",
                    "force": False,
                    "preset_id": activation.get("preset_id"),
                    "params_override": params_override,
                    "source": "strategy_activation",
                    "activation_id": activation.get("activation_id"),
                    "auto": False,
                    "auto_approve": bool(activation.get("auto_approve")),
                    "status": "pending",
                    "attempts": 0,
                    "plan_id": None,
                    "error_message": None,
                    "created_at": now,
                    "updated_at": now,
                }
            )
        collections.strategy_activations.update_one(
            {"activation_id": activation.get("activation_id")},
            {
                "$set": {
                    "status": "active",
                    "activated_at": now,
                    "activation_base_date": base_date,
                    "first_task_id": task_id,
                    "paused_lineages": paused,
                    "updated_at": now,
                }
            },
        )
        if activation.get("preset_id"):
            collections.param_presets.update_one(
                {"preset_id": activation.get("preset_id"), "user_id": user_id},
                {"$set": {"activation_status": "active", "updated_at": now}},
            )
        if activation.get("source_result_id"):
            collections.research_results.update_one(
                {"result_id": activation.get("source_result_id")},
                {"$set": {"live_activation_status": "active", "updated_at": now}},
            )
        collections.plan_generation_tasks.update_many(
            {
                "activation_id": activation.get("activation_id"),
                "task_type": "preview_portfolio_plan",
            },
            {"$set": {"activation_status": "active", "updated_at": now}},
        )
        applied.append(
            {
                "activation_id": activation.get("activation_id"),
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "task_id": task_id,
                "paused": paused,
            }
        )
    return {"base_date": base_date, "due": len(due), "applied": applied}


def enqueue_auto_generation_tasks(
    collections: TradingCollections,
    base_date: str,
    *,
    mode: str = "auto",
    lookback_days: int = 120,
    source: str = "auto_scheduler",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Enqueue one ``generate_portfolio_plan`` task per subscribed combo.

    Idempotent: skips combos that already have a strategy plan for
    ``base_date`` or a non-failed generation task for the same combo/date. Manual
    plans are discretionary ledger facts and must not cancel the scheduled
    proposal cadence.
    """
    base_date = normalize_date(base_date) or base_date
    now = now or datetime.utcnow()
    activation_result = apply_due_activations(collections, base_date, now=now)
    subscriptions = find_active_plan_subscriptions(collections, as_of_date=base_date, lookback_days=lookback_days)
    enqueued: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for sub in subscriptions:
        strategy_template_id = sub["strategy_template_id"]
        params_hash = sub["params_hash"]
        user_id = sub["user_id"]
        combo = {"strategy_template_id": strategy_template_id, "params_hash": params_hash, "user_id": user_id}
        existing_plan = collections.trade_plans.find_one(
            strategy_generated_record_mongo_filter(**combo, base_date=base_date),
            {"_id": 0, "plan_id": 1},
        )
        if existing_plan:
            skipped.append({**combo, "reason": "plan_exists", "plan_id": existing_plan.get("plan_id")})
            continue
        existing_task = collections.plan_generation_tasks.find_one(
            {
                "task_type": "generate_portfolio_plan",
                "strategy_template_id": strategy_template_id,
                "params_hash": params_hash,
                "user_id": user_id,
                "base_date": base_date,
                "status": {"$in": list(AUTO_TASK_DEDUP_STATUSES)},
            },
            {"_id": 0, "task_id": 1},
        )
        if existing_task:
            skipped.append({**combo, "reason": "task_exists", "task_id": existing_task.get("task_id")})
            continue
        task = {
            "task_id": str(uuid.uuid4()),
            "task_type": "generate_portfolio_plan",
            "user_id": user_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "base_date": base_date,
            "mode": mode,
            "force": False,
            "preset_id": sub.get("preset_id"),
            "params_override": sub.get("params_override") or {},
            "source": source,
            "auto": True,
            "status": "pending",
            "attempts": 0,
            "plan_id": None,
            "error_message": None,
            "created_at": now,
            "updated_at": now,
        }
        collections.plan_generation_tasks.insert_one(task)
        enqueued.append({**combo, "task_id": task["task_id"]})
    return {
        "base_date": base_date,
        "mode": mode,
        "subscriptions": len(subscriptions),
        "enqueued": enqueued,
        "skipped": skipped,
        "activations": activation_result,
    }
