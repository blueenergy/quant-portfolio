#!/usr/bin/env python3
"""Consume portfolio plan generation tasks from quant_trading."""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

from pymongo import ASCENDING, ReturnDocument

log = logging.getLogger(__name__)

# A task that cannot start yet goes back to ``waiting_for_scores`` instead of
# failing, and the generation loop drains up to ``--limit-per-iteration`` tasks
# per pass while sleeping only *after* the drain. Claiming waiting tasks without
# a backoff therefore re-claimed the same task ~8 times/second (4801 attempts
# over one 10 minute wait), hammering a single document with Mongo writes.
# Re-check on a backoff instead: the state lives on the task, so it survives a
# worker restart.
WAITING_RECHECK_BACKOFF_SECONDS = 30

# Scoring runs finish in 13-18 minutes historically (worst observed 17m45s), and
# all six universes are marked completed together, so a single universe can never
# finish early. 45 minutes is ~2.5x the worst observed: long enough not to fire
# on a slow day, short enough that a scoring run stuck in "running" fails the
# task loudly (and reaches the Feishu alert) instead of hanging forever.
WAITING_MAX_SECONDS = 45 * 60


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process pending portfolio plan generation tasks.")
    parser.add_argument("--worker-id", default="quant-scorer-plan-generator", help="Worker id written to claimed tasks")
    parser.add_argument("--limit", type=int, default=10, help="Maximum tasks to process in this run")
    parser.add_argument("--poll-interval", type=float, default=0, help="Seconds to sleep between empty polls; 0 exits when empty")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload")
    parser.add_argument("--verbose-empty", action="store_true", help="Print a summary even when no tasks were processed")
    return parser.parse_args()


def _claim_task(collections, worker_id: str):
    now = datetime.utcnow()
    return collections.plan_generation_tasks.find_one_and_update(
        {
            "$or": [
                {"status": "pending"},
                {
                    "status": "waiting_for_scores",
                    "$or": [
                        # Tasks written before the backoff existed have no
                        # next_attempt_at; claim them once so they get one.
                        {"next_attempt_at": {"$exists": False}},
                        {"next_attempt_at": {"$lte": now}},
                    ],
                },
            ]
        },
        {
            "$set": {
                "status": "running",
                "worker_id": worker_id,
                "started_at": now,
                "updated_at": now,
                "error_message": None,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("created_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def _wait_for_scores(collections, task_id: str, score_date: str, reason: str, universe_index: Optional[str] = None) -> None:
    now = datetime.utcnow()
    # Stamp the first wait so the deadline can measure how long the task has been
    # waiting. Set it only when absent so later re-checks do not reset the clock.
    collections.plan_generation_tasks.update_one(
        {"task_id": task_id, "waiting_since": {"$exists": False}},
        {"$set": {"waiting_since": now}},
    )
    collections.plan_generation_tasks.update_one(
        {"task_id": task_id},
        {
            "$set": {
                "status": "waiting_for_scores",
                "waiting_score_date": score_date,
                # Kept structured so the dashboard can explain the wait in
                # Chinese without parsing the English error_message.
                "waiting_universe_index": universe_index,
                "error_message": reason,
                "next_attempt_at": now + timedelta(seconds=WAITING_RECHECK_BACKOFF_SECONDS),
                "updated_at": now,
            }
        },
    )


def _waited_seconds(task: dict) -> Optional[float]:
    """Seconds since the task first entered the waiting state, if known."""
    since = task.get("waiting_since")
    if not isinstance(since, datetime):
        return None
    return (datetime.utcnow() - since).total_seconds()


def _complete_task(collections, task_id: str, plan_id: str) -> None:
    now = datetime.utcnow()
    collections.plan_generation_tasks.update_one(
        {"task_id": task_id},
        {"$set": {"status": "completed", "plan_id": plan_id, "completed_at": now, "updated_at": now}},
    )


def _fail_task(collections, task_id: str, error: Exception) -> None:
    now = datetime.utcnow()
    collections.plan_generation_tasks.update_one(
        {"task_id": task_id},
        {"$set": {"status": "failed", "error_message": str(error), "completed_at": now, "updated_at": now}},
    )


def _record_operation_log(collections, task: dict, status: str, message: str = "", plan_id: Optional[str] = None) -> None:
    op_type = task.get("operation_type")
    if not op_type:
        return
    now = datetime.utcnow()
    doc = {
        "log_id": str(uuid.uuid4()),
        "plan_id": plan_id or task.get("plan_id"),
        "user_id": task.get("user_id"),
        "task_id": task.get("task_id"),
        "operation_type": op_type,
        "operation_symbol": task.get("operation_symbol"),
        "status": status,
        "message": message,
        "payload": {
            "excluded_symbols": task.get("excluded_symbols") or [],
        },
        "created_at": now,
        "updated_at": now,
    }
    try:
        collections.operation_logs.insert_one(doc)
    except Exception:
        # Operation logging should never block task processing, but a gap in the
        # audit trail should still be traceable.
        log.debug("operation log insert failed", exc_info=True)


def _normalize_date(value: str) -> str:
    """Normalize a date string to YYYYMMDD format (strip hyphens, slashes)."""
    return str(value).replace("-", "").replace("/", "").strip()[:8]


def _scoring_run_status(app_db, score_date: str, universe_index: str) -> str:
    if app_db is None:
        return "completed"
    score_date = _normalize_date(score_date)
    run = app_db["scoring_runs"].find_one(
        {"score_date": score_date, "universe_index": universe_index},
        sort=[("updated_at", -1)],
    )
    if not run:
        return "missing"
    return str(run.get("status") or "missing")


def _resolve_scoring_decision(app_db, base_date: str, universe_index: str):
    """Decide whether scoring is ready for a plan-generation task.

    Returns a ``(decision, effective_date, status)`` tuple where ``decision`` is
    one of ``"proceed" | "wait" | "fail"``.

    The run recorded for ``base_date`` itself takes precedence, so an in-flight
    ("running") or explicitly "failed" same-day scoring keeps blocking. Only when
    no run is recorded for ``base_date`` (e.g. a non-scoring calendar day or a
    historical backfill) does the guard fall back to the score_date that
    ``generate_trade_plan`` will actually consume (via ``resolve_score_date``)
    and require *that* run to be completed. This keeps the guard aligned with the
    scores the planner really reads instead of hard-failing whenever
    ``base_date`` happens not to be a scoring day.
    """
    status = _scoring_run_status(app_db, base_date, universe_index)
    if status == "running":
        return "wait", base_date, status
    if status == "completed":
        return "proceed", base_date, status
    if status == "failed":
        return "fail", base_date, status
    # status == "missing": no run recorded for base_date itself.
    if app_db is None:
        return "proceed", base_date, "completed"
    from shared.scores import resolve_score_date

    try:
        resolved = resolve_score_date(app_db["stock_scores"], base_date, universe_index)
    except Exception:
        resolved = None
    if not resolved or resolved == base_date:
        return "fail", base_date, status
    resolved_status = _scoring_run_status(app_db, resolved, universe_index)
    if resolved_status == "running":
        return "wait", resolved, resolved_status
    if resolved_status == "completed":
        return "proceed", resolved, resolved_status
    return "fail", resolved, resolved_status


def _record_worker_status(collections, worker_id: str, status: str, fields=None, increments=None) -> None:
    worker_status = getattr(collections, "worker_status", None)
    if worker_status is None:
        return
    now = datetime.utcnow()
    update = {
        "$set": {
            "worker_id": worker_id,
            "worker_type": "portfolio_plan_generator",
            "status": status,
            "last_seen_at": now,
            "updated_at": now,
            **(fields or {}),
        },
        "$setOnInsert": {"created_at": now},
    }
    if increments:
        update["$inc"] = increments
    worker_status.update_one(
        {"worker_type": "portfolio_plan_generator", "worker_id": worker_id},
        update,
        upsert=True,
    )


def process_one(collections, worker_id: str, app_db=None, data_db=None):
    from portfolio.db import get_app_db, get_data_db
    from portfolio.lineage import get_lineage_excluded_symbols
    from portfolio.parity import build_research_live_parity, suggest_effective_date
    from portfolio.plans import generate_manual_rebalance_plan, generate_trade_plan
    from portfolio.risk import rerun_plan_risk_review, review_holdings, review_symbols
    from portfolio.strategies import (
        effective_strategy_config,
        load_default_param_preset,
        load_strategy_config,
        plan_params_hash,
    )

    task = _claim_task(collections, worker_id)
    if not task:
        _record_worker_status(collections, worker_id, "idle", {"current_task_id": None, "last_message": "no pending task"})
        return None

    task_id = task["task_id"]
    _record_worker_status(
        collections,
        worker_id,
        "running",
        {
            "current_task_id": task_id,
            "last_task_id": task_id,
            "last_strategy_template_id": task.get("strategy_template_id"),
            "last_base_date": task.get("base_date"),
            "last_message": None,
        },
    )
    if task.get("task_type") == "generate_manual_rebalance_plan":
        try:
            result = generate_manual_rebalance_plan(
                anchor_plan_id=task["anchor_plan_id"],
                base_date=task.get("base_date"),
                targets={str(k): int(v) for k, v in (task.get("targets") or {}).items()},
                user_id=task.get("user_id"),
                exclude_after=bool(task.get("exclude_after")),
                app_db=app_db,
                data_db=data_db,
                trading_collections=collections,
            )
            plan = result["plan"]
            plan_id = plan["plan_id"]
            now = datetime.utcnow()
            collections.plan_generation_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "completed",
                        "plan_id": plan_id,
                        "result": {
                            "plan_id": plan_id,
                            "status": plan.get("status"),
                            "executable": plan.get("executable"),
                            "non_executable_reason": plan.get("non_executable_reason"),
                        },
                        "completed_at": now,
                        "updated_at": now,
                    }
                },
            )
            _record_operation_log(collections, task, "completed", plan_id=plan_id)
            _record_worker_status(
                collections,
                worker_id,
                "completed",
                {"current_task_id": None, "last_task_id": task_id, "last_plan_id": plan_id, "last_message": None},
                {"completed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "completed", "plan_id": plan_id}
        except Exception as exc:
            _fail_task(collections, task_id, exc)
            _record_operation_log(collections, task, "failed", message=str(exc))
            _record_worker_status(
                collections,
                worker_id,
                "failed",
                {"current_task_id": None, "last_task_id": task_id, "last_message": str(exc)},
                {"failed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "failed", "error_message": str(exc)}

    if task.get("task_type") == "holdings_risk_review":
        try:
            effective_data_db = data_db if data_db is not None else get_data_db()
            payload = review_holdings(
                effective_data_db,
                task.get("positions") or [],
                task.get("base_date"),
            )
            now = datetime.utcnow()
            collections.plan_generation_tasks.update_one(
                {"task_id": task_id},
                {"$set": {"status": "completed", "result": payload, "completed_at": now, "updated_at": now}},
            )
            _record_worker_status(
                collections,
                worker_id,
                "completed",
                {"current_task_id": None, "last_task_id": task_id, "last_plan_id": task.get("plan_id"), "last_message": None},
                {"completed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "completed", "plan_id": task.get("plan_id")}
        except Exception as exc:
            _fail_task(collections, task_id, exc)
            _record_worker_status(
                collections,
                worker_id,
                "failed",
                {"current_task_id": None, "last_task_id": task_id, "last_message": str(exc)},
                {"failed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "failed", "error_message": str(exc)}

    if task.get("task_type") == "bench_risk_review":
        try:
            effective_data_db = data_db if data_db is not None else get_data_db()
            payload = review_symbols(
                effective_data_db,
                task.get("symbols") or [],
                task.get("base_date"),
            )
            now = datetime.utcnow()
            collections.plan_generation_tasks.update_one(
                {"task_id": task_id},
                {"$set": {"status": "completed", "result": payload, "completed_at": now, "updated_at": now}},
            )
            _record_worker_status(
                collections,
                worker_id,
                "completed",
                {"current_task_id": None, "last_task_id": task_id, "last_plan_id": task.get("plan_id"), "last_message": None},
                {"completed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "completed", "plan_id": task.get("plan_id")}
        except Exception as exc:
            _fail_task(collections, task_id, exc)
            _record_worker_status(
                collections,
                worker_id,
                "failed",
                {"current_task_id": None, "last_task_id": task_id, "last_message": str(exc)},
                {"failed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "failed", "error_message": str(exc)}

    if task.get("task_type") == "ai_risk_review":
        try:
            summary = rerun_plan_risk_review(
                collections,
                task["plan_id"],
                data_db=data_db,
                user_id=task.get("user_id"),
            )
            _complete_task(collections, task_id, task["plan_id"])
            _record_operation_log(
                collections,
                task,
                "completed",
                message=f"ai risk review done: {summary.get('high', 0)} high / {summary.get('medium', 0)} medium",
                plan_id=task["plan_id"],
            )
            _record_worker_status(
                collections,
                worker_id,
                "completed",
                {"current_task_id": None, "last_task_id": task_id, "last_plan_id": task["plan_id"], "last_message": None},
                {"completed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "completed", "plan_id": task["plan_id"]}
        except Exception as exc:
            _fail_task(collections, task_id, exc)
            _record_operation_log(collections, task, "failed", message=str(exc))
            _record_worker_status(
                collections,
                worker_id,
                "failed",
                {"current_task_id": None, "last_task_id": task_id, "last_message": str(exc)},
                {"failed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "failed", "error_message": str(exc)}

    try:
        effective_app_db = get_app_db() if app_db is None else app_db
        if task.get("task_type") == "preview_portfolio_plan":
            # generate_trade_plan resolves a missing data_db internally, but the
            # parity helpers below read it directly; resolve once for both.
            effective_data_db = data_db if data_db is not None else get_data_db()
            result = generate_trade_plan(
                strategy_template_id=task["strategy_template_id"],
                base_date=task["base_date"],
                mode=task.get("mode") or "auto",
                user_id=task["user_id"],
                app_db=effective_app_db,
                data_db=effective_data_db,
                trading_collections=collections,
                params_override=task.get("params_override"),
                preset_id=task.get("preset_id"),
                exact_override=True,
                run_risk_review=False,
                write=False,
            )
            parity = None
            suggested_effective_date = None
            if task.get("research_periods") is not None:
                parity_config = effective_strategy_config(
                    load_strategy_config(collections, task["strategy_template_id"]),
                    None,
                    task.get("params_override"),
                    exact_override=True,
                )
                parity_params_hash = plan_params_hash(parity_config)
                parity = build_research_live_parity(
                    collections=collections,
                    app_db=effective_app_db,
                    data_db=effective_data_db,
                    strategy_template_id=task["strategy_template_id"],
                    config=parity_config,
                    user_id=task.get("user_id"),
                    base_date=task["base_date"],
                    research_periods=task.get("research_periods") or [],
                    research_trades=task.get("research_trades") or [],
                    target_amount=float(
                        (result["plan"].get("summary") or {}).get("target_amount_per_position") or 0.0
                    ),
                    excluded_symbols=get_lineage_excluded_symbols(
                        collections,
                        task["strategy_template_id"],
                        parity_params_hash,
                        task.get("user_id"),
                    ),
                )
                suggested_effective_date = suggest_effective_date(
                    collections=collections,
                    data_db=effective_data_db,
                    user_id=task.get("user_id"),
                    base_date=task["base_date"],
                    fallback=result["plan"].get("next_rebalance_date"),
                )
            now = datetime.utcnow()
            collections.plan_generation_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "completed",
                        "result": {
                            "plan": result["plan"],
                            "items": result["items"],
                            "parity": parity,
                            "suggested_effective_date": suggested_effective_date,
                        },
                        "completed_at": now,
                        "updated_at": now,
                    }
                },
            )
            _record_operation_log(collections, task, "completed", message="live preview generated")
            _record_worker_status(
                collections,
                worker_id,
                "completed",
                {"current_task_id": None, "last_task_id": task_id, "last_message": None},
                {"completed_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "completed"}
        template = load_strategy_config(collections, task["strategy_template_id"])
        preset = collections.param_presets.find_one({"preset_id": task.get("preset_id")}, {"_id": 0}) if task.get("preset_id") else load_default_param_preset(collections, task["strategy_template_id"])
        # Force rebalance regenerates from an anchor plan. Restore that plan's params
        # verbatim (None included) and verify the lineage survives; generate_trade_plan
        # refuses when it does not.
        reconstruct_from_anchor = bool(task.get("anchor_plan_id"))
        expected_params_hash: Optional[str] = None
        if reconstruct_from_anchor:
            anchor = collections.trade_plans.find_one({"plan_id": task["anchor_plan_id"]}, {"_id": 0, "params_hash": 1})
            if not anchor:
                raise ValueError(f"anchor plan not found for force task: {task['anchor_plan_id']}")
            expected_params_hash = str(anchor.get("params_hash") or "") or None
        if expected_params_hash is None:
            # Auto-scheduled tasks name the lineage they maintain but carry no anchor.
            # When preset_id is null the fallback preset picks the hash, so the plan is
            # filed under whatever lineage that preset happens to hash to -- 22 auto
            # tasks for 8f73d17eb0ab were absorbed by 7cd2a2143a5f that way, leaving the
            # larger book unmonitored with no error anywhere. Hold this path to the same
            # invariant the force path uses.
            expected_params_hash = str(task.get("params_hash") or "") or None
        config = effective_strategy_config(template, preset, task.get("params_override"), exact_override=reconstruct_from_anchor)
        universe_index = str(config.get("universe_index") or "csi1000")
        decision, effective_score_date, scoring_status = _resolve_scoring_decision(
            effective_app_db, task["base_date"], universe_index
        )
        if decision == "wait":
            reason = f"scoring is still running for score_date={effective_score_date} universe={universe_index}"
            waited = _waited_seconds(task)
            if waited is not None and waited > WAITING_MAX_SECONDS:
                raise TimeoutError(
                    f"scoring did not complete for score_date={effective_score_date} "
                    f"universe={universe_index} after {int(waited // 60)} minutes"
                )
            _wait_for_scores(collections, task_id, effective_score_date, reason, universe_index)
            _record_worker_status(
                collections,
                worker_id,
                "waiting_for_scores",
                {"current_task_id": None, "last_task_id": task_id, "last_message": reason},
                {"waiting_count": 1, "processed_count": 1},
            )
            return {"task_id": task_id, "status": "waiting_for_scores", "reason": reason}
        if decision != "proceed":
            raise ValueError(
                f"scoring is not completed for base_date={task['base_date']} "
                f"score_date={effective_score_date} universe={universe_index}: status={scoring_status}"
            )
        result = generate_trade_plan(
            strategy_template_id=task["strategy_template_id"],
            base_date=task["base_date"],
            mode=task.get("mode") or "auto",
            user_id=task["user_id"],
            app_db=effective_app_db,
            data_db=data_db,
            trading_collections=collections,
            force=bool(task.get("force")),
            params_override=task.get("params_override"),
            preset_id=task.get("preset_id"),
            excluded_symbols=task.get("excluded_symbols"),
            exact_override=reconstruct_from_anchor,
            expected_params_hash=expected_params_hash,
        )
        plan_id = result["plan"]["plan_id"]
        if bool(task.get("auto_approve")) and str(result["plan"].get("status") or "") == "needs_review":
            from portfolio.reviews import approve_plan

            approve_plan(
                collections,
                plan_id,
                reviewer="system:activation_auto",
                comment="auto-approved on research activation",
            )
        _complete_task(collections, task_id, plan_id)
        _record_operation_log(
            collections,
            task,
            "completed",
            message=(
                "plan generated and auto-approved"
                if bool(task.get("auto_approve"))
                else "plan generated"
            ),
            plan_id=plan_id,
        )
        _record_worker_status(
            collections,
            worker_id,
            "completed",
            {"current_task_id": None, "last_task_id": task_id, "last_plan_id": plan_id, "last_message": None},
            {"completed_count": 1, "processed_count": 1},
        )
        return {"task_id": task_id, "status": "completed", "plan_id": plan_id}
    except Exception as exc:
        _fail_task(collections, task_id, exc)
        _record_operation_log(collections, task, "failed", message=str(exc))
        _record_worker_status(
            collections,
            worker_id,
            "failed",
            {"current_task_id": None, "last_task_id": task_id, "last_message": str(exc)},
            {"failed_count": 1, "processed_count": 1},
        )
        return {"task_id": task_id, "status": "failed", "error_message": str(exc)}


def main() -> int:
    from portfolio.db import ensure_trading_indexes, get_trading_collections

    args = _parse_args()
    collections = get_trading_collections()
    ensure_trading_indexes(collections)

    processed = []
    while len(processed) < args.limit:
        result = process_one(collections, args.worker_id)
        if result:
            processed.append(result)
            continue
        if args.poll_interval <= 0:
            break
        time.sleep(args.poll_interval)

    summary = {"processed": processed, "count": len(processed)}
    if processed or args.verbose_empty:
        print(f"Portfolio plan generation tasks processed: {len(processed)}")
        for item in processed:
            print(f"  {item['status']} task_id={item['task_id']} plan_id={item.get('plan_id')} error={item.get('error_message')}")
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 1 if any(item["status"] == "failed" for item in processed) else 0


if __name__ == "__main__":
    raise SystemExit(main())
