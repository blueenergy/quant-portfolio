"""Mongo accessors, collection bundle, and index/seed helpers."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Optional, Tuple

from dotenv import load_dotenv
from pymongo import ASCENDING, DESCENDING, MongoClient

from portfolio.constants import (
    DEFAULT_STRATEGY_TEMPLATES,
    PLAN_SNAPSHOT_PARAM_KEYS,
    UNIQUE_STRATEGY_PLAN_STATUSES,
)
from utils.mongo_client import create_mongo_client

log = logging.getLogger(__name__)

load_dotenv(override=False)


@dataclass(frozen=True)
class TradingCollections:
    strategy_configs: Any
    trade_plans: Any
    trade_plan_items: Any
    plan_reviews: Any
    paper_positions: Any
    paper_executions: Any
    paper_equity: Any
    plan_generation_tasks: Any
    operation_logs: Any
    worker_status: Any
    param_presets: Any
    research_jobs: Any
    research_results: Any
    research_reviews: Any
    lineage_controls: Any
    strategy_activations: Any
    trailing_stop_runs: Any
    trailing_stop_tasks: Any
    factor_jobs: Any
    factor_results: Any
    factor_catalog: Any


def get_mongo_client() -> MongoClient:
    return create_mongo_client()


def get_app_db(client: Optional[MongoClient] = None):
    client = client or get_mongo_client()
    return client[os.getenv("MONGO_DB", "finance")]


def get_data_db(client: Optional[MongoClient] = None):
    client = client or get_mongo_client()
    return client[os.getenv("DATA_DB_NAME", "quant_data")]


def get_ml_db(client: Optional[MongoClient] = None):
    """Model platform DB (``quant_ml.model_predictions``)."""
    client = client or get_mongo_client()
    return client[os.getenv("QUANT_ML_DB", "quant_ml")]


def get_trading_db(client: Optional[MongoClient] = None):
    client = client or get_mongo_client()
    return client[os.getenv("TRADING_DB_NAME", "quant_trading")]


def get_trading_collections(db=None) -> TradingCollections:
    db = db if db is not None else get_trading_db()
    return TradingCollections(
        strategy_configs=db["portfolio_strategy_configs"],
        trade_plans=db["portfolio_trade_plans"],
        trade_plan_items=db["portfolio_trade_plan_items"],
        plan_reviews=db["portfolio_plan_reviews"],
        paper_positions=db["portfolio_paper_positions"],
        paper_executions=db["portfolio_paper_executions"],
        paper_equity=db["portfolio_paper_equity"],
        plan_generation_tasks=db["portfolio_plan_generation_tasks"],
        operation_logs=db["portfolio_plan_operation_logs"],
        worker_status=db["portfolio_worker_status"],
        param_presets=db["portfolio_strategy_param_presets"],
        research_jobs=db["portfolio_research_jobs"],
        research_results=db["portfolio_research_results"],
        research_reviews=db["portfolio_research_reviews"],
        lineage_controls=db["portfolio_lineage_controls"],
        strategy_activations=db["portfolio_strategy_activations"],
        trailing_stop_runs=db["portfolio_trailing_stop_runs"],
        trailing_stop_tasks=db["portfolio_trailing_stop_tasks"],
        factor_jobs=db["factor_backtest_jobs"],
        factor_results=db["factor_backtest_results"],
        factor_catalog=db["factor_catalog"],
    )


def _drop_indexes_by_key(
    collection,
    key_pattern: List[Tuple[str, int]],
    unique_only: bool = False,
    drop_partial: bool = False,
) -> None:
    if not hasattr(collection, "index_information"):
        return
    try:
        indexes = collection.index_information()
    except Exception:
        return
    for name, spec in indexes.items():
        if name == "_id_":
            continue
        if spec.get("key") != key_pattern:
            continue
        if unique_only and not spec.get("unique"):
            continue
        # This helper is used for legacy index migration. Once the replacement is
        # a partial index with the same key pattern, ensure_trading_indexes must be
        # idempotent and leave it in place on subsequent runs.
        if not drop_partial and spec.get("partialFilterExpression"):
            continue
        try:
            collection.drop_index(name)
        except Exception:
            # a concurrent startup may have dropped it already; a permission
            # problem looks the same here, so leave a trace either way
            log.debug("could not drop legacy index %s", name, exc_info=True)


def _drop_conflicting_unique_strategy_plan_indexes(collection) -> None:
    key_pattern = [
        ("user_id", ASCENDING),
        ("strategy_template_id", ASCENDING),
        ("params_hash", ASCENDING),
        ("base_date", ASCENDING),
    ]
    desired_partial = {
        "origin": "strategy",
        "status": {"$in": UNIQUE_STRATEGY_PLAN_STATUSES},
    }
    if not hasattr(collection, "index_information"):
        return
    try:
        indexes = collection.index_information()
    except Exception:
        return
    for name, spec in indexes.items():
        if name == "_id_":
            continue
        if spec.get("key") != key_pattern or not spec.get("unique"):
            continue
        if spec.get("partialFilterExpression") == desired_partial:
            continue
        try:
            collection.drop_index(name)
        except Exception:
            log.debug("could not drop conflicting unique index %s", name, exc_info=True)


def ensure_trading_indexes(collections: Optional[TradingCollections] = None) -> None:
    cols = collections or get_trading_collections()
    cols.strategy_configs.create_index([("strategy_template_id", ASCENDING)], unique=True)
    cols.strategy_configs.create_index([("status", ASCENDING)])
    cols.param_presets.create_index([("preset_id", ASCENDING)], unique=True)
    cols.param_presets.create_index([("strategy_template_id", ASCENDING), ("status", ASCENDING)])
    cols.param_presets.create_index([("user_id", ASCENDING), ("strategy_template_id", ASCENDING), ("created_at", DESCENDING)])
    cols.trade_plans.create_index([("plan_id", ASCENDING)], unique=True)
    cols.trade_plans.create_index([("user_id", ASCENDING), ("plan_id", ASCENDING)])
    # Keep at most one effective strategy-generated plan per lineage/day.
    # Terminal plans (rejected/cancelled/superseded) remain in the timeline but
    # must not block a fresh same-day regeneration.
    _drop_conflicting_unique_strategy_plan_indexes(cols.trade_plans)
    cols.trade_plans.create_index(
        [("user_id", ASCENDING), ("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("base_date", ASCENDING)],
        name="user_strategy_params_base_date_active_strategy",
        unique=True,
        partialFilterExpression={
            "origin": "strategy",
            "status": {"$in": UNIQUE_STRATEGY_PLAN_STATUSES},
        },
    )
    cols.trade_plans.create_index([("strategy_template_id", ASCENDING), ("execute_date", ASCENDING)])
    cols.trade_plans.create_index([("status", ASCENDING), ("execute_date", ASCENDING)])
    cols.trade_plan_items.create_index([("plan_id", ASCENDING), ("rank", ASCENDING)])
    cols.plan_reviews.create_index([("plan_id", ASCENDING), ("created_at", DESCENDING)])
    cols.paper_positions.create_index([("strategy_template_id", ASCENDING), ("as_of_date", DESCENDING)])
    cols.paper_positions.create_index(
        [("user_id", ASCENDING), ("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("as_of_date", DESCENDING)]
    )
    cols.paper_positions.create_index([("user_id", ASCENDING), ("plan_id", ASCENDING), ("as_of_date", DESCENDING)])
    cols.paper_executions.create_index([("plan_id", ASCENDING), ("symbol", ASCENDING)])
    _drop_indexes_by_key(cols.paper_equity, [("user_id", ASCENDING), ("strategy_id", ASCENDING), ("date", ASCENDING)], unique_only=True)
    _drop_indexes_by_key(
        cols.paper_equity, [("user_id", ASCENDING), ("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("date", ASCENDING)], unique_only=True
    )
    cols.paper_equity.create_index([("user_id", ASCENDING), ("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("date", ASCENDING)])
    cols.paper_equity.create_index([("user_id", ASCENDING), ("plan_id", ASCENDING), ("date", ASCENDING)], unique=True)
    cols.plan_generation_tasks.create_index([("task_id", ASCENDING)], unique=True)
    cols.plan_generation_tasks.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    # Serves the waiting-task re-check filter (status + next_attempt_at) that
    # keeps the generation loop from re-claiming a waiting task in a hot loop.
    cols.plan_generation_tasks.create_index([("status", ASCENDING), ("next_attempt_at", ASCENDING), ("created_at", ASCENDING)])
    cols.plan_generation_tasks.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.operation_logs.create_index([("log_id", ASCENDING)], unique=True)
    cols.operation_logs.create_index([("plan_id", ASCENDING), ("created_at", DESCENDING)])
    cols.operation_logs.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.operation_logs.create_index([("task_id", ASCENDING)])
    cols.worker_status.create_index([("worker_type", ASCENDING), ("worker_id", ASCENDING)], unique=True)
    cols.worker_status.create_index([("last_seen_at", DESCENDING)])
    cols.research_jobs.create_index([("job_id", ASCENDING)], unique=True)
    cols.research_jobs.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.research_jobs.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    cols.research_results.create_index([("result_id", ASCENDING)], unique=True)
    cols.research_results.create_index([("job_id", ASCENDING)])
    cols.research_results.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.research_reviews.create_index([("job_id", ASCENDING), ("created_at", DESCENDING)])
    cols.research_reviews.create_index([("research_result_id", ASCENDING), ("created_at", DESCENDING)])
    cols.lineage_controls.create_index(
        [("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("user_id", ASCENDING)],
        unique=True,
    )
    cols.strategy_activations.create_index([("activation_id", ASCENDING)], unique=True)
    cols.strategy_activations.create_index([("status", ASCENDING), ("effective_date", ASCENDING)])
    cols.strategy_activations.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.trailing_stop_runs.create_index([("run_id", ASCENDING)], unique=True)
    cols.trailing_stop_runs.create_index(
        [("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("user_id", ASCENDING), ("base_date", ASCENDING)],
        unique=True,
        name="trailing_stop_combo_base_date",
    )
    cols.trailing_stop_runs.create_index(
        [("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("user_id", ASCENDING), ("created_at", DESCENDING)]
    )
    cols.trailing_stop_tasks.create_index([("task_id", ASCENDING)], unique=True)
    cols.trailing_stop_tasks.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    cols.trailing_stop_tasks.create_index(
        [("strategy_template_id", ASCENDING), ("params_hash", ASCENDING), ("user_id", ASCENDING), ("base_date", ASCENDING)]
    )
    cols.factor_jobs.create_index([("job_id", ASCENDING)], unique=True)
    # The claim query is {status: pending} sorted by created_at, so this index
    # is what keeps pickup cheap once completed jobs pile up.
    cols.factor_jobs.create_index([("status", ASCENDING), ("created_at", ASCENDING)])
    cols.factor_jobs.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    cols.factor_results.create_index([("result_id", ASCENDING)], unique=True)
    cols.factor_results.create_index([("job_id", ASCENDING)])
    cols.factor_results.create_index([("user_id", ASCENDING), ("created_at", DESCENDING)])
    # One document per factor set, replaced wholesale by the worker, so the API
    # can serve factor metadata without importing this repo.
    cols.factor_catalog.create_index([("factor_set", ASCENDING)], unique=True)


def seed_default_strategy_configs(collections: Optional[TradingCollections] = None) -> List[str]:
    cols = collections or get_trading_collections()
    ensure_trading_indexes(cols)
    seeded: List[str] = []
    now = datetime.utcnow()
    for config in DEFAULT_STRATEGY_TEMPLATES:
        doc = {**config, "created_at": now}
        update_doc = {key: value for key, value in config.items() if key != "strategy_template_id"}
        cols.strategy_configs.update_one(
            {"strategy_template_id": doc["strategy_template_id"]},
            {
                "$setOnInsert": {"created_at": now},
                "$set": {**update_doc, "updated_at": now},
                "$unset": {key: "" for key in PLAN_SNAPSHOT_PARAM_KEYS if key != "score_type"},
            },
            upsert=True,
        )
        seeded.append(doc["strategy_template_id"])
    return seeded
