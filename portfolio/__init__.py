"""Portfolio package (split of ``tools.portfolio_trading``).

Leaf modules can be imported without loading plan/paper core::

    from portfolio.db import get_trading_collections

``portfolio.paper`` (paper execution) loads only on demand via ``__getattr__``.
"""

from __future__ import annotations

from typing import Any

from pymongo import ASCENDING, DESCENDING

from portfolio.calendar import (
    compute_next_rebalance_date,
    forward_trading_dates,
    latest_trading_date_on_or_before,
    next_trading_date,
    normalize_date,
    trading_dates_between,
)
from portfolio.constants import (
    ACTIVE_PLAN_STATUSES,
    AUTO_PAPER_APPROVE_REVIEWER,
    CN_TZ,
    DEFAULT_STRATEGY_TEMPLATES,
    GROWTH_CYCLE_PARAMETER_SCHEMA,
    PAPER_EXECUTION_MODE_AUTO_SHADOW,
    PAPER_EXECUTION_MODE_MANUAL_REVIEW,
    PAPER_EXECUTION_MODES,
    PLAN_MUTABLE_STATUSES,
    PLAN_PARAM_KEYS,
    PLAN_SNAPSHOT_PARAM_KEYS,
    RECORD_KIND_OBSERVATION,
    RECORD_KIND_TRADE_PLAN,
    SAME_DAY_PLAN_REUSE_STATUSES,
    TERMINAL_LIVE_SIGNAL_STATUSES,
    TERMINAL_PLAN_STATUSES,
    TERMINAL_REVIEW_STATUSES,
    UNIQUE_STRATEGY_PLAN_STATUSES,
    business_today_yyyymmdd,
)
from portfolio.construction import (
    candidate_stats,
    load_index_universe_symbols_asof,
    load_latest_universe_symbols,
    load_model_cross_section,
    load_score_frame,
    resolve_plan_score_date,
    round_lot_shares,
    select_portfolio,
    select_trade_plan_portfolio,
    volatility_position_scale_map,
)
from portfolio.db import (
    TradingCollections,
    ensure_trading_indexes,
    get_app_db,
    get_data_db,
    get_mongo_client,
    get_trading_collections,
    get_trading_db,
    seed_default_strategy_configs,
)
from portfolio.lineage import (
    CAPITAL_BASIS_ACCOUNT_EQUITY,
    CAPITAL_BASIS_FIXED_NOTIONAL,
    CAPITAL_BASIS_OPTIONS,
    CAPITAL_BASIS_ROLLING_PAPER,
    LIVE_BASELINE_STALE_SECONDS,
    CapitalBaselineError,
    add_excluded_symbols,
    apply_manual_plan_lineage_effects,
    get_lineage_control,
    get_lineage_excluded_symbols,
    is_lineage_paused,
    latest_lineage_position_snapshot,
    latest_plan_position_snapshot,
    latest_position_snapshot,
    pause_lineage,
    position_map,
    reconstruct_live_lineage_baseline,
    resolve_capital_baseline,
    resume_lineage,
)
from portfolio.maintenance import backfill_plan_items_from_lineage
from portfolio.market_data import (
    latest_close_prices_on_or_before,
    latest_prices,
    paper_execution_prices,
)
from portfolio.plans import (
    build_force_plan_id,
    build_manual_plan_id,
    determine_plan_type,
    generate_manual_rebalance_plan,
    generate_trade_plan,
    maybe_auto_approve_paper_plan,
    normalize_manual_target_shares,
    resolve_paper_execution_mode,
    select_same_day_strategy_plan_for_generation,
    supersede_stale_active_strategy_plans,
    sweep_stale_zero_fill_plans,
)
from portfolio.reviews import approve_plan, reject_plan, review_plan
from portfolio.risk import rerun_plan_risk_review, review_holdings, review_symbols
from portfolio.parity import build_research_live_parity, suggest_effective_date
from portfolio.scheduler import (
    apply_due_activations,
    enqueue_auto_generation_tasks,
    find_active_plan_subscriptions,
)
from portfolio.trailing_stop import (
    TRAILING_STOP_ORIGIN,
    TRAILING_STOP_PLAN_TYPE,
    LINEAGE_TRAILING_STOP_FIELD,
    check_trailing_stop,
    enqueue_trailing_stop_tasks,
    fetch_trailing_stop_price_rows,
    get_trailing_stop_cycle_exclusions,
    is_trailing_stop_monitor_verbose,
    latest_trailing_stop_run,
    list_trailing_stop_runs,
    lineage_has_explicit_trailing_stop_setting,
    process_trailing_stop_task,
    reclaim_stale_trailing_stop_tasks,
    resolve_trailing_stop_pct,
    resolve_trailing_stop_pct_for_lineage,
    run_trailing_stop_monitor,
    seed_lineage_trailing_stop_from_config,
    set_lineage_trailing_stop_pct,
    trailing_stop_pct_for_snapshot,
)
from portfolio.strategies import (
    build_user_plan_id,
    effective_strategy_config,
    load_default_param_preset,
    load_strategy_config,
    normalized_lineage_params,
    normalized_plan_params,
    plan_params_hash,
    selected_preset_id_for_plan,
    strategy_display_name,
)
from shared.scores import compute_score_series, resolve_score_date

# Intentionally do NOT import portfolio.paper here.

# Re-exports. ``portfolio.paper`` symbols are intentionally excluded so that
# ``from portfolio import *`` does not eagerly load the paper-execution core;
# they remain reachable via ``__getattr__`` below.
__all__ = [
    "ASCENDING",
    "DESCENDING",
    # calendar
    "compute_next_rebalance_date",
    "forward_trading_dates",
    "latest_trading_date_on_or_before",
    "next_trading_date",
    "normalize_date",
    "trading_dates_between",
    # constants
    "ACTIVE_PLAN_STATUSES",
    "AUTO_PAPER_APPROVE_REVIEWER",
    "CN_TZ",
    "DEFAULT_STRATEGY_TEMPLATES",
    "GROWTH_CYCLE_PARAMETER_SCHEMA",
    "PAPER_EXECUTION_MODE_AUTO_SHADOW",
    "PAPER_EXECUTION_MODE_MANUAL_REVIEW",
    "PAPER_EXECUTION_MODES",
    "PLAN_MUTABLE_STATUSES",
    "PLAN_PARAM_KEYS",
    "PLAN_SNAPSHOT_PARAM_KEYS",
    "RECORD_KIND_OBSERVATION",
    "RECORD_KIND_TRADE_PLAN",
    "SAME_DAY_PLAN_REUSE_STATUSES",
    "TERMINAL_LIVE_SIGNAL_STATUSES",
    "TERMINAL_PLAN_STATUSES",
    "TERMINAL_REVIEW_STATUSES",
    "UNIQUE_STRATEGY_PLAN_STATUSES",
    "business_today_yyyymmdd",
    # construction
    "candidate_stats",
    "load_index_universe_symbols_asof",
    "load_latest_universe_symbols",
    "load_model_cross_section",
    "load_score_frame",
    "resolve_plan_score_date",
    "round_lot_shares",
    "select_portfolio",
    "select_trade_plan_portfolio",
    "volatility_position_scale_map",
    # db
    "TradingCollections",
    "ensure_trading_indexes",
    "get_app_db",
    "get_data_db",
    "get_mongo_client",
    "get_trading_collections",
    "get_trading_db",
    "seed_default_strategy_configs",
    # lineage
    "CAPITAL_BASIS_ACCOUNT_EQUITY",
    "CAPITAL_BASIS_FIXED_NOTIONAL",
    "CAPITAL_BASIS_OPTIONS",
    "CAPITAL_BASIS_ROLLING_PAPER",
    "CapitalBaselineError",
    "LIVE_BASELINE_STALE_SECONDS",
    "add_excluded_symbols",
    "apply_manual_plan_lineage_effects",
    "get_lineage_control",
    "get_lineage_excluded_symbols",
    "is_lineage_paused",
    "latest_lineage_position_snapshot",
    "latest_plan_position_snapshot",
    "latest_position_snapshot",
    "pause_lineage",
    "position_map",
    "reconstruct_live_lineage_baseline",
    "resolve_capital_baseline",
    "resume_lineage",
    # maintenance
    "backfill_plan_items_from_lineage",
    # market_data
    "latest_close_prices_on_or_before",
    "latest_prices",
    "paper_execution_prices",
    # plans
    "build_force_plan_id",
    "build_manual_plan_id",
    "determine_plan_type",
    "generate_manual_rebalance_plan",
    "generate_trade_plan",
    "maybe_auto_approve_paper_plan",
    "normalize_manual_target_shares",
    "resolve_paper_execution_mode",
    "select_same_day_strategy_plan_for_generation",
    "supersede_stale_active_strategy_plans",
    "sweep_stale_zero_fill_plans",
    # reviews
    "approve_plan",
    "reject_plan",
    "review_plan",
    # risk
    "rerun_plan_risk_review",
    "review_holdings",
    "review_symbols",
    # scheduler
    "apply_due_activations",
    "enqueue_auto_generation_tasks",
    "find_active_plan_subscriptions",
    # research/live parity
    "build_research_live_parity",
    "suggest_effective_date",
    # trailing_stop
    "TRAILING_STOP_ORIGIN",
    "TRAILING_STOP_PLAN_TYPE",
    "LINEAGE_TRAILING_STOP_FIELD",
    "check_trailing_stop",
    "enqueue_trailing_stop_tasks",
    "fetch_trailing_stop_price_rows",
    "get_trailing_stop_cycle_exclusions",
    "is_trailing_stop_monitor_verbose",
    "latest_trailing_stop_run",
    "list_trailing_stop_runs",
    "lineage_has_explicit_trailing_stop_setting",
    "process_trailing_stop_task",
    "reclaim_stale_trailing_stop_tasks",
    "resolve_trailing_stop_pct",
    "resolve_trailing_stop_pct_for_lineage",
    "run_trailing_stop_monitor",
    "seed_lineage_trailing_stop_from_config",
    "set_lineage_trailing_stop_pct",
    "trailing_stop_pct_for_snapshot",
    # strategies
    "build_user_plan_id",
    "effective_strategy_config",
    "load_default_param_preset",
    "load_strategy_config",
    "normalized_lineage_params",
    "normalized_plan_params",
    "plan_params_hash",
    "selected_preset_id_for_plan",
    "strategy_display_name",
    # shared score contract
    "compute_score_series",
    "resolve_score_date",
]

_PAPER_EXPORTS = frozenset(
    {
        "apply_approved_paper_plans",
        "apply_paper_trade_plan",
        "mark_paper_equity",
        "mark_plan_equity_for_date",
        "sync_paper_executed_plan_statuses",
    }
)


def __getattr__(name: str) -> Any:
    if name in _PAPER_EXPORTS:
        from portfolio import paper as _paper

        value = getattr(_paper, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_PAPER_EXPORTS) | {"ASCENDING", "DESCENDING"})
