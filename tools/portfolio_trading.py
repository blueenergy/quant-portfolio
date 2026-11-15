#!/usr/bin/env python3
"""Backward-compatible facade for the portfolio package.

Implementation lives in ``portfolio.*``. Existing
``from tools.portfolio_trading import ...`` call sites keep working here.
"""

from __future__ import annotations

from portfolio import ASCENDING, DESCENDING  # noqa: F401
from portfolio.calendar import (  # noqa: F401
    compute_next_rebalance_date,
    forward_trading_dates,
    latest_trading_date_on_or_before,
    next_trading_date,
    normalize_date,
    trading_dates_between,
)
from portfolio.constants import (  # noqa: F401
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
from portfolio.construction import (  # noqa: F401
    candidate_stats,
    load_latest_universe_symbols,
    load_score_frame,
    round_lot_shares,
    select_portfolio,
    select_trade_plan_portfolio,
    volatility_position_scale_map,
)
from portfolio.db import (  # noqa: F401
    TradingCollections,
    ensure_trading_indexes,
    get_app_db,
    get_data_db,
    get_mongo_client,
    get_trading_collections,
    get_trading_db,
    seed_default_strategy_configs,
)
from portfolio.lineage import (  # noqa: F401
    CAPITAL_BASIS_ACCOUNT_EQUITY,
    CAPITAL_BASIS_FIXED_NOTIONAL,
    CAPITAL_BASIS_OPTIONS,
    CAPITAL_BASIS_ROLLING_PAPER,
    CapitalBaselineError,
    LIVE_BASELINE_STALE_SECONDS,
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
from portfolio.market_data import (  # noqa: F401
    latest_close_prices_on_or_before,
    latest_prices,
    paper_execution_prices,
)
from portfolio.reviews import approve_plan, reject_plan, review_plan  # noqa: F401
from portfolio.risk import rerun_plan_risk_review, review_holdings, review_symbols  # noqa: F401
from portfolio.strategies import (  # noqa: F401
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
from portfolio.plans import (  # noqa: F401
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
from portfolio.scheduler import (  # noqa: F401
    enqueue_auto_generation_tasks,
    find_active_plan_subscriptions,
)
from portfolio.trailing_stop import (  # noqa: F401
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
from portfolio.maintenance import backfill_plan_items_from_lineage  # noqa: F401
from portfolio.paper import *  # noqa: F403
from shared.scores import compute_score_series, resolve_score_date
