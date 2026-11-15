"""Trade plan generation, supersede, and approval."""

from portfolio.plans.approval import (
    maybe_auto_approve_paper_plan,
    resolve_paper_execution_mode,
)
from portfolio.plans.generation import (
    build_force_plan_id,
    build_manual_plan_id,
    determine_plan_type,
    generate_manual_rebalance_plan,
    generate_trade_plan,
    normalize_manual_target_shares,
)
from portfolio.plans.supersede import (
    select_same_day_strategy_plan_for_generation,
    supersede_stale_active_strategy_plans,
    sweep_stale_zero_fill_plans,
)

__all__ = [
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
]
