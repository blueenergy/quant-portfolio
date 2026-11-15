"""Portfolio-domain constants and small time helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Tuple
from zoneinfo import ZoneInfo

CN_TZ = ZoneInfo("Asia/Shanghai")


def business_today_yyyymmdd() -> str:
    return datetime.now(CN_TZ).strftime("%Y%m%d")


GROWTH_CYCLE_PARAMETER_SCHEMA: Dict[str, Dict[str, Any]] = {
    "universe_index": {"type": "enum", "options": ["hs300", "a500", "csi500", "csi1000", "csi2000", "star50"]},
    "index_benchmark_symbol": {"type": "string"},
    "growth_weight": {"type": "number", "min": 0, "max": 1},
    "cycle_weight": {"type": "number", "min": 0, "max": 1},
    # ``weighted_score`` recipes: dimension -> weight over the six atomic
    # dimensions (growth/cycle/value/fundamental/technical/money_flow).
    "score_weights": {"type": "object", "nullable": True},
    "top_n": {"type": "integer", "min": 1},
    # Measured in *trading days* (weekends/holidays skipped), not calendar days:
    # the portfolio rebalances once this many trading days have elapsed since the
    # previous rebalance. See ``determine_plan_type`` / ``compute_next_rebalance_date``.
    "rebalance_days": {"type": "integer", "min": 1},
    "construction_mode": {"type": "enum", "options": ["top_n", "industry_capped", "industry_neutral"]},
    "max_industry_weight": {"type": "number", "min": 0, "max": 1, "nullable": True},
    # CSI1000-only: bull → full book, non-bull → cash. Explicit true on any
    # other universe is rejected. Unset auto-enables for csi1000.
    "regime_cash": {"type": "boolean"},
    "regime_rule": {
        "type": "enum",
        "options": [
            "k_slow_anchor",
            "k_slow_anchor_with_vol_and_atr",
            "k_slow_anchor_with_volume",
            "k_slow_anchor_with_volatility",
        ],
        "nullable": True,
    },
    "industry_l1": {"type": "string", "nullable": True},
    "lot_size": {"type": "integer", "min": 1},
    "transaction_cost": {"type": "number", "min": 0},
    "buy_commission_rate": {"type": "number", "min": 0, "nullable": True},
    "sell_commission_rate": {"type": "number", "min": 0, "nullable": True},
    "min_commission": {"type": "number", "min": 0, "nullable": True},
    "stamp_tax_rate": {"type": "number", "min": 0, "nullable": True},
    "transfer_fee_rate": {"type": "number", "min": 0, "nullable": True},
    "execution_price": {"type": "enum", "options": ["next_open"]},
    "cash_buffer": {"type": "number", "min": 0, "max": 1},
    "initial_capital": {"type": "number", "min": 0},
    # Peak-high trailing stop: close <= peak*(1-pct) exits full position (live monitor).
    # Excluded from ``PLAN_PARAM_KEYS`` so toggling it never forks the plan lineage;
    # authoritative runtime value lives on ``portfolio_lineage_controls``.
    "trailing_stop_pct": {"type": "number", "min": 0, "max": 1, "nullable": True},
    "candidate_pool_enabled": {"type": "boolean"},
    "candidate_pool_multiplier": {"type": "integer", "min": 1},
    "min_buy_lots": {"type": "integer", "min": 1},
    # When false (default) non-rebalance ("monitor") days only display portfolio
    # drift and never produce executable trades; the portfolio is rebalanced
    # strictly every ``rebalance_days``. Set true to allow intra-cycle drift
    # trading. Intentionally excluded from ``PLAN_PARAM_KEYS`` so toggling it
    # does not fork the plan/position lineage.
    "monitor_can_trade": {"type": "boolean"},
    # Capital baseline used to size each rebalance. Defaults to account-scoped equity;
    # set ``rolling_paper`` explicitly to inherit the paper snapshot.
    # intentionally excluded from ``PLAN_PARAM_KEYS`` so switching it never forks
    # the plan/position lineage.
    "capital_basis": {"type": "enum", "options": ["account_equity", "rolling_paper", "fixed_notional"], "nullable": True},
    # Paper rebalance approval policy. Default ``manual_review``; set ``auto_shadow``
    # explicitly to skip human review for paper-only shadow runs.
    # Excluded from ``PLAN_PARAM_KEYS`` so Plan B can toggle without forking lineage.
    "paper_execution_mode": {"type": "enum", "options": ["auto_shadow", "manual_review"], "nullable": True},
    # Optional position sizing: scale per-symbol target notional by target_vol / stock_vol.
    # Excluded from ``PLAN_PARAM_KEYS`` (operational toggle, not lineage fork).
    "volatility_position_scaling_enabled": {"type": "boolean"},
    "target_vol_annual": {"type": "number", "min": 0, "max": 2, "nullable": True},
}


PAPER_EXECUTION_MODE_AUTO_SHADOW = "auto_shadow"


PAPER_EXECUTION_MODE_MANUAL_REVIEW = "manual_review"


PAPER_EXECUTION_MODES = (PAPER_EXECUTION_MODE_AUTO_SHADOW, PAPER_EXECUTION_MODE_MANUAL_REVIEW)


AUTO_PAPER_APPROVE_REVIEWER = "system:paper_auto_shadow"


UNIQUE_STRATEGY_PLAN_STATUSES = [
    "generated",
    "needs_review",
    "approved",
    "executed_paper",
    "partially_executed",
    "executed_live",
]


DEFAULT_STRATEGY_TEMPLATES: Tuple[Dict[str, Any], ...] = (
    {
        "strategy_template_id": "growth_cycle_index_enhanced",
        "strategy_type": "template",
        "name": "成长周期指数增强",
        "algorithm": "growth_cycle_index_enhanced",
        "score_type": "growth_cycle_weighted",
        "parameter_schema": GROWTH_CYCLE_PARAMETER_SCHEMA,
        "status": "enabled",
        "version": 1,
    },
)

# Authoritative record-kind taxonomy. Canonical source / rationale lives in
# quantFinance/services/portfolio_record_kind.py; mirrored here because the
# worker is a separate repo. The worker only ever writes strategy-generated
# rebalance plans and monitor observations (never external ledger events).


RECORD_KIND_TRADE_PLAN = "trade_plan"


RECORD_KIND_OBSERVATION = "observation"


PLAN_MUTABLE_STATUSES = {"draft", "generated", "needs_review"}


ACTIVE_PLAN_STATUSES = {"needs_review", "approved"}


SAME_DAY_PLAN_REUSE_STATUSES = PLAN_MUTABLE_STATUSES | ACTIVE_PLAN_STATUSES


TERMINAL_PLAN_STATUSES = {
    "cancelled",
    "executed_live",
    "executed_paper",
    "expired",
    "partially_executed",
    "rejected",
    "superseded",
}


TERMINAL_REVIEW_STATUSES = {"approved", "rejected"}


TERMINAL_LIVE_SIGNAL_STATUSES = {"cancelled", "canceled", "filled", "rejected", "failed", "partial_cancelled", "expired"}


from shared.strategies import PLAN_PARAM_KEYS, PLAN_SNAPSHOT_PARAM_KEYS  # noqa: E402
