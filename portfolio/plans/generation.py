"""Strategy and manual trade-plan generation."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
from pymongo import DESCENDING
from stock_data_access.board_lots import (  # pyright: ignore[reportMissingImports]
    is_star_market,
    normalize_board_target_shares,
    round_board_lot_shares,
)

from portfolio.ai_risk_review import review_plan_items
from portfolio.calendar import (
    compute_next_rebalance_date,
    next_trading_date,
    normalize_date,
    trading_dates_between,
)
from portfolio.constants import (
    PLAN_MUTABLE_STATUSES,
    RECORD_KIND_OBSERVATION,
    RECORD_KIND_TRADE_PLAN,
    TERMINAL_PLAN_STATUSES,
    business_today_yyyymmdd,
)
from portfolio.construction import (
    apply_regime_cash_to_selection,
    candidate_stats,
    entry_vol_blocked_symbols,
    load_model_score_frame,
    load_score_frame,
    resolve_plan_score_date,
    select_portfolio,
    select_trade_plan_portfolio,
    volatility_position_scale_map,
)
from portfolio.db import (
    TradingCollections,
    ensure_trading_indexes,
    get_app_db,
    get_data_db,
    get_ml_db,
    get_mongo_client,
    get_trading_collections,
    get_trading_db,
)
from portfolio.lineage import (
    CAPITAL_BASIS_ROLLING_PAPER,
    _lineage_has_live_fills,
    _symbol_key,
    get_lineage_excluded_symbols,
    resolve_capital_baseline,
)
from portfolio.market_data import latest_close_prices_on_or_before
from portfolio.plans.approval import _finalize_saved_plan, resolve_paper_execution_mode
from portfolio.plans.supersede import (
    select_same_day_strategy_plan_for_generation,
    supersede_stale_active_strategy_plans,
    sweep_stale_zero_fill_plans,
)
from portfolio.record_kind import (
    strategy_generated_record_mongo_filter,
    strategy_rebalance_mongo_filter,
)
from portfolio.regime_cash import resolve_live_regime_cash
from shared.regime_label import coerce_optional_bool
from portfolio.strategies import (
    build_user_plan_id,
    effective_strategy_config,
    load_default_param_preset,
    load_strategy_config,
    normalized_plan_params,
    plan_params_hash,
    selected_preset_id_for_plan,
    strategy_display_name,
)
from portfolio.trailing_stop import (
    get_trailing_stop_cycle_exclusions,
    resolve_trailing_stop_pct_for_lineage,
    seed_lineage_trailing_stop_from_config,
    trailing_stop_pct_for_snapshot,
)


def _score_rows_by_symbol(frame: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
    if frame.empty or "symbol" not in frame.columns:
        return {}
    rows: Dict[str, Dict[str, Any]] = {}
    for row in frame.to_dict(orient="records"):
        symbol = row.get("symbol")
        key = _symbol_key(symbol)
        if key:
            rows[key] = row
    return rows

def determine_plan_type(
    collections: TradingCollections,
    data_db,
    strategy_template_id: str,
    base_date: str,
    config: Dict[str, Any],
    mode: str,
    user_id: Optional[str] = None,
    params_hash: Optional[str] = None,
) -> Tuple[str, bool, Optional[str], Optional[str]]:
    query: Dict[str, Any] = strategy_rebalance_mongo_filter(
        strategy_template_id=strategy_template_id,
        base_date={"$lt": base_date},
        status={"$nin": ["rejected", "cancelled", "failed"]},
    )
    if user_id:
        query["user_id"] = user_id
    if params_hash:
        query["params_hash"] = params_hash
    latest = collections.trade_plans.find_one(
        query,
        sort=[("base_date", DESCENDING)],
        projection={"_id": 0, "base_date": 1, "plan_id": 1},
    )
    if not latest:
        if mode in {"monitor", "rebalance"}:
            return mode, mode == "rebalance", None, None
        return "rebalance", True, None, None
    last_date = str(latest["base_date"])
    last_plan_id = str(latest.get("plan_id") or "") or None
    if mode in {"monitor", "rebalance"}:
        return mode, mode == "rebalance", last_date, last_plan_id
    elapsed = len(trading_dates_between(data_db, last_date, base_date)) - 1
    if elapsed >= int(config.get("rebalance_days", 20)):
        return "rebalance", True, last_date, last_plan_id
    return "monitor", False, last_date, last_plan_id


def normalize_manual_target_shares(current_shares: int, requested_target: int, lot_size: int) -> Tuple[int, List[str]]:
    """Snap a manual target to tradable lots; clearing to zero keeps full exit."""
    current = max(0, int(current_shares or 0))
    requested = max(0, int(requested_target or 0))
    lot = max(1, int(lot_size or 1))
    blockers: List[str] = []
    if requested == current:
        return current, blockers
    if requested == 0:
        return 0, blockers
    delta = requested - current
    if delta > 0:
        rounded_delta = (delta // lot) * lot
        if rounded_delta <= 0:
            blockers.append("below_lot_size")
            return current, blockers
        return current + rounded_delta, blockers
    sell_qty = (abs(delta) // lot) * lot
    if sell_qty <= 0:
        blockers.append("below_lot_size")
        return current, blockers
    return current - sell_qty, blockers


def build_manual_plan_id(strategy_template_id: str, base_date: str, user_id: Optional[str], params_hash: Optional[str]) -> str:
    # Seconds-level stamp plus a short random suffix so multiple manual plans in the
    # same second (and same lineage) never collide on the unique plan_id index.
    stamp = datetime.utcnow().strftime("%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"{build_user_plan_id(strategy_template_id, base_date, user_id, params_hash)}_manual_{stamp}{suffix}"


def build_force_plan_id(strategy_template_id: str, base_date: str, user_id: Optional[str], params_hash: Optional[str]) -> str:
    # Force regeneration after a terminal plan should preserve lineage but create
    # an auditable plan record instead of rewriting the cancelled/superseded one.
    stamp = datetime.utcnow().strftime("%H%M%S")
    suffix = uuid.uuid4().hex[:6]
    return f"{build_user_plan_id(strategy_template_id, base_date, user_id, params_hash)}_force_{stamp}{suffix}"


def generate_trade_plan(
    *,
    strategy_template_id: str,
    base_date: str,
    mode: str = "auto",
    user_id: Optional[str] = None,
    app_db=None,
    data_db=None,
    ml_db=None,
    trading_collections: Optional[TradingCollections] = None,
    force: bool = False,
    params_override: Optional[Dict[str, Any]] = None,
    preset_id: Optional[str] = None,
    excluded_symbols: Optional[Iterable[str]] = None,
    run_risk_review: bool = True,
    exact_override: bool = False,
    expected_params_hash: Optional[str] = None,
    write: bool = True,
) -> Dict[str, Any]:
    base_date = normalize_date(base_date) or business_today_yyyymmdd()
    # User-rejected symbols are excluded from target selection (and, if currently
    # held, are sold off below). They are intentionally NOT part of params_hash so
    # regenerating keeps the same plan_id and replaces the plan in place.
    caller_excluded = sorted({str(s) for s in (excluded_symbols or []) if s})
    client = None
    if app_db is None or data_db is None or trading_collections is None:
        client = get_mongo_client()
    app_db = get_app_db(client) if app_db is None else app_db
    data_db = get_data_db(client) if data_db is None else data_db
    ml_db = get_ml_db(client) if ml_db is None else ml_db
    cols = get_trading_collections(get_trading_db(client)) if trading_collections is None else trading_collections
    ensure_trading_indexes(cols)

    template = dict(load_strategy_config(cols, strategy_template_id))
    preset = cols.param_presets.find_one({"preset_id": preset_id}, {"_id": 0}) if preset_id else load_default_param_preset(cols, strategy_template_id)
    config = effective_strategy_config(template, preset, params_override, exact_override=exact_override)
    owner_id = user_id
    params_hash = plan_params_hash(config)
    # Lineage invariant. A caller that regenerates from an anchor plan passes the
    # anchor's hash; if the resolved params cannot reproduce it we must refuse
    # rather than write. Without this the plan is silently filed under whatever
    # lineage the fallback preset happens to hash to (see the force-rebalance
    # 8f73d17eb0ab -> 7cd2a2143a5f case), which puts live execution on the wrong
    # parameter set with no error anywhere.
    if expected_params_hash and params_hash != expected_params_hash:
        raise ValueError(
            "refusing to generate: resolved params_hash "
            f"{params_hash} does not match expected lineage {expected_params_hash}. "
            "The anchor plan's parameters could not be reconstructed exactly "
            "(a sparse params_snapshot and/or a preset that no longer matches it), "
            "so generating would silently create the plan under a different strategy lineage. "
            "Migrate the portfolio to a known parameter set explicitly instead."
        )
    lineage_excluded = get_lineage_excluded_symbols(cols, strategy_template_id, params_hash, owner_id)
    universe = str(config.get("universe_index") or "csi1000")
    score_date = resolve_plan_score_date(
        app_db["stock_scores"], ml_db, base_date, universe, config
    )
    model_pred_date = None
    model_id = str(config.get("model_id") or "").strip()
    if model_id and ml_db is not None:
        # Audit the actual prediction date used. ``score_date`` is the
        # cross-section date; a T-day plan can legitimately use a T-1 score.
        _, model_pred_date = load_model_score_frame(
            ml_db,
            model_id,
            score_date,
            column=str(config.get("score_column") or "model_score"),
        )
    regime_cash = resolve_live_regime_cash(
        config=config,
        data_db=data_db,
        score_date=score_date,
        universe_index=universe,
    )
    execute_date = next_trading_date(data_db, base_date)
    if not execute_date:
        raise ValueError(
            f"No next trading date available after base_date {base_date}; cannot set execute_date. "
            "Check TUSHARE_TOKEN in the portfolio worker or load future trading-calendar data."
        )
    plan_type, is_rebalance_day, previous_rebalance_date, previous_rebalance_plan_id = determine_plan_type(
        cols,
        data_db,
        strategy_template_id,
        base_date,
        config,
        mode,
        user_id=owner_id,
        params_hash=params_hash,
    )
    if write:
        seed_lineage_trailing_stop_from_config(cols, strategy_template_id, params_hash, owner_id, config)
    resolved_trailing_stop_pct = resolve_trailing_stop_pct_for_lineage(
        cols,
        strategy_template_id,
        params_hash,
        owner_id,
        config=config,
    )
    trailing_excluded = (
        get_trailing_stop_cycle_exclusions(
            cols,
            strategy_template_id=strategy_template_id,
            params_hash=params_hash,
            user_id=owner_id,
            since_date=previous_rebalance_date,
        )
        # On a rebalance day the cycle resets: stopped-out names may re-enter.
        # Keep exclusions only for mid-cycle monitor / force trades.
        if resolved_trailing_stop_pct is not None and previous_rebalance_date and not is_rebalance_day
        else []
    )
    excluded = sorted(set(caller_excluded) | set(lineage_excluded) | set(trailing_excluded))
    rebalance_days_cfg = int(config.get("rebalance_days", 20))
    elapsed_trading_days = len(trading_dates_between(data_db, previous_rebalance_date, base_date)) - 1 if previous_rebalance_date else 0
    # Project the next rebalance: if today rebalances, count from today;
    # otherwise from the previous rebalance. ``rebalance_days`` is measured in
    # trading days (weekends/holidays skipped), matching ``determine_plan_type``.
    rebalance_anchor = base_date if is_rebalance_day else (previous_rebalance_date or base_date)
    next_rebalance_date = compute_next_rebalance_date(data_db, rebalance_anchor, rebalance_days_cfg)
    # Same-day idempotency covers strategy observations too. An executable
    # monitor can be approved, so excluding record_kind=observation here would
    # let a retry overwrite its review state via the deterministic plan_id.
    existing_plan_query: Dict[str, Any] = strategy_generated_record_mongo_filter(
        strategy_template_id=strategy_template_id,
        params_hash=params_hash,
        base_date=base_date,
    )
    if owner_id:
        existing_plan_query["user_id"] = owner_id
    existing_rows = list(
        cols.trade_plans.find(
            existing_plan_query,
            {"_id": 0, "plan_id": 1, "status": 1, "created_at": 1, "updated_at": 1},
        ).sort("created_at", DESCENDING)
    )
    existing = select_same_day_strategy_plan_for_generation(existing_rows)
    existing_status = str((existing or {}).get("status") or "")
    if existing and force and existing_status in TERMINAL_PLAN_STATUSES:
        plan_id = build_force_plan_id(strategy_template_id, base_date, owner_id, params_hash)
    else:
        plan_id = str((existing or {}).get("plan_id") or build_user_plan_id(strategy_template_id, base_date, owner_id, params_hash))

    if existing and existing_status not in PLAN_MUTABLE_STATUSES and not force:
        raise ValueError(f"Plan {plan_id} is already {existing_status}; pass --force to regenerate")

    frame = load_score_frame(
        app_db["stock_scores"],
        score_date,
        universe,
        config,
        data_db=data_db,
        ml_db=ml_db,
    )
    score_rows = _score_rows_by_symbol(frame)
    stats = candidate_stats(
        app_db["stock_scores"],
        base_date,
        universe,
        config,
        data_db=data_db,
        ml_db=ml_db,
    ) if config.get("candidate_pool_enabled", True) else {}
    allow_stale_account = coerce_optional_bool(config.get("plan_on_stale_account"))
    if allow_stale_account is None:
        allow_stale_account = True
    baseline = resolve_capital_baseline(
        cols,
        app_db,
        data_db,
        plan_meta={
            "plan_id": plan_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": owner_id,
        },
        base_date=base_date,
        config=config,
        allow_stale_account=allow_stale_account,
    )
    baseline_stale = bool(baseline.get("baseline_stale"))
    baseline_error = baseline.get("baseline_error")
    current_positions = baseline["positions"]
    current_cash = float(baseline["cash"])
    equity = float(baseline["equity"])
    capital_basis = baseline["capital_basis"]
    effective_top_n = max(1, int(config.get("top_n", 10)))
    target_capital = equity * (1 - float(config.get("cash_buffer", 0.03)))
    target_amount = target_capital / effective_top_n
    pool_config = {**config, "top_n": effective_top_n * max(1, int(config.get("candidate_pool_multiplier", 3)))}
    candidate_pool = select_portfolio(frame, pool_config).reset_index(drop=True)
    pool_symbols = list(candidate_pool["symbol"].dropna()) if "symbol" in candidate_pool.columns else []
    price_symbols = sorted(set(pool_symbols + list(current_positions.keys())))
    prices, price_dates = latest_close_prices_on_or_before(data_db, price_symbols, base_date)
    entry_vol_cap = config.get("entry_vol_cap")
    if entry_vol_cap is not None:
        blocked_symbols = entry_vol_blocked_symbols(
            data_db,
            pool_symbols,
            base_date,
            max_vol_annual=float(entry_vol_cap),
        )
        if blocked_symbols:
            frame = frame[~frame["symbol"].isin(blocked_symbols)].reset_index(drop=True)
    selected = select_trade_plan_portfolio(frame, config, prices, target_amount, excluded_symbols=excluded).reset_index(drop=True)
    selected = apply_regime_cash_to_selection(selected, str(regime_cash.get("action") or "")).reset_index(drop=True)
    lot_size = int(config.get("lot_size", 100))

    # Trading is allowed on rebalance days, or on monitor days only when the
    # strategy explicitly opts into intra-cycle drift trading. Otherwise monitor
    # plans surface drift (target vs current) but carry no executable delta.
    monitor_can_trade = bool(config.get("monitor_can_trade", False))
    trading_enabled = bool(is_rebalance_day) or monitor_can_trade
    if str(regime_cash.get("action")) == "cash" and any(
        int((pos or {}).get("shares", 0) or 0) > 0 for pos in current_positions.values()
    ):
        # Non-bull cash overlay must be able to flatten mid-cycle, like a risk exit.
        trading_enabled = True
    if baseline_stale:
        # Generation is advisory until the brokerage snapshot is fresh again.
        # The plan still shows the target/drift, but no executable delta.
        trading_enabled = False

    now = datetime.utcnow()
    superseded_plan_ids = []
    if write and is_rebalance_day and trading_enabled and not existing:
        superseded_plan_ids = supersede_stale_active_strategy_plans(
            cols,
            app_db=app_db,
            strategy_template_id=strategy_template_id,
            params_hash=params_hash,
            user_id=owner_id,
            base_date=base_date,
            replacement_plan_id=plan_id,
            now=now,
        )
    # Origin-agnostic, date-independent cleanup: any published live plan in this
    # lineage whose signals all settled with zero fills is a dead-end that
    # supersede (origin != manual, date-gated) never reaches. Runs on every
    # generation — including daily monitor days — so a stuck plan is expired
    # within a day even when nobody opens its detail page.
    if write:
        sweep_stale_zero_fill_plans(
            cols,
            app_db=app_db,
            strategy_template_id=strategy_template_id,
            params_hash=params_hash,
            user_id=owner_id,
            now=now,
        )
    items: List[Dict[str, Any]] = []
    target_symbols = set()
    vol_scaling_enabled = bool(config.get("volatility_position_scaling_enabled"))
    target_vol_annual = float(config.get("target_vol_annual") or 0.30)
    vol_scale_map: Dict[str, float] = {}
    if vol_scaling_enabled:
        vol_scale_map = volatility_position_scale_map(
            data_db,
            pool_symbols,
            base_date,
            target_vol_annual=target_vol_annual,
        )
    for rank, row in enumerate(selected.to_dict(orient="records"), start=1):
        symbol = row.get("symbol")
        target_symbols.add(symbol)
        price = prices.get(symbol)
        price_trade_date = price_dates.get(symbol)
        position_amount = target_amount
        vol_scale = vol_scale_map.get(symbol, 1.0) if vol_scaling_enabled else 1.0
        if vol_scaling_enabled and vol_scale < 1.0:
            position_amount = target_amount * vol_scale
        current_shares = int(current_positions.get(symbol, {}).get("shares", 0) or 0)
        target_shares = round_board_lot_shares(position_amount, price, lot_size, symbol)
        size_blockers: List[str] = []
        if is_star_market(symbol):
            # STAR (科创板) single orders must be >= 200 shares; bump/skip/liquidate.
            target_shares, size_blockers = normalize_board_target_shares(
                current_shares,
                target_shares,
                price,
                lot_size,
                symbol,
                available_cash=current_cash,
            )
        drift_delta = target_shares - current_shares
        # On no-trade monitor days the would-be trade is preserved as
        # ``drift_delta_shares`` for display, while the executable
        # ``delta_shares`` is zeroed so downstream execution skips the item.
        delta = drift_delta if trading_enabled else 0
        # If board-lot normalization resolves a too-small STAR order by keeping
        # the current position unchanged, the final hold item has no order to
        # block. Keep blockers only for real buy/sell/skip outcomes.
        blockers = [] if current_shares > 0 and drift_delta == 0 else list(size_blockers)
        if not price:
            blockers.append("missing_price")
        elif current_shares == 0 and target_shares == 0:
            blockers.append("target_below_lot_size")
        action = "skip"
        if delta > 0:
            action = "buy"
        elif delta < 0:
            action = "sell"
        elif current_shares > 0:
            action = "hold"
        stat = stats.get(symbol, {})
        items.append(
            {
                "plan_id": plan_id,
                "strategy_template_id": strategy_template_id,
                "user_id": owner_id,
                "params_hash": params_hash,
                "base_date": base_date,
                "execute_date": execute_date,
                "rank": rank,
                "symbol": symbol,
                "name": row.get("name", ""),
                "industry": row.get("industry", ""),
                "score_value": _safe_float(row.get("score_value")),
                "action": action,
                "current_shares": current_shares,
                "target_shares": target_shares,
                "delta_shares": delta,
                "drift_delta_shares": drift_delta,
                "target_weight": target_amount / equity if equity else None,
                "volatility_position_scale": vol_scale if vol_scaling_enabled else None,
                "estimated_price": price,
                "price_trade_date": price_trade_date,
                "estimated_amount": abs(delta) * price if price else None,
                "blockers": blockers,
                "candidate_appearances": stat.get("candidate_appearances", 0),
                "avg_candidate_rank": stat.get("avg_candidate_rank"),
                "last_candidate_rank": stat.get("last_candidate_rank"),
                "last_candidate_date": stat.get("last_candidate_date"),
                "created_at": now,
            }
        )

    for symbol, pos in current_positions.items():
        if symbol in target_symbols or int(pos.get("shares", 0) or 0) <= 0:
            continue
        score_row = score_rows.get(_symbol_key(symbol), {})
        held_shares = int(pos.get("shares", 0) or 0)
        drift_delta = -held_shares
        delta = drift_delta if trading_enabled else 0
        action = "sell" if delta < 0 else "hold"
        price = prices.get(symbol)
        items.append(
            {
                "plan_id": plan_id,
                "strategy_template_id": strategy_template_id,
                "user_id": owner_id,
                "params_hash": params_hash,
                "base_date": base_date,
                "execute_date": execute_date,
                "rank": None,
                "symbol": symbol,
                "name": pos.get("name") or score_row.get("name", ""),
                "industry": pos.get("industry") or score_row.get("industry", ""),
                "score_value": _safe_float(score_row.get("score_value")),
                "action": action,
                "current_shares": held_shares,
                "target_shares": 0,
                "delta_shares": delta,
                "drift_delta_shares": drift_delta,
                "target_weight": 0,
                "estimated_price": price,
                "price_trade_date": price_dates.get(symbol),
                "estimated_amount": abs(delta) * price if price and delta else None,
                "blockers": [],
                "created_at": now,
            }
        )

    # Rule-based AI risk-control pass (Layer 1): annotate buy/hold items with
    # event-style risks (recent surge / share unlock / holder reduction).
    # Advisory only; never raises so plan generation stays robust.
    ai_risk_summary: Dict[str, Any] = {"status": "skipped", "engine_version": None}
    if run_risk_review:
        try:
            ai_risk_summary = review_plan_items(data_db, items, base_date)
        except Exception as exc:  # pragma: no cover - defensive
            ai_risk_summary = {"status": "failed", "error": str(exc), "engine_version": None}

    buy_amount = sum(float(item.get("estimated_amount") or 0) for item in items if item.get("action") == "buy")
    sell_amount = sum(float(item.get("estimated_amount") or 0) for item in items if item.get("action") == "sell")
    turnover = (buy_amount + sell_amount) / equity if equity else None
    # Only trading-enabled plans become reviewable/executable. No-trade monitor
    # plans stay "generated" (advisory drift view) and are flagged non-executable
    # so reviewers and executors never turn drift into orders by mistake.
    status = "needs_review" if trading_enabled else "generated"
    if baseline_stale:
        non_executable_reason = "account_not_synced"
    else:
        non_executable_reason = None if trading_enabled else "monitor_no_trade"
    plan_meta = {
        "plan_id": plan_id,
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": owner_id,
    }
    paper_execution_mode = resolve_paper_execution_mode(config, app_db, cols, plan_meta)
    plan_doc = {
        "plan_id": plan_id,
        "strategy_template_id": strategy_template_id,
        "user_id": owner_id,
        "preset_id": selected_preset_id_for_plan(template, preset, config),
        "params_hash": params_hash,
        "params_snapshot": {
            **normalized_plan_params(config),
            **trailing_stop_pct_for_snapshot(resolved_trailing_stop_pct),
            **(
                {"regime_cash": True}
                if regime_cash.get("enabled")
                else ({"regime_cash": False} if "regime_cash" in config else {})
            ),
        },
        "params_override": params_override or {},
        "strategy_name": strategy_display_name(config, strategy_template_id),
        "base_date": base_date,
        "score_date": score_date,
        "model_pred_date": model_pred_date,
        "execute_date": execute_date,
        "plan_type": plan_type,
        "record_kind": (RECORD_KIND_OBSERVATION if plan_type == "monitor" else RECORD_KIND_TRADE_PLAN),
        # Strategy-generated plans are tagged so the partial unique index on
        # (user, strategy, params_hash, base_date) applies to them but not to
        # manual off-cycle plans (origin == "manual").
        "origin": "strategy",
        "is_rebalance_day": is_rebalance_day,
        "previous_rebalance_date": previous_rebalance_date,
        "previous_rebalance_plan_id": previous_rebalance_plan_id,
        "supersedes_plan_ids": superseded_plan_ids,
        "rebalance_days": rebalance_days_cfg,
        "rebalance_unit": "trading_days",
        "elapsed_trading_days_since_rebalance": elapsed_trading_days,
        "next_rebalance_date": next_rebalance_date,
        "monitor_can_trade": monitor_can_trade,
        "regime_cash": regime_cash,
        "capital_basis": capital_basis,
        "paper_execution_mode": paper_execution_mode,
        "trading_enabled": trading_enabled,
        "executable": trading_enabled,
        "non_executable_reason": non_executable_reason,
        "baseline_stale": baseline_stale,
        "baseline_error": baseline_error,
        "excluded_symbols": excluded,
        "status": status,
        "template_snapshot": {
            "strategy_template_id": strategy_template_id,
            "strategy_type": template.get("strategy_type", "template"),
            "name": strategy_display_name(template, strategy_template_id),
            "algorithm": template.get("algorithm"),
            "version": template.get("version"),
        },
        "summary": {
            "target_top_n": effective_top_n,
            "target_amount_per_position": target_amount,
            "selected_count": int(len(selected)),
            "buy_count": sum(1 for item in items if item.get("action") == "buy"),
            "sell_count": sum(1 for item in items if item.get("action") == "sell"),
            "hold_count": sum(1 for item in items if item.get("action") == "hold"),
            "skip_count": sum(1 for item in items if item.get("action") == "skip"),
            "estimated_buy_amount": buy_amount,
            "estimated_sell_amount": sell_amount,
            "estimated_turnover": turnover,
            "cash": current_cash,
            "equity": equity,
            "capital_basis": capital_basis,
            "baseline_source": baseline["source"],
            "baseline_equity": equity,
            "baseline_cash": current_cash,
            "baseline_securities_account_id": baseline.get("securities_account_id"),
            "baseline_synced_at": baseline.get("baseline_synced_at"),
            "baseline_stale": baseline_stale,
            "baseline_error": baseline_error,
            "account_positions_synced": bool(baseline.get("account_positions_synced")),
            "account_position_adjusted_symbols": baseline.get("account_position_adjusted_symbols") or [],
            "risk_flags": sorted({flag for item in items for flag in item.get("blockers", [])}),
            "ai_risk_summary": ai_risk_summary,
            "rebalance_days": rebalance_days_cfg,
            "rebalance_unit": "trading_days",
            "elapsed_trading_days": elapsed_trading_days,
            "next_rebalance_date": next_rebalance_date,
            "regime_cash_enabled": bool(regime_cash.get("enabled")),
            "regime_cash_action": regime_cash.get("action"),
            "regime_cash_label": regime_cash.get("label"),
            "regime_cash_note": regime_cash.get("note") if regime_cash.get("enabled") else None,
        },
        "updated_at": now,
        "created_at": now,
    }
    if not write:
        return {"plan": plan_doc, "items": items}
    replace_query: Dict[str, Any] = {"plan_id": plan_id}
    if owner_id:
        replace_query["user_id"] = owner_id
    cols.trade_plans.replace_one(replace_query, plan_doc, upsert=True)
    delete_items_query = {"plan_id": plan_id}
    if owner_id:
        delete_items_query["user_id"] = owner_id
    cols.trade_plan_items.delete_many(delete_items_query)
    if items:
        cols.trade_plan_items.insert_many(items)
    plan_doc = _finalize_saved_plan(cols, app_db, plan_id, plan_doc, config=config)
    return {"plan": plan_doc, "items": items}


def generate_manual_rebalance_plan(
    *,
    anchor_plan_id: str,
    base_date: Optional[str] = None,
    targets: Dict[str, int],
    user_id: Optional[str] = None,
    exclude_after: bool = False,
    app_db=None,
    data_db=None,
    ml_db=None,
    trading_collections: Optional[TradingCollections] = None,
    run_risk_review: bool = True,
) -> Dict[str, Any]:
    """Create an off-cycle manual rebalance plan from explicit per-symbol targets."""
    if not targets:
        raise ValueError("targets must include at least one symbol")
    client = None
    if app_db is None or data_db is None or trading_collections is None:
        client = get_mongo_client()
    app_db = get_app_db(client) if app_db is None else app_db
    data_db = get_data_db(client) if data_db is None else data_db
    ml_db = get_ml_db(client) if ml_db is None else ml_db
    cols = get_trading_collections(get_trading_db(client)) if trading_collections is None else trading_collections
    ensure_trading_indexes(cols)

    anchor_query: Dict[str, Any] = {"plan_id": anchor_plan_id}
    if user_id:
        anchor_query["user_id"] = user_id
    anchor = cols.trade_plans.find_one(anchor_query, {"_id": 0})
    if not anchor:
        raise ValueError(f"Anchor plan not found: {anchor_plan_id}")

    strategy_template_id = str(anchor.get("strategy_template_id") or "")
    params_hash = str(anchor.get("params_hash") or "")
    owner_id = user_id or anchor.get("user_id")
    config = dict(anchor.get("params_snapshot") or {})
    for key in ("capital_basis", "paper_execution_mode", "monitor_can_trade"):
        if anchor.get(key) is not None:
            config[key] = anchor[key]
    if str(anchor.get("status") or "") == "executed_paper":
        fill_meta = {
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": owner_id,
        }
        if app_db is not None and not _lineage_has_live_fills(app_db, cols, fill_meta):
            config["capital_basis"] = CAPITAL_BASIS_ROLLING_PAPER
    base_date = normalize_date(base_date) or normalize_date(anchor.get("base_date")) or business_today_yyyymmdd()
    execute_date = next_trading_date(data_db, base_date) or base_date
    lot_size = int(config.get("lot_size", 100))

    _, _, previous_rebalance_date, previous_rebalance_plan_id = determine_plan_type(
        cols,
        data_db,
        strategy_template_id,
        base_date,
        config,
        "auto",
        user_id=owner_id,
        params_hash=params_hash,
    )
    rebalance_days_cfg = int(config.get("rebalance_days", 20))
    elapsed_trading_days = len(trading_dates_between(data_db, previous_rebalance_date, base_date)) - 1 if previous_rebalance_date else 0
    next_rebalance_date = compute_next_rebalance_date(data_db, base_date, rebalance_days_cfg)

    plan_id = build_manual_plan_id(strategy_template_id, base_date, owner_id, params_hash)
    allow_stale_account = coerce_optional_bool(config.get("plan_on_stale_account"))
    if allow_stale_account is None:
        allow_stale_account = True
    baseline = resolve_capital_baseline(
        cols,
        app_db,
        data_db,
        plan_meta={
            "plan_id": plan_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": owner_id,
        },
        base_date=base_date,
        config=config,
        allow_stale_account=allow_stale_account,
    )
    baseline_stale = bool(baseline.get("baseline_stale"))
    baseline_error = baseline.get("baseline_error")
    current_positions = baseline["positions"]
    current_cash = float(baseline["cash"])
    equity = float(baseline["equity"])
    capital_basis = baseline["capital_basis"]

    symbols = sorted(set(current_positions.keys()) | {str(s) for s in targets.keys() if s})
    price_symbols = list(symbols)
    prices, price_dates = latest_close_prices_on_or_before(data_db, price_symbols, base_date)
    score_rows: Dict[str, Dict[str, Any]] = {}
    anchor_score_date = normalize_date(anchor.get("score_date"))
    universe = str(config.get("universe_index") or "csi1000")
    if anchor_score_date:
        score_rows = _score_rows_by_symbol(
            load_score_frame(
                app_db["stock_scores"],
                anchor_score_date,
                universe,
                config,
                data_db=data_db,
                ml_db=ml_db,
            )
        )

    now = datetime.utcnow()
    items: List[Dict[str, Any]] = []
    manual_exclude_symbols: List[str] = []
    estimated_buy = 0.0
    estimated_sell = 0.0

    # Resolve per-symbol deltas first, then settle cash sell-first: A-share
    # sell proceeds are usable for same-day buys (only withdrawal is T+1), so
    # credit every sell before checking buy affordability. A single symbol-order
    # pass would otherwise flag insufficient_cash on buys that the sells fund.
    prelim: List[Dict[str, Any]] = []
    for rank, symbol in enumerate(symbols, start=1):
        pos = current_positions.get(symbol, {})
        current_shares = int(pos.get("shares", 0) or 0)
        if symbol in targets:
            requested_target = int(targets[symbol])
        else:
            requested_target = current_shares
        price = prices.get(symbol)
        if is_star_market(symbol):
            # STAR (科创板) single orders must be >= 200 shares; bump/skip/liquidate.
            target_shares, lot_blockers = normalize_board_target_shares(
                current_shares,
                requested_target,
                price,
                lot_size,
                symbol,
                available_cash=current_cash,
            )
        else:
            target_shares, lot_blockers = normalize_manual_target_shares(current_shares, requested_target, lot_size)
        if target_shares < current_shares and exclude_after:
            manual_exclude_symbols.append(symbol)

        delta = target_shares - current_shares
        blockers = list(lot_blockers)
        if not price and delta != 0:
            blockers.append("missing_price")
        prelim.append(
            {
                "rank": rank,
                "symbol": symbol,
                "pos": pos,
                "current_shares": current_shares,
                "target_shares": target_shares,
                "delta": delta,
                "price": price,
                "blockers": blockers,
            }
        )

    # Credit sell proceeds up front (only when priced)...
    for entry in prelim:
        if entry["delta"] < 0 and entry["price"]:
            current_cash += abs(entry["delta"]) * float(entry["price"])
    # ...then debit buys against post-sell cash, flagging genuine shortfalls.
    for entry in prelim:
        if entry["delta"] > 0:
            buy_cost = entry["delta"] * float(entry["price"] or 0)
            if buy_cost > current_cash + 1e-6:
                entry["blockers"].append("insufficient_cash")
            else:
                current_cash -= buy_cost

    for entry in prelim:
        rank = entry["rank"]
        symbol = entry["symbol"]
        pos = entry["pos"]
        score_row = score_rows.get(_symbol_key(symbol), {})
        current_shares = entry["current_shares"]
        target_shares = entry["target_shares"]
        delta = entry["delta"]
        price = entry["price"]
        blockers = entry["blockers"]
        action = "skip"
        if delta > 0:
            action = "buy"
            estimated_buy += abs(delta) * float(price or 0)
        elif delta < 0:
            action = "sell"
            estimated_sell += abs(delta) * float(price or 0)
        elif current_shares > 0:
            action = "hold"

        items.append(
            {
                "plan_id": plan_id,
                "strategy_template_id": strategy_template_id,
                "user_id": owner_id,
                "params_hash": params_hash,
                "base_date": base_date,
                "execute_date": execute_date,
                "rank": rank,
                "symbol": symbol,
                "name": pos.get("name") or score_row.get("name", ""),
                "industry": pos.get("industry") or score_row.get("industry", ""),
                "score_value": _safe_float(score_row.get("score_value")),
                "action": action,
                "current_shares": current_shares,
                "target_shares": target_shares,
                "delta_shares": delta,
                "drift_delta_shares": delta,
                "target_weight": (target_shares * float(price or 0) / equity) if equity and price else None,
                "estimated_price": price,
                "price_trade_date": price_dates.get(symbol),
                "estimated_amount": abs(delta) * float(price or 0) if price else None,
                "blockers": blockers,
                "created_at": now,
            }
        )

    if not any(int(item.get("delta_shares", 0) or 0) != 0 for item in items):
        raise ValueError("manual rebalance produced no executable changes after lot-size normalization")

    # A change that still carries blockers (insufficient cash / missing price /
    # below-lot-size) cannot be executed faithfully, so the whole plan is held back
    # from the auto-approval / review path instead of silently producing orders that
    # would fail. The frontend surfaces these blockers on the confirmation step.
    blocking_reasons = sorted({reason for item in items if int(item.get("delta_shares", 0) or 0) != 0 for reason in (item.get("blockers") or [])})
    if baseline_stale:
        blocking_reasons = sorted(set(blocking_reasons) | {"account_not_synced"})
    has_blockers = bool(blocking_reasons)

    ai_risk_summary: Dict[str, Any] = {"status": "skipped", "engine_version": None}
    if run_risk_review:
        try:
            ai_risk_summary = review_plan_items(data_db, items, base_date)
        except Exception as exc:  # pragma: no cover
            ai_risk_summary = {"status": "failed", "error": str(exc), "engine_version": None}

    paper_execution_mode = resolve_paper_execution_mode(
        config,
        app_db,
        cols,
        {
            "plan_id": plan_id,
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": owner_id,
        },
    )
    plan_doc = {
        "plan_id": plan_id,
        "strategy_template_id": strategy_template_id,
        "user_id": owner_id,
        "preset_id": anchor.get("preset_id"),
        "params_hash": params_hash,
        "params_snapshot": normalized_plan_params(config),
        "params_override": anchor.get("params_override") or {},
        "strategy_name": anchor.get("strategy_name") or strategy_display_name(config, strategy_template_id),
        "base_date": base_date,
        "score_date": anchor.get("score_date"),
        "execute_date": execute_date,
        "plan_type": "rebalance",
        "record_kind": RECORD_KIND_TRADE_PLAN,
        "origin": "manual",
        "is_rebalance_day": True,
        "previous_rebalance_date": previous_rebalance_date,
        "previous_rebalance_plan_id": previous_rebalance_plan_id,
        "rebalance_days": rebalance_days_cfg,
        "rebalance_unit": "trading_days",
        "elapsed_trading_days_since_rebalance": elapsed_trading_days,
        "next_rebalance_date": next_rebalance_date,
        "monitor_can_trade": False,
        "capital_basis": capital_basis,
        "paper_execution_mode": paper_execution_mode,
        "trading_enabled": not has_blockers,
        "executable": not has_blockers,
        "non_executable_reason": ("blocked:" + ",".join(blocking_reasons)) if has_blockers else None,
        "baseline_stale": baseline_stale,
        "baseline_error": baseline_error,
        "exclude_after": bool(exclude_after),
        "manual_exclude_symbols": sorted(set(manual_exclude_symbols)),
        "excluded_symbols": get_lineage_excluded_symbols(cols, strategy_template_id, params_hash, owner_id),
        "status": "generated" if has_blockers else "needs_review",
        "template_snapshot": anchor.get("template_snapshot") or {},
        "summary": {
            "buy_count": sum(1 for item in items if item.get("action") == "buy"),
            "sell_count": sum(1 for item in items if item.get("action") == "sell"),
            "hold_count": sum(1 for item in items if item.get("action") == "hold"),
            "skip_count": sum(1 for item in items if item.get("action") == "skip"),
            "estimated_buy_amount": estimated_buy,
            "estimated_sell_amount": estimated_sell,
            "estimated_turnover": (estimated_buy + estimated_sell) / equity if equity else None,
            "cash": float(baseline["cash"]),
            "equity": equity,
            "capital_basis": capital_basis,
            "baseline_source": baseline["source"],
            "baseline_equity": equity,
            "baseline_cash": float(baseline["cash"]),
            "baseline_securities_account_id": baseline.get("securities_account_id"),
            "baseline_synced_at": baseline.get("baseline_synced_at"),
            "baseline_stale": baseline_stale,
            "baseline_error": baseline_error,
            "risk_flags": sorted({flag for item in items for flag in item.get("blockers", [])}),
            "ai_risk_summary": ai_risk_summary,
            "rebalance_days": rebalance_days_cfg,
            "rebalance_unit": "trading_days",
            "elapsed_trading_days": elapsed_trading_days,
            "next_rebalance_date": next_rebalance_date,
            "manual_target_count": len(targets),
        },
        "updated_at": now,
        "created_at": now,
    }
    cols.trade_plans.insert_one(plan_doc)
    cols.trade_plan_items.insert_many(items)
    plan_doc = _finalize_saved_plan(cols, app_db, plan_id, plan_doc, config=config)
    return {"plan": plan_doc, "items": items}


# Statuses that prove a user has committed to a (strategy, params_hash) combo and
# therefore wants the daily auto cadence maintained for it.


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None
