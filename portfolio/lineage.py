"""Lineage controls, position snapshots, and capital baseline resolution."""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from pymongo import DESCENDING

from portfolio.db import TradingCollections
from portfolio.market_data import latest_close_prices_on_or_before


def _filled_quantity_from_signal(row: Dict[str, Any]) -> float:
    for key in ("filled_qty", "filled_size", "filled_quantity"):
        raw = row.get(key)
        if raw is None:
            continue
        try:
            qty = float(raw)
        except (TypeError, ValueError):
            continue
        if qty > 0:
            return qty
    return 1.0 if str(row.get("status") or "").lower() == "filled" else 0.0


def latest_position_snapshot(
    collections: TradingCollections,
    strategy_template_id: str,
    before_or_on: Optional[str] = None,
    user_id: Optional[str] = None,
    params_hash: Optional[str] = None,
    exclude_plan_id: Optional[str] = None,
) -> Dict[str, Any]:
    query: Dict[str, Any] = {"strategy_template_id": strategy_template_id}
    if user_id:
        query["user_id"] = user_id
    if params_hash:
        query["params_hash"] = params_hash
    if exclude_plan_id:
        query["plan_id"] = {"$ne": exclude_plan_id}
    if before_or_on:
        query["as_of_date"] = {"$lte": before_or_on}
    return collections.paper_positions.find_one(query, sort=[("as_of_date", DESCENDING)], projection={"_id": 0}) or {}


def latest_lineage_position_snapshot(
    collections: TradingCollections,
    plan: Dict[str, Any],
    before_or_on: Optional[str] = None,
    exclude_current_plan: bool = True,
) -> Dict[str, Any]:
    strategy_template_id = plan.get("strategy_template_id")
    params_hash = plan.get("params_hash")
    user_id = plan.get("user_id")
    plan_id = plan.get("plan_id")
    plan_ids: List[str] = []
    if params_hash:
        plan_query: Dict[str, Any] = {
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
        }
        if user_id:
            plan_query["user_id"] = user_id
        plan_ids = [str(row.get("plan_id")) for row in collections.trade_plans.find(plan_query, {"_id": 0, "plan_id": 1}) if row.get("plan_id")]
    if plan_id and str(plan_id) not in plan_ids:
        plan_ids.append(str(plan_id))

    candidates: List[Dict[str, Any]] = []
    if params_hash:
        snapshot = latest_position_snapshot(
            collections,
            str(strategy_template_id),
            before_or_on,
            user_id=user_id,
            params_hash=params_hash,
            exclude_plan_id=plan_id if exclude_current_plan else None,
        )
        if snapshot:
            candidates.append(snapshot)
    legacy_query: Dict[str, Any] = {"strategy_template_id": strategy_template_id, "plan_id": {"$in": plan_ids}}
    if user_id:
        legacy_query["user_id"] = user_id
    if plan_id and exclude_current_plan:
        legacy_query["plan_id"] = {"$in": [pid for pid in plan_ids if pid != plan_id]}
    if before_or_on:
        legacy_query["as_of_date"] = {"$lte": before_or_on}
    candidates.extend(collections.paper_positions.find(legacy_query, {"_id": 0}))
    if exclude_current_plan:
        candidates = [row for row in candidates if row.get("plan_id") != plan_id]
    if params_hash:
        # Never inherit from a snapshot that explicitly belongs to a different
        # lineage. ``params_hash is None`` snapshots are tolerated only because
        # legacy_query already scoped them to this lineage's plan_ids.
        lineage_ids = set(plan_ids)
        candidates = [row for row in candidates if (row.get("params_hash") in (None, params_hash)) or (str(row.get("plan_id")) in lineage_ids)]
    candidates.sort(key=lambda row: str(row.get("as_of_date") or ""), reverse=True)
    return candidates[0] if candidates else {}


def latest_plan_position_snapshot(
    collections: TradingCollections,
    plan_id: str,
    before_or_on: Optional[str] = None,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    query: Dict[str, Any] = {"plan_id": plan_id}
    if user_id:
        query["user_id"] = user_id
    if before_or_on:
        query["as_of_date"] = {"$lte": before_or_on}
    return collections.paper_positions.find_one(query, sort=[("as_of_date", DESCENDING)], projection={"_id": 0}) or {}


def position_map(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {row.get("symbol"): row for row in snapshot.get("positions", []) if row.get("symbol")}


def _int_value(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _symbol_key(value: Any) -> str:
    return str(value or "").split(".", 1)[0]


def _account_position_qty(pos: Dict[str, Any]) -> int:
    for field in ("qty", "quantity", "volume", "shares", "current_volume", "total_volume"):
        if pos.get(field) is not None:
            return _int_value(pos.get(field))
    return 0


def _account_positions_by_symbol(app_db, *, user_id: Optional[str], securities_account_id: str) -> Tuple[Dict[str, int], bool]:
    if not user_id or not securities_account_id:
        return {}, False
    rows = list(
        app_db["trader_positions"].find(
            {"user_id": user_id, "securities_account_id": securities_account_id},
            {
                "_id": 0,
                "symbol": 1,
                "qty": 1,
                "quantity": 1,
                "volume": 1,
                "shares": 1,
                "current_volume": 1,
                "total_volume": 1,
            },
        )
    )
    positions: Dict[str, int] = {}
    for row in rows:
        key = _symbol_key(row.get("symbol"))
        if not key:
            continue
        positions[key] = positions.get(key, 0) + max(0, _account_position_qty(row))
    return positions, bool(rows)


# Reuse the same staleness window the live-equity account snapshotter uses so a
# disconnected / not-yet-synced broker account never silently seeds a plan with
# wrong carried state. Overridable via env for ops.


LIVE_BASELINE_STALE_SECONDS = int(os.getenv("PORTFOLIO_LIVE_BASELINE_STALE_SECONDS", str(6 * 60 * 60)))


CAPITAL_BASIS_ACCOUNT_EQUITY = "account_equity"


CAPITAL_BASIS_ROLLING_PAPER = "rolling_paper"


CAPITAL_BASIS_FIXED_NOTIONAL = "fixed_notional"


CAPITAL_BASIS_OPTIONS = (
    CAPITAL_BASIS_ACCOUNT_EQUITY,
    CAPITAL_BASIS_ROLLING_PAPER,
    CAPITAL_BASIS_FIXED_NOTIONAL,
)


class CapitalBaselineError(ValueError):
    """Raised when a live capital baseline cannot be trusted (e.g. account not synced).

    The plan generation worker surfaces ``str(exc)`` as the task ``error_message``;
    messages here start with ``account_not_synced`` so operators can filter them.
    """


def _lineage_plan_ids(cols: TradingCollections, plan_meta: Dict[str, Any]) -> List[str]:
    """All plan_ids sharing this (strategy_template_id, params_hash, user_id) lineage."""
    strategy_template_id = plan_meta.get("strategy_template_id")
    params_hash = plan_meta.get("params_hash")
    user_id = plan_meta.get("user_id")
    plan_id = plan_meta.get("plan_id")
    ids: set = set()
    if params_hash:
        query: Dict[str, Any] = {"strategy_template_id": strategy_template_id, "params_hash": params_hash}
        if user_id:
            query["user_id"] = user_id
        for row in cols.trade_plans.find(query, {"_id": 0, "plan_id": 1}):
            if row.get("plan_id"):
                ids.add(str(row["plan_id"]))
    if plan_id:
        ids.add(str(plan_id))
    return sorted(ids)


def _lineage_has_live_signals(app_db, cols: TradingCollections, plan_meta: Dict[str, Any]) -> bool:
    plan_ids = _lineage_plan_ids(cols, plan_meta)
    if not plan_ids:
        return False
    return bool(app_db["trade_signals"].find_one({"plan_id": {"$in": plan_ids}, "mode": "live"}))


def _plan_has_live_signals(app_db, *, plan_id: str, user_id: Optional[str] = None) -> bool:
    query: Dict[str, Any] = {"plan_id": plan_id, "mode": "live"}
    if user_id:
        query["user_id"] = user_id
    return bool(app_db["trade_signals"].find_one(query, {"_id": 0, "plan_id": 1}))


def _execution_fill_quantity(execution: Dict[str, Any]) -> float:
    for key in ("filled_size", "quantity", "filled_qty"):
        raw = execution.get(key)
        if raw is None:
            continue
        try:
            qty = float(raw)
        except (TypeError, ValueError):
            continue
        if qty > 0:
            return qty
    return 0.0


def _lineage_has_live_fills(app_db, cols: TradingCollections, plan_meta: Dict[str, Any]) -> bool:
    """True when the lineage has at least one live fill in trade_executions."""
    plan_ids = _lineage_plan_ids(cols, plan_meta)
    if not plan_ids:
        return False
    user_id = plan_meta.get("user_id")
    signals = list(app_db["trade_signals"].find({"plan_id": {"$in": plan_ids}, "mode": "live"}))
    if user_id:
        signals = [row for row in signals if row.get("user_id") in (None, user_id)]
    order_ids = [row.get("order_id") for row in signals if row.get("order_id")]
    if not order_ids:
        return False
    executions = list(app_db["trade_executions"].find({"order_id": {"$in": order_ids}}))
    if user_id:
        executions = [row for row in executions if row.get("user_id") in (None, user_id)]
    return any(_execution_fill_quantity(row) > 0 for row in executions)


def _group_executions_by_order(executions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One representative execution per order_id (latest timestamp wins).

    Mirrors ``quantFinance.services.trade_ledger.group_executions_by_order`` so the
    generator's combo-scoped reconstruction matches the live-equity service口径.
    """
    grouped: Dict[str, Dict[str, Any]] = {}
    for doc in executions:
        order_id = doc.get("order_id") or str(doc.get("_id"))
        current = grouped.get(order_id)
        if (not current) or (doc.get("timestamp", 0) > current.get("timestamp", 0)):
            grouped[order_id] = doc
    return list(grouped.values())


def _reconstruct_symbol_position(trades: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Weighted-average-cost replay for one symbol; returns net qty, avg cost, cash flow.

    ``cash_flow`` is the signed change to combo cash: BUY consumes price*qty+fee,
    SELL adds price*qty-fee. Matches ``compute_realized_pnl_for_group`` field
    handling (filled_size / filled_price / total_fee fallbacks).
    """
    sorted_trades = sorted(trades, key=lambda t: t.get("timestamp", 0))
    qty = 0
    avg_cost = 0.0
    cash_flow = 0.0
    name = ""
    industry = ""
    for trade in sorted_trades:
        side = (trade.get("side") or trade.get("action") or "").upper()
        if side not in ("BUY", "SELL"):
            continue
        raw_qty = trade.get("filled_size") or trade.get("quantity") or trade.get("size") or 0
        raw_price = (
            trade.get("filled_price") or trade.get("traded_price") or trade.get("avg_price") or trade.get("price") or trade.get("target_price") or 0.0
        )
        raw_fee = trade.get("total_fee") if trade.get("total_fee") is not None else (trade.get("commission", 0.0) or 0.0)
        try:
            filled_qty = int(raw_qty)
            price = float(raw_price)
            fee = float(raw_fee or 0.0)
        except (TypeError, ValueError):
            continue
        if not filled_qty or not price:
            continue
        name = trade.get("name") or name
        industry = trade.get("industry") or industry
        if side == "BUY":
            total_cost = avg_cost * qty + price * filled_qty + fee
            qty += filled_qty
            if qty > 0:
                avg_cost = total_cost / qty
            cash_flow -= price * filled_qty + fee
        else:  # SELL
            sell_qty = min(filled_qty, qty) if qty > 0 else 0
            qty -= sell_qty
            cash_flow += price * filled_qty - fee
            if qty <= 0:
                qty = 0
                avg_cost = 0.0
    return {
        "net_quantity": qty,
        "avg_cost": round(avg_cost, 4) if qty > 0 else 0.0,
        "cash_flow": cash_flow,
        "name": name,
        "industry": industry,
    }


def _snapshot_age_seconds(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return time.time() - float(value)
    if isinstance(value, datetime):
        base = datetime.utcnow() if value.tzinfo is None else datetime.now(value.tzinfo)
        return (base - value).total_seconds()
    return None


def reconstruct_live_lineage_baseline(
    app_db,
    data_db,
    cols: TradingCollections,
    *,
    plan_meta: Dict[str, Any],
    base_date: str,
    initial_capital: float,
    stale_seconds: int = LIVE_BASELINE_STALE_SECONDS,
    allow_stale_account: bool = False,
) -> Dict[str, Any]:
    """Combo-scoped rolling baseline from the lineage's own live executions.

    Equity/cash/positions are derived purely from ``initial_capital`` + every live
    fill in this (strategy, params_hash, user) lineage + current close prices, so
    other strategies or manual holdings in the same brokerage account never leak
    in. The brokerage account snapshot is used only as a freshness gate: if there
    is carried live state but the account has no fresh ``trader_accounts`` snapshot,
    the default is to raise ``CapitalBaselineError``.

    ``allow_stale_account=True`` downgrades a stale/missing snapshot to an
    advisory baseline: plan generation still succeeds from the execution ledger,
    but the caller must mark the plan non-executable until a fresh snapshot
    arrives. Execution readiness is a separate gate from plan generation.
    """
    user_id = plan_meta.get("user_id")
    plan_ids = _lineage_plan_ids(cols, plan_meta)
    signals = list(app_db["trade_signals"].find({"plan_id": {"$in": plan_ids}, "mode": "live"})) if plan_ids else []
    if user_id:
        signals = [row for row in signals if row.get("user_id") in (None, user_id)]
    order_ids = [row.get("order_id") for row in signals if row.get("order_id")]
    account_ids = sorted({str(row.get("securities_account_id")) for row in signals if row.get("securities_account_id")})
    securities_account_id = account_ids[0] if account_ids else None

    executions: List[Dict[str, Any]] = []
    if order_ids:
        executions = list(app_db["trade_executions"].find({"order_id": {"$in": order_ids}}))
        if user_id:
            executions = [row for row in executions if row.get("user_id") in (None, user_id)]

    baseline_synced_at = None
    baseline_stale = False
    baseline_error = None
    if executions:
        if not securities_account_id:
            message = "account_not_synced: live executions without securities_account_id"
            if not allow_stale_account:
                raise CapitalBaselineError(message)
            baseline_stale = True
            baseline_error = message
        else:
            account = app_db["trader_accounts"].find_one(
                {"user_id": user_id, "securities_account_id": securities_account_id},
                sort=[("synced_at", -1)],
            )
            if not account:
                message = f"account_not_synced: no trader_accounts snapshot for {securities_account_id}"
                if not allow_stale_account:
                    raise CapitalBaselineError(message)
                baseline_stale = True
                baseline_error = message
            else:
                age = _snapshot_age_seconds(account.get("synced_at"))
                if age is None or age > stale_seconds:
                    message = (
                        f"account_not_synced: snapshot stale for {securities_account_id} "
                        f"(age={age})"
                    )
                    if not allow_stale_account:
                        raise CapitalBaselineError(message)
                    baseline_stale = True
                    baseline_error = message
                else:
                    baseline_synced_at = account.get("synced_at")

    grouped = _group_executions_by_order(executions)
    by_symbol: Dict[str, List[Dict[str, Any]]] = {}
    for row in grouped:
        symbol = row.get("symbol")
        if symbol:
            by_symbol.setdefault(str(symbol), []).append(row)

    positions: Dict[str, Dict[str, Any]] = {}
    cash = float(initial_capital)
    for symbol, trades in by_symbol.items():
        recon = _reconstruct_symbol_position(trades)
        cash += recon["cash_flow"]
        shares = int(recon["net_quantity"])
        if shares > 0:
            positions[symbol] = {
                "symbol": symbol,
                "name": recon.get("name", ""),
                "industry": recon.get("industry", ""),
                "shares": shares,
                "cost_basis": recon["avg_cost"],
            }

    account_positions_synced = False
    account_position_adjusted_symbols: List[str] = []
    if securities_account_id and not baseline_stale:
        account_positions, account_positions_synced = _account_positions_by_symbol(
            app_db,
            user_id=user_id,
            securities_account_id=securities_account_id,
        )
        if account_positions_synced:
            for symbol in list(positions.keys()):
                account_current = account_positions.get(_symbol_key(symbol), 0)
                system_shares = int(positions[symbol].get("shares", 0) or 0)
                if account_current <= 0:
                    positions.pop(symbol, None)
                    account_position_adjusted_symbols.append(symbol)
                    continue
                if account_current < system_shares:
                    positions[symbol]["shares"] = account_current
                    positions[symbol]["account_current_shares"] = account_current
                    account_position_adjusted_symbols.append(symbol)

    symbols = list(positions.keys())
    mark_prices, _ = latest_close_prices_on_or_before(data_db, symbols, base_date) if symbols else ({}, {})
    market_value = 0.0
    for symbol, pos in positions.items():
        price = mark_prices.get(symbol) or float(pos.get("cost_basis") or 0.0)
        pos["mark_price"] = price
        market_value += int(pos["shares"]) * price
    equity = cash + market_value

    return {
        "positions": positions,
        "cash": cash,
        "equity": equity,
        "capital_basis": CAPITAL_BASIS_ACCOUNT_EQUITY,
        "source": "live_lineage_reconstruction" if executions else "initial_capital",
        "securities_account_id": securities_account_id,
        "baseline_synced_at": baseline_synced_at,
        "baseline_stale": baseline_stale,
        "baseline_error": baseline_error,
        "account_positions_synced": account_positions_synced,
        "account_position_adjusted_symbols": sorted(set(account_position_adjusted_symbols)),
    }


def resolve_capital_baseline(
    cols: TradingCollections,
    app_db,
    data_db,
    *,
    plan_meta: Dict[str, Any],
    base_date: str,
    config: Dict[str, Any],
    allow_stale_account: bool = False,
) -> Dict[str, Any]:
    """Resolve the capital baseline (equity / cash / positions) for plan sizing.

    Modes:
      - ``account_equity``  : combo-scoped rolling equity from live executions
                              (default when unset).
      - ``rolling_paper``   : inherit the paper-trading lineage snapshot
                              (opt-in via ``capital_basis``).
      - ``fixed_notional``  : size every period against a fixed ``initial_capital``
                              while still carrying lineage positions for sell/hold.

    ``capital_basis`` is read from ``config`` (preset / params_override) and is
    intentionally not part of ``params_hash`` so switching it never forks the plan
    lineage.
    """
    initial_capital = float(config.get("initial_capital", 1_000_000.0) or 1_000_000.0)
    requested = str(config.get("capital_basis") or "").strip().lower() or None
    if requested and requested not in CAPITAL_BASIS_OPTIONS:
        raise ValueError(f"unsupported capital_basis: {requested}")

    mode = requested or CAPITAL_BASIS_ACCOUNT_EQUITY

    if mode == CAPITAL_BASIS_ACCOUNT_EQUITY:
        return reconstruct_live_lineage_baseline(
            app_db,
            data_db,
            cols,
            plan_meta=plan_meta,
            base_date=base_date,
            initial_capital=initial_capital,
            allow_stale_account=allow_stale_account,
        )

    paper_snapshot = latest_lineage_position_snapshot(cols, plan_meta, base_date, exclude_current_plan=False)

    if mode == CAPITAL_BASIS_FIXED_NOTIONAL:
        return {
            "positions": position_map(paper_snapshot),
            "cash": initial_capital,
            "equity": initial_capital,
            "capital_basis": CAPITAL_BASIS_FIXED_NOTIONAL,
            "source": "fixed_notional",
            "securities_account_id": None,
            "baseline_synced_at": None,
        }

    return {
        "positions": position_map(paper_snapshot),
        "cash": float(paper_snapshot.get("cash", initial_capital)),
        "equity": float(paper_snapshot.get("equity", initial_capital)),
        "capital_basis": CAPITAL_BASIS_ROLLING_PAPER,
        "source": "paper_snapshot" if paper_snapshot else "initial_capital",
        "securities_account_id": None,
        "baseline_synced_at": None,
    }


def _lineage_control_key(strategy_template_id: str, params_hash: str, user_id: Optional[str]) -> Dict[str, Any]:
    return {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
    }


def get_lineage_control(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
) -> Dict[str, Any]:
    doc = collections.lineage_controls.find_one(
        _lineage_control_key(strategy_template_id, params_hash, user_id),
        {"_id": 0},
    )
    if doc:
        return doc
    return {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
        "paused": False,
        "excluded_symbols": [],
    }


def is_lineage_paused(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
) -> bool:
    return bool(get_lineage_control(collections, strategy_template_id, params_hash, user_id).get("paused"))


def get_lineage_excluded_symbols(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
) -> List[str]:
    doc = get_lineage_control(collections, strategy_template_id, params_hash, user_id)
    return sorted({str(s) for s in (doc.get("excluded_symbols") or []) if s})


def pause_lineage(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    *,
    reason: str = "manual_liquidation",
    paused_by: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow()
    key = _lineage_control_key(strategy_template_id, params_hash, user_id)
    collections.lineage_controls.update_one(
        key,
        {
            "$set": {
                **key,
                "paused": True,
                "paused_at": now,
                "paused_by": paused_by,
                "paused_reason": reason,
                "updated_at": now,
            },
            "$unset": {"resumed_at": ""},
        },
        upsert=True,
    )
    return get_lineage_control(collections, strategy_template_id, params_hash, user_id)


def resume_lineage(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    *,
    resumed_by: Optional[str] = None,
) -> Dict[str, Any]:
    now = datetime.utcnow()
    key = _lineage_control_key(strategy_template_id, params_hash, user_id)
    collections.lineage_controls.update_one(
        key,
        {
            "$set": {
                **key,
                "paused": False,
                "resumed_at": now,
                "resumed_by": resumed_by,
                "updated_at": now,
            },
            "$unset": {"paused_at": "", "paused_by": "", "paused_reason": ""},
        },
        upsert=True,
    )
    return get_lineage_control(collections, strategy_template_id, params_hash, user_id)


def add_excluded_symbols(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    symbols: Iterable[str],
) -> Dict[str, Any]:
    incoming = sorted({str(s) for s in symbols if s})
    if not incoming:
        return get_lineage_control(collections, strategy_template_id, params_hash, user_id)
    now = datetime.utcnow()
    key = _lineage_control_key(strategy_template_id, params_hash, user_id)
    existing = get_lineage_excluded_symbols(collections, strategy_template_id, params_hash, user_id)
    merged = sorted(set(existing) | set(incoming))
    collections.lineage_controls.update_one(
        key,
        {"$set": {**key, "excluded_symbols": merged, "updated_at": now}, "$setOnInsert": {"paused": False}},
        upsert=True,
    )
    return get_lineage_control(collections, strategy_template_id, params_hash, user_id)


def apply_manual_plan_lineage_effects(
    collections: TradingCollections,
    plan: Dict[str, Any],
    *,
    final_positions: Optional[Iterable[Dict[str, Any]]] = None,
    acted_by: Optional[str] = None,
    pause_on_empty: bool = True,
) -> Dict[str, Any]:
    """After a manual plan executes, pause an emptied lineage and/or extend exclusions.

    ``pause_on_empty`` should only be true once the positions are *settled*: paper
    execution fills synchronously at the simulated open so it can pause immediately,
    but live signal *publishing* does not imply fills, so callers pass
    ``pause_on_empty=False`` and only the (harmless, forward-looking) exclusion is
    applied at publish time. Pausing a live lineage that still holds an unfilled
    position would wrongly stop daily maintenance of a real, risk-bearing book.
    """
    if str(plan.get("origin") or "") != "manual":
        return {}
    strategy_template_id = str(plan.get("strategy_template_id") or "")
    params_hash = str(plan.get("params_hash") or "")
    user_id = plan.get("user_id")
    effects: Dict[str, Any] = {}
    if plan.get("exclude_after"):
        symbols = [str(s) for s in (plan.get("manual_exclude_symbols") or []) if s]
        if symbols:
            effects["excluded"] = add_excluded_symbols(collections, strategy_template_id, params_hash, user_id, symbols)
    if not pause_on_empty:
        return effects
    positions = list(final_positions or [])
    holding_count = sum(1 for row in positions if int(row.get("shares", 0) or 0) > 0)
    if holding_count == 0:
        effects["paused"] = pause_lineage(
            collections,
            strategy_template_id,
            params_hash,
            user_id,
            reason="manual_liquidation",
            paused_by=acted_by,
        )
    return effects
