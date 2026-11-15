"""Paper execution, equity marking, and paper status sync."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import ASCENDING, DESCENDING

from portfolio.calendar import normalize_date
from portfolio.constants import business_today_yyyymmdd
from portfolio.db import TradingCollections, get_app_db, get_data_db, get_mongo_client, get_trading_collections, get_trading_db
from portfolio.lineage import (
    _plan_has_live_signals,
    latest_lineage_position_snapshot,
    latest_plan_position_snapshot,
)
from portfolio.market_data import latest_close_prices_on_or_before, latest_prices, paper_execution_prices
from portfolio.strategies import plan_params_hash


def sync_paper_executed_plan_statuses(
    collections: TradingCollections,
    *,
    plan_id: Optional[str] = None,
    apply: bool = False,
) -> Dict[str, Any]:
    """Sync legacy plan status from existing paper execution records."""
    query: Dict[str, Any] = {"status": "approved"}
    if plan_id:
        query["plan_id"] = plan_id
    updated: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for plan in collections.trade_plans.find(query, {"_id": 0, "plan_id": 1, "user_id": 1, "status": 1}):
        execution_query: Dict[str, Any] = {"plan_id": plan.get("plan_id")}
        if plan.get("user_id"):
            execution_query["user_id"] = plan.get("user_id")
        latest = collections.paper_executions.find_one(execution_query, sort=[("created_at", DESCENDING)])
        if not latest:
            skipped.append({"plan_id": plan.get("plan_id"), "reason": "no_paper_executions"})
            continue
        patch = {
            "status": "executed_paper",
            "paper_executed_at": latest.get("created_at") or datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
        updated.append(
            {
                "plan_id": plan.get("plan_id"),
                "before_status": plan.get("status"),
                "after_status": "executed_paper",
                "paper_executed_at": patch["paper_executed_at"],
            }
        )
        if apply:
            collections.trade_plans.update_one({"plan_id": plan.get("plan_id")}, {"$set": patch})
    return {"applied": bool(apply), "updated": updated, "skipped": skipped}


def apply_paper_trade_plan(
    *,
    plan_id: str,
    trading_collections: Optional[TradingCollections] = None,
    app_db=None,
    data_db=None,
    force: bool = False,
    execute_date: Optional[str] = None,
) -> Dict[str, Any]:
    client = None
    if trading_collections is None or data_db is None:
        client = get_mongo_client()
    cols = get_trading_collections(get_trading_db(client)) if trading_collections is None else trading_collections
    app_db = get_app_db(client) if app_db is None and client is not None else app_db
    data_db = get_data_db(client) if data_db is None else data_db
    plan = cols.trade_plans.find_one({"plan_id": plan_id}, {"_id": 0})
    if not plan:
        raise ValueError(f"Plan not found: {plan_id}")
    if plan.get("status") != "approved" and not force:
        raise ValueError(f"Plan {plan_id} must be approved before paper execution")
    owner_id = plan.get("user_id")
    if app_db is not None and _plan_has_live_signals(app_db, plan_id=plan_id, user_id=owner_id):
        raise ValueError(f"Plan {plan_id} has live signals; cannot execute paper")
    strategy_template_id = plan["strategy_template_id"]
    params_snapshot = plan.get("params_snapshot") or {}
    # A snapshot must always carry params_hash; otherwise daily marking and
    # lineage inheritance degrade to strategy_template_id-only matching and one
    # lineage can inherit / overwrite another's positions. Derive it from the
    # frozen params when a legacy plan is missing the field.
    params_hash = plan.get("params_hash") or (plan_params_hash(params_snapshot) if params_snapshot else None)
    requested_execute_date = normalize_date(execute_date)
    plan_execute_date = normalize_date(plan.get("execute_date"))
    if requested_execute_date and plan_execute_date and requested_execute_date != plan_execute_date:
        raise ValueError(f"Plan {plan_id} execute_date is {plan_execute_date}, not {requested_execute_date}")
    execute_date = plan_execute_date or requested_execute_date
    if not execute_date:
        raise ValueError(f"Plan {plan_id} has no execute_date")
    plan["execute_date"] = execute_date
    if cols.paper_executions.find_one({"plan_id": plan_id}) and not force:
        raise ValueError(f"Plan {plan_id} already has paper executions")
    if not plan_execute_date:
        cols.trade_plans.update_one({"plan_id": plan_id}, {"$set": {"execute_date": execute_date, "updated_at": datetime.utcnow()}})

    inherited_snapshot = latest_lineage_position_snapshot(cols, plan, execute_date)
    positions: Dict[str, Dict[str, Any]] = {row.get("symbol"): dict(row) for row in (inherited_snapshot.get("positions") or []) if row.get("symbol")}
    cash = float(inherited_snapshot.get("cash", params_snapshot.get("initial_capital", 1_000_000.0)) or 0)
    inherited_equity = float(inherited_snapshot.get("equity", params_snapshot.get("initial_capital", 1_000_000.0)) or 0)
    items = list(cols.trade_plan_items.find({"plan_id": plan_id}, {"_id": 0}).sort("rank", ASCENDING))
    symbols = [item["symbol"] for item in items if item.get("symbol")]
    items_by_symbol = {str(item["symbol"]): item for item in items if item.get("symbol")}
    prices, price_sources, price_dates = paper_execution_prices(data_db, symbols, execute_date, items_by_symbol)
    executions = []
    partial = False
    now = datetime.utcnow()

    for item in items:
        symbol = item.get("symbol")
        delta = int(item.get("delta_shares", 0) or 0)
        if not symbol or delta == 0:
            continue
        price = prices.get(symbol)
        if not price:
            partial = True
            executions.append(_execution_doc(plan, item, "blocked", 0, None, 0, "missing_execution_price", now))
            continue
        current = positions.get(
            symbol, {"symbol": symbol, "name": item.get("name", ""), "industry": item.get("industry", ""), "shares": 0, "cost_basis": 0.0}
        )
        if delta < 0:
            quantity = min(abs(delta), int(current.get("shares", 0) or 0))
            amount = quantity * price
            cash += amount
            current["shares"] = int(current.get("shares", 0) or 0) - quantity
            executions.append(
                _execution_doc(
                    plan,
                    item,
                    "sell",
                    quantity,
                    price,
                    amount,
                    None,
                    now,
                    price_source=price_sources.get(symbol),
                    price_trade_date=price_dates.get(symbol),
                )
            )
        else:
            quantity = delta
            amount = quantity * price
            if amount > cash:
                lot_size = int(params_snapshot.get("lot_size", 100))
                quantity = (int(cash // price) // lot_size) * lot_size
                amount = quantity * price
                partial = True
            if quantity <= 0:
                executions.append(
                    _execution_doc(
                        plan,
                        item,
                        "blocked",
                        0,
                        price,
                        0,
                        "insufficient_cash",
                        now,
                        price_source=price_sources.get(symbol),
                        price_trade_date=price_dates.get(symbol),
                    )
                )
                partial = True
                continue
            cash -= amount
            old_shares = int(current.get("shares", 0) or 0)
            old_cost = float(current.get("cost_basis", 0) or 0)
            current["cost_basis"] = ((old_shares * old_cost) + amount) / (old_shares + quantity) if old_shares + quantity else price
            current["shares"] = old_shares + quantity
            executions.append(
                _execution_doc(
                    plan,
                    item,
                    "buy",
                    quantity,
                    price,
                    amount,
                    None,
                    now,
                    price_source=price_sources.get(symbol),
                    price_trade_date=price_dates.get(symbol),
                )
            )
        if int(current.get("shares", 0) or 0) > 0:
            positions[symbol] = current
        elif symbol in positions:
            del positions[symbol]

    mark_prices = latest_prices(data_db, list(positions.keys()), execute_date, "close")
    position_rows = []
    market_value = 0.0
    for symbol, pos in sorted(positions.items()):
        price = mark_prices.get(symbol) or prices.get(symbol) or float(pos.get("cost_basis", 0) or 0)
        value = int(pos.get("shares", 0) or 0) * price
        market_value += value
        position_rows.append({**pos, "mark_price": price, "market_value": value})
    equity = cash + market_value
    snapshot = {
        "strategy_template_id": strategy_template_id,
        "user_id": owner_id,
        "params_hash": params_hash,
        "as_of_date": execute_date,
        "plan_id": plan_id,
        "inherited_from_plan_id": inherited_snapshot.get("plan_id"),
        "inherited_from_as_of_date": inherited_snapshot.get("as_of_date"),
        "cash": cash,
        "market_value": market_value,
        "equity": equity,
        "positions": position_rows,
        "updated_at": now,
    }
    if force:
        cols.paper_executions.delete_many({"plan_id": plan_id})
    if executions:
        cols.paper_executions.insert_many(executions)
    position_query = {"plan_id": plan_id, "as_of_date": execute_date}
    equity_query = {"plan_id": plan_id, "date": execute_date}
    if params_hash:
        snapshot["params_hash"] = params_hash
    if owner_id:
        position_query["user_id"] = owner_id
        equity_query["user_id"] = owner_id
    cols.paper_positions.replace_one(position_query, snapshot, upsert=True)
    cols.paper_equity.replace_one(
        equity_query,
        {
            "strategy_template_id": strategy_template_id,
            "user_id": owner_id,
            "params_hash": params_hash,
            "date": execute_date,
            "plan_id": plan_id,
            "inherited_from_plan_id": inherited_snapshot.get("plan_id"),
            "inherited_from_as_of_date": inherited_snapshot.get("as_of_date"),
            "equity": equity,
            "cash": cash,
            "market_value": market_value,
            "updated_at": now,
        },
        upsert=True,
    )
    status = "partially_executed" if partial else "executed_paper"
    cols.trade_plans.update_one({"plan_id": plan_id}, {"$set": {"status": status, "paper_executed_at": now}})
    return {
        "plan_id": plan_id,
        "status": status,
        "executions": executions,
        "position_snapshot": snapshot,
        "inherited_from_plan_id": inherited_snapshot.get("plan_id"),
        "inherited_from_as_of_date": inherited_snapshot.get("as_of_date"),
        "inherited_equity": inherited_equity,
    }


def apply_approved_paper_plans(
    *,
    execute_date: Optional[str] = None,
    strategy_template_id: Optional[str] = None,
    trading_collections: Optional[TradingCollections] = None,
    app_db=None,
    data_db=None,
    force: bool = False,
    limit: Optional[int] = None,
) -> Dict[str, Any]:
    target_date = normalize_date(execute_date) or business_today_yyyymmdd()
    client = None
    if trading_collections is None or data_db is None:
        client = get_mongo_client()
    cols = get_trading_collections(get_trading_db(client)) if trading_collections is None else trading_collections
    app_db = get_app_db(client) if app_db is None and client is not None else app_db
    data_db = get_data_db(client) if data_db is None else data_db

    query: Dict[str, Any] = {"status": "approved"}
    if strategy_template_id:
        query["strategy_template_id"] = strategy_template_id
    plans = list(cols.trade_plans.find(query, {"_id": 0}))
    plans.sort(
        key=lambda plan: (
            normalize_date(plan.get("execute_date")) or "",
            str(plan.get("base_date") or ""),
            # Trailing-stop sells clear names before same-day rebalance rebuilds.
            0 if plan.get("plan_type") == "trailing_stop_sell" else 1,
            str(plan.get("created_at") or ""),
        )
    )
    applied: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for plan in plans:
        if limit is not None and len(applied) >= limit:
            break
        plan_id = plan.get("plan_id")
        plan_execute_date = normalize_date(plan.get("execute_date"))
        if plan.get("plan_type") not in {"rebalance", "trailing_stop_sell"}:
            skipped.append({"plan_id": plan_id, "reason": "not_executable_plan_type"})
            continue
        if plan_execute_date and plan_execute_date != target_date:
            skipped.append({"plan_id": plan_id, "reason": "different_execute_date", "execute_date": plan_execute_date})
            continue
        if not plan_execute_date and str(plan.get("base_date") or "") >= target_date:
            skipped.append({"plan_id": plan_id, "reason": "base_date_not_before_execute_date"})
            continue
        if cols.paper_executions.find_one({"plan_id": plan_id}) and not force:
            skipped.append({"plan_id": plan_id, "reason": "already_executed"})
            continue
        if app_db is not None and _plan_has_live_signals(app_db, plan_id=plan_id, user_id=plan.get("user_id")):
            skipped.append({"plan_id": plan_id, "reason": "live_signals"})
            continue

        items = list(
            cols.trade_plan_items.find({"plan_id": plan_id}, {"_id": 0, "symbol": 1, "delta_shares": 1, "estimated_price": 1, "price_trade_date": 1})
        )
        symbols = sorted({item["symbol"] for item in items if item.get("symbol") and int(item.get("delta_shares", 0) or 0) != 0})
        items_by_symbol = {str(item["symbol"]): item for item in items if item.get("symbol")}
        prices, _, _ = paper_execution_prices(data_db, symbols, target_date, items_by_symbol) if symbols else ({}, {}, {})
        missing = [symbol for symbol in symbols if symbol not in prices]
        if missing:
            skipped.append(
                {
                    "plan_id": plan_id,
                    "reason": "missing_execution_price",
                    "execute_date": target_date,
                    "missing_count": len(missing),
                    "missing_symbols": missing[:10],
                }
            )
            continue

        try:
            applied.append(
                apply_paper_trade_plan(
                    plan_id=plan_id,
                    trading_collections=cols,
                    app_db=app_db,
                    data_db=data_db,
                    force=force,
                    execute_date=target_date,
                )
            )
        except Exception as exc:  # Keep a daemon/cron batch from stopping on one bad plan.
            errors.append({"plan_id": plan_id, "reason": str(exc)})

    return {"execute_date": target_date, "applied": applied, "skipped": skipped, "errors": errors}


def _active_head_plan_ids(
    cols: TradingCollections,
    plan_ids: List[str],
    target_date: str,
) -> Dict[str, Dict[str, Any]]:
    """Pick the active head plan per (strategy_template_id, params_hash, user_id).

    A later rebalance supersedes the prior plan in the same lineage, yet both
    leave behind position snapshots. Re-marking every historical plan_id forks
    one logical portfolio into several parallel ones, so we keep only the latest
    plan per lineage (newest base_date, tie-broken by snapshot as_of_date).

    Lineages without a ``params_hash`` cannot be safely merged, so each such
    plan_id is treated as its own lineage rather than collapsed together.
    """

    heads: Dict[tuple, Dict[str, Any]] = {}
    for plan_id in plan_ids:
        snapshot = latest_plan_position_snapshot(cols, plan_id, target_date)
        if not snapshot:
            continue
        plan = (
            cols.trade_plans.find_one(
                {"plan_id": plan_id},
                {"_id": 0, "strategy_template_id": 1, "params_hash": 1, "user_id": 1, "base_date": 1},
            )
            or {}
        )
        strategy_template_id = plan.get("strategy_template_id") or snapshot.get("strategy_template_id")
        params_hash = plan.get("params_hash") or snapshot.get("params_hash")
        user_id = plan.get("user_id") or snapshot.get("user_id")
        base_date = str(plan.get("base_date") or "")
        as_of_date = str(snapshot.get("as_of_date") or "")
        key = (strategy_template_id, params_hash, user_id) if params_hash else ("__plan__", plan_id, user_id)
        rank = (base_date, as_of_date)
        current = heads.get(key)
        if current is None or rank > current["rank"]:
            heads[key] = {"plan_id": plan_id, "rank": rank}
    return {info["plan_id"]: {"key": key} for key, info in heads.items()}


def mark_plan_equity_for_date(
    cols: TradingCollections,
    data_db,
    plan_id: str,
    target_date: str,
    now: Optional[datetime] = None,
) -> Optional[Dict[str, Any]]:
    """Re-mark a single plan's holdings at ``target_date`` using close prices.

    Carries the plan's most recent position snapshot forward, re-pricing every
    holding. Returns the equity summary, or ``None`` when the plan has no
    snapshot on or before ``target_date``. Shared by the daily marking worker and
    the paper-position rebuild tool so both stay byte-for-byte consistent.
    """

    now = now or datetime.utcnow()
    snapshot = latest_plan_position_snapshot(cols, plan_id, target_date)
    if not snapshot:
        return None

    template_id = str(snapshot.get("strategy_template_id") or "")
    positions = snapshot.get("positions", []) or []
    symbols = [row.get("symbol") for row in positions if row.get("symbol")]
    prices, price_dates = latest_close_prices_on_or_before(data_db, symbols, target_date)
    position_rows = []
    market_value = 0.0
    missing_symbols = []
    fallback_symbols = []

    for pos in positions:
        symbol = pos.get("symbol")
        shares = int(pos.get("shares", 0) or 0)
        price = prices.get(symbol)
        price_date = price_dates.get(symbol)
        price_source = "close" if price_date == target_date else "latest_close"
        if price is None:
            try:
                price = float(pos.get("mark_price") or pos.get("cost_basis") or 0)
            except (TypeError, ValueError):
                price = 0.0
            price_source = "paper_mark"
            missing_symbols.append(symbol)
        elif price_date != target_date:
            fallback_symbols.append(symbol)
        value = shares * float(price or 0)
        market_value += value
        position_rows.append(
            {
                **pos,
                "mark_price": float(price or 0),
                "market_value": value,
                "price_trade_date": price_date,
                "price_source": price_source,
            }
        )

    cash = float(snapshot.get("cash", 0) or 0)
    equity = cash + market_value
    marked_snapshot = {
        "strategy_template_id": template_id,
        "user_id": snapshot.get("user_id"),
        "params_hash": snapshot.get("params_hash"),
        "as_of_date": target_date,
        "plan_id": plan_id,
        "cash": cash,
        "market_value": market_value,
        "equity": equity,
        "positions": position_rows,
        "price_source_summary": {
            "close_count": sum(1 for row in position_rows if row.get("price_source") == "close"),
            "latest_close_count": sum(1 for row in position_rows if row.get("price_source") == "latest_close"),
            "paper_mark_count": sum(1 for row in position_rows if row.get("price_source") == "paper_mark"),
            "fallback_symbols": fallback_symbols[:20],
            "missing_symbols": missing_symbols[:20],
        },
        "updated_at": now,
    }
    position_query = {"plan_id": plan_id, "as_of_date": target_date}
    equity_query = {"plan_id": plan_id, "date": target_date}
    if snapshot.get("user_id"):
        position_query["user_id"] = snapshot.get("user_id")
        equity_query["user_id"] = snapshot.get("user_id")
    cols.paper_positions.replace_one(position_query, marked_snapshot, upsert=True)
    cols.paper_equity.replace_one(
        equity_query,
        {
            "strategy_template_id": template_id,
            "user_id": snapshot.get("user_id"),
            "params_hash": snapshot.get("params_hash"),
            "date": target_date,
            "plan_id": plan_id,
            "equity": equity,
            "cash": cash,
            "market_value": market_value,
            "price_source_summary": marked_snapshot["price_source_summary"],
            "updated_at": now,
        },
        upsert=True,
    )
    return {
        "strategy_template_id": template_id,
        "user_id": snapshot.get("user_id"),
        "date": target_date,
        "equity": equity,
        "cash": cash,
        "market_value": market_value,
        "plan_id": plan_id,
    }


def mark_paper_equity(
    *,
    as_of_date: Optional[str] = None,
    strategy_template_id: Optional[str] = None,
    trading_collections: Optional[TradingCollections] = None,
    data_db=None,
) -> Dict[str, Any]:
    target_date = normalize_date(as_of_date) or business_today_yyyymmdd()
    client = None
    if trading_collections is None or data_db is None:
        client = get_mongo_client()
    cols = get_trading_collections(get_trading_db(client)) if trading_collections is None else trading_collections
    data_db = get_data_db(client) if data_db is None else data_db

    account_query: Dict[str, Any] = {"as_of_date": {"$lte": target_date}, "plan_id": {"$exists": True}}
    if strategy_template_id:
        account_query["strategy_template_id"] = strategy_template_id
    plan_ids = sorted(str(value) for value in cols.paper_positions.distinct("plan_id", account_query) if value)
    head_plan_ids = _active_head_plan_ids(cols, plan_ids, target_date)
    marked: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    now = datetime.utcnow()

    for plan_id in plan_ids:
        if plan_id not in head_plan_ids:
            skipped.append({"plan_id": plan_id, "reason": "superseded_by_lineage_head"})
            continue
        summary = mark_plan_equity_for_date(cols, data_db, plan_id, target_date, now)
        if summary is None:
            skipped.append({"plan_id": plan_id, "reason": "no_position_snapshot"})
            continue
        marked.append(summary)

    return {"as_of_date": target_date, "marked": marked, "skipped": skipped}


def _execution_doc(
    plan: Dict[str, Any],
    item: Dict[str, Any],
    action: str,
    quantity: int,
    price: Optional[float],
    amount: float,
    blocker: Optional[str],
    now: datetime,
    *,
    price_source: Optional[str] = None,
    price_trade_date: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "execution_id": str(uuid.uuid4()),
        "plan_id": plan["plan_id"],
        "strategy_template_id": plan["strategy_template_id"],
        "user_id": plan.get("user_id"),
        "symbol": item.get("symbol"),
        "name": item.get("name", ""),
        "action": action,
        "quantity": int(quantity),
        "price": price,
        "amount": amount,
        "blocker": blocker,
        "execute_date": plan.get("execute_date"),
        "price_source": price_source,
        "price_trade_date": price_trade_date,
        "created_at": now,
    }
