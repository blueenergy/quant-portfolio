"""Portfolio plan maintenance / backfill helpers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from portfolio.calendar import normalize_date
from portfolio.db import TradingCollections
from portfolio.lineage import latest_lineage_position_snapshot, position_map
from portfolio.market_data import latest_close_prices_on_or_before


def _plan_item_action(*, current_shares: int, target_shares: int, delta_shares: int) -> str:
    if delta_shares > 0:
        return "buy"
    if delta_shares < 0:
        return "sell"
    if current_shares > 0:
        return "hold"
    if target_shares > 0:
        return "hold"
    return "skip"


def backfill_plan_items_from_lineage(
    collections: TradingCollections,
    plan_id: str,
    *,
    data_db=None,
    apply: bool = False,
) -> Dict[str, Any]:
    """Backfill existing plan items from the latest upstream lineage snapshot.

    This fixes plans generated before lineage-aware position lookup existed:
    selected targets may have been persisted as fresh buys even when the same
    symbols were already held upstream.
    """
    plan = collections.trade_plans.find_one({"plan_id": plan_id}, {"_id": 0})
    if not plan:
        raise ValueError(f"Plan not found: {plan_id}")
    base_date = normalize_date(plan.get("base_date")) or str(plan.get("base_date") or "")
    if not base_date:
        raise ValueError(f"Plan {plan_id} has no base_date")
    snapshot = latest_lineage_position_snapshot(collections, plan, base_date, exclude_current_plan=True)
    if not snapshot:
        return {
            "plan_id": plan_id,
            "applied": False,
            "reason": "no_lineage_position_snapshot",
            "updated": [],
            "inserted": [],
            "summary": {},
        }

    current_positions = position_map(snapshot)
    items = list(collections.trade_plan_items.find({"plan_id": plan_id}))
    target_symbols = {item.get("symbol") for item in items if item.get("symbol")}
    trading_enabled = bool(plan.get("trading_enabled", True))
    if plan.get("executable") is False:
        trading_enabled = False
    now = datetime.utcnow()
    updated: List[Dict[str, Any]] = []
    rewritten_items: List[Dict[str, Any]] = []

    for item in items:
        symbol = item.get("symbol")
        target_shares = int(item.get("target_shares", 0) or 0)
        current_shares = int(current_positions.get(symbol, {}).get("shares", 0) or 0)
        drift_delta = target_shares - current_shares
        delta = drift_delta if trading_enabled else 0
        action = _plan_item_action(current_shares=current_shares, target_shares=target_shares, delta_shares=delta)
        price = item.get("estimated_price")
        estimated_amount = abs(delta) * float(price) if price else None
        patch = {
            "action": action,
            "current_shares": current_shares,
            "delta_shares": delta,
            "drift_delta_shares": drift_delta,
            "estimated_amount": estimated_amount,
            "updated_at": now,
        }
        rewritten = {**item, **patch}
        rewritten_items.append(rewritten)
        before = {
            "action": item.get("action"),
            "current_shares": item.get("current_shares"),
            "target_shares": item.get("target_shares"),
            "delta_shares": item.get("delta_shares"),
        }
        after = {
            "action": action,
            "current_shares": current_shares,
            "target_shares": target_shares,
            "delta_shares": delta,
        }
        if before != after:
            updated.append({"symbol": symbol, "before": before, "after": after})
            if apply:
                collections.trade_plan_items.update_one(
                    {"plan_id": plan_id, "symbol": symbol, "rank": item.get("rank")},
                    {"$set": patch},
                )

    dropped_symbols = [symbol for symbol, pos in current_positions.items() if symbol not in target_symbols and int(pos.get("shares", 0) or 0) > 0]
    prices: Dict[str, Optional[float]] = {}
    price_dates: Dict[str, Optional[str]] = {}
    if dropped_symbols and data_db is not None:
        prices, price_dates = latest_close_prices_on_or_before(data_db, dropped_symbols, base_date)

    inserted: List[Dict[str, Any]] = []
    for symbol in sorted(dropped_symbols):
        pos = current_positions[symbol]
        held_shares = int(pos.get("shares", 0) or 0)
        drift_delta = -held_shares
        delta = drift_delta if trading_enabled else 0
        action = _plan_item_action(current_shares=held_shares, target_shares=0, delta_shares=delta)
        new_item = {
            "plan_id": plan_id,
            "strategy_template_id": plan.get("strategy_template_id"),
            "user_id": plan.get("user_id"),
            "params_hash": plan.get("params_hash"),
            "base_date": plan.get("base_date"),
            "execute_date": plan.get("execute_date"),
            "rank": None,
            "symbol": symbol,
            "name": pos.get("name", ""),
            "industry": pos.get("industry", ""),
            "score_value": None,
            "action": action,
            "current_shares": held_shares,
            "target_shares": 0,
            "delta_shares": delta,
            "drift_delta_shares": drift_delta,
            "target_weight": 0,
            "estimated_price": prices.get(symbol),
            "price_trade_date": price_dates.get(symbol),
            "estimated_amount": abs(delta) * prices[symbol] if prices.get(symbol) else None,
            "blockers": [],
            "created_at": now,
            "updated_at": now,
        }
        rewritten_items.append(new_item)
        inserted.append(
            {
                "symbol": symbol,
                "action": action,
                "current_shares": held_shares,
                "target_shares": 0,
                "delta_shares": delta,
            }
        )
        if apply:
            collections.trade_plan_items.insert_one(new_item)

    summary_patch = {
        "summary.buy_count": sum(1 for item in rewritten_items if item.get("action") == "buy"),
        "summary.sell_count": sum(1 for item in rewritten_items if item.get("action") == "sell"),
        "summary.hold_count": sum(1 for item in rewritten_items if item.get("action") == "hold"),
        "summary.skip_count": sum(1 for item in rewritten_items if item.get("action") == "skip"),
        "summary.estimated_buy_amount": sum(float(item.get("estimated_amount") or 0) for item in rewritten_items if item.get("action") == "buy"),
        "summary.estimated_sell_amount": sum(float(item.get("estimated_amount") or 0) for item in rewritten_items if item.get("action") == "sell"),
        "updated_at": now,
    }
    if apply and (updated or inserted):
        collections.trade_plans.update_one({"plan_id": plan_id}, {"$set": summary_patch})

    return {
        "plan_id": plan_id,
        "applied": bool(apply),
        "source_snapshot": {
            "plan_id": snapshot.get("plan_id"),
            "as_of_date": snapshot.get("as_of_date"),
            "params_hash": snapshot.get("params_hash"),
        },
        "updated": updated,
        "inserted": inserted,
        "summary": summary_patch,
    }
