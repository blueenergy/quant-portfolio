"""AI risk-review entry points for symbols and holdings."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

from portfolio.ai_risk_review import review_plan_items, review_symbol
from portfolio.constants import business_today_yyyymmdd
from portfolio.db import TradingCollections, get_data_db, get_mongo_client


def review_symbols(data_db, symbols: Iterable[Any], base_date: str) -> Dict[str, Any]:
    """AI risk pass over a bare symbol list (e.g. the bench candidate pool).

    Holdings carry shares; bench candidates do not, so this is the share-less
    sibling of ``review_holdings``. Duplicates are de-duped; the single risk
    engine (``ai_risk_review.review_symbol``) stays the source of truth.
    """
    rows: List[Dict[str, Any]] = []
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    seen: set = set()
    for sym in symbols:
        symbol = str(sym or "").strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        risk = review_symbol(data_db, symbol, base_date)
        severity = risk.get("severity", "none")
        counts[severity] = counts.get(severity, 0) + 1
        rows.append({"symbol": symbol, "ai_risk": risk})
    return {
        "status": "done",
        "base_date": base_date,
        "reviewed_count": len(rows),
        "high": counts["high"],
        "medium": counts["medium"],
        "low": counts["low"],
        "none": counts["none"],
        "symbols": rows,
        "checked_at": datetime.utcnow(),
    }


def review_holdings(data_db, positions: Iterable[Dict[str, Any]], base_date: str) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    for pos in positions:
        symbol = pos.get("symbol")
        shares = int(pos.get("shares", 0) or 0)
        if not symbol or shares <= 0:
            continue
        risk = review_symbol(data_db, str(symbol), base_date)
        counts[risk.get("severity", "none")] = counts.get(risk.get("severity", "none"), 0) + 1
        rows.append(
            {
                "symbol": symbol,
                "name": pos.get("name", ""),
                "shares": shares,
                "ai_risk": risk,
            }
        )
    return {
        "status": "done",
        "base_date": base_date,
        "reviewed_count": len(rows),
        "high": counts["high"],
        "medium": counts["medium"],
        "low": counts["low"],
        "none": counts["none"],
        "holdings": rows,
        "checked_at": datetime.utcnow(),
    }


def rerun_plan_risk_review(
    collections: TradingCollections,
    plan_id: str,
    *,
    data_db=None,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Re-run the rule-based AI risk pass on an existing plan's items in place.

    Used by the manual "运行 AI 风控" trigger. Recomputes ``ai_risk`` on each
    buy/hold item and refreshes ``summary.ai_risk_summary`` without touching the
    plan's selection or status.
    """
    client = None
    if data_db is None:
        client = get_mongo_client()
        data_db = get_data_db(client)

    plan_query: Dict[str, Any] = {"plan_id": plan_id}
    if user_id:
        plan_query["user_id"] = user_id
    plan = collections.trade_plans.find_one(plan_query, {"_id": 0, "base_date": 1})
    if not plan:
        raise ValueError(f"Plan not found: {plan_id}")
    base_date = str(plan.get("base_date") or business_today_yyyymmdd())

    items_query: Dict[str, Any] = {"plan_id": plan_id}
    if user_id:
        items_query["user_id"] = user_id
    items = list(collections.trade_plan_items.find(items_query))
    summary = review_plan_items(data_db, items, base_date)

    for item in items:
        collections.trade_plan_items.update_one(
            {"_id": item["_id"]},
            {"$set": {"ai_risk": item.get("ai_risk")}},
        )
    collections.trade_plans.update_one(
        {"plan_id": plan_id},
        {"$set": {"summary.ai_risk_summary": summary, "updated_at": datetime.utcnow()}},
    )
    return summary
