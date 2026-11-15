"""Internal helpers for live signal cancellation during plan supersede."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from portfolio.constants import TERMINAL_LIVE_SIGNAL_STATUSES
from portfolio.lineage import _execution_fill_quantity, _filled_quantity_from_signal


def cancel_open_live_signals_for_plan(
    app_db,
    *,
    plan_id: str,
    user_id: Optional[str],
    reviewer: str,
    reason: str,
    now: datetime,
) -> Tuple[int, bool]:
    query: Dict[str, Any] = {"plan_id": plan_id, "mode": "live"}
    if user_id:
        query["user_id"] = user_id
    signals = list(app_db["trade_signals"].find(query))
    order_ids = [row.get("order_id") for row in signals if row.get("order_id")]
    executions = list(app_db["trade_executions"].find({"order_id": {"$in": order_ids}})) if order_ids else []
    if user_id:
        executions = [row for row in executions if row.get("user_id") in (None, user_id)]
    has_fills = any(_filled_quantity_from_signal(row) > 0 for row in signals) or any(_execution_fill_quantity(row) > 0 for row in executions)
    if has_fills:
        return 0, True

    cancelled_count = 0
    for signal in signals:
        order_id = signal.get("order_id")
        if not order_id or str(signal.get("status") or "").lower() in TERMINAL_LIVE_SIGNAL_STATUSES:
            continue
        result = app_db["trade_signals"].update_one(
            {"order_id": order_id, "user_id": signal.get("user_id")},
            {
                "$set": {
                    "status": "cancelled",
                    "cancelled_at": now,
                    "cancelled_by": reviewer,
                    "cancel_reason": reason,
                    "updated_at": now,
                }
            },
        )
        cancelled_count += int(getattr(result, "modified_count", 0) or 0)
    return cancelled_count, False
