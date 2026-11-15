"""Live bull-invest / non-bull-cash overlay for plan construction."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Mapping, Optional

from shared.regime_label import (
    REGIME_CASH_REMINDER,
    build_regime_cash_schedule,
    load_regime_rule,
    padded_index_start,
    regime_cash_requested,
)
from shared.universe_benchmark import default_index_benchmark_symbol

log = logging.getLogger(__name__)


def load_index_rows_from_data_db(
    data_db,
    symbol: str,
    start_date: Optional[str],
) -> List[Dict[str, Any]]:
    """Load benchmark closes from live ``data_db`` collections."""
    start = str(start_date or "00000000")
    symbol_text = str(symbol or "").strip()
    if not symbol_text:
        return []
    rows = _find_sorted(
        data_db,
        "index_prices",
        {"ts_code": symbol_text, "trade_date": {"$gte": start}},
        {"_id": 0, "ts_code": 1, "trade_date": 1, "close": 1, "vol": 1, "volume": 1, "high": 1, "low": 1},
    )
    if rows:
        return rows
    return _find_sorted(
        data_db,
        "volume_price",
        {"symbol": symbol_text, "trade_date": {"$gte": start}},
        {"_id": 0, "symbol": 1, "trade_date": 1, "close": 1, "vol": 1, "volume": 1, "high": 1, "low": 1},
    )


def resolve_live_regime_cash(
    *,
    config: Mapping[str, Any],
    data_db,
    score_date: str,
    universe_index: Optional[str] = None,
) -> Dict[str, Any]:
    """Decide invest vs cash for one live construction date.

    Opt-in only. The note is a sample-evidence reminder, not a universe gate.
    """
    universe = str(universe_index or config.get("universe_index") or "csi1000").strip().lower()
    as_of = str(score_date or "").strip()
    requested = regime_cash_requested(universe, config.get("regime_cash"))
    benchmark = str(
        config.get("index_benchmark_symbol") or default_index_benchmark_symbol(universe) or ""
    ).strip()
    decision: Dict[str, Any] = {
        "enabled": False,
        "requested": requested,
        "action": "off",
        "label": None,
        "universe_index": universe,
        "benchmark_symbol": benchmark or None,
        "score_date": as_of,
        "scope": "opt_in",
        "note": REGIME_CASH_REMINDER,
    }
    if not requested:
        return decision
    if not as_of:
        raise ValueError("非牛空仓需要 score_date 才能标注指数状态")

    symbol = benchmark
    if not symbol:
        raise ValueError(f"非牛空仓需要指数代码（universe_index={universe}）")
    padded_start = padded_index_start(as_of)
    rows = load_index_rows_from_data_db(data_db, symbol, padded_start)
    if not rows:
        raise ValueError(f"非牛空仓需要指数行情 {symbol}（自 {padded_start}）。{REGIME_CASH_REMINDER}")
    _cash_dates, schedule = build_regime_cash_schedule(
        rows,
        [as_of],
        regime_rule=config.get("regime_rule"),
    )
    entry = schedule[0] if schedule else {}
    action = str(entry.get("action") or "cash")
    decision.update(
        {
            "enabled": True,
            "action": action,
            "label": entry.get("label"),
            "benchmark_symbol": symbol,
            "regime_rule": load_regime_rule(config.get("regime_rule")).rule_id,
        }
    )
    return decision


def _find_sorted(data_db, collection: str, query: Mapping[str, Any], projection: Mapping[str, Any]) -> List[Dict[str, Any]]:
    try:
        cursor = data_db[collection].find(query, projection)
        sort = getattr(cursor, "sort", None)
        if callable(sort):
            cursor = sort("trade_date", 1)
        return list(cursor)
    except Exception:
        log.warning("index lookup failed on %s", collection, exc_info=True)
        return []
