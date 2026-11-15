"""Trailing-stop monitor for live/paper portfolio lineages."""

from __future__ import annotations

import os
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd
from pymongo import DESCENDING

from portfolio.calendar import next_trading_date, normalize_date
from portfolio.constants import (
    AUTO_PAPER_APPROVE_REVIEWER,
    PAPER_EXECUTION_MODE_AUTO_SHADOW,
    PAPER_EXECUTION_MODE_MANUAL_REVIEW,
    RECORD_KIND_TRADE_PLAN,
)
from portfolio.db import TradingCollections, ensure_trading_indexes
from portfolio.lineage import _lineage_control_key, get_lineage_control, latest_position_snapshot
from portfolio.market_data import latest_close_prices_on_or_before
from portfolio.record_kind import strategy_rebalance_mongo_filter
from portfolio.scheduler import find_active_plan_subscriptions
from portfolio.strategies import (
    build_user_plan_id,
    effective_strategy_config,
    load_default_param_preset,
    load_strategy_config,
    plan_params_hash,
    strategy_display_name,
)

TRAILING_STOP_TASK_DEDUP_STATUSES = ("pending", "running", "completed")
TRAILING_STOP_PLAN_TYPE = "trailing_stop_sell"
TRAILING_STOP_ORIGIN = "trailing_stop_monitor"
# Stale running tasks older than this are reclaimed so a crashed worker cannot
# permanently block the same-day monitor.
TRAILING_STOP_RUNNING_STALE_SECONDS = 30 * 60
EXECUTABLE_REBALANCE_STATUSES = {
    "needs_review",
    "approved",
    "executed_paper",
    "partially_executed",
    "executed_live",
}


def reclaim_stale_trailing_stop_tasks(
    collections: TradingCollections,
    *,
    now: Optional[datetime] = None,
    stale_seconds: int = TRAILING_STOP_RUNNING_STALE_SECONDS,
) -> int:
    """Move crash-orphaned ``running`` tasks back to ``pending``."""
    now = now or datetime.utcnow()
    cutoff = now.timestamp() - max(60, int(stale_seconds))
    reclaimed = 0
    for task in list(
        collections.trailing_stop_tasks.find(
            {"status": "running"},
            {"_id": 0, "task_id": 1, "started_at": 1, "updated_at": 1},
        )
    ):
        started = task.get("started_at") or task.get("updated_at")
        try:
            started_ts = started.timestamp() if hasattr(started, "timestamp") else None
        except Exception:
            started_ts = None
        if started_ts is None or started_ts > cutoff:
            continue
        result = collections.trailing_stop_tasks.update_one(
            {"task_id": task.get("task_id"), "status": "running"},
            {
                "$set": {
                    "status": "pending",
                    "error_message": "reclaimed_stale_running",
                    "updated_at": now,
                }
            },
        )
        if getattr(result, "modified_count", 0):
            reclaimed += 1
    return reclaimed



def is_trailing_stop_monitor_verbose() -> bool:
    raw = os.getenv("TRAILING_STOP_MONITOR_VERBOSE", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


LINEAGE_TRAILING_STOP_FIELD = "trailing_stop_pct"


def resolve_trailing_stop_pct(params: Mapping[str, Any]) -> Optional[float]:
    raw = params.get(LINEAGE_TRAILING_STOP_FIELD)
    if raw in (None, ""):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    return value


def lineage_has_explicit_trailing_stop_setting(control: Optional[Mapping[str, Any]]) -> bool:
    return control is not None and LINEAGE_TRAILING_STOP_FIELD in control


def resolve_trailing_stop_pct_for_lineage(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    config: Optional[Mapping[str, Any]] = None,
) -> Optional[float]:
    """Resolve trailing-stop pct for a lineage (controls override preset/snapshot)."""
    control = collections.lineage_controls.find_one(
        _lineage_control_key(strategy_template_id, params_hash, user_id),
        {"_id": 0, LINEAGE_TRAILING_STOP_FIELD: 1},
    )
    if lineage_has_explicit_trailing_stop_setting(control):
        return resolve_trailing_stop_pct(control or {})
    if config is not None:
        return resolve_trailing_stop_pct(config)
    return None


def set_lineage_trailing_stop_pct(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    trailing_stop_pct: Any,
    *,
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    """Write trailing-stop pct on lineage controls (null/<=0 disables monitoring)."""
    now = datetime.utcnow()
    key = _lineage_control_key(strategy_template_id, params_hash, user_id)
    stored = None
    if trailing_stop_pct not in (None, ""):
        try:
            stored = float(trailing_stop_pct)
        except (TypeError, ValueError):
            stored = None
    update: Dict[str, Any] = {
        **key,
        LINEAGE_TRAILING_STOP_FIELD: stored,
        "trailing_stop_updated_at": now,
        "updated_at": now,
    }
    if updated_by:
        update["trailing_stop_updated_by"] = updated_by
    collections.lineage_controls.update_one(
        key,
        {"$set": update, "$setOnInsert": {"paused": False, "excluded_symbols": []}},
        upsert=True,
    )
    return get_lineage_control(collections, strategy_template_id, params_hash, user_id)


def trailing_stop_pct_for_snapshot(resolved_trailing_stop_pct: Optional[float]) -> Dict[str, Any]:
    """Persist the lineage-resolved pct (None = disabled / unset)."""
    return {LINEAGE_TRAILING_STOP_FIELD: resolved_trailing_stop_pct}


def seed_lineage_trailing_stop_from_config(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    config: Mapping[str, Any],
) -> None:
    """Copy preset trailing-stop into controls when the lineage has no explicit setting."""
    control = collections.lineage_controls.find_one(
        _lineage_control_key(strategy_template_id, params_hash, user_id),
        {"_id": 0, LINEAGE_TRAILING_STOP_FIELD: 1},
    )
    if lineage_has_explicit_trailing_stop_setting(control):
        return
    if LINEAGE_TRAILING_STOP_FIELD not in config:
        return
    set_lineage_trailing_stop_pct(
        collections,
        strategy_template_id,
        params_hash,
        user_id,
        config.get(LINEAGE_TRAILING_STOP_FIELD),
    )


def _to_float(value: Any) -> Optional[float]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    return numeric


def _bar_high(row: Mapping[str, Any]) -> Optional[float]:
    return _to_float(row.get("high") or row.get("high_price") or row.get("adj_high") or row.get("close"))


def _bar_close(row: Mapping[str, Any]) -> Optional[float]:
    return _to_float(row.get("close") or row.get("close_price") or row.get("adj_close"))


def check_trailing_stop(
    price_rows: Sequence[Mapping[str, Any]],
    *,
    entry_date: str,
    base_date: str,
    trailing_pct: float,
) -> Dict[str, Any]:
    """Mirror ``score_backtester._find_trailing_stop_target`` on a bar window."""
    entry_date = normalize_date(entry_date) or entry_date
    base_date = normalize_date(base_date) or base_date
    bars = [row for row in price_rows if normalize_date(str(row.get("trade_date") or ""))]
    bars.sort(key=lambda row: str(row.get("trade_date") or ""))
    start_idx = next((idx for idx, row in enumerate(bars) if str(row.get("trade_date")) >= entry_date), None)
    end_idx = next((idx for idx, row in enumerate(bars) if str(row.get("trade_date")) == base_date), None)
    if start_idx is None or end_idx is None or end_idx < start_idx:
        return {
            "peak_high": None,
            "close": None,
            "stop_floor": None,
            "triggered": False,
            "trigger_date": None,
            "distance_to_stop_pct": None,
            "skipped_reason": "missing_price_window",
        }

    peak_high = _bar_high(bars[start_idx])
    if peak_high is None:
        return {
            "peak_high": None,
            "close": None,
            "stop_floor": None,
            "triggered": False,
            "trigger_date": None,
            "distance_to_stop_pct": None,
            "skipped_reason": "missing_entry_bar",
        }

    trigger_date = None
    for idx in range(start_idx, end_idx + 1):
        bar_high = _bar_high(bars[idx])
        if bar_high is not None and bar_high > peak_high:
            peak_high = bar_high
        close_price = _bar_close(bars[idx])
        if close_price is None:
            continue
        stop_floor = peak_high * (1.0 - trailing_pct)
        if close_price <= stop_floor:
            trigger_date = str(bars[idx].get("trade_date") or "")
            close = close_price
            distance = (close - stop_floor) / stop_floor if stop_floor else None
            return {
                "peak_high": peak_high,
                "close": close,
                "stop_floor": stop_floor,
                "triggered": True,
                "trigger_date": trigger_date,
                "distance_to_stop_pct": distance,
                "skipped_reason": None,
            }

    close = _bar_close(bars[end_idx])
    stop_floor = peak_high * (1.0 - trailing_pct) if peak_high else None
    distance = ((close - stop_floor) / stop_floor) if close is not None and stop_floor else None
    return {
        "peak_high": peak_high,
        "close": close,
        "stop_floor": stop_floor,
        "triggered": False,
        "trigger_date": None,
        "distance_to_stop_pct": distance,
        "skipped_reason": None,
    }


def fetch_trailing_stop_price_rows(
    data_db,
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    price_fetcher: Optional[Callable[[Any, str, str, str], List[Dict[str, Any]]]] = None,
) -> List[Dict[str, Any]]:
    if price_fetcher is not None:
        return price_fetcher(data_db, symbol, start_date, end_date)
    return _load_hfq_price_rows(
        data_db,
        symbol,
        start_date,
        end_date,
        projection={"trade_date": 1, "open": 1, "high": 1, "low": 1, "close": 1},
    )


def _load_hfq_price_rows(
    data_db,
    symbol: str,
    start_date: str,
    end_date: str,
    *,
    projection: Mapping[str, int],
) -> List[Dict[str, Any]]:
    """Read ``volume_price`` and apply hfq with ``stock_adj_factor``.

    Replaces the old ``data_manager.DataManager`` dependency. Mirrors its
    adjustment semantics: merge the factor by trade date, backfill a leading
    factor gap with the first known factor, apply ``stock_data_access``'s pure
    ``apply_adjustment``, and turn non-positive prices / NaNs into ``None``.
    """
    rows = list(
        data_db["volume_price"]
        .find(
            {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
            dict(projection),
        )
        .sort("trade_date", 1)
    )
    if not rows:
        return []
    frame = pd.DataFrame(rows).drop_duplicates(subset=["trade_date"], keep="last")
    factors = list(
        data_db["stock_adj_factor"].find(
            {"symbol": symbol, "trade_date": {"$gte": start_date, "$lte": end_date}},
            {"_id": 0, "trade_date": 1, "adj_factor": 1},
        )
    )
    if factors:
        factor_frame = pd.DataFrame(factors).drop_duplicates(
            subset=["trade_date"], keep="last"
        )
        frame = frame.merge(factor_frame, on="trade_date", how="left")
        frame["adj_factor"] = frame["adj_factor"].ffill().bfill()
    else:
        frame["adj_factor"] = pd.NA

    from stock_data_access.prices import apply_adjustment

    adjusted = apply_adjustment(frame, adjust="hfq")
    for column in ("open", "high", "low", "close", "pre_close"):
        if column in adjusted.columns:
            adjusted.loc[
                pd.to_numeric(adjusted[column], errors="coerce") <= 0, column
            ] = pd.NA

    records: List[Dict[str, Any]] = []
    for record in adjusted.to_dict("records"):
        clean: Dict[str, Any] = {}
        for key, value in record.items():
            try:
                clean[key] = None if pd.isna(value) else value
            except (TypeError, ValueError):
                clean[key] = value
        records.append(clean)
    return records


def _latest_adj_factor(data_db, symbol: str, as_of_date: str) -> Optional[float]:
    """Latest cumulative adjustment factor for converting hfq levels to raw."""
    try:
        row = data_db["stock_adj_factor"].find_one(
            {"symbol": symbol, "trade_date": {"$lte": as_of_date}},
            {"_id": 0, "adj_factor": 1},
            sort=[("trade_date", -1)],
        )
    except Exception:
        return None
    if not row:
        return None
    try:
        factor = float(row.get("adj_factor"))
    except (TypeError, ValueError):
        return None
    return factor if factor > 0 else None


def _with_raw_price_levels(result: Dict[str, Any], factor: Optional[float]) -> Dict[str, Any]:
    """Keep the hfq trigger fields and add raw-equivalent levels for display.

    The trigger must stay on the hfq series (aligned with research and immune to
    dividend/split jumps). The levels a human reads, however, should be
    comparable to the live quote; divide by the latest factor.
    """
    out = dict(result)
    if factor is None or factor <= 0:
        return out
    out["adj_factor"] = factor
    out["price_basis"] = "hfq"
    for key in ("peak_high", "close", "stop_floor"):
        value = out.get(key)
        if value is None:
            continue
        try:
            out[f"raw_{key}"] = float(value) / factor
        except (TypeError, ValueError):
            continue
    return out


def _lineage_plan_ids(
    collections: TradingCollections,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
) -> List[str]:
    query: Dict[str, Any] = {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
    }
    if user_id:
        query["user_id"] = user_id
    return [str(row.get("plan_id")) for row in collections.trade_plans.find(query, {"_id": 0, "plan_id": 1}) if row.get("plan_id")]


def resolve_position_entry_dates(
    collections: TradingCollections,
    *,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    positions: Iterable[Mapping[str, Any]],
    fallback_date: Optional[str] = None,
) -> Dict[str, str]:
    symbols = sorted({str(row.get("symbol")) for row in positions if row.get("symbol")})
    if not symbols:
        return {}
    plan_ids = _lineage_plan_ids(collections, strategy_template_id, params_hash, user_id)
    entry_dates: Dict[str, str] = {}
    rows = list(
        collections.paper_executions.find(
            {"plan_id": {"$in": plan_ids}, "symbol": {"$in": symbols}, "action": "buy"},
            {"_id": 0, "symbol": 1, "execute_date": 1, "price_trade_date": 1, "created_at": 1},
        )
    )
    fallback = normalize_date(fallback_date) if fallback_date else None
    # Prefer the earliest buy on/after the current cycle anchor so peak tracking
    # starts at position entry (aligned with score_backtester entry_idx).
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        trade_date = normalize_date(row.get("execute_date") or row.get("price_trade_date"))
        if not trade_date:
            continue
        if fallback and trade_date < fallback:
            continue
        filtered.append({**row, "_trade_date": trade_date})
    filtered.sort(key=lambda row: (str(row.get("_trade_date") or ""), str(row.get("created_at") or "")))
    for row in filtered:
        symbol = str(row.get("symbol") or "")
        if not symbol or symbol in entry_dates:
            continue
        entry_dates[symbol] = str(row["_trade_date"])
    for symbol in symbols:
        if symbol not in entry_dates and fallback:
            entry_dates[symbol] = fallback
    return entry_dates


def get_trailing_stop_cycle_exclusions(
    collections: TradingCollections,
    *,
    strategy_template_id: str,
    params_hash: str,
    user_id: Optional[str],
    since_date: Optional[str],
) -> List[str]:
    if not since_date:
        return []
    query: Dict[str, Any] = {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "status": "completed",
        "base_date": {"$gt": since_date},
        "triggered_symbols": {"$ne": []},
    }
    if user_id:
        query["user_id"] = user_id
    excluded: set[str] = set()
    for run in collections.trailing_stop_runs.find(query, {"_id": 0, "triggered_symbols": 1}):
        excluded.update(str(symbol) for symbol in (run.get("triggered_symbols") or []) if symbol)
    return sorted(excluded)


def _build_summary_message(*, symbols_checked: int, triggered_count: int, trailing_stop_pct: float, verbose: bool) -> str:
    pct_label = f"{trailing_stop_pct * 100:g}%"
    if triggered_count:
        return f"已检查 {symbols_checked} 只，{triggered_count} 只跌破 {pct_label} 移动止盈"
    if verbose:
        return f"已检查 {symbols_checked} 只，无标的跌破 {pct_label} 移动止盈"
    return f"已检查 {symbols_checked} 只，触发 {triggered_count} 只"


def _resolve_combo_config(
    collections: TradingCollections,
    sub: Mapping[str, Any],
) -> Tuple[Dict[str, Any], Optional[float]]:
    strategy_template_id = str(sub.get("strategy_template_id") or "")
    preset_id = sub.get("preset_id")
    params_override = sub.get("params_override") or {}
    template = dict(load_strategy_config(collections, strategy_template_id))
    preset = (
        collections.param_presets.find_one({"preset_id": preset_id}, {"_id": 0})
        if preset_id
        else load_default_param_preset(collections, strategy_template_id)
    )
    config = effective_strategy_config(template, preset, params_override)
    trailing_stop_pct = resolve_trailing_stop_pct_for_lineage(
        collections,
        strategy_template_id=strategy_template_id,
        params_hash=str(sub.get("params_hash") or ""),
        user_id=sub.get("user_id"),
        config=config,
    )
    return config, trailing_stop_pct


def run_trailing_stop_monitor(
    collections: TradingCollections,
    data_db,
    *,
    strategy_template_id: str,
    params_hash: str,
    user_id: str,
    base_date: str,
    preset_id: Optional[str] = None,
    params_override: Optional[Dict[str, Any]] = None,
    verbose: Optional[bool] = None,
    price_fetcher: Optional[Callable[[Any, str, str, str], List[Dict[str, Any]]]] = None,
    generate_sell_plan: bool = True,
) -> Dict[str, Any]:
    ensure_trading_indexes(collections)
    base_date = normalize_date(base_date) or base_date
    verbose = is_trailing_stop_monitor_verbose() if verbose is None else bool(verbose)
    existing = collections.trailing_stop_runs.find_one(
        {
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": user_id,
            "base_date": base_date,
            "status": "completed",
        },
        {"_id": 0, "run_id": 1},
    )
    if existing:
        return {"skipped": True, "reason": "run_exists", "run_id": existing.get("run_id")}

    sub = {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
        "preset_id": preset_id,
        "params_override": params_override or {},
    }
    config, trailing_stop_pct = _resolve_combo_config(collections, sub)
    if trailing_stop_pct is None:
        return {"skipped": True, "reason": "trailing_stop_disabled"}

    snapshot = latest_position_snapshot(
        collections,
        strategy_template_id,
        base_date,
        user_id=user_id,
        params_hash=params_hash,
    )
    positions = [row for row in (snapshot.get("positions") or []) if int(row.get("shares", 0) or 0) > 0]
    if not positions:
        return {"skipped": True, "reason": "no_positions"}

    previous_rebalance = collections.trade_plans.find_one(
        strategy_rebalance_mongo_filter(
            strategy_template_id=strategy_template_id,
            params_hash=params_hash,
            user_id=user_id,
        ),
        sort=[("base_date", DESCENDING)],
        projection={"_id": 0, "base_date": 1},
    )
    fallback_entry = (previous_rebalance or {}).get("base_date")
    entry_dates = resolve_position_entry_dates(
        collections,
        strategy_template_id=strategy_template_id,
        params_hash=params_hash,
        user_id=user_id,
        positions=positions,
        fallback_date=fallback_entry,
    )

    items: List[Dict[str, Any]] = []
    triggered_symbols: List[str] = []
    skipped_no_price = 0
    for pos in positions:
        symbol = str(pos.get("symbol") or "")
        if not symbol:
            continue
        entry_date = entry_dates.get(symbol)
        if not entry_date:
            skipped_no_price += 1
            item = {
                "symbol": symbol,
                "shares": int(pos.get("shares", 0) or 0),
                "entry_date": None,
                "peak_high": None,
                "close": None,
                "stop_floor": None,
                "triggered": False,
                "trigger_date": None,
                "distance_to_stop_pct": None,
                "skipped_reason": "missing_entry_date",
            }
            if verbose:
                items.append(item)
            continue
        bars = fetch_trailing_stop_price_rows(data_db, symbol, entry_date, base_date, price_fetcher=price_fetcher)
        result = check_trailing_stop(
            bars,
            entry_date=entry_date,
            base_date=base_date,
            trailing_pct=trailing_stop_pct,
        )
        result = _with_raw_price_levels(
            result, _latest_adj_factor(data_db, symbol, base_date)
        )
        if result.get("skipped_reason") == "missing_price_window":
            skipped_no_price += 1
        item = {
            "symbol": symbol,
            "name": pos.get("name", ""),
            "shares": int(pos.get("shares", 0) or 0),
            "entry_date": entry_date,
            **result,
        }
        if result.get("triggered"):
            triggered_symbols.append(symbol)
            items.append(item)
        elif verbose:
            items.append(item)

    now = datetime.utcnow()
    run_id = str(uuid.uuid4())
    summary = {
        "symbols_checked": len(positions),
        "triggered_count": len(triggered_symbols),
        "skipped_no_price": skipped_no_price,
        "message": _build_summary_message(
            symbols_checked=len(positions),
            triggered_count=len(triggered_symbols),
            trailing_stop_pct=trailing_stop_pct,
            verbose=verbose,
        ),
    }
    run_doc = {
        "run_id": run_id,
        "base_date": base_date,
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
        "trailing_stop_pct": trailing_stop_pct,
        "status": "completed",
        "summary": summary,
        "items": items,
        "triggered_symbols": triggered_symbols,
        "sell_plan_id": None,
        "verbosity": "verbose" if verbose else "triggers_only",
        "created_at": now,
        "updated_at": now,
    }
    sell_plan_id = None
    sell_plan_skipped_reason = None
    if generate_sell_plan and triggered_symbols:
        same_day_rebalance = collections.trade_plans.find_one(
            strategy_rebalance_mongo_filter(
                strategy_template_id=strategy_template_id,
                params_hash=params_hash,
                user_id=user_id,
                base_date=base_date,
                status={"$in": sorted(EXECUTABLE_REBALANCE_STATUSES)},
            ),
            {"_id": 0, "plan_id": 1, "status": 1},
        )
        if same_day_rebalance:
            # Same-day rebalance rebuilds the book; a parallel trailing sell would
            # race on snapshots and fixed deltas. Record the trigger only.
            sell_plan_skipped_reason = "same_day_rebalance"
            summary["sell_plan_skipped_reason"] = sell_plan_skipped_reason
            summary["message"] = (
                f"{summary['message']}；同日已有调仓计划 "
                f"{same_day_rebalance.get('plan_id')}，不另生成止盈卖单"
            )
        else:
            sell_plan = generate_trailing_stop_sell_plan(
                collections,
                data_db,
                run_doc=run_doc,
                positions=positions,
                config=config,
            )
            sell_plan_id = sell_plan.get("plan_id")
            run_doc["sell_plan_id"] = sell_plan_id
    collections.trailing_stop_runs.insert_one(run_doc)
    return {
        "skipped": False,
        "run_id": run_id,
        "summary": summary,
        "triggered_symbols": triggered_symbols,
        "sell_plan_id": sell_plan_id,
        "sell_plan_skipped_reason": sell_plan_skipped_reason,
    }


def generate_trailing_stop_sell_plan(
    collections: TradingCollections,
    data_db,
    *,
    run_doc: Mapping[str, Any],
    positions: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    base_date = normalize_date(run_doc.get("base_date")) or str(run_doc.get("base_date") or "")
    strategy_template_id = str(run_doc.get("strategy_template_id") or "")
    params_hash = str(run_doc.get("params_hash") or "")
    user_id = str(run_doc.get("user_id") or "")
    triggered = {str(symbol) for symbol in (run_doc.get("triggered_symbols") or []) if symbol}
    execute_date = next_trading_date(data_db, base_date)
    if not execute_date:
        raise ValueError(f"No next trading date after {base_date} for trailing-stop sell plan")

    existing = collections.trade_plans.find_one(
        {
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": user_id,
            "base_date": base_date,
            "plan_type": TRAILING_STOP_PLAN_TYPE,
            "origin": TRAILING_STOP_ORIGIN,
        },
        {"_id": 0, "plan_id": 1},
    )
    if existing:
        existing_plan_id = str(existing.get("plan_id") or "")
        item_count = collections.trade_plan_items.count_documents({"plan_id": existing_plan_id}) if existing_plan_id else 0
        if item_count > 0:
            return existing
        # Incomplete prior write: drop the empty shell so this attempt can recreate.
        if existing_plan_id:
            collections.trade_plans.delete_one({"plan_id": existing_plan_id})
            collections.trade_plan_items.delete_many({"plan_id": existing_plan_id})

    position_by_symbol = {str(row.get("symbol")): row for row in positions if row.get("symbol")}
    symbols = sorted(triggered)
    close_prices, price_dates = latest_close_prices_on_or_before(data_db, symbols, base_date)
    template = dict(load_strategy_config(collections, strategy_template_id))
    plan_id = f"{build_user_plan_id(strategy_template_id, base_date, user_id, params_hash)}_trailing_{uuid.uuid4().hex[:6]}"
    now = datetime.utcnow()
    items: List[Dict[str, Any]] = []
    sell_amount = 0.0
    for rank, symbol in enumerate(symbols, start=1):
        pos = position_by_symbol.get(symbol) or {}
        shares = int(pos.get("shares", 0) or 0)
        if shares <= 0:
            continue
        price = close_prices.get(symbol)
        amount = shares * float(price or 0)
        sell_amount += amount
        items.append(
            {
                "plan_id": plan_id,
                "user_id": user_id,
                "rank": rank,
                "symbol": symbol,
                "name": pos.get("name", ""),
                "industry": pos.get("industry", ""),
                "action": "sell",
                "current_shares": shares,
                "target_shares": 0,
                "delta_shares": -shares,
                "estimated_price": price,
                "price_trade_date": price_dates.get(symbol),
                "blockers": [],
            }
        )
    if not items:
        raise ValueError("trailing-stop sell plan has no sellable positions")

    paper_execution_mode = str(config.get("paper_execution_mode") or PAPER_EXECUTION_MODE_MANUAL_REVIEW)
    auto_shadow = paper_execution_mode == PAPER_EXECUTION_MODE_AUTO_SHADOW
    plan_doc = {
        "plan_id": plan_id,
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
        "strategy_name": strategy_display_name(template, strategy_template_id),
        "base_date": base_date,
        "execute_date": execute_date,
        "plan_type": TRAILING_STOP_PLAN_TYPE,
        "record_kind": RECORD_KIND_TRADE_PLAN,
        "origin": TRAILING_STOP_ORIGIN,
        "trailing_stop_run_id": run_doc.get("run_id"),
        "trailing_stop_pct": run_doc.get("trailing_stop_pct"),
        "is_rebalance_day": False,
        "trading_enabled": True,
        "executable": True,
        "non_executable_reason": None,
        "paper_execution_mode": paper_execution_mode,
        # Insert as non-executable first so a crash between plan/items writes cannot
        # leave an approved empty sell plan that later retries would reuse.
        "status": "generated",
        "reviewer": None,
        "approved_at": None,
        "params_snapshot": {
            **dict(config),
            LINEAGE_TRAILING_STOP_FIELD: run_doc.get("trailing_stop_pct"),
        },
        "summary": {
            "selected_count": len(items),
            "buy_count": 0,
            "sell_count": len(items),
            "hold_count": 0,
            "estimated_sell_amount": sell_amount,
            "estimated_turnover": sell_amount,
            "trailing_stop_triggered_symbols": symbols,
        },
        "created_at": now,
        "updated_at": now,
    }
    collections.trade_plans.insert_one(plan_doc)
    try:
        collections.trade_plan_items.insert_many(items)
        final_status = "approved" if auto_shadow else "needs_review"
        status_update: Dict[str, Any] = {
            "status": final_status,
            "updated_at": datetime.utcnow(),
        }
        if auto_shadow:
            status_update["reviewer"] = AUTO_PAPER_APPROVE_REVIEWER
            status_update["approved_at"] = datetime.utcnow()
        collections.trade_plans.update_one({"plan_id": plan_id}, {"$set": status_update})
        plan_doc.update(status_update)
    except Exception:
        collections.trade_plan_items.delete_many({"plan_id": plan_id})
        collections.trade_plans.delete_one({"plan_id": plan_id})
        raise
    return plan_doc


def latest_trailing_stop_run(
    collections: TradingCollections,
    *,
    strategy_template_id: str,
    params_hash: str,
    user_id: str,
    include_items: bool = False,
) -> Dict[str, Any]:
    projection = {"_id": 0}
    if not include_items:
        projection["items"] = 0
    doc = collections.trailing_stop_runs.find_one(
        {
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": user_id,
            "status": "completed",
        },
        projection,
        sort=[("base_date", DESCENDING), ("created_at", DESCENDING)],
    )
    return doc or {}


def list_trailing_stop_runs(
    collections: TradingCollections,
    *,
    strategy_template_id: str,
    params_hash: str,
    user_id: str,
    limit: int = 7,
    triggered_only: bool = False,
    include_items: bool = False,
) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {
        "strategy_template_id": strategy_template_id,
        "params_hash": params_hash,
        "user_id": user_id,
        "status": "completed",
    }
    if triggered_only:
        query["triggered_symbols"] = {"$ne": []}
    projection = {"_id": 0}
    if not include_items:
        projection["items"] = 0
    cursor = collections.trailing_stop_runs.find(query, projection).sort([("base_date", DESCENDING), ("created_at", DESCENDING)]).limit(max(1, int(limit)))
    return list(cursor)


def enqueue_trailing_stop_tasks(
    collections: TradingCollections,
    base_date: str,
    *,
    lookback_days: int = 120,
    source: str = "trailing_stop_scheduler",
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    ensure_trading_indexes(collections)
    base_date = normalize_date(base_date) or base_date
    now = now or datetime.utcnow()
    reclaim_stale_trailing_stop_tasks(collections, now=now)
    subscriptions = find_active_plan_subscriptions(collections, as_of_date=base_date, lookback_days=lookback_days)
    enqueued: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    for sub in subscriptions:
        strategy_template_id = sub["strategy_template_id"]
        params_hash = sub["params_hash"]
        user_id = sub["user_id"]
        combo = {"strategy_template_id": strategy_template_id, "params_hash": params_hash, "user_id": user_id}
        config, trailing_stop_pct = _resolve_combo_config(collections, sub)
        if trailing_stop_pct is None:
            skipped.append({**combo, "reason": "trailing_stop_disabled"})
            continue
        existing_run = collections.trailing_stop_runs.find_one(
            {**combo, "base_date": base_date, "status": "completed"},
            {"_id": 0, "run_id": 1},
        )
        if existing_run:
            skipped.append({**combo, "reason": "run_exists", "run_id": existing_run.get("run_id")})
            continue
        existing_task = collections.trailing_stop_tasks.find_one(
            {
                **combo,
                "base_date": base_date,
                "status": {"$in": list(TRAILING_STOP_TASK_DEDUP_STATUSES)},
            },
            {"_id": 0, "task_id": 1},
        )
        if existing_task:
            skipped.append({**combo, "reason": "task_exists", "task_id": existing_task.get("task_id")})
            continue
        task = {
            "task_id": str(uuid.uuid4()),
            "strategy_template_id": strategy_template_id,
            "params_hash": params_hash,
            "user_id": user_id,
            "base_date": base_date,
            "preset_id": sub.get("preset_id"),
            "params_override": sub.get("params_override") or {},
            "source": source,
            "status": "pending",
            "attempts": 0,
            "run_id": None,
            "error_message": None,
            "created_at": now,
            "updated_at": now,
        }
        collections.trailing_stop_tasks.insert_one(task)
        enqueued.append({**combo, "task_id": task["task_id"]})
    return {
        "base_date": base_date,
        "subscriptions": len(subscriptions),
        "enqueued": enqueued,
        "skipped": skipped,
    }


def process_trailing_stop_task(
    collections: TradingCollections,
    data_db,
    task: Mapping[str, Any],
    *,
    price_fetcher: Optional[Callable[[Any, str, str, str], List[Dict[str, Any]]]] = None,
) -> Dict[str, Any]:
    result = run_trailing_stop_monitor(
        collections,
        data_db,
        strategy_template_id=str(task["strategy_template_id"]),
        params_hash=str(task["params_hash"]),
        user_id=str(task["user_id"]),
        base_date=str(task["base_date"]),
        preset_id=task.get("preset_id"),
        params_override=task.get("params_override") or {},
        price_fetcher=price_fetcher,
    )
    return result
