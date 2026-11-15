"""Research-vs-live selection parity for promoting a research combo.

The research backtester and the live construction path share score semantics but
not every execution detail (lot-size backfill, point-in-time universe coverage,
industry taxonomy). Activation is blocked when a symbol difference cannot be
explained by those known live-only behaviours, so the default is to surface the
difference instead of silently switching the book.

Pure read path: builds a report, never writes plans or lineage state.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from portfolio.calendar import compute_next_rebalance_date, normalize_date
from portfolio.construction import (
    apply_regime_cash_to_selection,
    load_score_frame,
    select_trade_plan_portfolio,
)
from portfolio.market_data import latest_close_prices_on_or_before
from portfolio.record_kind import strategy_rebalance_mongo_filter
from portfolio.regime_cash import resolve_live_regime_cash
from portfolio.scheduler import find_active_plan_subscriptions

PARITY_STATUS_CLEAN = "clean"
PARITY_STATUS_DIFF = "diff"
PARITY_STATUS_UNAVAILABLE = "unavailable"

# Live construction backfills unbuyable top-N names from the enlarged candidate
# pool; the research backtester leaves the slot empty. A live-only superset is
# therefore the one symbol difference we can explain without replaying research.
_EXPLAINED_DATE_STATUSES = {"match", "live_backfill", "lot_size_backfill"}


def _normalized_symbols(values: Iterable[Any]) -> List[str]:
    return sorted({str(value).strip() for value in values if str(value or "").strip()})


def _period_symbols(period: Dict[str, Any], trades: Sequence[Dict[str, Any]]) -> List[str]:
    score_date = normalize_date(period.get("score_date"))
    return _normalized_symbols(
        trade.get("symbol")
        for trade in trades
        if normalize_date(trade.get("score_date")) == score_date
    )


def _is_cash(action: Any) -> bool:
    # ``off`` means the overlay is disabled, which is the same book as invest.
    return str(action or "").strip().lower() == "cash"


def build_research_live_parity(
    *,
    collections,
    app_db,
    data_db,
    strategy_template_id: str,
    config: Dict[str, Any],
    user_id: Optional[str],
    base_date: str,
    research_periods: Sequence[Dict[str, Any]],
    research_trades: Sequence[Dict[str, Any]],
    target_amount: float,
    excluded_symbols: Optional[Iterable[str]] = None,
    max_dates: int = 5,
    ml_db=None,
) -> Dict[str, Any]:
    """Compare research holdings with live construction on recent rebalance dates.

    ``research_periods`` carries one entry per research rebalance period;
    ``research_trades`` is the combo's trade list (same ``score_date`` key).
    Only the most recent ``max_dates`` periods with holdings (or a cash signal)
    are checked.
    """
    if ml_db is None:
        from portfolio.db import get_ml_db

        ml_db = get_ml_db()
    universe = str(config.get("universe_index") or "csi1000")
    normalized_base = normalize_date(base_date)
    periods: List[Dict[str, Any]] = []
    seen = set()
    for period in reversed(list(research_periods or [])):
        score_date = normalize_date(period.get("score_date"))
        if not score_date or score_date in seen:
            continue
        if normalized_base and score_date > normalized_base:
            continue
        symbols = _period_symbols(period, research_trades)
        if not symbols and not _is_cash(period.get("regime_action")):
            # The research window's last period often has no forward return yet.
            continue
        seen.add(score_date)
        periods.append({"period": period, "score_date": score_date, "symbols": symbols})
        if len(periods) >= max(1, int(max_dates)):
            break
    periods.reverse()

    dates: List[Dict[str, Any]] = []
    for item in periods:
        score_date = item["score_date"]
        research_symbols = item["symbols"]
        period = item["period"]
        live_symbols: List[str] = []
        live_regime_action = "off"
        unavailable_reason = None
        regime_error = None
        prices: Dict[str, float] = {}
        try:
            frame = load_score_frame(
                app_db["stock_scores"],
                score_date,
                universe,
                config,
                data_db=data_db,
                ml_db=ml_db,
            )
        except Exception as exc:  # pragma: no cover - defensive around data access
            frame = None
            unavailable_reason = f"score_frame_error: {exc}"
        if frame is not None and frame.empty:
            unavailable_reason = "live_score_frame_empty"
        if frame is not None and not frame.empty:
            try:
                symbols_in_frame = _normalized_symbols(frame.get("symbol", []))
                prices, _ = latest_close_prices_on_or_before(
                    data_db, symbols_in_frame, score_date
                )
                selected = select_trade_plan_portfolio(
                    frame,
                    config,
                    prices,
                    max(float(target_amount or 0.0), 0.0),
                    excluded_symbols=excluded_symbols,
                )
                try:
                    regime = resolve_live_regime_cash(
                        config=config,
                        data_db=data_db,
                        score_date=score_date,
                        universe_index=universe,
                    )
                    live_regime_action = str(regime.get("action") or "off")
                except Exception as exc:
                    regime_error = str(exc)
                selected = apply_regime_cash_to_selection(selected, live_regime_action)
                live_symbols = _normalized_symbols(selected.get("symbol", []))
            except Exception as exc:  # pragma: no cover - defensive around construction
                unavailable_reason = f"live_selection_error: {exc}"

        research_set = set(research_symbols)
        live_set = set(live_symbols)
        research_cash = _is_cash(period.get("regime_action"))
        live_cash = _is_cash(live_regime_action)
        research_only = sorted(research_set - live_set)
        live_only = sorted(live_set - research_set)

        rank_by_symbol: Dict[str, int] = {}
        if frame is not None and not frame.empty and "symbol" in frame.columns:
            rank_by_symbol = {
                str(symbol): rank
                for rank, symbol in enumerate(frame["symbol"].tolist(), start=1)
            }
        min_buy_shares = max(1, int(config.get("lot_size", 100))) * max(
            1, int(config.get("min_buy_lots", 1))
        )
        research_missing_frame = [
            symbol for symbol in research_only if symbol not in rank_by_symbol
        ]
        research_only_unbuyable = [
            symbol
            for symbol in research_only
            if symbol in rank_by_symbol
            and (
                prices.get(symbol) is None
                or float(prices[symbol]) * min_buy_shares > float(target_amount or 0.0)
            )
        ]
        research_ranked = [
            rank_by_symbol[symbol] for symbol in research_symbols if symbol in rank_by_symbol
        ]
        max_research_rank = max(research_ranked) if research_ranked else 0
        live_only_high_rank = [
            symbol
            for symbol in live_only
            if symbol in rank_by_symbol
            and max_research_rank
            and rank_by_symbol[symbol] <= max_research_rank
        ]
        lot_explained = (
            not research_missing_frame
            and len(research_only_unbuyable) == len(research_only)
            and not live_only_high_rank
        )
        if unavailable_reason:
            status = "unavailable"
        elif research_cash != live_cash:
            status = "regime_mismatch"
        elif research_set == live_set:
            status = "match"
        elif research_set.issubset(live_set) and not live_only_high_rank:
            status = "live_backfill"
        elif lot_explained and live_set.issuperset(research_set - set(research_only)):
            status = "lot_size_backfill"
        else:
            status = "selection_diff"
        dates.append(
            {
                "score_date": score_date,
                "status": status,
                "research_symbols": research_symbols,
                "live_symbols": live_symbols,
                "research_only": research_only,
                "live_only": live_only,
                "research_only_unbuyable": research_only_unbuyable,
                "live_only_high_rank": live_only_high_rank,
                "research_missing_frame": research_missing_frame,
                "overlap": len(research_set & live_set),
                "research_regime_action": period.get("regime_action"),
                "live_regime_action": live_regime_action,
                "regime_error": regime_error,
                "unavailable_reason": unavailable_reason,
                "explained": status in _EXPLAINED_DATE_STATUSES,
            }
        )

    matched = sum(1 for row in dates if row["status"] == "match")
    backfilled = sum(
        1 for row in dates if row["status"] in {"live_backfill", "lot_size_backfill"}
    )
    unexplained_dates = [row for row in dates if not row["explained"]]
    if not dates:
        status = PARITY_STATUS_UNAVAILABLE
    elif unexplained_dates:
        status = PARITY_STATUS_DIFF
    else:
        status = PARITY_STATUS_CLEAN
    return {
        "status": status,
        "max_dates": max(1, int(max_dates)),
        "checked_dates": len(dates),
        "matched_dates": matched,
        "backfilled_dates": backfilled,
        "unexplained_dates": len(unexplained_dates),
        "unexplained": [
            {
                "score_date": row["score_date"],
                "status": row["status"],
                "research_only": row["research_only"],
                "live_only": row["live_only"],
                "research_only_unbuyable": row["research_only_unbuyable"],
                "live_only_high_rank": row["live_only_high_rank"],
                "research_missing_frame": row["research_missing_frame"],
                "research_regime_action": row["research_regime_action"],
                "live_regime_action": row["live_regime_action"],
                "unavailable_reason": row["unavailable_reason"],
            }
            for row in unexplained_dates
        ],
        "dates": dates,
        "generated_at": datetime.utcnow().isoformat(),
    }


def suggest_effective_date(
    *,
    collections,
    data_db,
    user_id: Optional[str],
    base_date: str,
    fallback: Optional[str],
) -> Optional[str]:
    """Next rebalance date of the user's most recently committed live lineage.

    Falls back to the preview plan's own next rebalance date (a fresh lineage)
    when nothing is committed yet.
    """
    base = normalize_date(base_date)
    if not user_id or not base:
        return normalize_date(fallback)
    try:
        subscriptions = find_active_plan_subscriptions(
            collections, as_of_date=base, lookback_days=120
        )
    except Exception:
        subscriptions = []
    next_dates = []
    for subscription in subscriptions:
        if str(subscription.get("user_id") or "") != str(user_id):
            continue
        # Anchor on the latest *committed* plan. A generated-but-unexecuted plan
        # in the same lineage must not reset the cadence clock.
        latest = collections.trade_plans.find_one(
            strategy_rebalance_mongo_filter(
                strategy_template_id=subscription.get("strategy_template_id"),
                params_hash=subscription.get("params_hash"),
                user_id=user_id,
                base_date=subscription.get("source_base_date"),
                status={"$nin": ["rejected", "cancelled", "failed"]},
            ),
            sort=[("base_date", -1)],
            projection={"_id": 0, "base_date": 1, "params_snapshot": 1},
        )
        if not latest:
            continue
        params = latest.get("params_snapshot") or {}
        rebalance_days = int(params.get("rebalance_days") or 20)
        next_date = compute_next_rebalance_date(data_db, latest.get("base_date"), rebalance_days)
        if not next_date:
            continue
        next_dates.append(max(next_date, base))
    if not next_dates:
        return normalize_date(fallback)
    # Activation replaces every committed lineage of this template, so the
    # earliest upcoming rebalance is the last date on which leaving them running
    # would still be consistent with their own cadence.
    return min(next_dates)
