"""Score-frame loading and portfolio construction helpers.

``apply_regime_cash_to_selection`` is the live cash overlay: when the
index MA label is not bull, construction returns an empty book. Callers
decide whether to enable it; this helper only applies an already-resolved action.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd
from pymongo import DESCENDING

from portfolio.calendar import normalize_date
from shared.scores import compute_score_series, resolve_score_date
from shared.volatility import compute_volatility_features_from_rows, fetch_volume_price_rows

MODEL_SCORE_MAX_STALENESS_DAYS = 45


def load_model_score_frame(
    ml_db,
    model_id: str,
    score_date: str,
    *,
    column: str = "model_score",
    max_staleness_days: int = MODEL_SCORE_MAX_STALENESS_DAYS,
    strict: bool = True,
) -> tuple[pd.DataFrame, Optional[str]]:
    """Load the latest model prediction at or before ``score_date``.

    Model scores are published on a slower cadence than stock_scores (monthly
    walk-forward or daily shadow). Carry the latest available score forward,
    but refuse to trade one older than ``max_staleness_days`` so a stopped
    shadow job cannot silently keep an old book alive.
    """
    empty = pd.DataFrame(columns=["symbol", column])
    normalized = normalize_date(score_date)
    if ml_db is None or not model_id:
        if strict:
            raise ValueError(f"model score requires ml_db and model_id (score_date={normalized})")
        return empty, None
    latest = ml_db["model_predictions"].find_one(
        {"model_id": model_id, "trade_date": {"$lte": normalized}},
        {"_id": 0, "trade_date": 1},
        sort=[("trade_date", -1)],
    )
    if not latest or not latest.get("trade_date"):
        if strict:
            raise ValueError(f"no model_predictions for model_id={model_id} at/before {normalized}")
        return empty, None
    pred_date = str(latest["trade_date"])
    try:
        age_days = (
            datetime.strptime(normalized, "%Y%m%d") - datetime.strptime(pred_date, "%Y%m%d")
        ).days
    except ValueError:
        age_days = 0
    if age_days > max_staleness_days:
        message = (
            f"model_predictions for {model_id} are stale: latest {pred_date}, "
            f"score_date {normalized} ({age_days}d > {max_staleness_days}d)"
        )
        if strict:
            raise ValueError(message)
        return empty, pred_date
    rows = list(
        ml_db["model_predictions"].find(
            {"model_id": model_id, "trade_date": pred_date},
            {"_id": 0, "ts_code": 1, "score": 1},
        )
    )
    if not rows:
        if strict:
            raise ValueError(f"model_predictions for {model_id} on {pred_date} are empty")
        return empty, pred_date
    frame = pd.DataFrame(rows).rename(columns={"ts_code": "symbol", "score": column})
    frame = frame.dropna(subset=["symbol", column]).drop_duplicates(subset=["symbol"], keep="last")
    return frame, pred_date


def _load_stock_info(data_db, symbols: Sequence[str]) -> pd.DataFrame:
    """Current name/industry metadata for the model cross-section.

    Only display/fallback fields: the industry cap itself uses PIT SW L1 from
    ``sw_index_member`` via ``attach_sw_l1``.
    """
    if data_db is None or not symbols:
        return pd.DataFrame()
    rows = list(
        data_db["stock_info"].find(
            {"symbol": {"$in": list(symbols)}},
            {"_id": 0, "symbol": 1, "name": 1, "industry": 1},
        )
    )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["symbol"], keep="last")


def load_index_universe_symbols_asof(
    data_db,
    universe_index: str,
    as_of_date: str,
) -> List[str]:
    """PIT index membership at ``as_of_date`` from ``quant_data.index_constituents``.

    Usable snapshots need both ``weight_trade_date`` and ``update_date`` at or
    before the as-of date; among them, the most recently published wins. This
    is the same rule as the ML panel loader: a weight snapshot published on
    2026-11-02 is not usable on 2026-10-30.
    """
    if data_db is None or not universe_index:
        return []
    as_of = normalize_date(as_of_date)
    if not as_of:
        return []
    doc = data_db["index_constituents"].find_one(
        {"index_code": universe_index, "weight_trade_date": {"$lte": as_of}, "update_date": {"$lte": as_of}},
        {"_id": 0, "weight_trade_date": 1, "update_date": 1},
        sort=[("update_date", DESCENDING), ("weight_trade_date", DESCENDING)],
    )
    if not doc:
        # Legacy docs may lack weight_trade_date; fall back to update_date only.
        doc = data_db["index_constituents"].find_one(
            {"index_code": universe_index, "update_date": {"$lte": as_of}},
            {"_id": 0, "update_date": 1},
            sort=[("update_date", DESCENDING)],
        )
    if not doc or not doc.get("update_date"):
        return []
    query: Dict[str, Any] = {"index_code": universe_index, "update_date": doc["update_date"]}
    if doc.get("weight_trade_date"):
        query["weight_trade_date"] = doc["weight_trade_date"]
    return sorted({str(s) for s in data_db["index_constituents"].distinct("symbol", query) if s})


def _apply_industry_cap(
    df: pd.DataFrame,
    data_db,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Attach PIT SW L1 and the ``industry_cap`` grouping column."""
    industry_l1 = config.get("industry_l1") or config.get("industry_l1_code")
    construction_mode = str(config.get("construction_mode") or "top_n")
    # Industry caps must group on the same taxonomy as research. ``industry``
    # in stock_scores is a fine-grained label (e.g. 铝), while the research
    # backtester caps on SW L1 (e.g. 有色金属). Without this the live book can
    # stack eight names from one SW L1 industry that research would cap.
    needs_sw_l1 = bool(industry_l1) or construction_mode in {"industry_capped", "industry_neutral"}
    if not needs_sw_l1 or data_db is None:
        return df
    from shared.industry_universe import (
        SW_L1_CODE_COL,
        attach_sw_l1,
        apply_industry_filter,
        resolve_industry_l1_codes,
        sw_l1_catalog,
    )

    sw_db = type("SwDb", (), {"data_db": data_db})()
    # Point-in-time membership: a stock must be judged against the industry
    # it belonged to on that score_date, not the one it belongs to today.
    df = attach_sw_l1(df, sw_db, as_of_column="score_date")
    if industry_l1:
        raw_values = [industry_l1] if isinstance(industry_l1, str) else list(industry_l1)
        codes = resolve_industry_l1_codes(raw_values, sw_l1_catalog(sw_db))
        if codes:
            df = apply_industry_filter(df, codes)
    if SW_L1_CODE_COL in df.columns:
        # Keep the fine-grained label for display; caps/neutral use this.
        # combine_first (not fillna(series)) keeps older pandas happy and
        # avoids the "Must specify a fill value" error on newer pandas.
        industry_cap = df[SW_L1_CODE_COL]
        if "industry" in df.columns:
            industry_cap = industry_cap.combine_first(df["industry"])
        df["industry_cap"] = industry_cap.fillna("Unknown")
    return df


def load_model_cross_section(
    ml_db,
    data_db,
    model_id: str,
    score_date: str,
    universe_index: str,
    config: Dict[str, Any],
) -> pd.DataFrame:
    """Plan cross-section built only from ``quant_ml.model_predictions``.

    This is the ``stock_scores``-free path for model-backed plans:

    1. take the latest prediction at/before ``score_date``;
    2. join name/industry from ``quant_data.stock_info``;
    3. intersect with the base-date index membership (so a name that left the
       index at ``score_date`` cannot be bought even though the model scored it
       the previous day);
    4. attach PIT SW L1 for industry caps/neutral.

    No ``stock_scores`` collection is read.
    """
    if ml_db is None:
        raise ValueError(
            f"model cross-section requires ml_db (model_id={model_id}, score_date={score_date})"
        )
    score_column = str(config.get("score_column") or "model_score")
    model_frame, pred_date = load_model_score_frame(
        ml_db,
        model_id,
        score_date,
        column=score_column,
    )
    if model_frame.empty:
        return pd.DataFrame()
    symbols = model_frame["symbol"].astype(str).tolist()
    info = _load_stock_info(data_db, symbols)
    frame = model_frame.merge(info, on="symbol", how="left") if not info.empty else model_frame.copy()
    frame["score_date"] = pred_date
    if universe_index:
        universe_symbols = load_index_universe_symbols_asof(data_db, universe_index, score_date)
        if universe_symbols:
            frame = frame[frame["symbol"].astype(str).isin(universe_symbols)]
    if frame.empty:
        return pd.DataFrame()
    frame["score_value"] = compute_score_series(frame, config)
    frame = frame.dropna(subset=["symbol", "score_value"])
    frame = _apply_industry_cap(frame, data_db, config)
    return frame.sort_values("score_value", ascending=False).reset_index(drop=True)


def resolve_plan_score_date(
    stock_scores,
    ml_db,
    base_date: str,
    universe_index: str,
    config: Dict[str, Any],
) -> str:
    """Plan cross-section date.

    Model-backed plans use ``base_date`` as the as-of cutoff; the model loader
    picks the latest prediction at/before it. Rule-score plans keep the legacy
    ``stock_scores`` date resolution.
    """
    if str(config.get("model_id") or "").strip():
        normalized = normalize_date(base_date)
        if not normalized:
            raise ValueError(f"invalid base_date {base_date!r}")
        return normalized
    return resolve_score_date(stock_scores, base_date, universe_index)


def _recent_trading_dates(data_db, base_date: str, count: int) -> List[str]:
    """Last ``count`` open SSE trading days at/before ``base_date``."""
    if data_db is None or count <= 0:
        return []
    rows = (
        data_db["trade_calendar"]
        .find(
            {
                "exchange": "SSE",
                "is_open": {"$in": [1, "1"]},
                "trade_date": {"$lte": base_date},
            },
            {"_id": 0, "trade_date": 1},
        )
        .sort("trade_date", DESCENDING)
        .limit(int(count))
    )
    dates = [str(row["trade_date"]) for row in rows if row.get("trade_date")]
    return list(reversed(dates))


def load_score_frame(
    stock_scores,
    score_date: str,
    universe_index: str,
    config: Dict[str, Any],
    data_db=None,
    ml_db=None,
) -> pd.DataFrame:
    score_type = str(config.get("score_type") or "")
    score_column = str(config.get("score_column") or "score_value")
    model_id = str(config.get("model_id") or "").strip()
    if score_type == "score_column" and model_id:
        # Model-backed plans must not read stock_scores: the cross-section is
        # the model's own prediction universe at the latest date <= score_date.
        return load_model_cross_section(
            ml_db,
            data_db,
            model_id,
            score_date,
            universe_index,
            config,
        )
    projection = {
        "_id": 0,
        "symbol": 1,
        "name": 1,
        "industry": 1,
        "score_date": 1,
        "growth_score": 1,
        "cycle_score": 1,
        "composite_score": 1,
    }
    if score_column and not score_column.startswith("composite_score."):
        projection[score_column] = 1
    if score_type == "weighted_score":
        for dimension in (config.get("score_weights") or {}):
            projection[f"{dimension}_score"] = 1
    query = {"score_date": score_date, "index_codes": universe_index}
    rows = list(stock_scores.find(query, projection))
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if score_type == "score_column" and score_column not in df.columns:
        raise ValueError(
            f"score_column {score_column!r} is not present in stock_scores and "
            "no model_id was provided"
        )
    df["score_value"] = compute_score_series(df, config)
    df = df.dropna(subset=["symbol", "score_value"])
    df = _apply_industry_cap(df, data_db, config)
    return df.sort_values("score_value", ascending=False).reset_index(drop=True)


def load_latest_universe_symbols(data_db, universe_index: str) -> List[str]:
    coll = data_db["index_constituents"]
    latest = coll.find_one(
        {"index_code": universe_index}, sort=[("update_date", DESCENDING), ("update_time", DESCENDING)], projection={"_id": 0, "update_date": 1}
    )
    if not latest or not latest.get("update_date"):
        return []
    rows = coll.find({"index_code": universe_index, "update_date": latest["update_date"]}, {"_id": 0, "symbol": 1})
    return [row["symbol"] for row in rows if row.get("symbol")]


def apply_regime_cash_to_selection(df: pd.DataFrame, action: str) -> pd.DataFrame:
    """Return an empty frame when the overlay says hold cash.

    Callers must resolve the action before invoking this helper.
    Non-cash actions leave ``df`` unchanged.
    """
    if str(action or "") != "cash":
        return df
    if df is None or df.empty:
        return df
    return df.iloc[0:0].copy()


def _industry_group_column(df: pd.DataFrame) -> Optional[str]:
    if "industry_cap" in df.columns:
        return "industry_cap"
    if "industry" in df.columns:
        return "industry"
    return None


def select_portfolio(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    top_n = int(config.get("top_n", 10))
    mode = str(config.get("construction_mode") or "top_n")
    valid = df.sort_values("score_value", ascending=False).copy()
    group_column = _industry_group_column(valid)
    # ``top_n_buffer`` only changes which held names survive, never which names
    # qualify: for a candidate-pool slice it is plain ``top_n``.
    if mode in {"top_n", "top_n_buffer"} or group_column is None:
        return valid.head(top_n).copy()
    if mode == "industry_capped":
        max_weight = float(config.get("max_industry_weight") or 0.3)
        max_count = max(1, int(top_n * max_weight))
        counts: Dict[str, int] = {}
        selected = []
        for _, row in valid.iterrows():
            industry = str(row.get(group_column) or "Unknown")
            if counts.get(industry, 0) >= max_count:
                continue
            selected.append(row)
            counts[industry] = counts.get(industry, 0) + 1
            if len(selected) >= top_n:
                break
        return pd.DataFrame(selected)
    if mode == "industry_neutral":
        industry_counts = valid[group_column].fillna("Unknown").replace("", "Unknown").value_counts()
        total_count = float(industry_counts.sum())
        if total_count <= 0:
            return valid.head(top_n).copy()
        raw_quotas = {industry: top_n * count / total_count for industry, count in industry_counts.items()}
        quotas = {industry: int(raw) for industry, raw in raw_quotas.items()}
        remaining = top_n - sum(quotas.values())
        remainders = sorted(raw_quotas.items(), key=lambda item: item[1] - int(item[1]), reverse=True)
        for industry, _ in remainders[:remaining]:
            quotas[industry] += 1
        rows = []
        selected_symbols = set()
        for industry, quota in quotas.items():
            group = valid[group_column].fillna("Unknown").replace("", "Unknown") == industry
            for _, row in valid[group].head(quota).iterrows():
                rows.append(row)
                selected_symbols.add(row.get("symbol"))
        if len(rows) < top_n:
            for _, row in valid.iterrows():
                if row.get("symbol") in selected_symbols:
                    continue
                rows.append(row)
                if len(rows) >= top_n:
                    break
        return pd.DataFrame(rows).sort_values("score_value", ascending=False).head(top_n)
    raise ValueError(f"Unsupported construction_mode: {mode}")


def _buffer_hold_n(config: Dict[str, Any], top_n: int) -> int:
    """Hold band for ``top_n_buffer``, floored at ``top_n``.

    A band narrower than the book would evict names for being ranked just
    outside it, which is exactly the churn the buffer exists to stop. Clamping
    makes ``holding_buffer_n < top_n`` behave as "no buffer" rather than as a
    stricter rule nobody asked for.
    """
    raw = config.get("holding_buffer_n")
    if raw is None:
        return top_n
    try:
        hold_n = int(raw)
    except (TypeError, ValueError):
        return top_n
    return max(top_n, hold_n)


def _order_with_buffer(pool: pd.DataFrame, held: set, hold_n: int) -> pd.DataFrame:
    """Ranked pool with held names inside the band promoted ahead of new names.

    Order only: eligibility (score cutoff, buyability, exclusions) is unchanged,
    so a buffered plan buys a subset of what ``top_n`` would consider. The
    caller truncates to ``top_n`` afterwards, which yields "keep what you hold
    while it is still ranked, then fill the rest from the top".
    """
    if not held or pool.empty:
        return pool
    ranked = pool.reset_index(drop=True)
    in_band = ranked.index < hold_n
    keep_mask = in_band & ranked["symbol"].astype(str).isin(held)
    if not keep_mask.any():
        return pool
    keep = ranked[keep_mask]
    rest = ranked[~keep_mask]
    return pd.concat([keep, rest], ignore_index=True)


def select_trade_plan_portfolio(
    df: pd.DataFrame,
    config: Dict[str, Any],
    prices: Dict[str, float],
    target_amount: float,
    excluded_symbols: Optional[Iterable[str]] = None,
    held_symbols: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    top_n = int(config.get("top_n", 10))
    lot_size = max(1, int(config.get("lot_size", 100)))
    pool_multiplier = max(1, int(config.get("candidate_pool_multiplier", 3)))
    min_buy_lots = max(1, int(config.get("min_buy_lots", 1)))
    mode = str(config.get("construction_mode") or "top_n")
    excluded = {str(s) for s in (excluded_symbols or []) if s}
    # Enlarge the candidate pool by the number of exclusions so user rejections
    # can be back-filled by the next-best names without running short of top_n.
    pool = df.sort_values("score_value", ascending=False).head(top_n * pool_multiplier + len(excluded)).reset_index(drop=True)
    if mode == "top_n_buffer":
        held = {str(s) for s in (held_symbols or []) if s and str(s) not in excluded}
        pool = _order_with_buffer(pool, held, _buffer_hold_n(config, top_n))
    rows = []
    selected_symbols = set()
    industry_counts: Dict[str, int] = {}
    min_buy_shares = lot_size * min_buy_lots
    max_industry_count = None
    industry_quotas: Dict[str, int] = {}
    group_column = _industry_group_column(pool)

    if mode == "industry_capped" and group_column is not None:
        max_weight = float(config.get("max_industry_weight") or 0.3)
        max_industry_count = max(1, int(top_n * max_weight))
    elif mode == "industry_neutral" and group_column is not None:
        industry_counts_all = pool[group_column].fillna("Unknown").replace("", "Unknown").value_counts()
        total_count = float(industry_counts_all.sum())
        if total_count > 0:
            raw_quotas = {industry: top_n * count / total_count for industry, count in industry_counts_all.items()}
            industry_quotas = {industry: int(raw) for industry, raw in raw_quotas.items()}
            remaining = top_n - sum(industry_quotas.values())
            remainders = sorted(raw_quotas.items(), key=lambda item: item[1] - int(item[1]), reverse=True)
            for industry, _ in remainders[:remaining]:
                industry_quotas[industry] += 1

    def is_buyable(row: Dict[str, Any]) -> bool:
        symbol = row.get("symbol")
        if not symbol or symbol in selected_symbols or symbol in excluded:
            return False
        price = prices.get(symbol)
        return bool(price and price * min_buy_shares <= target_amount)

    def add_row(row: Dict[str, Any], *, enforce_neutral_quota: bool) -> bool:
        symbol = row.get("symbol")
        industry = str(row.get(group_column) if group_column else row.get("industry") or "Unknown")
        if not is_buyable(row):
            return False
        if max_industry_count is not None and industry_counts.get(industry, 0) >= max_industry_count:
            return False
        if enforce_neutral_quota and industry_quotas and industry_counts.get(industry, 0) >= industry_quotas.get(industry, 0):
            return False
        rows.append(row)
        selected_symbols.add(symbol)
        industry_counts[industry] = industry_counts.get(industry, 0) + 1
        return True

    for row in pool.to_dict(orient="records"):
        add_row(row, enforce_neutral_quota=mode == "industry_neutral")
        if len(rows) >= top_n:
            break
    if mode == "industry_neutral" and len(rows) < top_n:
        for row in pool.to_dict(orient="records"):
            add_row(row, enforce_neutral_quota=False)
            if len(rows) >= top_n:
                break
    selected = pd.DataFrame(rows)
    if mode == "top_n_buffer" and not selected.empty and "score_value" in selected.columns:
        # Held names were promoted to the front to be considered first; the
        # book itself is still reported in rank order so downstream weight and
        # display code sees one ordering rule for every mode.
        selected = selected.sort_values("score_value", ascending=False).reset_index(drop=True)
    return selected


def candidate_stats(
    stock_scores,
    base_date: str,
    universe_index: str,
    config: Dict[str, Any],
    data_db=None,
    ml_db=None,
) -> Dict[str, Dict[str, Any]]:
    rebalance_days = int(config.get("rebalance_days", 20))
    if str(config.get("model_id") or "").strip():
        # Model-backed plans: same 20-trading-day window, but the date grid is
        # the market calendar, not stock_scores.
        score_dates = _recent_trading_dates(data_db, base_date, rebalance_days)
    else:
        score_dates = sorted(
            str(d) for d in stock_scores.distinct("score_date", {"score_date": {"$lte": base_date}, "index_codes": universe_index}) if normalize_date(d)
        )[-rebalance_days:]
    stats: Dict[str, Dict[str, Any]] = {}
    for score_date in score_dates:
        try:
            frame = load_score_frame(
                stock_scores,
                score_date,
                universe_index,
                config,
                data_db=data_db,
                ml_db=ml_db,
            )
        except ValueError:
            # A newly promoted model has no predictions before its first
            # publication date; candidate history starts there, not at the
            # stock_scores date range.
            continue
        selected = select_portfolio(frame, config).reset_index(drop=True)
        for rank, row in enumerate(selected.to_dict(orient="records"), start=1):
            symbol = row.get("symbol")
            if not symbol:
                continue
            item = stats.setdefault(
                symbol,
                {"candidate_appearances": 0, "candidate_ranks": [], "last_candidate_rank": None, "last_candidate_date": None},
            )
            item["candidate_appearances"] += 1
            item["candidate_ranks"].append(rank)
            item["last_candidate_rank"] = rank
            item["last_candidate_date"] = score_date
    for item in stats.values():
        ranks = item.get("candidate_ranks") or []
        item["avg_candidate_rank"] = sum(ranks) / len(ranks) if ranks else None
    return stats


def round_lot_shares(amount: float, price: Optional[float], lot_size: int) -> int:
    if not price or price <= 0 or amount <= 0:
        return 0
    raw = int(amount // price)
    return (raw // max(1, int(lot_size))) * max(1, int(lot_size))


def volatility_position_scale_map(
    data_db,
    symbols: Sequence[str],
    base_date: str,
    *,
    target_vol_annual: float = 0.30,
) -> Dict[str, float]:
    """Return per-symbol scale in (0, 1] using target_vol / realized_vol_20d."""
    if target_vol_annual <= 0:
        return {symbol: 1.0 for symbol in symbols}

    scales: Dict[str, float] = {}
    for symbol in symbols:
        try:
            rows = fetch_volume_price_rows(data_db, symbol, base_date, lookback_days=80)
            features = compute_volatility_features_from_rows(rows, vol_windows=(20,), percentile_lookback=60)
            stock_vol = features.get("vol_20d_annual")
            if stock_vol is None or float(stock_vol) <= 0:
                scales[symbol] = 1.0
            else:
                scales[symbol] = min(1.0, float(target_vol_annual) / float(stock_vol))
        except Exception:
            scales[symbol] = 1.0
    return scales


def entry_vol_blocked_symbols(
    data_db,
    symbols: Sequence[str],
    base_date: str,
    *,
    max_vol_annual: Optional[float] = None,
) -> set:
    """Names the live extreme-volatility guard would skip at entry.

    The live publish path blocks buys with 20d annualized vol >= 1.0, but plan
    construction used to keep them in the selection and let publish skip them,
    leaving a top10 book with only 8 names. Filter before selection so the
    discretionary cash is explicit and top10 still holds ten tradable names.
    """
    if max_vol_annual is None or float(max_vol_annual) <= 0:
        return set()
    blocked = set()
    for symbol in symbols:
        try:
            rows = fetch_volume_price_rows(data_db, symbol, base_date, lookback_days=80)
            features = compute_volatility_features_from_rows(
                rows, vol_windows=(20,), percentile_lookback=60
            )
            vol = features.get("vol_20d_annual")
            if vol is not None and float(vol) >= float(max_vol_annual):
                blocked.add(symbol)
        except Exception:
            continue
    return blocked
