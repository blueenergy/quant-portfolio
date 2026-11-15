"""Stock volatility helpers for plan risk review and reporting.

Uses unadjusted ``volume_price`` close/OHLC, aligned with ``cycle_scorer``.
Core math lives in ``stock_data_access.indicators.volatility``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

from stock_data_access.indicators.volatility import (
    abnormal_range_flag,
    atr_pct,
    compute_volatility_features,
    realized_vol,
    vol_percentile,
)

__all__ = [
    "abnormal_range_flag",
    "atr_pct",
    "compute_volatility_features",
    "compute_volatility_features_from_rows",
    "realized_vol",
    "rows_to_ohlc_frame",
    "vol_percentile",
]


def rows_to_ohlc_frame(rows: Sequence[Dict[str, Any]]):
    """Sort ascending OHLCV rows into a pandas DataFrame."""
    import pandas as pd

    if not rows:
        return pd.DataFrame()
    frame = pd.DataFrame(list(rows))
    if "trade_date" in frame.columns:
        frame = frame.sort_values("trade_date")
    return frame


def compute_volatility_features_from_rows(
    rows: Sequence[Dict[str, Any]],
    *,
    vol_windows: Sequence[int] = (20, 60),
    atr_window: int = 20,
    percentile_lookback: int = 250,
) -> Dict[str, Any]:
    """Convenience wrapper around shared ``compute_volatility_features``."""
    return compute_volatility_features(
        rows,
        vol_windows=vol_windows,
        atr_window=atr_window,
        percentile_lookback=percentile_lookback,
    )


def fetch_volume_price_rows(data_db, symbol: str, base_date: str, *, lookback_days: int = 320) -> List[Dict[str, Any]]:
    """Load the most recent ``lookback_days`` OHLCV rows up to ``base_date`` (ascending)."""
    try:
        rows = list(
            data_db["volume_price"]
            .find(
                {"symbol": symbol, "trade_date": {"$lte": base_date}},
                {"_id": 0, "trade_date": 1, "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1},
            )
            .sort("trade_date", -1)
            .limit(lookback_days)
        )
    except Exception:
        return []
    rows.reverse()
    return rows
