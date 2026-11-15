"""Trading-calendar helpers for plan scheduling."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from typing import Any, List, Optional

from pymongo import ASCENDING, DESCENDING

log = logging.getLogger(__name__)


def normalize_date(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    text = str(value).replace("-", "").replace("/", "")[:8]
    return text if len(text) == 8 and text.isdigit() else None


def next_trading_date(data_db, base_date: str) -> Optional[str]:
    base = normalize_date(base_date)
    if not base:
        return None
    for day in forward_trading_dates(data_db, base, 2):
        normalized = normalize_date(day)
        if normalized and normalized > base:
            return normalized
    row = data_db["volume_price"].find_one(
        {"trade_date": {"$gt": base}},
        sort=[("trade_date", ASCENDING)],
        projection={"_id": 0, "trade_date": 1},
    )
    return normalize_date((row or {}).get("trade_date"))


def trading_dates_between(data_db, start_date: str, end_date: str) -> List[str]:
    return sorted(
        str(d)
        for d in data_db["volume_price"].distinct(
            "trade_date",
            {"trade_date": {"$gte": start_date, "$lte": end_date}},
        )
        if normalize_date(d)
    )


def _tushare_pro():
    token = os.getenv("TUSHARE_TOKEN")
    if not token:
        return None
    try:
        import tushare as ts  # type: ignore

        return ts.pro_api(token)
    except Exception:
        return None


def forward_trading_dates(data_db, start_date: str, min_count: int) -> List[str]:
    """Ascending trading dates on/after ``start_date``.

    Prefers tushare's exchange calendar (which knows *future* trading days and
    holidays) so we can project a rebalance date that has no market data yet,
    falling back to the locally loaded ``volume_price`` history. ``min_count``
    only sizes the lookahead window.
    """
    start = normalize_date(start_date)
    if not start:
        return []
    span_days = max(30, int(min_count) * 2 + 20)
    end = (datetime.strptime(start, "%Y%m%d") + timedelta(days=span_days)).strftime("%Y%m%d")
    pro = _tushare_pro()
    if pro is not None:
        try:
            df = pro.trade_cal(exchange="", start_date=start, end_date=end, is_open=1)
            if df is not None and not df.empty:
                days = sorted(str(d) for d in df["cal_date"].tolist() if normalize_date(str(d)))
                if days:
                    return days
        except Exception:
            # local history stops at today, so a rebalance date in the future
            # becomes unreachable rather than merely less accurate
            log.warning(
                "tushare trade_cal failed for %s..%s; falling back to loaded history",
                start,
                end,
                exc_info=True,
            )
    # Fallback: local market data only (no future dates beyond loaded history).
    return trading_dates_between(data_db, start, end)


def compute_next_rebalance_date(data_db, anchor_date: Optional[str], rebalance_days: int) -> Optional[str]:
    """Trading date exactly ``rebalance_days`` trading days after ``anchor_date``.

    ``anchor_date`` is the most recent rebalance date. Returns ``None`` when the
    available calendar does not reach far enough.
    """
    anchor = normalize_date(anchor_date)
    if not anchor or int(rebalance_days) <= 0:
        return None
    days = forward_trading_dates(data_db, anchor, int(rebalance_days) + 1)
    if anchor not in days:
        days = sorted(set(days) | {anchor})
    try:
        idx = days.index(anchor)
    except ValueError:
        return None
    target = idx + int(rebalance_days)
    return days[target] if target < len(days) else None


def _shift_date_days(yyyymmdd: str, days: int) -> Optional[str]:
    try:
        parsed = datetime.strptime(str(yyyymmdd)[:8], "%Y%m%d")
    except (TypeError, ValueError):
        return None
    return (parsed + timedelta(days=days)).strftime("%Y%m%d")


def latest_trading_date_on_or_before(data_db, base_date: str) -> Optional[str]:
    row = data_db["volume_price"].find_one(
        {"trade_date": {"$lte": base_date}},
        sort=[("trade_date", DESCENDING)],
        projection={"_id": 0, "trade_date": 1},
    )
    return normalize_date((row or {}).get("trade_date"))
