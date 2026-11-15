"""Market price helpers for plan sizing and paper execution."""

from __future__ import annotations

import logging
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

from pymongo import DESCENDING

log = logging.getLogger(__name__)


def latest_prices(data_db, symbols: Sequence[str], trade_date: str, field: str = "close") -> Dict[str, float]:
    rows = data_db["volume_price"].find(
        {"symbol": {"$in": list(symbols)}, "trade_date": trade_date}, {"_id": 0, "symbol": 1, field: 1, "close": 1, "open": 1}
    )
    prices: Dict[str, float] = {}
    for row in rows:
        value = row.get(field) or row.get("close") or row.get("open")
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            prices[row["symbol"]] = numeric
    return prices


def latest_close_prices_on_or_before(data_db, symbols: Sequence[str], trade_date: str) -> Tuple[Dict[str, float], Dict[str, str]]:
    volume_price = data_db["volume_price"]
    unique_symbols = sorted({str(symbol) for symbol in symbols if symbol})
    aggregate = getattr(volume_price, "aggregate", None)
    if aggregate is not None and unique_symbols:
        pipeline = [
            {"$match": {"symbol": {"$in": unique_symbols}, "trade_date": {"$lte": trade_date}}},
            {"$sort": {"symbol": 1, "trade_date": -1}},
            {"$group": {"_id": "$symbol", "doc": {"$first": "$$ROOT"}}},
            {"$project": {"_id": 1, "doc.symbol": 1, "doc.trade_date": 1, "doc.close": 1}},
        ]
        try:
            prices: Dict[str, float] = {}
            price_dates: Dict[str, str] = {}
            for row in aggregate(pipeline):
                doc = row.get("doc") or {}
                symbol = doc.get("symbol")
                if not symbol:
                    continue
                try:
                    price = float(doc.get("close"))
                except (TypeError, ValueError):
                    continue
                if price > 0:
                    prices[str(symbol)] = price
                    price_dates[str(symbol)] = str(doc.get("trade_date") or "")
            return prices, price_dates
        except Exception:
            # the per-symbol loop below returns the same prices, just slower, so
            # this is a speed regression rather than a correctness problem
            log.debug("price aggregation failed; falling back to per-symbol lookup", exc_info=True)

    prices: Dict[str, float] = {}
    price_dates: Dict[str, str] = {}
    for symbol in unique_symbols:
        row = volume_price.find_one(
            {"symbol": symbol, "trade_date": {"$lte": trade_date}},
            sort=[("trade_date", DESCENDING)],
            projection={"_id": 0, "symbol": 1, "trade_date": 1, "close": 1},
        )
        if not row:
            continue
        try:
            price = float(row.get("close"))
        except (TypeError, ValueError):
            continue
        if price > 0:
            prices[symbol] = price
            price_dates[symbol] = str(row.get("trade_date") or "")
    return prices, price_dates


def paper_execution_prices(
    data_db,
    symbols: Sequence[str],
    execute_date: str,
    items_by_symbol: Optional[Mapping[str, Mapping[str, Any]]] = None,
) -> Tuple[Dict[str, float], Dict[str, str], Dict[str, str]]:
    """Execution prices for paper trading: open, else latest close, else estimated."""
    unique_symbols = sorted({str(symbol) for symbol in symbols if symbol})
    open_prices = latest_prices(data_db, unique_symbols, execute_date, "open")
    close_prices, close_dates = latest_close_prices_on_or_before(data_db, unique_symbols, execute_date)
    item_map = items_by_symbol or {}

    prices: Dict[str, float] = {}
    price_sources: Dict[str, str] = {}
    price_dates: Dict[str, str] = {}
    for symbol in unique_symbols:
        if symbol in open_prices:
            prices[symbol] = open_prices[symbol]
            price_sources[symbol] = "open"
            price_dates[symbol] = execute_date
        elif symbol in close_prices:
            prices[symbol] = close_prices[symbol]
            price_sources[symbol] = "latest_close"
            price_dates[symbol] = close_dates.get(symbol, "")
        else:
            item = item_map.get(symbol) or {}
            try:
                estimated_price = float(item.get("estimated_price"))
            except (TypeError, ValueError):
                estimated_price = 0.0
            if estimated_price > 0:
                prices[symbol] = estimated_price
                price_sources[symbol] = "estimated_price"
                price_dates[symbol] = str(item.get("price_trade_date") or "")
    return prices, price_sources, price_dates
