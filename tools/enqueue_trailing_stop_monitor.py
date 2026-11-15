#!/usr/bin/env python3
"""Enqueue daily trailing-stop monitor tasks for subscribed portfolio combos."""

from __future__ import annotations

import argparse
import json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue trailing-stop monitor tasks for subscribed combos.")
    parser.add_argument("--base-date", default=None, help="Trading date YYYYMMDD; defaults to latest on or before today")
    parser.add_argument("--lookback-days", type=int, default=120)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def main() -> int:
    from portfolio.calendar import latest_trading_date_on_or_before, normalize_date
    from portfolio.constants import business_today_yyyymmdd
    from portfolio.db import ensure_trading_indexes, get_data_db, get_trading_collections
    from portfolio.trailing_stop import enqueue_trailing_stop_tasks

    args = _parse_args()
    collections = get_trading_collections()
    ensure_trading_indexes(collections)
    today = business_today_yyyymmdd()
    base_date = normalize_date(args.base_date) if args.base_date else latest_trading_date_on_or_before(get_data_db(), today)
    if not base_date:
        print("No trading date available to enqueue trailing-stop monitor; aborting.")
        return 1
    result = enqueue_trailing_stop_tasks(collections, base_date, lookback_days=args.lookback_days)
    print(
        f"Trailing-stop enqueue: base_date={result['base_date']} "
        f"subscriptions={result['subscriptions']} enqueued={len(result['enqueued'])} "
        f"skipped={len(result['skipped'])}"
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
