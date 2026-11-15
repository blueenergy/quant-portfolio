#!/usr/bin/env python3
"""Enqueue daily auto plan-generation tasks for subscribed portfolio combos.

This is the scheduled trigger that keeps the rebalance cadence alive: for every
(strategy, params_hash, user) combo the user has committed to, it enqueues one
``generate_portfolio_plan`` task per trading day in ``auto`` mode. The existing
plan generation worker consumes the queue and decides monitor vs rebalance based
on ``rebalance_days``.
"""

from __future__ import annotations

import argparse
import json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Enqueue auto plan-generation tasks for subscribed combos.")
    parser.add_argument(
        "--base-date",
        default=None,
        help="Trading date YYYYMMDD to generate for; defaults to the latest trading date on or before today",
    )
    parser.add_argument("--mode", choices=["auto", "monitor", "rebalance"], default="auto")
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=120,
        help="Only treat combos with a committed plan within this many days as subscribed",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON payload")
    return parser.parse_args()


def main() -> int:
    from portfolio.calendar import latest_trading_date_on_or_before, normalize_date
    from portfolio.constants import business_today_yyyymmdd
    from portfolio.db import ensure_trading_indexes, get_data_db, get_trading_collections
    from portfolio.scheduler import enqueue_auto_generation_tasks

    args = _parse_args()
    collections = get_trading_collections()
    ensure_trading_indexes(collections)

    today = business_today_yyyymmdd()
    base_date = normalize_date(args.base_date) if args.base_date else None
    if not base_date:
        base_date = latest_trading_date_on_or_before(get_data_db(), today)
    if not base_date:
        print("No trading date available to enqueue plan generation; aborting.")
        return 1

    result = enqueue_auto_generation_tasks(
        collections,
        base_date,
        mode=args.mode,
        lookback_days=args.lookback_days,
    )
    print(
        f"Auto plan generation enqueue: base_date={result['base_date']} "
        f"subscriptions={result['subscriptions']} enqueued={len(result['enqueued'])} "
        f"skipped={len(result['skipped'])}"
    )
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
