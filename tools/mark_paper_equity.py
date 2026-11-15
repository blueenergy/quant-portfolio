#!/usr/bin/env python3
"""Mark paper-trading portfolios to market and persist daily equity."""

from __future__ import annotations

import argparse
import json


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist daily mark-to-market paper equity.")
    parser.add_argument("--as-of-date", help="Valuation date, defaults to Beijing business date")
    parser.add_argument("--strategy-template-id", help="Optional strategy template id filter")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload")
    return parser.parse_args()


def main() -> int:
    from portfolio.paper import mark_paper_equity

    args = _parse_args()
    result = mark_paper_equity(as_of_date=args.as_of_date, strategy_template_id=args.strategy_template_id)
    print(
        f"Paper equity marked: as_of_date={result['as_of_date']}, "
        f"marked={len(result['marked'])}, skipped={len(result['skipped'])}"
    )
    for item in result["marked"]:
        print(
            f"  marked plan_id={item.get('plan_id')} strategy_template_id={item.get('strategy_template_id')} equity={item['equity']:.2f} "
            f"cash={item['cash']:.2f} market_value={item['market_value']:.2f}"
        )
    for item in result["skipped"]:
        print(f"  skipped plan_id={item.get('plan_id')} strategy_template_id={item.get('strategy_template_id')} reason={item.get('reason')}")
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
