#!/usr/bin/env python3
"""Apply approved paper-trading plans whose execution data is ready."""

from __future__ import annotations

import argparse
import json
import sys


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply approved paper-trading plans when open prices are available.")
    parser.add_argument("--execute-date", help="Execution date, defaults to Beijing business date")
    parser.add_argument("--strategy-template-id", help="Optional strategy template id filter")
    parser.add_argument("--limit", type=int, help="Maximum number of plans to execute")
    parser.add_argument("--force", action="store_true", help="Reapply plans even if executions already exist")
    parser.add_argument("--json", action="store_true", help="Print full JSON payload")
    return parser.parse_args()


def main() -> int:
    from portfolio.paper import apply_approved_paper_plans

    args = _parse_args()
    result = apply_approved_paper_plans(
        execute_date=args.execute_date,
        strategy_template_id=args.strategy_template_id,
        force=args.force,
        limit=args.limit,
    )
    print(
        f"Approved paper plans checked: execute_date={result['execute_date']}, "
        f"applied={len(result['applied'])}, skipped={len(result['skipped'])}, errors={len(result['errors'])}"
    )
    for item in result["applied"]:
        print(f"  applied plan_id={item['plan_id']} status={item['status']} executions={len(item['executions'])}")
    for item in result["skipped"]:
        print(f"  skipped plan_id={item.get('plan_id')} reason={item.get('reason')}")
    for item in result["errors"]:
        print(f"  error plan_id={item.get('plan_id')} reason={item.get('reason')}", file=sys.stderr)
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
