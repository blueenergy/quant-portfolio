#!/usr/bin/env python3
"""Process pending trailing-stop monitor tasks."""

from __future__ import annotations

import argparse
import json
from datetime import datetime

from pymongo import ASCENDING, ReturnDocument


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process pending trailing-stop monitor tasks.")
    parser.add_argument("--worker-id", default="quant-scorer-trailing-stop")
    parser.add_argument("--limit", type=int, default=20)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args()


def _claim_task(collections, worker_id: str):
    from portfolio.trailing_stop import reclaim_stale_trailing_stop_tasks

    now = datetime.utcnow()
    reclaim_stale_trailing_stop_tasks(collections, now=now)
    return collections.trailing_stop_tasks.find_one_and_update(
        {"status": "pending"},
        {
            "$set": {
                "status": "running",
                "worker_id": worker_id,
                "started_at": now,
                "updated_at": now,
                "error_message": None,
            },
            "$inc": {"attempts": 1},
        },
        sort=[("created_at", ASCENDING)],
        return_document=ReturnDocument.AFTER,
    )


def main() -> int:
    from portfolio.db import ensure_trading_indexes, get_data_db, get_trading_collections
    from portfolio.trailing_stop import process_trailing_stop_task

    args = _parse_args()
    collections = get_trading_collections()
    ensure_trading_indexes(collections)
    data_db = get_data_db()
    processed = []
    errors = []
    for _ in range(max(1, int(args.limit))):
        task = _claim_task(collections, args.worker_id)
        if not task:
            break
        task_id = task.get("task_id")
        now = datetime.utcnow()
        try:
            result = process_trailing_stop_task(collections, data_db, task)
            collections.trailing_stop_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "completed",
                        "run_id": result.get("run_id"),
                        "completed_at": now,
                        "updated_at": now,
                        "result": result,
                    }
                },
            )
            processed.append({"task_id": task_id, **result})
        except Exception as exc:
            collections.trailing_stop_tasks.update_one(
                {"task_id": task_id},
                {
                    "$set": {
                        "status": "failed",
                        "error_message": str(exc),
                        "completed_at": now,
                        "updated_at": now,
                    }
                },
            )
            errors.append({"task_id": task_id, "error": str(exc)})

    print(f"Trailing-stop processed={len(processed)} errors={len(errors)}")
    if args.json:
        print(json.dumps({"processed": processed, "errors": errors}, ensure_ascii=False, indent=2, default=str))
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
