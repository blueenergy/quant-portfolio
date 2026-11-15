#!/usr/bin/env python3
"""Run trailing-stop monitor polling without noisy cron iteration logs."""

from __future__ import annotations

import argparse
import logging
import signal
import time

log = logging.getLogger("trailing_stop_monitor_loop")

_STOP = False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously process trailing-stop monitor tasks.")
    parser.add_argument("--worker-id", default="quant-scorer-trailing-stop")
    parser.add_argument("--interval-seconds", type=float, default=2)
    parser.add_argument("--limit-per-iteration", type=int, default=20)
    return parser.parse_args()


def _handle_stop(signum, frame) -> None:  # noqa: ARG001
    global _STOP
    _STOP = True


def main() -> int:
    from observability.logging_setup import configure_worker_logging
    from portfolio.db import ensure_trading_indexes, get_data_db, get_trading_collections
    from portfolio.trailing_stop import process_trailing_stop_task
    from tools.workers.process_trailing_stop_monitor import _claim_task

    configure_worker_logging()
    args = _parse_args()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    try:
        collections = get_trading_collections()
        ensure_trading_indexes(collections)
        data_db = get_data_db()
    except Exception:
        log.exception("startup failed")
        return 1
    log.info(
        "loop started: interval_seconds=%s limit_per_iteration=%s",
        args.interval_seconds,
        args.limit_per_iteration,
    )

    while not _STOP:
        processed_count = 0
        while processed_count < args.limit_per_iteration and not _STOP:
            task = _claim_task(collections, args.worker_id)
            if not task:
                break
            task_id = task.get("task_id")
            try:
                result = process_trailing_stop_task(collections, data_db, task)
                from datetime import datetime

                now = datetime.utcnow()
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
                log.info(
                    "task processed: task_id=%s run_id=%s skipped=%s",
                    task_id,
                    result.get("run_id"),
                    result.get("skipped"),
                )
            except Exception as exc:
                from datetime import datetime

                now = datetime.utcnow()
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
                # the task row keeps str(exc); the traceback only ever existed here
                log.exception("task failed: task_id=%s error=%s", task_id, exc)
            processed_count += 1
        if _STOP:
            break
        time.sleep(max(1, float(args.interval_seconds)))

    log.info("loop stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
