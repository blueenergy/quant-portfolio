#!/usr/bin/env python3
"""Run portfolio plan generation polling without noisy cron iteration logs."""

from __future__ import annotations

import argparse
import logging
import signal
import time

log = logging.getLogger("portfolio_plan_generation_loop")

_STOP = False


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously process portfolio plan generation tasks.")
    parser.add_argument("--worker-id", default="quant-scorer-plan-generator", help="Worker id written to claimed tasks")
    parser.add_argument("--interval-seconds", type=float, default=2, help="Seconds to sleep between polling iterations")
    parser.add_argument("--limit-per-iteration", type=int, default=20, help="Maximum tasks to process per polling iteration")
    return parser.parse_args()


def _handle_stop(signum, frame) -> None:  # noqa: ARG001
    global _STOP
    _STOP = True


def _log_processed(result) -> None:
    status = result.get("status")
    task_id = result.get("task_id")
    plan_id = result.get("plan_id")
    reason = result.get("reason") or result.get("error_message")
    detail = f" plan_id={plan_id}" if plan_id else ""
    if reason:
        detail += f" reason={reason}"
    message = f"task processed: status={status} task_id={task_id}{detail}"
    log.warning(message) if result.get("error_message") else log.info(message)


def main() -> int:
    from observability.logging_setup import configure_worker_logging
    from portfolio.db import ensure_trading_indexes, get_trading_collections
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    configure_worker_logging()
    args = _parse_args()
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    try:
        collections = get_trading_collections()
        ensure_trading_indexes(collections)
    except Exception:
        log.exception("startup failed")
        return 1
    log.info(
        "loop started: interval_seconds=%s limit_per_iteration=%s",
        args.interval_seconds,
        args.limit_per_iteration,
    )

    try:
        while not _STOP:
            processed_count = 0
            while processed_count < args.limit_per_iteration and not _STOP:
                result = process_one(collections, args.worker_id)
                if not result:
                    break
                processed_count += 1
                _log_processed(result)

            if _STOP:
                break
            time.sleep(max(1, float(args.interval_seconds)))
    except KeyboardInterrupt:
        log.info("interrupted; shutting down")
        return 0
    except Exception:
        log.exception("loop aborted")
        return 1

    log.info("loop stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
