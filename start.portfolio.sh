#!/bin/bash
set -e

echo "[quant-portfolio] Starting portfolio planning scheduler..."
echo "[quant-portfolio] Portfolio plan generation loop interval: ${PORTFOLIO_PLAN_LOOP_INTERVAL_SECONDS:-2}s"
echo "[quant-portfolio] Trailing-stop monitor loop interval: ${TRAILING_STOP_LOOP_INTERVAL_SECONDS:-2}s"

if [ "${PORTFOLIO_REQUIRE_TUSHARE_TOKEN:-true}" = "true" ] && [ -z "${TUSHARE_TOKEN:-}" ]; then
  echo "[quant-portfolio] ERROR: TUSHARE_TOKEN is required by quant-portfolio for future trading-calendar lookups." >&2
  echo "[quant-portfolio] Set TUSHARE_TOKEN in env/quant-scorer.env or explicitly set PORTFOLIO_REQUIRE_TUSHARE_TOKEN=false." >&2
  exit 1
fi

# The token alone is not enough: tushare also needs its runtime imports to work
# (six is an undeclared dependency of tushare 1.4.x). Fail loudly at startup
# instead of silently falling back to local history and failing plan generation.
if ! python -c "import tushare" >/dev/null 2>&1; then
  echo "[quant-portfolio] ERROR: tushare is not importable in this image; trading-calendar lookups would silently fall back to local history." >&2
  echo "[quant-portfolio] Check requirements-portfolio.txt / image dependencies." >&2
  exit 1
fi

python /app/tools/run_portfolio_plan_generation_loop.py \
  --interval-seconds "${PORTFOLIO_PLAN_LOOP_INTERVAL_SECONDS:-2}" \
  --limit-per-iteration "${PORTFOLIO_PLAN_LOOP_LIMIT:-20}" &
PLAN_LOOP_PID=$!

python /app/tools/run_trailing_stop_monitor_loop.py \
  --interval-seconds "${TRAILING_STOP_LOOP_INTERVAL_SECONDS:-2}" \
  --limit-per-iteration "${TRAILING_STOP_LOOP_LIMIT:-20}" &
TRAILING_STOP_LOOP_PID=$!

supercronic /app/crontab.portfolio.txt &
SUPERCRONIC_PID=$!

shutdown() {
  echo "[quant-portfolio] Shutting down portfolio planning scheduler..."
  kill -TERM "$PLAN_LOOP_PID" "$TRAILING_STOP_LOOP_PID" "$SUPERCRONIC_PID" 2>/dev/null || true
  wait "$PLAN_LOOP_PID" "$TRAILING_STOP_LOOP_PID" "$SUPERCRONIC_PID" 2>/dev/null || true
}

trap shutdown TERM INT

wait -n "$PLAN_LOOP_PID" "$TRAILING_STOP_LOOP_PID" "$SUPERCRONIC_PID"
EXIT_CODE=$?
shutdown
exit "$EXIT_CODE"
