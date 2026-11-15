import os

import pytest

from portfolio.trailing_stop import (
    check_trailing_stop,
    enqueue_trailing_stop_tasks,
    get_trailing_stop_cycle_exclusions,
    is_trailing_stop_monitor_verbose,
    process_trailing_stop_task,
    resolve_trailing_stop_pct,
    resolve_trailing_stop_pct_for_lineage,
    run_trailing_stop_monitor,
    set_lineage_trailing_stop_pct,
)
from tests.test_portfolio_trading import _Collection, _Db, _setup_dbs


def _price_fetcher(data_db, symbol, start_date, end_date):
    rows = []
    for row in data_db["volume_price"].docs:
        if row.get("symbol") != symbol:
            continue
        trade_date = str(row.get("trade_date") or "")
        if trade_date < start_date or trade_date > end_date:
            continue
        rows.append(dict(row))
    rows.sort(key=lambda item: item.get("trade_date") or "")
    return rows


def test_resolve_trailing_stop_pct():
    assert resolve_trailing_stop_pct({"trailing_stop_pct": 0.15}) == 0.15
    assert resolve_trailing_stop_pct({"trailing_stop_pct": 0}) is None
    assert resolve_trailing_stop_pct({}) is None


def test_trailing_stop_pct_for_snapshot_uses_resolved_value():
    from portfolio.trailing_stop import trailing_stop_pct_for_snapshot

    assert trailing_stop_pct_for_snapshot(0.15) == {"trailing_stop_pct": 0.15}
    # Explicit disable must not fall back to a preset value elsewhere.
    assert trailing_stop_pct_for_snapshot(None) == {"trailing_stop_pct": None}


def test_check_trailing_stop_triggers_on_close_drawdown():
    bars = [
        {"trade_date": "20250101", "high": 100, "close": 100},
        {"trade_date": "20250102", "high": 110, "close": 108},
        {"trade_date": "20250103", "high": 112, "close": 93},
    ]
    result = check_trailing_stop(bars, entry_date="20250101", base_date="20250103", trailing_pct=0.15)
    assert result["triggered"] is True
    assert result["trigger_date"] == "20250103"
    assert result["peak_high"] == 112
    assert result["stop_floor"] == pytest.approx(112 * 0.85)


def test_check_trailing_stop_no_trigger_when_above_floor():
    bars = [
        {"trade_date": "20250101", "high": 100, "close": 100},
        {"trade_date": "20250102", "high": 110, "close": 108},
        {"trade_date": "20250103", "high": 112, "close": 100},
    ]
    result = check_trailing_stop(bars, entry_date="20250101", base_date="20250103", trailing_pct=0.15)
    assert result["triggered"] is False
    assert result["peak_high"] == 112


def test_run_trailing_stop_monitor_writes_verbose_run():
    app_db, data_db, cols = _setup_dbs()
    symbol = "TS001"
    for day in ["20250106", "20250107"]:
        data_db["volume_price"].docs.append(
            {"symbol": symbol, "trade_date": day, "high": 20, "close": 19, "open": 19}
        )
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "plan_type": "rebalance",
        }
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [{"symbol": symbol, "name": "Stock 1", "shares": 100}],
        }
    )
    cols.paper_executions.docs.append(
        {
            "plan_id": "p1",
            "symbol": symbol,
            "action": "buy",
            "execute_date": "20250106",
        }
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {"trailing_stop_pct": 0.15, "rebalance_days": 10, "top_n": 5},
        }
    )

    result = run_trailing_stop_monitor(
        cols,
        data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        base_date="20250107",
        preset_id="preset-trailing",
        price_fetcher=_price_fetcher,
    )

    assert result["skipped"] is False
    run = cols.trailing_stop_runs.find_one({"run_id": result["run_id"]})
    assert run["summary"]["symbols_checked"] == 1
    assert run["summary"]["triggered_count"] == 0
    assert "无标的跌破" in run["summary"]["message"]
    assert len(run["items"]) == 1
    assert run["items"][0]["triggered"] is False
    assert run["items"][0]["entry_date"] == "20250106"


def test_run_trailing_stop_monitor_generates_sell_plan_on_trigger():
    app_db, data_db, cols = _setup_dbs()
    symbol = "TS002"
    data_db["volume_price"].docs.extend(
        [
            {"symbol": symbol, "trade_date": "20250106", "high": 20, "close": 20, "open": 20},
            {"symbol": symbol, "trade_date": "20250107", "high": 22, "close": 17, "open": 17},
            {"symbol": symbol, "trade_date": "20250108", "high": 17, "close": 17, "open": 17},
        ]
    )
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "plan_type": "rebalance",
        }
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [{"symbol": symbol, "name": "Stock 1", "shares": 100}],
        }
    )
    cols.paper_executions.docs.append(
        {
            "plan_id": "p1",
            "symbol": symbol,
            "action": "buy",
            "execute_date": "20250106",
        }
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {
                "trailing_stop_pct": 0.15,
                "rebalance_days": 10,
                "top_n": 5,
                "paper_execution_mode": "auto_shadow",
            },
        }
    )

    result = run_trailing_stop_monitor(
        cols,
        data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        base_date="20250107",
        preset_id="preset-trailing",
        price_fetcher=_price_fetcher,
    )

    assert result["triggered_symbols"] == [symbol]
    assert result["sell_plan_id"]
    sell_plan = cols.trade_plans.find_one({"plan_id": result["sell_plan_id"]})
    assert sell_plan["plan_type"] == "trailing_stop_sell"
    assert sell_plan["status"] == "approved"
    items = list(cols.trade_plan_items.find({"plan_id": result["sell_plan_id"]}))
    assert len(items) == 1
    assert items[0]["delta_shares"] == -100


def test_enqueue_trailing_stop_tasks_skips_without_trailing_pct():
    _, _, cols = _setup_dbs()
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
        }
    )
    result = enqueue_trailing_stop_tasks(cols, "20250107")
    assert result["enqueued"] == []
    assert any(item.get("reason") == "trailing_stop_disabled" for item in result["skipped"])


def test_get_trailing_stop_cycle_exclusions():
    _, _, cols = _setup_dbs()
    cols.trailing_stop_runs.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250107",
            "status": "completed",
            "triggered_symbols": ["000001"],
        }
    )
    excluded = get_trailing_stop_cycle_exclusions(
        cols,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        since_date="20250106",
    )
    assert excluded == ["000001"]


def test_is_trailing_stop_monitor_verbose_env(monkeypatch):
    monkeypatch.delenv("TRAILING_STOP_MONITOR_VERBOSE", raising=False)
    assert is_trailing_stop_monitor_verbose() is True
    monkeypatch.setenv("TRAILING_STOP_MONITOR_VERBOSE", "false")
    assert is_trailing_stop_monitor_verbose() is False


def test_run_trailing_stop_monitor_triggers_only_mode():
    app_db, data_db, cols = _setup_dbs()
    symbol = "TS003"
    data_db["volume_price"].docs.extend(
        [
            {"symbol": symbol, "trade_date": "20250106", "high": 20, "close": 20, "open": 20},
            {"symbol": symbol, "trade_date": "20250107", "high": 22, "close": 19, "open": 19},
        ]
    )
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "plan_type": "rebalance",
        }
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [{"symbol": symbol, "name": "Stock 1", "shares": 100}],
        }
    )
    cols.paper_executions.docs.append(
        {"plan_id": "p1", "symbol": symbol, "action": "buy", "execute_date": "20250106"}
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {"trailing_stop_pct": 0.15},
        }
    )
    result = run_trailing_stop_monitor(
        cols,
        data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        base_date="20250107",
        preset_id="preset-trailing",
        verbose=False,
        generate_sell_plan=False,
        price_fetcher=_price_fetcher,
    )
    run = cols.trailing_stop_runs.find_one({"run_id": result["run_id"]})
    assert run["verbosity"] == "triggers_only"
    assert run["items"] == []


def test_process_trailing_stop_task_idempotent(monkeypatch):
    app_db, data_db, cols = _setup_dbs()
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "plan_type": "rebalance",
        }
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [],
        }
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {"trailing_stop_pct": 0.15},
        }
    )
    task = {
        "task_id": "t1",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "params_hash": "hash-a",
        "user_id": "user-1",
        "base_date": "20250107",
        "preset_id": "preset-trailing",
        "params_override": {},
    }
    first = process_trailing_stop_task(cols, data_db, task, price_fetcher=_price_fetcher)
    assert first["skipped"] is True
    assert first["reason"] == "no_positions"


def test_run_trailing_stop_skips_sell_when_same_day_rebalance_exists():
    app_db, data_db, cols = _setup_dbs()
    symbol = "TS004"
    data_db["volume_price"].docs.extend(
        [
            {"symbol": symbol, "trade_date": "20250106", "high": 20, "close": 20, "open": 20},
            {"symbol": symbol, "trade_date": "20250107", "high": 22, "close": 17, "open": 17},
            {"symbol": symbol, "trade_date": "20250108", "high": 17, "close": 17, "open": 17},
        ]
    )
    cols.trade_plans.docs.extend(
        [
            {
                "plan_id": "p-reb-prev",
                "strategy_template_id": "growth_cycle_index_enhanced",
                "params_hash": "hash-a",
                "user_id": "user-1",
                "base_date": "20250106",
                "status": "executed_paper",
                "plan_type": "rebalance",
            },
            {
                "plan_id": "p-reb-same-day",
                "strategy_template_id": "growth_cycle_index_enhanced",
                "params_hash": "hash-a",
                "user_id": "user-1",
                "base_date": "20250107",
                "status": "approved",
                "plan_type": "rebalance",
                "origin": "strategy",
            },
        ]
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [{"symbol": symbol, "name": "Stock 1", "shares": 100}],
        }
    )
    cols.paper_executions.docs.append(
        {"plan_id": "p-reb-prev", "symbol": symbol, "action": "buy", "execute_date": "20250106"}
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {"trailing_stop_pct": 0.15, "paper_execution_mode": "auto_shadow"},
        }
    )
    result = run_trailing_stop_monitor(
        cols,
        data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        base_date="20250107",
        preset_id="preset-trailing",
        price_fetcher=_price_fetcher,
    )
    assert result["triggered_symbols"] == [symbol]
    assert result["sell_plan_id"] is None
    assert result["sell_plan_skipped_reason"] == "same_day_rebalance"
    assert not any(plan.get("plan_type") == "trailing_stop_sell" for plan in cols.trade_plans.docs)


def test_reclaim_stale_trailing_stop_tasks():
    from datetime import datetime, timedelta

    from portfolio.trailing_stop import reclaim_stale_trailing_stop_tasks

    _, _, cols = _setup_dbs()
    now = datetime.utcnow()
    cols.trailing_stop_tasks.docs.append(
        {
            "task_id": "stale-1",
            "status": "running",
            "started_at": now - timedelta(hours=2),
            "updated_at": now - timedelta(hours=2),
        }
    )
    cols.trailing_stop_tasks.docs.append(
        {
            "task_id": "fresh-1",
            "status": "running",
            "started_at": now - timedelta(minutes=5),
            "updated_at": now - timedelta(minutes=5),
        }
    )
    reclaimed = reclaim_stale_trailing_stop_tasks(cols, now=now, stale_seconds=30 * 60)
    assert reclaimed == 1
    stale = cols.trailing_stop_tasks.find_one({"task_id": "stale-1"})
    fresh = cols.trailing_stop_tasks.find_one({"task_id": "fresh-1"})
    assert stale["status"] == "pending"
    assert fresh["status"] == "running"


def test_resolve_trailing_stop_pct_for_lineage_controls_priority():
    _app_db, _data_db, cols = _setup_dbs()
    set_lineage_trailing_stop_pct(cols, "growth_cycle_index_enhanced", "hash-a", "user-1", 0.2)
    resolved = resolve_trailing_stop_pct_for_lineage(
        cols,
        "growth_cycle_index_enhanced",
        "hash-a",
        "user-1",
        config={"trailing_stop_pct": 0.1},
    )
    assert resolved == 0.2


def test_resolve_trailing_stop_pct_for_lineage_fallback_preset():
    _app_db, _data_db, cols = _setup_dbs()
    resolved = resolve_trailing_stop_pct_for_lineage(
        cols,
        "growth_cycle_index_enhanced",
        "hash-a",
        "user-1",
        config={"trailing_stop_pct": 0.15},
    )
    assert resolved == 0.15


def test_resolve_trailing_stop_pct_for_lineage_explicit_null_disables():
    _app_db, _data_db, cols = _setup_dbs()
    set_lineage_trailing_stop_pct(cols, "growth_cycle_index_enhanced", "hash-a", "user-1", None)
    resolved = resolve_trailing_stop_pct_for_lineage(
        cols,
        "growth_cycle_index_enhanced",
        "hash-a",
        "user-1",
        config={"trailing_stop_pct": 0.15},
    )
    assert resolved is None


def test_enqueue_trailing_stop_uses_lineage_controls_over_preset():
    _app_db, _data_db, cols = _setup_dbs()
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "origin": "strategy",
        }
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "enabled",
            "is_default": True,
            "params": {"trailing_stop_pct": 0.1},
        }
    )
    set_lineage_trailing_stop_pct(cols, "growth_cycle_index_enhanced", "hash-a", "user-1", 0.15)
    result = enqueue_trailing_stop_tasks(cols, "20250107")
    assert result["enqueued"]
    assert result["enqueued"][0]["params_hash"] == "hash-a"


def test_run_trailing_stop_monitor_adds_raw_price_levels():
    """Trigger fields stay hfq; the displayed levels are converted to raw."""
    app_db, data_db, cols = _setup_dbs()
    symbol = "TS003"
    for day in ["20250106", "20250107"]:
        data_db["volume_price"].docs.append(
            {"symbol": symbol, "trade_date": day, "high": 20, "close": 19, "open": 19}
        )
    data_db["stock_adj_factor"].docs.append(
        {"symbol": symbol, "trade_date": "20250107", "adj_factor": 2.0}
    )
    cols.trade_plans.docs.append(
        {
            "plan_id": "p1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
            "plan_type": "rebalance",
        }
    )
    cols.paper_positions.docs.append(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "as_of_date": "20250107",
            "positions": [{"symbol": symbol, "name": "Stock 1", "shares": 100}],
        }
    )
    cols.paper_executions.docs.append(
        {"plan_id": "p1", "symbol": symbol, "action": "buy", "execute_date": "20250106"}
    )
    cols.param_presets.docs.append(
        {
            "preset_id": "preset-trailing",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "status": "active",
            "params": {"trailing_stop_pct": 0.15, "rebalance_days": 10, "top_n": 5},
        }
    )

    def _hfq_fetcher(data_db, symbol, start_date, end_date):
        return [
            {**row, "high": row["high"] * 2, "close": row["close"] * 2, "open": row["open"] * 2}
            for row in _price_fetcher(data_db, symbol, start_date, end_date)
        ]

    result = run_trailing_stop_monitor(
        cols,
        data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-a",
        user_id="user-1",
        base_date="20250107",
        preset_id="preset-trailing",
        price_fetcher=_hfq_fetcher,
    )

    run = cols.trailing_stop_runs.find_one({"run_id": result["run_id"]})
    item = run["items"][0]
    assert item["peak_high"] == pytest.approx(40.0)
    assert item["raw_peak_high"] == pytest.approx(20.0)
    assert item["raw_close"] == pytest.approx(19.0)
    assert item["raw_stop_floor"] == pytest.approx(17.0)
    assert item["adj_factor"] == pytest.approx(2.0)
    assert item["price_basis"] == "hfq"
