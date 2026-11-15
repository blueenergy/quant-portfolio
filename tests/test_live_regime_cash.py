"""Live bull-invest / non-bull-cash construction overlay."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import pytest

from shared.regime_label import REGIME_CASH_REMINDER, regime_cash_requested
from portfolio.construction import apply_regime_cash_to_selection
from portfolio.regime_cash import resolve_live_regime_cash
from shared.strategies import plan_params_hash
from tests.test_portfolio_trading import _seed_csi1000_index, _setup_dbs
from tools.portfolio_trading import generate_trade_plan


def _seed_index_prices(data_db, symbol: str, *, end_date: str = "20250110", bull: bool = True) -> None:
    end = datetime.strptime(end_date, "%Y%m%d")
    start = end - timedelta(days=220)
    close = 1000.0
    day = start
    crash_from = end - timedelta(days=25)
    while day <= end:
        trade_date = day.strftime("%Y%m%d")
        if bull or day < crash_from:
            close += 1.5
        else:
            close = max(100.0, close * 0.92)
        data_db["index_prices"].docs.append({"ts_code": symbol, "trade_date": trade_date, "close": close})
        day += timedelta(days=1)


def _mirror_universe(app_db, data_db, source: str, target: str) -> None:
    for doc in list(data_db["index_constituents"].docs):
        if doc.get("index_code") == source:
            data_db["index_constituents"].docs.append({**doc, "index_code": target})
    for doc in app_db["stock_scores"].docs:
        codes = list(doc.get("index_codes") or [])
        if source in codes and target not in codes:
            codes.append(target)
            doc["index_codes"] = codes


def test_regime_cash_requested_is_explicit_opt_in():
    assert regime_cash_requested("csi1000") is False
    assert regime_cash_requested("csi1000", None) is False
    assert regime_cash_requested("csi500") is False
    assert regime_cash_requested("csi1000", False) is False
    assert regime_cash_requested("csi1000", True) is True
    assert regime_cash_requested("csi500", True) is True


def test_apply_regime_cash_to_selection_empties_on_cash():
    frame = pd.DataFrame([{"symbol": "000001", "score_value": 1.0}])
    assert list(apply_regime_cash_to_selection(frame, "invest")["symbol"]) == ["000001"]
    assert apply_regime_cash_to_selection(frame, "cash").empty


def test_plan_params_hash_ignores_regime_cash_flag():
    base = {"universe_index": "csi1000", "top_n": 20, "rebalance_days": 20}
    assert plan_params_hash(base) == plan_params_hash({**base, "regime_cash": True})
    assert plan_params_hash(base) == plan_params_hash({**base, "regime_cash": False})


def test_generate_trade_plan_skips_overlay_when_unset():
    app_db, data_db, cols = _setup_dbs()
    data_db["index_prices"].docs = []
    _seed_csi1000_index(data_db, bull=False)
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    assert result["plan"]["regime_cash"]["enabled"] is False
    assert result["plan"]["summary"]["selected_count"] == 10


def test_generate_trade_plan_csi1000_bull_keeps_book_and_records_overlay():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={"regime_cash": True},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    overlay = result["plan"]["regime_cash"]
    assert overlay["enabled"] is True
    assert overlay["action"] == "invest"
    assert overlay["universe_index"] == "csi1000"
    assert overlay["scope"] == "opt_in"
    assert "是否启用由你决定" in overlay["note"]
    assert result["plan"]["params_snapshot"]["regime_cash"] is True
    assert result["plan"]["summary"]["selected_count"] == 10
    assert len(result["items"]) == 10


def test_generate_trade_plan_csi1000_non_bull_goes_to_cash():
    app_db, data_db, cols = _setup_dbs()
    data_db["index_prices"].docs = []
    _seed_csi1000_index(data_db, bull=False)
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={"regime_cash": True},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    overlay = result["plan"]["regime_cash"]
    assert overlay["enabled"] is True
    assert overlay["action"] == "cash"
    assert overlay["label"] != "bull"
    assert result["plan"]["summary"]["selected_count"] == 0
    assert result["items"] == []
    assert REGIME_CASH_REMINDER in overlay["note"]


def test_generate_trade_plan_explicit_false_skips_overlay_in_bear():
    app_db, data_db, cols = _setup_dbs()
    data_db["index_prices"].docs = []
    _seed_csi1000_index(data_db, bull=False)
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={"regime_cash": False},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    assert result["plan"]["regime_cash"]["enabled"] is False
    assert result["plan"]["params_snapshot"]["regime_cash"] is False
    assert result["plan"]["summary"]["selected_count"] == 10


def test_generate_trade_plan_allows_regime_cash_on_csi500():
    app_db, data_db, cols = _setup_dbs()
    _mirror_universe(app_db, data_db, "csi1000", "csi500")
    data_db["index_prices"].docs = []
    _seed_index_prices(data_db, "000905.SH", bull=False)
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={
            "universe_index": "csi500",
            "regime_cash": True,
            "index_benchmark_symbol": "000905.SH",
        },
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    overlay = result["plan"]["regime_cash"]
    assert overlay["enabled"] is True
    assert overlay["action"] == "cash"
    assert overlay["universe_index"] == "csi500"
    assert overlay["benchmark_symbol"] == "000905.SH"
    assert result["plan"]["summary"]["selected_count"] == 0


def test_resolve_live_regime_cash_requires_index_prices_when_enabled():
    _app_db, data_db, _cols = _setup_dbs()
    data_db["index_prices"].docs = []
    with pytest.raises(ValueError, match="指数行情"):
        resolve_live_regime_cash(
            config={"universe_index": "csi1000", "regime_cash": True},
            data_db=data_db,
            score_date="20250106",
        )


def test_resolve_live_regime_cash_forwards_volume_price_rule(monkeypatch):
    captured = {}

    def fake_schedule(rows, dates, **kwargs):
        captured["kwargs"] = kwargs
        return [], [{"score_date": "20250110", "label": "bull", "action": "invest"}]

    monkeypatch.setattr("portfolio.regime_cash.build_regime_cash_schedule", fake_schedule)
    monkeypatch.setattr(
        "portfolio.regime_cash.load_index_rows_from_data_db",
        lambda *args, **kwargs: [{"trade_date": "20250110", "close": 1.0, "vol": 100.0}],
    )

    decision = resolve_live_regime_cash(
        config={
            "universe_index": "csi1000",
            "regime_cash": True,
            "index_benchmark_symbol": "000852.SH",
            "regime_rule": "k_slow_anchor_with_vol_and_atr",
        },
        data_db=object(),
        score_date="20250110",
    )

    assert captured["kwargs"]["regime_rule"] == "k_slow_anchor_with_vol_and_atr"
    assert decision["regime_rule"] == "k_slow_anchor_with_vol_and_atr"
    assert decision["action"] == "invest"


def test_regime_warmup_is_long_enough_for_hysteresis():
    """The label is path-dependent; 200 days left 20260812 on the wrong side."""
    from shared.regime_label import MA_WARMUP_CALENDAR_DAYS

    assert MA_WARMUP_CALENDAR_DAYS >= 400
