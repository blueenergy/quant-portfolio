"""Live score-frame support for model-backed score_column strategies."""

from __future__ import annotations

import mongomock
import pytest

from portfolio.construction import (
    candidate_stats,
    entry_vol_blocked_symbols,
    load_index_universe_symbols_asof,
    load_model_score_frame,
    load_score_frame,
    resolve_plan_score_date,
)


def _stock_scores():
    stock_scores = mongomock.MongoClient()["app"]["stock_scores"]
    stock_scores.insert_many(
        [
            {
                "symbol": "000001.SZ",
                "score_date": "20261106",
                "index_codes": ["csi1000"],
                "growth_score": 10.0,
                "cycle_score": 10.0,
            },
            {
                "symbol": "000002.SZ",
                "score_date": "20261106",
                "index_codes": ["csi1000"],
                "growth_score": 20.0,
                "cycle_score": 20.0,
            },
        ]
    )
    return stock_scores


def _ml_db():
    ml_db = mongomock.MongoClient()["quant_ml"]
    ml_db["model_predictions"].insert_many(
        [
            {"model_id": "m1", "trade_date": "20261105", "ts_code": "000001.SZ", "score": 0.9},
            {"model_id": "m1", "trade_date": "20261105", "ts_code": "000002.SZ", "score": 0.1},
        ]
    )
    return ml_db


class _ExplodingCollection:
    def find(self, *args, **kwargs):
        raise AssertionError("stock_scores must not be read for a model plan")

    def find_one(self, *args, **kwargs):
        raise AssertionError("stock_scores must not be read for a model plan")

    def distinct(self, *args, **kwargs):
        raise AssertionError("stock_scores must not be read for a model plan")


def test_load_score_frame_uses_model_cross_section_without_stock_scores():
    config = {
        "score_type": "score_column",
        "score_column": "model_score",
        "model_id": "m1",
    }
    frame = load_score_frame(
        _ExplodingCollection(),
        "20261106",
        "csi1000",
        config,
        ml_db=_ml_db(),
    )
    assert list(frame.sort_values("score_value", ascending=False)["symbol"]) == [
        "000001.SZ",
        "000002.SZ",
    ]
    assert set(frame["score_date"]) == {"20261105"}


def test_load_score_frame_requires_model_source_for_missing_column():
    config = {"score_type": "score_column", "score_column": "model_score"}
    with pytest.raises(ValueError, match="no model_id"):
        load_score_frame(_stock_scores(), "20261106", "csi1000", config)


def test_load_model_score_frame_rejects_stale_prediction():
    ml_db = mongomock.MongoClient()["quant_ml"]
    ml_db["model_predictions"].insert_one(
        {"model_id": "m1", "trade_date": "20250101", "ts_code": "000001.SZ", "score": 0.9}
    )
    with pytest.raises(ValueError, match="stale"):
        load_model_score_frame(ml_db, "m1", "20260301")


def test_load_model_score_frame_returns_latest_at_or_before():
    frame, pred_date = load_model_score_frame(_ml_db(), "m1", "20261106")
    assert pred_date == "20261105"
    assert len(frame) == 2


def test_load_model_cross_section_joins_metadata_and_index_membership():
    ml_db = mongomock.MongoClient()["quant_ml"]
    ml_db["model_predictions"].insert_many(
        [
            {"model_id": "m1", "trade_date": "20261105", "ts_code": "000001.SZ", "score": 0.9},
            {"model_id": "m1", "trade_date": "20261105", "ts_code": "000002.SZ", "score": 0.1},
            {"model_id": "m1", "trade_date": "20261105", "ts_code": "000003.SZ", "score": 0.5},
        ]
    )
    data_db = mongomock.MongoClient()["quant_data"]
    data_db["stock_info"].insert_many(
        [
            {"symbol": "000001.SZ", "name": "A", "industry": "银行"},
            {"symbol": "000002.SZ", "name": "B", "industry": "地产"},
            {"symbol": "000003.SZ", "name": "C", "industry": "化工"},
        ]
    )
    data_db["index_constituents"].insert_many(
        [
            {"index_code": "csi1000", "weight_trade_date": "20261105", "update_date": "20261105", "symbol": "000001.SZ"},
            {"index_code": "csi1000", "weight_trade_date": "20261105", "update_date": "20261105", "symbol": "000002.SZ"},
        ]
    )
    config = {
        "score_type": "score_column",
        "score_column": "model_score",
        "model_id": "m1",
        "construction_mode": "top_n",
    }
    frame = load_score_frame(
        None, "20261106", "csi1000", config, data_db=data_db, ml_db=ml_db
    )
    assert list(frame["symbol"]) == ["000001.SZ", "000002.SZ"]
    assert frame["name"].tolist() == ["A", "B"]
    assert set(frame["score_date"]) == {"20261105"}


def test_load_index_universe_symbols_asof_is_pit():
    data_db = mongomock.MongoClient()["quant_data"]
    data_db["index_constituents"].insert_many(
        [
            {"index_code": "csi1000", "weight_trade_date": "20260930", "update_date": "20261030", "symbol": "000001.SZ"},
            {"index_code": "csi1000", "weight_trade_date": "20261030", "update_date": "20261102", "symbol": "000002.SZ"},
        ]
    )
    assert load_index_universe_symbols_asof(data_db, "csi1000", "20261030") == ["000001.SZ"]
    assert load_index_universe_symbols_asof(data_db, "csi1000", "20261102") == ["000002.SZ"]


def test_resolve_plan_score_date_model_skips_stock_scores():
    config = {"model_id": "m1"}
    assert (
        resolve_plan_score_date(
            _ExplodingCollection(), None, "20261106", "csi1000", config
        )
        == "20261106"
    )


def test_candidate_stats_model_uses_trade_calendar():
    data_db = mongomock.MongoClient()["quant_data"]
    data_db["trade_calendar"].insert_many(
        [
            {"exchange": "SSE", "is_open": "1", "trade_date": "20261105"},
            {"exchange": "SSE", "is_open": "1", "trade_date": "20261106"},
        ]
    )
    config = {
        "score_type": "score_column",
        "score_column": "model_score",
        "model_id": "m1",
        "rebalance_days": 2,
        "top_n": 1,
        "construction_mode": "top_n",
    }
    stats = candidate_stats(
        None, "20261106", "csi1000", config, data_db=data_db, ml_db=_ml_db()
    )
    assert stats["000001.SZ"]["candidate_appearances"] == 2
    assert stats["000001.SZ"]["last_candidate_date"] == "20261106"


def test_entry_vol_blocked_symbols_matches_live_threshold():
    data_db = mongomock.MongoClient()["quant_data"]
    rows = []
    date = "20261001"
    stable = 100.0
    volatile = 100.0
    for i in range(25):
        date = f"202610{i + 1:02d}" if i < 9 else f"202611{i - 8:02d}"
        if i:
            stable *= 1.001 if i % 2 else 0.999
            volatile *= 1.20 if i % 2 else 0.80
        rows.append({"symbol": "000001.SZ", "trade_date": date, "close": stable, "open": stable, "high": stable, "low": stable, "volume": 1000})
        rows.append({"symbol": "688001.SH", "trade_date": date, "close": volatile, "open": volatile, "high": volatile, "low": volatile, "volume": 1000})
    data_db["volume_price"].insert_many(rows)
    blocked = entry_vol_blocked_symbols(
        data_db,
        ["000001.SZ", "688001.SH"],
        "20261120",
        max_vol_annual=1.0,
    )
    assert blocked == {"688001.SH"}
