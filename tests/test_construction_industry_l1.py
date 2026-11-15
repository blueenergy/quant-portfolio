"""Tests for Shenwan L1 filtering in portfolio construction."""

import os

import mongomock
import pytest

os.environ.pop("TUSHARE_TOKEN", None)

from portfolio.construction import load_score_frame, select_portfolio


@pytest.fixture
def data_db():
    client = mongomock.MongoClient()
    db = client["quant_data"]
    db["sw_index_classify"].insert_one(
        {
            "src": "SW2021",
            "level": "L1",
            "index_code": "801080.SI",
            "industry_name": "电子",
        }
    )
    db["sw_index_member"].insert_many(
        [
            {
                "con_code": "000001.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "in_date": "20150105",
                "out_date": None,
                "l1_code": "801080.SI",
                "l1_name": "电子",
            },
            {
                "con_code": "600000.SH",
                "src": "SW2021",
                "is_new": "Y",
                "in_date": "20150105",
                "out_date": None,
                "l1_code": "801780.SI",
                "l1_name": "银行",
            },
            {
                "con_code": "000002.SZ",
                "src": "SW2021",
                "is_new": "Y",
                "in_date": "20150105",
                "out_date": None,
                "l1_code": "801080.SI",
                "l1_name": "电子",
            },
            {
                "con_code": "600001.SH",
                "src": "SW2021",
                "is_new": "Y",
                "in_date": "20150105",
                "out_date": None,
                "l1_code": "801780.SI",
                "l1_name": "银行",
            },
        ]
    )
    return db


def test_load_score_frame_filters_by_industry_chinese_name(data_db):
    stock_scores = mongomock.MongoClient()["app"]["stock_scores"]
    stock_scores.insert_many(
        [
            {
                "symbol": "000001",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 90.0,
                "cycle_score": 80.0,
            },
            {
                "symbol": "600000",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 85.0,
                "cycle_score": 75.0,
            },
        ]
    )
    config = {
        "score_type": "growth_cycle_weighted",
        "growth_weight": 0.3,
        "cycle_weight": 0.7,
        "industry_l1": "电子",
    }
    frame = load_score_frame(stock_scores, "20250102", "csi1000", config, data_db=data_db)
    assert list(frame["symbol"]) == ["000001"]


def test_load_score_frame_uses_sw_l1_for_industry_cap(data_db):
    stock_scores = mongomock.MongoClient()["app"]["stock_scores"]
    stock_scores.insert_many(
        [
            {
                "symbol": "000001",
                "industry": "半导体",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 90.0,
                "cycle_score": 90.0,
            },
            {
                "symbol": "000002",
                "industry": "元器件",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 89.0,
                "cycle_score": 89.0,
            },
            {
                "symbol": "600000",
                "industry": "银行",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 80.0,
                "cycle_score": 80.0,
            },
            {
                "symbol": "600001",
                "industry": "保险",
                "score_date": "20250102",
                "index_codes": ["csi1000"],
                "growth_score": 79.0,
                "cycle_score": 79.0,
            },
        ]
    )
    config = {
        "score_type": "growth_cycle_weighted",
        "growth_weight": 0.3,
        "cycle_weight": 0.7,
        "construction_mode": "industry_capped",
        "top_n": 2,
        "max_industry_weight": 0.5,
    }
    frame = load_score_frame(stock_scores, "20250102", "csi1000", config, data_db=data_db)
    assert "industry_cap" in frame.columns
    # Both electronics names share SW L1 801080.SI even though their fine
    # industry labels differ; the cap must group them together.
    assert frame.set_index("symbol").loc["000001", "industry_cap"] == "801080.SI"
    assert frame.set_index("symbol").loc["000002", "industry_cap"] == "801080.SI"

    selected = select_portfolio(frame, config)
    assert list(selected["symbol"]) == ["000001", "600000"]
