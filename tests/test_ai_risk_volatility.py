import os
import sys
from unittest.mock import patch

import pytest

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


class TestAiRiskVolatility:
    def test_check_high_volatility_medium(self):
        from portfolio.ai_risk_review import check_high_volatility

        features = {
            "vol_20d_annual": 0.65,
            "vol_60d_annual": 0.55,
            "atr_pct_20d": 0.05,
            "vol_percentile_250d": 70.0,
            "abnormal_range_flag": False,
        }
        with patch("portfolio.ai_risk_review.fetch_volume_price_rows", return_value=[{"close": 10}]):
            with patch("portfolio.ai_risk_review.compute_volatility_features_from_rows", return_value=features):
                findings = check_high_volatility(object(), "000001.SZ", "20240110")
        assert len(findings) == 1
        assert findings[0].code == "high_volatility"
        assert findings[0].severity == "medium"

    def test_check_high_volatility_high_on_atr(self):
        from portfolio.ai_risk_review import check_high_volatility

        features = {
            "vol_20d_annual": 0.40,
            "vol_60d_annual": 0.35,
            "atr_pct_20d": 0.12,
            "vol_percentile_250d": 50.0,
            "abnormal_range_flag": False,
        }
        with patch("portfolio.ai_risk_review.fetch_volume_price_rows", return_value=[{"close": 10}]):
            with patch("portfolio.ai_risk_review.compute_volatility_features_from_rows", return_value=features):
                findings = check_high_volatility(object(), "000001.SZ", "20240110")
        assert findings[0].severity == "high"

    def test_check_high_volatility_no_data(self):
        from portfolio.ai_risk_review import check_high_volatility

        with patch("portfolio.ai_risk_review.fetch_volume_price_rows", return_value=[]):
            with patch("portfolio.ai_risk_review.compute_volatility_features_from_rows", return_value={}):
                assert check_high_volatility(object(), "000001.SZ", "20240110") == []

    def test_review_symbol_includes_volatility(self):
        from portfolio.ai_risk_review import review_symbol

        features = {
            "vol_20d_annual": 0.85,
            "vol_60d_annual": 0.70,
            "atr_pct_20d": 0.06,
            "vol_percentile_250d": 80.0,
            "abnormal_range_flag": False,
        }
        with patch("portfolio.ai_risk_review.check_recent_surge", return_value=[]):
            with patch("portfolio.ai_risk_review.check_share_unlock", return_value=[]):
                with patch("portfolio.ai_risk_review.check_holder_reduction", return_value=[]):
                    with patch("portfolio.ai_risk_review.fetch_volume_price_rows", return_value=[{"close": 10}]):
                        with patch("portfolio.ai_risk_review.compute_volatility_features_from_rows", return_value=features):
                            risk = review_symbol(object(), "000001.SZ", "20240110")
        assert "波动率偏高" in risk["reasons"]
        assert risk["severity"] == "high"


class TestAiRiskEntryDistribution:
    def test_classify_entry_distribution_high(self):
        from portfolio.ai_risk_review import classify_entry_distribution_risk

        metrics = {
            "hipos120": 0.98,
            "mainflow20": -0.06,
            "distrib": 0.0588,
            "ret_20d": 35.0,
            "ret_60d": 55.0,
        }

        assert classify_entry_distribution_risk(metrics) == "high"

    def test_classify_entry_distribution_ignores_inflow(self):
        from portfolio.ai_risk_review import classify_entry_distribution_risk

        metrics = {
            "hipos120": 0.99,
            "mainflow20": 0.03,
            "distrib": -0.0297,
            "ret_20d": 40.0,
        }

        assert classify_entry_distribution_risk(metrics) == "none"

    def test_check_distribution_at_highs_returns_finding(self):
        from portfolio.ai_risk_review import check_distribution_at_highs

        metrics = {
            "status": "ok",
            "as_of": "20240110",
            "hipos120": 0.97,
            "mainflow20": -0.03,
            "distrib": 0.0291,
            "ret_20d": 20.0,
            "ret_60d": 45.0,
        }
        with patch("portfolio.ai_risk_review.compute_entry_risk_metrics", return_value=metrics):
            findings = check_distribution_at_highs(object(), "000001.SZ", "20240110")

        assert len(findings) == 1
        assert findings[0].code == "distribution_at_highs"
        assert findings[0].severity == "medium"
        assert findings[0].metrics["hipos120"] == 0.97
