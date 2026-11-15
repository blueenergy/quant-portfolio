import mongomock

from portfolio.trailing_stop import fetch_trailing_stop_price_rows


def _db(rows, factors):
    db = mongomock.MongoClient()["quant_data"]
    if rows:
        db["volume_price"].insert_many(rows)
    if factors:
        db["stock_adj_factor"].insert_many(factors)
    return db


def test_fetch_trailing_stop_price_rows_applies_hfq():
    db = _db(
        [
            {"symbol": "000001.SZ", "trade_date": "20260101", "open": 10, "high": 11, "low": 9, "close": 10},
            {"symbol": "000001.SZ", "trade_date": "20260102", "open": 10, "high": 12, "low": 10, "close": 11},
        ],
        [
            {"symbol": "000001.SZ", "trade_date": "20260101", "adj_factor": 2.0},
            {"symbol": "000001.SZ", "trade_date": "20260102", "adj_factor": 2.0},
        ],
    )
    rows = fetch_trailing_stop_price_rows(db, "000001.SZ", "20260101", "20260102")
    assert [row["close"] for row in rows] == [20.0, 22.0]
    assert [row["high"] for row in rows] == [22.0, 24.0]


def test_fetch_trailing_stop_price_rows_bfills_leading_factor_gap():
    db = _db(
        [
            {"symbol": "000001.SZ", "trade_date": "20260101", "open": 10, "high": 10, "low": 10, "close": 10},
            {"symbol": "000001.SZ", "trade_date": "20260102", "open": 10, "high": 10, "low": 10, "close": 10},
        ],
        [{"symbol": "000001.SZ", "trade_date": "20260102", "adj_factor": 3.0}],
    )
    rows = fetch_trailing_stop_price_rows(db, "000001.SZ", "20260101", "20260102")
    assert [row["close"] for row in rows] == [30.0, 30.0]


def test_fetch_trailing_stop_price_rows_without_factors_keeps_raw():
    db = _db(
        [{"symbol": "000001.SZ", "trade_date": "20260101", "open": 10, "high": 10, "low": 10, "close": 10}],
        [],
    )
    rows = fetch_trailing_stop_price_rows(db, "000001.SZ", "20260101", "20260101")
    assert rows[0]["close"] == 10.0
