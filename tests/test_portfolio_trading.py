import time
from datetime import datetime, timedelta

import pytest

PAPER_LINEAGE_OVERRIDE = {"capital_basis": "rolling_paper"}

import portfolio.db as portfolio_db
import tools.portfolio_trading as portfolio_trading
from tools.portfolio_trading import (
    CapitalBaselineError,
    approve_plan,
    apply_approved_paper_plans,
    apply_manual_plan_lineage_effects,
    apply_paper_trade_plan,
    backfill_plan_items_from_lineage,
    compute_next_rebalance_date,
    enqueue_auto_generation_tasks,
    find_active_plan_subscriptions,
    generate_manual_rebalance_plan,
    generate_trade_plan,
    get_lineage_excluded_symbols,
    get_trading_collections,
    is_lineage_paused,
    latest_lineage_position_snapshot,
    mark_paper_equity,
    normalize_manual_target_shares,
    pause_lineage,
    resume_lineage,
    seed_default_strategy_configs,
    sweep_stale_zero_fill_plans,
    sync_paper_executed_plan_statuses,
)


class _Result:
    def __init__(self, upserted_id=None, modified_count=0):
        self.upserted_id = upserted_id
        self.modified_count = modified_count


class _Cursor:
    def __init__(self, docs):
        self.docs = list(docs)

    def sort(self, key, direction=None):
        if isinstance(key, list) and direction is None:
            for sort_key, sort_direction in reversed(key):
                self.docs = sorted(
                    self.docs,
                    key=lambda doc: doc.get(sort_key) or "",
                    reverse=sort_direction < 0,
                )
            return self
        self.docs = sorted(self.docs, key=lambda doc: doc.get(key) or "", reverse=direction < 0)
        return self

    def limit(self, n):
        self.docs = self.docs[:n]
        return self

    def __iter__(self):
        return iter(self.docs)


class _Collection:
    def __init__(self):
        self.docs = []

    def create_index(self, *args, **kwargs):
        return True

    def find_one(self, query=None, projection=None, sort=None):
        docs = list(self.find(query, projection))
        if sort:
            for key, direction in reversed(sort):
                docs = sorted(docs, key=lambda doc: doc.get(key) or "", reverse=direction < 0)
        return docs[0] if docs else None

    def find(self, query=None, projection=None):
        rows = [self._project(doc, projection) for doc in self.docs if self._matches(doc, query or {})]
        return _Cursor(rows)

    def distinct(self, field, query=None):
        values = []
        seen = set()
        for doc in self.docs:
            if not self._matches(doc, query or {}):
                continue
            value = doc.get(field)
            if value not in seen:
                seen.add(value)
                values.append(value)
        return values

    def update_one(self, query, update, upsert=False):
        for doc in self.docs:
            if self._matches(doc, query):
                doc.update(update.get("$set", {}))
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                return _Result(modified_count=1)
        if upsert:
            new_doc = {**query, **update.get("$setOnInsert", {}), **update.get("$set", {})}
            for key, value in update.get("$inc", {}).items():
                new_doc[key] = value
            self.docs.append(new_doc)
            return _Result(upserted_id="new")
        return _Result()

    def update_many(self, query, update):
        count = 0
        for doc in self.docs:
            if self._matches(doc, query):
                doc.update(update.get("$set", {}))
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                count += 1
        return _Result(modified_count=count)

    def find_one_and_update(self, query, update, sort=None, return_document=None):
        docs = list(self.find(query))
        if sort:
            for key, direction in reversed(sort):
                docs = sorted(docs, key=lambda doc: doc.get(key) or "", reverse=direction < 0)
        if not docs:
            return None
        target = docs[0]
        for doc in self.docs:
            if doc is target or self._matches(doc, {"task_id": target.get("task_id")}):
                doc.update(update.get("$set", {}))
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                return dict(doc)
        return None

    def replace_one(self, query, new_doc, upsert=False):
        for idx, doc in enumerate(self.docs):
            if self._matches(doc, query):
                self.docs[idx] = dict(new_doc)
                return _Result(modified_count=1)
        if upsert:
            self.docs.append(dict(new_doc))
            return _Result(upserted_id="new")
        return _Result()

    def insert_one(self, doc):
        self.docs.append(dict(doc))
        return _Result(upserted_id="new")

    def insert_many(self, docs):
        self.docs.extend(dict(doc) for doc in docs)
        return _Result(upserted_id="new")

    def delete_many(self, query):
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not self._matches(doc, query)]
        return _Result(modified_count=before - len(self.docs))

    def delete_one(self, query):
        for idx, doc in enumerate(self.docs):
            if self._matches(doc, query):
                del self.docs[idx]
                return _Result(modified_count=1)
        return _Result(modified_count=0)

    def count_documents(self, query=None):
        return sum(1 for doc in self.docs if self._matches(doc, query or {}))

    def _project(self, doc, projection):
        if not projection:
            return dict(doc)
        include = {key for key, value in projection.items() if value}
        if not include:
            return dict(doc)
        return {key: doc.get(key) for key in include if key in doc}

    def _matches(self, doc, query):
        if "$or" in query:
            rest = {key: value for key, value in query.items() if key != "$or"}
            if rest and not self._matches(doc, rest):
                return False
            return any(self._matches(doc, clause) for clause in query["$or"])
        for key, expected in query.items():
            value = doc.get(key)
            if isinstance(expected, dict):
                if "$exists" in expected:
                    exists = key in doc
                    if bool(expected["$exists"]) != exists:
                        return False
                if "$lte" in expected and not (value <= expected["$lte"]):
                    return False
                if "$lt" in expected and not (value < expected["$lt"]):
                    return False
                if "$gte" in expected and not (value >= expected["$gte"]):
                    return False
                if "$gt" in expected and not (value > expected["$gt"]):
                    return False
                if "$ne" in expected and value == expected["$ne"]:
                    return False
                if "$nin" in expected and value in expected["$nin"]:
                    return False
                if "$in" in expected:
                    if isinstance(value, list):
                        if not set(value).intersection(expected["$in"]):
                            return False
                    elif value not in expected["$in"]:
                        return False
                    continue
            elif isinstance(value, list):
                if expected not in value:
                    return False
            elif value != expected:
                return False
        return True


class _AggregatingVolumePriceCollection(_Collection):
    def __init__(self):
        super().__init__()
        self.aggregate_calls = []

    def aggregate(self, pipeline):
        self.aggregate_calls.append(pipeline)
        match = pipeline[0]["$match"]
        rows = [
            self._project(doc, {"_id": 0, "symbol": 1, "trade_date": 1, "close": 1})
            for doc in self.docs
            if self._matches(doc, match)
        ]
        latest_by_symbol = {}
        for row in rows:
            symbol = row.get("symbol")
            if not symbol:
                continue
            current = latest_by_symbol.get(symbol)
            if current is None or str(row.get("trade_date") or "") > str(current.get("trade_date") or ""):
                latest_by_symbol[symbol] = row
        return [{"_id": symbol, "doc": doc} for symbol, doc in latest_by_symbol.items()]


class _Db(dict):
    def __getitem__(self, name):
        if name not in self:
            self[name] = _Collection()
        return dict.__getitem__(self, name)


def _seed_csi1000_index(data_db, *, end_date: str = "20250110", bull: bool = True) -> None:
    """Enough MA120 history for live CSI1000 cash overlay in unit tests."""
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
        data_db["index_prices"].docs.append(
            {"ts_code": "000852.SH", "trade_date": trade_date, "close": close}
        )
        day += timedelta(days=1)


def _setup_dbs():
    app_db = _Db()
    data_db = _Db()
    trading_db = _Db()
    cols = get_trading_collections(trading_db)
    seed_default_strategy_configs(cols)
    for date in ["20250101", "20250102", "20250103", "20250106"]:
        for idx in range(1, 15):
            symbol = f"000{idx:03d}"
            data_db["index_constituents"].docs.append(
                {
                    "index_code": "csi1000",
                    "symbol": symbol,
                    "update_date": "20250101",
                    "update_time": "000000",
                }
            )
            app_db["stock_scores"].docs.append(
                {
                    "symbol": symbol,
                    "name": f"Stock {idx}",
                    "industry": "Tech" if idx <= 8 else "Bank",
                    "score_date": date,
                    "index_codes": ["csi1000"],
                    "growth_score": 100 - idx,
                    "cycle_score": 100 - idx * 2,
                }
            )
            data_db["volume_price"].docs.append(
                {
                    "symbol": symbol,
                    "trade_date": date,
                    "open": 10 + idx,
                    "close": 10 + idx,
                }
            )
    data_db["volume_price"].docs.append({"symbol": "000001", "trade_date": "20250107", "open": 12, "close": 12})
    for idx in range(2, 15):
        data_db["volume_price"].docs.append({"symbol": f"000{idx:03d}", "trade_date": "20250107", "open": 10 + idx, "close": 10 + idx})
    _seed_csi1000_index(data_db, bull=True)
    return app_db, data_db, cols


def test_latest_close_prices_on_or_before_uses_batched_aggregate():
    volume_price = _AggregatingVolumePriceCollection()
    volume_price.docs.extend(
        [
            {"symbol": "000001", "trade_date": "20250105", "close": 10},
            {"symbol": "000001", "trade_date": "20250106", "close": 11},
            {"symbol": "000002", "trade_date": "20250104", "close": 20},
            {"symbol": "000002", "trade_date": "20250107", "close": 21},
            {"symbol": "000003", "trade_date": "20250106", "close": None},
        ]
    )

    prices, price_dates = portfolio_trading.latest_close_prices_on_or_before(
        {"volume_price": volume_price},
        ["000001", "000002", "000002", ""],
        "20250106",
    )

    assert prices == {"000001": 11.0, "000002": 20.0}
    assert price_dates == {"000001": "20250106", "000002": "20250104"}
    assert len(volume_price.aggregate_calls) == 1


def test_generate_trade_plan_writes_rebalance_plan_and_items():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["status"] == "needs_review"
    assert result["plan"]["paper_execution_mode"] == "manual_review"
    assert result["plan"].get("reviewer") is None
    assert result["plan"]["plan_type"] == "rebalance"
    assert result["plan"]["execute_date"] == "20250107"
    assert len(result["items"]) == 10
    assert result["items"][0]["candidate_appearances"] >= 1
    assert cols.trade_plans.find_one({"plan_id": result["plan"]["plan_id"]})


def test_generate_trade_plan_refuses_when_lineage_cannot_be_reproduced():
    """Lineage invariant: a regeneration that resolves to another lineage must abort."""
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    real_hash = result["plan"]["params_hash"]

    same = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        expected_params_hash=real_hash,
    )
    assert same["plan"]["params_hash"] == real_hash

    with pytest.raises(ValueError, match="does not match expected lineage"):
        generate_trade_plan(
            strategy_template_id="growth_cycle_index_enhanced",
            base_date="20250106",
            mode="rebalance",
            app_db=app_db,
            data_db=data_db,
            trading_collections=cols,
            expected_params_hash="deadbeef1234",
        )


def test_force_rebalance_rebuilds_anchor_lineage_from_snapshot():
    """End-to-end force rebalance against the 8f73d17eb0ab -> 7cd2a2143a5f case.

    A lineage key that is legitimately ``None`` must survive the snapshot
    round-trip and the preset substitution. Without ``exact_override`` it is
    dropped, the substituted preset's value wins, and the anchor's hash is not
    reproduced -- which the invariant then has to catch.
    """
    app_db, data_db, cols = _setup_dbs()

    anchor = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        params_override={"max_industry_weight": None},
        exact_override=True,
    )
    assert anchor["plan"]["params_snapshot"]["max_industry_weight"] is None
    anchor_hash = anchor["plan"]["params_hash"]
    # What the force-rebalance endpoint forwards: the normalized anchor snapshot.
    forwarded = anchor["plan"]["params_snapshot"]

    rebuilt = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        params_override=forwarded,
        exact_override=True,
        expected_params_hash=anchor_hash,
    )
    assert rebuilt["plan"]["params_hash"] == anchor_hash

    with pytest.raises(ValueError, match="does not match expected lineage"):
        generate_trade_plan(
            strategy_template_id="growth_cycle_index_enhanced",
            base_date="20250106",
            mode="rebalance",
            app_db=app_db,
            data_db=data_db,
            trading_collections=cols,
            params_override=forwarded,
            expected_params_hash=anchor_hash,
        )


def test_generate_trade_plan_requires_next_execute_date(monkeypatch):
    monkeypatch.setattr("portfolio.calendar._tushare_pro", lambda: None)
    app_db, data_db, cols = _setup_dbs()

    with pytest.raises(ValueError, match="No next trading date available"):
        generate_trade_plan(
            strategy_template_id="growth_cycle_index_enhanced",
            base_date="20250107",
            mode="rebalance",
            app_db=app_db,
            data_db=data_db,
            trading_collections=cols,
        )


def test_generate_trade_plan_uses_trade_calendar_for_execute_date(monkeypatch):
    class _Series(list):
        def tolist(self):
            return list(self)

    class _Frame:
        empty = False

        def __getitem__(self, key):
            assert key == "cal_date"
            return _Series(["20250107", "20250108"])

    class _Pro:
        def trade_cal(self, **kwargs):
            assert kwargs["start_date"] == "20250107"
            return _Frame()

    monkeypatch.setattr("portfolio.calendar._tushare_pro", lambda: _Pro())
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250107",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["execute_date"] == "20250108"


def test_generate_rebalance_supersedes_stale_active_plan():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 1, "initial_capital": 100_000}
    old = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    new = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    old_doc = cols.trade_plans.find_one({"plan_id": old["plan"]["plan_id"]})
    assert old_doc["status"] == "superseded"
    assert old_doc["superseded_by_plan_id"] == new["plan"]["plan_id"]
    assert new["plan"]["supersedes_plan_ids"] == [old["plan"]["plan_id"]]


def test_generate_rebalance_supersede_cancels_open_live_signals():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 1, "initial_capital": 100_000}
    old = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    old_plan_id = old["plan"]["plan_id"]
    app_db["trade_signals"].insert_one(
        {
            "order_id": "open-live-1",
            "plan_id": old_plan_id,
            "user_id": "user-1",
            "mode": "live",
            "status": "submitted",
        }
    )

    new = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    old_doc = cols.trade_plans.find_one({"plan_id": old_plan_id})
    signal = app_db["trade_signals"].find_one({"order_id": "open-live-1"})
    assert old_doc["status"] == "superseded"
    assert old_doc["supersede_cancelled_signal_count"] == 1
    assert signal["status"] == "cancelled"
    assert signal["cancel_reason"] == f"superseded by {new['plan']['plan_id']}"
    assert new["plan"]["supersedes_plan_ids"] == [old_plan_id]


def test_generate_rebalance_does_not_supersede_live_filled_plan():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 1, "initial_capital": 100_000}
    old = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    old_plan_id = old["plan"]["plan_id"]
    approve_plan(cols, old_plan_id, reviewer="tester", comment="ok")
    app_db["trade_signals"].insert_one(
        {
            "order_id": "filled-live-1",
            "plan_id": old_plan_id,
            "user_id": "user-1",
            "mode": "live",
            "status": "partial_filled",
            "filled_qty": 100,
        }
    )

    new = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    old_doc = cols.trade_plans.find_one({"plan_id": old_plan_id})
    blocked_log = cols.operation_logs.find_one({"plan_id": old_plan_id, "operation_type": "supersede_plan"})
    assert old_doc["status"] == "approved"
    assert new["plan"]["supersedes_plan_ids"] == []
    assert blocked_log["status"] == "blocked"


def _seed_stuck_plan(cols, app_db, *, plan_id="stuck-1", origin="manual", signals=None):
    cols.trade_plans.insert_one(
        {
            "plan_id": plan_id,
            "user_id": "user-1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-1",
            "record_kind": "trade_plan",
            "origin": origin,
            "status": "approved",
            "live_signals_published_at": "2026-07-08T02:17:35",
        }
    )
    for sig in signals or []:
        app_db["trade_signals"].insert_one({"plan_id": plan_id, "user_id": "user-1", "mode": "live", **sig})


def test_sweep_expires_published_zero_fill_plan():
    app_db, _data_db, cols = _setup_dbs()
    _seed_stuck_plan(
        cols,
        app_db,
        signals=[{"order_id": "o1", "status": "cancelled", "filled_qty": 0}],
    )
    app_db["trade_executions"].insert_one({"order_id": "o1", "user_id": "user-1", "status": "cancelled", "filled_size": 0})

    expired = sweep_stale_zero_fill_plans(
        cols,
        app_db=app_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-1",
        user_id="user-1",
    )

    assert expired == ["stuck-1"]
    doc = cols.trade_plans.find_one({"plan_id": "stuck-1"})
    assert doc["status"] == "expired"
    assert doc.get("expired_at") is not None
    log = cols.operation_logs.find_one({"plan_id": "stuck-1", "operation_type": "expire_plan"})
    assert log["status"] == "success"


def test_sweep_skips_plan_with_live_fill():
    app_db, _data_db, cols = _setup_dbs()
    _seed_stuck_plan(
        cols,
        app_db,
        signals=[{"order_id": "o1", "status": "partial_cancelled", "filled_qty": 100}],
    )

    expired = sweep_stale_zero_fill_plans(
        cols,
        app_db=app_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-1",
        user_id="user-1",
    )

    assert expired == []
    assert cols.trade_plans.find_one({"plan_id": "stuck-1"})["status"] == "approved"


def test_sweep_skips_plan_with_active_signal():
    app_db, _data_db, cols = _setup_dbs()
    _seed_stuck_plan(
        cols,
        app_db,
        signals=[
            {"order_id": "o1", "status": "cancelled", "filled_qty": 0},
            {"order_id": "o2", "status": "submitted"},
        ],
    )

    expired = sweep_stale_zero_fill_plans(
        cols,
        app_db=app_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-1",
        user_id="user-1",
    )

    assert expired == []
    assert cols.trade_plans.find_one({"plan_id": "stuck-1"})["status"] == "approved"


def test_sweep_skips_unpublished_plan():
    app_db, _data_db, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "unpub-1",
            "user_id": "user-1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-1",
            "record_kind": "trade_plan",
            "status": "approved",
        }
    )

    expired = sweep_stale_zero_fill_plans(
        cols,
        app_db=app_db,
        strategy_template_id="growth_cycle_index_enhanced",
        params_hash="hash-1",
        user_id="user-1",
    )

    assert expired == []
    assert cols.trade_plans.find_one({"plan_id": "unpub-1"})["status"] == "approved"


def test_sweep_expires_manual_origin_plan_generation_leaves_behind():
    # End-to-end: a manual-origin published zero-fill plan is never superseded
    # (origin != manual), but the next generation sweeps it to expired.
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 1, "initial_capital": 100_000}
    stuck = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    stuck_plan_id = stuck["plan"]["plan_id"]
    cols.trade_plans.update_one(
        {"plan_id": stuck_plan_id},
        {"$set": {"origin": "manual", "live_signals_published_at": "20250101", "status": "approved"}},
    )
    app_db["trade_signals"].insert_one(
        {"order_id": "dead-1", "plan_id": stuck_plan_id, "user_id": "user-1", "mode": "live", "status": "cancelled", "filled_qty": 0}
    )

    generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    doc = cols.trade_plans.find_one({"plan_id": stuck_plan_id})
    assert doc["status"] == "expired"
    assert doc.get("expired_at") is not None


def test_force_generate_after_cancelled_plan_creates_new_plan_id():
    app_db, data_db, cols = _setup_dbs()
    old = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    cols.trade_plans.update_one({"plan_id": old["plan"]["plan_id"]}, {"$set": {"status": "cancelled"}})

    new = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        force=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    old_doc = cols.trade_plans.find_one({"plan_id": old["plan"]["plan_id"]})
    assert old_doc["status"] == "cancelled"
    assert new["plan"]["plan_id"] != old["plan"]["plan_id"]
    assert "_force_" in new["plan"]["plan_id"]
    assert cols.trade_plans.find_one({"plan_id": new["plan"]["plan_id"]})["status"] == "needs_review"


def test_repeated_force_generate_after_cancelled_plans_creates_unique_plan_ids():
    app_db, data_db, cols = _setup_dbs()
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    cols.trade_plans.update_one({"plan_id": first["plan"]["plan_id"]}, {"$set": {"status": "cancelled"}})

    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        force=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    cols.trade_plans.update_one({"plan_id": second["plan"]["plan_id"]}, {"$set": {"status": "cancelled"}})

    third = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        force=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    plan_ids = [first["plan"]["plan_id"], second["plan"]["plan_id"], third["plan"]["plan_id"]]
    assert len(set(plan_ids)) == 3
    assert "_force_" in second["plan"]["plan_id"]
    assert "_force_" in third["plan"]["plan_id"]
    assert cols.trade_plans.find_one({"plan_id": first["plan"]["plan_id"]})["status"] == "cancelled"
    assert cols.trade_plans.find_one({"plan_id": second["plan"]["plan_id"]})["status"] == "cancelled"
    assert cols.trade_plans.find_one({"plan_id": third["plan"]["plan_id"]})["status"] == "needs_review"


def test_generate_monitor_plan_does_not_supersede_active_plan():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 10, "initial_capital": 100_000}
    old = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250103",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    approve_plan(cols, old["plan"]["plan_id"], reviewer="tester", comment="ok")

    monitor = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="auto",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    old_doc = cols.trade_plans.find_one({"plan_id": old["plan"]["plan_id"]})
    assert old_doc["status"] == "approved"
    assert monitor["plan"]["plan_type"] == "monitor"
    assert monitor["plan"]["supersedes_plan_ids"] == []


def test_generate_trade_plan_clears_preset_id_when_params_are_overridden():
    app_db, data_db, cols = _setup_dbs()
    preset = {
        "preset_id": "preset-top10-10d",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "status": "enabled",
        "params": {
            "universe_index": "csi1000",
            "growth_weight": 0.3,
            "cycle_weight": 0.7,
            "top_n": 10,
            "rebalance_days": 10,
            "construction_mode": "top_n",
            "cash_buffer": 0.03,
            "initial_capital": 1_000_000,
            "lot_size": 100,
            "transaction_cost": 0.001,
        },
    }
    cols.param_presets.insert_one(preset)

    unchanged = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        preset_id=preset["preset_id"],
        params_override=dict(preset["params"]),
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    changed = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        preset_id=preset["preset_id"],
        params_override={**dict(preset["params"]), "top_n": 20, "rebalance_days": 20},
        force=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert unchanged["plan"]["preset_id"] == preset["preset_id"]
    assert changed["plan"].get("preset_id") is None
    assert changed["plan"]["params_snapshot"]["top_n"] == 20
    assert changed["plan"]["params_snapshot"]["rebalance_days"] == 20


def test_generate_trade_plan_rejects_untagged_scores():
    app_db, data_db, cols = _setup_dbs()
    for doc in app_db["stock_scores"].docs:
        doc.pop("index_codes", None)

    with pytest.raises(ValueError, match="csi1000"):
        generate_trade_plan(
            strategy_template_id="growth_cycle_index_enhanced",
            base_date="20250106",
            mode="rebalance",
            app_db=app_db,
            data_db=data_db,
            trading_collections=cols,
        )


def test_generate_trade_plan_ignores_same_day_manual_plan():
    app_db, data_db, cols = _setup_dbs()
    params = {
        "universe_index": "csi1000",
        "top_n": 10,
        "rebalance_days": 10,
        "initial_capital": 100_000,
    }
    previous = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250103",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    params_hash = previous["plan"]["params_hash"]
    cols.trade_plans.insert_one(
        {
            "plan_id": "manual-plan",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": params_hash,
            "user_id": "user-1",
            "base_date": "20250106",
            "origin": "manual",
            "plan_type": "rebalance",
            "status": "executed_paper",
        }
    )

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["origin"] == "strategy"
    assert result["plan"]["plan_id"] != "manual-plan"
    assert result["plan"]["base_date"] == "20250106"
    assert result["plan"]["params_hash"] == params_hash


def test_cadence_anchor_skips_manual_rebalance():
    """A discretionary change must not reset the rebalance clock.

    Manual plans carry explicit per-symbol targets, so they are not the
    strategy re-deciding the whole portfolio. Anchoring on one would starve
    the cadence: reduce or swap a holding every few days and the scheduled
    rebalance never comes due.
    """
    app_db, data_db, cols = _setup_dbs()
    params = {
        "universe_index": "csi1000",
        "top_n": 10,
        "rebalance_days": 3,
        "initial_capital": 100_000,
    }
    strategy_plan = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    params_hash = strategy_plan["plan"]["params_hash"]
    cols.trade_plans.insert_one(
        {
            "plan_id": "manual-plan",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": params_hash,
            "user_id": "user-1",
            "base_date": "20250103",
            "origin": "manual",
            "plan_type": "rebalance",
            "status": "executed_paper",
        }
    )

    # 3 trading days elapsed since the strategy plan, only 1 since the manual
    # one, so anchoring on the manual plan would yield a monitor day.
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="auto",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["plan_type"] == "rebalance"
    assert result["plan"]["is_rebalance_day"] is True
    assert result["plan"]["previous_rebalance_date"] == "20250101"
    assert result["plan"]["previous_rebalance_plan_id"] == strategy_plan["plan"]["plan_id"]


def test_generate_trade_plan_replaces_unaffordable_new_positions_from_candidate_pool():
    app_db, data_db, cols = _setup_dbs()
    for doc in data_db["volume_price"].docs:
        if doc.get("symbol") == "000001" and doc.get("trade_date") == "20250106":
            doc["open"] = 2000
            doc["close"] = 2000

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    symbols = [item["symbol"] for item in result["items"]]

    assert "000001" not in symbols
    assert "000011" in symbols
    assert len(result["items"]) == 10
    assert all(item["target_shares"] >= 100 for item in result["items"])
    assert result["plan"]["summary"]["skip_count"] == 0
    assert result["plan"]["summary"]["buy_count"] == 10
    assert result["plan"]["summary"]["target_top_n"] == 10
    assert result["plan"]["summary"]["hold_count"] == 0


def test_generate_trade_plan_uses_initial_capital_param_without_snapshot():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={"initial_capital": 250_000},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert "initial_capital_override" not in result["plan"]
    assert result["plan"]["params_snapshot"]["initial_capital"] == 250_000
    assert result["plan"]["params_snapshot"]["initial_capital"] == 250_000
    assert result["plan"]["summary"]["cash"] == 250_000
    assert result["plan"]["summary"]["equity"] == 250_000


def test_generate_trade_plan_uses_latest_close_before_non_trading_base_date():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250104",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["score_date"] == "20250103"
    assert result["plan"]["execute_date"] == "20250106"
    assert result["plan"]["summary"]["buy_count"] == 10
    assert result["plan"]["summary"]["skip_count"] == 0
    assert {item["price_trade_date"] for item in result["items"]} == {"20250103"}
    assert all("missing_price" not in item["blockers"] for item in result["items"])


def test_approve_and_apply_paper_trade_plan_updates_state():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]

    review = approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    applied = apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, data_db=data_db)

    assert review["decision"] == "approved"
    assert applied["status"] == "executed_paper"
    assert cols.paper_positions.find_one({"plan_id": plan_id, "as_of_date": "20250107"})
    assert cols.paper_equity.find_one({"plan_id": plan_id, "date": "20250107"})
    assert len(list(cols.paper_executions.find({"plan_id": plan_id}))) == 10


def test_apply_paper_trade_plan_blocks_live_signals():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    app_db["trade_signals"].insert_one(
        {
            "order_id": "live-open-paper-block",
            "plan_id": plan_id,
            "user_id": "user-1",
            "mode": "live",
            "status": "submitted",
        }
    )

    with pytest.raises(ValueError, match="live signals"):
        apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, app_db=app_db, data_db=data_db)
    with pytest.raises(ValueError, match="live signals"):
        apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, app_db=app_db, data_db=data_db, force=True)


def test_apply_paper_trade_plan_blocks_terminal_live_signals():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    app_db["trade_signals"].insert_one(
        {
            "order_id": "live-cancelled-paper-ok",
            "plan_id": plan_id,
            "user_id": "user-1",
            "mode": "live",
            "status": "cancelled",
        }
    )

    with pytest.raises(ValueError, match="live signals"):
        apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, app_db=app_db, data_db=data_db)


def test_user_id_flows_through_plan_and_paper_state():
    app_db, data_db, cols = _setup_dbs()
    user_id = "user-1"

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id=user_id,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]

    assert result["plan"]["user_id"] == user_id
    assert result["plan"]["plan_id"].endswith("_user1")
    assert {item["user_id"] for item in result["items"]} == {user_id}

    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    applied = apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, data_db=data_db)

    assert applied["executions"][0]["user_id"] == user_id
    assert applied["position_snapshot"]["user_id"] == user_id
    assert cols.paper_equity.find_one({"plan_id": plan_id, "date": "20250107"})["user_id"] == user_id


def test_apply_paper_trade_plan_inherits_lineage_snapshot():
    app_db, data_db, cols = _setup_dbs()
    parent = {
        "plan_id": "parent-plan",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "params_hash": "hash-1",
        "user_id": "user-1",
        "base_date": "20250102",
        "execute_date": "20250103",
        "status": "executed_paper",
    }
    child = {
        "plan_id": "child-plan",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "params_hash": "hash-1",
        "user_id": "user-1",
        "base_date": "20250106",
        "execute_date": "20250107",
        "status": "approved",
        "params_snapshot": {"initial_capital": 1_000_000, "lot_size": 100},
    }
    cols.trade_plans.insert_many([parent, child])
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "user_id": "user-1",
            "as_of_date": "20250103",
            "plan_id": "parent-plan",
            "cash": 99_000,
            "equity": 100_000,
            "positions": [{"symbol": "000001", "name": "Stock 1", "shares": 100, "cost_basis": 10.0}],
        }
    )
    cols.trade_plan_items.insert_one(
        {
            "plan_id": "child-plan",
            "user_id": "user-1",
            "symbol": "000001",
            "name": "Stock 1",
            "industry": "Tech",
            "rank": 1,
            "delta_shares": -100,
        }
    )

    applied = apply_paper_trade_plan(plan_id="child-plan", trading_collections=cols, data_db=data_db)

    assert applied["inherited_from_plan_id"] == "parent-plan"
    assert applied["position_snapshot"]["cash"] == 100_200
    assert applied["position_snapshot"]["positions"] == []
    assert cols.paper_equity.find_one({"plan_id": "child-plan"})["inherited_from_plan_id"] == "parent-plan"


def test_global_strategy_can_generate_same_date_plans_for_multiple_users():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-2",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert first["plan"]["plan_id"] != second["plan"]["plan_id"]
    assert first["plan"]["strategy_template_id"] == second["plan"]["strategy_template_id"]
    assert first["plan"]["user_id"] == "user-1"
    assert second["plan"]["user_id"] == "user-2"
    assert cols.trade_plans.find_one({"plan_id": first["plan"]["plan_id"], "user_id": "user-1"})
    assert cols.trade_plans.find_one({"plan_id": second["plan"]["plan_id"], "user_id": "user-2"})


def test_generate_trade_plan_allows_same_date_with_different_params_hash():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override={"top_n": 5},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override={"top_n": 8},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert first["plan"]["plan_id"] != second["plan"]["plan_id"]
    assert first["plan"]["params_hash"] != second["plan"]["params_hash"]
    assert first["plan"]["params_snapshot"]["top_n"] == 5
    assert second["plan"]["params_snapshot"]["top_n"] == 8


def test_plan_params_hash_ignores_execution_and_pool_tuning_params():
    base = {
        "universe_index": "csi1000",
        "score_type": "growth_cycle_weighted",
        "growth_weight": 0.3,
        "cycle_weight": 0.7,
        "top_n": 10,
        "rebalance_days": 20,
        "construction_mode": "top_n",
        "max_industry_weight": 0.3,
        "initial_capital": 1_000_000,
        "lot_size": 100,
        "cash_buffer": 0.03,
        "candidate_pool_enabled": True,
        "candidate_pool_multiplier": 3,
        "min_buy_lots": 1,
    }
    tuned = {
        **base,
        "initial_capital": 2_000_000,
        "lot_size": 200,
        "cash_buffer": 0.1,
        "candidate_pool_enabled": False,
        "candidate_pool_multiplier": 10,
        "min_buy_lots": 3,
        "transaction_cost": 0.002,
    }
    different_strategy = {**base, "top_n": 20}

    assert portfolio_trading.plan_params_hash(tuned) == portfolio_trading.plan_params_hash(base)
    assert portfolio_trading.plan_params_hash(different_strategy) != portfolio_trading.plan_params_hash(base)


def test_plan_params_hash_ignores_trailing_stop_pct():
    base = {
        "universe_index": "csi1000",
        "score_type": "growth_cycle_weighted",
        "growth_weight": 0.3,
        "cycle_weight": 0.7,
        "top_n": 10,
        "rebalance_days": 20,
        "construction_mode": "top_n",
        "max_industry_weight": 0.3,
    }
    with_trailing = {**base, "trailing_stop_pct": 0.15}
    without_trailing = {**base, "trailing_stop_pct": None}

    assert portfolio_trading.plan_params_hash(with_trailing) == portfolio_trading.plan_params_hash(base)
    assert portfolio_trading.plan_params_hash(without_trailing) == portfolio_trading.plan_params_hash(base)


def test_process_portfolio_plan_generation_task_completes_with_plan_id():
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "completed",
            "updated_at": "20250106",
        }
    )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "task-1",
            "status": "pending",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "base_date": "20250106",
            "mode": "rebalance",
            "user_id": "user-1",
            "force": False,
            "params_override": {"initial_capital": 300_000},
            "attempts": 0,
            "created_at": "20250106",
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})

    assert result["status"] == "completed"
    assert result["plan_id"].endswith("_user1")
    assert task["status"] == "completed"
    assert task["plan_id"] == result["plan_id"]
    assert task["attempts"] == 1
    plan = cols.trade_plans.find_one({"plan_id": result["plan_id"]})
    assert "initial_capital_override" not in plan
    assert plan["params_snapshot"]["initial_capital"] == 300_000
    status = cols.worker_status.find_one({"worker_id": "worker-1"})
    assert status["status"] == "completed"
    assert status["last_task_id"] == "task-1"
    assert status["last_plan_id"] == result["plan_id"]
    assert status["completed_count"] == 1


def test_process_portfolio_plan_generation_task_waits_for_running_scores():
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "running",
            "updated_at": "20250106",
        }
    )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "task-1",
            "status": "pending",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "base_date": "20250106",
            "mode": "rebalance",
            "user_id": "user-1",
            "force": False,
            "attempts": 0,
            "created_at": "20250106",
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})

    assert result["status"] == "waiting_for_scores"
    assert task["status"] == "waiting_for_scores"
    assert task["waiting_score_date"] == "20250106"
    status = cols.worker_status.find_one({"worker_id": "worker-1"})
    assert status["status"] == "waiting_for_scores"
    assert status["last_task_id"] == "task-1"
    assert status["waiting_count"] == 1
    assert not cols.trade_plans.find_one({"user_id": "user-1", "strategy_template_id": "growth_cycle_index_enhanced"})


def _waiting_task_fixture():
    """A pending rebalance task whose scoring run is still in flight."""
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "running",
            "updated_at": "20250106",
        }
    )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "task-1",
            "status": "pending",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "base_date": "20250106",
            "mode": "rebalance",
            "user_id": "user-1",
            "force": False,
            "attempts": 0,
            "created_at": "20250106",
        }
    )
    return process_one, app_db, data_db, cols


def test_waiting_plan_task_is_not_reclaimed_until_the_backoff_elapses():
    process_one, app_db, data_db, cols = _waiting_task_fixture()

    first = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert first["status"] == "waiting_for_scores"
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})
    assert task["attempts"] == 1
    assert task["waiting_since"] is not None
    assert task["next_attempt_at"] > task["waiting_since"]

    # The waiting task must NOT be claimable again right away. The generation
    # loop drains up to 20 tasks per pass and sleeps only afterwards, so a
    # claimable waiting task was re-claimed ~8 times/second (4801 attempts over
    # one 10 minute wait).
    second = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert second is None
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})
    assert task["attempts"] == 1
    assert task["status"] == "waiting_for_scores"


def test_waiting_plan_task_is_reclaimed_after_the_backoff_elapses():
    process_one, app_db, data_db, cols = _waiting_task_fixture()

    process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    cols.plan_generation_tasks.update_one(
        {"task_id": "task-1"},
        {"$set": {"next_attempt_at": datetime.utcnow() - timedelta(seconds=1)}},
    )

    second = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert second["status"] == "waiting_for_scores"
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})
    assert task["attempts"] == 2
    # The waiting clock is not reset by a re-check, only the first wait stamps it.
    assert task["waiting_since"] is not None


def test_waiting_plan_task_fails_after_the_waiting_deadline():
    process_one, app_db, data_db, cols = _waiting_task_fixture()

    process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    cols.plan_generation_tasks.update_one(
        {"task_id": "task-1"},
        {
            "$set": {
                "waiting_since": datetime.utcnow() - timedelta(minutes=46),
                "next_attempt_at": datetime.utcnow() - timedelta(seconds=1),
            }
        },
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert result["status"] == "failed"
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})
    assert task["status"] == "failed"
    assert "did not complete" in task["error_message"]


@pytest.mark.parametrize("scoring_status", ["failed", None])
def test_process_portfolio_plan_generation_task_fails_without_completed_scores(scoring_status):
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    if scoring_status is not None:
        app_db["scoring_runs"].docs.append(
            {
                "score_date": "20250106",
                "universe_index": "csi1000",
                "status": scoring_status,
                "updated_at": "20250106",
            }
        )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "task-1",
            "status": "pending",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "base_date": "20250106",
            "mode": "rebalance",
            "user_id": "user-1",
            "force": False,
            "attempts": 0,
            "created_at": "20250106",
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    task = cols.plan_generation_tasks.find_one({"task_id": "task-1"})

    expected_status = scoring_status or "missing"
    assert result["status"] == "failed"
    assert f"status={expected_status}" in result["error_message"]
    assert task["status"] == "failed"
    assert f"status={expected_status}" in task["error_message"]
    status = cols.worker_status.find_one({"worker_id": "worker-1"})
    assert status["status"] == "failed"
    assert status["failed_count"] == 1
    assert not cols.trade_plans.find_one({"user_id": "user-1", "strategy_template_id": "growth_cycle_index_enhanced"})


def test_resolve_scoring_decision_uses_resolved_score_date_for_non_scoring_day():
    from tools.workers.process_portfolio_plan_generation_tasks import _resolve_scoring_decision

    app_db, data_db, cols = _setup_dbs()
    # base_date 20250107 is not a scoring day (no stock_scores / scoring_runs for
    # it). The latest scores are from 20250106 whose run is completed, so the
    # guard must align with that resolved score_date instead of hard-failing.
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "completed",
            "updated_at": "20250106",
        }
    )

    decision, effective_score_date, status = _resolve_scoring_decision(app_db, "20250107", "csi1000")

    assert decision == "proceed"
    assert effective_score_date == "20250106"
    assert status == "completed"


def test_resolve_scoring_decision_normalizes_hyphenated_base_date():
    from tools.workers.process_portfolio_plan_generation_tasks import _resolve_scoring_decision

    app_db, data_db, cols = _setup_dbs()
    # Plan-generation tasks historically stored API dates as YYYY-MM-DD. Without
    # normalization, Mongo string $lte misses YYYYMMDD score_date values.
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "completed",
            "updated_at": "20250106",
        }
    )

    decision, effective_score_date, status = _resolve_scoring_decision(app_db, "2025-01-07", "csi1000")

    assert decision == "proceed"
    assert effective_score_date == "20250106"
    assert status == "completed"


def test_resolve_scoring_decision_waits_when_same_day_scoring_running():
    from tools.workers.process_portfolio_plan_generation_tasks import _resolve_scoring_decision

    app_db, data_db, cols = _setup_dbs()
    # A still-running same-day run must keep blocking even though older completed
    # scores exist, so we never generate off a half-written scoring batch.
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "running",
            "updated_at": "20250106",
        }
    )

    decision, effective_score_date, status = _resolve_scoring_decision(app_db, "20250106", "csi1000")

    assert decision == "wait"
    assert effective_score_date == "20250106"
    assert status == "running"


def test_resolve_scoring_decision_fails_when_resolved_score_date_missing():
    from tools.workers.process_portfolio_plan_generation_tasks import _resolve_scoring_decision

    app_db, data_db, cols = _setup_dbs()
    # No scoring_runs recorded at all: base_date resolves to 20250106 (scores
    # exist) but that run was never marked, so the guard fails explicitly.
    decision, effective_score_date, status = _resolve_scoring_decision(app_db, "20250107", "csi1000")

    assert decision == "fail"
    assert effective_score_date == "20250106"
    assert status == "missing"


def test_process_portfolio_plan_generation_task_records_idle_status_when_empty():
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    status = cols.worker_status.find_one({"worker_id": "worker-1"})

    assert result is None
    assert status["status"] == "idle"
    assert status["current_task_id"] is None


def test_apply_approved_paper_plans_uses_requested_execute_date_when_missing():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    cols.trade_plans.update_one({"plan_id": plan_id}, {"$set": {"execute_date": None}})
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")

    applied = apply_approved_paper_plans(execute_date="20250107", trading_collections=cols, data_db=data_db)

    assert len(applied["applied"]) == 1
    assert applied["applied"][0]["plan_id"] == plan_id
    assert cols.trade_plans.find_one({"plan_id": plan_id})["execute_date"] == "20250107"


def test_apply_approved_paper_plans_falls_back_to_latest_close_when_open_missing():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    data_db["volume_price"].delete_many({"trade_date": "20250107"})
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")

    applied = apply_approved_paper_plans(execute_date="20250107", trading_collections=cols, data_db=data_db)

    assert len(applied["applied"]) == 1
    assert applied["skipped"] == []
    assert cols.trade_plans.find_one({"plan_id": plan_id})["status"] == "executed_paper"
    executions = list(cols.paper_executions.find({"plan_id": plan_id}))
    assert executions
    assert {row["price_source"] for row in executions} == {"latest_close"}
    assert {row["price_trade_date"] for row in executions} == {"20250106"}


def test_apply_approved_paper_plans_skips_when_no_execution_price_exists():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    data_db["volume_price"].delete_many({})
    cols.trade_plan_items.update_many(
        {"plan_id": plan_id},
        {"$set": {"estimated_price": None, "price_trade_date": None}},
    )
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")

    applied = apply_approved_paper_plans(execute_date="20250107", trading_collections=cols, data_db=data_db)

    assert applied["applied"] == []
    assert applied["skipped"][0]["reason"] == "missing_execution_price"
    assert cols.trade_plans.find_one({"plan_id": plan_id})["status"] == "approved"


def test_apply_approved_paper_plans_falls_back_to_estimated_price_when_market_missing():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    data_db["volume_price"].delete_many({})
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")

    applied = apply_approved_paper_plans(execute_date="20250107", trading_collections=cols, data_db=data_db)

    assert len(applied["applied"]) == 1
    assert applied["skipped"] == []
    assert cols.trade_plans.find_one({"plan_id": plan_id})["status"] == "executed_paper"
    executions = list(cols.paper_executions.find({"plan_id": plan_id}))
    assert executions
    assert {row["price_source"] for row in executions} == {"estimated_price"}


def test_apply_approved_paper_plans_skips_live_signals():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    app_db["trade_signals"].insert_one(
        {
            "order_id": "cron-live-open-paper-block",
            "plan_id": plan_id,
            "user_id": "user-1",
            "mode": "live",
            "status": "submitted",
        }
    )

    applied = apply_approved_paper_plans(
        execute_date="20250107",
        trading_collections=cols,
        app_db=app_db,
        data_db=data_db,
    )

    assert applied["applied"] == []
    assert applied["skipped"][0]["reason"] == "live_signals"
    assert not list(cols.paper_executions.find({"plan_id": plan_id}))


def test_mark_paper_equity_writes_daily_equity_without_rebalance():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    applied = apply_paper_trade_plan(plan_id=plan_id, trading_collections=cols, data_db=data_db)
    positions = applied["position_snapshot"]["positions"]
    for pos in positions:
        data_db["volume_price"].docs.append(
            {
                "symbol": pos["symbol"],
                "trade_date": "20250108",
                "open": 20,
                "close": 20,
            }
        )

    marked = mark_paper_equity(as_of_date="20250108", trading_collections=cols, data_db=data_db)
    row = cols.paper_equity.find_one({"plan_id": plan_id, "date": "20250108"})
    snapshot = cols.paper_positions.find_one({"plan_id": plan_id, "as_of_date": "20250108"})

    assert len(marked["marked"]) == 1
    assert marked["marked"][0]["plan_id"] == plan_id
    assert row["date"] == "20250108"
    assert row["plan_id"] == plan_id
    assert snapshot["plan_id"] == plan_id
    assert row["market_value"] == sum(int(pos["shares"]) * 20 for pos in positions)
    assert row["equity"] == row["cash"] + row["market_value"]
    assert row["price_source_summary"]["close_count"] == len(positions)


def test_mark_paper_equity_keeps_same_strategy_plans_isolated():
    app_db, data_db, cols = _setup_dbs()
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override={"top_n": 5},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override={"top_n": 8},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    for result in (first, second):
        approve_plan(cols, result["plan"]["plan_id"], reviewer="tester", comment="ok")
        apply_paper_trade_plan(plan_id=result["plan"]["plan_id"], trading_collections=cols, data_db=data_db)

    symbols = {pos["symbol"] for snapshot in cols.paper_positions.docs for pos in snapshot.get("positions", [])}
    for symbol in symbols:
        data_db["volume_price"].docs.append({"symbol": symbol, "trade_date": "20250108", "open": 20, "close": 20})

    marked = mark_paper_equity(as_of_date="20250108", trading_collections=cols, data_db=data_db)
    first_row = cols.paper_equity.find_one({"plan_id": first["plan"]["plan_id"], "date": "20250108"})
    second_row = cols.paper_equity.find_one({"plan_id": second["plan"]["plan_id"], "date": "20250108"})

    assert {row["plan_id"] for row in marked["marked"]} == {first["plan"]["plan_id"], second["plan"]["plan_id"]}
    assert first_row["plan_id"] == first["plan"]["plan_id"]
    assert second_row["plan_id"] == second["plan"]["plan_id"]
    assert first_row["market_value"] != second_row["market_value"]


def test_latest_lineage_snapshot_never_inherits_across_params_hash():
    app_db, data_db, cols = _setup_dbs()
    # Another lineage under the SAME strategy_template_id but a different
    # params_hash holds a more recent snapshot. It must never leak in.
    cols.paper_positions.docs.append(
        {
            "plan_id": "other-lineage-plan",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-other",
            "user_id": "user-1",
            "as_of_date": "20250115",
            "cash": 0.0,
            "positions": [{"symbol": "000999", "name": "Other", "shares": 100, "cost_basis": 10.0}],
        }
    )
    cols.trade_plans.docs.append(
        {
            "plan_id": "mine-0106",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-mine",
            "user_id": "user-1",
            "base_date": "20250106",
        }
    )
    plan = {
        "plan_id": "mine-0120",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "params_hash": "hash-mine",
        "user_id": "user-1",
        "base_date": "20250120",
    }

    # No snapshot exists for the "mine" lineage yet -> empty, not the other one.
    assert latest_lineage_position_snapshot(cols, plan, "20250120") == {}

    # Add a same-lineage snapshot; now it is inherited, the other stays excluded.
    cols.paper_positions.docs.append(
        {
            "plan_id": "mine-0106",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-mine",
            "user_id": "user-1",
            "as_of_date": "20250108",
            "cash": 0.0,
            "positions": [{"symbol": "000001", "name": "Mine", "shares": 100, "cost_basis": 10.0}],
        }
    )
    inherited = latest_lineage_position_snapshot(cols, plan, "20250120")
    assert inherited.get("params_hash") == "hash-mine"
    assert [p["symbol"] for p in inherited.get("positions", [])] == ["000001"]


def test_mark_paper_equity_only_marks_active_lineage_head():
    app_db, data_db, cols = _setup_dbs()

    def _plan(plan_id, params_hash, base_date):
        cols.trade_plans.docs.append(
            {
                "plan_id": plan_id,
                "strategy_template_id": "growth_cycle_index_enhanced",
                "params_hash": params_hash,
                "user_id": "user-1",
                "base_date": base_date,
                "plan_type": "rebalance",
                "status": "executed_paper",
            }
        )

    def _snapshot(plan_id, params_hash, as_of_date, symbol):
        cols.paper_positions.docs.append(
            {
                "plan_id": plan_id,
                "strategy_template_id": "growth_cycle_index_enhanced",
                "params_hash": params_hash,
                "user_id": "user-1",
                "as_of_date": as_of_date,
                "cash": 0.0,
                "positions": [{"symbol": symbol, "name": symbol, "shares": 100, "cost_basis": 10.0}],
            }
        )

    # Lineage A: a later rebalance (plan-a2) supersedes plan-a1.
    _plan("plan-a1", "hash-a", "20250106")
    _plan("plan-a2", "hash-a", "20250120")
    _snapshot("plan-a1", "hash-a", "20250106", "000001")
    _snapshot("plan-a2", "hash-a", "20250120", "000002")
    # Lineage B: independent, must still be marked.
    _plan("plan-b1", "hash-b", "20250106")
    _snapshot("plan-b1", "hash-b", "20250106", "000003")

    for symbol in ("000001", "000002", "000003"):
        data_db["volume_price"].docs.append(
            {"symbol": symbol, "trade_date": "20250121", "open": 20, "close": 20}
        )

    result = mark_paper_equity(as_of_date="20250121", trading_collections=cols, data_db=data_db)

    marked_ids = {row["plan_id"] for row in result["marked"]}
    assert marked_ids == {"plan-a2", "plan-b1"}
    superseded = {row["plan_id"] for row in result["skipped"] if row["reason"] == "superseded_by_lineage_head"}
    assert superseded == {"plan-a1"}
    # The retired plan must not get a fresh snapshot for the mark date.
    assert cols.paper_positions.find_one({"plan_id": "plan-a1", "as_of_date": "20250121"}) is None
    assert cols.paper_positions.find_one({"plan_id": "plan-a2", "as_of_date": "20250121"}) is not None


def test_monitor_plan_is_not_executable_and_zeroes_deltas():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="monitor",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan = result["plan"]

    assert plan["plan_type"] == "monitor"
    assert plan["status"] == "generated"
    assert plan["executable"] is False
    assert plan["trading_enabled"] is False
    assert plan["non_executable_reason"] == "monitor_no_trade"
    assert plan["monitor_can_trade"] is False
    # No executable deltas, but drift is preserved for display.
    assert all(item["delta_shares"] == 0 for item in result["items"])
    assert any(item["drift_delta_shares"] > 0 for item in result["items"])
    assert plan["summary"]["buy_count"] == 0
    assert plan["summary"]["sell_count"] == 0


def test_monitor_plan_can_trade_when_opted_in():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="monitor",
        params_override={"monitor_can_trade": True},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan = result["plan"]

    assert plan["plan_type"] == "monitor"
    assert plan["status"] == "needs_review"
    assert plan["executable"] is True
    assert plan["monitor_can_trade"] is True
    assert plan["non_executable_reason"] is None
    assert plan["summary"]["buy_count"] > 0
    assert any(item["delta_shares"] == item["drift_delta_shares"] for item in result["items"])


def test_approved_executable_monitor_is_not_overwritten_by_same_day_retry():
    app_db, data_db, cols = _setup_dbs()
    kwargs = {
        "strategy_template_id": "growth_cycle_index_enhanced",
        "base_date": "20250106",
        "mode": "monitor",
        "params_override": {"monitor_can_trade": True},
        "app_db": app_db,
        "data_db": data_db,
        "trading_collections": cols,
    }
    plan = generate_trade_plan(**kwargs)["plan"]
    cols.trade_plans.update_one(
        {"plan_id": plan["plan_id"]},
        {"$set": {"status": "approved", "approved_by": "reviewer"}},
    )

    with pytest.raises(ValueError, match="already approved"):
        generate_trade_plan(**kwargs)

    stored = cols.trade_plans.find_one({"plan_id": plan["plan_id"]})
    assert stored["status"] == "approved"
    assert stored["approved_by"] == "reviewer"


def _seed_matching_positions(cols, plan, items, *, as_of_date, equity=1_000_000.0, extra=None):
    positions = [
        {"symbol": item["symbol"], "shares": item["target_shares"], "name": "", "industry": ""}
        for item in items
        if item.get("action") == "buy"
    ]
    if extra:
        positions.extend(extra)
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": plan["strategy_template_id"],
            "user_id": plan.get("user_id"),
            "plan_id": plan.get("plan_id"),
            "params_hash": plan["params_hash"],
            "as_of_date": as_of_date,
            "equity": equity,
            "cash": equity * 0.03,
            "positions": positions,
        }
    )


def test_rebalance_marks_retained_as_hold_and_dropped_as_sell():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    # Seed a position snapshot that exactly matches the target portfolio, plus one
    # extra holding that is no longer in the target universe.
    _seed_matching_positions(
        cols,
        first["plan"],
        first["items"],
        as_of_date="20250106",
        extra=[{"symbol": "000999", "shares": 300, "name": "Dropped", "industry": "Old"}],
    )
    data_db["volume_price"].docs.append({"symbol": "000999", "trade_date": "20250106", "open": 20.0, "close": 20.0})

    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        force=True,
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    items = {item["symbol"]: item for item in second["items"]}

    # Retained names: target equals current -> hold with zero executable delta.
    retained = [item for item in second["items"] if item["symbol"] != "000999"]
    assert retained, "expected retained target positions"
    assert all(item["action"] == "hold" for item in retained)
    assert all(item["delta_shares"] == 0 for item in retained)
    # Dropped name: sold off on a rebalance day.
    assert items["000999"]["action"] == "sell"
    assert items["000999"]["delta_shares"] == -300
    assert items["000999"]["target_shares"] == 0
    assert items["000999"]["estimated_amount"] == pytest.approx(6000.0)
    assert second["plan"]["summary"]["hold_count"] == len(retained)
    assert second["plan"]["summary"]["sell_count"] == 1


def test_star_retained_hold_does_not_keep_min_order_blocker():
    app_db, data_db, cols = _setup_dbs()
    symbol = "688001"
    for date in ["20250101", "20250102", "20250103", "20250106"]:
        data_db["index_constituents"].docs.append(
            {
                "index_code": "csi1000",
                "symbol": symbol,
                "update_date": "20250101",
                "update_time": "000000",
            }
        )
        app_db["stock_scores"].docs.append(
            {
                "symbol": symbol,
                "name": "Star Held",
                "industry": "Tech",
                "score_date": date,
                "index_codes": ["csi1000"],
                "growth_score": 200,
                "cycle_score": 200,
            }
        )
        data_db["volume_price"].docs.append(
            {"symbol": symbol, "trade_date": date, "open": 10.0, "close": 10.0}
        )
    data_db["volume_price"].docs.append(
        {"symbol": symbol, "trade_date": "20250107", "open": 10.0, "close": 10.0}
    )

    params_override = {**PAPER_LINEAGE_OVERRIDE, "top_n": 1, "cash_buffer": 0.0, "initial_capital": 7000.0}
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override=params_override,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": first["plan"]["strategy_template_id"],
            "params_hash": first["plan"]["params_hash"],
            "user_id": first["plan"].get("user_id"),
            "plan_id": "previous-star-snapshot",
            "as_of_date": "20250106",
            "equity": 7000.0,
            "cash": 1000.0,
            "positions": [
                {"symbol": symbol, "shares": 600, "name": "Star Held", "industry": "Tech"}
            ],
        }
    )

    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        force=True,
        params_override=params_override,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    item = next(item for item in second["items"] if item["symbol"] == symbol)

    assert item["current_shares"] == 600
    assert item["target_shares"] == 600
    assert item["delta_shares"] == 0
    assert item["action"] == "hold"
    assert "below_star_min_order" not in item["blockers"]
    assert second["plan"]["summary"]["risk_flags"] == []


def test_rebalance_inherits_legacy_snapshot_without_params_hash():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    first_items = {item["symbol"]: item for item in first["items"]}
    retained_symbol = first["items"][0]["symbol"]
    reduced_symbol = first["items"][1]["symbol"]
    retained_target = int(first_items[retained_symbol]["target_shares"])
    reduced_target = int(first_items[reduced_symbol]["target_shares"])
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": first["plan"]["strategy_template_id"],
            "user_id": first["plan"].get("user_id"),
            "plan_id": first["plan"]["plan_id"],
            "as_of_date": "20250106",
            "equity": 1_000_000.0,
            "cash": 30_000.0,
            "positions": [
                {"symbol": retained_symbol, "shares": retained_target, "name": "", "industry": ""},
                {"symbol": reduced_symbol, "shares": reduced_target + 100, "name": "", "industry": ""},
            ],
        }
    )

    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        force=True,
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    items = {item["symbol"]: item for item in second["items"]}

    assert items[retained_symbol]["current_shares"] == retained_target
    assert items[retained_symbol]["delta_shares"] == 0
    assert items[retained_symbol]["action"] == "hold"
    assert items[reduced_symbol]["current_shares"] == reduced_target + 100
    assert items[reduced_symbol]["delta_shares"] == -100
    assert items[reduced_symbol]["action"] == "sell"


def test_backfill_plan_items_from_lineage_updates_existing_bad_items():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    for idx in range(1, 15):
        data_db["volume_price"].docs.append({"symbol": f"000{idx:03d}", "trade_date": "20250108", "open": 10 + idx, "close": 10 + idx})
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250107",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    retained_symbol = second["items"][0]["symbol"]
    reduced_symbol = second["items"][1]["symbol"]
    retained_target = int(second["items"][0]["target_shares"])
    reduced_target = int(second["items"][1]["target_shares"])
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": first["plan"]["strategy_template_id"],
            "user_id": first["plan"].get("user_id"),
            "plan_id": first["plan"]["plan_id"],
            "as_of_date": "20250106",
            "equity": 1_000_000.0,
            "cash": 30_000.0,
            "positions": [
                {"symbol": retained_symbol, "shares": retained_target, "name": "", "industry": ""},
                {"symbol": reduced_symbol, "shares": reduced_target + 100, "name": "", "industry": ""},
            ],
        }
    )
    # Simulate a plan already persisted by the old empty-position logic.
    cols.trade_plan_items.update_one(
        {"plan_id": second["plan"]["plan_id"], "symbol": retained_symbol},
        {"$set": {"action": "buy", "current_shares": 0, "delta_shares": retained_target}},
    )
    cols.trade_plan_items.update_one(
        {"plan_id": second["plan"]["plan_id"], "symbol": reduced_symbol},
        {"$set": {"action": "buy", "current_shares": 0, "delta_shares": reduced_target}},
    )

    dry_run = backfill_plan_items_from_lineage(cols, second["plan"]["plan_id"], data_db=data_db, apply=False)
    assert len(dry_run["updated"]) >= 2
    assert cols.trade_plan_items.find_one({"plan_id": second["plan"]["plan_id"], "symbol": retained_symbol})["action"] == "buy"

    applied = backfill_plan_items_from_lineage(cols, second["plan"]["plan_id"], data_db=data_db, apply=True)
    items = {item["symbol"]: item for item in cols.trade_plan_items.find({"plan_id": second["plan"]["plan_id"]})}

    assert applied["source_snapshot"]["plan_id"] == first["plan"]["plan_id"]
    assert items[retained_symbol]["action"] == "hold"
    assert items[retained_symbol]["current_shares"] == retained_target
    assert items[retained_symbol]["delta_shares"] == 0
    assert items[reduced_symbol]["action"] == "sell"
    assert items[reduced_symbol]["current_shares"] == reduced_target + 100
    assert items[reduced_symbol]["delta_shares"] == -100


def test_sync_paper_executed_plan_statuses_updates_legacy_approved_plan():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = result["plan"]["plan_id"]
    approve_plan(cols, plan_id, reviewer="tester", comment="ok")
    cols.paper_executions.insert_one({"plan_id": plan_id, "created_at": "2025-01-07T09:30:00"})

    dry_run = sync_paper_executed_plan_statuses(cols, plan_id=plan_id, apply=False)
    assert dry_run["updated"][0]["plan_id"] == plan_id
    assert cols.trade_plans.find_one({"plan_id": plan_id})["status"] == "approved"

    applied = sync_paper_executed_plan_statuses(cols, plan_id=plan_id, apply=True)
    plan = cols.trade_plans.find_one({"plan_id": plan_id})
    assert applied["updated"][0]["after_status"] == "executed_paper"
    assert plan["status"] == "executed_paper"
    assert plan["paper_executed_at"] == "2025-01-07T09:30:00"


def test_monitor_day_does_not_sell_dropped_position():
    app_db, data_db, cols = _setup_dbs()

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    _seed_matching_positions(
        cols,
        first["plan"],
        first["items"],
        as_of_date="20250106",
        extra=[{"symbol": "000999", "shares": 300, "name": "Dropped", "industry": "Old"}],
    )

    monitor = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="monitor",
        force=True,
        params_override=PAPER_LINEAGE_OVERRIDE,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    dropped = {item["symbol"]: item for item in monitor["items"]}["000999"]

    assert monitor["plan"]["executable"] is False
    assert dropped["delta_shares"] == 0
    assert dropped["drift_delta_shares"] == -300
    assert monitor["plan"]["summary"]["sell_count"] == 0


def test_auto_plan_records_previous_rebalance_plan_id():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 3}

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="auto",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert second["plan"]["plan_type"] == "rebalance"
    assert second["plan"]["previous_rebalance_date"] == "20250101"
    assert second["plan"]["previous_rebalance_plan_id"] == first["plan"]["plan_id"]


def test_forced_rebalance_records_previous_rebalance_plan_id():
    app_db, data_db, cols = _setup_dbs()
    params = {"rebalance_days": 20}

    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250101",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    second = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert second["plan"]["plan_type"] == "rebalance"
    assert second["plan"]["is_rebalance_day"] is True
    assert second["plan"]["previous_rebalance_date"] == "20250101"
    assert second["plan"]["previous_rebalance_plan_id"] == first["plan"]["plan_id"]


def test_find_active_plan_subscriptions_requires_commitment():
    _, _, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-approved",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "preset_id": "preset-1",
            "params_override": {"top_n": 10},
            "base_date": "20250106",
            "status": "executed_paper",
        }
    )
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-generated",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-b",
            "user_id": "user-2",
            "base_date": "20250106",
            "status": "generated",
        }
    )

    subs = find_active_plan_subscriptions(cols, as_of_date="20250107", lookback_days=120)

    assert len(subs) == 1
    assert subs[0]["params_hash"] == "hash-a"
    assert subs[0]["user_id"] == "user-1"
    assert subs[0]["preset_id"] == "preset-1"


def test_approved_plan_without_execution_is_not_auto_subscription():
    _, _, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-approved",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "preset_id": "preset-1",
            "params_override": {"top_n": 10},
            "base_date": "20250106",
            "status": "approved",
        }
    )

    result = enqueue_auto_generation_tasks(cols, "20250107", lookback_days=120)

    assert result["subscriptions"] == 0
    assert result["enqueued"] == []
    assert result["skipped"] == []


def test_enqueue_auto_generation_tasks_is_idempotent():
    _, _, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-approved",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "preset_id": "preset-1",
            "params_override": {"top_n": 10},
            "base_date": "20250106",
            "status": "executed_paper",
        }
    )

    first = enqueue_auto_generation_tasks(cols, "20250107", lookback_days=120)
    second = enqueue_auto_generation_tasks(cols, "20250107", lookback_days=120)

    assert first["subscriptions"] == 1
    assert len(first["enqueued"]) == 1
    task = cols.plan_generation_tasks.find_one({"task_id": first["enqueued"][0]["task_id"]})
    assert task["mode"] == "auto"
    assert task["source"] == "auto_scheduler"
    assert task["base_date"] == "20250107"
    assert task["params_hash"] == "hash-a"
    # Second run must not duplicate the task for the same combo/date.
    assert second["enqueued"] == []
    assert second["skipped"][0]["reason"] == "task_exists"
    assert len(cols.plan_generation_tasks.docs) == 1


def test_enqueue_auto_generation_skips_when_plan_already_exists():
    _, _, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-approved",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
        }
    )
    # A plan already exists for the target base_date.
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-today",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250107",
            "origin": "strategy",
            "plan_type": "monitor",
            "record_kind": "observation",
            "status": "generated",
        }
    )

    result = enqueue_auto_generation_tasks(cols, "20250107", lookback_days=120)

    assert result["enqueued"] == []
    assert result["skipped"][0]["reason"] == "plan_exists"
    assert cols.plan_generation_tasks.docs == []


def test_enqueue_auto_generation_ignores_same_day_manual_plan():
    _, _, cols = _setup_dbs()
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-approved",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250106",
            "status": "executed_paper",
        }
    )
    cols.trade_plans.insert_one(
        {
            "plan_id": "p-manual-today",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-a",
            "user_id": "user-1",
            "base_date": "20250107",
            "origin": "manual",
            "plan_type": "rebalance",
            "status": "executed_paper",
        }
    )

    result = enqueue_auto_generation_tasks(cols, "20250107", lookback_days=120)

    assert result["subscriptions"] == 1
    assert len(result["enqueued"]) == 1
    assert result["skipped"] == []
    task = cols.plan_generation_tasks.find_one({"task_id": result["enqueued"][0]["task_id"]})
    assert task["base_date"] == "20250107"
    assert task["params_hash"] == "hash-a"


def _seed_live_lineage(app_db, data_db, cols, *, user_id="user-1", buy_price=10.0, fee=0.0):
    """Generate a first plan, then publish a synthetic live fill + fresh account.

    Returns the first plan dict so callers can assert lineage continuity.
    """
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250102",
        mode="rebalance",
        user_id=user_id,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan_id = first["plan"]["plan_id"]
    account_id = "acct-1"
    app_db["trade_signals"].insert_one(
        {
            "user_id": user_id,
            "plan_id": plan_id,
            "mode": "live",
            "order_id": "O1",
            "symbol": "000001",
            "securities_account_id": account_id,
        }
    )
    app_db["trade_executions"].insert_one(
        {
            "user_id": user_id,
            "order_id": "O1",
            "symbol": "000001",
            "side": "BUY",
            "filled_size": 100,
            "filled_price": buy_price,
            "total_fee": fee,
            "timestamp": 1,
        }
    )
    app_db["trader_accounts"].insert_one(
        {
            "user_id": user_id,
            "securities_account_id": account_id,
            "total_asset": 1_000_100,
            "available_cash": 999_000,
            "synced_at": time.time(),
        }
    )
    return first


def test_generate_trade_plan_live_lineage_rolls_account_equity():
    app_db, data_db, cols = _setup_dbs()
    _seed_live_lineage(app_db, data_db, cols, buy_price=10.0)

    # Next rebalance must size against rolling combo equity, not reset to 1,000,000.
    # cash = 1,000,000 - 100*10 = 999,000; 000001 marks at 11 on 20250106 ->
    # market_value = 1,100; equity = 1,000,100.
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    summary = result["plan"]["summary"]
    assert result["plan"]["capital_basis"] == "account_equity"
    assert summary["capital_basis"] == "account_equity"
    assert summary["baseline_source"] == "live_lineage_reconstruction"
    assert summary["equity"] == 1_000_100.0
    assert summary["cash"] == 999_000.0
    assert summary["baseline_securities_account_id"] == "acct-1"
    assert result["plan"]["status"] == "needs_review"
    assert result["plan"]["paper_execution_mode"] == "manual_review"


def test_generate_trade_plan_does_not_sell_position_cleared_in_account():
    app_db, data_db, cols = _setup_dbs()
    params = {"top_n": 3, "rebalance_days": 1}
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250102",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    account_id = "acct-1"
    app_db["trade_signals"].insert_one(
        {
            "user_id": "user-1",
            "plan_id": first["plan"]["plan_id"],
            "mode": "live",
            "order_id": "O-cleared",
            "symbol": "000014",
            "securities_account_id": account_id,
        }
    )
    app_db["trade_executions"].insert_one(
        {
            "user_id": "user-1",
            "order_id": "O-cleared",
            "symbol": "000014",
            "side": "BUY",
            "filled_size": 100,
            "filled_price": 24,
            "timestamp": 1,
        }
    )
    app_db["trader_accounts"].insert_one(
        {
            "user_id": "user-1",
            "securities_account_id": account_id,
            "synced_at": time.time(),
        }
    )
    app_db["trader_positions"].insert_one(
        {
            "user_id": "user-1",
            "securities_account_id": account_id,
            "symbol": "000014",
            "qty": 0,
        }
    )

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override=params,
        force=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["summary"]["account_positions_synced"] is True
    assert result["plan"]["summary"]["account_position_adjusted_symbols"] == ["000014"]
    assert not [
        item
        for item in result["items"]
        if item["symbol"] == "000014" and item["action"] == "sell"
    ]


def test_generate_trade_plan_auto_approves_when_auto_shadow_configured():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-paper",
        params_override={"paper_execution_mode": "auto_shadow", "capital_basis": "rolling_paper"},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    plan = result["plan"]
    assert plan["status"] == "approved"
    assert plan["paper_execution_mode"] == "auto_shadow"
    assert plan["capital_basis"] == "rolling_paper"
    review = cols.plan_reviews.find_one({"plan_id": plan["plan_id"]})
    assert review["reviewer"] == "system:paper_auto_shadow"


def test_generate_trade_plan_manual_review_is_default():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["status"] == "needs_review"
    assert result["plan"]["paper_execution_mode"] == "manual_review"
    assert result["plan"]["capital_basis"] == "account_equity"
    assert cols.plan_reviews.find_one({"plan_id": result["plan"]["plan_id"]}) is None


def test_generate_trade_plan_live_lineage_generates_advisory_when_account_stale():
    app_db, data_db, cols = _setup_dbs()
    _seed_live_lineage(app_db, data_db, cols)
    # Force the account snapshot stale.
    for doc in app_db["trader_accounts"].docs:
        doc["synced_at"] = 0

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    plan = result["plan"]
    assert plan["status"] == "generated"
    assert plan["executable"] is False
    assert plan["non_executable_reason"] == "account_not_synced"
    assert plan["baseline_stale"] is True
    assert "account_not_synced" in str(plan["baseline_error"])
    assert plan["summary"]["baseline_stale"] is True
    assert plan["summary"]["account_positions_synced"] is False


def test_generate_trade_plan_live_lineage_generates_advisory_when_account_missing():
    app_db, data_db, cols = _setup_dbs()
    _seed_live_lineage(app_db, data_db, cols)
    app_db["trader_accounts"].docs.clear()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    plan = result["plan"]
    assert plan["status"] == "generated"
    assert plan["executable"] is False
    assert plan["non_executable_reason"] == "account_not_synced"
    assert plan["baseline_stale"] is True
    assert "account_not_synced" in str(plan["baseline_error"])


def test_generate_trade_plan_can_require_fresh_account_snapshot():
    app_db, data_db, cols = _setup_dbs()
    _seed_live_lineage(app_db, data_db, cols)
    for doc in app_db["trader_accounts"].docs:
        doc["synced_at"] = 0

    with pytest.raises(CapitalBaselineError) as exc:
        generate_trade_plan(
            strategy_template_id="growth_cycle_index_enhanced",
            base_date="20250106",
            mode="rebalance",
            user_id="user-1",
            params_override={"plan_on_stale_account": False},
            app_db=app_db,
            data_db=data_db,
            trading_collections=cols,
        )
    assert "account_not_synced" in str(exc.value)


def test_generate_manual_rebalance_plan_generates_advisory_when_account_stale():
    app_db, data_db, cols = _setup_dbs()
    first = _seed_live_lineage(app_db, data_db, cols)
    for doc in app_db["trader_accounts"].docs:
        doc["synced_at"] = 0

    result = generate_manual_rebalance_plan(
        anchor_plan_id=first["plan"]["plan_id"],
        base_date="20250106",
        targets={"000001": 200},
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )

    plan = result["plan"]
    assert plan["status"] == "generated"
    assert plan["executable"] is False
    assert "account_not_synced" in str(plan["non_executable_reason"])
    assert plan["baseline_stale"] is True
    assert plan["summary"]["baseline_stale"] is True


def test_generate_trade_plan_fixed_notional_sizes_against_initial_capital():
    app_db, data_db, cols = _setup_dbs()
    _seed_live_lineage(app_db, data_db, cols)

    # Explicit fixed_notional overrides the live default and sizes on initial_capital.
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        params_override={"capital_basis": "fixed_notional", "initial_capital": 500_000},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    summary = result["plan"]["summary"]
    assert result["plan"]["capital_basis"] == "fixed_notional"
    assert summary["equity"] == 500_000
    assert summary["cash"] == 500_000


def test_generate_trade_plan_defaults_to_account_equity():
    app_db, data_db, cols = _setup_dbs()

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )

    assert result["plan"]["capital_basis"] == "account_equity"
    assert result["plan"]["summary"]["baseline_source"] in {"initial_capital", "live_lineage_reconstruction"}


def test_compute_next_rebalance_date_counts_trading_days(monkeypatch):
    # Force the mongo fallback so the test never reaches the tushare network path.
    monkeypatch.setattr("portfolio.calendar._tushare_pro", lambda: None)
    data_db = _Db()
    for date in [
        "20250101", "20250102", "20250103", "20250106", "20250107",
        "20250108", "20250109", "20250110", "20250113", "20250114",
    ]:
        data_db["volume_price"].docs.append({"symbol": "000001", "trade_date": date})

    # 3 trading days after the anchor (index 0 = anchor) -> 4th date in the list.
    assert compute_next_rebalance_date(data_db, "20250101", 3) == "20250106"
    assert compute_next_rebalance_date(data_db, "20250106", 2) == "20250108"
    # Calendar does not reach far enough -> None.
    assert compute_next_rebalance_date(data_db, "20250113", 10) is None
    # Guards.
    assert compute_next_rebalance_date(data_db, None, 3) is None
    assert compute_next_rebalance_date(data_db, "20250101", 0) is None


def test_generate_trade_plan_records_next_rebalance_schedule(monkeypatch):
    monkeypatch.setattr("portfolio.calendar._tushare_pro", lambda: None)
    app_db, data_db, cols = _setup_dbs()
    # Extend the local calendar so the fallback can resolve the next rebalance.
    for date in ["20250108", "20250109", "20250110", "20250113", "20250114"]:
        data_db["volume_price"].docs.append({"symbol": "000001", "trade_date": date})

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        params_override={"rebalance_days": 3},
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
    )
    plan = result["plan"]
    assert plan["rebalance_days"] == 3
    assert plan["rebalance_unit"] == "trading_days"
    assert plan["elapsed_trading_days_since_rebalance"] == 0
    # First rebalance counts from base_date: 3 trading days after 20250106.
    assert plan["next_rebalance_date"] == "20250109"
    assert plan["summary"]["next_rebalance_date"] == "20250109"
    assert plan["summary"]["rebalance_unit"] == "trading_days"


def test_drop_indexes_by_key_keeps_partial_replacement_index():
    key_pattern = [
        ("user_id", portfolio_trading.ASCENDING),
        ("strategy_template_id", portfolio_trading.ASCENDING),
        ("params_hash", portfolio_trading.ASCENDING),
        ("base_date", portfolio_trading.ASCENDING),
    ]

    class _IndexedCollection:
        def __init__(self):
            self.dropped = []

        def index_information(self):
            return {
                "_id_": {"key": [("_id", portfolio_trading.ASCENDING)]},
                "legacy_unique": {"key": key_pattern, "unique": True},
                "strategy_partial_unique": {
                    "key": key_pattern,
                    "unique": True,
                    "partialFilterExpression": {"origin": "strategy"},
                },
            }

        def drop_index(self, name):
            self.dropped.append(name)

    coll = _IndexedCollection()

    portfolio_db._drop_indexes_by_key(coll, key_pattern, unique_only=True)

    assert coll.dropped == ["legacy_unique"]


def test_normalize_manual_target_shares_rounds_partial_sell_and_allows_clear():
    assert normalize_manual_target_shares(400, 200, 100) == (200, [])
    assert normalize_manual_target_shares(400, 250, 100) == (300, [])
    assert normalize_manual_target_shares(400, 0, 100) == (0, [])
    assert normalize_manual_target_shares(400, 500, 100) == (500, [])
    assert normalize_manual_target_shares(400, 450, 100) == (400, ["below_lot_size"])


def test_generate_manual_rebalance_plan_partial_sell_and_full_liquidation():
    app_db, data_db, cols = _setup_dbs()
    for idx in range(1, 15):
        data_db["volume_price"].docs.append({"symbol": f"000{idx:03d}", "trade_date": "20250108", "open": 10 + idx, "close": 10 + idx})
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, first["plan"]["plan_id"])
    apply_result = apply_paper_trade_plan(plan_id=first["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    held_symbol = next(
        row["symbol"]
        for row in (apply_result.get("position_snapshot", {}).get("positions") or [])
        if int(row.get("shares", 0) or 0) > 0
    )

    partial = generate_manual_rebalance_plan(
        anchor_plan_id=first["plan"]["plan_id"],
        base_date="20250107",
        targets={held_symbol: 0},
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    plan = partial["plan"]
    assert plan["plan_type"] == "rebalance"
    assert plan["origin"] == "manual"
    assert plan["executable"] is True
    assert any(item.get("action") == "sell" and item.get("delta_shares", 0) < 0 for item in partial["items"])
    assert is_lineage_paused(cols, plan["strategy_template_id"], plan["params_hash"], "user-1") is False

    full_targets = {item["symbol"]: 0 for item in partial["items"] if int(item.get("current_shares", 0) or 0) > 0}
    full = generate_manual_rebalance_plan(
        anchor_plan_id=first["plan"]["plan_id"],
        base_date="20250107",
        targets=full_targets,
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, full["plan"]["plan_id"])
    apply_paper_trade_plan(plan_id=full["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    snapshot = cols.paper_positions.find_one(
        {"plan_id": full["plan"]["plan_id"]},
        sort=[("as_of_date", -1)],
    )
    apply_manual_plan_lineage_effects(cols, full["plan"], final_positions=snapshot.get("positions", []), acted_by="user-1")
    assert is_lineage_paused(cols, full["plan"]["strategy_template_id"], full["plan"]["params_hash"], "user-1") is True
    resume_lineage(cols, full["plan"]["strategy_template_id"], full["plan"]["params_hash"], "user-1")
    assert is_lineage_paused(cols, full["plan"]["strategy_template_id"], full["plan"]["params_hash"], "user-1") is False


def test_generate_manual_rebalance_credits_sell_before_buy_affordability():
    # Low cash baseline holding a sellable position: a buy that is only
    # affordable after the sell must NOT be flagged insufficient_cash. The
    # feasibility pass credits sell proceeds before checking buys.
    app_db, data_db, cols = _setup_dbs()
    anchor = {
        "plan_id": "anchor-plan",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "params_hash": "hash-1",
        "user_id": "user-1",
        "base_date": "20250106",
        "execute_date": "20250107",
        "status": "executed_paper",
        "params_snapshot": {"initial_capital": 1_000_000, "lot_size": 100},
    }
    cols.trade_plans.insert_one(anchor)
    cols.paper_positions.insert_one(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "hash-1",
            "user_id": "user-1",
            "as_of_date": "20250106",
            "plan_id": "anchor-plan",
            "cash": 1_000,  # far below the 000002 buy cost on its own
            "equity": 11_000,
            "positions": [{"symbol": "000001", "name": "Sell Me", "shares": 1000, "cost_basis": 10.0}],
        }
    )
    data_db["volume_price"].docs.extend(
        [
            {"symbol": "000001", "trade_date": "20250107", "open": 10.0, "close": 10.0},
            {"symbol": "000002", "trade_date": "20250107", "open": 20.0, "close": 20.0},
        ]
    )

    manual = generate_manual_rebalance_plan(
        anchor_plan_id="anchor-plan",
        base_date="20250107",
        targets={"000001": 0, "000002": 500},
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )

    buy_item = next(item for item in manual["items"] if item["symbol"] == "000002")
    assert buy_item["action"] == "buy"
    assert buy_item["delta_shares"] == 500
    assert "insufficient_cash" not in (buy_item.get("blockers") or [])
    assert manual["plan"]["executable"] is True


def test_generate_trade_plan_tags_strategy_origin():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    assert result["plan"]["origin"] == "strategy"


def test_apply_manual_plan_lineage_effects_pause_on_empty_false_skips_pause():
    app_db, data_db, cols = _setup_dbs()
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, first["plan"]["plan_id"])
    apply_result = apply_paper_trade_plan(plan_id=first["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    symbol = next(
        row["symbol"]
        for row in (apply_result.get("position_snapshot", {}).get("positions") or [])
        if int(row.get("shares", 0) or 0) > 0
    )
    manual = generate_manual_rebalance_plan(
        anchor_plan_id=first["plan"]["plan_id"],
        base_date="20250107",
        targets={symbol: 0},
        user_id="user-1",
        exclude_after=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    # Simulate "live signals published but not yet filled": positions still held,
    # pause must be skipped while the (forward-looking) exclusion still applies.
    effects = apply_manual_plan_lineage_effects(
        cols,
        manual["plan"],
        final_positions=[{"symbol": symbol, "shares": 0}],
        acted_by="user-1",
        pause_on_empty=False,
    )
    assert "paused" not in effects
    assert is_lineage_paused(cols, manual["plan"]["strategy_template_id"], manual["plan"]["params_hash"], "user-1") is False
    assert symbol in get_lineage_excluded_symbols(
        cols, manual["plan"]["strategy_template_id"], manual["plan"]["params_hash"], "user-1"
    )


def test_manual_exclude_after_injects_into_strategy_generation():
    app_db, data_db, cols = _setup_dbs()
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, first["plan"]["plan_id"])
    apply_result = apply_paper_trade_plan(plan_id=first["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    symbol = next(
        row["symbol"]
        for row in (apply_result.get("position_snapshot", {}).get("positions") or [])
        if int(row.get("shares", 0) or 0) > 0
    )
    manual = generate_manual_rebalance_plan(
        anchor_plan_id=first["plan"]["plan_id"],
        base_date="20250107",
        targets={symbol: 0},
        user_id="user-1",
        exclude_after=True,
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    apply_manual_plan_lineage_effects(cols, manual["plan"], acted_by="user-1")
    excluded = get_lineage_excluded_symbols(
        cols,
        manual["plan"]["strategy_template_id"],
        manual["plan"]["params_hash"],
        "user-1",
    )
    assert symbol in excluded


def test_process_manual_rebalance_task_generates_plan():
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    first = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, first["plan"]["plan_id"])
    apply_result = apply_paper_trade_plan(plan_id=first["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    held_symbol = next(
        row["symbol"]
        for row in (apply_result.get("position_snapshot", {}).get("positions") or [])
        if int(row.get("shares", 0) or 0) > 0
    )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "manual-1",
            "status": "pending",
            "task_type": "generate_manual_rebalance_plan",
            "user_id": "user-1",
            "anchor_plan_id": first["plan"]["plan_id"],
            "base_date": "20250107",
            "targets": {held_symbol: 0},
            "exclude_after": False,
            "attempts": 0,
            "created_at": "20250107",
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert result["status"] == "completed"
    task = cols.plan_generation_tasks.find_one({"task_id": "manual-1"})
    assert task["status"] == "completed"
    assert task["plan_id"] == result["plan_id"]
    assert task["result"]["plan_id"] == result["plan_id"]
    plan = cols.trade_plans.find_one({"plan_id": result["plan_id"]})
    assert plan["origin"] == "manual"
    assert plan["plan_type"] == "rebalance"


def test_process_holdings_risk_task_returns_review():
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": "holdings-1",
            "status": "pending",
            "task_type": "holdings_risk_review",
            "user_id": "user-1",
            "plan_id": "plan-x",
            "base_date": "20250106",
            "positions": [{"symbol": "000001", "shares": 100, "name": "Stock 1"}],
            "attempts": 0,
            "created_at": "20250106",
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    assert result["status"] == "completed"
    task = cols.plan_generation_tasks.find_one({"task_id": "holdings-1"})
    assert task["status"] == "completed"
    assert task["result"]["status"] == "done"
    assert task["result"]["reviewed_count"] == 1
    assert task["result"]["holdings"][0]["symbol"] == "000001"


def test_find_active_plan_subscriptions_skips_paused_lineage():
    app_db, data_db, cols = _setup_dbs()
    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        run_risk_review=False,
    )
    approve_plan(cols, result["plan"]["plan_id"])
    apply_paper_trade_plan(plan_id=result["plan"]["plan_id"], trading_collections=cols, data_db=data_db, force=True)
    pause_lineage(cols, result["plan"]["strategy_template_id"], result["plan"]["params_hash"], "user-1")
    subs = find_active_plan_subscriptions(cols, as_of_date="20250107")
    assert not any(
        row.get("params_hash") == result["plan"]["params_hash"] and row.get("user_id") == "user-1"
        for row in subs
    )


def _auto_task(cols, app_db, *, task_id, params_hash, params_override=None):
    """Queue one auto-scheduled generation task the way the scheduler does."""
    app_db["scoring_runs"].docs.append(
        {
            "score_date": "20250106",
            "universe_index": "csi1000",
            "status": "completed",
            "updated_at": "20250106",
        }
    )
    cols.plan_generation_tasks.docs.append(
        {
            "task_id": task_id,
            "status": "pending",
            "task_type": "generate_portfolio_plan",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": params_hash,
            "preset_id": None,
            "base_date": "20250106",
            "mode": "rebalance",
            "user_id": "user-1",
            "force": False,
            "params_override": params_override if params_override is not None else {},
            "source": "auto_scheduler",
            "auto": True,
            "attempts": 0,
            "created_at": "20250106",
        }
    )


def test_process_auto_task_keeps_the_plan_on_its_named_lineage():
    """Positive control: when the resolved params reproduce the task's lineage the
    auto path generates normally, so the invariant never costs a healthy lineage a
    rebalance."""
    from tools.portfolio_trading import (
        effective_strategy_config,
        load_default_param_preset,
        load_strategy_config,
        plan_params_hash,
    )
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    template = load_strategy_config(cols, "growth_cycle_index_enhanced")
    preset = load_default_param_preset(cols, "growth_cycle_index_enhanced")
    lineage = plan_params_hash(effective_strategy_config(template, preset, None))
    _auto_task(cols, app_db, task_id="auto-ok", params_hash=lineage)

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)

    assert result["status"] == "completed"
    plan = cols.trade_plans.find_one({"plan_id": result["plan_id"]})
    assert plan["params_hash"] == lineage


def test_process_auto_task_refuses_to_write_under_a_different_lineage():
    """Regression for the lineage drift that swallowed 22 auto tasks.

    Auto tasks carry a null preset_id, so the *fallback* preset picks the hash: a
    task naming lineage A lands under lineage B and the book it was meant to
    maintain is left unmonitored with no error anywhere. The task must fail loudly
    instead.
    """
    from tools.portfolio_trading import (
        effective_strategy_config,
        load_default_param_preset,
        load_strategy_config,
        plan_params_hash,
    )
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    template = load_strategy_config(cols, "growth_cycle_index_enhanced")
    # The default preset is what a null preset_id resolves to -- lineage B.
    cols.param_presets.insert_one(
        {
            "preset_id": "csi1000-default",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "user_id": None,
            "status": "enabled",
            "is_default": True,
            "priority": 1,
            "params": {"universe_index": "csi1000", "top_n": 10, "rebalance_days": 10},
        }
    )
    # A named, non-default preset: lineage A, the one the task is supposed to maintain.
    named_preset = {
        "preset_id": "csi1000-alt",
        "strategy_template_id": "growth_cycle_index_enhanced",
        "user_id": None,
        "status": "enabled",
        "is_default": False,
        "priority": 10,
        "params": {"universe_index": "csi1000", "top_n": 12, "rebalance_days": 7},
    }
    cols.param_presets.insert_one(named_preset)
    named_lineage = plan_params_hash(effective_strategy_config(template, named_preset, None))
    fallback_lineage = plan_params_hash(
        effective_strategy_config(
            template,
            load_default_param_preset(cols, "growth_cycle_index_enhanced"),
            None,
        )
    )
    assert named_lineage != fallback_lineage

    _auto_task(cols, app_db, task_id="auto-drift", params_hash=named_lineage)

    result = process_one(cols, "worker-1", app_db=app_db, data_db=data_db)
    task = cols.plan_generation_tasks.find_one({"task_id": "auto-drift"})

    assert result["status"] == "failed"
    assert "does not match expected lineage" in result["error_message"]
    assert task["status"] == "failed"
    assert task.get("plan_id") is None
    # Refusing means refusing to write: not under the named lineage, and not under
    # the fallback preset's lineage either.
    assert cols.trade_plans.count_documents({}) == 0


def test_generate_trade_plan_preview_is_read_only():
    app_db, data_db, cols = _setup_dbs()
    before_plans = len(cols.trade_plans.docs)
    before_items = len(cols.trade_plan_items.docs)

    result = generate_trade_plan(
        strategy_template_id="growth_cycle_index_enhanced",
        base_date="20250106",
        mode="rebalance",
        user_id="user-1",
        app_db=app_db,
        data_db=data_db,
        trading_collections=cols,
        write=False,
        run_risk_review=False,
    )

    assert result["plan"]["plan_id"]
    assert isinstance(result["items"], list)
    assert len(cols.trade_plans.docs) == before_plans
    assert len(cols.trade_plan_items.docs) == before_items


# --- research/live parity and activation scheduling -------------------------

from portfolio.parity import build_research_live_parity, suggest_effective_date
from portfolio.scheduler import apply_due_activations


def _parity_config(**overrides):
    config = {
        "universe_index": "csi1000",
        "score_type": "growth_cycle_weighted",
        "growth_weight": 0.3,
        "cycle_weight": 0.7,
        "top_n": 10,
        "construction_mode": "top_n",
        "lot_size": 100,
        "candidate_pool_multiplier": 3,
        "min_buy_lots": 1,
        "cash_buffer": 0.03,
        "regime_cash": False,
    }
    config.update(overrides)
    return config


def _parity_periods(symbols):
    return (
        [{"score_date": "20250106", "regime_action": "invest", "selected_count": len(symbols)}],
        [{"score_date": "20250106", "symbol": symbol} for symbol in symbols],
    )


def test_build_research_live_parity_treats_live_backfill_as_explained():
    app_db, data_db, cols = _setup_dbs()
    research_symbols = [f"0000{idx:02d}" for idx in range(1, 9)]
    periods, trades = _parity_periods(research_symbols)

    report = build_research_live_parity(
        collections=cols,
        app_db=app_db,
        data_db=data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        config=_parity_config(),
        user_id="user-1",
        base_date="20250106",
        research_periods=periods,
        research_trades=trades,
        target_amount=1_000_000_000.0,
    )

    assert report["status"] == "clean"
    assert report["backfilled_dates"] == 1
    assert report["unexplained_dates"] == 0
    row = report["dates"][0]
    assert row["status"] == "live_backfill"
    assert row["research_only"] == []
    assert row["live_only"] == ["000009", "000010"]


def test_build_research_live_parity_flags_research_only_symbol_as_unexplained():
    app_db, data_db, cols = _setup_dbs()
    # 000014 is in the live score frame but outside the live top-10.
    research_symbols = [f"0000{idx:02d}" for idx in range(1, 10)] + ["000014"]
    periods, trades = _parity_periods(research_symbols)

    report = build_research_live_parity(
        collections=cols,
        app_db=app_db,
        data_db=data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        config=_parity_config(),
        user_id="user-1",
        base_date="20250106",
        research_periods=periods,
        research_trades=trades,
        target_amount=1_000_000_000.0,
    )

    assert report["status"] == "diff"
    assert report["unexplained_dates"] == 1
    assert report["unexplained"][0]["research_only"] == ["000014"]


def test_apply_due_activations_pauses_replaced_lineage_and_enqueues_rebalance():
    app_db, data_db, cols = _setup_dbs()
    cols.strategy_activations.insert_one(
        {
            "activation_id": "act-1",
            "user_id": "user-1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "preset_id": "research_preset",
            "params_hash": "newhash",
            "params_override": {"top_n": 10},
            "execution_venue": "paper",
            "effective_date": "20250106",
            "status": "scheduled",
            "replaces": [
                {
                    "strategy_template_id": "growth_cycle_index_enhanced",
                    "params_hash": "oldhash",
                }
            ],
            "requested_by": "user-1",
        }
    )

    result = apply_due_activations(cols, "20250106")

    assert result["due"] == 1
    assert result["applied"][0]["paused"] == [
        {"strategy_template_id": "growth_cycle_index_enhanced", "params_hash": "oldhash"}
    ]
    activation = cols.strategy_activations.find_one({"activation_id": "act-1"})
    assert activation["status"] == "active"
    task = cols.plan_generation_tasks.find_one({"task_id": activation["first_task_id"]})
    assert task["source"] == "strategy_activation"
    assert task["mode"] == "rebalance"
    assert task["base_date"] == "20250106"
    assert task["params_hash"] == "newhash"
    assert task["params_override"]["capital_basis"] == "rolling_paper"
    new_lineage = cols.lineage_controls.find_one(
        {
            "strategy_template_id": "growth_cycle_index_enhanced",
            "params_hash": "newhash",
            "user_id": "user-1",
        }
    )
    assert new_lineage["execution_venue"] == "paper"
    assert is_lineage_paused(
        cols, "growth_cycle_index_enhanced", "oldhash", "user-1"
    )

    # The activation is consumed, so the next scheduler run must not duplicate it.
    second = apply_due_activations(cols, "20250106")
    assert second["due"] == 0
    assert cols.plan_generation_tasks.count_documents({"source": "strategy_activation"}) == 1


def test_suggest_effective_date_falls_back_when_no_live_lineage():
    app_db, data_db, cols = _setup_dbs()

    assert (
        suggest_effective_date(
            collections=cols,
            data_db=data_db,
            user_id="user-1",
            base_date="20250106",
            fallback="20250120",
        )
        == "20250120"
    )


def test_build_research_live_parity_explains_unbuyable_research_holdings():
    app_db, data_db, cols = _setup_dbs()
    research_symbols = [f"0000{idx:02d}" for idx in range(1, 11)]
    periods, trades = _parity_periods(research_symbols)

    report = build_research_live_parity(
        collections=cols,
        app_db=app_db,
        data_db=data_db,
        strategy_template_id="growth_cycle_index_enhanced",
        config=_parity_config(),
        user_id="user-1",
        base_date="20250106",
        research_periods=periods,
        research_trades=trades,
        # Prices are 11..20; one lot above 1,500 is unbuyable.
        target_amount=1_500.0,
    )

    row = report["dates"][0]
    assert row["status"] == "lot_size_backfill"
    assert row["research_only_unbuyable"] == [
        "000006",
        "000007",
        "000008",
        "000009",
        "000010",
    ]
    assert row["live_only_high_rank"] == []
    assert report["status"] == "clean"
    assert report["backfilled_dates"] == 1


def test_preview_task_resolves_data_db_when_worker_passes_none(monkeypatch):
    """The loop calls process_one without data_db; preview parity must still run."""
    from tools.workers.process_portfolio_plan_generation_tasks import process_one

    app_db, data_db, cols = _setup_dbs()
    monkeypatch.setattr(portfolio_db, "get_data_db", lambda: data_db)
    periods, trades = _parity_periods([f"0000{idx:02d}" for idx in range(1, 9)])
    cols.plan_generation_tasks.insert_one(
        {
            "task_id": "preview-1",
            "task_type": "preview_portfolio_plan",
            "user_id": "user-1",
            "strategy_template_id": "growth_cycle_index_enhanced",
            "base_date": "20250106",
            "mode": "auto",
            "force": False,
            "preset_id": None,
            "params_override": _parity_config(),
            "research_periods": periods,
            "research_trades": trades,
            "source": "research_live_preview",
            "auto": False,
            "status": "pending",
            "attempts": 0,
            "plan_id": None,
            "error_message": None,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow(),
        }
    )

    result = process_one(cols, "worker-1", app_db=app_db, data_db=None)

    task = cols.plan_generation_tasks.find_one({"task_id": "preview-1"})
    assert result["status"] == "completed"
    assert task["status"] == "completed"
    parity = task["result"]["parity"]
    assert parity["status"] == "clean"
    assert parity["unexplained_dates"] == 0
