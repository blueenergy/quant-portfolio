"""Record-kind and strategy predicates for portfolio_trade_plans queries.

Canonical taxonomy lives in quantFinance ``services/portfolio_record_kind.py``.
This module mirrors its classification behavior because the worker is a
separate repository but reads the same mixed collection.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping

from portfolio.constants import RECORD_KIND_OBSERVATION, RECORD_KIND_TRADE_PLAN

RECORD_KIND_LEDGER_EVENT = "ledger_event"
PLAN_TYPE_REBALANCE = "rebalance"
PLAN_TYPE_INTRADAY_T0 = "intraday_t0"
TRADE_PLAN_PLAN_TYPES = frozenset({PLAN_TYPE_REBALANCE, PLAN_TYPE_INTRADAY_T0})
VALID_RECORD_KINDS = frozenset(
    {
        RECORD_KIND_TRADE_PLAN,
        RECORD_KIND_OBSERVATION,
        RECORD_KIND_LEDGER_EVENT,
    }
)
LEDGER_MANUAL_ACTIONS = frozenset({"external_manual", "external_liquidation"})


def classify_record(doc: Mapping[str, Any]) -> str:
    """Derive ``record_kind`` using the canonical quantFinance precedence."""
    manual_action = doc.get("manual_action")
    if manual_action in LEDGER_MANUAL_ACTIONS:
        return RECORD_KIND_LEDGER_EVENT
    if "_extman_" in str(doc.get("plan_id") or ""):
        return RECORD_KIND_LEDGER_EVENT

    plan_type = doc.get("plan_type")
    if plan_type == "monitor":
        return RECORD_KIND_OBSERVATION
    if plan_type in TRADE_PLAN_PLAN_TYPES:
        return RECORD_KIND_TRADE_PLAN
    if plan_type == "manual":
        return RECORD_KIND_LEDGER_EVENT
    raise ValueError(
        "cannot classify record_kind: "
        f"plan_id={doc.get('plan_id')!r} plan_type={plan_type!r} "
        f"manual_action={manual_action!r}"
    )


def require_record_kind(doc: Mapping[str, Any]) -> str:
    """Return a valid stamped kind, preserving ledger-event overrides."""
    try:
        classified = classify_record(doc)
    except ValueError:
        classified = None
    if classified == RECORD_KIND_LEDGER_EVENT:
        return RECORD_KIND_LEDGER_EVENT

    kind = doc.get("record_kind")
    if kind not in VALID_RECORD_KINDS:
        raise ValueError(
            "missing/invalid record_kind: "
            f"plan_id={doc.get('plan_id')!r} record_kind={kind!r}"
        )
    return str(kind)


def is_strategy_decision(doc: Mapping[str, Any]) -> bool:
    """True for strategy-generated trade plans (excludes manual discretionary)."""
    if str(doc.get("origin") or "") == "manual":
        return False
    return require_record_kind(doc) == RECORD_KIND_TRADE_PLAN


def strategy_generated_record_mongo_filter(**extra: Any) -> Dict[str, Any]:
    """Mongo query fragment for any non-manual record, including observations."""
    query: Dict[str, Any] = {"origin": {"$ne": "manual"}}
    query.update(extra)
    return query


def strategy_decision_mongo_filter(**extra: Any) -> Dict[str, Any]:
    """Mongo query fragment for strategy-generated trade plans."""
    query: Dict[str, Any] = {
        "origin": {"$ne": "manual"},
        "$or": [
            {"record_kind": RECORD_KIND_TRADE_PLAN},
            {"record_kind": {"$exists": False}},
        ],
    }
    query.update(extra)
    return query


def strategy_rebalance_mongo_filter(**extra: Any) -> Dict[str, Any]:
    """``strategy_decision_mongo_filter`` narrowed to cadence rebalance rows."""
    return strategy_decision_mongo_filter(plan_type=PLAN_TYPE_REBALANCE, **extra)
