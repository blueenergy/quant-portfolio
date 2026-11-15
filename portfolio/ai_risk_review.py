#!/usr/bin/env python3
"""Rule-based AI risk-control pass for portfolio trade plans (Layer 1).

This module runs *after* stock selection and annotates each actionable plan
item with an ``ai_risk`` structure describing event-style risks that the
factor model does not capture (recent excessive gains, share-unlock pressure,
insider/holder reductions, ...).

Design goals for the first version:

* **Deterministic & cheap** — reads only data already in ``quant_data``; no LLM
  and no external network calls. The plan worker stays fast and reliable.
* **Advisory only** — findings never mutate the plan automatically; they are
  surfaced to the user who decides whether to "换一只" (reuses the existing
  reject/re-select flow).
* **Graceful degradation** — every check returns no finding (instead of
  raising) when its data source is missing, so partial data never breaks plan
  generation. Signals that lack data today (e.g. 股东减持) light up
  automatically once the corresponding collection is populated.

The LLM-based layer (公告/舆情/监管事件) is intentionally out of scope here and
belongs in ``quantAnalyzer`` as a separate async task type.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional

from pymongo import ASCENDING, DESCENDING

from shared.volatility import compute_volatility_features_from_rows, fetch_volume_price_rows

ENGINE_VERSION = "rules-v2"

SEVERITY_ORDER = {"none": 0, "low": 1, "medium": 2, "high": 3}

# Recent-surge thresholds (cumulative close-to-close return over the window, %).
SURGE_RET_20D_MEDIUM = 30.0
SURGE_RET_20D_HIGH = 50.0
SURGE_RET_60D_MEDIUM = 50.0
SURGE_RET_60D_HIGH = 80.0

# Share-unlock look-ahead window (calendar days) and recency cutoff for "high".
UNLOCK_WINDOW_DAYS = 90
UNLOCK_IMMINENT_DAYS = 30
# Ratio (% of float/total) thresholds when share_float carries a ratio column.
UNLOCK_RATIO_MEDIUM = 2.0
UNLOCK_RATIO_HIGH = 5.0

# Optional collection that may hold 股东增减持 (tushare stk_holdertrade) rows.
# Empty/missing -> the holder-reduction check is a no-op.
HOLDER_TRADE_COLLECTION = "stk_holdertrade"
HOLDER_REDUCE_WINDOW_DAYS = 60

# Volatility thresholds (annualized realized vol and ATR as fraction of close).
VOL_ANNUAL_MEDIUM = 0.60
VOL_ANNUAL_HIGH = 0.80
ATR_PCT_MEDIUM = 0.08
ATR_PCT_HIGH = 0.10
VOL_PCTL_MEDIUM = 75.0
VOL_PCTL_HIGH = 90.0
VOL_LOOKBACK_DAYS = 320

# Entry-risk thresholds for "high + distribution" diagnostics.
ENTRY_HIGH_WINDOW = 120
ENTRY_FLOW_WINDOW = 20
ENTRY_MIN_HIGH_SAMPLES = 60
ENTRY_MIN_FLOW_SAMPLES = 10
ENTRY_HIPOS_LOW = 0.90
ENTRY_HIPOS_MEDIUM = 0.95
ENTRY_HIPOS_HIGH = 0.97
ENTRY_DISTRIB_LOW = 0.005
ENTRY_DISTRIB_MEDIUM = 0.02
ENTRY_DISTRIB_HIGH = 0.05
ENTRY_MAINFLOW_HIGH_OUTFLOW = -0.05


@dataclass
class RiskFinding:
    code: str
    severity: str
    title: str
    detail: str
    metrics: Dict[str, Any] = field(default_factory=dict)


def _max_severity(severities: Iterable[str]) -> str:
    best = "none"
    for sev in severities:
        if SEVERITY_ORDER.get(sev, 0) > SEVERITY_ORDER.get(best, 0):
            best = sev
    return best


def _to_yyyymmdd(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y%m%d")
    text = str(value).replace("-", "").replace("/", "")[:8]
    return text if len(text) == 8 and text.isdigit() else None


def _parse_date(value: Any) -> Optional[datetime]:
    text = _to_yyyymmdd(value)
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y%m%d")
    except ValueError:
        return None


def _safe_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:  # NaN
        return None
    return out


def _recent_rows_for_symbol(
    data_db,
    collection_name: str,
    symbol: str,
    base_date: str,
    projection: Dict[str, int],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    """Fetch latest symbol rows, trying both common symbol fields."""
    try:
        collection = data_db[collection_name]
    except Exception:
        return []
    for field in ("symbol", "ts_code"):
        try:
            rows = list(
                collection.find(
                    {field: symbol, "trade_date": {"$lte": base_date}},
                    projection,
                )
                .sort("trade_date", DESCENDING)
                .limit(limit)
            )
        except Exception:
            rows = []
        if rows:
            return rows
    return []


def _price_amount_wan(row: Dict[str, Any]) -> Optional[float]:
    amount = _safe_float(row.get("amount"))
    if amount is None:
        volume = _safe_float(row.get("vol") if row.get("vol") is not None else row.get("volume"))
        close = _safe_float(row.get("close"))
        if volume is not None and close is not None:
            # Tushare daily vol is in hands; convert shares * price to 万元.
            return volume * 100 * close / 10000
    if amount is None or amount <= 0:
        return None
    # Tushare daily amount is 千元, while money_flow amounts are 万元.
    return amount / 10


def _main_net_from_money_flow(row: Dict[str, Any]) -> Optional[float]:
    buy_elg = _safe_float(row.get("buy_elg_amount"))
    buy_lg = _safe_float(row.get("buy_lg_amount"))
    sell_elg = _safe_float(row.get("sell_elg_amount"))
    sell_lg = _safe_float(row.get("sell_lg_amount"))
    if any(v is not None for v in (buy_elg, buy_lg, sell_elg, sell_lg)):
        return (buy_elg or 0.0) + (buy_lg or 0.0) - (sell_elg or 0.0) - (sell_lg or 0.0)
    return _safe_float(row.get("net_mf_amount"))


def _ret_from_closes(closes: List[float], lookback: int) -> Optional[float]:
    if len(closes) <= lookback:
        return None
    latest = closes[-1]
    past = closes[-1 - lookback]
    if latest <= 0 or past <= 0:
        return None
    return (latest / past - 1.0) * 100.0


def compute_entry_risk_metrics(data_db, symbol: str, base_date: str) -> Dict[str, Any]:
    """Compute point-in-time hipos/mainflow/distrib metrics for one symbol."""
    base = _to_yyyymmdd(base_date) or str(base_date)
    price_rows_desc = _recent_rows_for_symbol(
        data_db,
        "volume_price",
        symbol,
        base,
        {
            "_id": 0,
            "symbol": 1,
            "ts_code": 1,
            "trade_date": 1,
            "close": 1,
            "vol": 1,
            "volume": 1,
            "amount": 1,
        },
        limit=ENTRY_HIGH_WINDOW + ENTRY_FLOW_WINDOW + 20,
    )
    price_by_date: Dict[str, Dict[str, Any]] = {}
    for row in reversed(price_rows_desc):
        trade_date = _to_yyyymmdd(row.get("trade_date"))
        close = _safe_float(row.get("close"))
        if not trade_date or close is None or close <= 0:
            continue
        price_by_date[trade_date] = {**row, "trade_date": trade_date, "close": close}
    price_rows = list(price_by_date.values())
    closes = [_safe_float(row.get("close")) for row in price_rows]
    closes = [value for value in closes if value is not None and value > 0]
    latest = closes[-1] if closes else None
    if latest is None or len(closes) < ENTRY_MIN_HIGH_SAMPLES:
        return {
            "status": "insufficient_data",
            "sample_count": len(closes),
            "as_of": (price_rows[-1] if price_rows else {}).get("trade_date"),
        }

    high_window = closes[-ENTRY_HIGH_WINDOW:]
    trailing_high = max(high_window) if high_window else None
    hipos120 = latest / trailing_high if trailing_high and trailing_high > 0 else None

    flow_rows_desc = _recent_rows_for_symbol(
        data_db,
        "money_flow",
        symbol,
        base,
        {
            "_id": 0,
            "symbol": 1,
            "ts_code": 1,
            "trade_date": 1,
            "buy_elg_amount": 1,
            "sell_elg_amount": 1,
            "buy_lg_amount": 1,
            "sell_lg_amount": 1,
            "net_mf_amount": 1,
        },
        limit=ENTRY_FLOW_WINDOW * 3,
    )
    flow_by_date: Dict[str, float] = {}
    for row in flow_rows_desc:
        trade_date = _to_yyyymmdd(row.get("trade_date"))
        main_net = _main_net_from_money_flow(row)
        if trade_date and main_net is not None:
            flow_by_date[trade_date] = main_net

    recent_prices = price_rows[-ENTRY_FLOW_WINDOW:]
    main_values = [
        flow_by_date[str(row.get("trade_date"))]
        for row in recent_prices
        if str(row.get("trade_date")) in flow_by_date
    ]
    amounts = [_price_amount_wan(row) for row in recent_prices]
    amount_den = sum(value for value in amounts if value is not None and value > 0)
    mainflow20 = None
    if len(main_values) >= ENTRY_MIN_FLOW_SAMPLES and amount_den > 0:
        mainflow20 = sum(main_values) / amount_den
    distrib = hipos120 * (-mainflow20) if hipos120 is not None and mainflow20 is not None else None

    volumes = [
        _safe_float(row.get("vol") if row.get("vol") is not None else row.get("volume"))
        for row in price_rows
    ]
    volumes = [value for value in volumes if value is not None and value > 0]
    volratio = None
    if len(volumes) >= ENTRY_FLOW_WINDOW:
        slow = sum(volumes[-ENTRY_FLOW_WINDOW:]) / ENTRY_FLOW_WINDOW
        fast = sum(volumes[-5:]) / 5 if len(volumes) >= 5 else None
        if slow > 0 and fast is not None:
            volratio = fast / slow

    return {
        "status": "ok",
        "as_of": price_rows[-1].get("trade_date") if price_rows else None,
        "sample_count": len(closes),
        "flow_sample_count": len(main_values),
        "latest_close": latest,
        "trailing_high_120d": trailing_high,
        "hipos120": hipos120,
        "distance_to_120d_high_pct": (hipos120 - 1.0) * 100.0 if hipos120 is not None else None,
        "mainflow20": mainflow20,
        "main_net_20d": sum(main_values) if main_values else None,
        "amount_20d_wan": amount_den if amount_den > 0 else None,
        "volratio": volratio,
        "distrib": distrib,
        "ret_20d": _ret_from_closes(closes, 20),
        "ret_60d": _ret_from_closes(closes, 60),
    }


def classify_entry_distribution_risk(metrics: Dict[str, Any]) -> str:
    hipos = _safe_float(metrics.get("hipos120"))
    mainflow = _safe_float(metrics.get("mainflow20"))
    distrib = _safe_float(metrics.get("distrib"))
    if hipos is None or mainflow is None or distrib is None:
        return "none"
    if mainflow >= 0 or distrib <= 0:
        return "none"
    ret_20 = _safe_float(metrics.get("ret_20d"))
    ret_60 = _safe_float(metrics.get("ret_60d"))
    surged = (
        (ret_20 is not None and ret_20 >= SURGE_RET_20D_MEDIUM)
        or (ret_60 is not None and ret_60 >= SURGE_RET_60D_MEDIUM)
    )
    if (
        hipos >= ENTRY_HIPOS_HIGH
        and distrib >= ENTRY_DISTRIB_HIGH
        and (mainflow <= ENTRY_MAINFLOW_HIGH_OUTFLOW or surged)
    ):
        return "high"
    if hipos >= ENTRY_HIPOS_MEDIUM and distrib >= ENTRY_DISTRIB_MEDIUM:
        return "medium"
    if hipos >= ENTRY_HIPOS_LOW and distrib >= ENTRY_DISTRIB_LOW:
        return "low"
    return "none"


# --------------------------------------------------------------------------- #
# Individual checks. Each returns a (possibly empty) list of findings and never
# raises; data-access failures degrade to "no signal".
# --------------------------------------------------------------------------- #


def check_recent_surge(data_db, symbol: str, base_date: str) -> List[RiskFinding]:
    """近期涨幅过大: cumulative close return over the last 20/60 trading days."""
    try:
        rows = list(
            data_db["volume_price"]
            .find(
                {"symbol": symbol, "trade_date": {"$lte": base_date}},
                {"_id": 0, "trade_date": 1, "close": 1},
            )
            .sort("trade_date", DESCENDING)
            .limit(70)
        )
    except Exception:
        return []
    closes: List[float] = []
    for row in rows:  # rows are newest-first
        close = _safe_float(row.get("close"))
        if close and close > 0:
            closes.append(close)
    if len(closes) < 21:
        return []

    latest = closes[0]
    findings: List[RiskFinding] = []

    def _ret(lookback: int) -> Optional[float]:
        if len(closes) <= lookback:
            return None
        past = closes[lookback]
        if not past or past <= 0:
            return None
        return (latest / past - 1.0) * 100.0

    ret_20 = _ret(20)
    ret_60 = _ret(60)

    severity = "none"
    if ret_20 is not None and ret_20 >= SURGE_RET_20D_HIGH:
        severity = "high"
    elif ret_60 is not None and ret_60 >= SURGE_RET_60D_HIGH:
        severity = "high"
    elif ret_20 is not None and ret_20 >= SURGE_RET_20D_MEDIUM:
        severity = "medium"
    elif ret_60 is not None and ret_60 >= SURGE_RET_60D_MEDIUM:
        severity = "medium"

    if severity == "none":
        return []

    parts = []
    if ret_20 is not None:
        parts.append(f"近20日 {ret_20:+.1f}%")
    if ret_60 is not None:
        parts.append(f"近60日 {ret_60:+.1f}%")
    detail = "、".join(parts) + "，短期涨幅偏高，注意回撤/获利了结风险。"
    findings.append(
        RiskFinding(
            code="recent_surge",
            severity=severity,
            title="近期涨幅过大",
            detail=detail,
            metrics={"ret_20d": ret_20, "ret_60d": ret_60},
        )
    )
    return findings


def check_share_unlock(data_db, symbol: str, base_date: str) -> List[RiskFinding]:
    """大额解禁: limited shares unlocking within a forward window."""
    try:
        rows = list(
            data_db["share_float"].find(
                {"ts_code": symbol}, {"_id": 0}
            )
        )
    except Exception:
        return []
    if not rows:
        return []

    base = _parse_date(base_date)
    if base is None:
        return []
    window_end = base + timedelta(days=UNLOCK_WINDOW_DAYS)
    imminent_end = base + timedelta(days=UNLOCK_IMMINENT_DAYS)

    nearest: Optional[datetime] = None
    total_share = 0.0
    max_ratio: Optional[float] = None
    hit = False
    for row in rows:
        fdate = _parse_date(row.get("float_date") or row.get("floatdate"))
        if fdate is None or fdate <= base or fdate > window_end:
            continue
        hit = True
        if nearest is None or fdate < nearest:
            nearest = fdate
        share = _safe_float(row.get("float_share") or row.get("float_shares") or row.get("ft_share"))
        if share:
            total_share += share
        ratio = _safe_float(
            row.get("float_ratio")
            or row.get("float_ratio_to_total")
            or row.get("total_ratio")
            or row.get("holder_ratio")
        )
        if ratio is not None:
            max_ratio = ratio if max_ratio is None else max(max_ratio, ratio)
    if not hit:
        return []

    severity = "medium"
    if max_ratio is not None:
        if max_ratio >= UNLOCK_RATIO_HIGH:
            severity = "high"
        elif max_ratio >= UNLOCK_RATIO_MEDIUM:
            severity = "medium"
        else:
            severity = "low"
    if nearest is not None and nearest <= imminent_end:
        # Imminent unlock bumps severity up one notch (capped at high).
        severity = "high" if severity in ("medium", "high") else "medium"

    detail_parts = []
    if nearest is not None:
        detail_parts.append(f"最近解禁日 {nearest.strftime('%Y-%m-%d')}")
    if max_ratio is not None:
        detail_parts.append(f"占比约 {max_ratio:.2f}%")
    detail_parts.append(f"未来{UNLOCK_WINDOW_DAYS}天内有限售解禁，关注潜在抛压。")
    return [
        RiskFinding(
            code="share_unlock",
            severity=severity,
            title="近期限售解禁",
            detail="，".join(detail_parts),
            metrics={
                "nearest_float_date": nearest.strftime("%Y%m%d") if nearest else None,
                "total_float_share": total_share or None,
                "max_ratio": max_ratio,
            },
        )
    ]


def check_holder_reduction(data_db, symbol: str, base_date: str) -> List[RiskFinding]:
    """股东/高管减持: data-driven, no-op until a holder-trade collection exists."""
    base = _parse_date(base_date)
    if base is None:
        return []
    window_start = (base - timedelta(days=HOLDER_REDUCE_WINDOW_DAYS)).strftime("%Y%m%d")
    try:
        if HOLDER_TRADE_COLLECTION not in data_db.list_collection_names():
            return []
        rows = list(
            data_db[HOLDER_TRADE_COLLECTION].find(
                {
                    "ts_code": symbol,
                    "ann_date": {"$gte": window_start, "$lte": base_date},
                },
                {"_id": 0},
            )
        )
    except Exception:
        return []
    if not rows:
        return []

    # tushare stk_holdertrade: in_de == 'DE' marks a reduction (减持).
    reductions = [r for r in rows if str(r.get("in_de") or r.get("type") or "").upper().startswith("DE")]
    if not reductions:
        return []
    total_ratio = 0.0
    for r in reductions:
        ratio = _safe_float(r.get("change_ratio") or r.get("ratio"))
        if ratio:
            total_ratio += abs(ratio)
    severity = "high" if total_ratio >= 3.0 else "medium" if total_ratio >= 1.0 else "low"
    detail = f"近{HOLDER_REDUCE_WINDOW_DAYS}天披露减持{len(reductions)}条"
    if total_ratio > 0:
        detail += f"，合计约 {total_ratio:.2f}%"
    detail += "，注意股东/高管减持信号。"
    return [
        RiskFinding(
            code="holder_reduction",
            severity=severity,
            title="股东/高管减持",
            detail=detail,
            metrics={"count": len(reductions), "total_ratio": total_ratio or None},
        )
    ]


def check_high_volatility(data_db, symbol: str, base_date: str) -> List[RiskFinding]:
    """高波动: realized vol / ATR% / historical vol percentile."""
    try:
        rows = fetch_volume_price_rows(data_db, symbol, base_date, lookback_days=VOL_LOOKBACK_DAYS)
        features = compute_volatility_features_from_rows(rows)
    except Exception:
        return []

    vol_20 = _safe_float(features.get("vol_20d_annual"))
    vol_60 = _safe_float(features.get("vol_60d_annual"))
    atr_pct_20 = _safe_float(features.get("atr_pct_20d"))
    vol_pctl = _safe_float(features.get("vol_percentile") or features.get("vol_percentile_250d"))
    abnormal = bool(features.get("abnormal_range_flag"))

    if vol_20 is None and atr_pct_20 is None and vol_pctl is None:
        return []

    severity = "none"
    if (
        (vol_20 is not None and vol_20 >= VOL_ANNUAL_HIGH)
        or (atr_pct_20 is not None and atr_pct_20 >= ATR_PCT_HIGH)
        or (vol_pctl is not None and vol_pctl >= VOL_PCTL_HIGH)
        or abnormal
    ):
        severity = "high"
    elif (
        (vol_20 is not None and vol_20 >= VOL_ANNUAL_MEDIUM)
        or (atr_pct_20 is not None and atr_pct_20 >= ATR_PCT_MEDIUM)
        or (vol_pctl is not None and vol_pctl >= VOL_PCTL_MEDIUM)
    ):
        severity = "medium"

    if severity == "none":
        return []

    parts = []
    if vol_20 is not None:
        parts.append(f"20日年化波动 {vol_20 * 100:.1f}%")
    if vol_60 is not None:
        parts.append(f"60日年化波动 {vol_60 * 100:.1f}%")
    if atr_pct_20 is not None:
        parts.append(f"ATR/收盘 {atr_pct_20 * 100:.1f}%")
    if vol_pctl is not None:
        parts.append(f"250日分位 {vol_pctl:.0f}%")
    if abnormal:
        parts.append("当日振幅超2倍ATR")
    detail = "、".join(parts) + "，波动偏高，注意仓位与止损。"

    return [
        RiskFinding(
            code="high_volatility",
            severity=severity,
            title="波动率偏高",
            detail=detail,
            metrics={
                "vol_20d_annual": vol_20,
                "vol_60d_annual": vol_60,
                "atr_pct_20d": atr_pct_20,
                "vol_percentile": vol_pctl,
                "vol_percentile_250d": vol_pctl,
                "abnormal_range_flag": abnormal,
            },
        )
    ]


def check_distribution_at_highs(data_db, symbol: str, base_date: str) -> List[RiskFinding]:
    """高位派发: near 120d highs while 20d main-flow is net negative."""
    metrics = compute_entry_risk_metrics(data_db, symbol, base_date)
    severity = classify_entry_distribution_risk(metrics)
    if severity == "none":
        return []

    hipos = _safe_float(metrics.get("hipos120"))
    mainflow = _safe_float(metrics.get("mainflow20"))
    distrib = _safe_float(metrics.get("distrib"))
    ret_20 = _safe_float(metrics.get("ret_20d"))
    ret_60 = _safe_float(metrics.get("ret_60d"))

    parts = []
    if hipos is not None:
        parts.append(f"hipos120 {hipos:.3f}")
    if mainflow is not None:
        parts.append(f"20日主力净流/成交额 {mainflow * 100:+.1f}%")
    if distrib is not None:
        parts.append(f"distrib {distrib:.3f}")
    if ret_20 is not None:
        parts.append(f"近20日 {ret_20:+.1f}%")
    if ret_60 is not None:
        parts.append(f"近60日 {ret_60:+.1f}%")
    detail = "、".join(parts) + "。股价处于阶段高位且主力资金阶段性净流出，进场前建议重点复核。"

    return [
        RiskFinding(
            code="distribution_at_highs",
            severity=severity,
            title="高位派发",
            detail=detail,
            metrics=metrics,
        )
    ]


CHECKS = (
    ("volume_price", check_recent_surge),
    ("volume_price", check_high_volatility),
    ("money_flow", check_distribution_at_highs),
    ("share_float", check_share_unlock),
    (HOLDER_TRADE_COLLECTION, check_holder_reduction),
)


def review_symbol(data_db, symbol: str, base_date: str) -> Dict[str, Any]:
    """Run all checks for a single symbol and aggregate into an ``ai_risk`` dict."""
    findings: List[RiskFinding] = []
    sources: List[str] = []
    for source, check in CHECKS:
        try:
            hits = check(data_db, symbol, base_date)
        except Exception:
            hits = []
        if hits:
            findings.extend(hits)
            sources.append(source)
    severity = _max_severity(f.severity for f in findings) if findings else "none"
    return {
        "severity": severity,
        "reasons": [f.title for f in findings],
        "findings": [asdict(f) for f in findings],
        # Advisory only: 'high' suggests replacing, but the engine never acts.
        "recommend_replace": severity == "high",
        "checked_at": datetime.utcnow(),
        "sources": sources,
        "engine_version": ENGINE_VERSION,
    }


def review_plan_items(
    data_db,
    items: List[Dict[str, Any]],
    base_date: str,
) -> Dict[str, Any]:
    """Annotate ``items`` in place with ``ai_risk`` and return a plan-level summary.

    Only actionable buy/hold targets are reviewed; positions being sold off do
    not need a risk-driven replacement.
    """
    counts = {"high": 0, "medium": 0, "low": 0, "none": 0}
    reviewed = 0
    for item in items:
        symbol = item.get("symbol")
        action = item.get("action")
        if not symbol or action not in ("buy", "hold"):
            item.setdefault("ai_risk", None)
            continue
        risk = review_symbol(data_db, symbol, base_date)
        item["ai_risk"] = risk
        counts[risk["severity"]] = counts.get(risk["severity"], 0) + 1
        reviewed += 1
    return {
        "status": "done",
        "engine_version": ENGINE_VERSION,
        "reviewed_count": reviewed,
        "high": counts["high"],
        "medium": counts["medium"],
        "low": counts["low"],
        "none": counts["none"],
        "checked_at": datetime.utcnow(),
    }
