# src/grading/grade.py
import os
import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from ..notion_client import (
    query_database,
    extract_page_fields,
    build_extra_properties,
    update_lead,
)
from ..settings import get_dts_window
from ..scoring import COUNTY_LIQUIDITY
from ..gating.convertibility import is_institutional

DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


def _clamp(x: float, lo: float, hi: float) -> float:
    return float(max(lo, min(hi, x)))


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _days_to_sale(fields: Dict[str, Any]) -> Optional[int]:
    dts = fields.get("days_to_sale")
    if dts is not None:
        try:
            return int(round(float(dts)))
        except Exception:
            pass
    # fallback from sale_date
    sd = fields.get("sale_date") or ""
    if sd:
        try:
            d = datetime.fromisoformat(sd).date()
            return (d - date.today()).days
        except Exception:
            return None
    return None


def _equity_proxy(value_low: Optional[float], value_high: Optional[float], tax_assessed: Optional[float], loan_indicators: str) -> float:
    """
    Without payoff data, we use a conservative equity proxy:
    - start from value_mid vs assessed (higher spread -> higher equity score)
    - penalize if loan_indicators suggests heavy leverage.
    Returns 0..100.
    """
    if value_low is None and value_high is None and tax_assessed is None:
        return 30.0

    v_mid = None
    if value_low is not None and value_high is not None:
        v_mid = (float(value_low) + float(value_high)) / 2.0
    elif value_low is not None:
        v_mid = float(value_low)
    elif value_high is not None:
        v_mid = float(value_high)

    assessed = float(tax_assessed) if tax_assessed is not None else None

    score = 50.0
    if v_mid is not None and assessed is not None and assessed > 0:
        ratio = v_mid / assessed
        # ratio 1.0 -> 50, 1.25 -> ~65, 1.5 -> ~80, >=2 -> 95
        score = 50.0 + (ratio - 1.0) * 60.0
    elif v_mid is not None:
        # value only: modest confidence
        score = 55.0

    li = (loan_indicators or "").lower()
    if any(k in li for k in ["2nd", "second", "heloc", "home equity", "cash out", "refi"]):
        score -= 12.0
    if any(k in li for k in ["high ltv", "95", "97", "100", "fha", "va"]):
        score -= 8.0

    return _clamp(score, 0.0, 100.0)


def _time_score(dts: Optional[int], dts_min: int, dts_max: int) -> float:
    """
    We want: closer sales = more urgent, but too close = operational risk.
    Produces 0..100.
    """
    if dts is None:
        return 40.0
    if dts < 0:
        return 0.0

    # sweet spot: 21-45 days
    if dts <= 10:
        return 35.0
    if dts <= 21:
        return 70.0
    if dts <= 45:
        return 90.0
    if dts <= dts_max:
        # taper down as it gets further away
        # dts=45 -> 90, dts=dts_max -> 55
        span = max(1, dts_max - 45)
        return _clamp(90.0 - (dts - 45) * (35.0 / span), 55.0, 90.0)

    return 45.0


def _liquidity_score_proxy(county_full: str, liquidity_score_field: Optional[float]) -> float:
    if liquidity_score_field is not None:
        try:
            return _clamp(float(liquidity_score_field) * 20.0, 0.0, 100.0)
        except Exception:
            pass
    county = (county_full or "").replace(" County", "").strip()
    base = float(COUNTY_LIQUIDITY.get(county, 2))
    return _clamp(base * 20.0, 0.0, 100.0)


def _complexity_penalty(fields: Dict[str, Any]) -> float:
    """
    0..30 penalty
    """
    p = 0.0
    raw = " ".join([
        fields.get("raw_snippet") or "",
        fields.get("trustee_attorney") or "",
        fields.get("comps_summary") or "",
        fields.get("enrichment_json") or "",
    ]).lower()

    # can't verify title / parties easily
    if any(k in raw for k in ["estate of", "probate", "executor", "administrator"]):
        p += 8.0
    if "bankruptcy" in raw or "chapter" in raw:
        p += 12.0
    if any(k in raw for k in ["hoa", "condominium", "condo", "horizontal property regime"]):
        p += 4.0

    # missing key basics
    if not (fields.get("address") or "").strip():
        p += 10.0
    if not (fields.get("sale_date") or "").strip() and fields.get("days_to_sale") is None:
        p += 8.0

    return _clamp(p, 0.0, 30.0)


def grade_lead(fields: Dict[str, Any], *, dts_min: int, dts_max: int) -> Dict[str, Any]:
    dts = _days_to_sale(fields)

    # Inputs
    band_low = _safe_float(fields.get("value_band_low"))
    band_high = _safe_float(fields.get("value_band_high"))
    tax_assessed = _safe_float(fields.get("tax_assessed_value"))
    loan_indicators = fields.get("loan_indicators") or ""

    time_s = _time_score(dts, dts_min, dts_max)  # 0..100
    equity_s = _equity_proxy(band_low, band_high, tax_assessed, loan_indicators)  # 0..100
    liquidity_s = _liquidity_score_proxy(fields.get("county") or "", _safe_float(fields.get("liquidity_score")))  # 0..100
    penalty = _complexity_penalty(fields)  # 0..30

    # Weights
    # - time 25%
    # - equity 40%
    # - liquidity 35%
    base = (0.25 * time_s) + (0.40 * equity_s) + (0.35 * liquidity_s)
    score = _clamp(base - penalty, 0.0, 100.0)

    # Grade rules (auction-fit)
    reasons: List[str] = []
    if band_low is None and band_high is None:
        reasons.append("No value band yet (needs comps or estimate).")
    if dts is None:
        reasons.append("Unknown sale timing.")
    elif dts <= 10:
        reasons.append("Sale is very soon (ops risk).")
    elif dts <= 21:
        reasons.append("Sale soon — prioritize outreach.")
    if (fields.get("address") or "").strip() == "":
        reasons.append("Missing address.")
    if penalty >= 12:
        reasons.append("Complexity flags present (bankruptcy/probate/HOA/unknowns).")

    grade = "C"
    if score >= 80 and penalty <= 8:
        grade = "A"
    elif score >= 65:
        grade = "B"
    elif score >= 45:
        grade = "C"
    else:
        grade = "Reject"

    # Status flag: URGENT/HOT/GREEN
    status_flag = "GREEN"
    if dts is not None and dts <= 21 and score >= 60:
        status_flag = "URGENT"
    elif score >= 75:
        status_flag = "HOT"
    elif dts is not None and dts <= 35 and score >= 55:
        status_flag = "HOT"

    return {
        "grade_score": round(score, 1),
        "grade": grade,
        "status_flag": status_flag,
        "time_score": round(time_s, 1),
        "equity_score": round(equity_s, 1),
        "complexity_penalty": round(penalty, 1),
        "grade_reasons": "; ".join(reasons)[:900],
    }


def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("GRADE")
    max_items = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "50"))

    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
            {"property": "Address", "rich_text": {"is_not_empty": True}},
        ]
    }
    pages = query_database(
        filter_obj,
        page_size=50,
        sorts=[{"property": "Sale Date", "direction": "ascending"}],
        max_pages=10,
    )

    graded = 0
    skipped_already = 0
    skipped_missing_value = 0
    skipped_errors = 0
    skipped_institutional = 0

    for page in pages:
        if graded >= max_items:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        if not page_id:
            continue

        if is_institutional(fields):
            skipped_institutional += 1
            continue

        # already graded?
        if (fields.get("grade") or "").strip() or (fields.get("grade_score") or 0) > 0:
            skipped_already += 1
            continue

        # require at least some value estimate for meaningful grade
        if fields.get("value_band_low") is None and fields.get("value_band_high") is None and fields.get("estimated_value_low") is None and fields.get("estimated_value_high") is None:
            skipped_missing_value += 1
            continue

        # fallback: if value band missing but estimated exists, use it for equity proxy (without writing band here)
        if fields.get("value_band_low") is None and fields.get("value_band_high") is None:
            fields["value_band_low"] = fields.get("estimated_value_low")
            fields["value_band_high"] = fields.get("estimated_value_high")

        try:
            g = grade_lead(fields, dts_min=dts_min, dts_max=dts_max)
            props = build_extra_properties(g)
            update_lead(page_id, props)
            graded += 1
            if DEBUG:
                print(f"[Grader] graded page_id={page_id} score={g['grade_score']} grade={g['grade']} status={g['status_flag']}")
        except Exception as e:
            skipped_errors += 1
            print(f"[Grader] ERROR grading page_id={page_id}: {type(e).__name__}: {e}")

    summary = {
        "graded_count": graded,
        "skipped_grading_already_done": skipped_already,
        "skipped_grading_missing_value": skipped_missing_value,
        "skipped_grading_errors": skipped_errors,
        "skipped_grading_institutional": skipped_institutional,
    }
    print(f"[Grader] summary {summary}")
    return summary
