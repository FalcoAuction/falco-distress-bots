# src/enrichment/comps.py
import os
import json
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


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, dict):
            for k in ("value", "amount", "val", "low", "high"):
                if k in x:
                    v = _safe_float(x.get(k))
                    if v is not None:
                        return v
            return None
        s = str(x).strip()
        if not s:
            return None
        s = s.replace("$", "").replace(",", "")
        return float(s)
    except Exception:
        return None


def _clip_json(obj: Any, max_chars: int = 1800) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


def _compute_liquidity(county_full: str, comps_count: int, dts: Optional[float]) -> float:
    base = 2.0
    county = (county_full or "").replace(" County", "").strip()
    base = float(COUNTY_LIQUIDITY.get(county, 2))

    if comps_count >= 6:
        base += 1.0
    elif comps_count >= 3:
        base += 0.5

    if dts is not None:
        if dts <= 21:
            base -= 0.5
        elif dts <= 35:
            base -= 0.25

    return float(max(1.0, min(5.0, round(base, 2))))


def _parse_enrichment_json(enrichment_json: str) -> Dict[str, Any]:
    if not enrichment_json:
        return {}
    try:
        obj = json.loads(enrichment_json)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_value_band_from_attom_avm(enrichment_json: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Reads from the stored blob:
      {"attom_avm": {"eventDate":"...", "amount":{"value":..., "low":..., "high":..., "scr":...}}}
    Returns (value, low, high)
    """
    obj = _parse_enrichment_json(enrichment_json)
    avm = obj.get("attom_avm")
    if not isinstance(avm, dict):
        return None, None, None
    amt = avm.get("amount")
    if isinstance(amt, dict):
        v = _safe_float(amt.get("value"))
        lo = _safe_float(amt.get("low"))
        hi = _safe_float(amt.get("high"))
        return v, lo, hi
    # sometimes amount is numeric
    v2 = _safe_float(avm.get("amount"))
    return v2, None, None


def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("COMPS")
    max_items = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "25"))

    # IMPORTANT:
    # Only compute comps/bands for leads that actually have a value source:
    # - Enrichment JSON present (ATTOM AVM stored), OR
    # - Estimated Value Low/High populated.
    # This prevents 100+ empty leads from dominating "missing_value" counts.
    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
            {"property": "Address", "rich_text": {"is_not_empty": True}},
            {
                "or": [
                    {"property": "Enrichment JSON", "rich_text": {"is_not_empty": True}},
                    {"property": "Estimated Value Low", "number": {"is_not_empty": True}},
                    {"property": "Estimated Value High", "number": {"is_not_empty": True}},
                ]
            },
        ]
    }

    pages = query_database(
        filter_obj,
        page_size=50,
        sorts=[{"property": "Sale Date", "direction": "ascending"}],
        max_pages=10,
    )

    computed = 0
    skipped_already = 0
    skipped_missing_value = 0
    skipped_errors = 0
    skipped_institutional = 0

    for page in pages:
        if computed >= max_items:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        addr = (fields.get("address") or "").strip()
        if not page_id or not addr:
            continue

        if is_institutional(fields):
            skipped_institutional += 1
            continue

        # Already computed?
        if (fields.get("value_band_low") or 0) > 0 or (fields.get("value_band_high") or 0) > 0:
            skipped_already += 1
            continue

        enrichment_json = fields.get("enrichment_json") or ""

        # Primary: Use ATTOM AVM band if present
        v, lo, hi = _extract_value_band_from_attom_avm(enrichment_json)

        # Fallback: if Notion numeric fields exist and are populated
        ev_low = fields.get("estimated_value_low")
        ev_high = fields.get("estimated_value_high")
        if lo is None and ev_low is not None:
            lo = float(ev_low)
        if hi is None and ev_high is not None:
            hi = float(ev_high)
        if v is None and lo is not None:
            v = lo
        if v is None and hi is not None:
            v = hi

        if v is None and lo is None and hi is None:
            skipped_missing_value += 1
            continue

        # Construct band
        band_low = lo if lo is not None else (v * 0.9 if v is not None else None)
        band_high = hi if hi is not None else (v * 1.1 if v is not None else None)

        if band_low is None or band_high is None:
            skipped_missing_value += 1
            continue

        comps_payload = {
            "source": "attom_avm_or_estimated_value",
            "band_low": band_low,
            "band_high": band_high,
            "value": v,
            "notes": "Derived from ATTOM AVM (preferred) else Estimated Value Low/High.",
        }

        comps_count = 0  # Placeholder until real comps provider added
        liquidity = _compute_liquidity(fields.get("county") or "", comps_count=comps_count, dts=fields.get("days_to_sale"))

        write_obj = {
            "value_band_low": float(band_low),
            "value_band_high": float(band_high),
            "liquidity_score": float(liquidity),
            "comps_json": _clip_json(comps_payload),
            "comps_summary": f"Band ${int(band_low):,}–${int(band_high):,} | Liquidity {liquidity}/5 | Source: AVM/Estimated",
        }

        try:
            update_lead(page_id, build_extra_properties(write_obj))
            computed += 1
            if DEBUG:
                print(f"[CompsEngine] computed band for {addr}: low={band_low} high={band_high} liq={liquidity}")
        except Exception as e:
            skipped_errors += 1
            if DEBUG:
                print(f"[CompsEngine][DEBUG] error updating {page_id}: {type(e).__name__}: {e}")

    return {
        "computed_comps_count": computed,
        "skipped_comps_already_done": skipped_already,
        "skipped_comps_missing_value": skipped_missing_value,
        "skipped_comps_errors": skipped_errors,
        "skipped_comps_institutional": skipped_institutional,
    }
