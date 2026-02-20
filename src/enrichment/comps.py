# src/enrichment/comps.py
import os
import json
import statistics
from typing import Any, Dict, List, Optional, Tuple

from ..notion_client import (
    query_database,
    extract_page_fields,
    build_extra_properties,
    update_lead,
)
from ..settings import get_dts_window
from ..scoring import COUNTY_LIQUIDITY

DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        s = s.replace("$", "").replace(",", "")
        return float(s)
    except Exception:
        return None


def _median(vals: List[float]) -> Optional[float]:
    v = [x for x in vals if x is not None]
    if not v:
        return None
    try:
        return float(statistics.median(v))
    except Exception:
        v = sorted(v)
        n = len(v)
        mid = n // 2
        return (v[mid] if n % 2 == 1 else (v[mid - 1] + v[mid]) / 2.0)


def _clip_json(obj: Any, max_chars: int = 1800) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


def _parse_comps_from_enrichment_json(enrichment_json: str) -> List[dict]:
    """
    Supports two minimal patterns:
      1) {"comps":[{sale_price, sqft, sale_date, distance_miles, address}, ...]}
      2) {"comparables":[...]} (alias)
    """
    if not enrichment_json:
        return []
    try:
        obj = json.loads(enrichment_json)
        if isinstance(obj, dict):
            comps = obj.get("comps") or obj.get("comparables") or []
            if isinstance(comps, list):
                return [c for c in comps if isinstance(c, dict)]
        return []
    except Exception:
        return []


def _compute_liquidity(county_full: str, comps_count: int, dts: Optional[float]) -> float:
    base = 2.0
    county = (county_full or "").replace(" County", "").strip()
    base = float(COUNTY_LIQUIDITY.get(county, 2))
    # comps availability proxy
    if comps_count >= 6:
        base += 1.0
    elif comps_count >= 3:
        base += 0.5
    # time pressure (closer sales need higher liquidity)
    if dts is not None:
        if dts <= 21:
            base -= 0.5
        elif dts <= 35:
            base -= 0.25
    return float(max(1.0, min(5.0, round(base, 2))))


def _value_band_from_comps(comps: List[dict], subject_sqft: Optional[float]) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Returns (median_price, median_ppsf, band_low, band_high)
    """
    prices: List[float] = []
    ppsf: List[float] = []

    for c in comps:
        sp = _safe_float(c.get("sale_price"))
        sf = _safe_float(c.get("sqft"))
        if sp is not None:
            prices.append(sp)
        if sp is not None and sf and sf > 200:
            ppsf.append(sp / sf)

    median_price = _median(prices)
    median_ppsf = _median(ppsf)

    # band logic:
    # - prefer ppsf if we have subject sqft
    # - else band around median_price
    band_low = None
    band_high = None

    if median_ppsf is not None and subject_sqft and subject_sqft > 200:
        est = median_ppsf * subject_sqft
        band_low = est * 0.85
        band_high = est * 1.15
    elif median_price is not None:
        band_low = median_price * 0.85
        band_high = median_price * 1.15

    if band_low is not None:
        band_low = round(band_low, 0)
    if band_high is not None:
        band_high = round(band_high, 0)

    return median_price, median_ppsf, band_low, band_high


def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("COMPS")
    max_items = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "25"))  # share limit with enrich by default

    # Query candidates: within DTS window, address not empty.
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

    computed = 0
    skipped_missing_enrichment = 0
    skipped_already = 0
    skipped_no_comps = 0
    skipped_errors = 0

    for page in pages:
        if computed >= max_items:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        addr = (fields.get("address") or "").strip()

        if not addr:
            continue

        # already computed?
        if (fields.get("value_band_low") or 0) > 0 or (fields.get("comps_summary") or "").strip():
            skipped_already += 1
            continue

        enrichment_json = fields.get("enrichment_json") or ""
        comps = _parse_comps_from_enrichment_json(enrichment_json)

        if not comps:
            skipped_missing_enrichment += 1
            # fallback: use estimated value if we have it (still produce band)
            ev_low = fields.get("estimated_value_low")
            ev_high = fields.get("estimated_value_high")
            if ev_low or ev_high:
                low = float(ev_low) if ev_low else float(ev_high) * 0.9
                high = float(ev_high) if ev_high else float(ev_low) * 1.1
                liquidity = _compute_liquidity(fields.get("county") or "", 0, fields.get("days_to_sale"))
                summary = f"Value band derived from estimated value (no comps provided)."
                data = {
                    "value_band_low": round(low, 0),
                    "value_band_high": round(high, 0),
                    "liquidity_score": liquidity,
                    "comps_json": _clip_json({"comps": [], "method": "estimated_value_fallback"}),
                    "comps_summary": summary,
                }
                try:
                    props = build_extra_properties(data)
                    update_lead(page_id, props)
                    computed += 1
                    continue
                except Exception as e:
                    skipped_errors += 1
                    print(f"[CompsEngine] ERROR updating fallback band page_id={page_id}: {type(e).__name__}: {e}")
                    continue

            skipped_no_comps += 1
            continue

        try:
            subject_sqft = fields.get("sqft")
            median_price, median_ppsf, band_low, band_high = _value_band_from_comps(comps, subject_sqft)
            liquidity = _compute_liquidity(fields.get("county") or "", len(comps), fields.get("days_to_sale"))

            # human summary (short)
            parts = []
            if median_price is not None:
                parts.append(f"Median sale price: ${median_price:,.0f}")
            if median_ppsf is not None:
                parts.append(f"Median $/sqft: ${median_ppsf:,.0f}")
            if band_low is not None and band_high is not None:
                parts.append(f"Value band: ${band_low:,.0f}–${band_high:,.0f}")
            parts.append(f"Comps used: {len(comps)}")
            parts.append(f"Liquidity score: {liquidity:.1f}/5")

            data = {
                "value_band_low": band_low,
                "value_band_high": band_high,
                "liquidity_score": liquidity,
                "comps_json": _clip_json({"comps": comps[:6], "method": "enrichment_json"}),
                "comps_summary": " | ".join(parts)[:900],
            }

            props = build_extra_properties(data)
            update_lead(page_id, props)
            computed += 1

            if DEBUG:
                print(f"[CompsEngine] computed page_id={page_id} addr={addr} band=({band_low},{band_high}) comps={len(comps)}")

        except Exception as e:
            skipped_errors += 1
            print(f"[CompsEngine] ERROR computing comps page_id={page_id}: {type(e).__name__}: {e}")

    summary = {
        "computed_comps_count": computed,
        "skipped_comps_missing_enrichment": skipped_missing_enrichment,
        "skipped_comps_already_done": skipped_already,
        "skipped_comps_no_comps": skipped_no_comps,
        "skipped_comps_errors": skipped_errors,
    }
    print(f"[CompsEngine] summary {summary}")
    return summary
