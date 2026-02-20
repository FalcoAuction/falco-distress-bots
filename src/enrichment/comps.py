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
        if isinstance(x, (int, float)):
            return float(x)
        if isinstance(x, dict):
            # common nested shapes
            for k in ("value", "amount", "val"):
                if k in x:
                    return _safe_float(x.get(k))
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


def _extract_estimated_value_from_enrichment_json(enrichment_json: str) -> Optional[float]:
    """
    ATTOM AVM shape we observed:
      {"falco":{"attom":{"meta":...}}} OR {"falco":{"attom":...}} plus meta
    But we also logged:
      avm.avm={"eventDate": "...", "amount": {"scr":95,"value":529582,"high":..., "low":..., ...}}
    We try a few tolerant paths.
    """
    if not enrichment_json:
        return None
    try:
        obj = json.loads(enrichment_json)
        if not isinstance(obj, dict):
            return None

        # Most recent pattern we wrote:
        # {"falco":{"attom":{"no_result":..., "ts":...}}, "meta":{"address1":...,"value_source":...}}
        falco = obj.get("falco")
        if isinstance(falco, dict):
            attom = falco.get("attom")
            if isinstance(attom, dict):
                # if we later store a direct value, catch it
                for k in ("avm_value", "value", "estimated_value", "estimatedValue"):
                    v = _safe_float(attom.get(k))
                    if v is not None:
                        return v

        # Also try to pull from a raw-ish stored AVM object if present:
        # {"attom_avm": {"eventDate":..., "amount": {"value":...}}}
        for key in ("attom_avm", "avm", "attom", "attomAVM"):
            maybe = obj.get(key)
            if isinstance(maybe, dict):
                amt = maybe.get("amount")
                if isinstance(amt, dict):
                    v = _safe_float(amt.get("value"))
                    if v is not None:
                        return v
                v2 = _safe_float(maybe.get("value"))
                if v2 is not None:
                    return v2

        # Deep search (last resort): find first dict that looks like {"scr":..,"value":..,"high":..,"low":..}
        def dfs(x: Any) -> Optional[float]:
            if isinstance(x, dict):
                if "value" in x and ("high" in x or "low" in x or "scr" in x):
                    return _safe_float(x.get("value"))
                for vv in x.values():
                    out = dfs(vv)
                    if out is not None:
                        return out
            elif isinstance(x, list):
                for it in x:
                    out = dfs(it)
                    if out is not None:
                        return out
            return None

        return dfs(obj)

    except Exception:
        return None


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

        if not page_id or not addr:
            continue

        # already computed?
        if (fields.get("value_band_low") or 0) > 0 or (fields.get("comps_summary") or "").strip():
            skipped_already += 1
            continue

        enrichment_json = fields.get("enrichment_json") or ""
        comps = _parse_comps_from_enrichment_json(enrichment_json)

        if not comps:
            skipped_missing_enrichment += 1

            # fallback: use estimated value if we have it (or can extract it from enrichment_json)
            ev_low = fields.get("estimated_value_low")
            ev_high = fields.get("estimated_value_high")

            if ev_low is None and ev_high is None:
                ev = _extract_estimated_value_from_enrichment_json(enrichment_json)
                if ev is not None:
                    ev_low = ev
                    ev_high = ev

            if ev_low or ev_high:
                low = float(ev_low) if ev_low else float(ev_high) * 0.9
                high = float(ev_high) if ev_high else float(ev_low) * 1.1
                liquidity = _compute_liquidity(fields.get("county") or "", 0, fields.get("days_to_sale"))
                summary = "Value band derived from estimated value (no comps provided)."

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
                    if DEBUG:
                        print(f"[CompsEngine] fallback-band page_id={page_id} addr={addr} band=({low},{high})")
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
