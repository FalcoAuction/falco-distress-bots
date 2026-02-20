# src/enrichment/propstream_enricher.py
import os
import csv
import json
import re
from typing import Any, Dict, List, Optional, Tuple

from ..notion_client import (
    query_database,
    extract_page_fields,
    build_extra_properties,
    update_lead,
)
from ..settings import get_dts_window


# ============================================================
# ENV / CONTROLS
# ============================================================

MAX_ENRICH_DEFAULT = 25
DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")

# Minimal, dependency-free enrichment source:
# A PropStream export CSV (or any CSV) you provide with required columns.
PROPSTREAM_CSV_PATH = os.getenv("FALCO_PROPSTREAM_CSV_PATH", "").strip()

# If you want to tighten matching, provide a delimiter-normalized county column in the CSV.
CSV_ADDR_COL = os.getenv("FALCO_PROPSTREAM_ADDR_COL", "address").strip()
CSV_COUNTY_COL = os.getenv("FALCO_PROPSTREAM_COUNTY_COL", "county").strip()


# ============================================================
# NORMALIZATION / MATCHING
# ============================================================

_STREET_ABBR = {
    "street": "st",
    "st.": "st",
    "avenue": "ave",
    "ave.": "ave",
    "road": "rd",
    "rd.": "rd",
    "drive": "dr",
    "dr.": "dr",
    "lane": "ln",
    "ln.": "ln",
    "court": "ct",
    "ct.": "ct",
    "boulevard": "blvd",
    "blvd.": "blvd",
    "place": "pl",
    "pl.": "pl",
    "circle": "cir",
    "cir.": "cir",
    "highway": "hwy",
    "pike": "pike",
}


def _norm_addr(s: str) -> str:
    if not s:
        return ""
    t = s.lower().strip()
    t = re.sub(r"\s+", " ", t)
    t = t.replace(",", " ")
    t = re.sub(r"\s+", " ", t).strip()
    parts = t.split(" ")
    out = []
    for p in parts:
        out.append(_STREET_ABBR.get(p, p))
    t = " ".join(out)
    # remove unit markers that churn matches
    t = re.sub(r"\b(apt|unit|#)\b\s*\w+\b", "", t).strip()
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _norm_county(s: str) -> str:
    if not s:
        return ""
    t = s.strip()
    t = re.sub(r"\s+", " ", t)
    if t.lower().endswith(" county"):
        t = t[:-7].strip()
    return t.lower()


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


def _safe_int(x: Any) -> Optional[int]:
    try:
        f = _safe_float(x)
        if f is None:
            return None
        return int(round(f))
    except Exception:
        return None


def _safe_date_iso(x: Any) -> str:
    # Keep it simple: accept yyyy-mm-dd; else return empty.
    if not x:
        return ""
    s = str(x).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return s
    return ""


def _row_to_enrichment(row: Dict[str, Any]) -> Dict[str, Any]:
    # Column names are configurable but we keep common defaults.
    # You can map your export CSV to these columns using env vars or by renaming columns.
    out: Dict[str, Any] = {}

    def g(*names: str) -> str:
        for n in names:
            if n in row and str(row.get(n) or "").strip():
                return str(row.get(n)).strip()
        return ""

    owner = g("owner_name", "Owner Name", "owner")
    mailing = g("mailing_address", "Mailing Address", "mailing")
    absentee = g("absentee_flag", "Absentee Flag", "absentee")
    beds = g("beds", "Beds")
    baths = g("baths", "Baths")
    sqft = g("sqft", "Sqft", "Living Area", "living_area")
    year_built = g("year_built", "Year Built", "yearbuilt")
    ev_low = g("estimated_value_low", "Estimated Value Low", "est_value_low", "Est Value Low")
    ev_high = g("estimated_value_high", "Estimated Value High", "est_value_high", "Est Value High")
    loan = g("loan_indicators", "Loan Indicators", "loan")
    last_sale = g("last_sale_date", "Last Sale Date", "last_sale")
    tax_assessed = g("tax_assessed_value", "Tax Assessed Value", "assessed_value")

    if owner:
        out["owner_name"] = owner
    if mailing:
        out["mailing_address"] = mailing
    if absentee:
        out["absentee_flag"] = str(absentee).strip().lower() in ("1", "true", "yes", "y")
    if beds:
        out["beds"] = _safe_int(beds)
    if baths:
        out["baths"] = _safe_float(baths)
    if sqft:
        out["sqft"] = _safe_int(sqft)
    if year_built:
        out["year_built"] = _safe_int(year_built)
    if ev_low:
        out["estimated_value_low"] = _safe_float(ev_low)
    if ev_high:
        out["estimated_value_high"] = _safe_float(ev_high)
    if loan:
        out["loan_indicators"] = loan
    if last_sale:
        out["last_sale_date"] = _safe_date_iso(last_sale)
    if tax_assessed:
        out["tax_assessed_value"] = _safe_float(tax_assessed)

    return out


def _load_csv_index() -> Tuple[Dict[Tuple[str, str], Dict[str, Any]], int]:
    if not PROPSTREAM_CSV_PATH:
        return {}, 0
    if not os.path.exists(PROPSTREAM_CSV_PATH):
        print(f"[PropStreamEnricher] WARNING: FALCO_PROPSTREAM_CSV_PATH not found: {PROPSTREAM_CSV_PATH}")
        return {}, 0

    index: Dict[Tuple[str, str], Dict[str, Any]] = {}
    rows = 0

    with open(PROPSTREAM_CSV_PATH, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows += 1
            addr = _norm_addr(str(row.get(CSV_ADDR_COL) or row.get("address") or ""))
            if not addr:
                continue
            cty = _norm_county(str(row.get(CSV_COUNTY_COL) or row.get("county") or ""))
            key = (addr, cty)
            # keep first match; you can pre-dedupe your CSV
            if key not in index:
                index[key] = row

    return index, rows


# ============================================================
# MAIN RUNNER
# ============================================================

def run() -> Dict[str, int]:
    dts_min, dts_max = get_dts_window("ENRICH")
    max_enrich = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", str(MAX_ENRICH_DEFAULT)))

    if not PROPSTREAM_CSV_PATH:
        print("[PropStreamEnricher] WARNING: missing FALCO_PROPSTREAM_CSV_PATH; skipping Stage 2 enrichment.")
        return {
            "enriched_count": 0,
            "skipped_enrich_missing_address": 0,
            "skipped_enrich_already_enriched": 0,
            "skipped_enrich_no_match": 0,
            "skipped_enrich_errors": 0,
        }

    index, csv_rows = _load_csv_index()
    if not index:
        print("[PropStreamEnricher] WARNING: PropStream CSV index empty; skipping Stage 2 enrichment.")
        return {
            "enriched_count": 0,
            "skipped_enrich_missing_address": 0,
            "skipped_enrich_already_enriched": 0,
            "skipped_enrich_no_match": 0,
            "skipped_enrich_errors": 0,
        }

    if DEBUG:
        print(f"[PropStreamEnricher] loaded_csv_rows={csv_rows} indexed_keys={len(index)}")

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

    enriched = 0
    skipped_missing_addr = 0
    skipped_already = 0
    skipped_no_match = 0
    skipped_errors = 0

    for page in pages:
        if enriched >= max_enrich:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        addr = (fields.get("address") or "").strip()
        county = (fields.get("county") or "").strip()

        if not addr:
            skipped_missing_addr += 1
            continue

        # Already enriched?
        if (fields.get("enrichment_confidence") or 0) > 0 or (fields.get("owner_name") or "").strip():
            skipped_already += 1
            continue

        key = (_norm_addr(addr), _norm_county(county))
        row = index.get(key)

        if not row:
            # fallback: match ignoring county if county missing or mismatch
            if not county:
                for (a, _c), r in index.items():
                    if a == _norm_addr(addr):
                        row = r
                        break
            if not row:
                skipped_no_match += 1
                continue

        try:
            enrichment = _row_to_enrichment(row)
            if not enrichment:
                skipped_no_match += 1
                continue

            # confidence heuristic:
            # - base 0.6 if matched by normalized address
            # - +0.2 if county present and matches
            conf = 0.6
            if county and _norm_county(county) == _norm_county(str(row.get(CSV_COUNTY_COL) or row.get("county") or "")):
                conf += 0.2
            # - +0.2 if we got sqft or value
            if enrichment.get("sqft") or enrichment.get("estimated_value_low") or enrichment.get("estimated_value_high"):
                conf += 0.2
            enrichment["enrichment_confidence"] = round(min(conf, 1.0), 2)

            # store minimal structured payload as JSON (clipped upstream by notion_client)
            enrichment_payload = dict(enrichment)
            enrichment_payload["_source"] = "propstream_csv"
            enrichment_payload["_match_key"] = {"addr_norm": key[0], "county_norm": key[1]}
            enrichment["enrichment_json"] = json.dumps(enrichment_payload, ensure_ascii=False)

            props = build_extra_properties(enrichment)
            update_lead(page_id, props)
            enriched += 1

            if DEBUG:
                print(f"[PropStreamEnricher] enriched page_id={page_id} addr={addr} county={county} conf={enrichment['enrichment_confidence']}")

        except Exception as e:
            skipped_errors += 1
            print(f"[PropStreamEnricher] ERROR enriching page_id={page_id}: {type(e).__name__}: {e}")

    summary = {
        "enriched_count": enriched,
        "skipped_enrich_missing_address": skipped_missing_addr,
        "skipped_enrich_already_enriched": skipped_already,
        "skipped_enrich_no_match": skipped_no_match,
        "skipped_enrich_errors": skipped_errors,
    }
    print(f"[PropStreamEnricher] summary {summary}")
    return summary
