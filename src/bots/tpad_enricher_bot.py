"""
TPAD enricher — replaces ATTOM AVM + property data using TN Comptroller's
free statewide property assessment system.

Source: assessment.cot.tn.gov/TPAD — public AJAX endpoint
   POST /TPAD/Search/GetSearchResults
Returns: parcel ID, owner, property address, sale history, GIS link
   GET /TPAD/Parcel/Details?parcelId=...&jur=...&parcelKey=...
Returns: full parcel detail (sqft, year built, mailing address, acres,
appraised value, last reappraisal year)

Coverage: 86 of 95 TN counties. The 9 NOT covered (redirect externally):
  Davidson, Williamson, Rutherford, Hamilton, Knox, Shelby, Montgomery,
  Hickman, Chester — those need per-county scrapers separately.

This bot does TWO things:
  1. ENRICHES existing leads (live + staging) by looking them up in TPAD
     via owner name + county. Adds parcel ID, sqft, year built, mailing
     address, last sale date, last reappraisal year. Sets property_value
     to county appraised value when found.
  2. Optionally lead-source mode (see scrape_distress_query()) — query
     TPAD for properties in distress profile (e.g. recent sale + tax
     delinquency cross-reference) — disabled by default.

Distress type: N/A — enrichment only, doesn't change distress_type.
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
import urllib3

try:
    import requests
except ImportError:
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    from supabase import create_client, Client
except ImportError:
    print("[tpad-enricher] supabase-py not installed", file=sys.stderr)
    raise


# Suppress SSL warnings for assessment.cot.tn.gov (cert chain issue on some
# routes; we set verify=False)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


TPAD_BASE = "https://assessment.cot.tn.gov/TPAD"
TPAD_SEARCH = f"{TPAD_BASE}/Search/GetSearchResults"
TPAD_DETAIL = f"{TPAD_BASE}/Parcel/Details"

# 9 counties NOT covered by TPAD (redirect to external systems).
EXTERNAL_COUNTIES = {
    "davidson", "williamson", "rutherford", "hamilton", "knox",
    "shelby", "montgomery", "hickman", "chester",
}

# Map of TN county name → TPAD jurisdiction code
# Codes 001..095 correspond alphabetical to the 95 TN counties as filed
# with the Comptroller. We only need the ones IN tpad (skip external).
COUNTY_CODES = {
    "anderson": "001", "bedford": "002", "benton": "003", "bledsoe": "004",
    "blount": "005", "bradley": "006", "campbell": "007", "cannon": "008",
    "carroll": "009", "carter": "010", "cheatham": "011", "claiborne": "013",
    "clay": "014", "cocke": "015", "coffee": "016", "crockett": "017",
    "cumberland": "018", "decatur": "020", "dekalb": "021", "dickson": "022",
    "dyer": "023", "fayette": "024", "fentress": "025", "franklin": "026",
    "gibson": "027", "giles": "028", "grainger": "029", "greene": "030",
    "grundy": "031", "hamblen": "032", "hancock": "033",
    "hardeman": "034", "hardin": "035", "hawkins": "036", "haywood": "037",
    "henderson": "038", "henry": "039", "houston": "040", "humphreys": "041",
    "jackson": "042", "jefferson": "043", "johnson": "044", "lake": "045",
    "lauderdale": "046", "lawrence": "047", "lewis": "048", "lincoln": "049",
    "loudon": "050", "macon": "051", "madison": "052", "marion": "053",
    "marshall": "054", "maury": "055", "mcminn": "056", "mcnairy": "057",
    "meigs": "058", "monroe": "059", "moore": "060", "morgan": "061",
    "obion": "062", "overton": "063", "perry": "064", "pickett": "065",
    "polk": "066", "putnam": "067", "rhea": "068", "roane": "069",
    "robertson": "070", "scott": "071", "sequatchie": "072", "sevier": "073",
    "smith": "074", "stewart": "076", "sullivan": "077", "sumner": "078",
    "tipton": "079", "trousdale": "080", "unicoi": "081", "union": "082",
    "vanburen": "083", "warren": "084", "washington": "085", "wayne": "086",
    "weakley": "087", "white": "088", "wilson": "095",
}


# ─── Supabase ───────────────────────────────────────────────────────────


def _supabase() -> Optional[Client]:
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        return None
    return create_client(url, key)


# ─── Helpers ────────────────────────────────────────────────────────────


def _norm_county(county: str) -> Optional[str]:
    """Normalize a county string to a lookup key (used for both
    COUNTY_CODES and EXTERNAL_COUNTIES). Returns None only if the input
    isn't recognizable as a TN county at all."""
    if not county:
        return None
    c = re.sub(r"\s+county$", "", county.strip().lower())
    c = c.replace(" ", "").replace("'", "")
    if c in COUNTY_CODES or c in EXTERNAL_COUNTIES:
        return c
    return None


def _iso_date(value: Any) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    return s[:10] if re.match(r"^\d{4}-\d{2}-\d{2}$", s[:10]) else None


def _build_session() -> "requests.Session":
    s = requests.Session()
    s.headers["User-Agent"] = "FALCO-Lead-Research/1.0 (+ops@falco.llc)"
    s.headers["Accept"] = "application/json, text/html"
    s.verify = False
    # Prime cookies via initial GET
    try:
        s.get(f"{TPAD_BASE}/", timeout=15)
    except Exception:
        pass
    return s


# ─── TPAD search + detail ───────────────────────────────────────────────


def search_by_owner(
    session: "requests.Session", jur_code: str, owner: str, address: str = ""
) -> List[Dict[str, Any]]:
    """POST to TPAD AJAX endpoint, return list of result dicts."""
    payload = {
        "Jur": jur_code,
        "Owner": owner,
        "PropertyAddress": address,
        "SubdivisionName": "",
        "PropertyType": "",
        "SaleDateRangeStart": "",
        "SaleDateRangeEnd": "",
        "ControlMap": "",
        "MapGroup": "",
        "ParcelNumber": "",
        "GISLink": "",
    }
    try:
        r = session.post(TPAD_SEARCH, data=payload, timeout=30)
        if r.status_code != 200:
            return []
        return r.json() or []
    except Exception:
        return []


def fetch_detail(
    session: "requests.Session", parcel_id: str, jur: str, parcel_key: str
) -> Dict[str, Any]:
    """Fetch parcel detail page + extract structured fields via regex."""
    if BeautifulSoup is None:
        return {}
    url = (
        f"{TPAD_DETAIL}?parcelId={quote(parcel_id)}"
        f"&jur={quote(jur)}&parcelKey={quote(parcel_key)}"
    )
    try:
        r = session.get(url, timeout=20)
        if r.status_code != 200:
            return {}
    except Exception:
        return {}

    soup = BeautifulSoup(r.text, "html.parser")
    text = re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

    out: Dict[str, Any] = {"detail_url": url}
    field_patterns = {
        "year_built": r"Year Built[:\s]+(\d{4})",
        "sqft_living": r"Total Living Area[:\s]+([\d,]+)",
        "sqft_living_alt": r"of Living Area[:\s]+([\d,]+)",
        "acres": r"Acres[:\s]+([\d.]+)",
        "appraised_value": r"Total Appraisal[^\d]*\$?([\d,]+)",
        "appraised_value_alt": r"Total Appraised Value[^\d]*\$?([\d,]+)",
        "land_value": r"Land Value[:\s]*\$?([\d,]+)",
        "building_value": r"(?:Improvement|Building) Value[:\s]*\$?([\d,]+)",
        "tax_year": r"Tax Year[:\s]+(\d{4})",
        "reappraisal_year": r"Reappraisal Year[:\s]+(\d{4})",
        "property_class": r"Property Class[:\s]+([A-Za-z][A-Za-z0-9 ,/-]*?)(?:\s{2,}|$)",
        "last_sale_price": r"(?:Last Sale Price|Sale Price)[:\s]*\$?([\d,]+)",
        "last_sale_date": r"(?:Last Sale Date|Sale Date)[:\s]+(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
    }
    for field, pat in field_patterns.items():
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            out[field] = m.group(1).strip()

    # Mailing address (may be different from property address — absentee signal)
    mail_m = re.search(
        r"Mailing Address[:\s]+(?:January 1 Owner )?([A-Z][A-Z0-9 .,&'\-/()]+?(?:TN|TENNESSEE)\s*\d{5})",
        text,
    )
    if mail_m:
        out["mailing_address"] = mail_m.group(1).strip()

    # Normalize values to floats where possible
    for k in ("appraised_value", "appraised_value_alt", "land_value", "building_value", "last_sale_price"):
        if k in out:
            try:
                out[k.replace("_alt", "") + "_num"] = float(out[k].replace(",", ""))
            except ValueError:
                pass
    if out.get("last_sale_date"):
        out["last_sale_date"] = _iso_date(out["last_sale_date"])

    return out


# ─── Match scoring ──────────────────────────────────────────────────────


def _normalize_addr(addr: str) -> str:
    """Strip apt/unit + lowercase + collapse whitespace for fuzzy match."""
    a = (addr or "").lower()
    a = re.sub(r"\s+(apt|unit|suite|ste|#)\s*\S+", "", a)
    a = re.sub(r"[^a-z0-9 ]", " ", a)
    a = re.sub(r"\s+", " ", a).strip()
    return a


def best_match(results: List[Dict[str, Any]], target_address: str) -> Optional[Dict[str, Any]]:
    """Pick the result whose property_address most closely matches target."""
    if not results or not target_address:
        return None
    target = _normalize_addr(target_address)
    target_tokens = set(target.split())
    if not target_tokens:
        return None
    scored: List[Tuple[float, int, Dict[str, Any]]] = []
    for idx, r in enumerate(results):
        addr = _normalize_addr(r.get("propertyAddress") or "")
        if not addr:
            continue
        tokens = set(addr.split())
        if not tokens:
            continue
        intersection = target_tokens & tokens
        union = target_tokens | tokens
        jaccard = len(intersection) / len(union) if union else 0
        target_num = next((t for t in target_tokens if t.isdigit() and len(t) >= 2), None)
        addr_num = next((t for t in tokens if t.isdigit() and len(t) >= 2), None)
        num_bonus = 0.3 if target_num and target_num == addr_num else 0
        # Index in the tuple is the tiebreaker so sorting never has to
        # compare dicts (which raises TypeError in Python 3).
        scored.append((jaccard + num_bonus, idx, r))
    scored.sort(key=lambda t: (-t[0], t[1]))
    if not scored or scored[0][0] < 0.25:
        return None
    return scored[0][2]


# ─── Main enrichment loop ────────────────────────────────────────────────


def enrich_lead(
    session: "requests.Session", lead: Dict[str, Any]
) -> Tuple[bool, Dict[str, Any]]:
    """Try to find this lead in TPAD and pull detail."""
    address = lead.get("property_address") or ""
    county = lead.get("county") or ""
    owner = lead.get("owner_name_records") or lead.get("full_name") or ""

    county_key = _norm_county(county)
    if not county_key:
        return False, {"error": "unknown_or_external_county", "county": county}
    if county_key.lower() in EXTERNAL_COUNTIES:
        return False, {"error": "external_county", "county": county}

    jur = COUNTY_CODES[county_key]

    # Strategy: search by owner if we have a real-looking name; otherwise
    # search by partial address. Try owner first, fall back to address.
    results: List[Dict[str, Any]] = []
    if owner and len(owner) >= 4 and re.search(r"[A-Za-z]", owner):
        # Use last name only — reduces over-specificity
        last_name = owner.split()[-1] if " " in owner else owner
        results = search_by_owner(session, jur, last_name)
    if not results and address:
        # Pull street-number tokens from address
        m = re.match(r"^(\d+)\s+([^,]+)", address)
        if m:
            street = m.group(2).strip()[:30]
            results = search_by_owner(session, jur, "", street)

    if not results:
        return False, {"error": "no_results", "owner": owner, "county": county}

    match = best_match(results, address)
    if not match:
        return False, {"error": "no_match", "results_count": len(results)}

    # Drill into detail page
    parcel_id = match.get("parcelId") or ""
    parcel_key = match.get("parcelKey") or ""
    if not parcel_id or not parcel_key:
        return True, {"match_only": True, "match": match}

    detail = fetch_detail(session, parcel_id, jur, parcel_key)
    last_sale_raw = match.get("dateOfSaleShort")
    last_sale_date = detail.get("last_sale_date") or _iso_date(last_sale_raw)
    out = {
        "tpad": {
            "parcel_id": parcel_id,
            "parcel_key": parcel_key,
            "jur": jur,
            "county": match.get("countyName"),
            "owner": match.get("owner"),
            "property_address": match.get("propertyAddress"),
            "subdivision": match.get("subdivisionName"),
            "lot": match.get("lotNumber"),
            "class": match.get("class"),
            "property_type": match.get("propertyType"),
            "last_sale": last_sale_raw,
            "last_sale_date": last_sale_date,
            "last_sale_price": detail.get("last_sale_price_num"),
            "tax_year": match.get("taxYear"),
            "gis_map": match.get("gisMap"),
            **detail,
        },
    }
    return True, out


def update_lead(
    client: Client, table: str, lead_id: str, tpad: Dict[str, Any], lead: Dict[str, Any]
) -> bool:
    """Push enrichment back to the lead row."""
    update: Dict[str, Any] = {}

    # Set property_value (AVM) if absent and TPAD has appraised value
    appraised = tpad.get("appraised_value_num") or tpad.get("appraised_value_alt_num")
    if appraised and not lead.get("property_value"):
        update["property_value"] = appraised
        update["property_value_source"] = "TPAD"
        update["property_value_as_of"] = datetime.now(timezone.utc).isoformat()

    # Owner name from TPAD if missing
    if tpad.get("owner") and not lead.get("owner_name_records"):
        update["owner_name_records"] = tpad["owner"]

    # Append to admin_notes
    summary_bits = []
    for k in ("parcel_id", "year_built", "sqft_living", "appraised_value",
              "mailing_address", "last_sale", "reappraisal_year", "class"):
        v = tpad.get(k) or tpad.get(f"{k}_alt")
        if v:
            summary_bits.append(f"{k}={str(v)[:60]}")
    notes_addition = " · ".join(summary_bits)
    existing_notes = lead.get("admin_notes") or ""
    update["admin_notes"] = (
        existing_notes + (" · " if existing_notes else "") + f"TPAD: {notes_addition}"
    )[:4000]

    # Stash full payload in raw_payload (staging) or skiptrace_data (live has this column)
    if table == "homeowner_requests_staging":
        rp = lead.get("raw_payload") or {}
        if not isinstance(rp, dict):
            rp = {}
        rp["tpad"] = tpad
        update["raw_payload"] = rp

    try:
        client.table(table).update(update).eq("id", lead_id).execute()
        return True
    except Exception as e:
        print(f"  update failed: {e}")
        return False


def run() -> Dict[str, Any]:
    if requests is None:
        return {"name": "tpad_enricher", "status": "missing_deps"}
    client = _supabase()
    if client is None:
        return {"name": "tpad_enricher", "status": "no_supabase"}

    session = _build_session()
    enriched = 0
    no_match = 0
    external = 0
    skipped = 0

    for table in ("homeowner_requests", "homeowner_requests_staging"):
        print(f"\n--- {table} ---")
        try:
            sel = "id, county, property_address, owner_name_records, full_name, admin_notes, property_value"
            if table == "homeowner_requests_staging":
                sel += ", raw_payload"
            else:
                sel += ", source"
            q = client.table(table).select(sel).limit(500)
            if table == "homeowner_requests":
                q = q.eq("source", "bot")
            res = q.execute()
        except Exception as e:
            print(f"  fetch failed: {e}")
            continue

        rows = getattr(res, "data", None) or []
        # Skip leads already enriched
        rows = [r for r in rows if "TPAD:" not in (r.get("admin_notes") or "")]
        print(f"  {len(rows)} candidates")

        for i, lead in enumerate(rows[:100]):  # cap per run
            print(f"  [{i+1}] {(lead.get('property_address') or '')[:55]:58s}", end=" ")
            ok, result = enrich_lead(session, lead)
            if not ok:
                err = result.get("error", "unknown")
                if err == "external_county":
                    external += 1
                else:
                    no_match += 1
                print(f"-> SKIP ({err})")
                skipped += 1
                continue
            tpad = result.get("tpad") or result.get("match", {})
            print(f"-> OK (parcel={tpad.get('parcel_id', tpad.get('parcelId', '?'))[:20]})")
            if update_lead(client, table, lead["id"], tpad, lead):
                enriched += 1

            time.sleep(0.5)  # polite

    print(f"\nenriched={enriched} no_match={no_match} external={external} skipped={skipped}")
    return {
        "name": "tpad_enricher",
        "status": "ok" if enriched > 0 else "zero_yield",
        "enriched": enriched,
        "no_match": no_match,
        "external_county": external,
        "skipped": skipped,
    }


if __name__ == "__main__":
    print(run())
