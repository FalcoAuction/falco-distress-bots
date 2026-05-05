"""Free fetchers for property sale_date + sale_price + deed_reference,
keyed off the lead's property_address.

Used by the HMDA enricher to tighten its tract+amount match window
from 6-year + ±10% (~1000 candidates) down to 1-year + ±5% (~3-10
candidates) which lifts match accuracy from ~38% to an expected ~75%.

County-specific endpoints (all free, public, no auth):

  Davidson County (FIPS 47037)
    Quick search by address: portal.padctn.org/OFS/WP/PropertySearch/
                              QuickPropertySearchAsync
    Printable card (sale data + deed ref): portal.padctn.org/OFS/WP/Print/{account_id}
    Returns: Most Recent Sale Date, Most Recent Sale Price, Deed Reference

  TPAD-covered counties (Sumner, Wilson, Maury)
    Statewide TN comptroller: assessment.cot.tn.gov/TPAD
    See tpad_enricher_bot.py for endpoint details.

  Williamson County (Inigo)
    inigo.williamson-tn.org/property_search/json/search returns sale
    history JSON.

  Rutherford County (ArcGIS)
    services5.arcgis.com Parcel_Data feature service exposes SaleDate
    + SalePrice fields.

This module exposes a single resolve() function that auto-routes by
county and returns:
  {
    "sale_date": "YYYY-MM-DD" or None,
    "sale_price": float or None,
    "deed_reference": str or None,
    "appraised": float or None,
    "parcel": str or None,
    "source": str,
  }
"""
from __future__ import annotations

import re
import time
from typing import Any, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup


PADCTN_QUICKSEARCH = (
    "https://portal.padctn.org/OFS/WP/PropertySearch/QuickPropertySearchAsync"
)
PADCTN_PRINT = "https://portal.padctn.org/OFS/WP/Print/{account_id}"
PADCTN_HOME = "https://portal.padctn.org/OFS/WP/Home"

# Padctn card field regexes
PADCTN_SALE_DATE_RE = re.compile(
    r"Most Recent Sale Date:\s*(\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE
)
PADCTN_SALE_PRICE_RE = re.compile(
    r"Most Recent Sale Price:\s*\$?([\d,]+)", re.IGNORECASE
)
PADCTN_DEED_REF_RE = re.compile(
    r"Deed Reference:\s*(\d{8}-\d+)", re.IGNORECASE
)
PADCTN_APPRAISED_RE = re.compile(
    r"Total Appraisal Value:\s*\$?([\d,]+)", re.IGNORECASE
)
PADCTN_PARCEL_RE = re.compile(
    r"Map & Parcel:\s*(\S[\s\S]+?\d{2,3}\.\d{2})", re.IGNORECASE
)

# Account_id from the quicksearch HTML — extracted via the JS callback
# the dxdvControl widget emits per row.
PADCTN_ACCOUNT_RE = re.compile(r"OnSearchGridSelectAccount\((\d+)")

ADDRESS_NUMBER_STREET_RE = re.compile(r"^\s*(\d+)\s+(.+)$")


def _parse_address_for_padctn(address: str) -> Optional[Tuple[str, str]]:
    """Split '1234 Main St, Nashville, TN 37206' → ('1234', 'Main St')."""
    if not address:
        return None
    head = address.split(",")[0].strip()
    m = ADDRESS_NUMBER_STREET_RE.match(head)
    if not m:
        return None
    return m.group(1), m.group(2).strip()


def _money_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    try:
        return float(s.replace(",", "").replace("$", "").strip())
    except ValueError:
        return None


def _date_to_iso(s: str) -> Optional[str]:
    if not s:
        return None
    s = s.strip().split()[0]
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            from datetime import datetime
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _padctn_account(session: requests.Session, address: str) -> Optional[int]:
    """Quick search padctn by address → account_id.

    Tries several street-name variations to handle:
      - directional suffixes (1723 7TH AVE N → "7TH AVE N", "7TH AVE", "7TH")
      - period suffixes ("Riverside Dr." → "Riverside Dr", "Riverside")
      - sub-letters ("323 B FOREST PARK RD" → "B FOREST PARK RD", "FOREST PARK RD", "FOREST PARK")
      - missing commas ("104 Creighton Ave Nashville" → "Creighton Ave Nashville", "Creighton Ave", "Creighton")
    """
    parsed = _parse_address_for_padctn(address)
    if not parsed:
        return None
    number, street = parsed

    # Build search variations
    variations = []
    s = street.upper()
    # Strip trailing periods + extra whitespace
    s = re.sub(r"\.", "", s).strip()
    # Strip city-name tail if present (only first 1-2 words are likely
    # the actual street name on padctn)
    variations.append(s)
    # Strip directional suffix (N/S/E/W)
    s2 = re.sub(r"\s+(N|S|E|W|NORTH|SOUTH|EAST|WEST|NE|NW|SE|SW)$", "", s)
    if s2 != s:
        variations.append(s2)
    # Strip sub-letter prefix ("B FOREST PARK RD" → "FOREST PARK RD")
    s3 = re.sub(r"^[A-Z]\s+", "", s2 or s)
    if s3 not in variations:
        variations.append(s3)
    # Strip street type suffix (RD/AVE/ST/BLVD/CT/DR/LN/PL/CIR/PKWY)
    s4 = re.sub(
        r"\s+(RD|AVE|AV|ST|BLVD|CT|DR|LN|PL|CIR|PKWY|WAY|TER|TRL|HWY|"
        r"ROAD|AVENUE|STREET|BOULEVARD|COURT|DRIVE|LANE|PLACE|CIRCLE|"
        r"PARKWAY|TERRACE|TRAIL|HIGHWAY)$",
        "", s3 or s2 or s,
    )
    if s4 not in variations:
        variations.append(s4)
    # Just first word as last resort
    first_word = s.split()[0] if s.split() else ""
    if first_word and first_word not in variations:
        variations.append(first_word)

    for variant in variations:
        if not variant:
            continue
        try:
            r = session.post(
                PADCTN_QUICKSEARCH,
                data={
                    "RealEstate": "true",
                    "SelectedSearch": "2",
                    "StreetNumber": number,
                    "SingleSearchCriteria": variant,
                    "AlterCriteria": "False",
                },
                headers={"X-Requested-With": "XMLHttpRequest"},
                timeout=15,
            )
            if r.status_code != 200:
                continue
            m = PADCTN_ACCOUNT_RE.search(r.text)
            if m:
                return int(m.group(1))
        except Exception:
            continue
    return None


def _padctn_card(session: requests.Session, account_id: int) -> Dict[str, Any]:
    """Fetch padctn printable card → sale_date / sale_price / deed_reference."""
    out: Dict[str, Any] = {"source": "padctn"}
    try:
        r = session.get(
            PADCTN_PRINT.format(account_id=account_id),
            timeout=20,
        )
        if r.status_code != 200:
            return out
        soup = BeautifulSoup(r.text, "html.parser")
        for t in soup(["script", "style"]):
            t.decompose()
        text = soup.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text)

        if (m := PADCTN_SALE_DATE_RE.search(text)):
            out["sale_date"] = _date_to_iso(m.group(1))
        if (m := PADCTN_SALE_PRICE_RE.search(text)):
            price = _money_to_float(m.group(1))
            # padctn shows $0 for non-arms-length transfers — treat as missing
            if price and price > 0:
                out["sale_price"] = price
        if (m := PADCTN_DEED_REF_RE.search(text)):
            out["deed_reference"] = m.group(1)
        if (m := PADCTN_APPRAISED_RE.search(text)):
            out["appraised"] = _money_to_float(m.group(1))
        if (m := PADCTN_PARCEL_RE.search(text)):
            out["parcel"] = m.group(1).strip()
    except Exception:
        pass
    return out


def _resolve_davidson(
    session: requests.Session, address: str
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"source": "padctn", "county": "davidson"}
    if not address:
        return out
    account_id = _padctn_account(session, address)
    if not account_id:
        return out
    out["account_id"] = account_id
    out.update(_padctn_card(session, account_id))
    return out


# Rutherford ArcGIS — direct service query for sale data
RUTHERFORD_ARCGIS = (
    "https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/"
    "Parcel_Data/FeatureServer/0/query"
)


def _resolve_rutherford(
    session: requests.Session, address: str
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"source": "rutherford_arcgis", "county": "rutherford"}
    if not address:
        return out
    head = address.split(",")[0].strip().upper()
    try:
        r = session.get(
            RUTHERFORD_ARCGIS,
            params={
                "where": f"FormattedLocation LIKE '%{head}%'",
                "outFields": (
                    "ParcelID,FormattedLocation,SaleDate,SalePrice,"
                    "TotalValue,Grantee,Grantor"
                ),
                "f": "json",
                "resultRecordCount": "1",
            },
            timeout=15,
        )
        if r.status_code != 200:
            return out
        data = r.json()
        feats = data.get("features", []) or []
        if not feats:
            return out
        attrs = feats[0].get("attributes", {}) or {}
        out["parcel"] = attrs.get("ParcelID")
        sd = attrs.get("SaleDate")
        if sd:
            try:
                from datetime import datetime, timezone
                # ArcGIS dates are epoch milliseconds
                dt = datetime.fromtimestamp(int(sd) / 1000, tz=timezone.utc)
                out["sale_date"] = dt.date().isoformat()
            except Exception:
                pass
        if attrs.get("SalePrice"):
            sp = float(attrs["SalePrice"])
            if sp > 0:
                out["sale_price"] = sp
        if attrs.get("TotalValue"):
            out["appraised"] = float(attrs["TotalValue"])
    except Exception:
        pass
    return out


# Williamson Inigo — JSON search, exposes sale history
WILLIAMSON_SEARCH = (
    "https://inigo.williamson-tn.org/property_search/json/search"
)


def _resolve_williamson(
    session: requests.Session, address: str
) -> Dict[str, Any]:
    out: Dict[str, Any] = {"source": "williamson_inigo", "county": "williamson"}
    if not address:
        return out
    head = address.split(",")[0].strip()
    try:
        r = session.get(
            WILLIAMSON_SEARCH,
            params={"property_address": head},
            timeout=15,
        )
        if r.status_code != 200:
            return out
        data = r.json()
        results = data if isinstance(data, list) else data.get("results", [])
        if not results:
            return out
        first = results[0]
        out["parcel"] = first.get("parcel_id") or first.get("parcel")
        # Some Inigo schemas use last_sale_price/date, some use SalePrice
        sp = first.get("last_sale_price") or first.get("sale_price")
        if sp:
            try:
                sp = float(str(sp).replace(",", "").replace("$", ""))
                if sp > 0:
                    out["sale_price"] = sp
            except (ValueError, TypeError):
                pass
        sd = first.get("last_sale_date") or first.get("sale_date")
        if sd:
            out["sale_date"] = _date_to_iso(str(sd))
        if first.get("appraised") or first.get("total_appraisal"):
            out["appraised"] = float(
                first.get("appraised") or first.get("total_appraisal")
            )
    except Exception:
        pass
    return out


# Authoritative TPAD jurisdiction codes (extracted live from the
# assessment.cot.tn.gov/TPAD home dropdown 2026-05-05). The existing
# tpad_enricher_bot.COUNTY_CODES has WRONG codes for many counties —
# do NOT reuse it.
TPAD_JUR_CODES = {
    "sumner": "083",
    "wilson": "095",
    "maury":  "060",
    "robertson": "074",
    "cheatham": "011",
    "dickson": "022",
    "rutherford": "075",
    # Davidson/Montgomery/Williamson are listed in TPAD as "external link"
    # — searching them via TPAD returns 0. Use county-specific resolvers.
}

TPAD_BASE = "https://assessment.cot.tn.gov/TPAD"
TPAD_SEARCH = f"{TPAD_BASE}/Search/GetSearchResults"
TPAD_DETAIL = f"{TPAD_BASE}/Parcel/Details"


def _tpad_search(
    session: requests.Session, jur: str, owner: str, address: str = ""
) -> list:
    payload = {
        "Jur": jur,
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


def _tpad_detail(
    session: requests.Session, parcel_id: str, jur: str, parcel_key: str
) -> Dict[str, Any]:
    from urllib.parse import quote
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

    out: Dict[str, Any] = {}

    # Appraised value
    for pat in (
        r"Total Appraisal[^\d]*\$?([\d,]+)",
        r"Total Appraised Value[^\d]*\$?([\d,]+)",
    ):
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            out["appraised"] = m.group(1)
            break

    # TPAD detail page renders sale history as a table:
    #   "Sale Date Price Book Page Vacant/Improved Type Instrument Qual"
    #   "7/19/2022 $235,000 6001 465 I-IMPROVED Warranty Deed ..."
    # Take the FIRST sale row (most recent) — stripped of leading header.
    sale_table_re = re.compile(
        r"Sale Date Price Book Page[^\n]*?\s+"
        r"(\d{1,2}/\d{1,2}/\d{2,4})\s+"
        r"\$?([\d,]+)",
        re.IGNORECASE,
    )
    m = sale_table_re.search(text)
    if m:
        out["last_sale_date"] = m.group(1)
        out["last_sale_price"] = m.group(2)
    else:
        # Fall back to label-style "Last Sale" / "Sale Price"
        for pat in (
            r"(?:Last Sale Price|Sale Price)[:\s]+\$?([\d,]+)",
        ):
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                out["last_sale_price"] = m.group(1)
                break
        for pat in (
            r"(?:Last Sale Date|Sale Date)[:\s]+(\d{1,2}/\d{1,2}/\d{2,4}|\d{4}-\d{2}-\d{2})",
        ):
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                out["last_sale_date"] = m.group(1)
                break

    return out


def _build_tpad_session() -> requests.Session:
    s = requests.Session()
    s.headers["User-Agent"] = "FALCO-Lead-Research/1.0"
    s.headers["Accept"] = "application/json, text/html"
    s.verify = False
    try:
        s.get(f"{TPAD_BASE}/", timeout=15)
    except Exception:
        pass
    return s


def _normalize_owner_for_tpad(owner: str) -> list:
    """TPAD's owner search is fuzzy. Try several common reorderings."""
    if not owner:
        return []
    cleaned = re.sub(r"[,;]", " ", owner).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    parts = cleaned.split()
    if len(parts) < 2:
        return [cleaned]
    # First Last → "Last First", "First Last", "Last, First"
    return [
        f"{parts[-1]} {parts[0]}",         # LAST FIRST
        cleaned,                             # FIRST LAST
        f"{parts[-1]}, {parts[0]}",         # LAST, FIRST
    ]


def _resolve_tpad(
    session: requests.Session, address: str, county: str, owner: str
) -> Dict[str, Any]:
    """TPAD covers most TN counties (free, statewide). Search by
    owner+jurisdiction, match by address, fetch detail for sale data."""
    out: Dict[str, Any] = {"source": "tpad", "county": county}
    if not owner or not address:
        return out

    jur = TPAD_JUR_CODES.get(county)
    if not jur:
        return out

    sess = _build_tpad_session()

    # Try several owner-name orderings
    candidates = []
    for owner_variant in _normalize_owner_for_tpad(owner):
        candidates = _tpad_search(sess, jur, owner_variant, "")
        if candidates:
            break
    if not candidates:
        return out

    # Pick the candidate whose property address best matches lead's address
    addr_norm = address.split(",")[0].strip().upper()
    addr_number = ""
    m = re.match(r"^(\d+)\s", addr_norm)
    if m:
        addr_number = m.group(1)

    best = None
    for c in candidates:
        cand_addr = (c.get("propertyAddress") or "").upper().strip()
        # TPAD returns "STREET NAME  NUMBER" format with double-space
        if addr_number and addr_number in cand_addr:
            best = c
            break
    if not best:
        best = candidates[0]

    parcel_id = best.get("parcelId")
    parcel_key = best.get("parcelKey")
    if not parcel_id:
        return out

    detail = _tpad_detail(sess, parcel_id, jur, parcel_key or "")

    if detail.get("last_sale_date"):
        out["sale_date"] = _date_to_iso(detail["last_sale_date"])
    if detail.get("last_sale_price"):
        sp = _money_to_float(detail["last_sale_price"])
        if sp and sp > 0:
            out["sale_price"] = sp
    if detail.get("appraised") or detail.get("appraised_alt"):
        ap = _money_to_float(
            detail.get("appraised") or detail.get("appraised_alt")
        )
        if ap:
            out["appraised"] = ap
    # Also use sale data from the search-result row if detail had nothing
    if not out.get("sale_date") and best.get("dateOfSaleShort"):
        out["sale_date"] = _date_to_iso(best["dateOfSaleShort"])
    out["parcel"] = parcel_id
    return out


def resolve(address: str, county: str, owner: Optional[str] = None) -> Dict[str, Any]:
    """Top-level dispatch: returns sale_date / sale_price / deed_ref /
    appraised based on the county's free portal."""
    county = (county or "").lower().replace(" county", "").strip()
    session = requests.Session()
    session.headers.update({"User-Agent": "curl/8.0"})

    if county == "davidson":
        return _resolve_davidson(session, address)
    if county == "rutherford":
        return _resolve_rutherford(session, address)
    if county == "williamson":
        return _resolve_williamson(session, address)
    # Sumner, Wilson, Maury — covered by TPAD (Davidson/Williamson/
    # Montgomery are listed as "external link" on TPAD and return 0).
    if county in TPAD_JUR_CODES:
        return _resolve_tpad(session, address, county, owner or "")
    return {"source": "no_resolver", "county": county}


if __name__ == "__main__":
    # Test via CLI
    import sys
    if len(sys.argv) < 3:
        print("usage: python -m src.bots._assessor_sale_data <county> <address>")
        sys.exit(1)
    county, address = sys.argv[1], " ".join(sys.argv[2:])
    print(resolve(address, county))
