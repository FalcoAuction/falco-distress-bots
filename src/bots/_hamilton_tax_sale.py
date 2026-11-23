"""Hamilton County Clerk and Master delinquent tax sale list (free county PDF).

The Clerk and Master publishes one list a year (each May, before the June
sale) and revises it as owners pay: every line carries the docket, the item
number, the street address, the map-group-parcel and the minimum bid, and a
PAID or REMOVED mark once the parcel leaves the sale. That makes it an exact
parcel record of a court tax sale, independent of the Trustee's delinquent
roll.

Pure parsing here; land_review_bot fetches the PDF.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

BASE = "https://www.hamiltontn.gov/Clerkmasterforms/taxsale/"
SOURCE = "hamilton_clerk_master"
NAMESPACE = "hamilton_roll"

_LINE = re.compile(
    r"^(?:(PAID|REMOVED)\s+)?(\d{5})\s+(\d+)\s+(.*?)\s*(\d{3}[A-Z0-9]?)\s+(?:([A-Z]{1,2})\s+)?(\d{3}(?:\.\d+)?)"
    r"(?:\s+([A-Z]\d{3}))?\s+\$([\d,]+\.\d{2})\s*$")
_REVISED = re.compile(r"REVISED\s+(\d{1,2}/\d{1,2}/\d{4})", re.I)
_SALE_DAY = re.compile(r"tax sale auction is on\s+\w+,\s+([A-Z][a-z]+ \d{1,2}, \d{4})", re.I)


def list_url(year: int) -> str:
    return f"{BASE}{year}TaxSale/DELINQUENT%20TAX%20SALE%20LIST%20{year}.pdf"


def info_url(year: int) -> str:
    return f"{BASE}{year}TaxSale/TAX%20SALE%20INFORMATION%20{year}.pdf"


def parse_list(text: str) -> Dict[str, Any]:
    """Entries keyed by map-group-parcel. A unit line (condo suffix such as
    C158) is kept apart from the parent parcel so it never matches it."""
    revised = _REVISED.search(text or "")
    entries: Dict[str, Dict[str, Any]] = {}
    unparsed = 0
    for line in (text or "").splitlines():
        line = " ".join(line.split())
        if not re.match(r"^(?:(?:PAID|REMOVED)\s+)?\d{5}\s+\d+\s", line):
            continue
        m = _LINE.match(line)
        if not m:
            unparsed += 1
            continue
        parcel = "-".join((m[5], m[6] or "", m[7])) + (" " + m[8] if m[8] else "")
        entries[parcel] = {"mark": m[1], "docket": m[2], "item": m[3], "address": m[4].strip() or None,
                           "parcel": parcel, "minimum_bid": float(m[9].replace(",", ""))}
    return {"revised": datetime.strptime(revised[1], "%m/%d/%Y").date().isoformat() if revised else None,
            "entries": entries, "unparsed": unparsed}


def parse_sale_date(text: str, year: int) -> Optional[str]:
    """The sale day printed on the Clerk's notice, only when it is in the
    list's own year (the county has left a prior year's form posted)."""
    m = _SALE_DAY.search(" ".join((text or "").split()))
    if not m:
        return None
    try:
        day = datetime.strptime(m[1], "%B %d, %Y").date()
    except ValueError:
        return None
    return day.isoformat() if day.year == year else None


def sale_events(parsed: Dict[str, Any], year: int, url: str, sale_date: Optional[str], today: date) -> List[Dict[str, Any]]:
    out = []
    for entry in parsed["entries"].values():
        if entry["mark"] == "PAID":
            status, detail = "resolved", "paid_before_sale"
        elif entry["mark"] == "REMOVED":
            status, detail = "resolved", "removed_from_sale"
        elif sale_date and sale_date >= today.isoformat():
            status, detail = "active", None
        else:
            status, detail = "unknown", "past_sale_date" if sale_date or year < today.year else "sale_date_unknown"
        out.append({
            "staging_id": None, "source": SOURCE, "kind": "tax", "identity": [NAMESPACE, "hamilton", entry["parcel"]],
            "link": "printed_parcel", "status": status, "detail": detail,
            "what": (f"Hamilton County Clerk and Master {year} delinquent tax sale list, item {entry['item']}, "
                     f"minimum bid ${entry['minimum_bid']:,.2f}" + (f", marked {entry['mark']}" if entry["mark"] else "")),
            "source_url": url, "case_id": "docket " + entry["docket"], "date": parsed.get("revised"),
            "sale_date": sale_date, "subject": None, "representatives": []})
    return out
