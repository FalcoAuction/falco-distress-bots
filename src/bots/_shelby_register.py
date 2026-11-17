"""
Parse the Shelby County Register of Deeds public index
(https://search.register.shelby.tn.us). Free, no login, no CAPTCHA.

Two page types:
  p2.php       instrument-type search results. One row per instrument:
               Details link, Inst #, Inst Code, Grantor/Debtor, Grantee/
               Secured Party, Date, amounts, street fields (blank for APPT),
               Image. Party cells hold one name per <br>.
  pdetail.php  one instrument: file and instrument dates, doc type, Mort $,
               grantors and grantees (one per line in a read-only
               <textarea>), cross references (hidden instnum + year inputs),
               and parcel, lot, subdivision and street address.

An Appointment of Substitute Trustee (code APPT) names the lender and the
borrower(s) as grantors and the new trustee as grantee, and cross-references
the deed of trust it acts under. The deed of trust's own page carries the
property address, parcel, original loan amount and date.

Pure functions only; no network. The bot wraps these.
"""

from __future__ import annotations

import html as _html
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

BASE = "https://search.register.shelby.tn.us"

# Lender, servicer or trust words. Used only when the deed of trust is not
# reachable; otherwise the deed of trust says who the borrowers are.
_LENDERISH = re.compile(
    r"\b(BANK|BANCORP|MORTGAGE|MTG|LENDING|LENDER|LOANS?|FINANCIAL|FINANCE|FUNDING|SERVIC\w*|CREDIT UNION|"
    r"NATIONAL ASSOCIATION|N\s?A|FSB|FEDERAL|SAVINGS|CAPITAL|ACQUISITION|SECURITIZATION|ASSET|"
    r"HOME LOANS|TRUST CO\w*|FANNIE|FREDDIE|HUD|SECRETARY|VETERANS)\b|\bTR$|\bTRUST$",
    re.I,
)
_TRUSTEE_ROLE = re.compile(r"\s+S?\s?TR$", re.I)   # "MOSS WILLIAM P III S TR", "ORANGE CARLTON W TR"


def _text(node) -> str:
    return re.sub(r"\s+", " ", node.get_text(" ", strip=True)).strip() if node else ""


def _lines(cell) -> List[str]:
    """Names from a results cell, one per <br>."""
    out = []
    for part in cell.get_text("\n").split("\n"):
        part = re.sub(r"\s+", " ", part).strip()
        if part:
            out.append(part)
    return out


def _iso(mdy: str) -> Optional[str]:
    try:
        return datetime.strptime(mdy.strip(), "%m/%d/%Y").date().isoformat()
    except (ValueError, AttributeError):
        return None


def parse_results(page_html: str) -> List[Dict[str, Any]]:
    """Rows of a p2.php instrument-type search. [] if the table is missing."""
    soup = BeautifulSoup(page_html or "", "html.parser")
    table = None
    for t in soup.find_all("table"):
        head = t.find("tr")
        if head and "Inst Code" in _text(head) and "Grantor" in _text(head):
            table = t
            break
    if table is None:
        return []
    rows: List[Dict[str, Any]] = []
    for tr in table.find_all("tr")[1:]:
        cells = tr.find_all("td")
        if len(cells) < 6:
            continue
        inst = _text(cells[1])
        if not re.fullmatch(r"\w+", inst):
            continue
        link = tr.find("a", href=re.compile(r"pdetail\.php"))
        rows.append({
            "instrument": inst,
            "code": _text(cells[2]),
            "grantors": _lines(cells[3]),
            "grantees": _lines(cells[4]),
            "recorded": _iso(_text(cells[5])),
            "detail_url": f"{BASE}/{_html.unescape(link['href'])}" if link else None,
        })
    return rows


def _labeled(text: str, label: str) -> Optional[str]:
    m = re.search(rf"{label}\s*:?\s*([^\n]*)", text)
    return m.group(1).strip() if m and m.group(1).strip() else None


def parse_detail(page_html: str) -> Dict[str, Any]:
    """Fields from a pdetail.php page. Missing fields come back as None."""
    soup = BeautifulSoup(page_html or "", "html.parser")
    text = soup.get_text("\n", strip=True)
    areas = soup.find_all("textarea")
    parties = [[re.sub(r"\s+", " ", ln).strip() for ln in a.get_text().split("\n") if ln.strip()] for a in areas]
    xrefs = []
    for form in soup.find_all("form", action=re.compile(r"pdetail", re.I)):
        inst = form.find("input", attrs={"name": "instnum"})
        year = form.find("input", attrs={"name": "year"})
        if inst and inst.get("value"):
            xrefs.append({"instrument": inst["value"].strip(), "year": (year.get("value") or "").strip() if year else ""})

    def grid(*labels: str) -> Dict[str, Optional[str]]:
        """Header row of silver cells, value row underneath."""
        for t in soup.find_all("table"):
            trs = t.find_all("tr")
            if len(trs) < 2:
                continue
            heads = [_text(c) for c in trs[0].find_all("td")]
            if heads[: len(labels)] == list(labels):
                vals = [_text(c) for c in trs[1].find_all("td")]
                return {lab: (vals[i] or None) if i < len(vals) else None for i, lab in enumerate(heads)}
        return {}

    parcel = grid("Parcel ID", "Lot", "Subdivision")
    street = grid("Street #", "Street Name", "City", "State", "Zip")
    mort = _labeled(text, r"Mort \$")
    try:
        mort_amount = float(mort.replace(",", "")) if mort else None
    except ValueError:
        mort_amount = None
    return {
        "doc_type": _labeled(text, "Doc Type"),
        "file_date": _iso(_labeled(text, "File Date") or ""),
        "instrument_date": _iso(_labeled(text, "Instrument Date") or ""),
        "mortgage_amount": mort_amount or None,
        "grantors": parties[0] if len(parties) > 0 else [],
        "grantees": parties[1] if len(parties) > 1 else [],
        "xrefs": xrefs,
        "parcel_id": parcel.get("Parcel ID"),
        "lot": parcel.get("Lot"),
        "subdivision": parcel.get("Subdivision"),
        "street_number": street.get("Street #"),
        "street_name": street.get("Street Name"),
        "city": street.get("City"),
        "state": street.get("State"),
        "zip": street.get("Zip"),
    }


def detail_url(instrument: str, year: str) -> str:
    return f"{BASE}/pdetail.php?year={year}&instnum={instrument}&db=0&book=**0"


def address_of(detail: Dict[str, Any]) -> Optional[str]:
    """'2958 EAST GLENGARRY RD, MEMPHIS, TN 38128', or None without a street."""
    num, name = detail.get("street_number"), detail.get("street_name")
    if not (num and name) or num.strip("0") == "":
        return None
    tail = ", ".join(p for p in (detail.get("city"), " ".join(p for p in (detail.get("state") or "TN", detail.get("zip")) if p)) if p)
    return f"{num} {name}, {tail}"


def split_parties(appt_grantors: List[str], dot_grantors: Optional[List[str]]) -> Dict[str, List[str]]:
    """Separate borrowers from the lender on an APPT. The deed of trust's
    grantors are the borrowers, so use them when the deed of trust was read;
    otherwise fall back to lender words."""
    if dot_grantors:
        borrowers = [g for g in appt_grantors if g in dot_grantors] or list(dot_grantors)
        lenders = [g for g in appt_grantors if g not in dot_grantors]
        return {"borrowers": borrowers, "lenders": lenders, "basis": "deed_of_trust"}
    lenders = [g for g in appt_grantors if _LENDERISH.search(g)]
    borrowers = [g for g in appt_grantors if g not in lenders]
    return {"borrowers": borrowers, "lenders": lenders, "basis": "lender_words"}


def trustees(names: List[str]) -> List[str]:
    """Substitute trustee names without the trailing 'S TR' role marker."""
    return [_TRUSTEE_ROLE.sub("", n).strip() for n in names]


def deed_of_trust_ref(detail: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """The cross reference that can be looked up: a numeric instrument with a
    year. Old book-style references (e.g. 'LM6983') have no detail page."""
    for x in detail.get("xrefs") or []:
        if re.fullmatch(r"\d{6,}", x.get("instrument") or "") and re.fullmatch(r"\d{4}", x.get("year") or ""):
            return x
    return None


def years_since(iso_day: Optional[str], today: Optional[date] = None) -> Optional[float]:
    if not iso_day:
        return None
    d = date.fromisoformat(iso_day)
    return max(0.0, ((today or date.today()) - d).days / 365.25)
