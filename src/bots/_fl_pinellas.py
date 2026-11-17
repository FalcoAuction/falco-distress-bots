"""
Pinellas County Clerk daily public data files, translated into the row
shape `_fl_clerk` already parses for Hillsborough, so both counties share
one set of case rules (which case types count, who is an owner).

  https://publicfiles.mypinellasclerk.gov/download/
    CIVIL/NEW_CASE_FILINGS_DAILY/CivilNewFilingsDaily_MM-DD-YYYY.csv
    PROBATE/NEW_ESTATE_CASE_FILINGS_DAILY/EstateNewCaseFilingsDaily_MM-DD-YYYY.csv

Civil files carry one row per party with the party's address, so mortgage
foreclosure defendants come with a mailing address. Probate files carry one
row per case (decedent, petitioner and attorney, no addresses); the DOR roll
enricher finds the decedent's parcel by name.

The Clerk confirmed in writing (GovQA I196030-111526, 2026-11-17) that it
"does not object to the automated downloading of files specifically from
the publicfiles site for normal business purposes"; the site's robots.txt
exists only to keep search engines from indexing it.

Pure functions; no network.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Dict, List, Optional

# "REAL PROP/MTGE FRCL HOMESTEAD RES3 $250,000 OR MORE"
_CIRCUIT_MTG = re.compile(r"^REAL PROP/MTGE FRCL\s+(NON-HOMESTEAD|HOMESTEAD|COMMERCIAL|OTHER REAL PROP)\s*(?:RES)?\d?\s*(.*)$",
                          re.I)
# "REAL PROP - MORTGAGE FORECLOSURE - COUNTY  UP TO $15,000": county court,
# claims up to $50,000; mostly condominium and homeowner association liens,
# the same category Hillsborough files as "CC Real Property/Mortgage Foreclosure".
_COUNTY_MTG = re.compile(r"^REAL PROP\s*-\s*MORTGAGE FORECLOSURE\s*-\s*COUNTY\s*(.*)$", re.I)
_OCCUPANCY = {"HOMESTEAD": "Homestead", "NON-HOMESTEAD": "NonHomestead", "COMMERCIAL": "Commercial",
              # Land and other non-residential parcels: not homestead.
              "OTHER REAL PROP": "NonHomestead"}


def civil_type(pinellas_type: str) -> str:
    """Pinellas case type -> the Hillsborough wording _fl_clerk matches on.
    Anything else is returned unchanged (and ignored downstream)."""
    t = re.sub(r"\s+", " ", pinellas_type or "").strip()
    m = _CIRCUIT_MTG.match(t)
    if m:
        return f"Mortgage Foreclosure - {_OCCUPANCY[m.group(1).upper()]} {m.group(2)}".strip()
    m = _COUNTY_MTG.match(t)
    if m:
        return f"CC Real Property/Mortgage Foreclosure {m.group(1)}".strip()
    if re.search(r"\bPARTITION\b", t, re.I):
        return "Partition"
    return t


def probate_type(pinellas_type: str) -> str:
    t = re.sub(r"\s+", " ", pinellas_type or "").strip()
    if re.match(r"^SUMMARY ADMINISTRATION OVER", t, re.I):
        return "Summary Administration Greater than $1000"
    return t.title()


def iso_day(value: str) -> str:
    v = (value or "").strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(v, fmt).date().isoformat()
        except ValueError:
            continue
    return v


def _join(*parts: Optional[str]) -> str:
    return re.sub(r"\s+", " ", " ".join(p.strip() for p in parts if p and p.strip())).strip()


def civil_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Pinellas civil CSV rows -> _fl_clerk rows."""
    out = []
    for r in rows:
        out.append({
            "CaseNumber": (r.get("Case Number") or "").strip(),
            "CaseTypeDescription": civil_type(r.get("Case Type") or ""),
            "PinellasCaseType": (r.get("Case Type") or "").strip(),
            "Title": (r.get("Title") or "").strip(),
            "FilingDate": iso_day(r.get("Case Create Date") or ""),
            "PartyType": (r.get("Party Type") or "").strip().title(),
            "FirstName": (r.get("Party First Name") or "").strip(),
            "MiddleName": (r.get("Party Middle Name") or "").strip(),
            "LastName/CompanyName": (r.get("Party Last Name/Company Name") or "").strip(),
            "PartyAddress": (r.get("Party Address") or "").strip(),
            "Attorney": _join(r.get("Attorney First Name"), r.get("Attorney Middle Name"), r.get("Attorney Last Name")),
        })
    return out


def _get(r: Dict[str, str], *names: str) -> str:
    """Header spelling varies ("Rep or Petitioner Attorney 's Last Name")."""
    for n in names:
        if n in r:
            return (r.get(n) or "").strip()
    norm = {re.sub(r"[^a-z]", "", k.lower()): k for k in r}
    for n in names:
        k = norm.get(re.sub(r"[^a-z]", "", n.lower()))
        if k:
            return (r.get(k) or "").strip()
    return ""


def probate_rows(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Pinellas estate CSV rows (one per case and representative) -> a
    decedent row plus a petitioner row per case, in _fl_clerk shape."""
    out = []
    seen_decedent = set()
    for r in rows:
        cn = _get(r, "Case Number")
        base = {"CaseNumber": cn, "CaseTypeDescription": probate_type(_get(r, "Case Type")),
                "Title": _get(r, "Title"), "FilingDate": iso_day(_get(r, "Case Create Date"))}
        if cn not in seen_decedent:
            seen_decedent.add(cn)
            out.append({**base, "PartyType": "Decedent",
                        "FirstName": _get(r, "Decedent's First Name"), "MiddleName": _get(r, "Decedent's Middle Name"),
                        "LastName/CompanyName": _get(r, "Decedent's Last Name"),
                        "DateofDeath": iso_day(_get(r, "Decedent's Date of Death")) or "", "PartyAddress": ""})
        rep_type = _get(r, "Rep or Petitioner Type")
        if rep_type:
            out.append({**base, "PartyType": rep_type.title(),
                        "FirstName": _get(r, "Rep or Petitioner First Name"),
                        "MiddleName": _get(r, "Rep or Petitioner's Middle Name"),
                        "LastName/CompanyName": _get(r, "Rep or Petitioner's Last Name"),
                        "PartyAddress": "",
                        "Attorney": _join(_get(r, "Rep or Petitioner Attorney's First Name"),
                                          _get(r, "Rep or Petitioner Attorney's Middle Name"),
                                          _get(r, "Rep or Petitioner Attorney 's Last Name",
                                               "Rep or Petitioner Attorney's Last Name")),
                        "AttorneyFirm": _get(r, "Rep or Petitioner Attorney's Address")})
    return out


PROBATE_HEADER = (
    "Case Category", "Case Type", "Case Number", "Title", "Case Create Date",
    "Decedent's First Name", "Decedent's Middle Name", "Decedent's Last Name",
    "Decedent's Date of Birth", "Decedent's Date of Death", "Rep or Petitioner Type",
    "Rep or Petitioner First Name", "Rep or Petitioner's Middle Name", "Rep or Petitioner's Last Name",
    "Rep or Petitioner Attorney's First Name", "Rep or Petitioner Attorney's Middle Name",
    "Rep or Petitioner Attorney 's Last Name", "Rep or Petitioner Attorney's Address", "Uniform Case Number",
)
_MDY = re.compile(r"^\d{2}/\d{2}/\d{4}$")


def read_probate(text: str) -> List[Dict[str, str]]:
    """The estate file is written without quoting, so a comma inside a title
    ("ESTATE OF JOHN W GRAY, SR") or an attorney address shifts every column
    after it. Rows are rebuilt around two anchors: the case create date that
    follows the title, and the uniform case number that ends the row."""
    out = []
    lines = (text or "").lstrip("﻿").splitlines()
    for line in lines[1:]:
        f = line.split(",")
        if len(f) < len(PROBATE_HEADER):
            continue
        date_idx = next((i for i in range(4, len(f)) if _MDY.match(f[i].strip())), None)
        if date_idx is None:
            continue
        head = f[:3] + [",".join(f[3:date_idx])]
        tail = f[date_idx:]
        if len(tail) < 15:
            continue
        if len(tail) > 15:  # extra commas in the attorney address
            tail = tail[:13] + [",".join(tail[13:-1])] + [tail[-1]]
        out.append({k: v.strip() for k, v in zip(PROBATE_HEADER, head + tail)})
    return out


def file_day(name: str) -> Optional[date]:
    m = re.search(r"_(\d{2})-(\d{2})-(\d{4})\.csv$", name)
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None
