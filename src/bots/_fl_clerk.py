"""
Parsers for Florida clerk daily new-case files (Hillsborough format:
one CSV row per party, many rows per case). Pure functions, no network.

Kept case types:
  Mortgage Foreclosure (circuit, homestead / non-homestead / commercial,
    with the clerk's value tier)                     -> LIS_PENDENS
  CC Real Property/Mortgage Foreclosure (county court, small dollar;
    mostly HOA and condo association lien foreclosures) -> LIS_PENDENS
  Partition (co-owners or heirs forcing a sale)      -> PARTITION
  Formal / Ancillary / Summary (> $1000) Administration -> PROBATE
Guardianship, caveats, wills on deposit and small-estate summary
administrations are dropped: confidential or no property to sell.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Any, Dict, Iterable, List, Optional

PROBATE_KEEP = re.compile(r"^(Formal Administration|Ancillary Administration|Summary Administration Greater)", re.I)
MORTGAGE = re.compile(r"^Mortgage Foreclosure\s*-\s*(Homestead|NonHomestead|Non-Homestead|Commercial)", re.I)
HOA = re.compile(r"^CC Real Property/Mortgage Foreclosure", re.I)
PARTITION = re.compile(r"^Partition\b", re.I)
TIER = re.compile(r"\$[\d,.]+\s*(?:-|to)\s*\$[\d,.]+|\$[\d,.]+\s+or\s+(?:More|Less)", re.I)

# Defendants that are never the owner: placeholders, tenants, lenders,
# associations, government lienholders.
NOT_OWNER = re.compile(
    r"\b(UNKNOWN|TENANT|JOHN DOE|JANE DOE|SPOUSE|HEIRS|DEVISEES|GRANTEES|ASSIGNEES|LIENORS|CREDITORS|TRUSTEES OF|"
    r"BANK|MORTGAGE|LENDING|LOAN|SAVINGS|CREDIT UNION|FINANCIAL|FUNDING|SERVICING|CAPITAL|HOMEOWNERS|"
    r"ASSOCIATION|ASSN|CONDOMINIUM|HOA|COMMUNITY DEVELOPMENT|CLERK|STATE OF FLORIDA|DEPARTMENT|"
    r"UNITED STATES|SECRETARY OF HOUSING|INTERNAL REVENUE|CITY OF|COUNTY|PORTFOLIO|MIDLAND|CAVALRY|"
    r"DISCOVER|CAPITAL ONE|AMERICAN EXPRESS|SYNCHRONY|JPMORGAN|WELLS FARGO|CITIBANK|NATIONSTAR|FREEDOM)\b",
    re.I)


def read_rows(text: str) -> List[Dict[str, str]]:
    return list(csv.DictReader(io.StringIO(text.lstrip("﻿"))))


def party_name(r: Dict[str, str]) -> str:
    first, mid = (r.get("FirstName") or "").strip(), (r.get("MiddleName") or "").strip()
    last = (r.get("LastName/CompanyName") or "").strip()
    return re.sub(r"\s+", " ", " ".join(x for x in (first, mid, last) if x)).strip()


def _attorney(rows: List[Dict[str, str]], party_type: str) -> Optional[str]:
    for r in rows:
        if (r.get("PartyType") or "").lower() == party_type.lower():
            a = (r.get("Attorney") or "").strip()
            if a and a.lower() != "no attorney":
                return a
    return None


def group_cases(rows: Iterable[Dict[str, str]]) -> Dict[str, List[Dict[str, str]]]:
    cases: Dict[str, List[Dict[str, str]]] = {}
    for r in rows:
        cn = (r.get("CaseNumber") or "").strip()
        if cn:
            cases.setdefault(cn, []).append(r)
    return cases


def civil_case(case_number: str, rows: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    ctype = (rows[0].get("CaseTypeDescription") or "").strip()
    m = MORTGAGE.match(ctype)
    if m:
        kind, dt = "mortgage", "LIS_PENDENS"
        occupancy = m.group(1).lower().replace("-", "")
    elif HOA.match(ctype):
        kind, dt, occupancy = "association_lien", "LIS_PENDENS", None
    elif PARTITION.match(ctype):
        kind, dt, occupancy = "partition", "PARTITION", None
    else:
        return None
    plaintiffs = [party_name(r) for r in rows if (r.get("PartyType") or "").lower() == "plaintiff"]
    defendants = [(party_name(r), (r.get("PartyAddress") or "").strip()) for r in rows
                  if (r.get("PartyType") or "").lower() == "defendant"]
    if kind == "partition":
        # Every co-owner is a seller; plaintiffs are owners too.
        owners = [(party_name(r), (r.get("PartyAddress") or "").strip()) for r in rows
                  if (r.get("PartyType") or "").lower() in ("plaintiff", "defendant")]
    else:
        owners = defendants
    owners = [(n, a) for n, a in owners if n and not NOT_OWNER.search(n)]
    if not owners:
        return None
    tier = TIER.search(ctype)
    fl_addr = next((a for _, a in owners if re.search(r"\bFL\b", a)), None)
    return {
        "case_number": case_number, "case_type": ctype, "kind": kind, "distress_type": dt,
        "occupancy": occupancy, "value_tier": tier.group(0) if tier else None,
        "filed": (rows[0].get("FilingDate") or "").strip(), "title": (rows[0].get("Title") or "").strip(),
        "owners": [n for n, _ in owners], "owner_address": fl_addr or owners[0][1] or None,
        "plaintiff": plaintiffs[0] if plaintiffs else None,
        "plaintiff_attorney": _attorney(rows, "Plaintiff"), "defense_attorney": _attorney(rows, "Defendant"),
    }


def probate_case(case_number: str, rows: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    ctype = (rows[0].get("CaseTypeDescription") or "").strip()
    if not PROBATE_KEEP.match(ctype):
        return None
    dec = next((r for r in rows if (r.get("PartyType") or "").lower() == "decedent"), None)
    if dec is None:
        return None
    pet = next((r for r in rows if (r.get("PartyType") or "").lower() in
                ("petitioner", "personal representative", "trustee")), None)
    return {
        "case_number": case_number, "case_type": ctype, "kind": "probate", "distress_type": "PROBATE",
        "filed": (rows[0].get("FilingDate") or "").strip(),
        "decedent": party_name(dec), "date_of_death": (dec.get("DateofDeath") or "").strip() or None,
        "decedent_address": (dec.get("PartyAddress") or "").strip() or None,
        "petitioner": party_name(pet) if pet else None,
        "petitioner_address": (pet.get("PartyAddress") or "").strip() if pet else None,
        "attorney": _attorney(rows, pet.get("PartyType")) if pet else None,
        "ancillary": ctype.lower().startswith("ancillary"),
        "beneficiaries": sum(1 for r in rows if (r.get("PartyType") or "").lower() in ("beneficiary", "next of kin")),
    }
