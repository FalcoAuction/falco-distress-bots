"""Land lane: Tennessee tracts with ownership research signals.

Middle Tennessee farm and timber land has sold at auction for generations,
and the parcel roll supplies candidates for independent evidence matching:

  heirs        the owner of record is an estate or heirs ("ESTATE OF",
               "HEIRS", "DECD"): succession and control remain unverified
  family_etal  several co-owners ("ETAL") on land held 20+ years: inherited
               interests may exist; inheritance and partition are unverified
  absentee     an owner living outside Tennessee who has held the land 25+
               years: this records mailing geography, not seller intent

Every tract must be 20+ acres, valued $150,000 to $5,000,000 (FALCO's fee
floor and the ranking's ceiling), farm, forest or residential class, and
owned by people rather than a company, church or government. A recent sale
to several buyers ("ETAL" bought in 2023) is co-buyers, not heirs, so ETAL
alone never qualifies.

Pure functions over a Comptroller parcel row (sqlite3.Row or dict with the
index column names).
"""

from __future__ import annotations

import re
import math
from datetime import date
from typing import Any, Dict, List, Mapping, Optional

MIN_ACRES = 20.0
MIN_VALUE = 150_000
MAX_VALUE = 5_000_000
FAMILY_HOLD_YEARS = 20
ABSENTEE_HOLD_YEARS = 25

HEIRS_RE = re.compile(r"\b(HEIRS?|EST\s+OF|ESTATE\s+OF|DECD|DECEASED|DEC'D)\b|(?<!REAL )(?<!LIFE )\bESTATE\b")
# A life estate means the owner is alive and the land passes on their death:
# a note for the call, not a reason to sell on its own.
LIFE_ESTATE_RE = re.compile(r"\bLIFE\s+ESTATE\b|\(LE\)|\bL/E\b|\bLE\b")
ETAL_RE = re.compile(r"\bET\s?AL\b")
ENTITY_RE = re.compile(
    r"\b(LLC|L L C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|LTD|TRUST|TRUSTEES?|PARTNERS(?:HIP)?|HOLDINGS?|"
    r"PROPERTIES|INVESTMENTS?|CHURCH|MINISTRIES|BANK|ASSOCIATION|ASSN|COUNTY|CITY|STATE|TOWN|"
    r"UNITED STATES|USA|TVA|AUTHORITY|DEVELOPMENT|FARMS INC|CEMETERY|SCHOOL|BOARD)\b")
LAND_CLASSES = ("AGRICULTURAL", "FARM", "FOREST", "RESIDENTIAL")
US_STATES = set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())


def _f(v: Any) -> float:
    try:
        n = float(v or 0)
        return n if math.isfinite(n) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _sale_year(v: Any) -> Optional[int]:
    s = str(v or "").strip()
    if re.fullmatch(r"\d{4}", s):
        return int(s) if int(s) > 1800 and int(s) != 1900 else None
    try:
        parsed = date.fromisoformat(s)
        return parsed.year if parsed.year > 1800 and parsed.year != 1900 else None
    except ValueError:
        return None


def signals(row: Mapping[str, Any], today: date) -> List[str]:
    """Ownership research signals; these do not establish heirs or intent to sell."""
    owner = " ".join(str(row[k] or "") for k in ("owner", "owner2")).upper()
    if not owner.strip() or _f(row["calcac"]) < MIN_ACRES:
        return []
    value = _f(row["appraisal"])
    if not (MIN_VALUE <= value <= MAX_VALUE):
        return []
    if not any(re.search(r"\b" + c + r"\b", str(row["class"] or "").upper()) for c in LAND_CLASSES):
        return []
    # Companies first: "FOX FAMILY REAL ESTATE LLC" is not an estate.
    if ENTITY_RE.search(owner):
        return []
    heirs = bool(HEIRS_RE.search(owner))
    held = None
    year = _sale_year(row["saledate"])
    if year and year <= today.year:
        held = today.year - year
    out: List[str] = []
    if heirs:
        out.append("heirs")
    if ETAL_RE.search(owner) and held is not None and held >= FAMILY_HOLD_YEARS:
        out.append("family_etal")
    state = str(row["state"] or "").strip().upper()
    if state in US_STATES - {"TN"} and held is not None and held >= ABSENTEE_HOLD_YEARS:
        out.append("absentee")
    return out


def person_name(owner: str) -> str:
    """'BATEMAN DANNY RAY ETAL' -> 'Danny Ray Bateman'. Only the first owner
    (text before ETAL / ETUX / & / C/O) is used. Estates and anything that
    does not look like LAST FIRST stay as recorded, title-cased."""
    o = re.split(r"\bET\s?AL\b|\bET\s?UX\b|\bET\s?VIR\b|&|\bC/O\b", (owner or "").upper())[0]
    o = LIFE_ESTATE_RE.sub("", o)
    o = re.sub(r"\s+", " ", o).strip(" ,&()")
    if not o:
        return ""
    if HEIRS_RE.search(o) or ENTITY_RE.search(o):
        return o.title()
    if "," in o:   # Metro style "TANG, TIANLAI" / "WALLER, ROBIN DEWAYNE REVOCABLE TRUST"
        last, _, rest = o.partition(",")
        rest = rest.strip()
        return f"{rest} {last.strip()}".title() if rest and len(last.split()) == 1 else o.title()
    parts = o.split(" ")
    if len(parts) < 2:
        return o.title()
    suffix = parts[-1] if parts[-1] in ("JR", "SR", "II", "III", "IV") else ""
    core = parts[:-1] if suffix else parts
    name = " ".join(core[1:] + core[:1]).title()
    return name + (f" {suffix.title() if suffix in ('JR', 'SR') else suffix}" if suffix else "")


def street_address(raw: Any) -> Optional[str]:
    """The roll writes 'S COMMERCE RD 4185'; mail and maps want '4185 S COMMERCE RD'."""
    a = re.sub(r"\s+", " ", str(raw or "")).strip()
    if not a:
        return None
    m = re.match(r"^(.*\D)\s+(\d+[A-Z]?)$", a)
    return f"{m.group(2)} {m.group(1)}" if m and not re.match(r"^\d", a) else a


def lane_record(row: Mapping[str, Any], sigs: List[str], county: str) -> Dict[str, Any]:
    owner = " ".join(str(row[k] or "") for k in ("owner", "owner2")).upper()
    return {
        "signals": sigs,
        "life_estate": bool(LIFE_ESTATE_RE.search(owner)),
        "acres": _f(row["calcac"]),
        "class": row["class"],
        "land_use": row["landuse"] or None,
        "held_since": str(_sale_year(row["saledate"])) if _sale_year(row["saledate"]) else None,
        "county": county,
        "ownership_signals": list(sigs),
        "owner_of_record": " & ".join(str(row[k] or "").strip() for k in ("owner", "owner2") if row[k]),
        "parcel_id": str(row["parcelid"] or ""),
        "mailing_state": str(row["state"] or "").strip().upper(),
        "reason_to_sell": "unverified",
        "sale_authority": "unverified",
        "title_status": "not_established",
    }
