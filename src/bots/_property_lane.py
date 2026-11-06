"""Which lane a staged lead belongs in: residential, commercial or land,
and whether a commercial/land lead is worth working (a real way to reach
the owner, a live distress signal, not government-owned).

Reads only what the assessor enrichers already stored in raw_payload
(padctn, tpad, shelby_arcgis, rutherford_arcgis, williamson_inigo,
Hamilton's land_use, probate owner matches). No network.

    lane = property_lane(row)   # row has raw_payload, phone, email, ...
    lane["lane"]   -> "residential" | "commercial" | "land" | "unknown"
    lane["grade"]  -> "A" | "B" | "C" | None (None = excluded or residential)

Land-use sources, in the order they are trusted:
  padctn             Davidson descriptions, e.g. "C32 - OFFICE BLDG"
  tpad               statewide class, e.g. "Commercial", "Agricultural"
  shelby_arcgis      one-letter class: R, C, I, E, A
  williamson_inigo   e.g. "108 Commercial", "112 Forest"
  rutherford_arcgis  building type description
  hamilton           numeric Standard Land Use codes (1xx residential,
                     2xx-7xx commercial, 8xx resource land, 9xx vacant)
  owner matches      probate matches written by the probate enricher
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from ._address import is_natural_person
from ._field_confidence import phone_trust

VERSION = "property-lane-2026-11-05"
ACREAGE_LAND = 10.0          # a house on 10+ acres sells as land
COMMERCIAL_MIN_VALUE = 25000
COMMERCIAL_MAX_VALUE = 10_000_000   # bigger is institutional: refer it, don't work it
LAND_MIN_VALUE = 3000

_LAND_RE = re.compile(r"VACANT|FARM|AGRIC|FOREST|GREENBELT|TIMBER|PASTURE|ACREAGE|UNDEVELOPED|\bAGRI\b", re.I)
_RES_RE = re.compile(
    r"SINGLE FAMILY|DUPLEX|TRIPLEX|QUAD|CONDO|ZERO LOT|TOWNHOUSE|TOWNHOME|TOWNHOUSE/CO|MOBILE HOME|"
    r"MANUFACTURED|RESIDENTIAL|^RS$|RURAL COMBO", re.I)
_COM_RE = re.compile(
    r"COMMERCIAL|INDUSTRIAL|OFFICE|RETAIL|STORE|SHOPPING|SHOP\b|WAREHOUSE|RESTAURANT|RESTURANT|AUTO|"
    r"APARTMENT|CHURCH|RELIGIOUS|MANUFACTURING|TERMINAL|MEDICAL|HOTEL|MOTEL|SERVICE STATION|GAS STATION|"
    r"CONVENIENCE|BANK|MORTUARY|FUNERAL|DAY CARE|NURSING|PARKING|STORAGE|THEATER|BOWLING|MARINA|CLUB|"
    r"LODGE|GARAGE|HOSPITAL|CLINIC|SCHOOL", re.I)
_EXEMPT_RE = re.compile(r"EXEMPT|\bCITY\b|\bCOUNTY\b|\bSTATE\b", re.I)

_GOV_OWNER_RE = re.compile(
    r"\b(CITY OF|COUNTY OF|STATE OF|UNITED STATES|SECRETARY OF HOUSING|HOUSING AND URBAN|HOUSING AUTHORITY|"
    r"METRO(POLITAN)? GOV\w*|GOVERNMENT|BOARD OF EDUCATION|DEPARTMENT OF|FEDERAL NATIONAL MORTGAGE|FEDERAL HOME LOAN|"
    r"FANNIE MAE|FREDDIE MAC|LAND BANK|DEVELOPMENT AUTHORITY|UTILITY DISTRICT)\b|\bCOUNTY$|\bHUD\b", re.I)
_RELIGIOUS_OWNER_RE = re.compile(
    r"CHURCH|MINISTR|BAPTIST|METHODIST|PRESBYTERIAN|LUTHERAN|EPISCOPAL|CATHOLIC|DIOCESE|PENTECOSTAL|"
    r"TABERNACLE|TEMPLE|MOSQUE|SYNAGOGUE|CONGREGATION|FELLOWSHIP|ASSEMBLY OF GOD|CHURCH OF CHRIST", re.I)

# Distress that means the owner may have to sell. DEMOLITION is left out on
# purpose: a demolition permit means the owner is already redeveloping, and
# the name and phone on it are usually the contractor's.
STRONG_DISTRESS = {"PRE_FORECLOSURE", "FORECLOSURE", "LIS_PENDENS", "TAX_LIEN", "TAX_DELINQUENT", "PROBATE",
                   "BANKRUPTCY", "BUSINESS_CLOSURE", "REO", "COURT_SALE", "TAX_SALE", "TRUSTEE_NOTICE",
                   "SOT", "NOD"}
WEAK_DISTRESS = {"CODE_VIOLATION", "FIRE_DAMAGE", "VACANT"}
LIVE_DISTRESS = STRONG_DISTRESS | WEAK_DISTRESS
# State + ZIP at the end of a mailing line; Hamilton writes "HIXSON TN37343".
_STATE_ZIP_RE = re.compile(r"(?:^|[\s,])([A-Z]{2})[\s,]*\d{5}(?:-\d{4})?\s*$")


def _d(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _num(v: Any) -> Optional[float]:
    if v in (None, ""):
        return None
    try:
        n = float(str(v).replace(",", "").replace("$", "").strip())
    except ValueError:
        return None
    return n if n > 0 else None


# ── land use ────────────────────────────────────────────────────────────────

def _classify_text(text: str) -> Optional[str]:
    """Lane for a free-text land-use description, or 'exempt', or None."""
    t = text.strip().upper()
    if not t:
        return None
    if re.match(r"^E\d+\s*-", t):           # Davidson exempt codes: state, park, church
        return "exempt"
    if _LAND_RE.search(t):
        return "land"
    if _RES_RE.search(t):
        return "residential"
    if _COM_RE.search(t):
        return "commercial"
    m = re.match(r"^([A-Z])\d", t)           # Davidson code with no keyword, e.g. "C77"
    if m:
        return {"R": "residential", "C": "commercial", "I": "commercial", "E": "exempt",
                "F": "residential", "A": "land"}.get(m.group(1))
    if _EXEMPT_RE.search(t):
        return "exempt"
    return None


def _classify_hamilton(code: str) -> Optional[str]:
    c = code.strip()
    if not c.isdigit() or len(c) != 3:
        return None
    first = c[0]
    if first == "1":
        return "residential"
    if first in "234567":
        return "commercial"
    if first in "89":
        return "land"
    return None


def _classify_shelby(cls: str) -> Optional[str]:
    return {"R": "residential", "C": "commercial", "I": "commercial", "A": "land",
            "E": "exempt"}.get(cls.strip().upper())


def land_use_signals(raw: Dict[str, Any], bot_source: str = "") -> List[Tuple[str, str]]:
    """(source, land-use text) pairs, most trusted first."""
    out: List[Tuple[str, str]] = []

    def add(src: str, v: Any) -> None:
        if v not in (None, "") and str(v).strip():
            out.append((src, str(v).strip()))

    add("padctn", _d(raw.get("padctn")).get("land_use"))
    tpad = _d(raw.get("tpad"))
    add("tpad", tpad.get("class"))
    shel = _d(raw.get("shelby_arcgis"))
    add("shelby", shel.get("class") or shel.get("asmt_class"))
    add("williamson", _d(raw.get("williamson_inigo")).get("property_class"))
    add("rutherford", _d(raw.get("rutherford_arcgis")).get("building_type"))
    if bot_source == "hamilton_tax_delinquent" or str(raw.get("county") or "").lower() == "hamilton":
        add("hamilton", raw.get("land_use"))
    add("padctn_owner_match", _d(raw.get("padctn_owner_match")).get("land_use"))
    om = _d(raw.get("owner_match"))
    if om.get("status") == "matched":
        for p in om.get("parcels") or []:
            add("owner_match", _d(p).get("class"))
    return out


def _classify_signal(src: str, text: str) -> Optional[str]:
    if src == "hamilton":
        return _classify_hamilton(text)
    if src == "shelby" and len(text.strip()) == 1:
        return _classify_shelby(text)
    return _classify_text(text)


def _acres(raw: Dict[str, Any]) -> Optional[float]:
    for v in (_d(raw.get("padctn")).get("land_acres"), _d(raw.get("tpad")).get("acres"),
              _d(raw.get("williamson_inigo")).get("acreage"), _d(raw.get("padctn_owner_match")).get("acres"),
              _d(raw.get("asset_profile")).get("total_acres")):
        n = _num(v)
        if n is not None:
            return n
    return None


def _value(row: Dict[str, Any], raw: Dict[str, Any]) -> Optional[float]:
    for v in (row.get("property_value"), _d(raw.get("padctn")).get("appraised"),
              _d(raw.get("tpad")).get("appraised_value_num"), _d(raw.get("shelby_arcgis")).get("appraised"),
              _d(raw.get("rutherford_arcgis")).get("appraised"), _d(raw.get("williamson_inigo")).get("appraised"),
              _d(raw.get("padctn_owner_match")).get("appraised")):
        n = _num(v)
        if n is not None:
            return n
    return None


# ── owner + contact ─────────────────────────────────────────────────────────

def owner_type(owner: Optional[str], owner_class: Optional[str] = None) -> str:
    name = (owner or "").strip()
    if owner_class == "government" or (name and _GOV_OWNER_RE.search(name)):
        return "government"
    if owner_class == "religious_or_education" or (name and _RELIGIOUS_OWNER_RE.search(name)):
        return "religious"
    if not name:
        return "unknown"
    return "person" if is_natural_person(name) else "entity"


def _mailing(raw: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    """Best owner mailing address and its state, from any assessor record."""
    cands: List[Tuple[Optional[str], Optional[str]]] = []
    for key in ("shelby_arcgis", "rutherford_arcgis"):
        r = _d(raw.get(key))
        if r.get("owner_mailing"):
            line = ", ".join(str(x) for x in (r.get("owner_mailing"), r.get("owner_city"),
                                               " ".join(str(y) for y in (r.get("owner_state"), r.get("owner_zip")) if y))
                             if x)
            cands.append((line, (r.get("owner_state") or "").strip().upper() or None))
    for v in (_d(raw.get("padctn")).get("mailing_address"), _d(raw.get("tpad")).get("mailing_address"),
              _d(raw.get("williamson_inigo")).get("owner_address"), raw.get("mailing_address"),
              _d(raw.get("padctn_owner_match")).get("mailing_address")):
        if v:
            cands.append((str(v), None))
    for line, state in cands:
        line = re.sub(r"\s+", " ", (line or "")).strip(" ,")
        if len(line) < 8 or not (re.search(r"\d", line) or "BOX" in line.upper()):
            continue
        if not state:
            m = _STATE_ZIP_RE.search(line.upper())
            state = m.group(1) if m else None
        return line, state
    return None, None


def _phones(row: Dict[str, Any]) -> List[str]:
    out = []
    for p in [row.get("phone")] + list(row.get("alternate_phones") or []):
        p = p.get("number") if isinstance(p, dict) else p
        digits = re.sub(r"\D", "", str(p or ""))
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        if len(digits) == 10 and digits not in out:
            out.append(digits)
    return out


def contact_path(row: Dict[str, Any], raw: Dict[str, Any]) -> Dict[str, Any]:
    """Ways to reach the owner. A phone counts only when phone_trust says it
    belongs to the owner (skip trace or resolver at 0.75+); notice-body and
    unattributed phones are usually trustees, attorneys or contractors."""
    phones = _phones(row) if phone_trust(row).trusted_for_hard_gate else []
    unverified_phone = bool(row.get("phone")) and not phones
    email = (row.get("email") or "").strip() or None
    if email and "@" not in email:
        email = None
    mailing, state = _mailing(raw)
    tier = "phone" if phones else "email" if email else "mail" if mailing else None
    return {"tier": tier, "phones": phones, "email": email, "mailing": mailing, "mailing_state": state,
            "out_of_state": bool(state and state != "TN"), "unverified_phone": unverified_phone}


# ── lane ────────────────────────────────────────────────────────────────────

def _sale_passed(row: Dict[str, Any]) -> bool:
    s = row.get("trustee_sale_date")
    if not s:
        return False
    try:
        return datetime.fromisoformat(str(s)[:10]).date() < date.today()
    except ValueError:
        return False


def _land_flip(row: Dict[str, Any], contact: Dict[str, Any], acres: Optional[float],
               value: Optional[float]) -> Dict[str, Any]:
    """Signals that a private TN landowner would sell vacant land cheap."""
    signals: List[str] = []
    score = 0
    dt = (row.get("distress_type") or "").upper()
    if contact["out_of_state"]:
        signals.append("out_of_state_owner"); score += 3
    if dt in ("TAX_LIEN", "TAX_DELINQUENT", "TAX_SALE"):
        signals.append("tax_delinquent"); score += 2
    if dt == "PROBATE":
        signals.append("probate"); score += 2
    if dt in ("PRE_FORECLOSURE", "FORECLOSURE", "LIS_PENDENS", "COURT_SALE", "BANKRUPTCY",
              "TRUSTEE_NOTICE", "SOT", "NOD"):
        signals.append(dt.lower()); score += 1
    if acres is not None and acres >= 5:
        signals.append("5_plus_acres"); score += 1
    if value is not None and 10000 <= value <= 250000:
        signals.append("flip_price_band"); score += 1
    return {"score": score, "signals": signals}


def property_lane(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = _d(row.get("raw_payload"))
    pm = _d(row.get("phone_metadata"))
    oc = pm.get("owner_class")
    oc = (oc.get("class") or oc.get("value")) if isinstance(oc, dict) else oc
    owner = row.get("owner_name_records") or row.get("full_name")
    otype = owner_type(owner, oc)

    lane, basis = "unknown", None
    for src, text in land_use_signals(raw, row.get("bot_source") or ""):
        got = _classify_signal(src, text)
        if got:
            lane, basis = got, f"{src}:{text}"
            break
    subtype = None
    if lane == "exempt":
        lane, subtype = ("commercial", "church") if otype == "religious" else ("unknown", "exempt")
    elif lane == "land" and basis and "COMMERCIAL" in basis.upper():
        subtype = "commercial_land"
    elif lane == "commercial" and basis and re.search(r"APARTMENT", basis, re.I):
        subtype = "multifamily"

    acres = _acres(raw)
    if acres is not None and acres >= ACREAGE_LAND and lane in ("residential", "unknown") and subtype != "exempt":
        lane, subtype = "land", "acreage_with_home" if lane == "residential" else "acreage"
        basis = basis or f"acres:{acres}"

    value = _value(row, raw)
    contact = contact_path(row, raw)
    out: Dict[str, Any] = {
        "lane": lane, "subtype": subtype, "basis": basis, "acres": acres, "value": value,
        "owner_type": otype, "contact": contact, "grade": None, "exclude_reason": None,
        "version": VERSION,
    }
    if lane not in ("commercial", "land"):
        return out

    dt = (row.get("distress_type") or "").upper()
    floor = COMMERCIAL_MIN_VALUE if lane == "commercial" else LAND_MIN_VALUE
    if otype == "government":
        out["exclude_reason"] = "government_owner"
    elif dt == "DEMOLITION":
        out["exclude_reason"] = "permit_not_distress"
    elif not dt or dt not in LIVE_DISTRESS:
        out["exclude_reason"] = "no_live_distress"
    elif _sale_passed(row):
        out["exclude_reason"] = "sale_passed"
    elif not contact["tier"]:
        out["exclude_reason"] = "no_contact_path"
    elif value is not None and value < floor:
        out["exclude_reason"] = "below_value_floor"
    elif lane == "commercial" and value is not None and value > COMMERCIAL_MAX_VALUE:
        out["exclude_reason"] = "institutional_size"
    else:
        # A: owner reachable directly, value known, distress that forces a sale.
        # Code violations alone never reach A.
        direct = contact["tier"] in ("phone", "email")
        strong = dt in STRONG_DISTRESS
        out["grade"] = "A" if (direct and value and strong) else "B" if (direct or (value and strong)) else "C"
    if lane == "land":
        out["land_flip"] = _land_flip(row, contact, acres, value)
    return out
