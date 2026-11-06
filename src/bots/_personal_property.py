"""
Picky personal-property lane: which leads are worth an estate, business
liquidation, or seizure auction, judged only by signals the pipeline already
stores. Pure; no network, no DB.

Patrick's bar: high-value only, no household contents. So a lead qualifies
only on a hard signal, never on the distress type alone:

  estate               PROBATE with a TN aircraft on the decedent (FAA overlay)
                       or a home appraised at ESTATE_HOME_MIN+ (affluent
                       household proxy for vehicles, guns, collections)
  business_liquidation BANKRUPTCY by an entity (not chapter 13) or a WARN
                       closure, in a hard-asset industry (equipment, fleet,
                       guns, collectibles, vehicles); WARN_WORKERS_MIN+
                       workers lifts it to A. Size alone never qualifies.
  seizure              delinquent business personal property tax, 2+ years,
                       whose implied equipment value is SEIZURE_VALUE_MIN+
  owner_asset          any other distressed lead whose owner holds a TN
                       aircraft (sell the plane, not the house)

Grade A = strongest, B = qualifies with a caveat. Anything below B is not
tagged at all. Dollar figures are rough, documented estimates, never
appraisals; `signals` says what each one rests on.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

ESTATE_HOME_MIN = 1_000_000
ESTATE_HOME_A = 2_000_000
WARN_WORKERS_MIN = 75             # grade A boost only; size alone never qualifies
SEIZURE_VALUE_MIN = 250_000
SEIZURE_MIN_YEARS = 2              # one year late is a slow payer, not a seizure
SEIZURE_DORMANT_YEARS = 6          # older than this: assets may be long gone
SEIZURE_DORMANT_VALUE_MIN = 500_000

# Delinquent personalty: TN assesses commercial/industrial tangible personal
# property at 30% of appraised value (Tenn. Code Ann. 67-5-801). Combined
# Hamilton County + Chattanooga 2025 rate is ~$3.07 per $100 assessed; 3.5%
# is used to stay conservative for older, higher-rate years. Cumulative owed
# includes interest and penalty (~1.5%/month), so principal is backed out
# with a simple 9%-per-year-of-delinquency haircut before annualizing.
ASSESSMENT_RATIO = 0.30
TAX_RATE = 0.035
INTEREST_PER_YEAR = 0.09

ENTITY_RE = re.compile(r"\b(LLC|L\.?L\.?C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|PLLC|LTD|PARTNERSHIP)\b\.?", re.I)

# Industry -> asset category. Order matters: first match names the category.
CATEGORY_PATTERNS = [
    # Not bare "ARMS" (apartments) or "OUTFITTERS" (clothing).
    ("guns", re.compile(r"\b(FIREARMS?|GUNS?|GUNSMITH|ARMORY|ARMOURY|PAWN|SPORTING GOODS|AMMO|AMMUNITION|TACTICAL)\b", re.I)),
    ("collections", re.compile(r"\b(COINS?|NUMISMATIC|JEWEL(?:ER|ERS|RY)|ANTIQUES?|GALLERY|FINE ART|COLLECTIBLES?|BULLION|PRECIOUS METALS)\b", re.I)),
    # Not bare "AIR" (HVAC companies).
    ("vehicles", re.compile(r"\b(AUTO(?:MOTIVE)?|MOTORS|MOTOR ?SPORTS|CARS?|TRUCK SALES|RV|MARINE|BOATS?|YACHTS?|AVIATION|AIRCRAFT|POWERSPORTS|CYCLES?)\b", re.I)),
    ("heavy_equipment", re.compile(r"\b(CONSTRUCTION|CONTRACTORS?|CONTRACTING|EXCAVAT\w*|GRADING|PAVING|ASPHALT|CONCRETE|"
                                   r"DEMOLITION|CRANES?|EQUIPMENT|EARTHWORK|SITE ?WORK|UTILITIES|PIPELINE|DRILLING|"
                                   r"TRUCKING|TRANSPORT\w*|FREIGHT|HAULING|LOGISTICS|DELIVERY|COURIER|TOWING|WRECKER|FLEET)\b", re.I)),
    ("business_equipment", re.compile(r"\b(MANUFACTUR\w*|MACHIN\w*|FABRICAT\w*|STEEL|METALS?|ALLOYS?|WELDING|FOUNDRY|"
                                      r"INDUSTRIES|INDUSTRIAL|PRINTING|PLASTICS|MILLS?|LUMBER|TIMBER|SAWMILL|"
                                      r"BREW\w*|DISTILL\w*|WINERY|BOTTLING|PACKAGING|ELECTRICAL|MECHANICAL|"
                                      r"FARMS?|DAIRY|LIVESTOCK|NURSERY|GREENHOUSE)\b", re.I)),
]
HARD_ASSET_CATEGORIES = {c for c, _ in CATEGORY_PATTERNS}
# Small service shops: tools and a van, not an auction.
SMALL_SHOP_RE = re.compile(r"\b(REPAIR|DETAIL(?:ING)?|CLEANING|JANITORIAL|LAWN|PRESSURE WASH\w*|STAFFING|"
                           r"CONSULTING|MARKETING|MEDIA|SALON|BARBER)\b", re.I)


def _num(v: Any) -> Optional[float]:
    try:
        n = float(str(v).replace(",", "").replace("$", "")) if v not in (None, "") else None
    except ValueError:
        return None
    return n if n is not None and n >= 0 else None


def category_of(name: Optional[str]) -> Optional[str]:
    if not name or SMALL_SHOP_RE.search(name):
        return None
    for cat, rx in CATEGORY_PATTERNS:
        if name and rx.search(name):
            return cat
    return None


def is_entity(name: Optional[str]) -> bool:
    return bool(name and ENTITY_RE.search(name))


def implied_equipment_value(cumulative_owed: Any, years: Any) -> Optional[float]:
    owed, yrs = _num(cumulative_owed), _num(years)
    if not owed:
        return None
    yrs = max(yrs or 1.0, 1.0)
    principal = owed / (1.0 + INTEREST_PER_YEAR * yrs)
    annual_tax = principal / yrs
    return round(annual_tax / TAX_RATE / ASSESSMENT_RATIO, -3)


def _aircraft(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    a = raw.get("aircraft")
    return a if isinstance(a, list) else []


def _aircraft_signal(planes: List[Dict[str, Any]]) -> str:
    return "; ".join(f"{p.get('n_number')} {p.get('aircraft')} ({p.get('match_type')}, {p.get('confidence')})"
                     for p in planes)


def _contacts(raw: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    notice = raw.get("court_notice")
    if isinstance(notice, dict):
        att = notice.get("attorney") or {}
        if att.get("names"):
            out["attorney"] = {"names": att.get("names"), "address": att.get("address")}
        reps = [r for r in notice.get("representatives") or [] if isinstance(r, dict) and r.get("name")]
        if reps:
            out["representatives"] = [{"name": r.get("name"), "address": r.get("address")} for r in reps]
        if notice.get("docket"):
            out["probate_docket"] = f'{notice.get("county") or ""} {notice.get("docket")}'.strip()
    if raw.get("courtlistener_docket_url"):
        out["bankruptcy_docket"] = raw.get("courtlistener_docket_url")
    if raw.get("docket_number"):
        out["case_number"] = raw.get("docket_number")
    return out


def _result(lane, grade, categories, est_value, signals, raw, caveats=()):
    return {
        "lane": lane,
        "grade": grade,
        "categories": sorted(set(categories)),
        "est_value_min": est_value,
        "signals": list(signals),
        "caveats": list(caveats),
        "contacts": _contacts(raw),
        "policy": "pp-v1",
    }


def classify(lead: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """lead: distress_type, full_name, owner_name_records, property_value, and raw_payload (dict)."""
    raw = lead.get("raw_payload") if isinstance(lead.get("raw_payload"), dict) else {}
    dt = (lead.get("distress_type") or "").upper()
    planes = _aircraft(raw)
    plane_bands = {p.get("value_band") for p in planes}

    tag = (_estate(lead, raw, planes, plane_bands) if dt == "PROBATE"
           else _liquidation(lead, raw, planes, dt) if dt in ("BANKRUPTCY", "BUSINESS_CLOSURE")
           else _seizure(lead, raw) if dt == "TAX_LIEN"
           else None)
    if tag:
        return tag
    # Any distressed owner (an individual bankruptcy, a small tax account, a
    # foreclosure) who holds a TN aircraft: sell the plane, not the house.
    if planes:
        grade = "A" if "A" in plane_bands else "B"
        return _result("owner_asset", grade, ["aircraft"], 300_000 if grade == "A" else 100_000,
                       ["owner's aircraft: " + _aircraft_signal(planes)], raw,
                       ["owner-direct: pitch selling the plane to cure the distress"])
    return None


def _estate(lead, raw, planes, plane_bands):
    home = _num(lead.get("property_value")) or 0
    signals, cats, caveats = [], [], []
    if planes:
        signals.append("decedent's aircraft: " + _aircraft_signal(planes))
        cats.append("aircraft")
    if home >= ESTATE_HOME_MIN:
        signals.append(f"home appraised ${home:,.0f} (affluent-household proxy)")
        cats += ["vehicles", "collections"]
        caveats.append("contents are unseen; home value is a proxy, confirm with the executor")
    if not signals:
        return None
    grade = "A" if ("A" in plane_bands or home >= ESTATE_HOME_A or (planes and home >= ESTATE_HOME_MIN)) else "B"
    est = 300_000 if "A" in plane_bands else 100_000 if planes else None
    return _result("estate", grade, cats, est, signals, raw, caveats)


def _liquidation(lead, raw, planes, dt):
    warn = raw.get("warn") if isinstance(raw.get("warn"), dict) else {}
    name = (warn.get("company") if warn else None) or lead.get("full_name") or raw.get("case_name")         or lead.get("owner_name_records") or ""
    chapter = str(raw.get("chapter") or "").strip()
    if dt == "BANKRUPTCY" and (not is_entity(name) or chapter == "13"):
        return None   # individual debtors belong to the estate/owner lanes, not liquidations
    cat = category_of(name)
    if not cat:
        return None   # national closures move their equipment; only hard-asset businesses qualify
    workers = _num(warn.get("workers")) if warn else None
    signals = [f"{'WARN closure' if warn else 'chapter ' + (chapter or '?') + ' business bankruptcy'}: {name}",
               f"hard-asset industry ({cat.replace('_', ' ')})"]
    caveats = []
    if workers:
        signals.append(f"{workers:.0f} workers on the WARN notice")
    elif dt == "BANKRUPTCY":
        caveats.append("size unverified: check estimated assets on the petition (Official Form 201)")
    grade = "A" if (workers and workers >= WARN_WORKERS_MIN) else "B"
    cats = [cat]
    if planes:
        cats.append("aircraft")
        signals.append("entity aircraft: " + _aircraft_signal(planes))
        grade = "A"
    return _result("business_liquidation", grade, cats, None, signals, raw, caveats)


def _seizure(lead, raw):
    if not str(raw.get("parcel") or "").upper().startswith("PER"):
        return None   # real-property tax liens are the real estate lanes' job
    name = raw.get("owner_name_1") or lead.get("full_name") or ""
    value = implied_equipment_value(raw.get("cumulative_owed"), raw.get("years_delinquent"))
    years = _num(raw.get("years_delinquent")) or 0
    if not value or value < SEIZURE_VALUE_MIN or years < SEIZURE_MIN_YEARS:
        return None
    dormant = years > SEIZURE_DORMANT_YEARS
    if dormant and value < SEIZURE_DORMANT_VALUE_MIN:
        return None
    cat = category_of(name)
    signals = [f"delinquent business personal property: ${_num(raw.get('cumulative_owed')):,.0f} owed over "
               f"{years:.0f} yr(s), implied equipment value ~${value:,.0f}"]
    if cat:
        signals.append(f"hard-asset industry ({cat.replace('_', ' ')})")
    caveats = ["implied value backs out of taxes owed; confirm with the assessor's personal property schedule"]
    if dormant:
        caveats.append(f"delinquent {years:.0f} years: business and equipment may be gone")
    grade = "A" if (cat and not dormant) else "B"
    return _result("seizure", grade, [cat or "business_equipment"], value, signals, raw, caveats)
