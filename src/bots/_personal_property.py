"""
Picky personal-property lane: which leads are worth an estate, business
liquidation, or seizure auction, judged only by signals the pipeline already
stores. Pure; no network, no DB.

Patrick's bar: high-value only, no household contents. So a lead qualifies
only on a hard signal, never on the distress type alone:

  estate               PROBATE with a contents signal at any home value: a TN
                       aircraft (FAA), an ATF firearms license, a big detached
                       garage / barn / shop on the assessor card, or an
                       obituary naming a collection; or, as a weak proxy, a
                       home appraised at ESTATE_HOME_MIN+
  business_liquidation BANKRUPTCY by an entity (not chapter 13) or a WARN
                       closure, in a hard-asset industry (equipment, fleet,
                       guns, collectibles, vehicles); WARN_WORKERS_MIN+
                       workers lifts it to A. Size alone never qualifies.
  seizure              delinquent business personal property tax, 2+ years,
                       whose implied equipment value is SEIZURE_VALUE_MIN+
  owner_asset          any other distressed lead whose owner holds a TN or
                       FL aircraft (sell the plane, not the house)
  vessel_arrest        a boat arrested in federal court (court RSS feeds);
                       A when the caption says yacht and a lender or marina
                       is the plaintiff

Grade A = strongest, B = qualifies with a caveat. Anything below B is not
tagged at all. Dollar figures are rough, documented estimates, never
appraisals; `signals` says what each one rests on.
"""

from __future__ import annotations

import re
import math
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

ESTATE_HOME_MIN = 1_000_000       # proxy only; contents signals qualify an estate at any home value
ESTATE_HOME_A = 2_000_000
OUTBUILDING_GARAGE_MIN = 600       # detached 2+ car garage
OUTBUILDING_BARN_MIN = 1_000
OUTBUILDING_SHOP_MIN = 400
WARN_WORKERS_MIN = 75             # grade A boost only; size alone never qualifies
SEIZURE_VALUE_MIN = 250_000
SEIZURE_MIN_YEARS = 2              # one year late is a slow payer, not a seizure
SEIZURE_DORMANT_YEARS = 6          # older than this: assets may be long gone
SEIZURE_DORMANT_VALUE_MIN = 500_000
# Florida: the DOR NAP roll states business equipment value directly, so
# no back-out from taxes owed is needed.
FL_EQUIPMENT_MIN = 100_000
FL_EQUIPMENT_A = 250_000

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
    ("vehicles", re.compile(r"\b(AUTO(?:MOTIVE)?|MOTORS|MOTOR ?SPORTS|CARS?|TRUCK SALES|RV|MARINE|BOATS?|YACHTS?|AVIATION|AIRCRAFT|JETS?|HELICOPTERS?|AVIONICS|SEAPLANES?|POWERSPORTS|CYCLES?)\b", re.I)),
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
    return n if n is not None and math.isfinite(n) and n >= 0 else None


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
    return [p for p in a if isinstance(p, dict)] if isinstance(a, list) else []


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
    if raw.get("courtlistener_docket_url") or raw.get("docket_url"):
        out["bankruptcy_docket"] = raw.get("courtlistener_docket_url") or raw.get("docket_url")
    if raw.get("docket_number") or raw.get("case_number"):
        out["case_number"] = raw.get("docket_number") or raw.get("case_number")
    vc = raw.get("vessel_case")
    if isinstance(vc, dict) and vc.get("plaintiff"):
        out["plaintiff"] = vc.get("plaintiff")
    vessel = raw.get("vessel")
    if isinstance(vessel, dict):
        if vessel.get("marina"):
            out["marina"] = {"name": vessel["marina"], "address": vessel.get("marina_address")}
        if vessel.get("owner"):
            out["owner"] = vessel["owner"]
    b2b = raw.get("b2b_contacts")
    if isinstance(b2b, dict):
        # Trustee and counsel phones (court_contacts bot): business lines, callable today.
        if isinstance(b2b.get("trustee"), dict):
            out["trustee"] = b2b["trustee"]
        if isinstance(b2b.get("counsel"), list) and b2b["counsel"]:
            out["counsel"] = b2b["counsel"][:4]
    if raw.get("trustee") and "trustee" not in out:
        # Chapter 7 petitions from the court RSS name the panel trustee.
        last, _, first = str(raw["trustee"]).partition(",")
        out["trustee"] = {"name": f"{first.strip()} {last.strip()}".strip().title(), "source": "court_rss"}
    if raw.get("source") in ("hillsclerk_daily", "pinellas_clerk_daily") and raw.get("kind") == "probate":
        if raw.get("attorney") and "attorney" not in out:
            # Pinellas gives the firm and office address in one field.
            out["attorney"] = {"names": [raw["attorney"]], "address": raw.get("attorney_firm")}
        if raw.get("petitioner") and "representatives" not in out:
            out["representatives"] = [{"name": raw["petitioner"], "address": raw.get("petitioner_address")}]
        county = "Pinellas" if raw.get("source") == "pinellas_clerk_daily" else "Hillsborough"
        out.setdefault("probate_docket", f"{county} {raw.get('case_number')}")
    return out


def _fl_equipment(raw: Dict[str, Any]) -> Tuple[float, Optional[Dict[str, Any]]]:
    """Total DOR NAP equipment value on the lead and the largest account."""
    accts = [t for t in raw.get("fl_tpp") or [] if isinstance(t, dict) and _num(t.get("equipment_value"))]
    if not accts:
        return 0.0, None
    top = max(accts, key=lambda t: _num(t.get("equipment_value")) or 0)
    return sum(_num(t.get("equipment_value")) or 0 for t in accts), top


def _equipment_signal(total: float, top: Dict[str, Any]) -> str:
    return (f"business equipment on the FL roll: ${total:,.0f} (NAICS {top.get('naics') or '?'}, "
            f"{top.get('situs') or top.get('county')})")


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

    tag = (_vessel_arrest(raw) if dt == "VESSEL_ARREST"
           else _vessel_lien(raw) if dt == "MARINA_LIEN"
           else _estate(lead, raw, planes, plane_bands) if dt == "PROBATE"
           else _liquidation(lead, raw, planes, dt) if dt in ("BANKRUPTCY", "BUSINESS_CLOSURE")
           else _seizure(lead, raw) if dt == "TAX_LIEN"
           else None)
    if tag:
        return routing(tag, lead, raw, dt)
    if isinstance(raw.get("personal_property_evidence"), dict) and raw["personal_property_evidence"].get("assets"):
        return routing(_result("owner_asset", "B", ["documented_assets"], None,
                               ["Documented personal-property inventory requires review"], raw), lead, raw, dt)
    # Any distressed owner (an individual bankruptcy, a small tax account, a
    # foreclosure) who holds a TN aircraft: sell the plane, not the house.
    if planes:
        grade = "A" if "A" in plane_bands else "B"
        return routing(_result("owner_asset", grade, ["aircraft"], 300_000 if grade == "A" else 100_000,
                               ["owner's aircraft: " + _aircraft_signal(planes)], raw,
                               ["owner-direct: pitch selling the plane to cure the distress"]), lead, raw, dt)
    return None


ROUTE_TO = "McLemore Auction Company"
_VALUE_BASIS = {
    "estate": "home value used as a proxy for the contents (no inventory yet)",
    "owner_asset": "FAA registry value band for the aircraft",
    "vessel_arrest": "unknown until the docket or listing is checked",
    "vessel_lien": "estimated from make, year and size",
}


def routing(tag: Dict[str, Any], lead: Dict[str, Any], raw: Dict[str, Any], dt: str) -> Dict[str, Any]:
    """Who controls the sale, whether FALCO can route it (and to whom), the
    value's basis, the deadline and the one contact to start with."""
    lane = tag["lane"]
    region = "FL" if str(raw.get("state") or "").upper() == "FL" else "TN"
    c = tag.get("contacts") or {}
    if lane == "business_liquidation" and dt == "BANKRUPTCY":
        controller = "chapter 7 trustee"
    else:
        controller = {"estate": "estate representative (through the estate attorney)",
                      "business_liquidation": "the business owner",
                      "seizure": "the business owner (a tax lien holder may force a sale)",
                      "owner_asset": "the owner",
                      "vessel_arrest": "federal court (the US Marshal sells)",
                      "vessel_lien": "the marina"}.get(lane, "unknown")
    routable, why_not = True, None
    if region == "FL":
        routable, why_not = False, "Florida: no auction partner there yet"
    elif dt == "BUSINESS_CLOSURE" and isinstance(raw.get("warn"), dict):
        routable, why_not = False, "Confirm specific surplus assets and the authorized seller"
        controller = "Asset owner or authorized disposition manager unverified"
    elif lane == "vessel_arrest":
        routable, why_not = False, "the court sells it; the plaintiff's counsel is a relationship, not a seller"
    elif lane == "vessel_lien" and not c.get("owner"):
        routable, why_not = False, "the marina runs the sale; call the marina as a consignor relationship"
    if c.get("trustee"):
        t = c["trustee"]
        primary = f"Trustee {t.get('name')}" + (f", {t.get('phone')}" if t.get("phone") else "")
    elif c.get("attorney") and c["attorney"].get("names"):
        primary = f"Attorney {c['attorney']['names'][0]}"
    elif c.get("representatives"):
        primary = f"Representative {c['representatives'][0].get('name')}"
    elif c.get("owner"):
        primary = f"Owner {c['owner']}"
    elif c.get("marina"):
        primary = f"Marina {c['marina'].get('name')}"
    elif c.get("plaintiff"):
        primary = f"Plaintiff {c['plaintiff']}"
    else:
        primary = "Owner of record (mail): " + str(lead.get("owner_name_records") or lead.get("full_name") or "unknown")
    basis = _VALUE_BASIS.get(lane)
    if tag.get("est_value_min") and lane in ("business_liquidation", "seizure"):
        basis = "business equipment on the county tangible personal property roll"
    if not tag.get("est_value_min") and lane not in _VALUE_BASIS:
        basis = "unknown: confirm on the first call"
    if region == "TN" and dt == "BUSINESS_CLOSURE" and isinstance(raw.get("warn"), dict):
        tag["grade"] = "B"
        primary = "Ask the employer who handles surplus assets"
        if not tag.get("est_value_min"):
            basis = "No documented asset value"
        if raw["warn"].get("asset_disposition") == "sold":
            why_not = "Documented equipment sale completed"
    tag.update({
        "region": region, "controller": controller, "routable": routable, "not_routable_reason": why_not,
        "route_to": ROUTE_TO if routable else None, "value_basis": basis,
        "deadline": lead.get("trustee_sale_date") or raw.get("sale_date"),
        "primary_contact": primary, "policy": "pp-v2",
    })
    return qualify_routing(tag, lead, raw)


def _fresh(value: Any) -> bool:
    try:
        return 0 <= (date.today() - date.fromisoformat(str(value))).days <= 7
    except ValueError:
        return False


def qualify_routing(tag, lead, raw):
    """Signals are research candidates. Routing requires a dated asset review.

    personal_property_evidence is an explicit review, never inferred from a
    house value, industry, license, tax roll or FAA registration alone.
    Each conclusion must carry its own source URL and verification date.
    """
    ev = raw.get("personal_property_evidence")
    ev = ev if isinstance(ev, dict) else {}
    holds = [] if tag["routable"] else [tag["not_routable_reason"]]
    subject = str(ev.get("subject_name") or "").strip().casefold()
    if not subject or subject not in {str(lead.get(k) or "").strip().casefold()
                                     for k in ("owner_name_records", "full_name")}:
        holds.append("Asset review subject does not match current lead owner")
    assets = ev.get("assets")
    assets = assets if isinstance(assets, list) else []
    documented = [a for a in assets if isinstance(a, dict) and a.get("asset_id") and a.get("kind")
                  and a.get("description") and a.get("source_url") and _fresh(a.get("verified_on"))]
    if not documented or len(documented) != len(assets):
        holds.append("Specific assets need current documented inventory")
    controller = ev.get("controller")
    controller = controller if isinstance(controller, dict) else {}
    if not (controller.get("name") and controller.get("role") and controller.get("source_url")
            and controller.get("authority") == "verified" and _fresh(controller.get("verified_on"))):
        holds.append("Named controller and authority need current evidence")
    court_controlled = lead.get("distress_type") in ("BANKRUPTCY", "PROBATE", "VESSEL_ARREST")
    for key in (("availability", "liens", "court") if court_controlled else ("availability", "liens")):
        fact = ev.get(key)
        fact = fact if isinstance(fact, dict) else {}
        if not (fact.get("status") in {"availability": ("available",), "liens": ("reviewed", "not_applicable"),
                                       "court": ("permitted", "not_applicable")}[key]
                and fact.get("source_url") and _fresh(fact.get("verified_on"))):
            holds.append(f"{key.title()} unresolved or stale")
    value = ev.get("valuation")
    value = value if isinstance(value, dict) else {}
    deadline = ev.get("deadline")
    deadline = deadline if isinstance(deadline, dict) else {}
    try:
        future = date.fromisoformat(str(deadline.get("date"))) > date.today()
    except ValueError:
        future = False
    if not (deadline.get("source_url") and _fresh(deadline.get("verified_on"))
            and (future or (not deadline.get("date") and deadline.get("status") == "none_documented"))):
        holds.append("Asset sale deadline unresolved or passed")
    # A changed/removed registry match invalidates a previously reviewed plane.
    planes = {p.get("n_number"): p for p in _aircraft(raw)}
    prospects = [p for p in planes.values() if p.get("n_number") and p.get("serial")
                 and p.get("registrant") and p.get("identity_verified") is True
                 and p.get("registration_status") == "V" and p.get("value_band") in ("A", "B")
                 and _fresh(p.get("verified_on")) and _fresh(p.get("registry_date"))]
    for asset in documented:
        if asset.get("kind") != "aircraft":
            continue
        plane = planes.get(asset["asset_id"], {})
        if not (plane.get("serial") == asset.get("serial") and plane.get("serial")
                and plane.get("registrant") == asset.get("registrant")
                and plane.get("identity_verified") is True
                and plane.get("registration_status") == "V"
                and _fresh(plane.get("verified_on")) and _fresh(plane.get("registry_date"))):
            holds.append("Aircraft identity or current registry status requires review")
    qualified = bool(prospects and lead.get("distress_type")) or bool(documented and subject and not any(
        "subject does not match" in h for h in holds))
    tag.update(policy="pp-v3", qualification=("qualified_prospect" if qualified else "research") if holds else "reviewed",
               holds=list(dict.fromkeys(holds)), routable=not holds,
               route_to=ROUTE_TO if not holds else None,
               not_routable_reason="; ".join(dict.fromkeys(holds)) or None,
               title_status="not_established", availability=ev.get("availability"),
               asset_evidence=documented, registry_assets=prospects, authority_review=ev,
               next_action="Resolve referral holds" if qualified else "Identify assets and corroborate owner")
    # Do not represent an unrelated mortgage sale date as the asset's deadline.
    tag["deadline"] = deadline.get("date")
    availability = ev.get("availability") if isinstance(ev.get("availability"), dict) else {}
    if availability.get("status") in ("sold", "unavailable") or (deadline.get("date") and not future):
        tag.update(qualification="hold", next_action="Revalidate disposition or expired asset deadline")
    if not holds:
        tag.update(controller=controller["name"], primary_contact=controller["name"],
                   est_value_min=value.get("amount") or tag.get("est_value_min"),
                   value_basis=value.get("source_url") or tag.get("value_basis"))
    return tag


def dedupe_key(tag: Dict[str, Any], lead: Dict[str, Any], raw: Dict[str, Any]) -> str:
    """Same opportunity filed twice (two newspapers, two notices)."""
    ident = (raw.get("docket_number") or raw.get("case_number") or lead.get("owner_name_records")
             or lead.get("full_name") or "")
    return tag["lane"] + "|" + re.sub(r"[^A-Z0-9]", "", str(ident).upper())


def _vessel_arrest(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """A boat arrested in federal court (court RSS bot). Yachts with a lender or
    marina plaintiff are the cleanest consignments: the lien holder wants the
    boat sold, and a private auction beats the Marshal's sale."""
    vc = raw.get("vessel_case")
    if not isinstance(vc, dict):
        return None
    vessel = vc.get("vessel") or "unnamed vessel"
    kind = {"lender": "lender foreclosure", "service_lien": "marina / yard lien"}.get(vc.get("claim_kind"), "maritime claim")
    signals = [f"federal arrest of {vessel}: {raw.get('case_name') or ''}".strip(),
               f"{kind} brought by {vc.get('plaintiff') or 'unknown plaintiff'} ({raw.get('court_id')} {raw.get('case_number')})"]
    caveats = ["value unknown until the docket or listing is checked; pitch the plaintiff's counsel, not the owner"]
    if not vc.get("yacht"):
        caveats.append("caption does not say yacht: may be a commercial vessel, confirm it is a recreational boat")
    grade = "A" if vc.get("yacht") and vc.get("claim_kind") in ("lender", "service_lien") else "B"
    return _result("vessel_arrest", grade, ["boats"], None, signals, raw, caveats)


def _vessel_lien(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """A boat held for unpaid marina or storage charges (TN newspaper notice,
    tn_boat_notice bot). The owner can still pay or sell before the lien sale;
    a marina running a multi-boat lien auction is itself a consignor."""
    v = raw.get("vessel")
    if not isinstance(v, dict) or not v.get("grade"):
        return None
    if v.get("kind") == "marina_auction":
        signals = [f"{v.get('marina')} is selling boats at a marina lien auction"]
        caveats = ["boats and owners are listed in the full notice; call the marina to consign the auction"]
        return _result("vessel_lien", "B", ["boats"], None, signals, raw, caveats)
    ident = v.get("registration") or v.get("hin")
    desc = v.get("description") or ""
    year = v.get("year") if v.get("year") and str(v.get("year")) not in desc else None
    boat = " ".join(str(x) for x in (year, desc) if x) or "vessel"
    signals = [f"{boat}" + (f" ({ident})" if ident else "") + f" held by {v.get('marina') or 'a recovery party'} "
               "for unpaid charges; lien sale after the notice period"]
    if v.get("owner"):
        signals.append(f"owner of record: {v['owner']}")
    caveats = ["value is estimated from make, year and size; confirm with the owner or marina"]
    if not v.get("owner"):
        caveats.append("owner not named in the notice excerpt: ask the marina")
    return _result("vessel_lien", v["grade"], ["boats"], None, signals, raw, caveats)


def _ffl_people(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    f = raw.get("ffl")
    return [x for x in f if isinstance(x, dict)] if isinstance(f, list) else []


def _ffl_signal(lic: List[Dict[str, Any]]) -> str:
    return "; ".join(f"ATF {x.get('license_label')} {x.get('license_name')} at {x.get('premise')} "
                     f"({x.get('match_type')}, {x.get('listing')})" for x in lic)


def outbuilding_signals(raw: Dict[str, Any]) -> List[Tuple[str, str]]:
    """(category, description) for each assessor outbuilding big enough to hold
    something worth selling. raw_payload.outbuildings: [{type, description, area}]."""
    out = []
    items = raw.get("outbuildings")
    for o in items if isinstance(items, list) else []:
        if not isinstance(o, dict):
            continue
        text = f"{o.get('type') or ''} {o.get('description') or ''}".upper()
        area = _num(o.get("area")) or 0
        label = f"{text.strip()} ({area:,.0f} sq ft)" if area else text.strip()
        if re.search(r"HORSE|STABLE|EQUINE", text):
            out.append(("farm_equipment", label))
        elif re.search(r"GARAGE|CARPORT", text) and re.search(r"DET|DETACHED|SEPARATE", text) and area >= OUTBUILDING_GARAGE_MIN:
            out.append(("vehicles", label))
        elif re.search(r"BARN|POLE|MACHINE|EQUIP|IMPLEMENT", text) and area >= OUTBUILDING_BARN_MIN:
            out.append(("farm_equipment", label))
        elif re.search(r"SHOP|WORKSHOP", text) and area >= OUTBUILDING_SHOP_MIN:
            out.append(("business_equipment", label))
    return out


def obituary_signals(raw: Dict[str, Any]) -> Tuple[List[str], List[str], Optional[str]]:
    """raw_payload.obituary: {categories, phrases, url} written by the obituary bot."""
    ob = raw.get("obituary")
    if not isinstance(ob, dict):
        return [], [], None
    cats = [c for c in ob.get("categories") or [] if isinstance(c, str)]
    return cats, [p for p in ob.get("phrases") or [] if isinstance(p, str)], ob.get("url")


def _estate(lead, raw, planes, plane_bands):
    home = _num(lead.get("property_value")) or 0
    signals, cats, caveats = [], [], []
    strong = 0
    if planes:
        signals.append("decedent's aircraft: " + _aircraft_signal(planes))
        cats.append("aircraft")
        strong += "A" in plane_bands
    lic = _ffl_people(raw)
    if lic:
        signals.append("decedent was a federal firearms licensee: " + _ffl_signal(lic))
        cats.append("guns")
        strong += 1
    sheds = outbuilding_signals(raw)
    if sheds:
        signals.append("assessor outbuildings: " + "; ".join(d for _, d in sheds))
        cats += [c for c, _ in sheds]
    ob_cats, ob_phrases, ob_url = obituary_signals(raw)
    if ob_cats:
        signals.append("obituary mentions: " + "; ".join(ob_phrases[:4]) + (f" ({ob_url})" if ob_url else ""))
        cats += ob_cats
    if home >= ESTATE_HOME_MIN:
        signals.append(f"home appraised ${home:,.0f} (affluent-household proxy)")
        cats += ["vehicles", "collections"]
        caveats.append("home value is a proxy for contents, confirm with the executor")
        strong += home >= ESTATE_HOME_A
    if not signals:
        return None
    if not lic and not planes:
        caveats.append("contents are unseen: ask the executor about vehicles, guns, collections and equipment")
    grade = "A" if (strong or len(signals) >= 2) else "B"
    est = 300_000 if "A" in plane_bands else 100_000 if planes else None
    return _result("estate", grade, cats, est, signals, raw, caveats)


def _liquidation(lead, raw, planes, dt):
    warn = raw.get("warn") if isinstance(raw.get("warn"), dict) else {}
    name = (warn.get("company") if warn else None) or lead.get("full_name") or raw.get("case_name")         or lead.get("owner_name_records") or ""
    chapter = str(raw.get("chapter") or "").strip()
    if dt == "BANKRUPTCY" and (not is_entity(name) or chapter == "13"):
        return None   # individual debtors belong to the estate/owner lanes, not liquidations
    lic = _ffl_people(raw)
    equip, top = _fl_equipment(raw)
    big = equip >= FL_EQUIPMENT_MIN
    cat = "guns" if lic else category_of(name) or ("business_equipment" if big else None)
    if not cat and dt == "BANKRUPTCY" and chapter == "7":
        # A chapter 7 trustee must sell whatever the business owns and hires an
        # auctioneer to do it; the name just does not say what the assets are.
        cat = "business_assets"
    if not cat:
        return None   # national closures move their equipment; only hard-asset businesses qualify
    workers = _num(warn.get("workers")) if warn else None
    event = raw.get("sunbiz_event") if isinstance(raw.get("sunbiz_event"), dict) else None
    head = ("WARN employment notice" if warn else f"{event['description'].lower()} filed {event['filed']}" if event
            else "chapter " + (chapter or "?") + " business bankruptcy")
    signals = [f"{head}: {name}",
               _ffl_signal(lic) if lic else _equipment_signal(equip, top) if big
               else "the chapter 7 trustee will sell whatever the business owns" if cat == "business_assets"
               else f"hard-asset industry ({cat.replace('_', ' ')})"]
    caveats = []
    if event and not big:
        return None   # a dissolution alone says nothing about assets
    if workers:
        signals.append(f"{workers:.0f} workers on the WARN notice")
    elif dt == "BANKRUPTCY":
        caveats.append("size unverified: check estimated assets on the petition (Official Form 201)")
        if cat == "business_assets":
            caveats.append("assets unknown: the trustee's schedules (Form 206A/B) list them")
    grade = "A" if (lic or (workers and workers >= WARN_WORKERS_MIN) or equip >= FL_EQUIPMENT_A) else "B"
    if big and top and top.get("late_filing_penalty"):
        signals.append("late filing penalty on the equipment return (business may already be winding down)")
    cats = [cat]
    if planes:
        cats.append("aircraft")
        signals.append("entity aircraft: " + _aircraft_signal(planes))
        grade = "A"
    return _result("business_liquidation", grade, cats, equip if big else None, signals, raw, caveats)


def _fl_federal_lien(lead, raw):
    """IRS lien filed against a Florida business that the DOR roll shows
    holding $100K+ of equipment: the IRS can levy it, an auction can beat that."""
    equip, top = _fl_equipment(raw)
    if equip < FL_EQUIPMENT_MIN:
        return None
    name = lead.get("full_name") or raw.get("debtor") or ""
    cat = category_of(name) or "business_equipment"
    signals = [f"federal tax lien filed {raw.get('filed') or '?'} against {name}", _equipment_signal(equip, top)]
    if top and top.get("late_filing_penalty"):
        signals.append("late filing penalty on the equipment return")
    caveats = ["lien amount is not in the state file; pull the notice of federal tax lien for the balance"]
    return _result("seizure", "A" if equip >= FL_EQUIPMENT_A else "B", [cat], equip, signals, raw, caveats)


def _seizure(lead, raw):
    if raw.get("source") == "sunbiz_flr":
        return _fl_federal_lien(lead, raw)
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
    lic = _ffl_people(raw)
    cat = "guns" if lic else category_of(name)
    signals = [f"delinquent business personal property: ${_num(raw.get('cumulative_owed')):,.0f} owed over "
               f"{years:.0f} yr(s), implied equipment value ~${value:,.0f}"]
    if lic:
        signals.append(_ffl_signal(lic))
    elif cat:
        signals.append(f"hard-asset industry ({cat.replace('_', ' ')})")
    caveats = ["implied value backs out of taxes owed; confirm with the assessor's personal property schedule"]
    if dormant:
        caveats.append(f"delinquent {years:.0f} years: business and equipment may be gone")
    grade = "A" if (cat and not dormant) else "B"
    return _result("seizure", grade, [cat or "business_equipment"], value, signals, raw, caveats)
