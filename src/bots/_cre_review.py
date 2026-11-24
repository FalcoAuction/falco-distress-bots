"""Commercial real estate review: one pilot_review card per commercial row.

Pure functions (no network, no database). cre_review_bot gathers the county
evidence and calls review_row().

What a commercial card must establish before anyone buys contact data:

  parcel    the exact county parcel, matched by parcel number
  owner     the owner of record on that parcel today, and its type
  signer    a person named in a primary record. An LLC or corporation has no
            signer until the Secretary of State record is read by a person;
            the card then says "path: SOS registered agent lookup" and holds.
  distress  current, and on the BUILDING OWNER's parcel: a trustee sale
            notice, or real property tax unpaid on the current Trustee file
            for two or more years, one past the statutory turnover date (the
            suit docket itself is not read, and the card says so). A tenant's WARN notice, a business closure or
            a tenant's personal property tax account is not owner distress.
  value     the county appraisal, labeled as such
  deadline  the advertised sale date, or the next annual tax sale
  listing   never claimed. LoopNet, Crexi and the MLS forbid scraping, so the
            card always asks for a manual check.

Tenant and equipment-owner records are routed to research_hold with the
equipment owner named apart from the landlord.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

from . import _entity_signer as entity_signer
from ._value_guard import unverified_label
from ._pilot_review import build_review, debt, source
from ._source_access import not_read_text, read_state
from ._property_lane import (COMMERCIAL_MAX_VALUE, COMMERCIAL_MIN_VALUE, _name_set, _num, last_sale,
                             owner_mismatch, owner_type, parcel_id, parcel_owner, value_source)
from ._warn_gate import is_tn_warn

VERSION = "cre-review-1"
REVIEWER = "cre_review_bot"
SOS_URL = "https://sos.tn.gov/businesses/services"
SOS_PATH = f"Signer path: SOS registered agent lookup for the owning entity ({SOS_URL})"
LISTING_CHECK = "Listing status not verified; check LoopNet/Crexi manually"
COMPANY_HOLD = "The owner of record is a company"      # how the signer hold reason for a company owner starts
HAMILTON_TRUSTEE_URL = "https://www.hamiltontn.gov/_downloadsTrusteeDelinquent/CTRUDELQCSV.zip"
HAMILTON_ASSESSOR_URL = "https://www.hamiltontn.gov/_downloadsAssessor/AssessorExportCSV.zip"
HAMILTON_SALE_LIST_URL = ("https://www.hamiltontn.gov/Clerkmasterforms/taxsale/{year}TaxSale/"
                          "DELINQUENT%20TAX%20SALE%20LIST%20{year}.pdf")

# Other lanes own these: land (heirs, absentee) and the church research list.
OTHER_LANE_DISTRESS = {"HEIRS_PROPERTY", "ABSENTEE_LAND", "CHURCH_CLOSURE"}
FORECLOSURE = {"PRE_FORECLOSURE", "FORECLOSURE", "TRUSTEE_NOTICE", "SOT", "NOD"}
TAX = {"TAX_LIEN", "TAX_DELINQUENT", "TAX_SALE"}

_CO_OWNERS = re.compile(r"\b(ET\s*AL|ETAL|ET\s*UX|ETUX|ET\s*VIR|ETVIR)\b", re.I)
_FIDUCIARY = re.compile(r"\b(TR|TRS|TRUSTEE|TRUSTEES|TRUST|LIFE\s+EST(ATE)?|REM|REMAINDER)\b", re.I)
_ESTATE = re.compile(r"\b((?<!REAL )ESTATE|EST\s+OF|HEIRS|DECEASED|DECD)\b", re.I)
_COMPANY = re.compile(r"\b(LIMITED PARTNERSHIP|PARTNERSHIP|PARTNERS|INCORPORATED|GP|LP|LLLP|PLLC|PC|ASSOCIATES|"
                      r"REAL ESTATE|VENTURES?|ENTERPRISES?|"
                      # A business name with no legal suffix ("HIXSON AUTO CENTER", "OM MD PLAZA") is not a person.
                      r"CORPORATION|CORP|COMPANY|CO|INC|LLC|LTD|PLAZA|CENTER|CENTRE|CENTERS|AUTO|AUTOMOTIVE|MOTORS?|GARAGE|"
                      r"TIRES?|TOWING|BODY SHOP|SHOP|SHOPPE|STORE|MARKET|MART|GROCERY|FOODS?|RESTAURANT|CAFE|GRILL|BAR|"
                      r"TAVERN|HOTEL|MOTEL|INN|SUITES|LODGE|APARTMENTS?|VILLAGE|MALL|SQUARE|TOWER|BUILDING|WAREHOUSE|"
                      r"STORAGE|SERVICES?|SALES|SUPPLY|SUPPLIES|EQUIPMENT|CONSTRUCTION|BUILDERS|CONTRACTORS?|ROOFING|"
                      r"PLUMBING|ELECTRIC|PAVING|TRUCKING|TRANSPORT|LOGISTICS|MANUFACTURING|INDUSTRIES|INDUSTRIAL|"
                      r"STEEL|PLATING|MACHINE|CLINIC|MEDICAL|DENTAL|PHARMACY|HEALTH ?CARE|FUNERAL|CHAPEL|ACADEMY|"
                      r"DAY ?CARE|SALON|BARBER|LAUNDRY|CLEANERS|CAR WASH|STUDIO|CLUB|RENTALS?|LEASING|FINANCIAL|"
                      r"INSURANCE|AGENCY|REALTY|HOMES|DEVELOPMENT|DEVELOPERS|MANAGEMENT|GROUP|FUND|CAPITAL|"
                      r"INTERNATIONAL|NATIONAL|AMERICAN?|SOUTHERN|CHATTANOOGA|TENNESSEE|MD|DDS|DBA)\b|\d", re.I)
_CARE_OF = re.compile(r"^\s*(ATTN\b|ATTENTION\b|C/O|%)", re.I)
# The ranked list drops anything under this value (lead_quality_reasons: below_fee_floor).
FEE_FLOOR_VALUE = 150_000
_BANK = re.compile(r"\b(BANK|CREDIT UNION|MORTGAGE|FEDERAL SAVINGS|SAVINGS AND LOAN|N\.?A\.?$)\b", re.I)
# "Owner of Property: The X Trust, Jane Doe, Trustee" in a foreclosure notice.
_NOTICE_TRUSTEE = re.compile(
    r"Owner of Property:\s*[^\n]*?\bTrust\b[^\n,]*,\s*([A-Z][A-Za-z.'\- ]{3,60}?),\s*Trustee\b")
_ROLL_SALE_LINE = re.compile(
    r"^(?P<flag>PAID|REMOVED|SOLD|BANKRUPTCY)?\s*(?P<docket>\d{5})\s+(?P<item>\d+)\s+(?P<rest>.+?)\s+\$(?P<bid>[\d,]+\.\d\d)\s*$")
_MAP_TAIL = re.compile(
    r"(?P<map>\d{3}[A-Z]{0,2})\s+(?:(?P<group>[A-Z]{1,2})\s+)?(?P<parcel>\d{3}(?:\.\d+)?(?:\s+[A-Z]\d{3})?)$")


def _d(v: Any) -> Dict[str, Any]:
    return v if isinstance(v, dict) else {}


def _iso(v: Any) -> Optional[str]:
    """ISO date from the formats the county records carry, or None."""
    s = str(v or "").strip()
    if not s:
        return None
    for fmt, width in (("%Y-%m-%d", 10), ("%Y%m%d", 8), ("%m/%d/%Y", 10), ("%m-%d-%Y", 10)):
        try:
            return datetime.strptime(s[:width], fmt).date().isoformat()
        except ValueError:
            continue
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})", s)
    if m:
        try:
            return date(int(m[3]), int(m[1]), int(m[2])).isoformat()
        except ValueError:
            return None
    return None


def money(v: Any) -> str:
    return f"${float(v):,.0f}"


# ── owner and signer ─────────────────────────────────────────────────────────

def owner_names_as_sourced(names: Any) -> List[str]:
    """Owner names as the county prints them, without care-of and attention
    lines (a mail handler is not an owner)."""
    if isinstance(names, str):
        names = re.split(r"\s+&\s+", names)
    return [str(n).strip() for n in (names or []) if n and str(n).strip() and not _CARE_OF.match(str(n))]


def owner_kind(name: Optional[str], owner_class: Optional[str] = None) -> str:
    """person | co_owners | fiduciary | estate | entity | religious | bank | government | unknown."""
    text = " & ".join(owner_names_as_sourced(name))
    base = owner_type(text, owner_class)
    if base == "person" and _COMPANY.search(text):
        base = "entity"
    if base in ("government", "religious", "unknown"):
        return base
    if _BANK.search(text):
        return "bank"
    if _ESTATE.search(text):
        return "estate"
    if base == "entity":
        return "fiduciary" if re.search(r"\b(TRUST|TRS|TRUSTEES?)\b", text, re.I) else "entity"
    if _FIDUCIARY.search(text):
        return "fiduciary"
    if _CO_OWNERS.search(text):
        return "co_owners"
    return "person"


def notice_trustee(body: Any) -> Optional[str]:
    """The trustee a foreclosure notice names for an owning trust, or None.
    Only the notice's own 'Owner of Property' line counts; nothing is guessed
    from a mailing address or a tenant."""
    m = _NOTICE_TRUSTEE.search(str(body or ""))
    return re.sub(r"\s+", " ", m[1]).strip() if m else None


def notice_parties(body: Any) -> Optional[str]:
    """Other lienholders or interested parties a foreclosure notice lists."""
    m = re.search(r"(?:Other interested parties:|may claim an interest in the above-referenced property:)\s*([^\n]{3,300})",
                  str(body or ""), re.I)
    return re.sub(r"\s+", " ", m[1]).strip(" .;") if m else None


# Keys on a staging row that carry a county owner or mailing line, with the label used to cite them.
_MAIL_KEYS = (("hamilton_assessor", "owner_mailing", "Hamilton County assessor mailing line"),
              ("hamilton_assessor", "owner", "Hamilton County assessor owner line"),
              ("comptroller_parcel", "owner_mailing", "State assessment roll mailing line"),
              ("metro_parcel", "owner_mailing", "Metro Nashville parcel mailing line"),
              ("tpad", "mailing_address", "State assessment (TPAD) mailing line"))


def mail_lines(row: Dict[str, Any], evidence: Optional[Dict[str, Any]] = None) -> List[Tuple[str, Dict[str, Any]]]:
    """(county owner or mailing line, source) pairs held for a row. For a
    Hamilton tax row the current assessor export wins over the staged copy."""
    raw = _d(row.get("raw_payload"))
    out: List[Tuple[str, Dict[str, Any]]] = []
    a = _d(_d(evidence).get("assessor"))
    if a:
        src = source("Hamilton County assessor export", HAMILTON_ASSESSOR_URL, _d(evidence).get("assessor_date"))
        out += [(a[k], src) for k in ("owner_mailing", "owner") if a.get(k)]
    for key, field, label in _MAIL_KEYS:
        if a and key == "hamilton_assessor":
            continue
        line = _d(raw.get(key)).get(field)
        if line:
            out.append((str(line), source(label, _d(raw.get(key)).get("detail_url"), None)))
    if row.get("bot_source") == "hamilton_tax_delinquent":
        for field in ("owner_name_1", "owner_name_2"):
            if raw.get(field):
                out.append((str(raw[field]), source("Hamilton County Trustee delinquent tax file owner line",
                                                    HAMILTON_TRUSTEE_URL, _d(evidence).get("roll_date"))))
    return out


def company_signer(row: Dict[str, Any], owner: Optional[str], notice: Optional[Dict[str, Any]] = None,
                   evidence: Optional[Dict[str, Any]] = None,
                   mailing_index: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """The signer answer for a company owner from what the row already holds:
    the saved notice text (dated by its publication) and the county owner and
    mailing lines. Never a network read."""
    raw = _d(row.get("raw_payload"))
    lines = mail_lines(row, evidence)
    mailing = next((line for line, _ in lines if entity_signer.mailing_key(line)), None)
    texts = [(raw.get("body"), notice)] if raw.get("body") and notice else []
    st = _d(raw.get("structured"))
    return entity_signer.find(
        owner, texts=texts, mail_lines=lines, mailing=mailing, mailing_index=mailing_index,
        # A stored county business license lookup (cre_review_bot fills it; never a network read here).
        licenses=_d(raw.get("signer_lookup")).get("hamilton_county_clerk"),
        addresses=[line for line, _ in lines] + [row.get("property_address")],
        # The lender's side of the notice can never sign for the owner.
        never=[st.get(k) for k in ("substitute_trustee", "original_trustee", "attorney", "lender")])


def mailing_index_for(rows: List[Dict[str, Any]], evidence_for=None) -> Dict[str, Any]:
    """Attention and care-of names per P.O. box across the rows of one run."""
    entries = []
    for row in rows:
        raw = _d(row.get("raw_payload"))
        ev = evidence_for(row) if evidence_for else None
        owner = (_d(_d(ev).get("assessor")).get("owner") or parcel_owner(raw) or row.get("owner_name_records")
                 or row.get("full_name"))
        owner = " & ".join(owner_names_as_sourced(owner)) or owner
        lines = mail_lines(row, ev)
        box = next((line for line, _ in lines if entity_signer.mailing_key(line)), None)
        if not box:
            continue
        for line, src in lines:
            # An attention name printed on the owner line is filed under the row's box.
            entries.append((owner, line if entity_signer.mailing_key(line) else f"{line}, {box}", src))
    return entity_signer.build_mailing_index(entries)


def _signer_decider(answer: Dict[str, Any], owner: Optional[str], county_matched: bool) -> Dict[str, Any]:
    first = answer["signers"][0]
    return {"name": first["name"], "role": f"{first['role']} of {str(owner or '').strip()}, the owner of record",
            "basis": entity_signer.signer_basis(answer), "verified": bool(county_matched), "source": first["source"]}


# ── Hamilton county evidence ─────────────────────────────────────────────────

def parse_sale_list(text: str) -> Dict[str, Dict[str, Any]]:
    """Clerk and Master delinquent tax sale list -> {parcel key: line facts}.
    Lines read 'PAID 11255 1738 1415 PARK AVE 145L K 002.04 $14,757.42'."""
    out: Dict[str, Dict[str, Any]] = {}
    for line in str(text or "").splitlines():
        m = _ROLL_SALE_LINE.match(line.strip())
        if not m:
            continue
        tail = _MAP_TAIL.search(m["rest"])
        if not tail:
            continue
        unit = "".join(tail["parcel"].split())
        key = f"{tail['map']}-{tail['group'] or ''}-{unit}"
        out[key] = {"docket": m["docket"], "item": m["item"], "flag": m["flag"] or "LISTED",
                    "minimum_bid": float(m["bid"].replace(",", ""))}
    return out


def _cents(v: Any) -> float:
    s = str(v or "").strip().lstrip("0")
    try:
        return int(s) / 100.0 if s else 0.0
    except ValueError:
        return 0.0


def roll_index(rows, parcels) -> Dict[str, List[Dict[str, Any]]]:
    """Trustee delinquent file rows for the wanted parcels, one entry per bill."""
    want = set(parcels)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        g = lambda k: r.get(k) if isinstance(r.get(k), str) else ""
        key = f"{g('Map').strip()}-{g('Group').strip()}-{g('Parcel').strip()}"
        if key not in want:
            continue
        year = g("Bill Year").strip()
        if not (year.isdigit() and len(year) == 4):
            continue
        owed = sum(_cents(g(c)) for c in ("Current County Owed", "Current Mun Owed", "Current Stw Owed",
                                           "E and R Pickup Owed"))
        owner = " & ".join(x for x in (g("Owner Name 1").strip(), g("Owner Name 2").strip()) if x)
        out.setdefault(key, []).append({"year": int(year), "owed": round(owed, 2), "memo": g("Memo Date").strip(),
                                        "owner": owner, "mortgage_company": g("Mortgage Company Name").strip()})
    return out


def roll_status(bills: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    """Current real property tax position from the Trustee file.

    turned_over_years are the bill years whose memo date is March of the second
    year after the bill year (2023 bills carry 03/2025), which is when state
    law has the Trustee hand unpaid years to the delinquent tax attorney. That
    is a reading of the file, not of a court docket: cards say the year is
    past the statutory turnover date and that the docket has not been read.
    """
    owed = sorted((b for b in (bills or []) if b["owed"] > 0), key=lambda b: b["year"])
    turned = [b["year"] for b in owed if b["memo"] == f"03{b['year'] + 2}"]
    return {
        "on_roll": bool(owed),
        "balance": round(sum(b["owed"] for b in owed), 2),
        "years": sorted({b["year"] for b in owed}),
        "turned_over_years": sorted(set(turned)),
        "roll_owner": owed[-1]["owner"] if owed else None,
        "mortgage_company": next((b["mortgage_company"] for b in owed if b["mortgage_company"]), None),
    }


# ── cards ────────────────────────────────────────────────────────────────────

def _hold(lane_card: Dict[str, Any], route: str, reason: str, verify: List[str],
          conflicts: Optional[List[str]] = None) -> Dict[str, Any]:
    c = dict(lane_card)
    av = dict(c["availability"])
    av["verify_next"] = list(dict.fromkeys(verify + av.get("verify_next", [])))
    av["conflicts"] = list(conflicts or [])
    c["availability"] = av
    return build_review(route=route, route_reason=reason, **c)


def _under_fee_floor(card: Dict[str, Any], value: float) -> Dict[str, Any]:
    return _hold(card, "research_hold",
                 f"County value {money(value)} is under the {money(FEE_FLOOR_VALUE)} floor the ranked list uses; "
                 "worth a look only alongside the same owner's other parcels.",
                 ["Check whether the same owner has other distressed parcels to sell together"])


def generic_hold_card(row: Dict[str, Any], today: date,
                      mailing_index: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """A graded commercial row whose record type has no parcel-level check yet."""
    raw = _d(row.get("raw_payload"))
    dt = str(row.get("distress_type") or "").upper()
    rec_owner = parcel_owner(raw)
    owner = rec_owner or row.get("owner_name_records") or row.get("full_name")
    kind = owner_kind(owner, None)
    pid = parcel_id(raw)
    vsrc = value_source(row, raw)
    value = _num(row.get("property_value")) if vsrc else None
    basis = f"County appraisal ({vsrc})" if value else None
    if value and unverified_label(row):
        basis = unverified_label(row) + ". " + basis
    src = source(f"{row.get('bot_source')} record", row.get("source_url"), today)
    answer = company_signer(row, owner, mailing_index=mailing_index) if kind == "entity" else None
    check = {"BANKRUPTCY": "Read the petition and schedules: is the debtor the entity that owns this parcel, and is the case open",
             "TAX_LIEN": "Confirm the parcel is still in the county tax suit or on a published sale list",
             "TAX_SALE": "Confirm the parcel is still on the published sale list and the sale date",
             "PROBATE": "Confirm the estate holds this parcel and who the personal representative is",
             }.get(dt, "Confirm a current event on this exact parcel in the county or court record")
    label = dt.replace("_", " ").lower()
    card = build_review(
        lane="cre", route="research_hold", reviewed_by=REVIEWER, county=row.get("county"), today=today,
        route_reason=f"A {label} record on a commercial parcel; the event is not yet confirmed as current on the "
                     "owner's own parcel.",
        owner_names=owner_names_as_sourced(owner),
        event={"what": f"{label.capitalize()} record from {row.get('bot_source')}", "status": "unknown",
               "checked_on": today, "source": src},
        asset={"description": f"Commercial property at {row.get('property_address')}", "parcel": pid,
               "address": row.get("property_address"), "tie": "", "source": src},
        decider={"name": owner, "role": f"owner of record ({kind.replace('_', ' ')})" if rec_owner else "name on the record",
                 "basis": "County parcel record" if rec_owner else "Name on the source record; county owner not matched"},
        economics={"deadline": _iso(row.get("trustee_sale_date")), "value": value, "value_basis": basis,
                   "value_source": src if value else None,
                   "unknowns": ["Whether the event is still active", "Debt and liens"]},
        availability={"evidence": "", "checked_on": today, "first_call_question": "",
                      "verify_next": [check, LISTING_CHECK] + entity_signer.path_lines(answer)
                      + ([SOS_PATH] if kind == "entity" else [])})
    return card, _facts(row, pid, owner, kind, f"unconfirmed: {dt}", value, basis, _iso(row.get("trustee_sale_date")),
                        None, answer)


def _facts(row, parcel, owner, kind, tie, value, basis, deadline, signer, answer=None) -> Dict[str, Any]:
    facts = {"version": VERSION, "parcel": parcel, "owner_entity": owner, "owner_type": kind, "distress_tie": tie,
             "value": value, "value_basis": basis, "deadline": deadline, "signer": signer,
             "listing_status": "not verified", "staging_id": row.get("id")}
    if answer and answer["status"] != "none":
        # Only a company owner with something found carries this, so other rows' facts are unchanged.
        facts["entity_signer"] = answer
    return facts


def _signer_gate(kind: str, owner: Optional[str], signer: Optional[str],
                 answer: Optional[Dict[str, Any]] = None) -> Optional[Tuple[str, str, List[str]]]:
    """(route, reason, verify_next) when the owner type blocks a first trace, else None."""
    if kind == "government":
        return "removed", "The owner of record is a public body, not a private seller.", []
    if kind == "bank":
        return ("professional_relationship", "The owner of record is a lender; the path is its special assets or REO "
                "officer, not an owner trace.", ["Identify the lender's special assets contact"])
    if kind == "religious":
        return ("research_hold", "A congregation or denomination owns the parcel; who can sign is set by its governing "
                "body and is not in the county record.", ["Identify the trustees or denominational body with authority to sell"])
    if kind == "estate":
        return ("research_hold", "The owner of record is an estate or heirs; no personal representative or court "
                "authority is in the record.", ["Find the probate file and the personal representative for the owner of record"])
    if kind == "unknown":
        return "research_hold", "No owner of record is tied to the parcel yet.", ["Match the exact county parcel and owner"]
    if kind in ("entity", "fiduciary") and not signer:
        what = "trust; the trustee is not named in the record" if kind == "fiduciary" else \
            "company; no member, manager or officer is named in a primary record"
        verify = ["Read the recorded deed for the trustee's name"] if kind == "fiduciary" else [SOS_PATH]
        paths = entity_signer.path_lines(answer) if kind == "entity" else []
        reason = f"The owner of record is a {what}, so the signer is unresolved."
        if paths:
            reason += " " + entity_signer.hold_note(answer)
        return "research_hold", reason, paths + verify
    if kind == "co_owners":
        return ("research_hold", "The county lists additional unnamed co-owners (et al, et ux); every owner must sign "
                "and they are not all named.", ["Read the vesting deed for every owner's name"])
    return None


def tenant_card(row: Dict[str, Any], today: date, kind_of: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """A tenant or equipment-owner record: never owner distress."""
    raw = _d(row.get("raw_payload"))
    business = row.get("full_name") or row.get("owner_name_records")
    landlord = parcel_owner(raw)
    if kind_of == "personal_property_tax":
        what = f"Business personal property tax account {raw.get('parcel') or ''} in the name of {business}".strip()
        reason = ("Tenant personal property tax is the business's debt on its equipment, not distress on the "
                  "building owner's parcel.")
    elif kind_of == "closure":
        what = f"Employer closure or layoff notice for {business}"
        reason = "A tenant or employer closure is not a landlord sale; nothing ties it to the building owner's parcel."
    else:
        what = f"Record in the name of {business}, which is not the owner of the parcel at this address"
        reason = ("The name on the record shares nothing with the parcel owner, so it is a tenant, contractor or "
                  "prior owner and not the building owner.")
    src = source(str(row.get("bot_source") or "staging record"), row.get("source_url"), today)
    equipment = f"Equipment owner (separate from the landlord): {business}." if business else ""
    card = build_review(
        lane="cre", route="research_hold", route_reason=reason, reviewed_by=REVIEWER, county=row.get("county"),
        owner_names=None, today=today,
        event={"what": what, "status": "unknown", "checked_on": today, "source": src},
        asset={"description": (f"Business location at {row.get('property_address') or 'unknown address'}. {equipment} "
                               + (f"Building owner per county record: {landlord}." if landlord else
                                  "Building owner not matched.")).strip(),
               "parcel": None, "address": row.get("property_address"),
               "tie": "Address of the business only; no event on the building owner's parcel.", "source": src},
        decider={"name": None, "role": "none identified",
                 "basis": "The business named is a tenant or equipment owner, not the owner of the real estate."},
        economics={"unknowns": ["Whether the building owner has any distress of its own"]},
        availability={"evidence": "", "checked_on": today, "conflicts": [],
                      "first_call_question": "",
                      "verify_next": ["Look for a tax suit, foreclosure notice, receivership or bankruptcy on the "
                                      "building owner's own parcel before treating this as real estate",
                                      "Equipment may suit a personal property auction; route separately"]})
    facts = _facts(row, None, landlord, "tenant_record", f"none: {kind_of}", None, None, None, None)
    facts["equipment_owner"] = business
    return card, facts


def foreclosure_card(row: Dict[str, Any], today: date,
                     mailing_index: Optional[Dict[str, Any]] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw = _d(row.get("raw_payload"))
    st = _d(raw.get("structured"))
    pm = _d(row.get("phone_metadata"))
    county = row.get("county")
    owner_rec = parcel_owner(raw)
    borrower = st.get("borrower") or row.get("owner_name_records") or row.get("full_name")
    owner = owner_rec or borrower
    kind = owner_kind(owner, None)
    pid = parcel_id(raw) or _d(raw.get("extracted")).get("parcel")
    sale = _iso(row.get("trustee_sale_date"))
    pub = _iso(raw.get("publication_date"))
    seen = _iso(_d(pm.get("notice_tracking")).get("last_seen_at"))
    adjourned = "adjourn" in " ".join(str(st.get(k) or "") for k in ("original_trustee", "notice_type")).lower()
    status = "postponed" if adjourned else "active"
    # A notice whose source is no longer read (see _source_access) cannot show
    # a postponement or cancellation. It reads "not read since", never current.
    feed = read_state(row.get("bot_source"), seen, today, 10 ** 6)
    unread = feed["status"] == "stopped"
    if not sale or unread:
        status = "unknown"
    elif sale < today.isoformat():
        status = "resolved"
    notice = source(f"Trustee sale notice, {row.get('bot_source')}", row.get("source_url"), pub or seen)
    vsrc = value_source(row, raw)
    value = _num(row.get("property_value")) if vsrc else None
    basis = f"County appraisal ({vsrc})" if value else None
    if value and unverified_label(row):
        basis = unverified_label(row) + ". " + basis
    tp = _d(raw.get("tpad"))
    if value is None and _num(tp.get("building_value_num")):
        value, vsrc = _num(tp.get("building_value_num")), "tpad"
        basis = "County building value only (TPAD); land and total appraisal not captured"
    county_src = source(f"County parcel record ({vsrc or 'assessor'})", tp.get("detail_url"), today) if (owner_rec or value) else None
    signer = notice_trustee(raw.get("body")) if kind == "fiduciary" else (owner if kind == "person" else None)
    # A company owner: the notice sometimes recites who signed the deed of trust for it.
    answer = company_signer(row, owner, notice, mailing_index=mailing_index) if kind == "entity" else None
    named = answer if answer and answer["status"] == "verified" else None
    if named:
        signer = named["signers"][0]["name"]
    lender = st.get("lender") or None
    trustee = st.get("substitute_trustee") or None
    sold = _iso(last_sale(raw))
    unknowns = ["Payoff: the notice prints no balance", "Other liens not searched",
                "Whether the sale has been postponed or cancelled since the last publication"]
    parties = notice_parties(raw.get("body"))
    if parties:
        unknowns.append(f"The notice names other interested parties ({parties}); their lien amounts are not printed")
    known_debt = []
    principal = _num(_d(raw.get("extracted")).get("original_principal"))
    if principal:
        known_debt.append(debt("mortgage_original", principal, pub, notice, "Original principal in the notice; not a payoff"))
    verify = [LISTING_CHECK,
              f"Confirm the sale is still set with the substitute trustee ({trustee})" if trustee else
              "Confirm the sale is still set with the substitute trustee",
              "Ask the lender or trustee for the payoff"]
    desc = f"Commercial property at {row.get('property_address')}"
    use = _d(raw.get("property_lane")).get("basis")
    if use:
        desc += f" ({re.sub(r'^[0-9]+ +', '', str(use).split(':', 1)[-1].strip()).lower()})"
    tie_bits = []
    if owner_rec and not owner_mismatch(borrower, raw):
        tie_bits.append(f"the county parcel owner ({owner_rec}) is the borrower named in the notice")
    if pid:
        tie_bits.append(f"parcel {pid}")
    role = {"person": "owner of record and borrower", "fiduciary": "trustee of the owning trust",
            }.get(kind, "owner of record (entity)")
    card = dict(
        lane="cre", reviewed_by=REVIEWER, county=county, today=today,
        owner_names=owner_names_as_sourced(borrower or owner_rec),
        event={"what": (f"Trustee foreclosure sale advertised for {sale}" if sale else "Trustee foreclosure notice")
               + (f" by {trustee}" if trustee else "") + (" (adjourned sale date)" if adjourned else "")
               + (f"; last seen in print {seen}" if seen and not unread else "")
               + ("; " + not_read_text("the newspaper notice", feed)[:-1] if unread else ""),
               "status": status, "checked_on": seen or today, "source": notice},
        asset={"description": desc, "parcel": pid, "address": row.get("property_address"),
               "tie": "The notice names this street address; " + " and ".join(tie_bits) if tie_bits else "",
               "source": county_src or notice},
        decider=_signer_decider(named, owner, bool(owner_rec)) if named else
        {"name": signer or (owner if kind in ("entity", "fiduciary") else None), "role": role,
         "basis": (f"The notice's owner line names {signer} as trustee of the owning trust" if kind == "fiduciary" and signer
                   else f"County parcel record lists {owner_rec} as owner; the notice names the same borrower" if owner_rec
                   else "Borrower named in the notice; county owner not matched"),
         "verified": bool(signer and owner_rec), "source": notice if kind == "fiduciary" else (county_src or notice)},
        economics={"deadline": sale, "deadline_kind": "trustee sale", "value": value, "value_basis": basis,
                   "value_is_estimate": False, "value_source": county_src if value else None,
                   "known_debt": known_debt, "unknowns": unknowns + ([f"Lender: {lender}"] if lender and len(lender) < 80 else [])},
        availability={"evidence": (f"No county transfer recorded after the notice (last recorded sale {sold or 'none on file'}); "
                                   "the notice advertises a trustee sale, not a private listing. Listing status not verified."),
                      "checked_on": today, "conflicts": [],
                      "first_call_question": ("Is the trustee sale still going ahead, and would you rather sell at a "
                                              "marketed auction before that date than let the lender take it at the courthouse?"),
                      "verify_next": verify})
    facts = _facts(row, pid, owner, kind, f"trustee sale notice naming the parcel owner; sale {sale}", value, basis, sale,
                   signer, answer)
    if unread:
        return _hold(card, "research_hold",
                     not_read_text("The newspaper that printed this notice", feed)
                     + (f" The sale date {sale} is as last advertised and is not confirmed as current." if sale else "")
                     + (" That date has passed; the outcome is not on file." if sale and sale < today.isoformat() else ""),
                     ["Confirm the sale date with the substitute trustee or the county before any spend"]), facts
    if status == "resolved":
        # A passed date is not proof of a sale: it may have been postponed or cancelled.
        return _hold(card, "research_hold", f"The advertised sale date {sale} has passed; the outcome is not on file.",
                     ["Check the trustee or county record: sold, postponed or cancelled"]), facts
    if not sale:
        return _hold(card, "research_hold", "No sale date is on file for this notice.", ["Find the advertised sale date"]), facts
    if not owner_rec or owner_mismatch(borrower, raw):
        return _hold(card, "research_hold", "The borrower in the notice is not matched to the county owner of the parcel.",
                     ["Match the exact county parcel and its owner of record"],
                     ["Parcel owner and notice borrower differ"] if owner_rec else None), facts
    if not pid:
        return _hold(card, "research_hold", "No county parcel number is tied to the notice.", ["Match the exact county parcel"]), facts
    if sold and pub and sold > pub:
        return _hold(card, "removed", f"The county shows a transfer on {sold}, after the notice.", [],
                     [f"Recorded transfer {sold}"]), facts
    gate = _signer_gate(kind, owner, signer, answer)
    if gate:
        return _hold(card, gate[0], gate[1], gate[2]), facts
    if value is None or not vsrc:
        return _hold(card, "research_hold", "No county appraisal is on file for the parcel.", ["Pull the county appraisal"]), facts
    if value > COMMERCIAL_MAX_VALUE:
        return _hold(card, "professional_relationship", "Institutional size; refer rather than work the owner directly.", []), facts
    if value < COMMERCIAL_MIN_VALUE:
        return _hold(card, "removed", "County value is below the commercial floor.", []), facts
    if value < FEE_FLOOR_VALUE:
        return _under_fee_floor(card, value), facts
    reason = (f"Dated trustee sale on the owner's own parcel, signer named in the notice, county value {money(value)}; "
              f"{(date.fromisoformat(sale) - today).days} days to the sale.")
    return build_review(route="first_trace_review", route_reason=reason, **card), facts


def hamilton_tax_card(row: Dict[str, Any], ev: Dict[str, Any], today: date) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """ev: roll (roll_status), roll_date, assessor (current export record or None), assessor_date,
    duplicate_parcel, sale_list ({parcel: line} from the latest published list), sale_list_year,
    sale_list_url, next_list_published."""
    raw = _d(row.get("raw_payload"))
    parcel = str(raw.get("parcel") or "")
    if parcel.upper().startswith("PER-"):
        return tenant_card(row, today, "personal_property_tax")
    roll = ev.get("roll") or roll_status(None)
    a = _d(ev.get("assessor"))
    roll_owner = roll.get("roll_owner") or row.get("owner_name_records") or row.get("full_name")
    owner = a.get("owner") or roll_owner
    kind = owner_kind(owner, None)
    value = _num(a.get("appraised"))
    basis = "Hamilton County assessor appraisal" if value else None
    roll_src = source("Hamilton County Trustee delinquent tax file", HAMILTON_TRUSTEE_URL, ev.get("roll_date"))
    asr_src = source("Hamilton County assessor export", HAMILTON_ASSESSOR_URL, ev.get("assessor_date"))
    years = roll.get("years") or []
    turned = roll.get("turned_over_years") or []
    span = f"{years[0]} to {years[-1]}" if len(years) > 1 else (str(years[0]) if years else "")
    sold = _iso(a.get("last_sale_date"))
    first_delinquent = f"{years[0] + 1}-03-01" if years else None
    listed = _d(ev.get("sale_list")).get(parcel)
    list_year = ev.get("sale_list_year")
    list_src = source(f"Hamilton County Clerk and Master delinquent tax sale list {list_year}", ev.get("sale_list_url"), today) \
        if list_year else None
    use = a.get("land_use") or a.get("property_type")
    signer = " & ".join(owner_names_as_sourced(owner)) if kind == "person" else None
    company = " & ".join(owner_names_as_sourced(owner)) or owner
    answer = company_signer(row, company, None, ev, ev.get("mailing_index")) if kind == "entity" else None
    named = answer if answer and answer["status"] == "verified" else None
    if named:
        signer = named["signers"][0]["name"]
    known_debt = [debt("delinquent_tax", roll["balance"], ev.get("roll_date"), roll_src,
                       f"Bill years {span}; excludes interest, fees and costs added since the file date. Not a payoff.")] \
        if roll.get("on_roll") else []
    next_list = (list_year or today.year) + 1
    unknowns = [f"Tax suit docket and any scheduled sale not verified; next Hamilton sale list expected May {next_list}",
                "Payoff with the Clerk and Master for any year already in suit",
                "Payments made since the Trustee file date",
                "Mortgage and other liens not searched (Hamilton Register of Deeds is a paid subscription)"]
    past = " and ".join(str(y) for y in turned)
    on_file = (f"Delinquent on the Trustee file dated {ev.get('roll_date') or 'unknown'} for bill years {span}, "
               f"{money(roll['balance'])}") if roll.get("on_roll") else ""
    verify = [LISTING_CHECK,
              "Read the tax suit docket for this parcel at the Hamilton County Clerk and Master tax office",
              "Recheck the Trustee file when it refreshes; a paid parcel drops off"]
    card = dict(
        lane="cre", reviewed_by=REVIEWER, county="hamilton", today=today,
        owner_names=owner_names_as_sourced([raw.get("owner_name_1"), raw.get("owner_name_2")]) or
        owner_names_as_sourced(roll_owner),
        event={"what": (on_file + (f"; year{'s' if len(turned) > 1 else ''} {past} {'are' if len(turned) > 1 else 'is'} past the "
                                   "statutory turnover date; the tax suit docket for this parcel has not been read"
                                   if turned else "; no year is past the statutory turnover date")) if roll.get("on_roll")
               else "Delinquent real property tax no longer on the Trustee file",
               "status": "active" if roll.get("on_roll") else "resolved", "checked_on": today, "source": roll_src},
        asset={"description": f"Commercial parcel at {a.get('property_address') or row.get('property_address')}"
                              + (f" ({str(use).lower()})" if use and use != "?" else ""),
               "parcel": parcel or None, "address": row.get("property_address"),
               "tie": (f"The tax bills are on parcel {parcel}; the assessor export lists the same parcel number and owner"
                       if a else f"The tax bills are on parcel {parcel}"), "source": asr_src if a else roll_src},
        decider=_signer_decider(named, company, bool(a)) if named else
        {"name": signer or (owner if kind in ("entity", "fiduciary", "co_owners", "estate", "religious", "bank") else None),
         "role": "owner of record and taxpayer" if kind == "person" else f"owner of record ({kind.replace('_', ' ')})",
         "basis": (f"Assessor export lists {a.get('owner')} as owner of parcel {parcel}; the Trustee bills the same name"
                   if a else "Taxpayer name on the Trustee file only; assessor parcel not matched"),
         "verified": bool(a) and kind == "person", "source": asr_src if a else roll_src},
        economics={"deadline": None, "deadline_kind": "none scheduled",
                   "value": value, "value_basis": basis, "value_is_estimate": False,
                   "value_source": asr_src if value else None, "known_debt": known_debt, "unknowns": unknowns},
        availability={"evidence": (f"Assessor export shows no transfer since {sold or 'the last recorded sale'}"
                                   + (f"; parcel is not on the {list_year} Clerk and Master sale list" if list_year and not listed else "")
                                   + ". Listing status not verified."),
                      "checked_on": today, "conflicts": [],
                      "first_call_question": (f"The county shows {span} property taxes unpaid on {str(row.get('property_address') or '').split(',')[0]}. "
                                              "Are you planning to bring them current, or would you consider selling the building?"),
                      "verify_next": verify})
    tie = (f"real property tax on the owner's parcel; years {span}; past the statutory turnover date {turned}; docket not read"
           if roll.get("on_roll") else "paid or removed")
    facts = _facts(row, parcel or None, owner, kind, tie, value, basis, None, signer, answer)
    facts.update(roll=roll, roll_date=ev.get("roll_date"), assessor_date=ev.get("assessor_date"),
                 sale_list=listed, sale_list_year=list_year)
    if not roll.get("on_roll"):
        return _hold(card, "removed", "The parcel is no longer on the Trustee's delinquent file: paid or removed.", []), facts
    if not a or ev.get("duplicate_parcel"):
        return _hold(card, "research_hold", "The tax parcel is not matched to exactly one parcel in the current assessor export.",
                     ["Match the tax account to its exact assessor parcel (condominium units and splits carry their own numbers)"],
                     ["Parcel not matched one to one"]), facts
    if _name_set(roll_owner) and _name_set(a.get("owner")) and not (_name_set(roll_owner) & _name_set(a.get("owner"))):
        return _hold(card, "research_hold", "The assessor's current owner differs from the taxpayer billed: the parcel may have changed hands.",
                     ["Read the latest deed and confirm who owes the back taxes"],
                     [f"Assessor owner {a.get('owner')} vs taxpayer {roll_owner}"]), facts
    if sold and first_delinquent and sold >= first_delinquent:
        return _hold(card, "research_hold", f"The county shows a transfer on {sold}, after the taxes went delinquent.",
                     ["Confirm the current owner and whether the taxes were settled at closing"], [f"Recorded transfer {sold}"]), facts
    if listed and listed["flag"] not in ("PAID", "REMOVED"):
        return _hold(card, "research_hold", f"The parcel was on the {list_year} Clerk and Master tax sale list and is not marked paid.",
                     ["Ask the Clerk and Master for the sale result and the redemption deadline"],
                     [f"On the {list_year} tax sale list, docket {listed['docket']}"]), facts
    if not turned:
        return _hold(card, "research_hold", "Only the current year is delinquent; no year is past the statutory turnover date.",
                     ["Recheck the Trustee file after the next March turnover date"]), facts
    if len(years) < 2:
        return _hold(card, "research_hold", "A single delinquent year is on file; no pattern of non-payment yet.",
                     ["Recheck the Trustee file next cycle"]), facts
    gate = _signer_gate(kind, owner, signer, answer)
    if gate:
        return _hold(card, gate[0], gate[1], gate[2]), facts
    if value is None:
        return _hold(card, "research_hold", "No county appraisal is on file for the parcel.", ["Pull the county appraisal"]), facts
    if value > COMMERCIAL_MAX_VALUE:
        return _hold(card, "professional_relationship", "Institutional size; refer rather than work the owner directly.", []), facts
    if value < COMMERCIAL_MIN_VALUE:
        return _hold(card, "removed", "County value is below the commercial floor.", []), facts
    if value < FEE_FLOOR_VALUE:
        return _under_fee_floor(card, value), facts
    reason = (f"Owner of record owes {len(years)} years of real property tax ({money(roll['balance'])}) on a commercial parcel "
              f"appraised at {money(value)}; {len(turned)} year(s) are past the statutory turnover date. The tax suit docket has not been read.")
    return build_review(route="first_trace_review", route_reason=reason, **card), facts


def review_row(row: Dict[str, Any], evidence: Optional[Dict[str, Any]] = None, today: Optional[date] = None,
               mailing_index: Optional[Dict[str, Any]] = None) -> Optional[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """(pilot_review card, cre_review facts) for a commercial-lane staging row,
    or None when the row belongs to another lane or cannot be carded yet."""
    today = today or date.today()
    raw = _d(row.get("raw_payload"))
    lane = _d(raw.get("property_lane"))
    dt = str(row.get("distress_type") or "").upper()
    if lane.get("lane") != "commercial" and not is_tn_warn(row):
        return None
    if dt in OTHER_LANE_DISTRESS or row.get("bot_source") in ("church_closure", "land_lane"):
        return None
    if is_tn_warn(row) or dt == "BUSINESS_CLOSURE":
        return tenant_card(row, today, "closure")
    if str(raw.get("parcel") or "").upper().startswith("PER-") or lane.get("exclude_reason") == "personal_property_tax":
        return tenant_card(row, today, "personal_property_tax")
    if row.get("bot_source") == "hamilton_tax_delinquent":
        if evidence is None:
            return None            # county files unavailable this run: keep the last card
        return hamilton_tax_card(row, {**evidence, "mailing_index": mailing_index}, today)
    if lane.get("exclude_reason") == "owner_mismatch":
        return tenant_card(row, today, "owner_mismatch")
    if dt in FORECLOSURE and (row.get("trustee_sale_date") or lane.get("grade")):
        return foreclosure_card(row, today, mailing_index)
    if lane.get("grade"):
        return generic_hold_card(row, today, mailing_index)
    return None
