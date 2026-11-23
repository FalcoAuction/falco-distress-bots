"""
Review cards for county Register index rows (see _register_index.py).

A Register listing gives an appointment of substitute trustee as names
only: borrower, lender, trustee. No street, no parcel, no deed of trust
number. So there is no instrument to follow to the property the way the
Shelby path does. What free records can add:

  roll_match()      the borrower names against the Tennessee Comptroller's
                    county assessment roll (86 counties, the index
                    comptroller_refresh keeps). The existing debtor matcher
                    (_bk_match.match_debtors) does the matching.
  link_notice()     a sale notice staged later for the same borrower in the
                    same county, and the days between the recording and the
                    first advertisement.
  appointment_card()
  lis_pendens_card()

The bar, which this module never lowers:

  - Owner and parcel count as confirmed only when both borrowers are named
    on one parcel, or one borrower matches first name, last name and middle
    initial, and in both cases it is the only parcel, nobody else by that
    first and last name owns in the county, and the roll lists no owner who
    is not on the appointment. Or when a linked notice names the street and
    the roll owner of that street is the borrower.
  - Anything less is "owner match by name only, N parcels" and holds.
  - A sale notice links on the borrower's name plus a second fact (the same
    substitute trustee, or the street of the borrower's roll parcel). A
    shared name alone is a candidate, shown but not linked.
  - No advertised sale date: "Early filing" research hold, never first trace.
  - The lender and the trustee are never the owner or the decider.
  - A lis pendens whose listing does not say who sued whom is a research
    hold with the owner not established.

review_register_index() is the one function with I/O. It is called from
preforeclosure_review_bot inside its own pass, shares its write batch and
its card merge rule, and makes no network request.
"""

from __future__ import annotations

import json
import re
import sqlite3
from datetime import date, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._bk_match import _names, _owner_key, _rows_for_last, match_debtors, person
from ._comptroller import ParcelIndex, county_key, street_key_from_lead
from ._pilot_review import build_review, source
from ._preforeclosure_review import (APPOINTMENT_QUIET_DAYS, FIRMS, LANE, LINK_KEY, REVIEWER, SALE_UNKNOWNS,
                                     VALUE_BASIS, _day, _money, lead_time, split_names)
from ._register_index import APPOINTMENT, LIS_PENDENS, canonical_county, is_institution, register_sources

TABLE = "homeowner_requests_staging"
ROLL_KEY = "register_roll_evidence"
ROW_COLUMNS = ("id,bot_source,county,distress_type,pipeline_lead_key,property_address,source_url,staging_status,"
               "staged_at,raw_payload")
# Sources that stage foreclosure sale notices outside Shelby County.
NOTICE_SOURCES = ("tn_public_notice", "nashville_ledger", "main_street_media", "mackie_wolf_trustee",
                  "brock_scott_trustee")
NOTICE_COLUMNS = (
    "id,bot_source,county,property_address,full_name,trustee_sale_date,source_url,staged_at,staging_status,"
    "borrowers:raw_payload->borrowers,publication_date:raw_payload->>publication_date,"
    "report_date:raw_payload->>report_date,trustee:raw_payload->>trustee,"
    "attorney:raw_payload->structured->>attorney,sub_trustee:raw_payload->structured->>substitute_trustee,"
    "excerpt:raw_payload->>excerpt,notice_ref:raw_payload->structured->>tdn_no,file_no:raw_payload->>file_no"
)
# A notice first advertised this long before the recording can still be the same loan.
NOTICE_LOOKBEHIND_DAYS = 60

CONFIRMED = "confirmed"
ROLL_HOLD = {
    "name_only": "Owner match by name only, {n} parcel{s}",
    "multiple_parcels": "Owner match by name only, {n} parcels",
    "ambiguous": "Owner match by name only, {n} parcels",
    "common_name": "Owner match by name only, {n} parcels",
}


# ── Names ───────────────────────────────────────────────────────────────────


def index_name(name: Any) -> str:
    """A Register index prints people surname first with no comma. The
    matcher reads 'SURNAME, GIVEN M'."""
    text = re.sub(r"\s+", " ", str(name or "")).strip()
    words = text.split()
    return f"{words[0]}, {' '.join(words[1:])}" if len(words) >= 2 and "," not in text else text


def borrower_people(borrowers: Iterable[Any]) -> List[Tuple[str, Tuple[str, str, Optional[str]]]]:
    """(name as indexed, (first, last, middle initial)) for each borrower
    that reads as one person. Institutions never get this far."""
    out = []
    for name in borrowers or []:
        if is_institution(name):
            continue
        who = person(index_name(name))
        if who:
            out.append((str(name), who))
    return out


_SEGMENT = re.compile(r"\s*(?:&|\bAND\b|\bETUX\b|\bET UX\b|\bETVIR\b|\bET VIR\b)\s*")
_ROLL_NOISE = {"WIFE", "HUSBAND", "HIS", "HER", "ETAL", "ET", "AL", "LE", "JR", "SR", "II", "III", "IV", "MR", "MRS",
               "TRUSTEE", "TR", "TRS"}


def roll_owner_check(owner_text: Any, people: List[Tuple[str, Tuple[str, str, Optional[str]]]]) -> Dict[str, Any]:
    """Compare the roll's owner line with the borrowers: who is on the roll
    but not on the appointment, and which borrower the roll does not name."""
    text = re.sub(r"%.*$", "", str(owner_text or "").upper())          # "%NAME" is a care-of line
    et_al = bool(re.search(r"\bET\s?AL\b", text))
    firsts = {who[0] for _, who in people}
    lasts = {who[1] for _, who in people}
    found: set = set()
    extra: List[str] = []
    for segment in _SEGMENT.split(text):
        words = [w for w in re.sub(r"[^A-Z ]", " ", segment).split() if w not in _ROLL_NOISE]
        given = [w for w in words if w not in lasts]
        if not given:
            continue
        hit = firsts & set(given)
        if hit:
            found |= hit
        else:
            extra.append(" ".join(words))
    return {"extra_owners": extra, "et_al": et_al,
            "missing_borrowers": [name for name, who in people if who[0] not in found]}


# ── The county roll ─────────────────────────────────────────────────────────


class Roll:
    """The Comptroller index plus, per county, the day the state posted the
    file it was built from."""

    def __init__(self, index: ParcelIndex, posted: Dict[str, str], built: str, urls: Dict[str, str]):
        self.index, self.posted, self.built, self.urls = index, posted, built, urls

    @property
    def con(self) -> sqlite3.Connection:
        return self.index.con

    def dated(self, county: Any) -> str:
        return self.posted.get(county_key(county) or "", self.built)


def open_roll(db_path: Any = None, state_path: Any = None) -> Optional[Roll]:
    """The local Comptroller index, or None when this machine has none."""
    from .comptroller_refresh import BASE, COUNTIES, DB_PATH, STATE_PATH

    path = Path(db_path or DB_PATH)
    if not path.exists():
        return None
    posted: Dict[str, str] = {}
    try:
        state = json.loads(Path(state_path or STATE_PATH).read_text(encoding="utf-8"))
        for name, stamp in (state.items() if isinstance(state, dict) else []):
            try:
                posted[county_key(name)] = parsedate_to_datetime(stamp).date().isoformat()
            except (TypeError, ValueError):
                continue
    except (OSError, ValueError):
        pass
    built = date.fromtimestamp(path.stat().st_mtime).isoformat()
    return Roll(ParcelIndex(str(path)), posted, built, {county_key(n): BASE + n + ".zip" for n in COUNTIES})


def _roll_fields(hit: Dict[str, Any], situs: Optional[str]) -> Dict[str, Any]:
    return {"roll_owner": hit.get("owner"), "parcel_id": hit.get("parcelid"), "situs": situs,
            "appraisal": hit.get("appraisal"), "property_class": hit.get("class")}


def roll_match(roll: Roll, county: Any, borrowers: Iterable[Any]) -> Dict[str, Any]:
    """What the county roll says about the borrowers on an appointment.

    status: confirmed | name_only | multiple_parcels | common_name |
    ambiguous | co_owner_mismatch | no_match | no_person_borrower |
    county_not_on_file. Only "confirmed" identifies the owner and parcel.
    """
    ck = county_key(county)
    people = borrower_people(borrowers)
    base = {"searched": [name for name, _ in people], "roll_date": roll.dated(county), "roll_source": roll.urls.get(ck or "")}
    if ck not in roll.index.counties:
        return {**base, "status": "county_not_on_file"}
    if not people:
        return {**base, "status": "no_person_borrower"}
    hit = match_debtors(roll.con, [index_name(name) for name, _ in people], [ck])
    if not hit:
        parcels, owners = set(), set()
        for _, who in people:
            for row in _rows_for_last(roll.con, [ck], who[1]):
                if _names(row, who):
                    parcels.add(row["parcelid"])
                    owners.add(_owner_key(row))
        if not parcels:
            return {**base, "status": "no_match"}
        return {**base, "status": "ambiguous", "parcel_count": len(parcels), "owner_count": len(owners)}
    out = {**base, **_roll_fields(hit, hit.get("situs_address")), "strength": hit["confidence"],
           "parcel_count": hit.get("parcel_count") or 1, "owner_count": 1}
    if hit["confidence"] != "joint":
        # Everyone in the county with this first and last name, whatever the
        # middle initial. A second one makes the name a common name here.
        first, last, _ = people[0][1]
        same = {_owner_key(r) for r in _rows_for_last(roll.con, [ck], last) if _names(r, (first, last, None))}
        if len(same) > 1:
            parcels = {r["parcelid"] for r in _rows_for_last(roll.con, [ck], last) if _names(r, (first, last, None))}
            return {**out, "status": "common_name", "parcel_count": len(parcels), "owner_count": len(same)}
    if out["parcel_count"] > 1:
        return {**out, "status": "multiple_parcels"}
    if hit["confidence"] == "name_only":
        return {**out, "status": "name_only"}
    check = roll_owner_check(hit.get("owner"), people)
    out.update(check)
    if check["extra_owners"] or check["et_al"] or check["missing_borrowers"]:
        return {**out, "status": "co_owner_mismatch"}
    return {**out, "status": CONFIRMED}


def street_evidence(roll: Roll, county: Any, borrowers: Iterable[Any], address: Any,
                    earlier: Dict[str, Any]) -> Dict[str, Any]:
    """Owner and parcel from the street a linked sale notice names. The
    notice ties the loan to the street; the roll gives that street's owner."""
    people = borrower_people(borrowers)
    hit = roll.index.match_address(address, county) if address else None
    if not hit:
        return earlier
    base = {"searched": [name for name, _ in people], "roll_date": roll.dated(county),
            "roll_source": roll.urls.get(county_key(county) or ""), **_roll_fields(hit, str(address)),
            "strength": "notice_street", "parcel_count": 1, "owner_count": 1}
    check = roll_owner_check(hit.get("owner"), people)
    base.update(check)
    if not people or len(check["missing_borrowers"]) == len(people):
        return {**base, "status": "owner_differs"}
    if check["extra_owners"] or check["et_al"] or check["missing_borrowers"]:
        return {**base, "status": "co_owner_mismatch"}
    return {**base, "status": CONFIRMED}


# ── Appointment to sale notice ──────────────────────────────────────────────


_FIRM_NOISE = {"LLC", "PLLC", "LLP", "PC", "PA", "PLC", "LP", "INC", "TN", "TENNESSEE", "OF", "THE", "AND", "LAW",
               "GROUP", "SOLUTIONS", "SERVICES", "TRUSTEE", "TRUSTEES", "SUBSTITUTE", "SUCCESSOR"}


def firm_words(name: Any) -> List[str]:
    return [w for w in re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper()).split() if w not in _FIRM_NOISE]


def same_firm(trustees: Iterable[Any], notice: Dict[str, Any]) -> bool:
    """The notice names the trustee the appointment appointed."""
    text = " " + " ".join(re.sub(r"[^A-Z0-9 ]", " ", str(notice.get("text") or "").upper()).split()) + " "
    for trustee in trustees or []:
        words = firm_words(trustee)
        if not words or (len(words) == 1 and len(words[0]) < 5):
            continue
        lead = words[:2]
        for firm in notice.get("firms") or []:
            if firm_words(firm)[:len(lead)] == lead:
                return True
        if f" {' '.join(lead)} " in text:
            return True
    return False


def _first_advertised(row: Dict[str, Any]) -> Tuple[Optional[str], str]:
    for key, basis in (("publication_date", "publication date on the notice"),
                       ("report_date", "date of the trustee's sale report")):
        value = row.get(key)
        if not value:
            continue
        iso = _day(value)
        if not iso:
            try:
                from dateutil import parser
                iso = parser.parse(str(value), fuzzy=True).date().isoformat()
            except (ValueError, OverflowError):
                iso = None
        if iso:
            return iso, basis
    return _day(row.get("staged_at")), "day the notice was first staged here"


def notice_identity(row: Dict[str, Any]) -> Dict[str, Any]:
    """What a staged sale notice outside Shelby says about its loan."""
    first, basis = _first_advertised(row)
    names = row.get("borrowers") if isinstance(row.get("borrowers"), list) and row.get("borrowers") else split_names(row.get("full_name"))
    firms = [f for f in (row.get("trustee"), row.get("sub_trustee"), row.get("attorney"), FIRMS.get(row.get("bot_source"))) if f]
    return {"staging_id": row.get("id"), "source": row.get("bot_source"), "county": row.get("county"),
            "notice_id": row.get("notice_ref") or row.get("file_no") or row.get("id"),
            "names": [str(n) for n in names], "address": row.get("property_address"),
            "sale_date": _day(row.get("trustee_sale_date")), "first_advertised": first, "first_advertised_basis": basis,
            "firms": firms, "text": row.get("excerpt"), "url": row.get("source_url")}


def _names_borrower(people: List[Tuple[str, Tuple[str, str, Optional[str]]]], names: Iterable[str]) -> bool:
    """One notice name carries a borrower's first and last name. Checked
    name by name, so two different people cannot add up to a match."""
    for name in names or []:
        words = set(re.sub(r"[^A-Z ]", " ", str(name).upper()).split())
        if any(who[0] in words and who[1] in words for _, who in people):
            return True
    return False


def _same_county(a: Any, b: Any) -> bool:
    return bool(county_key(a)) and county_key(a) == county_key(b)


def link_notice(appt: Dict[str, Any], notices: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Find the sale notice for one Register index appointment.

    appt: county, borrowers, trustees, recorded, roll (roll_match result).
    Returns {"status": "none" | "candidate" | "conflict" | "linked", ...}.
    """
    people = borrower_people(appt.get("borrowers"))
    recorded = _day(appt.get("recorded"))
    roll = appt.get("roll") or {}
    situs_key = street_key_from_lead(roll.get("situs")) if roll.get("parcel_count") == 1 and roll.get("situs") else None
    earliest = (date.fromisoformat(recorded) - timedelta(days=NOTICE_LOOKBEHIND_DAYS)).isoformat() if recorded else ""
    hits, candidates, problems = [], [], []
    for n in notices:
        if not _same_county(n.get("county"), appt.get("county")):
            continue
        if (n.get("first_advertised") or "9999") < earliest or (n.get("sale_date") and recorded and n["sale_date"] < recorded):
            continue
        named = bool(n.get("names"))
        name_ok = bool(people) and _names_borrower(people, n.get("names"))
        if named and not name_ok:
            continue                                   # the notice names someone else
        street = street_key_from_lead(n.get("address")) if n.get("address") else None
        street_ok = bool(situs_key) and street == situs_key
        firm_ok = same_firm(appt.get("trustees"), n)
        if name_ok and street_ok:
            basis = "The notice names the borrower and the street of the borrower's parcel on the county roll"
        elif name_ok and firm_ok:
            basis = "The notice names the borrower and the substitute trustee this appointment appointed"
        elif not named and street_ok and firm_ok:
            basis = ("The trustee this appointment appointed advertises the street of the borrower's parcel on the "
                     "county roll. The notice does not print the borrower's name")
        elif name_ok:
            candidates.append({"staging_id": n.get("staging_id"), "notice_id": n.get("notice_id"), "source": n.get("source"),
                               "sale_date": n.get("sale_date"), "address": n.get("address")})
            continue
        else:
            continue
        if roll.get("status") == CONFIRMED and situs_key and street and street != situs_key:
            problems.append(f"notice {n.get('notice_id')} names {n['address']}; the borrower's parcel on the county "
                            f"roll is {roll.get('situs')}")
        hits.append((n, basis))
    if not hits:
        if candidates:
            return {"status": "candidate", "candidates": sorted(candidates, key=lambda c: str(c["staging_id"])),
                    "reason": "Same borrower name on a staged sale notice, with no second fact tying it to this appointment"}
        return {"status": "none"}
    streets = {street_key_from_lead(n["address"]) for n, _ in hits if n.get("address")}
    if len(streets) > 1:
        problems.append("the notices tied to this appointment name more than one street")
    ids = sorted({str(n.get("notice_id")) for n, _ in hits if n.get("notice_id")})
    if problems:
        return {"status": "conflict", "notice_ids": ids, "reason": "; ".join(sorted(set(problems)))}
    dated = sorted(hits, key=lambda h: (h[0].get("first_advertised") or "9999", str(h[0].get("staging_id"))))
    first, first_basis = dated[0][0].get("first_advertised"), dated[0][0].get("first_advertised_basis")
    n, basis = max(hits, key=lambda h: (h[0].get("first_advertised") or "", str(h[0].get("staging_id"))))
    gained = (date.fromisoformat(first) - date.fromisoformat(recorded)).days if first and recorded else None
    return {"status": "linked", "basis": basis, "notice_ids": ids, "notice_id": n.get("notice_id"),
            "staging_id": n.get("staging_id"), "notice_source": n.get("source"), "notice_url": n.get("url"),
            "first_advertised": first, "first_advertised_basis": first_basis, "appointment_recorded": recorded,
            "days_gained": gained, "sale_date": n.get("sale_date"), "notice_address": n.get("address")}


# ── Cards ───────────────────────────────────────────────────────────────────


def _county_of(row: Dict[str, Any]) -> str:
    try:
        return canonical_county(row.get("county") or "")
    except ValueError:
        return str(row.get("county") or "")


def _register_source(raw: Dict[str, Any], county: str) -> Dict[str, Any]:
    evidence = raw.get("evidence") if isinstance(raw.get("evidence"), dict) else {}
    office = evidence.get("office") or f"{county} County Register of Deeds"
    label = (f"{office}, {evidence.get('listing') or 'recording index listing'}, entry {raw.get('book_page')}. "
             f"Received by public records request{' from ' + evidence['from'] if evidence.get('from') else ''}; "
             "held on file, not online")
    return source(label, None, _day(evidence.get("received")))


def _roll_source(roll: Dict[str, Any], county: str) -> Dict[str, Any]:
    return source(f"Tennessee Comptroller assessment roll file for {county} County, posted {roll.get('roll_date') or 'undated'}",
                  roll.get("roll_source"), _day(roll.get("roll_date")))


def _plural(n: Any) -> Dict[str, Any]:
    return {"n": n, "s": "" if n == 1 else "s"}


def roll_hold(roll: Dict[str, Any], county: str, borrower_text: Optional[str]) -> Optional[str]:
    """Why the roll does not settle the owner and parcel. None when it does."""
    status = roll.get("status")
    who = borrower_text or "the borrower"
    posted = roll.get("roll_date") or "undated"
    if status == CONFIRMED:
        return None
    if status == "name_only":
        return (ROLL_HOLD[status].format(**_plural(1)) + f": {who} matches {roll.get('roll_owner')} at {roll.get('situs') or 'an unnumbered parcel'} "
                f"on the {county} County roll by first and last name. No middle initial, co-borrower or street confirms "
                "it is the same person, and the Register listing names no property")
    if status == "multiple_parcels":
        return (ROLL_HOLD[status].format(**_plural(roll.get("parcel_count"))) + f": {roll.get('roll_owner')} holds "
                f"{roll.get('parcel_count')} parcels on the {county} County roll and the Register listing does not say "
                "which one secures the loan")
    if status in ("ambiguous", "common_name"):
        return (ROLL_HOLD[status].format(**_plural(roll.get("parcel_count"))) + f" under {roll.get('owner_count')} different "
                f"owners on the {county} County roll. {who} is a common name there, so no parcel is matched")
    if status == "co_owner_mismatch":
        parts = []
        if roll.get("extra_owners") or roll.get("et_al"):
            parts.append("the roll lists " + (", ".join(roll.get("extra_owners") or []) or "other owners")
                         + (" and others (et al)" if roll.get("et_al") else "") + ", not named on the appointment")
        if roll.get("missing_borrowers"):
            parts.append(", ".join(roll["missing_borrowers"]) + " is on the appointment but not on the roll")
        return (f"The borrower's name matches {roll.get('roll_owner')} at {roll.get('situs') or 'one parcel'} on the "
                f"{county} County roll, but the owners and the borrowers are not the same people: " + "; ".join(parts)
                + ". Who can sell is not settled")
    if status == "owner_differs":
        return (f"The {county} County roll shows {roll.get('roll_owner')} for the advertised street {roll.get('situs')}; "
                f"the borrower on the appointment is {who}. Who can sell is not settled")
    if status == "no_match":
        return (f"No owner named {who} is on the {county} County roll (posted {posted}). The Register listing names no "
                "property, so the parcel is not identified")
    if status == "no_person_borrower":
        return "The Register listing names no individual borrower on this appointment, so there is no owner to look up"
    if status == "county_not_on_file":
        return (f"{county} County keeps its own assessment system and is not in the free statewide roll file, so the "
                "borrower has not been checked against a county owner record")
    return "The county roll was not available for this review, so the owner and parcel are not checked"


def appointment_card(row: Dict[str, Any], roll: Dict[str, Any], link: Dict[str, Any], today: date,
                     notice_carded: bool = False, ranked: Optional[bool] = None) -> Dict[str, Any]:
    """Card for one appointment of substitute trustee from a Register listing.

    row: id, county, property_address, raw (the staging raw_payload).
    roll: roll_match() or street_evidence() result. link: link_notice() result.
    """
    raw = row.get("raw") or {}
    county = _county_of(row)
    reference = str(raw.get("book_page") or "")
    recorded = _day(raw.get("recorded"))
    borrowers = [str(b) for b in (raw.get("borrowers") or []) if not is_institution(b)]
    lender = next((str(x) for x in (raw.get("lenders") or []) if x), None)
    trustees = ", ".join(str(t) for t in (raw.get("substitute_trustees") or []) if t)
    borrower_text = ", ".join(borrowers) or None
    register = _register_source(raw, county)
    linked = link.get("status") == "linked"
    confirmed = roll.get("status") == CONFIRMED
    age = (today - date.fromisoformat(recorded)).days if recorded else None

    what = (f"Appointment of substitute trustee recorded {recorded or 'on an unknown date'} "
            f"({county} County Register {reference})")
    if lender:
        what += f" for {lender}"
    if trustees:
        what += f", naming {trustees}"
    what += ". This is the step a lender takes before advertising a foreclosure sale."
    if linked:
        what += (f" A sale notice ({link['notice_id']}) was first advertised {link['first_advertised']}"
                 f" ({link.get('first_advertised_basis')})"
                 + (f", with the sale set for {link['sale_date']}." if link.get("sale_date") else ".")
                 + (f" The appointment was recorded {lead_time(link)}." if lead_time(link) else ""))
    else:
        what += " No sale notice for this borrower has been linked yet."
    passed = linked and bool(link.get("sale_date")) and link["sale_date"] < today.isoformat()
    status = "resolved" if passed else ("active" if linked or age is None or age <= APPOINTMENT_QUIET_DAYS else "unknown")
    event = {"what": what, "status": status, "checked_on": register["date"] or today.isoformat(), "source": register}

    if confirmed:
        how = {"joint": "both borrowers are named together on this one parcel",
               "middle_initial": "first name, last name and middle initial match, and nobody else by that first and "
                                 "last name owns in the county",
               "notice_street": "the linked sale notice names this street and the roll owner of the street is the borrower",
               }.get(roll.get("strength"), "matched by name")
        asset = {"description": "Real property owned by the borrower, taken to secure the loan under the substitute trustee",
                 "parcel": roll.get("parcel_id"), "address": roll.get("situs"),
                 "tie": (f"The Register listing names no property. The parcel comes from the {county} County roll: {how}"
                         + (". It is the only parcel the roll shows for the borrower" if roll.get("strength") != "notice_street" else "")
                         + ". No deed of trust has been read"),
                 "source": _roll_source(roll, county)}
    else:
        asset = {"description": "Property not yet identified", "parcel": None, "address": None, "source": None,
                 "tie": ("Candidate only, not confirmed: " + str(roll.get("situs") or roll.get("parcel_id"))
                         if roll.get("status") in ("name_only", "co_owner_mismatch") and (roll.get("situs") or roll.get("parcel_id"))
                         else "")}

    economics: Dict[str, Any] = {"deadline": link.get("sale_date") if linked else None,
                                 "deadline_kind": "Advertised trustee sale" if linked and link.get("sale_date")
                                 else "Unknown. No sale date advertised yet",
                                 "value": None, "value_basis": None, "value_is_estimate": False, "value_source": None,
                                 "known_debt": []}
    if confirmed and roll.get("appraisal"):
        economics.update(value=float(roll["appraisal"]), value_basis=VALUE_BASIS["comptroller"][0],
                         value_is_estimate=False, value_source=_roll_source(roll, county))
    unknowns = list(SALE_UNKNOWNS)
    if not linked:
        unknowns.insert(0, "Sale date. None advertised yet")
    unknowns += ["Whether the default has been cured since the appointment",
                 "Original loan amount and the deed of trust (not on the Register listing)"]
    economics["unknowns"] = unknowns

    if confirmed:
        decider = {"name": roll.get("roll_owner"), "verified": True,
                   "role": "Owner on the county roll and borrower on the appointment",
                   "basis": (f"{county} County roll posted {roll.get('roll_date') or 'undated'} shows this name on parcel "
                             f"{roll.get('parcel_id')}, and the Register listing indexes {borrower_text} as borrower on "
                             "the appointment. Roll owner only: no deed or title search has been done."),
                   "source": _roll_source(roll, county)}
    else:
        decider = {"name": borrower_text, "verified": False,
                   "role": "Borrower on the appointment (Register listing)" if borrower_text else "Not identified",
                   "basis": ("Named in the grantor column of the Register listing, sorted from the lender by lender "
                             "words. Present ownership is not confirmed by a county record."
                             if borrower_text else "The Register listing names no individual borrower on this filing."),
                   "source": register}

    conflicts: List[str] = []
    verify: List[str] = []
    hold: Optional[str] = None
    if link.get("status") == "conflict":
        hold = "A sale notice is tied to this appointment but names a different property: " + link["reason"]
        conflicts.append(link["reason"])
        verify.append("Read the notice and the recorded appointment and settle which parcel the loan is on")
    gap = roll_hold(roll, county, borrower_text)
    if not hold and gap:
        hold = gap
        if roll.get("status") in ("co_owner_mismatch", "owner_differs"):
            conflicts.append(gap)
            verify.append("Read the latest deed at the Register for everyone on title before treating anyone as the seller")
        elif roll.get("status") == "no_person_borrower":
            verify.append("Read the recorded appointment (book and page) for the borrower's name")
        else:
            verify.append(f"Ask the {county} County Register for the deed of trust this appointment cites "
                          f"({reference}); it names the property")
            verify.append("Or wait for the published sale notice, which names the street")
    if not hold:
        if roll.get("roll_owner") and is_institution(roll.get("roll_owner")):
            hold = "The owner is a company. Who signs for it is not established"
            verify.append("Look up the entity with the Tennessee Secretary of State for its registered agent and members")
        elif economics.get("value") is None:
            hold = "Owner and parcel are matched on the county roll, but the roll carries no appraisal for the parcel"
            verify.append("County assessor record for the parcel")
        elif not economics.get("deadline"):
            hold = (f"Early filing: owner {roll.get('roll_owner')} on the {county} County roll matches the borrower, "
                    f"parcel {roll.get('parcel_id')}, roll appraisal {_money(economics['value'])}. The Register listing "
                    "itself names no property. No sale date is advertised yet"
                    + (". Not on the ranked list (a ranking rule excludes it, or it is not ranked yet)"
                       if ranked is False else ""))
            verify.append("Promote when a sale notice or trustee calendar date appears for this borrower")
        elif ranked is False and not (linked and notice_carded) and not passed:
            hold = ("Owner, parcel and value are matched, but the record is not on the ranked list (it fails a ranking "
                    "rule such as the expected fee floor, or the nightly ranking has not picked it up yet)")
            verify.append("Recheck after the next nightly ranking")
    if link.get("status") == "candidate":
        seen = ", ".join(f"{c.get('source')} record {c.get('staging_id')}" for c in link.get("candidates") or [])
        note = (f" A staged sale notice carries the same borrower name ({seen}) but is not linked: name only, "
                "no shared trustee or street")
        hold = (hold or "A sale notice may exist for this borrower, but the match is by name only") + "." + note
        verify.insert(0, "Compare the candidate sale notice with the recorded appointment (trustee, deed of trust book and page)")

    owner_line = (f"The {county} County roll posted {roll.get('roll_date')} still carries {roll.get('roll_owner')}, so no "
                  "transfer shows on the roll. " if confirmed else "")
    availability = {
        "evidence": (owner_line
                     + ("A sale is advertised but has not happened. " if linked and not passed
                        else "The advertised sale date has passed. " if passed
                        else "No sale notice for this borrower is linked in the notices staged here. ")
                     + "Listing status has not been checked.").strip(),
        "checked_on": today.isoformat(), "conflicts": conflicts,
        "first_call_question": (f"A substitute trustee was appointed on your loan"
                                + (f" with {lender}" if lender else "") + f" on {recorded}. "
                                "Have you been able to work anything out with the lender since then?"),
        "verify_next": verify + ["Check for an active listing or contract before buying contact data",
                                 "Check the Register for a release, a new deed or a trustee's deed since the appointment"],
    }
    common = dict(lane=LANE, reviewed_by=REVIEWER, county=county, owner_names=[index_name(b) for b in borrowers],
                  today=today, event=event, asset=asset, decider=decider, economics=economics, availability=availability)

    if linked and notice_carded:
        return build_review(
            route="removed",
            route_reason=(f"Duplicate of the sale notice record for the same borrower (notice {link['notice_id']}, record "
                          f"{link['staging_id']}). {link['basis']}. Work that card; it carries the sale date."
                          + (f" The appointment was recorded {lead_time(link)}." if lead_time(link) else "")),
            **common)
    if passed:
        return build_review(route="removed", **common,
                            route_reason=(f"The linked sale notice was set for {link['sale_date']}, which has passed, and "
                                          "no later date is published. The pre-sale window is closed."))
    if hold:
        return build_review(route="research_hold", route_reason=hold, **common)
    return build_review(
        route="first_trace_review",
        route_reason=("Foreclosure filing with the borrower confirmed as owner on the county roll, an exact parcel, "
                      f"a roll appraisal and a sale advertised for {economics['deadline']}."),
        **common)


def lis_pendens_card(row: Dict[str, Any], today: date, roll_hit: Optional[Dict[str, Any]] = None,
                     roll_dated: Optional[str] = None) -> Dict[str, Any]:
    """Card for a lis pendens from a Register listing. Always a research
    hold: the listing names the parties but not who sued whom."""
    raw = row.get("raw") or {}
    county = _county_of(row)
    recorded = _day(raw.get("recorded"))
    first = ", ".join(str(n) for n in raw.get("first_parties") or []) or "none"
    second = ", ".join(str(n) for n in raw.get("second_parties") or []) or "none"
    register = _register_source(raw, county)
    address = row.get("property_address")
    tie = f"Property as the Register indexed it: {raw.get('property_as_indexed') or 'not stated'}"
    parcel = None
    if roll_hit and roll_hit.get("parcelid"):
        parcel = roll_hit["parcelid"]
        tie += (f". The state roll file (posted {roll_dated or 'undated'}) has one parcel at this street, {parcel}, in the "
                f"name of {roll_hit.get('owner')}. That is a bulk street match, not a check of who owns it or who was sued")
    return build_review(
        lane=LANE, route="research_hold", reviewed_by=REVIEWER, county=county, owner_names=[], today=today,
        route_reason=("Lis pendens on the Register listing. The listing does not say who sued whom or why, so the "
                      "owner and whoever can sell are not established"),
        event={"what": (f"Lis pendens recorded {recorded or 'on an unknown date'} ({county} County Register "
                        f"{raw.get('book_page')}). Listed first: {first}. Listed second: {second}. A lis pendens gives "
                        "notice of a lawsuit that affects title. It is not a foreclosure notice and sets no sale date."),
               "status": "unknown", "checked_on": register["date"] or today.isoformat(), "source": register},
        asset={"description": "Real property named on the lis pendens index entry", "parcel": parcel, "address": address,
               "tie": tie, "source": register},
        decider={"name": None, "role": "Not established", "verified": False, "source": register,
                 "basis": "The Register listing prints two groups of parties without their roles. Neither group is "
                          "staged as the owner."},
        economics={"deadline": None, "deadline_kind": "Unknown. A lis pendens sets no sale date", "known_debt": [],
                   "unknowns": ["Who filed the suit and against whom", "What the suit asks for",
                                "Owner of record and everyone who must sign", "Value, debt and liens"]},
        availability={"evidence": "Nothing beyond the index entry has been checked.", "checked_on": today.isoformat(),
                      "conflicts": [], "first_call_question": "",
                      "verify_next": [f"Read the recorded lis pendens ({raw.get('book_page')}) or the court file for the "
                                      "plaintiff, the defendant and what is asked",
                                      "Confirm the owner of record on the county assessor record for this street"]})


# ── The pass (reads staging and the local roll index; no network) ───────────


def _county_spellings(county: str) -> List[str]:
    name = canonical_county(county)
    forms = {name, name.lower(), name.upper(), f"{name} County", f"{name.lower()} county", f"{name.upper()} COUNTY"}
    return sorted(forms)


def review_register_index(bot, client, today: date, only: Optional[set], sales: Dict[str, Dict[str, Any]],
                          linked_to: Dict[str, Dict[str, Any]], patches: Dict[str, Dict[str, Any]],
                          plan: List[Dict[str, Any]], counts: Dict[str, int], roll: Optional[Roll] = None) -> None:
    """Cards for staged Register index rows, added to the caller's batch.

    Without the local Comptroller index nothing is read or changed: a card
    written without the roll would be weaker than the one already there.
    A failure here adds nothing to the batch and leaves the caller's own
    cards (the Shelby pass) untouched.
    """
    from ._base import BotTimeout

    mine: Dict[str, Any] = {"patches": {}, "plan": [], "counts": dict(counts), "linked": {}}
    try:
        _review(bot, client, today, only, sales, mine["linked"], mine["patches"], mine["plan"], mine["counts"], roll)
    except BotTimeout:
        raise
    except Exception as exc:  # noqa: BLE001
        bot.logger.warning("Register index review failed (%s); no Register index card changed", type(exc).__name__)
        counts["register_failed"] = 1
        return
    counts.update(mine["counts"])
    patches.update(mine["patches"])
    plan.extend(mine["plan"])
    for key, value in mine["linked"].items():
        linked_to.setdefault(key, value)


def _review(bot, client, today: date, only: Optional[set], sales: Dict[str, Dict[str, Any]],
            linked_to: Dict[str, Dict[str, Any]], patches: Dict[str, Dict[str, Any]],
            plan: List[Dict[str, Any]], counts: Dict[str, int], roll: Optional[Roll] = None) -> None:
    counts.update(register_appointments=0, register_lis_pendens=0, register_owner_confirmed=0, register_linked=0)
    roll = roll or open_roll()
    if roll is None:
        bot.logger.warning("Register index review skipped: no Comptroller index on this machine")
        counts["register_skipped_no_roll"] = 1
        return
    cutoff = (today - timedelta(days=bot.appointment_lookback_days)).isoformat()
    rows = bot._page(lambda: client.table(TABLE).select(ROW_COLUMNS).in_("bot_source", register_sources())
                     .in_("staging_status", ["pending", "promoted"]).gte("staged_at", cutoff))
    rows = [r for r in rows if isinstance(r.get("raw_payload"), dict) and (only is None or r["id"] in only)]
    appointments = [r for r in rows if r.get("distress_type") == APPOINTMENT]
    if not rows:
        return

    notices: List[Dict[str, Any]] = []
    for county in sorted({_county_of(r) for r in appointments}):
        recorded = [d for d in (_day(r["raw_payload"].get("recorded")) for r in appointments if _county_of(r) == county) if d]
        since = (date.fromisoformat(min(recorded)) - timedelta(days=NOTICE_LOOKBEHIND_DAYS)).isoformat() if recorded else cutoff
        try:
            spellings = _county_spellings(county)
        except ValueError:
            continue
        for name in NOTICE_SOURCES:
            found = bot._page(lambda: client.table(TABLE).select(NOTICE_COLUMNS).eq("bot_source", name)
                              .in_("county", spellings).in_("staging_status", ["pending", "promoted"]).gte("staged_at", since))
            notices.extend(notice_identity(r) for r in found)
    ranked = bot._ranked(client, [r["id"] for r in appointments]) if appointments else set()

    for row in sorted(rows, key=lambda r: (r["raw_payload"].get("recorded") or "", r["id"]), reverse=True):
        raw = dict(row["raw_payload"])
        patch: Dict[str, Any] = {}
        county = _county_of(row)
        if row.get("distress_type") == LIS_PENDENS:
            counts["register_lis_pendens"] += 1
            hit = roll.index.match_address(row.get("property_address"), county) if row.get("property_address") else None
            card = lis_pendens_card({**row, "raw": raw}, today, hit, roll.dated(county))
            kind, link = "register_lis_pendens", {"status": "none"}
        elif row.get("distress_type") == APPOINTMENT:
            counts["register_appointments"] += 1
            found = roll_match(roll, county, raw.get("borrowers"))
            link = link_notice({"county": county, "borrowers": raw.get("borrowers"), "recorded": raw.get("recorded"),
                                "trustees": raw.get("substitute_trustees"), "roll": found}, notices)
            if link["status"] == "linked" and link.get("notice_address"):
                found = street_evidence(roll, county, raw.get("borrowers"), link["notice_address"], found)
            if found != raw.get(ROLL_KEY):
                raw[ROLL_KEY] = patch[ROLL_KEY] = found
            if found.get("status") == CONFIRMED:
                counts["register_owner_confirmed"] += 1
            if link["status"] == "linked":
                counts["register_linked"] += 1
                linked_to.setdefault(link["staging_id"], {
                    **link, "instrument": raw.get("book_page"),
                    "instrument_label": f"{county} County Register {raw.get('book_page')}"})
            if link["status"] != "none" and raw.get(LINK_KEY) != link:
                raw[LINK_KEY] = patch[LINK_KEY] = link
            card = appointment_card({**row, "raw": raw}, found, link, today,
                                    notice_carded=link.get("staging_id") in sales
                                    and (only is None or link.get("staging_id") in only),
                                    ranked=row["id"] in ranked)
            kind = "register_appointment"
        else:
            continue
        bot._plan_card(row["id"], raw.get("pilot_review"), card, patch, counts)
        if patch:
            patches[row["id"]] = patch
            plan.append({"id": row["id"], "kind": kind, "source": row.get("bot_source"),
                         "instrument": raw.get("book_page"), "keys": sorted(patch),
                         "route": (patch.get("pilot_review") or raw.get("pilot_review") or {}).get("route"),
                         "link": link["status"]})
