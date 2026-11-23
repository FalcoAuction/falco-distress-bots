"""
Probate lane: does the estate control a specific, available asset?

An appointment as executor or administrator does not by itself give authority
over a particular parcel. In Tennessee real property generally vests in the
heirs or devisees at death unless the will directs otherwise or the court
orders a sale. This module turns one estate (its published notice, the matched
parcel, and today's county roll record) into a pilot_review card:

  first_trace_review         the parcel is tied to the decedent by the county
                             roll, the decedent is still the sole owner of
                             record, a fiduciary is named without an identity
                             conflict, the value is sourced, and no listing,
                             transfer or contract conflict is known. Authority
                             over the parcel is still listed as unverified.
  professional_relationship  the fiduciary is an institution or is also the
                             estate's counsel: the path is the professional.
  research_hold              a named gap: co-owner on the roll, name-only tie,
                             pending petition, broker listing, identity
                             conflict, unclear signer, roll not available.
  removed                    the parcel left the decedent's name, the estate
                             was reported closed, or the row repeats another
                             publication of the same estate.

Pure functions only: no network, no database. probate_pilot_review_bot does
the fetching and writing.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from . import _owner_lookup as owner_lookup
from ._pilot_review import build_review, source

REVIEWER = "probate_pilot_review"
EVIDENCE_KEY = "probate_control_evidence"   # recorded court/listing evidence, read on every run
CHECK_KEY = "probate_asset_check"           # the latest county roll check and counsel grouping

AUTHORITY_UNKNOWN = ("Authority over this parcel unverified: needs will power of sale, court order, "
                     "or all heirs or devisees signing")
LISTING_UNVERIFIED = "Listing status not verified (MLS and listing sites are not checked)"

_INSTITUTION_RE = re.compile(r"\b(BANK|TRUST COMPANY|TRUST CO|N\.?A\.?|FIDUCIARY|TRUST SERVICES|"
                             r"WEALTH|PUBLIC ADMINISTRATOR)\b", re.I)
_CO_OWNER_RE = re.compile(r"&|\bAND\b|\bET\s?UX\b|\bET\s?VIR\b|\bET\s?AL\b", re.I)
_NON_PROBATE_RE = re.compile(r"\b(LIFE ESTATE|TRUST|TRUSTEE|TRS|TR|REVOCABLE|LIVING TR|TOD|TRANSFER ON DEATH|"
                             r"SURVIVOR|SURVIVORSHIP|REMAINDER)\b", re.I)
_ESTATE_RE = re.compile(r"\b(ESTATE OF|EST OF|ESTATE|HEIRS OF|HEIRS)\b", re.I)
_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%B %d, %Y", "%b %d, %Y", "%b. %d, %Y",
                 "%A, %B %d, %Y", "%Y%m%d")


def parse_date(value: Any) -> Optional[date]:
    text = re.sub(r"\s+", " ", str(value or "").strip()).replace("Sept.", "Sep.").replace("Sept ", "Sep ")
    if not text:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _money(value: Any) -> Optional[float]:
    try:
        v = float(re.sub(r"[^\d.]", "", str(value)))
    except (TypeError, ValueError):
        return None
    return v if v > 0 else None


def _norm(value: Any) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", str(value or "").upper())).strip()


# ── County record parsing ─────────────────────────────────────────────────

PADCTN_CARD = "https://portal.padctn.org/OFS/WP/Print/{account}"


def parse_padctn_card(page: str) -> Dict[str, Any]:
    """Davidson Assessor "Unofficial Property Record Card": the current owner,
    the most recent sale and its deed reference, and the current appraisal."""
    text = BeautifulSoup(page or "", "html.parser").get_text("\n", strip=True)

    def field(label: str) -> Optional[str]:
        m = re.search(re.escape(label) + r"\s*\n([^\n]*)", text)
        value = m.group(1).strip() if m else None
        # An empty field is followed directly by the next label.
        return None if not value or value.endswith(":") else value

    sale = parse_date(field("Most Recent Sale Date:"))
    return {
        "parcel": field("Map & Parcel:"),
        "owner": field("Current Owner:"),
        "mailing": field("Mailing Address:"),
        "address": field("Location:"),
        "last_sale_date": sale.isoformat() if sale else None,
        "last_sale_price": _money(field("Most Recent Sale Price:")),
        "deed_reference": field("Deed Reference:"),
        "appraisal": _money(field("Total Appraisal Value:")),
        "assessment_year": field("Assessment Year:"),
    }


# ── Reading the staged estate ─────────────────────────────────────────────

def estate_facts(row: Dict[str, Any]) -> Dict[str, Any]:
    raw = row.get("raw_payload") if isinstance(row.get("raw_payload"), dict) else {}
    notice = raw.get("court_notice") if isinstance(raw.get("court_notice"), dict) else {}
    review = raw.get("probate_review") if isinstance(raw.get("probate_review"), dict) else {}
    evidence = raw.get(EVIDENCE_KEY) if isinstance(raw.get(EVIDENCE_KEY), dict) else {}
    sale = raw.get("court_sale_review") if isinstance(raw.get("court_sale_review"), dict) else {}
    county = re.sub(r"\s+county$", "", str(notice.get("county") or row.get("county") or "").strip().lower())
    docket = notice.get("docket") or evidence.get("docket") or sale.get("docket") or notice.get("reviewed_docket")
    decedent = notice.get("decedent") or row.get("full_name") or ""
    if decedent.isupper():
        decedent = decedent.title()
    reps = [r for r in (notice.get("representatives") or []) if isinstance(r, dict) and r.get("name")]
    if not reps:
        reps = [r for r in (evidence.get("fiduciaries") or []) if isinstance(r, dict) and r.get("name")]
    attorneys = [a for a in ((notice.get("attorney") or {}).get("names") or []) if _looks_like_counsel(a)]
    if not attorneys and evidence.get("counsel"):
        attorneys = [evidence["counsel"]]
    return {
        "county": county, "docket": docket, "decedent": decedent.strip(),
        "estate_key": review.get("estate_key") or (f"{county}|{docket}" if county and docket else None),
        "letters_type": notice.get("letters_type"), "letters_date": notice.get("letters_date"),
        "date_of_death": parse_date(notice.get("date_of_death")),
        "date_of_death_printed": notice.get("date_of_death"),
        "representatives": reps, "attorneys": attorneys,
        "stage": review.get("stage"), "hold_reasons": list(review.get("attorney_draft_hold_reasons") or []),
        "names_printed": review.get("decedent_names_printed") or [],
        "publication_date": parse_date(raw.get("publication_date")),
        "notice_url": row.get("source_url"), "text": raw.get("text") or raw.get("excerpt") or "",
        "evidence": evidence, "kind": raw.get("kind"),
    }


def _looks_like_counsel(name: Any) -> bool:
    """The signature parser sometimes keeps an office address line ("One Belle
    Meade Place") beside the lawyer. A counsel name has no digits and is not a
    place line."""
    text = str(name or "").strip()
    if not text or re.search(r"\d", text):
        return False
    return not re.search(r"\b(Place|Plaza|Suite|Street|Avenue|Road|Building|Tower|Floor)\b", text, re.I)


def professional_key(name: Any) -> str:
    """One key per lawyer or firm: "Noah Allen-Darden" and "Noah Allen Darden"
    are one relationship."""
    return _norm(name)


def estate_parcels(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parcels the scheduled matchers tied to this decedent, primary first.
    Each: parcel, address, owner, value, account_id, basis, match."""
    raw = row.get("raw_payload") if isinstance(row.get("raw_payload"), dict) else {}
    out: List[Dict[str, Any]] = []
    seen = set()

    def add(parcel, address, owner, value, account_id, basis, match, extra=None):
        keys = {k for k in (_norm(parcel), _norm(address)) if k}
        if not keys or keys & seen:
            return
        seen.update(keys)
        out.append({"parcel": parcel, "address": address, "owner": owner, "value": _money(value),
                    "account_id": account_id, "basis": basis, "match": match, **(extra or {})})

    evidence = raw.get(EVIDENCE_KEY) if isinstance(raw.get(EVIDENCE_KEY), dict) else {}
    ev_parcel = evidence.get("parcel") if isinstance(evidence.get("parcel"), dict) else None
    if ev_parcel:
        add(ev_parcel.get("parcel_id"), ev_parcel.get("address"), ev_parcel.get("owner"), ev_parcel.get("value"),
            None, ev_parcel.get("value_basis"), ev_parcel.get("tie") or "court_record",
            {"source_url": ev_parcel.get("source_url"), "checked_on": ev_parcel.get("checked_on"),
             "last_sale_date": ev_parcel.get("last_sale_date"), "recorded": True})
    match = raw.get("owner_match") if isinstance(raw.get("owner_match"), dict) else {}
    if match.get("status") == "matched":
        for p in sorted(match.get("parcels") or [], key=lambda c: -(c.get("value") or 0)):
            add(p.get("parcel"), p.get("address"), p.get("owner"), p.get("value"),
                (p.get("extra") or {}).get("account_id"), "county assessor appraisal",
                match.get("confidence") or "name_only")
    comp = raw.get("comptroller_parcel") if isinstance(raw.get("comptroller_parcel"), dict) else {}
    if comp.get("parcelid") or comp.get("parcels"):
        # The parcel id and owner are kept exactly as the state file prints
        # them so the card shares a property and owner key with other lanes.
        for p in comp.get("parcels") or [comp]:
            sold = parse_date(p.get("last_sale_date"))
            add(p.get("parcelid"), p.get("address"), p.get("owner"), p.get("appraisal"), None,
                "county assessor appraisal (state Comptroller parcel file)",
                "address" if comp.get("method") == "address" else None,
                {"last_sale_date": sold.isoformat() if sold else None})
    padctn = raw.get("padctn_owner_match") if isinstance(raw.get("padctn_owner_match"), dict) else {}
    if padctn.get("parcel"):
        add(padctn.get("parcel"), padctn.get("property_address"), padctn.get("owner"), padctn.get("appraised"),
            padctn.get("account_id"), "county assessor appraisal", None)
    return out


def name_tie(decedent: str, owner: Optional[str]) -> str:
    """How the roll owner string names the decedent.

    middle_confirmed  first, last and a middle initial or middle name agree
    name_only         first and last agree; nothing confirms the middle
    none              the roll owner does not name this person
    """
    person = owner_lookup.parse_person(decedent)
    if not person or not owner:
        return "none"
    best = "none"
    for seg in owner_lookup.person_segments(owner):
        hit = owner_lookup.segment_person(person, seg)
        if not hit:
            continue
        if person.middle_initial and hit[2] == person.middle_initial:
            return "middle_confirmed"
        best = "name_only"
    return best


def co_owner_signal(decedent: str, owner: Optional[str]) -> Optional[str]:
    """A reason title may sit outside the probate estate, or None.

    The roll is not a title search. A second name, a spouse marker, a trust or
    a life estate on the owner line each mean the fiduciary may not control
    the parcel; say which was printed."""
    text = str(owner or "")
    if not text.strip():
        return None
    stripped = _ESTATE_RE.sub(" ", text)
    if _NON_PROBATE_RE.search(stripped):
        return f"the county roll owner line reads \"{text.strip()}\" (trust, life estate or survivorship wording)"
    if _CO_OWNER_RE.search(stripped):
        return f"the county roll lists a co-owner or spouse: \"{text.strip()}\""
    return None


def petition_deadline(text: str) -> Optional[date]:
    """Response date printed in an order of publication on a petition to sell."""
    m = re.search(r"on or before\s+([A-Z][a-z]+\.?) (\d{1,2})(?:st|nd|rd|th)?,? (\d{4})", text or "", re.I)
    if not m:
        return None
    return parse_date(f"{m.group(1)} {m.group(2)}, {m.group(3)}")


def shelby_docket_url(docket: Optional[str]) -> Optional[str]:
    m = re.match(r"^PR-?\s*0*(\d{4,6})", str(docket or "").upper().replace(" ", ""))
    if not m:
        return None
    return ("https://prdata.shelbycountytn.gov/prweb/ck_public_qry_doct.cp_dktrpt_docket_report"
            f"?backto=D&case_id=PR{int(m.group(1)):06d}&begin_date=&end_date=")


# ── The decision ──────────────────────────────────────────────────────────

def _people(reps: List[Dict[str, Any]]) -> str:
    return " and ".join(r["name"] for r in reps)


def _role(reps: List[Dict[str, Any]]) -> str:
    roles = sorted({str(r.get("role") or "fiduciary").replace("_", " ") for r in reps})
    return " / ".join(roles) + " (role as printed)"


def professional_of(facts: Dict[str, Any]) -> Optional[Dict[str, str]]:
    """The professional who is the realistic path, when the fiduciary is an
    institution or is also counsel of record."""
    for rep in facts["representatives"]:
        if _INSTITUTION_RE.search(rep["name"]):
            return {"name": rep["name"], "role": "corporate fiduciary", "key": professional_key(rep["name"]),
                    "basis": f"{rep['name']} is the named {str(rep.get('role') or 'fiduciary').replace('_', ' ')}"}
        for attorney in facts["attorneys"]:
            if professional_key(attorney) == professional_key(rep["name"]):
                return {"name": attorney, "role": "estate counsel", "key": professional_key(attorney),
                        "basis": f"{attorney} signs as both the fiduciary and the attorney for the estate"}
    return None


def review_estate(row: Dict[str, Any], roll: Optional[Dict[str, Any]], today: date,
                  other_parcels: Optional[List[Dict[str, Any]]] = None,
                  professional_estates: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """Build the card for one estate row, or None when there is nothing to
    review yet (no parcel and no court evidence).

    roll: today's county record for the primary parcel
          {status: ok | unavailable, owner, parcel, address, value, value_basis,
           last_sale_date, deed_reference, source_url, checked_on, note}
    """
    f = estate_facts(row)
    parcels = estate_parcels(row)
    primary = parcels[0] if parcels else None
    evidence = f["evidence"]
    county = f["county"]
    day = today.isoformat()
    reps = f["representatives"]
    notice_src = source(f"Published notice, {county.title()} County docket {f['docket'] or 'not printed'}",
                        f["notice_url"], f["publication_date"])
    docket_label = f"{county.title()} County probate docket {f['docket'] or '(not printed)'}"
    letters = {"testamentary": "Letters Testamentary", "administration": "Letters of Administration",
               "authority": "Letters of Authority"}.get(str(f["letters_type"] or "").lower(), "Letters")
    testate = {"testamentary": "Testate (Letters Testamentary); the will's sale terms were not read",
               "administration": "Intestate or will annexed (Letters of Administration)"}.get(
        str(f["letters_type"] or "").lower(), "Testate or intestate not stated in the notice")

    petition = evidence.get("petition") if isinstance(evidence.get("petition"), dict) else None
    order = evidence.get("order") if isinstance(evidence.get("order"), dict) else None
    listing = evidence.get("listing") if isinstance(evidence.get("listing"), dict) else None

    event_what = (f"Estate of {f['decedent']}: {letters} issued"
                  f"{' ' + str(f['letters_date']) if f['letters_date'] else ''}"
                  f"{' to ' + _people(reps) if reps else ''}, {docket_label}.")
    if f["stage"] == "sale_authority_requested":
        event_what = (f"Estate of {f['decedent']}: petition for authority to sell real property, {docket_label}. "
                      "A petition and an order of publication do not grant authority.")
    if order:
        event_what += f" Court order entered {order.get('date')}: {order.get('summary')}"
    event = {"what": event_what, "status": "active", "checked_on": day, "source": notice_src}

    address = (roll or {}).get("address") or (primary or {}).get("address") or row.get("property_address")
    parcel_id = (roll or {}).get("parcel") or (primary or {}).get("parcel")
    roll_owner = (roll or {}).get("owner") or (primary or {}).get("owner")
    roll_src = None
    if roll and roll.get("source_url"):
        roll_src = source(f"{county.title()} County property record, parcel {parcel_id}", roll["source_url"],
                          roll.get("checked_on") or day)
    elif primary and primary.get("source_url"):
        roll_src = source(f"{county.title()} County property record, parcel {parcel_id}", primary["source_url"],
                          primary.get("checked_on"))
    value = (roll or {}).get("value") or (primary or {}).get("value")
    value_basis = (roll or {}).get("value_basis") or (primary or {}).get("basis")

    tie = name_tie(f["decedent"], roll_owner) if roll_owner else "none"
    if primary and primary.get("match") == "middle_initial" and tie == "name_only":
        tie = "middle_confirmed"
    if primary and primary.get("match") == "court_record" and tie == "none":
        tie = "court_record"
    more = [p for p in (other_parcels if other_parcels is not None else parcels[1:]) if p]
    description = " ".join(x for x in (
        address or "", f"(parcel {parcel_id})" if parcel_id else "",
        f". Estate also matched to {len(more)} more parcel(s): " +
        "; ".join(f"{p.get('address') or p.get('parcel')} (roll owner {p.get('owner')})" for p in more[:4])
        if more else "") if x).replace(" .", ".")
    tie_text = ""
    if roll_owner:
        tie_text = (f"County roll owner \"{roll_owner}\"; decedent \"{f['decedent']}\" "
                    + {"middle_confirmed": "(first, last and middle agree).",
                       "name_only": "(first and last name only; nothing confirms the middle name).",
                       "court_record": "(parcel named in the court record).",
                       "none": "(the roll owner does not name the decedent)."}[tie])
        sale_date = (roll or {}).get("last_sale_date") or (primary or {}).get("last_sale_date")
        if sale_date:
            tie_text += f" Last recorded sale {sale_date}"
            if (roll or {}).get("deed_reference"):
                tie_text += f", deed reference {roll['deed_reference']}"
            tie_text += "."
    asset = {"description": description, "parcel": parcel_id, "address": address, "tie": tie_text,
             "source": roll_src}

    decider = {
        "name": _people(reps) or None,
        "role": _role(reps) if reps else "",
        "basis": (f"Named in the published notice as holder of {letters}. Letters alone do not give authority to "
                  "sell this parcel: in Tennessee real property vests in the heirs or devisees at death unless the "
                  "will directs otherwise or the court orders a sale.") if reps else "",
        "verified": False, "source": notice_src,
    }
    if reps and not (row.get("raw_payload") or {}).get("court_notice", {}).get("representatives"):
        decider["basis"] = (evidence.get("fiduciary_basis") or "Named in the court file.") + \
            " Authority over this parcel is a separate question."
        if evidence.get("fiduciary_source"):
            decider["source"] = source("Court case parties", evidence["fiduciary_source"],
                                       evidence.get("recorded_on"))
    counsel_note = ""
    if f["attorneys"]:
        counsel_note = "Estate counsel as printed: " + ", ".join(f["attorneys"]) + \
            " (separate professional route; do not treat as the seller)"
    unknowns = [AUTHORITY_UNKNOWN, testate, "Mortgage or lien payoff unknown", "Seller intent unknown",
                "Heirs or devisees not identified"]
    economics = {"deadline": None, "deadline_kind": None, "value": value, "value_basis": value_basis,
                 "value_is_estimate": bool(value_basis and "divided" in str(value_basis)),
                 "value_source": roll_src, "known_debt": [], "unknowns": unknowns}
    verify_next = [LISTING_UNVERIFIED,
                   f"{docket_label}: read the will or letters for a power of sale and look for any petition or "
                   "order to sell",
                   "Register of Deeds: read the vesting deed for survivorship, trust or life estate wording"]
    if counsel_note:
        verify_next.append(counsel_note)
    question = (f"Does the will give you power to sell {address or 'the property'}, or will every heir or devisee "
                "sign, and is it already listed or promised to anyone?")
    availability = {"evidence": "", "checked_on": day, "conflicts": [], "first_call_question": question,
                    "verify_next": verify_next}

    def card(route: str, reason: str, owner_names: Any = None) -> Dict[str, Any]:
        return build_review(lane="probate", route=route, route_reason=reason, reviewed_by=REVIEWER,
                            event=event, asset=asset, decider=decider, economics=economics,
                            availability=availability, county=county,
                            owner_names=owner_names if owner_names is not None else (roll_owner or f["decedent"]),
                            today=today)

    def hold(reason: str, check: str, conflict: Optional[str] = None) -> Dict[str, Any]:
        availability["verify_next"] = [check] + [v for v in verify_next if v != check]
        if conflict:
            availability["conflicts"] = [conflict]
        if not availability["evidence"]:
            availability["evidence"] = "Not established: " + reason
        return card("research_hold", reason)

    # 1. Holds that predate this review are preserved.
    if "conflicting_decedent_names" in f["hold_reasons"]:
        names = " / ".join(f["names_printed"]) or "two spellings"
        return hold(f"The notice prints two different decedent names ({names}); no parcel is matched until the "
                    "court file settles the name.",
                    f"{docket_label}: confirm the decedent's legal name, then match the parcel",
                    "Decedent name conflict in the published notice")
    if f["stage"] == "estate_inactive_reported":
        event["status"] = "resolved"
        return card("removed", "The estate was reported closed or the representative discharged.")

    # 2. A petition to sell is a request, not authority.
    if f["stage"] == "sale_authority_requested" and not order:
        deadline = parse_date((petition or {}).get("response_by")) or petition_deadline(f["text"])
        url = (petition or {}).get("docket_url") or shelby_docket_url(f["docket"])
        economics["deadline"] = deadline.isoformat() if deadline and deadline >= today else None
        economics["deadline_kind"] = "response date on the petition to sell" if economics["deadline"] else None
        when = deadline.isoformat() if deadline else "the response date"
        due = bool(deadline and deadline < today)
        check = (f"{'Response date ' + when + ' has passed: check' if due else 'On or after ' + when + ', check'} "
                 f"docket {f['docket']} for an entered order granting or denying the petition: {url or 'clerk'}")
        availability["evidence"] = ("Petition for authority to sell is pending; only a publication or service "
                                    "order is on file. No order granting the sale was found.")
        if roll_owner and tie == "none":
            availability["conflicts"] = [f"County roll owner \"{roll_owner}\" is not the decedent"]
        return hold(f"Petition to sell is pending with a response date of {when}; no order grants authority yet.",
                    check)

    if not reps:
        return hold("No fiduciary is named in the stored notice (excerpt only, or the signer's role is unclear).",
                    f"{docket_label}: confirm who holds letters and in what role")
    if not primary:
        return None

    # 3. Today's county record.
    if roll and roll.get("status") == "ok":
        current = roll.get("owner") or ""
        # "PERSON EVE F TRUSTEE" or "... LIFE ESTATE FOR PERSON EVE F" still
        # names her; the trust or life estate is handled as title outside probate.
        person = owner_lookup.parse_person(f["decedent"])
        names_decedent = name_tie(f["decedent"], current) != "none" or bool(
            person and (_ESTATE_RE.search(current) or _NON_PROBATE_RE.search(current))
            and {person.last, person.first} <= set(_norm(current).split()))
        staged_owner = primary.get("owner") or ""
        if current and not names_decedent and (
                not staged_owner or _norm(staged_owner) == _norm(current)
                or name_tie(f["decedent"], staged_owner) == "none"):
            # Never the decedent's on the roll: the staged match is the problem.
            return hold(f"The county roll owner \"{current}\" does not name the decedent \"{f['decedent']}\" "
                        "(different middle name or a different person), so this parcel is not tied to the estate.",
                        "Confirm which parcel, if any, the decedent owned: vesting deed or the inventory in the "
                        "court file",
                        "Roll owner does not match the decedent")
        if current and not names_decedent:
            event["status"] = "resolved"
            availability["evidence"] = (f"County record checked {day}: owner of record is now \"{current}\""
                                        + (f", last sale {roll['last_sale_date']}" if roll.get("last_sale_date") else "")
                                        + (f", deed reference {roll['deed_reference']}" if roll.get("deed_reference") else "")
                                        + ". The parcel is no longer in the decedent's name.")
            return card("removed", f"Parcel transferred out of the decedent's name: owner of record is now {current}.",
                        owner_names=current)
        sold = parse_date(roll.get("last_sale_date"))
        if sold and f["date_of_death"] and sold > f["date_of_death"]:
            return hold(f"The county record shows a sale on {sold.isoformat()}, after the date of death.",
                        f"Register of Deeds: read deed {roll.get('deed_reference') or '(reference not shown)'} "
                        "to see who conveyed and to whom",
                        f"Recorded sale {sold.isoformat()} after the date of death")
    elif roll is not None or not (primary.get("recorded") and primary.get("checked_on")):
        note = (roll or {}).get("note") or "no free current-owner check is wired for this county"
        staged_outside = co_owner_signal(f["decedent"], roll_owner)
        if staged_outside:
            return hold("Title may have passed outside probate to a surviving owner or under a trust or life "
                        f"estate, so the fiduciary may not control this parcel: {staged_outside}. This is the "
                        f"staged capture; the roll could not be rechecked today ({note}).",
                        "Register of Deeds: read the vesting deed for tenancy by the entirety, survivorship, trust "
                        "or life estate wording, and confirm whether the co-owner is living",
                        "Co-owner, trust or life estate on the county roll")
        return hold(f"Current owner of record could not be rechecked today ({note}); the staged match is an "
                    "earlier capture.",
                    f"Recheck the {county.title()} County roll for parcel {parcel_id}: current owner and any "
                    "transfer since the date of death")

    availability["evidence"] = (
        f"County record checked {(roll or {}).get('checked_on') or primary.get('checked_on') or day}: "
        f"\"{roll_owner}\" is still the owner of record"
        + (" and the last recorded sale predates the death"
           if f["date_of_death"] and (roll or {}).get("last_sale_date") else "")
        + ". No petition or order to sell was found in the published notices. Listing status not verified.")

    # 4. Known listing, contract or sale.
    if order:
        decider["basis"] += f" Court order entered {order.get('date')}: {order.get('summary')}"
        if order.get("source"):
            decider["source"] = source(f"Court docket {f['docket']}", order["source"], order.get("date"))
        unknowns[0] = "Scope of the court order and any further approval needed before a closing"
    if listing:
        availability["verify_next"] = [v for v in verify_next if v != LISTING_UNVERIFIED]
        verify_next[:] = availability["verify_next"]
        status = str(listing.get("status") or "").lower()
        label = (f"{listing.get('brokerage') or 'a broker'}"
                 f"{' MLS ' + str(listing['mls']) if listing.get('mls') else ''}"
                 f"{' at $' + format(int(listing['asking_usd']), ',') if listing.get('asking_usd') else ''}")
        if status in ("sold", "closed"):
            event["status"] = "resolved"
            availability["evidence"] = f"Recorded listing evidence: sold through {label}."
            return card("removed", f"The property was sold through {label}.")
        availability["evidence"] = (f"Listed through {label}, status {listing.get('status')} when recorded "
                                    f"{listing.get('recorded_on') or evidence.get('recorded_on')}. "
                                    "The assignment is not open."
                                    + (f" Source: {listing['source']}" if listing.get("source") else ""))
        return hold(f"Already listed through {label}.",
                    "Recheck the docket for court approval of a closing and whether the listing is still active",
                    f"Broker listing: {label}")

    # 5. Title that may pass outside probate.
    outside = co_owner_signal(f["decedent"], roll_owner)
    if outside:
        named = [r["name"] for r in reps if name_tie(r["name"], roll_owner) != "none"]
        decedent_last = _norm(f["decedent"]).split()[-1:] if f["decedent"] else []
        shared = [r["name"] for r in reps if _norm(r["name"]).split()[-1:] != decedent_last
                  and _norm(r["name"]).split()[-1] in _norm(roll_owner).split()]
        extra = (f" The co-owner on the roll, {named[0]}, is also the named fiduciary." if named else
                 f" The co-owner line carries the surname of the named fiduciary, {shared[0]}." if shared else "")
        availability["first_call_question"] = (
            f"How was {address or 'the property'} titled: did it pass to the surviving owner at death, "
            "or is a share in the estate?")
        return hold("Title may have passed outside probate to a surviving owner or under a trust or life estate, "
                    f"so the fiduciary may not control this parcel: {outside}.{extra}",
                    "Register of Deeds: read the vesting deed for tenancy by the entirety, survivorship, trust or "
                    "life estate wording, and confirm whether the co-owner is living",
                    "Co-owner, trust or life estate on the county roll")

    # 6. The tie itself.
    if tie not in ("middle_confirmed", "court_record"):
        return hold("The parcel is tied to the decedent by first and last name only.",
                    "Confirm the decedent owned this parcel: vesting deed, the will or inventory in the court "
                    "file, or a matching address in the estate papers")
    if value is None or not value_basis or not roll_src:
        return hold("No sourced value for the parcel.", "County assessor appraisal for the parcel")

    # 7. A professional fiduciary is its own route.
    pro = professional_of(f)
    if pro:
        estates = [e for e in (professional_estates or []) if e]
        decider.update({"name": pro["name"], "role": pro["role"], "basis": pro["basis"] + ". " + AUTHORITY_UNKNOWN + "."})
        if estates:
            availability["evidence"] += " Estates with this professional in the current cohort: " + "; ".join(estates) + "."
        return card("professional_relationship",
                    f"The fiduciary is a professional ({pro['role']}): reach {pro['name']} as a relationship, "
                    "not as an owner trace.", owner_names=[pro["name"]])

    return card("first_trace_review",
                "Parcel tied to the decedent by the county roll, decedent still the sole owner of record, named "
                "fiduciary without an identity conflict, sourced value, no listing or transfer conflict known. "
                "Authority over this parcel is the first question, not a given.")


def duplicate_card(row: Dict[str, Any], primary_id: str, today: date) -> Dict[str, Any]:
    """A repeat publication of an estate whose review sits on another row."""
    f = estate_facts(row)
    parcels = estate_parcels(row)
    p = parcels[0] if parcels else {}
    src = source(f"Published notice, estate of {f['decedent']}", f["notice_url"], f["publication_date"])
    return build_review(
        lane="probate", route="removed", reviewed_by=REVIEWER,
        route_reason=f"Repeat publication of the same estate; the review is on staging row {primary_id}.",
        event={"what": f"Estate of {f['decedent']}: repeat notice to creditors.", "status": "active",
               "checked_on": today.isoformat(), "source": src},
        asset={"description": p.get("address") or row.get("property_address") or "", "parcel": p.get("parcel"),
               "address": p.get("address") or row.get("property_address"), "tie": "Same estate and parcel as the "
               f"primary row {primary_id}.", "source": src},
        decider={}, economics={}, availability={"evidence": f"See staging row {primary_id}.",
                                                "checked_on": today.isoformat()},
        county=f["county"], owner_names=p.get("owner") or f["decedent"], today=today)
