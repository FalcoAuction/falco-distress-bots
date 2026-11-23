"""
Pre-foreclosure lane review: one pilot_review card per appointment of
substitute trustee and per advertised trustee sale.

Three jobs, all pure (no network, no database; preforeclosure_review_bot
does the reading and writing):

  link_notice()       tie an early Shelby appointment to the sale notice later
                      published for the same loan. A shared instrument number
                      is required. A shared borrower name is never enough, and
                      a different parcel or street on the notice is a conflict
                      that holds the record instead of linking it.
  sale_state()        what the trustee's own sale list says today: listed,
                      postponed, withdrawn from the list, or past its date. A
                      change updates the one existing record.
  appointment_card()  the shared card (see _pilot_review.py) for each record.
  sale_card()

Who can decide to sell is stated by role. The borrower on a notice, the
taxpayer on the county bill and the owner on the county roll are different
facts; a card names which one it has and holds the record when they disagree.
The original loan amount from a deed of trust is filed as mortgage_original.
It is not a payoff.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from ._pilot_review import _ENTITY, _SUFFIX, build_review, debt, source
from ._shelby_register import detail_url
from ._shelby_trustee_value import parcel_key, street_key

LANE = "pre_foreclosure"
REVIEWER = "preforeclosure_review"
TRACK_KEY = "preforeclosure_sale_track"
LINK_KEY = "sale_notice_link"
EVIDENCE_KEY = "preforeclosure_evidence"
COUNTY_KEY = "appointment_county_evidence"

FIRMS = {"brock_scott_trustee": "Brock & Scott", "mackie_wolf_trustee": "Mackie Wolf Zientz & Mann"}
SALE_SOURCES = ("memphis_daily_news", "mackie_wolf_trustee", "brock_scott_trustee")
# A listing absent from this many hours of successful reads has left the list.
WITHDRAWN_AFTER_HOURS = 36
# A trustee list not read for this long cannot confirm or deny anything.
SOURCE_STALE_DAYS = 4
APPOINTMENT_QUIET_DAYS = 150

VALUE_BASIS = {
    "comptroller": ("Tennessee Comptroller county assessment roll appraisal", False),
    "davidson_assessor": ("Davidson County assessor total appraised value", False),
    "rutherford_assessor": ("Rutherford County assessor total appraised value", False),
    "metro_nashville_parcels": ("Metro Nashville parcel record total appraised value", False),
    "shelby_trustee_assessment_estimate":
        ("Shelby County realty bill assessment divided by the class ratio. An estimate, not an appraisal", True),
    "shelby_loan_hpi_estimate":
        ("Original loan divided by an assumed 95% loan to value, grown by the Memphis FHFA house price index. "
         "A rough estimate", True),
    "xref_avm_enricher": ("Value carried over from another record at the same address. An estimate", True),
}

COUNTY_MISS = {
    "no_current_match": "no current realty bill was found for the street",
    "notice_parcel_conflict": "the bill for this street is on a different parcel number than the deed of trust",
    "ambiguous_parcels": "more than one parcel is billed at this street",
    "conflicting_current_bills": "the parcel has more than one current bill or billed name",
    "unit_requires_parcel": "the street has units and the parcel number is needed to pick one",
    "detail_identity_conflict": "the bill detail page does not match the search result",
    "invalid_address": "the street on the deed of trust is not a usable address",
}

_NAME_NOISE = {"AND", "THE", "OF", "MR", "MRS", "MS", "ETUX", "ETVIR", "ETAL", "UX", "VIR", "ET", "AL"}


def _day(value: Any) -> Optional[str]:
    """ISO day from a date, datetime or ISO string. None when unreadable."""
    if value in (None, ""):
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    text = str(value)[:10]
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


def _stamp(value: Any) -> Optional[datetime]:
    try:
        stamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return stamp if stamp.tzinfo is None else stamp.astimezone(timezone.utc).replace(tzinfo=None)


def _mdy(value: Any) -> Optional[str]:
    try:
        return datetime.strptime(str(value).strip(), "%m/%d/%Y").date().isoformat()
    except (TypeError, ValueError):
        return None


def _money(value: Any) -> str:
    return f"${float(value):,.0f}"


def name_tokens(name: Any) -> set:
    text = re.sub(r"[^A-Z ]", " ", str(name or "").upper())
    text = _SUFFIX.sub(" ", text)
    return {t for t in text.split() if len(t) > 1 and t not in _NAME_NOISE}


def split_names(names: Any) -> List[str]:
    if names is None:
        return []
    if isinstance(names, str):
        names = re.split(r"\s*(?:;|&| and )\s*", names, flags=re.I)
    return [str(n).strip() for n in names if str(n or "").strip()]


def is_entity(name: Any) -> bool:
    return bool(_ENTITY.search(re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())))


def names_agree(a: Any, b: Any) -> bool:
    """True when some person on one side shares two name words with someone
    on the other ("PRICE SHUNTEA S" and "Shuntea Price"), or an entity name
    matches word for word. One shared surname is not agreement."""
    left = [name_tokens(n) for n in split_names(a)]
    right = [name_tokens(n) for n in split_names(b)]
    whole_left, whole_right = set().union(*left) if left else set(), set().union(*right) if right else set()
    for x in left:
        if len(x & whole_right) >= 2:
            return True
    for y in right:
        if len(y & whole_left) >= 2:
            return True
    return False


def shelby_parcel(value: Any) -> Optional[str]:
    """One spelling for a full Shelby parcel number, or None for a partial one.

    The Trustee and most notices print ward-block, an optional sub-block
    letter and five digits ("076077  00021", "D0209A D00077"). The Register
    sometimes prints fourteen characters instead: ward-block, a two-character
    sub-block padded with a zero, five digits and a one-digit suffix
    ("076077 00000210", "A0142D0B000690").
    """
    key = parcel_key(value)
    if len(key) == 14:
        tail = "" if key[13] == "0" else "-" + key[13]
        return key[:6] + key[6:8].lstrip("0") + key[8:13] + tail
    return key if re.fullmatch(r"[A-Z0-9]{6}[A-Z]?[0-9]{5}", key) else None


def parcels_differ(a: Any, b: Any) -> bool:
    """True only when two parcel numbers cannot be the same parcel. A notice
    that prints just the ward and block ("A0142D") is consistent with any
    parcel in that block, which neither confirms nor contradicts."""
    full_a, full_b = shelby_parcel(a), shelby_parcel(b)
    if full_a and full_b:
        return full_a != full_b
    key_a, key_b = parcel_key(a), parcel_key(b)
    return not (key_a.startswith(key_b) or key_b.startswith(key_a))


def parcels_match(a: Any, b: Any) -> bool:
    full_a, full_b = shelby_parcel(a), shelby_parcel(b)
    return bool(full_a) and full_a == full_b


# ── Appointment to notice ───────────────────────────────────────────────────


def notice_identity(row: Dict[str, Any]) -> Dict[str, Any]:
    """What a Memphis Daily News staging row says about its loan and property."""
    tracking = (row.get("notice_tracking") or {}) if isinstance(row.get("notice_tracking"), dict) else {}
    cited = row.get("cited_appointment") or tracking.get("substitute_trustee_appointment") or {}
    review = row.get("trustee_review") or {}
    parcel = row.get("notice_parcel") or (review.get("parcel_id") if review.get("status") == "verified_assessment" else None)
    return {
        "staging_id": row.get("id"), "notice_id": row.get("notice_id"),
        "loan_instrument": re.sub(r"\s", "", str(row.get("loan_instrument") or "")).upper() or None,
        "cited_appointment": (cited.get("instrument_number") if isinstance(cited, dict) else None),
        "parcel": parcel, "address": row.get("property_address"),
        "first_advertised": _mdy(row.get("first_notice_date")) or _day(row.get("publication_date")),
        "sale_date": _day(row.get("trustee_sale_date")), "borrower": row.get("full_name"),
        "url": row.get("source_url"),
    }


def link_notice(appt: Dict[str, Any], notices: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Find the published sale notice for one appointment.

    appt: instrument, recorded, loan_instrument, parcel, address.
    Returns {"status": "none" | "conflict" | "linked", ...}.
    """
    instrument = str(appt.get("instrument") or "")
    loan = re.sub(r"\s", "", str(appt.get("loan_instrument") or "")).upper() or None
    hits = []
    for n in notices:
        cited = bool(instrument) and n.get("cited_appointment") == instrument
        same_loan = bool(loan) and n.get("loan_instrument") == loan
        if not (cited or same_loan):
            continue
        problems = []
        if appt.get("parcel") and n.get("parcel") and parcels_differ(appt["parcel"], n["parcel"]):
            problems.append(f"notice {n.get('notice_id')} names parcel {n['parcel']}, the appointment's loan is on {appt['parcel']}")
        if appt.get("address") and n.get("address") and street_key(appt["address"]) != street_key(n["address"]):
            problems.append(f"notice {n.get('notice_id')} names {n['address']}, the appointment's loan is on {appt['address']}")
        if cited and same_loan:
            basis = "The notice cites this appointment instrument and the same deed of trust instrument"
        elif cited:
            basis = "The notice cites this appointment instrument"
        else:
            basis = "The notice forecloses the same deed of trust instrument"
        if not problems:
            if parcels_match(appt.get("parcel"), n.get("parcel")):
                basis += ", and the parcels agree"
            elif appt.get("address") and n.get("address"):
                basis += ", and the streets agree"
        hits.append((n, basis, problems))
    if not hits:
        return {"status": "none"}
    problems = [p for _, _, ps in hits for p in ps]
    places = {shelby_parcel(n["parcel"]) for n, _, _ in hits if shelby_parcel(n.get("parcel"))}
    streets = {street_key(n["address"]) for n, _, _ in hits if n.get("address")}
    if len(places) > 1 or (not places and len(streets) > 1):
        problems.append("the notices tied to this appointment describe more than one property")
    ids = sorted({n.get("notice_id") for n, _, _ in hits if n.get("notice_id")})
    if problems:
        return {"status": "conflict", "notice_ids": ids, "reason": "; ".join(sorted(set(problems)))}
    # A republished notice is the same sale: keep the earliest advertisement
    # for the lead time and the most recent record for the sale date.
    dated = sorted(hits, key=lambda h: (h[0].get("first_advertised") or "9999", str(h[0].get("staging_id"))))
    first = dated[0][0].get("first_advertised")
    current = max(hits, key=lambda h: (h[0].get("first_advertised") or "", str(h[0].get("staging_id"))))
    n, basis, _ = current
    recorded = _day(appt.get("recorded"))
    gained = (date.fromisoformat(first) - date.fromisoformat(recorded)).days if first and recorded else None
    return {"status": "linked", "basis": basis, "notice_ids": ids, "notice_id": n.get("notice_id"),
            "staging_id": n.get("staging_id"), "notice_url": n.get("url"), "first_advertised": first,
            "appointment_recorded": recorded, "days_gained": gained, "sale_date": n.get("sale_date"),
            "notice_parcel": n.get("parcel"), "notice_address": n.get("address")}


# ── Trustee sale list status ────────────────────────────────────────────────


def sale_state(row: Dict[str, Any], source_last_read: Optional[datetime], now: datetime,
               today: Optional[date] = None) -> Dict[str, Any]:
    """Current standing of an advertised sale, from the stored sightings.

    row: bot_source, trustee_sale_date, notice_tracking, sale_status,
    calendar (Foundation evidence, Memphis rows only), track (this module's
    memory of earlier dates).
    """
    now = _stamp(now.isoformat())
    today = today or now.date()
    sale = _day(row.get("trustee_sale_date"))
    tracking = row.get("notice_tracking") if isinstance(row.get("notice_tracking"), dict) else {}
    calendar = row.get("calendar") if isinstance(row.get("calendar"), dict) else {}
    track = dict(row.get("track") or {}) if isinstance(row.get("track"), dict) else {}
    dates = [d for d in (track.get("dates") or []) if _day(d)]
    listing = calendar.get("listing") or {}
    if calendar.get("status") == "postponed" and listing.get("prior_date") and listing.get("date") == sale:
        if listing["prior_date"] not in dates:
            dates.append(listing["prior_date"])
    if sale and sale not in dates:
        dates.append(sale)
    elif sale and dates[-1] != sale:
        dates.remove(sale)
        dates.append(sale)
    seen = _stamp(tracking.get("last_seen_at"))
    state: Dict[str, Any] = {"dates": dates, "sale_date": sale, "last_listed_on": seen.date().isoformat() if seen else None,
                             "last_listed_by": tracking.get("last_seen_by")}
    prior = dates[-2] if len(dates) > 1 else None
    manual = row.get("sale_status")
    if manual:
        label = manual.get("status") if isinstance(manual, dict) else str(manual)
        return {**state, "status": "manual", "manual_status": label}
    if not sale:
        return {**state, "status": "undated"}
    if sale < today.isoformat():
        return {**state, "status": "passed"}
    direct = row.get("bot_source") in FIRMS
    if direct:
        if source_last_read is None or (now - source_last_read).days >= SOURCE_STALE_DAYS:
            return {**state, "status": "unverified", "prior_date": prior}
        if seen is None or (source_last_read - seen) > timedelta(hours=WITHDRAWN_AFTER_HOURS):
            return {**state, "status": "withdrawn", "source_read_on": source_last_read.date().isoformat()}
    elif calendar.get("status") == "omitted" and calendar.get("checked_on"):
        return {**state, "status": "withdrawn", "source_read_on": calendar["checked_on"]}
    else:
        # A newspaper notice keeps its printed date after a sale is pulled or
        # moved, so the date counts only when the trustee's own calendar shows it.
        seed = row.get("seed") if isinstance(row.get("seed"), dict) else {}
        seen_on_list = (seed.get("sale_list") or {}).get("date") == sale
        on_calendar = calendar.get("status") in ("corroborated", "postponed") and listing.get("date") == sale
        if not (on_calendar or seen_on_list):
            return {**state, "status": "publisher_only", "prior_date": prior}
    if prior and sale > prior:
        return {**state, "status": "postponed", "prior_date": prior}
    return {**state, "status": "listed"}


def track_of(state: Dict[str, Any], previous: Any, today: date) -> Dict[str, Any]:
    """The small memory kept on the row so the next run can see a change."""
    previous = previous if isinstance(previous, dict) else {}
    same = previous.get("status") == state["status"] and previous.get("dates") == state["dates"]
    return {"dates": state["dates"], "status": state["status"],
            "since": previous.get("since") if same and previous.get("since") else today.isoformat()}


# ── Evidence helpers ────────────────────────────────────────────────────────


def county_evidence(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Owner, parcel and value from a county record already on the row.
    Order: a reviewed county match, the Shelby realty bill, the Comptroller roll."""
    seed = row.get("seed") if isinstance(row.get("seed"), dict) else None
    if seed and seed.get("owner"):
        return {"owner": seed["owner"], "parcel": seed.get("parcel"), "value": seed.get("value"),
                "value_basis": seed.get("value_basis"), "estimate": bool(seed.get("value_is_estimate")),
                "url": seed.get("url"), "on": _day(seed.get("observed_on")),
                "label": seed.get("label") or "County parcel record", "role": seed.get("role") or "owner on the county roll",
                "independent": True}
    review = row.get("trustee_review") if isinstance(row.get("trustee_review"), dict) else None
    if review and review.get("status") == "verified_assessment" and review.get("owner"):
        return {"owner": review["owner"], "parcel": review.get("parcel_id"), "value": review.get("estimated_appraisal"),
                "value_basis": f"{review.get('tax_year')} Shelby County realty bill assessment of "
                               f"{_money(review.get('assessed_value') or 0)} divided by the "
                               f"{float(review.get('assessment_ratio') or 0):.0%} class ratio. An estimate, not an appraisal",
                "estimate": True, "url": review.get("source_url"), "on": _day(review.get("checked_on")),
                "label": f"Shelby County Trustee realty bill, parcel {review.get('parcel_id')}",
                "role": "taxpayer on the county realty bill", "independent": True}
    comp = row.get("comptroller") if isinstance(row.get("comptroller"), dict) else None
    if comp and comp.get("owner") and comp.get("parcelid") and comp.get("method") == "address":
        version = str(comp.get("version") or "")
        on = _day(version[-10:])
        return {"owner": comp["owner"], "parcel": comp["parcelid"], "value": comp.get("appraisal"),
                "value_basis": VALUE_BASIS["comptroller"][0], "estimate": False, "url": None, "on": on,
                "label": f"Tennessee Comptroller parcel file for {str(comp.get('county') or '').title()} County "
                         f"({version or 'undated'}), matched by street address", "role": "owner on the county roll",
                # A bulk street match made when the record was staged. The Nov 22
                # review did not accept it without a second, exact county check.
                "independent": False}
    return None


def _value(row: Dict[str, Any], evidence: Optional[Dict[str, Any]], checked: Optional[str]) -> Dict[str, Any]:
    if evidence and evidence.get("value"):
        return {"value": float(evidence["value"]), "value_basis": evidence["value_basis"],
                "value_is_estimate": evidence["estimate"],
                "value_source": source(evidence["label"], evidence.get("url"), evidence.get("on"))}
    if row.get("property_value"):
        basis, estimate = VALUE_BASIS.get(row.get("property_value_source") or "",
                                          (f"Value from {row.get('property_value_source') or 'an unnamed source'}. "
                                           "Basis not documented, treat as an estimate", True))
        return {"value": float(row["property_value"]), "value_basis": basis, "value_is_estimate": estimate,
                "value_source": source(f"Staging value, source {row.get('property_value_source') or 'unknown'}",
                                       None, checked)}
    return {"value": None, "value_basis": None, "value_is_estimate": False, "value_source": None}


def _loan_debt(amount: Any, loan_date: Any, instrument: Any) -> List[Dict[str, Any]]:
    if not amount:
        return []
    inst = re.sub(r"\D", "", str(instrument or ""))
    url = detail_url(inst, f"20{inst[:2]}") if len(inst) == 8 else None
    label = (f"Shelby County Register of Deeds index, deed of trust instrument {inst}" if inst
             else "Shelby County Register of Deeds index, deed of trust")
    return [debt("mortgage_original", float(amount), _day(loan_date), source(label, url, _day(loan_date)),
                 "Original loan amount on the recorded deed of trust. Not a payoff.")]


def lead_time(link: Dict[str, Any]) -> str:
    """'9 days before the first advertisement', or the honest reverse."""
    days = link.get("days_gained")
    if days is None:
        return ""
    if days > 0:
        return f"{days} days before the first advertisement"
    if days < 0:
        return f"{-days} days after the first advertisement, so it gave no head start"
    return "the same day as the first advertisement"


SALE_UNKNOWNS = [
    "Current payoff, arrears and fees",
    "Other liens, judgments and unpaid taxes",
    "Whether the owner wants to sell",
    "Current deed and everyone who must sign",
]


# ── Cards ───────────────────────────────────────────────────────────────────


def appointment_card(row: Dict[str, Any], link: Dict[str, Any], today: date,
                     notice_carded: bool = False, ranked: Optional[bool] = None) -> Dict[str, Any]:
    """Card for one Shelby appointment of substitute trustee.

    row: id, property_address, source_url, property_value,
    property_value_source, raw (the staging raw_payload).
    """
    raw = row.get("raw") or {}
    instrument = str(raw.get("instrument") or "")
    recorded = _day(raw.get("recorded"))
    borrowers = split_names(raw.get("borrowers"))
    recovery = raw.get("recovery") if isinstance(raw.get("recovery"), dict) else {}
    loan_names = split_names(recovery.get("loan_grantors"))
    lender = (raw.get("lenders") or [None])[0]
    trustees = ", ".join(raw.get("substitute_trustees") or [])
    county = raw.get(COUNTY_KEY) if isinstance(raw.get(COUNTY_KEY), dict) else {}
    county_ok = bool(county.get("roll_owner")) and county.get("status", "verified_assessment") in (
        "verified_assessment", "matched", "assessment_unverified")
    owner = county.get("roll_owner") if county_ok else None
    # The county bill's parcel number is the shared key across lanes; the
    # Register prints the same parcel without spacing.
    parcel = (county.get("parcel_id") if county_ok else None) or raw.get("parcel_id")
    address = row.get("property_address")
    ref = raw.get("deed_of_trust") if isinstance(raw.get("deed_of_trust"), dict) else {}
    loan = raw.get("loan") if isinstance(raw.get("loan"), dict) else {}
    register = source(f"Shelby County Register of Deeds index, appointment of substitute trustee, instrument {instrument}",
                      row.get("source_url"), recorded)
    age = (today - date.fromisoformat(recorded)).days if recorded else None
    linked = link.get("status") == "linked"

    what = f"Appointment of substitute trustee recorded {recorded or 'on an unknown date'} (Shelby instrument {instrument})"
    if lender:
        what += f" for {lender}"
    if trustees:
        what += f", naming {trustees}"
    what += ". This is the step a lender takes before advertising a foreclosure sale."
    if linked:
        what += (f" A sale notice ({link['notice_id']}) was first advertised {link['first_advertised']}"
                 + (f", with the sale set for {link['sale_date']}." if link.get("sale_date") else ".")
                 + (f" The appointment was recorded {lead_time(link)}." if lead_time(link) else ""))
    else:
        what += " No sale notice for this loan has been found yet."
    status = "active" if linked or age is None or age <= APPOINTMENT_QUIET_DAYS else "unknown"

    if parcel or address:
        if ref.get("instrument"):
            tie = (f"The appointment cross-references deed of trust instrument {ref['instrument']}; the Register's index "
                   "page for that deed of trust carries this parcel and street")
            asset_src = source(f"Shelby County Register of Deeds index, deed of trust instrument {ref['instrument']}",
                               detail_url(ref["instrument"], ref.get("year") or f"20{ref['instrument'][:2]}"),
                               raw.get("dot_checked_at"))
        elif county.get("notice_source"):
            tie = (f"Published notice cites appointment instrument {instrument} and names this street; the county "
                   "realty bill for that street carries this parcel")
            asset_src = source("Memphis Daily News foreclosure notice", county["notice_source"], county.get("notice_dated"))
        else:
            tie = "Parcel and street as recorded on the staging record"
            asset_src = register
        description = "Real property securing the loan under the substitute trustee"
    else:
        tie, asset_src, description = "", None, "Property not yet identified"

    economics: Dict[str, Any] = {"deadline": link.get("sale_date") if linked else None,
                                 "deadline_kind": "Advertised trustee sale" if linked and link.get("sale_date")
                                 else "Unknown. No sale date advertised yet"}
    if county_ok and county.get("estimated_appraisal"):
        economics.update(_value({}, {"value": county["estimated_appraisal"], "estimate": True,
                                     "value_basis": f"{county.get('tax_year')} Shelby County realty bill assessment of "
                                                    f"{_money(county.get('assessed_value') or 0)} divided by the "
                                                    f"{float(county.get('assessment_ratio') or 0):.0%} class ratio. "
                                                    "An estimate, not an appraisal",
                                     "label": f"Shelby County Trustee realty bill, parcel {county.get('parcel_id')}",
                                     "url": county.get("county_source"), "on": county.get("checked_on")}, None))
    else:
        economics.update(_value(row, None, today.isoformat()))
    economics["known_debt"] = _loan_debt(loan.get("original_amount"), loan.get("loan_date"), ref.get("instrument"))
    unknowns = list(SALE_UNKNOWNS)
    if not linked:
        unknowns.insert(0, "Sale date. None advertised yet")
    unknowns.append("Whether the default has been cured since the appointment")
    if not economics["known_debt"]:
        unknowns.append("Original loan amount (deed of trust page not read)")
    economics["unknowns"] = unknowns

    borrower_text = ", ".join(borrowers) or None
    all_borrowers = borrowers + loan_names
    conflicts: List[str] = []
    verify: List[str] = []
    hold: Optional[str] = None
    if link.get("status") == "conflict":
        hold = "A published notice is tied to this appointment but describes a different property: " + link["reason"]
        conflicts.append(link["reason"])
        verify.append("Read the notice and the deed of trust and settle which parcel the appointment covers")
    if owner and all_borrowers and not names_agree(owner, all_borrowers):
        conflicts.append(f"The county bill is in the name of {owner}; the borrower on the appointment is {borrower_text}")
        hold = hold or ("The taxpayer on the county bill and the borrower on the appointment are different names, "
                        "so who can sell is not settled")
        verify.append("Check the Register for a deed out of the borrower or an estate before treating either as the seller")
    if owner:
        decider = {"name": owner, "verified": not conflicts and bool(all_borrowers),
                   "role": "Taxpayer on the county realty bill" + (" and borrower on the appointment" if not conflicts and all_borrowers else ""),
                   "basis": (f"{county.get('tax_year') or 'Current'} Shelby County realty bill for parcel "
                             f"{county.get('parcel_id') or parcel} is in this name"
                             + (f", and the Register indexes {borrower_text} as borrower on the appointment" if borrower_text else "")
                             + ". No deed or title search has been done."),
                   "source": source(f"Shelby County Trustee realty bill, parcel {county.get('parcel_id') or parcel}",
                                    county.get("county_source"), county.get("checked_on"))}
    else:
        decider = {"name": borrower_text, "verified": False,
                   "role": "Borrower on the appointment (Register index)" if borrower_text else "Not identified",
                   "basis": ("Named as a grantor on the appointment in the Register index"
                             + (", sorted from the lender by lender words" if raw.get("party_basis") == "lender_words" else "")
                             + ". Present ownership has not been checked against a county record."
                             if borrower_text else "The Register index names no borrower on this filing."),
                   "source": register}

    if not hold:
        if not (parcel or address):
            attempts = recovery.get("detail_attempts")
            last = _day(recovery.get("last_detail_attempt"))
            if recovery.get("detail_outcome") == "no_linked_loan":
                hold = ("The appointment's Register page carries no deed of trust reference that can be opened, "
                        "so the parcel is not identified")
                verify.append("Wait for the published sale notice, which names the street, or read the recorded appointment image in person")
            elif recovery.get("source") and not recovery.get("detail_checked_at"):
                hold = ("The Register has not yet served this appointment's detail page, so the loan, parcel and "
                        "owner are not linked" + (f" ({attempts or 1} attempt(s), last {last})" if last else ""))
                verify.append("Scheduled retry reads the appointment and deed of trust pages at the Register's 30 second crawl delay")
            elif ref.get("instrument") and not raw.get("dot_checked_at"):
                hold = f"Deed of trust instrument {ref['instrument']} is referenced but its Register page has not been read yet"
                verify.append("Scheduled backfill reads the deed of trust page for the street and parcel")
            elif ref.get("instrument"):
                hold = (f"The Register's page for deed of trust instrument {ref['instrument']} carries no street or parcel, "
                        "so the property is not identified")
                verify.append("Wait for the published sale notice, which names the street")
            else:
                hold = ("The appointment's Register page carries no deed of trust reference that can be opened, "
                        "so the parcel is not identified")
                verify.append("Wait for the published sale notice, which names the street, or read the recorded appointment image in person")
        elif not owner:
            found = COUNTY_MISS.get(county.get("status"))
            hold = ("Parcel and street are known, but the current county bill has not confirmed who holds the property"
                    + (f": {found}" if found else ""))
            verify.append("Open the county realty bill for this street by hand and compare the parcel number"
                          if found else "Scheduled county bill check for this street and parcel")
        elif any(is_entity(n) for n in [owner] + borrowers):
            hold = "The owner is a company. Who signs for it is not established"
            verify.append("Look up the entity with the Tennessee Secretary of State for its registered agent and members")
        elif economics.get("value") is None:
            hold = "Owner and parcel are verified, but there is no sourced value yet"
            verify.append("Scheduled county assessment or recorded loan estimate")
        elif ranked is False:
            # Same rule as an advertised sale: the lane does not recommend paid
            # contact data for a record the ranking excludes or has not ranked yet.
            hold = ("Owner, parcel and value are verified, but the record is not on the ranked list (it fails a ranking "
                    "rule such as the expected fee floor, or the nightly ranking has not picked it up yet)")
            verify.append("Recheck after the next nightly ranking")
    if not borrower_text and not owner:
        verify.append("Read the recorded appointment for the borrower's name")

    availability = {
        "evidence": ((f"The {county.get('tax_year') or 'current'} county realty bill still carries {owner}, so no transfer "
                      "shows on the tax roll. " if owner else "")
                     + ("A sale is advertised but has not happened. " if linked and (link.get("sale_date") or "") >= today.isoformat()
                        else "No sale has been advertised for this loan in the Memphis Daily News records held here. ")
                     + "Listing status has not been checked.").strip(),
        "checked_on": county.get("checked_on") if owner else today.isoformat(), "conflicts": conflicts,
        "first_call_question": (f"A substitute trustee was appointed on your loan"
                                + (f" with {lender}" if lender else "") + f" on {recorded}. "
                                "Have you been able to work anything out with the lender since then?"),
        "verify_next": verify + ["Check for an active listing or contract before buying contact data",
                                 "Check the Register for a release, a new deed or a trustee's deed since the appointment"],
    }
    event = {"what": what, "status": status, "checked_on": today.isoformat(), "source": register}
    asset = {"description": description, "parcel": parcel, "address": address, "tie": tie, "source": asset_src}
    names = [owner] if owner else borrowers   # one sourced string, unsplit, so the cross-lane owner key agrees

    if linked and notice_carded:
        return build_review(
            lane=LANE, route="removed", reviewed_by=REVIEWER, county="Shelby", owner_names=names, today=today,
            route_reason=(f"Duplicate of the sale notice record for the same loan (notice {link['notice_id']}, record "
                          f"{link['staging_id']}). {link['basis']}. Work that card; it carries the sale date."
                          + (f" The appointment was recorded {lead_time(link)}." if lead_time(link) else "")),
            event=event, asset=asset, decider=decider, economics=economics, availability=availability)
    if hold:
        return build_review(lane=LANE, route="research_hold", route_reason=hold, reviewed_by=REVIEWER, county="Shelby",
                            owner_names=names, today=today, event=event, asset=asset, decider=decider,
                            economics=economics, availability=availability)
    return build_review(
        lane=LANE, route="first_trace_review", reviewed_by=REVIEWER, county="Shelby", owner_names=names, today=today,
        route_reason=("Early foreclosure filing with the borrower confirmed on the current county bill, an exact parcel "
                      "and a sourced value. " + ("Sale advertised." if linked else "No sale date is advertised yet, so the deadline is unknown.")),
        event=event, asset=asset, decider=decider, economics=economics, availability=availability)


def sale_card(row: Dict[str, Any], state: Dict[str, Any], today: date,
              appointment: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Card for one advertised trustee sale.

    row: id, bot_source, county, property_address, full_name,
    owner_name_records, property_value, property_value_source, source_url,
    staging_status, notice_tracking, seed, trustee_review, comptroller,
    calendar, value_estimate, notice_id, case_number, firm, borrowers.
    appointment: a linked appointment's sale_notice_link plus its instrument.
    """
    src_name = row.get("bot_source")
    tracking = row.get("notice_tracking") if isinstance(row.get("notice_tracking"), dict) else {}
    calendar = row.get("calendar") if isinstance(row.get("calendar"), dict) else {}
    firm = row.get("firm") or FIRMS.get(src_name) or "the substitute trustee"
    case = row.get("case_number") or tracking.get("case_number") or row.get("notice_id")
    sale = state.get("sale_date")
    listed_on = state.get("last_listed_on")
    status = state["status"]
    address = row.get("property_address")
    if src_name in FIRMS:
        list_url = tracking.get("report_url") or row.get("source_url")
        list_label = f"{firm} Tennessee sale list" + (f", file {case}" if case else "")
    elif calendar.get("source_url") and calendar.get("status") in ("corroborated", "postponed", "omitted"):
        list_url, list_label = calendar["source_url"], f"{firm} sale calendar, checked against notice {row.get('notice_id')}"
        listed_on = calendar.get("checked_on") or listed_on
    else:
        list_url, list_label = row.get("source_url"), f"Memphis Daily News foreclosure notice {row.get('notice_id') or ''}".strip()
    seed = row.get("seed") if isinstance(row.get("seed"), dict) else {}
    seen = seed.get("sale_list") if isinstance(seed.get("sale_list"), dict) else {}
    if seen.get("date") == sale and (seen.get("checked_on") or "") >= (listed_on or "") and src_name not in FIRMS:
        list_url, list_label, listed_on = seen.get("url"), seen.get("label") or f"{firm} sale calendar", seen["checked_on"]
    event_src = source(list_label, list_url, listed_on)

    what = f"Substitute trustee sale by {firm}" + (f" ({'file' if src_name in FIRMS else 'notice'} {case})" if case else "")
    if status == "postponed":
        what += f", postponed from {state['prior_date']} to {sale}."
        event_status = "postponed"
    elif status == "listed":
        what += f", set for {sale}. Still on the list when last read on {listed_on}."
        event_status = "active"
    elif status == "publisher_only":
        what += (f", advertised for {sale} in the newspaper notice. No trustee calendar has been matched to confirm "
                 "the date still stands.")
        event_status = "active"
    elif status == "withdrawn":
        what += (f", was set for {sale} but is no longer on the trustee's list (read {state.get('source_read_on')}). "
                 "A sale that drops off the list is usually cancelled or the loan reinstated, but the reason is not published.")
        event_status = "unknown"
    elif status == "passed":
        what += (f". The advertised sale date {sale} has passed. The outcome (sold, taken back by the lender, or a late "
                 "postponement) is not published in a free source.")
        event_status = "resolved"
    elif status == "manual":
        what += f", last advertised for {sale}. Marked {state.get('manual_status')} by hand in the CRM."
        event_status = "resolved" if str(state.get("manual_status")).lower() in ("sold", "cancelled", "canceled") else "unknown"
    elif status == "undated":
        what += ". No sale date is recorded."
        event_status = "unknown"
    else:
        what += f", last advertised for {sale}. The trustee's list has not been read since {listed_on}, so today's status is not confirmed."
        event_status = "unknown"
    if appointment and lead_time(appointment):
        what += (f" The appointment of substitute trustee (Shelby instrument {appointment.get('instrument')}) was recorded "
                 f"{appointment['appointment_recorded']}, {lead_time(appointment)}.")

    evidence = county_evidence(row)
    borrowers = split_names(row.get("borrowers")) or split_names(row.get("full_name"))
    staging_owner = row.get("owner_name_records")
    conflicts: List[str] = []
    verify: List[str] = []
    if evidence:
        others = [n for n in borrowers + split_names(staging_owner) if n]
        role = evidence["role"]
        agree = bool(others) and names_agree(evidence["owner"], others)
        if others and not agree:
            conflicts.append(f"County record shows {evidence['owner']}; the record also carries {', '.join(sorted(set(others)))}. "
                             "One of them may be a former owner, an heir or the borrower")
            verify.append("Read the latest deed or affidavit at the county Register to settle who holds title and who must sign")
        if borrowers and agree and names_agree(evidence["owner"], borrowers):
            role = role[0].upper() + role[1:] + " and borrower named by the trustee"
        else:
            role = role[0].upper() + role[1:] + (". The trustee's list does not name the borrower" if not borrowers else "")
        decider = {"name": evidence["owner"], "role": role, "verified": not conflicts and bool(evidence.get("independent")),
                   "basis": f"{evidence['label']} shows this name for the property"
                            + (f" (parcel {evidence['parcel']})" if evidence.get("parcel") else "")
                            + ". Roll owner only, no title opinion.",
                   "source": source(evidence["label"], evidence.get("url"), evidence.get("on"))}
        tie = (f"The trustee's sale list names this street; the county record for that street is parcel {evidence.get('parcel')}"
               if evidence.get("parcel") else "The trustee's sale list names this street")
        asset_src = source(evidence["label"], evidence.get("url"), evidence.get("on"))
    else:
        name = ", ".join(borrowers) or staging_owner
        decider = {"name": name, "verified": False,
                   "role": "Borrower named in the notice" if borrowers else ("Owner name on the staging record, source not documented" if name else "Not identified"),
                   "basis": ("Named as borrower in the trustee's notice. Not checked against a county ownership record."
                             if borrowers else "No county ownership record is attached to this record."),
                   "source": event_src}
        tie, asset_src = "The trustee's sale list names this street", event_src
    parcel = evidence.get("parcel") if evidence else None

    economics: Dict[str, Any] = {"deadline": sale, "deadline_kind": "Trustee sale date" + (" (postponed)" if status == "postponed" else "")}
    economics.update(_value(row, evidence, listed_on))
    estimate = row.get("value_estimate") if isinstance(row.get("value_estimate"), dict) else {}
    economics["known_debt"] = _loan_debt(estimate.get("original_loan"), estimate.get("loan_date"),
                                         estimate.get("instrument") or row.get("loan_instrument"))
    economics["unknowns"] = list(SALE_UNKNOWNS) + ([] if economics["known_debt"] else ["Original loan amount"])

    owner_line = (f"County record still shows {evidence['owner']} ({evidence.get('on') or 'undated'}). " if evidence else "")
    availability = {
        "evidence": (f"On the trustee's list for {sale} when last read on {listed_on}. " if status in ("listed", "postponed") else "")
                    + owner_line + "Listing and contract status have not been checked.",
        "checked_on": listed_on, "conflicts": conflicts,
        "first_call_question": (f"The trustee has {str(address).split(',')[0]} set for sale on {sale}. "
                                "Is the loan still behind, and has anyone already offered to buy or list the house?"),
        "verify_next": verify + ["Check for an active listing or contract before buying contact data",
                                 "Reread the trustee's list on the morning of any call. Dates move"],
    }
    if row.get("staging_status") == "promoted":
        conflicts.append("Already promoted into the live lead list. Check that record's status and any prior contact first")
    names = [evidence["owner"]] if evidence else (borrowers or [staging_owner])   # as sourced, unsplit
    common = dict(lane=LANE, reviewed_by=REVIEWER, county=row.get("county"), owner_names=names, today=today,
                  event={"what": what, "status": event_status, "checked_on": listed_on, "source": event_src},
                  asset={"description": "Real property under the deed of trust being foreclosed", "parcel": parcel,
                         "address": address, "tie": tie, "source": asset_src},
                  decider=decider, economics=economics, availability=availability)

    if status in ("passed", "manual") and event_status == "resolved":
        reason = (f"The advertised sale date {sale} has passed and no later date is published. The pre-sale window is closed."
                  if status == "passed" else f"Marked {state.get('manual_status')} by hand in the CRM.")
        return build_review(route="removed", route_reason=reason, **common)
    hold = None
    if status == "withdrawn":
        hold = (f"No longer on {firm}'s sale list as of {state.get('source_read_on')}. Likely cancelled or reinstated, not confirmed")
        availability["verify_next"].insert(0, "Check the publisher's notice and the trustee for a new date or a cancellation")
    elif status == "publisher_only":
        hold = "The sale date rests on the newspaper notice alone. No trustee calendar confirms it is still set"
        availability["verify_next"].insert(0, "Match the notice to the trustee's own sale calendar by street, ZIP and date")
    elif seed.get("hold"):
        hold = seed["hold"]
        availability["verify_next"] = list(seed.get("verify_next") or []) + availability["verify_next"]
    elif status in ("unverified", "undated", "manual"):
        hold = "Today's sale status is not confirmed by the trustee's list"
        availability["verify_next"].insert(0, "Reread the trustee's sale list")
    elif conflicts:
        hold = "Who can sell is not settled: " + conflicts[0]
    elif not evidence:
        hold = "No county ownership record ties a current owner and parcel to this sale"
        availability["verify_next"].insert(0, "Match the street to the county parcel record and compare the owner with the borrower")
    elif not evidence.get("independent"):
        hold = ("The owner and parcel come from a bulk street match on the state roll. No second, exact county "
                "record has confirmed them")
        availability["verify_next"].insert(0, "Confirm owner, parcel and value on the county assessor or trustee record for this exact street")
    elif row.get("ranked") is False:
        hold = ("Not on the ranked list (it fails a ranking rule such as the expected fee floor), so it is not "
                "recommended for a paid contact lookup")
        availability["verify_next"].insert(0, "No action unless the ranking inputs change")
    elif is_entity(evidence["owner"]):
        hold = "The owner is a company, trust or estate. Who signs for it is not established"
        availability["verify_next"].insert(0, "Identify the signer from the Secretary of State record or the recorded trust or estate papers")
    elif economics.get("value") is None:
        hold = "Owner and parcel are matched, but there is no sourced value"
        availability["verify_next"].insert(0, "County assessment for the matched parcel")
    if hold:
        return build_review(route="research_hold", route_reason=hold, **common)
    return build_review(
        route="first_trace_review",
        route_reason=(f"Sale {'postponed to' if status == 'postponed' else 'set for'} {sale} and still on the trustee's list; "
                      "county record gives the owner, parcel and value."),
        **common)


def same_card(a: Any, b: Any) -> bool:
    """Equal apart from the review timestamp, so a repeat run writes nothing."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return False
    return {k: v for k, v in a.items() if k != "reviewed_at"} == {k: v for k, v in b.items() if k != "reviewed_at"}
