"""Land lane pilot review: a tract advances only on a second, current event.

Acreage, an out-of-state mailing address and a family surname are prospecting
signals. A land record becomes a first-trace recommendation only when an
independent, current event is tied to the exact tract:

  tax          a delinquent tax suit or scheduled sale that prints the parcel
  foreclosure  a trustee sale notice whose street address resolved to the parcel
  partition    a partition or court sale notice resolved the same way
  estate       an open estate whose decedent is an owner of record on the tract

"Exact tract" is the county parcel control (_land_identity.parcel_control, or
the Hamilton map-group-parcel). A link made only by a debtor or owner name, a
shared surname or a nearby parcel never counts. A paid, sold, removed, past or
held event never counts; it is listed on the card with the reason.

Pure functions (no network, no database). land_review_bot does the reads and
the guarded writes.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._comptroller import county_key
from ._hamilton_herald_assessor import CONTROL as HAMILTON_CONTROL
from .comptroller_refresh import BASE as ROLL_BASE, COUNTIES as ROLL_COUNTIES
from ._land_identity import parcel_control
from ._land_qualification import ACTIVE, source_event
from ._pilot_review import build_review, debt, source

REVIEWER = "land_second_event"
ESTATE_CURRENT_DAYS = 365      # an estate cannot close inside the 4-month claims period and often runs a year
NOTICE_CURRENT_DAYS = 365
HAMILTON_NS = "hamilton_roll"
HAMILTON_ASSESSOR_URL = "https://www.hamiltontn.gov/_downloadsAssessor/AssessorExportCSV.zip"
HAMILTON_TRUSTEE_URL = "https://www.hamiltontn.gov/_downloadsTrusteeDelinquent/CTRUDELQCSV.zip"

TAX_TYPES = {"TAX_LIEN", "TAX_DELINQUENT", "TAX_SALE"}
FORECLOSURE_TYPES = {"PRE_FORECLOSURE", "FORECLOSURE", "TRUSTEE_NOTICE", "SUBSTITUTE_TRUSTEE"}
PARTITION_TYPES = {"PARTITION", "COURT_SALE"}
EVENT_TYPES = TAX_TYPES | FORECLOSURE_TYPES | PARTITION_TYPES | {"PROBATE"}

_ETAL = re.compile(r"\bET\s?AL\b|\bETAL\b")
_HEIRS = re.compile(r"\b(HEIRS?( OF)?|EST( OF)?|ESTATE( OF)?|DECD|DECEASED|DEC'D)\b")
_LIFE = re.compile(r"\bLIFE\s+ESTATE\b|\(LE\)|\bL/E\b")
_NAME_NOISE = re.compile(r"\b(JR|SR|II|III|IV|ETUX|ET UX|ETVIR|ET VIR|TRUSTEE|TR|AKA|FKA|NKA|MR|MRS|MS|DR)\b")

REASONS = {
    "name_link_only": "it is linked to this parcel only by a name match, not by a printed parcel or street address",
    "decedent_not_owner_of_record": "the decedent's full name is not an owner of record on this tract",
    "paid_before_sale": "the county marked it PAID before the sale",
    "removed_from_sale": "the county removed it from the sale",
    "sold": "it was sold",
    "past_sale_date": "the sale date has passed and the outcome is not confirmed",
    "sale_date_unknown": "the list gives no future sale date",
    "source_record_held": "the source record is held or rejected in review",
    "stale_notice": "the notice is more than a year old and the case status is not confirmed",
    "no_sale_date": "the notice has no future sale date",
    "event_owner_mismatch": "the party named in the notice is not the owner of record",
}


def obj(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _key(value: Any) -> str:
    return " ".join(str(value or "").upper().split())


def _day(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%A, %B %d, %Y", "%m/%d/%Y", "%Y%m%d"):
        try:
            return datetime.strptime(text[:10] if fmt == "%Y-%m-%d" else text, fmt).date()
        except ValueError:
            pass
    return None


def _iso_text(value: Any) -> Optional[str]:
    day = _day(value)
    return day.isoformat() if day else (str(value) if value else None)


def holder_remains(prior_owner: Any, current_owner: Any) -> bool:
    """True when someone on the current roll line was already on the prior
    line (a spouse dropped off, a name was respelled): title did not leave
    the family, so the row is rechecked rather than removed."""
    prior = set(re.sub(r"[^A-Z' ]", " ", _key(prior_owner)).split())
    for name in owner_group(current_owner)["names"]:
        person = _person(name, True)
        if person and person[0] in prior and person[1] in prior:
            return True
    return False


def hamilton_control(text: Any) -> Optional[str]:
    """'146O P 027 Commonly known as' and '146O-P-027' are the same parcel."""
    m = HAMILTON_CONTROL.match(str(text or "").strip().upper())
    return "-".join((m[1], m[2] or "", m[3])) if m else None


# ── names ────────────────────────────────────────────────────────────────

def _person(text: Any, roll_order: bool) -> Optional[Tuple[str, str, str]]:
    """(surname, first, middle initial). Roll names are LAST FIRST MIDDLE."""
    cleaned = re.sub(r"[^A-Z' ]", " ", str(text or "").upper())
    cleaned = _NAME_NOISE.sub(" ", _HEIRS.sub(" ", _ETAL.sub(" ", cleaned)))
    tokens = cleaned.split()
    if len(tokens) < 2:
        return None
    if roll_order:
        last, first, rest = tokens[0], tokens[1], tokens[2:]
    else:
        last, first, rest = tokens[-1], tokens[0], tokens[1:-1]
    return last, first, (rest[0][0] if rest else "")


def same_person(prose_name: Any, roll_name: Any) -> bool:
    """Full-name agreement between a notice ('Gary Lee Adcock') and a roll owner
    ('ADCOCK GARY L ET AL'). Surname alone is never enough."""
    a, b = _person(prose_name, False), _person(roll_name, True)
    if not a or not b or a[0] != b[0] or a[1] != b[1]:
        return False
    return not a[2] or not b[2] or a[2] == b[2]


def owner_group(owner_text: Any) -> Dict[str, Any]:
    """Every name on the roll line, and whether the line itself says the group
    is incomplete (ET AL) or held by heirs or an estate."""
    text = _key(owner_text)
    names = [n.strip() for n in re.split(r"\s*&\s*|\s*;\s*", text) if n.strip()]
    etal, heirs = bool(_ETAL.search(text)), bool(_HEIRS.search(_LIFE.sub(" ", text)))
    return {"text": text, "names": names, "etal": etal, "heirs_or_estate": heirs,
            "life_estate": bool(_LIFE.search(text)), "complete": bool(names) and not etal and not heirs}


def _roll_url(county: str) -> str:
    name = next((c for c in ROLL_COUNTIES if county_key(c) == county_key(county)), county.title())
    return ROLL_BASE + name + ".zip"


# ── the tract ────────────────────────────────────────────────────────────

def tract(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The exact tract a staging row describes, or None when the row does not
    identify one parcel on a county roll."""
    raw = obj(row.get("raw_payload"))
    land, roll, ham = obj(raw.get("land")), obj(raw.get("comptroller_parcel")), obj(raw.get("hamilton_assessor"))
    if row.get("bot_source") == "land_lane" or land:
        county = _key(land.get("county") or row.get("county")).lower()
        parcel = land.get("parcel_id") or roll.get("parcelid")
        identity = parcel_control(raw.get("source"), county, parcel)
        if not identity:
            return None
        gis = raw.get("source") == "county_gis"
        owner = land.get("owner_of_record") or roll.get("owner") or row.get("owner_name_records")
        return {"kind": "land_lane", "identity": identity, "county": identity[1], "parcel": parcel,
                "owner": _key(owner), "row_owner": _key(row.get("owner_name_records")),
                "value": roll.get("appraisal") or row.get("property_value"),
                "value_basis": ("County appraisal from the county parcel map service" if gis else
                                "County assessor appraisal on the Tennessee Comptroller parcel roll") + ", not a market price",
                "value_source": source("County parcel map service" if gis else "Tennessee Comptroller parcel roll, "
                                       + identity[1].title() + " County", land.get("source_url") or (None if gis else _roll_url(identity[1])),
                                       land.get("observed_on")),
                "acres": land.get("acres") or roll.get("acres"), "address": row.get("property_address"),
                "land_use": land.get("land_use") or roll.get("land_use"),
                "signals": land.get("ownership_signals") or land.get("signals") or [],
                "last_transfer": _iso_text(roll.get("last_sale_date") or land.get("held_since")),
                "legal": None, "mailing": roll.get("owner_mailing"),
                "acquisition_status": land.get("status"), "changed_fields": land.get("changed_fields") or []}
    if _key(raw.get("county") or row.get("county")).lower() == "hamilton" and ham.get("parcel"):
        parcel = hamilton_control(raw.get("parcel"))
        if not parcel or parcel != hamilton_control(ham.get("parcel")):
            return None
        file_date = obj(raw.get("hamilton_roll_valuation")).get("assessor_file_date")
        return {"kind": "hamilton", "identity": (HAMILTON_NS, "hamilton", parcel), "county": "hamilton", "parcel": parcel,
                "owner": _key(ham.get("owner")), "row_owner": _key(row.get("owner_name_records")),
                "value": ham.get("appraised") or row.get("property_value"),
                "value_basis": "Hamilton County Assessor appraisal, not a market price",
                "value_source": source("Hamilton County Assessor export file", HAMILTON_ASSESSOR_URL, file_date),
                "acres": ham.get("calc_acres"), "address": row.get("property_address"),
                "land_use": ham.get("land_use"), "signals": [], "last_transfer": _iso_text(ham.get("last_sale_date")),
                "legal": ham.get("legal_description"), "mailing": ham.get("owner_mailing"),
                "acquisition_status": None, "changed_fields": [],
                "roll_balance": raw.get("cumulative_owed"), "roll_years": raw.get("years_delinquent"),
                "roll_latest_year": raw.get("last_bill_year")}
    return None


# ── candidate events from staging rows ───────────────────────────────────

def _event(row, kind, identity, link, status, detail, what, **extra):
    return {"staging_id": row.get("id"), "source": row.get("bot_source"), "kind": kind, "identity": list(identity),
            "link": link, "status": status, "detail": detail, "what": what, "source_url": row.get("source_url"),
            "case_id": extra.get("case_id"), "date": extra.get("date"), "sale_date": extra.get("sale_date"),
            "subject": extra.get("subject"), "representatives": extra.get("representatives") or []}


def _roll_identities(row, raw) -> List[Tuple[str, str, str]]:
    roll = obj(raw.get("comptroller_parcel"))
    county = _key(row.get("county")).lower()
    if _key(roll.get("county")).lower() != county:
        return []
    ids = [roll.get("parcelid")] + [obj(p).get("parcelid") for p in roll.get("parcels") or []]
    out = []
    for pid in ids:
        control = parcel_control("comptroller_roll", county, pid)
        if control and control not in out:
            out.append(control)
    return out


def staged_events(row: Dict[str, Any], today: date) -> List[Dict[str, Any]]:
    """Candidate second events carried by one staging row, keyed to exact
    parcels. Each says how it was linked and whether it is still current."""
    raw = obj(row.get("raw_payload"))
    kind_of = row.get("distress_type")
    county = _key(row.get("county")).lower()
    if kind_of not in EVENT_TYPES or not county or row.get("bot_source") == "land_lane":
        return []
    held = row.get("staging_status") == "rejected" or bool(row.get("rejection_reason"))
    published = _day(raw.get("publication_date"))
    sale = _day(row.get("trustee_sale_date")) or _day(raw.get("advertised_auction_date"))
    notice = obj(raw.get("court_notice"))
    method = obj(raw.get("comptroller_parcel")).get("method")

    if kind_of in TAX_TYPES:
        event = source_event(row, today)   # strict producer shapes: printed parcel joined to the roll
        if not event:
            return []
        identity = parcel_control(event["parcel_namespace"], event["county"], event["parcel_id"])
        if not identity:
            return []
        status = event["status"]
        current = status in ACTIVE and not (event.get("sale_date") and (_day(event["sale_date"]) or date.min) < today)
        detail = None if current else ("past_sale_date" if status in ACTIVE else status)
        label = {"scheduled_tax_sale": "Scheduled delinquent tax sale", "tax_suit_notice": "Delinquent tax suit notice",
                 "published_delinquency": "Published county delinquent tax list"}.get(status, "Delinquent tax notice")
        return [_event(row, "tax", identity, "printed_parcel", "active" if current else "resolved", detail,
                       label + " printing parcel " + str(event.get("printed_parcel")),
                       case_id=event.get("case_id"), date=event.get("publication_date"), sale_date=event.get("sale_date"),
                       subject=event.get("owner"))]

    if kind_of == "PROBATE":
        if method not in ("owner_name", "address") or not published or published > today:
            return []
        decedent = notice.get("decedent") or raw.get("subject") or row.get("full_name")
        reps = [obj(r).get("name") for r in notice.get("representatives") or [] if obj(r).get("name")]
        fresh = (today - published).days <= ESTATE_CURRENT_DAYS
        status, detail = ("active", None) if fresh and not held else ("unknown", "source_record_held" if held else "stale_notice")
        what = "Estate of " + str(decedent) + ": notice to creditors published " + published.isoformat()
        return [_event(row, "estate", identity, "decedent_name", status, detail, what, case_id=notice.get("docket"),
                       date=published.isoformat(), subject=decedent, representatives=reps)
                for identity in _roll_identities(row, raw)]

    label = "Partition or court sale notice" if kind_of in PARTITION_TYPES else "Foreclosure sale notice"
    kind = "partition" if kind_of in PARTITION_TYPES else "foreclosure"
    if held:
        status, detail = "unknown", "source_record_held"
    elif sale:
        status, detail = ("active", None) if sale >= today else ("resolved", "past_sale_date")
    elif kind == "partition" and published and (today - published).days <= NOTICE_CURRENT_DAYS:
        status, detail = "active", None
    else:
        status, detail = "unknown", "stale_notice" if published else "no_sale_date"
    extra = dict(case_id=notice.get("docket") or raw.get("case_number"), date=published.isoformat() if published else None,
                 sale_date=sale.isoformat() if sale else None, subject=row.get("owner_name_records") or row.get("full_name"))
    if row.get("bot_source") == "hamilton_county_herald":
        parcel = hamilton_control(raw.get("parcel"))
        return [_event(row, kind, (HAMILTON_NS, "hamilton", parcel), "printed_parcel", status, detail,
                       label + " printing parcel " + parcel, **extra)] if parcel and county == "hamilton" else []
    link = "address" if method == "address" else "name_only"
    return [_event(row, kind, identity, link, status, detail,
                   label + (" for " + str(row.get("property_address")) if row.get("property_address") else ""), **extra)
            for identity in _roll_identities(row, raw)[:1]]


# ── the rule ─────────────────────────────────────────────────────────────

def review(row: Dict[str, Any], land: Dict[str, Any], events: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    """Split the candidates on this exact tract into counted and not counted."""
    group = owner_group(land["owner"])
    matched, rejected = [], []
    for event in sorted(events, key=lambda e: (str(e.get("kind")), str(e.get("staging_id")), str(e.get("case_id")))):
        if tuple(event.get("identity") or ()) != tuple(land["identity"]):
            continue                                  # a different parcel is not a candidate at all
        if event.get("staging_id") == row.get("id") or event.get("source") == row.get("bot_source"):
            continue                                  # the row's own source is not a second, independent event
        reason = None
        if event.get("link") == "name_only":
            reason = "name_link_only"
        elif event["kind"] == "estate" and not any(same_person(event.get("subject"), n) for n in group["names"]):
            reason = "decedent_not_owner_of_record"
        elif event.get("status") != "active":
            reason = event.get("detail") or "source_record_held"
        (rejected if reason else matched).append({**event, "reason": reason})
    return {"matched": matched, "rejected": rejected, "group": group}


def _reason_text(event):
    return REASONS.get(event.get("reason"), str(event.get("reason") or "").replace("_", " "))


def _event_source(event, today):
    label = {"tax": "Tax notice", "foreclosure": "Foreclosure notice", "partition": "Court sale notice",
             "estate": "Probate notice to creditors"}.get(event["kind"], "Notice")
    if event.get("source") == "hamilton_clerk_master":
        label = "Hamilton County Clerk and Master delinquent tax sale list"
    if event.get("case_id"):
        label += ", case " + str(event["case_id"])
    return source(label, event.get("source_url"), event.get("date") or today)


def build_card(row: Dict[str, Any], land: Dict[str, Any], result: Dict[str, Any], today: date,
               hamilton_roll: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """One pilot_review card for a land tract.

    hamilton_roll: {"file_date": iso, "parcels": {control: {"total", "years", "latest_year"}}} from the Trustee's
    current delinquent file, or None when it was not read this run.
    """
    group, matched, rejected = result["group"], result["matched"], result["rejected"]
    ham = land["kind"] == "hamilton"
    acres = land.get("acres")
    where = land.get("address") or "no street address on the roll"
    description = (("Vacant land" if "UNDEVELOPED" in _key(land.get("land_use")) or "VACANT" in _key(land.get("land_use")) else "Land tract")
                   + (", " + format(float(acres), "g") + (" acre" if float(acres) == 1 else " acres") + " on the county file" if acres else "")
                   + ", " + land["county"].title() + " County parcel " + " ".join(str(land["parcel"]).split()))
    roll_src = land["value_source"]
    unknowns = ["Mortgage, liens and payoff were not checked", "Whether the owner wants to sell is unknown",
                "Access not verified: no road frontage or easement record was read"
                + ((". County legal description: " + str(land["legal"])) if land.get("legal") else "")]
    if ham and acres and float(acres) == 1.0:
        unknowns.append("The county file shows exactly 1.00 acre, which may be a placeholder; confirm the lot size on the plat")
    debts, conflicts, verify = [], [], []
    removed_reason = None

    # The row's own first signal, and whether the county still shows it.
    if ham:
        on_roll = None
        balance, years, latest, as_of = land.get("roll_balance"), land.get("roll_years"), land.get("roll_latest_year"), None
        if hamilton_roll is not None:
            entry = obj(hamilton_roll.get("parcels")).get(land["parcel"])
            on_roll, as_of = entry is not None, hamilton_roll.get("file_date")
            if entry:
                balance, years, latest = entry.get("total"), entry.get("years"), entry.get("latest_year")
        trustee_src = source("Hamilton County Trustee delinquent tax file", HAMILTON_TRUSTEE_URL, as_of)
        if on_roll is False:
            removed_reason = ("The parcel is no longer on the Hamilton County Trustee delinquent tax file dated "
                              + str(as_of) + ", so the tax delinquency that put it here is paid or cleared.")
            first = "Delinquent county property tax, now cleared from the Trustee file"
        else:
            first = ("Delinquent county property tax on the Trustee file: $" + format(float(balance or 0), ",.2f")
                     + " across " + str(years or "?") + " bill year(s), latest " + str(latest or "?"))
            if balance:
                debts.append(debt("delinquent_tax", balance, as_of, trustee_src,
                                  "Trustee file balance for all listed years. Not a payoff: court costs and fees are not included."))
        if land["row_owner"] and land["owner"] != land["row_owner"]:
            conflicts.append("The Assessor now shows " + land["owner"].title() + " as owner; the delinquent tax row names "
                             + land["row_owner"].title() + ".")
            if not holder_remains(land["row_owner"], land["owner"]):
                removed_reason = ("Ownership changed: the Assessor file shows " + land["owner"].title()
                                  + (" after a transfer dated " + str(land["last_transfer"]) if land.get("last_transfer") else "")
                                  + ". The owner on this delinquent tax row no longer holds the lot.")
        first_status = "resolved" if removed_reason else "active"
        first_src = trustee_src
    else:
        names = {"heirs": "the roll names heirs or an estate", "family_etal": "a long-held family group (ET AL)",
                 "absentee": "an out-of-state owner holding 25 years or more"}
        first = ("Ownership research signal only: " + (", ".join(names.get(s, s) for s in land["signals"]) or "land lane tract")
                 + ". This is a reason to research, not a reason to sell")
        first_status, first_src = "unknown", roll_src
        if land["row_owner"] and land["owner"] != land["row_owner"] or "owner_of_record" in land["changed_fields"]:
            conflicts.append("The owner of record changed since this tract was first staged; recheck the county roll.")

    not_counted = "; ".join(e["what"] + (", case " + str(e["case_id"]) if e.get("case_id") else "")
                            + ": not counted because " + _reason_text(e) for e in rejected)
    for e in rejected:
        if e["reason"] in ("past_sale_date", "sale_date_unknown", "stale_notice", "no_sale_date"):
            verify.append("Confirm the outcome of " + e["what"] + (" (case " + str(e["case_id"]) + ")" if e.get("case_id") else "")
                          + " with the clerk: an unsold or continued sale would count")
        elif e["reason"] == "name_link_only":
            verify.append("Confirm the parcel named in " + e["what"] + " against the recorded instrument before counting it")

    # Who can decide: every owner on the roll, or a court path for an estate.
    estate = next((e for e in matched if e["kind"] == "estate"), None)
    sole_decedent = bool(estate and len(group["names"]) == 1 and not group["etal"]
                         and same_person(estate.get("subject"), group["names"][0]))
    if group["complete"]:
        decider = {"name": group["text"].title(), "role": "Owner of record",
                   "basis": "Every name on the county roll line for this parcel; the line does not say ET AL, heirs or estate",
                   "verified": True, "source": roll_src}
        decider_ok = True
    elif sole_decedent and estate.get("representatives"):
        decider = {"name": " and ".join(estate["representatives"]),
                   "role": "Personal representative of the estate of the sole owner of record",
                   "basis": "Named in the notice to creditors" + (" in case " + str(estate["case_id"]) if estate.get("case_id") else "")
                            + ". Appointment alone does not prove authority to sell the land",
                   "verified": False, "source": _event_source(estate, today)}
        unknowns.append("Power of sale under the will or a court order is not verified; the heirs or devisees may have to sign")
        decider_ok = True
    else:
        why = ("the roll line says ET AL, so not every co-owner is named" if group["etal"] else
               "the roll names heirs or an estate, so the people who can sign are not named" if group["heirs_or_estate"] else
               "no owner name is on the roll")
        decider = {"name": group["text"].title() or None, "role": "Owner group not verified",
                   "basis": "County roll line; " + why + ". A sale needs every owner or a court path (estate or partition)",
                   "verified": False, "source": roll_src}
        decider_ok = False
        verify.append("Pull the vesting deed" + (" (last transfer " + str(land["last_transfer"]) + ")" if land.get("last_transfer") else "")
                      + " to name every co-owner or heir")
        if estate and not estate.get("representatives"):
            verify.append("Read the full notice or docket" + (" " + str(estate["case_id"]) if estate.get("case_id") else "")
                          + " for the personal representative's name and whether the will gives a power of sale")
        elif estate:
            verify.append("The estate holds only the decedent's share. Ask "
                          + " and ".join(estate["representatives"]) + "'s counsel who the other co-owners are")

    value_ok = bool(land.get("value"))
    if not value_ok:
        verify.append("Find a county appraisal for this parcel")

    if removed_reason:
        route, reason = "removed", removed_reason
    elif conflicts:
        route, reason = "research_hold", conflicts[0]
        verify.append("Pull the latest recorded deed to confirm who holds title now before anything else")
    elif matched and decider_ok and value_ok:
        route = "first_trace_review"
        reason = ("A current " + matched[0]["kind"] + " event is tied to this exact parcel, the owner group is identified "
                  "and the county value is sourced.")
    else:
        route = "research_hold"
        if not matched:
            reason = ("No current second event is tied to this exact parcel" + (". " + not_counted[0].upper() + not_counted[1:] if not_counted else "")
                      + ". Land with " + ("only a tax delinquency" if ham else "only an ownership signal") + " stays in research.")
            verify.append("Qualifies when a current delinquent tax suit or sale listing, foreclosure notice, partition case "
                          "or estate filing names this exact parcel")
        elif not decider_ok:
            reason = "A current " + matched[0]["kind"] + " event is tied to this parcel, but the owner group is not verified."
        else:
            reason = "A current event is tied to this parcel, but no county value is sourced."
    if ham and route != "removed":
        verify.append("Check the next Hamilton County Clerk and Master delinquent tax sale list when it is published (each May) "
                      "for parcel " + land["parcel"])
        verify.append("Hamilton estate filings are not in the staged notices; check the Hamilton County Herald estate notices "
                      "for the owner by full name")

    lead = matched[0] if matched and route != "removed" else None
    if lead:
        what = lead["what"] + (", case " + str(lead["case_id"]) if lead.get("case_id") else "") + ". Also on file: " + first + "."
        event = {"what": what, "status": "active", "checked_on": today, "source": _event_source(lead, today)}
        tie = {"printed_parcel": "The notice prints this parcel number", "address": "The notice street address resolved to this parcel on the county roll",
               "decedent_name": "The decedent's full name is an owner of record on this parcel in the same county; the notice does not print a parcel"}[lead["link"]]
    else:
        what = first + "." + (" Checked and not counted: " + not_counted + "." if not_counted else "")
        event = {"what": what, "status": first_status, "checked_on": today, "source": first_src}
        tie = ("The Trustee file and the Assessor file carry the same map-group-parcel" if ham
               else "The tract is the county roll parcel itself")
    deadline = lead.get("sale_date") if lead else None
    transfer = str(land.get("last_transfer") or "")
    evidence = ("County roll still shows " + (group["text"].title() or "no owner") + " as owner"
                + (", last recorded transfer " + transfer if transfer else "")
                + ". No listing, contract or deed search was run." if route != "removed" else removed_reason)
    question = ("Is the estate planning to sell the land, and who has to sign?" if estate else
                "Do you still own the land at " + where + ", and would you sell it rather than carry the taxes?")
    seen = set()
    verify = [v for v in verify if not (v in seen or seen.add(v))]
    return build_review(
        lane="land", route=route, route_reason=reason, reviewed_by=REVIEWER, today=today, county=land["county"],
        owner_names=group["text"] or None, event=event,
        asset={"description": description, "parcel": land["parcel"], "address": land.get("address"), "tie": tie, "source": roll_src},
        decider=decider,
        economics={"deadline": deadline, "deadline_kind": (lead["kind"] + " sale date") if deadline else None,
                   "value": land.get("value"), "value_basis": land["value_basis"] if value_ok else None,
                   "value_is_estimate": True, "value_source": roll_src if value_ok else None,
                   "known_debt": debts, "unknowns": unknowns},
        availability={"evidence": evidence, "checked_on": today, "conflicts": conflicts,
                      "first_call_question": question, "verify_next": verify or ["Recheck the county roll for this parcel"]},
    )


def same_card(old: Any, new: Dict[str, Any]) -> bool:
    """A repeat run on the same evidence writes nothing."""
    if not isinstance(old, dict):
        return False
    return {k: v for k, v in old.items() if k != "reviewed_at"} == {k: v for k, v in new.items() if k != "reviewed_at"}
