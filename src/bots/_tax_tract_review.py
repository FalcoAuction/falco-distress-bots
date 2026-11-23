"""Dated status and review card for a tract in a delinquent tax sale.

Each run gives every tracked tract one dated status from the county's own
current source:

  listed     on the county's current sale list today
  postponed  still listed, with a later sale date than the one last recorded
  removed    no longer on the current list; consistent with payment or
             withdrawal, but the list does not say which
  sale_held  the advertised sale date has passed; the result is not read here
  noticed    printed on the county's posted sale notice, in a county that
             publishes no payment or release list (so current status is unknown)
  unknown    the county source could not be read; the prior status is kept
             as last_known_status and the failed check is recorded

A missing list is never "paid" and never "still eligible".

The card keeps delinquent tax apart from every other debt: the tax balance is
the only known debt, and the mortgage payoff is listed as unknown.

No network, no database: pure functions used by the scheduled tax bots.
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Optional

from ._pilot_review import _ENTITY, build_review, debt, merge_review, source

STATUS_KEY = "tax_status"
CARD_KEY = "pilot_review"
# The Nov 22 owner screen reviewed tax tracts at or above this county estimate.
FIRST_TRACE_VALUE_FLOOR = 150_000
REDEMPTION = ("Tennessee allows redemption after a tax sale, so the usable selling window "
              "is before the sale date")
_EVENT = {"listed": "active", "postponed": "postponed", "removed": "resolved",
          "sale_held": "resolved", "noticed": "unknown", "unknown": "unknown"}
_HEIRS = re.compile(r"\b(HEIRS?|ESTATE|DECEASED|DECD|EST OF)\b", re.I)


def next_status(prior: Optional[Dict[str, Any]], seen: Dict[str, Any], day: date) -> Dict[str, Any]:
    """One dated status from one check.

    seen: available (bool), reason (when unavailable), listed (bool),
          sale_date (ISO, when listed), release_list (False when the county
          source is a posted notice and not a current list), source_url,
          source_date.
    """
    prior = dict(prior or {})
    today = day.isoformat()
    out = {k: v for k, v in prior.items() if k not in ("note",)}
    out["checked_on"] = today
    if not seen.get("available"):
        known = prior.get("status")
        if known == "unknown":
            known = prior.get("last_known_status")
        failed = [f for f in prior.get("failed_checks") or [] if f.get("on") != today]
        failed.append({"on": today, "reason": seen.get("reason") or "county source unavailable"})
        out.update(status="unknown", last_known_status=known, failed_checks=failed[-5:])
        out["note"] = ("County source could not be read on " + today + ". Last confirmed status: "
                       + (known or "none") + (" on " + prior["last_confirmed_on"] if prior.get("last_confirmed_on") else "")
                       + ". This is not evidence of payment or of continued listing.")
        return out
    out.pop("last_known_status", None)
    out["source_url"] = seen.get("source_url")
    out["source_date"] = seen.get("source_date")
    was = prior.get("status")
    if was == "unknown":
        was = prior.get("last_known_status")
    old_date = prior.get("sale_date")
    new_date = seen.get("sale_date") if seen.get("listed") else None
    sale_date = new_date or old_date
    if sale_date and sale_date < today and (seen.get("listed") or was in ("listed", "postponed", "noticed", "sale_held")):
        out.update(status="sale_held", sale_date=sale_date)
        out.setdefault("result", None)
        out["note"] = ("The advertised sale date " + sale_date + " has passed. The sale result has not been read; "
                       "a redemption period can follow a Tennessee tax sale.")
        return out
    if not seen.get("listed"):
        out["status"] = "removed"
        if was != "removed":
            out["removed_on"] = today if was else None
            out["last_listed_on"] = prior.get("last_confirmed_on")
        if out.get("removed_on"):
            out["note"] = ("Removed from the county's current sale list on " + out["removed_on"]
                           + ". Removal is consistent with payment or withdrawal; the list does not say which.")
        else:
            out["note"] = ("Not on the county's current sale list when first checked on " + today
                           + "; the removal date and reason are not known.")
        return out
    out["sale_date"] = new_date
    out["last_confirmed_on"] = today
    for stale in ("removed_on", "last_listed_on"):
        out.pop(stale, None)
    if was == "removed":
        out["relisted_on"] = today
    if seen.get("release_list") is False:
        out["status"] = "noticed"
        out["note"] = ("Printed on the county's posted sale notice read on " + today
                       + ". The county publishes no payment or release list, so whether the tract "
                         "has since been paid or withdrawn is unknown.")
    elif old_date and new_date and new_date > old_date:
        out.update(status="postponed", prior_sale_date=old_date, postponed_noted_on=today)
        out["note"] = "Sale date moved from " + old_date + " to " + new_date + ", first seen on " + today + "."
    elif was == "postponed" and new_date == old_date and prior.get("prior_sale_date"):
        out["status"] = "postponed"
        out["note"] = ("Still listed on " + today + " for the postponed date " + new_date
                       + " (was " + prior["prior_sale_date"] + ").")
    else:
        out["status"] = "listed"
        out["note"] = "On the county's current sale list on " + today + "."
    if old_date and new_date and new_date != old_date and out["status"] != "postponed":
        out.update(prior_sale_date=old_date, sale_date_changed_on=today)
    if out["status"] == "noticed" and out.get("prior_sale_date"):
        out["note"] += " The notice now gives " + new_date + " as the sale date (was " + out["prior_sale_date"] + ")."
    return out


def _same(a: Optional[Dict[str, Any]], b: Optional[Dict[str, Any]]) -> bool:
    strip = lambda c: {k: v for k, v in (c or {}).items() if k != "reviewed_at"}
    return strip(a) == strip(b)


def patch_for(raw: Dict[str, Any], status: Dict[str, Any], card: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Top-level raw_payload keys to merge; empty on a repeat run. A manual
    card and a stronger card from another reviewer are kept."""
    patch: Dict[str, Any] = {}
    if raw.get(STATUS_KEY) != status:
        patch[STATUS_KEY] = status
    if card is not None:
        kept = merge_review(raw.get(CARD_KEY), card)
        if not _same(raw.get(CARD_KEY), kept):
            patch[CARD_KEY] = kept
    return patch


def is_entity(name: Any) -> bool:
    return bool(_ENTITY.search(re.sub(r"[^A-Z0-9 ]", " ", str(name or "").upper())))


def names_heirs(*texts: Any) -> bool:
    return any(_HEIRS.search(str(t or "")) for t in texts)


def _finish(*, lane_source, county, status, holds, removed_reason, event, asset, decider, economics,
            availability, owner_names, day):
    """Pick the route from the status and the holds, then build the card."""
    if status["status"] in ("removed", "sale_held"):
        route, reason = "removed", removed_reason
    elif holds:
        route, reason = "research_hold", holds[0]["why"]
        availability = {**availability, "verify_next": [h["check"] for h in holds] + availability["verify_next"]}
        if len(holds) > 1:
            reason += " Also: " + " ".join(h["why"] for h in holds[1:])
    else:
        route, reason = "first_trace_review", (
            "Owner of record, exact parcel and a county value are sourced, and the tract was on the "
            "county's current sale list on " + status["checked_on"] + ".")
    return build_review(lane="tax", route=route, route_reason=reason, reviewed_by=lane_source, event=event,
                        asset=asset, decider=decider, economics=economics, availability=availability,
                        county=county, owner_names=owner_names, today=day)


def shelby_wants_card(row: Dict[str, Any]) -> bool:
    raw = row.get("raw_payload") or {}
    return bool((row.get("property_value") or 0) >= FIRST_TRACE_VALUE_FLOOR or raw.get(CARD_KEY)
                or (raw.get("shelby_receiver_review") or {}).get("status") == "receiver_eligibility_research")


def shelby_card(row: Dict[str, Any], status: Dict[str, Any], day: date, list_url: str) -> Dict[str, Any]:
    raw = row.get("raw_payload") or {}
    detail = raw.get("trustee_detail") or {}
    sale_id, parcel = raw.get("sale_id"), raw.get("parcel")
    owner = detail.get("primary_owner") or row.get("owner_name_records")
    parties = (raw.get("publication_evidence") or {}).get("parties") or []
    pub_url = (raw.get("publication_evidence") or {}).get("url")
    sale_date = status.get("sale_date") or row.get("trustee_sale_date")
    page = source("Shelby County Trustee parcel page", row.get("source_url"), detail.get("checked_on"))
    listing = source("Shelby County Trustee current tax sale list", list_url, status.get("last_confirmed_on"))
    value = row.get("property_value")
    balance = detail.get("total_tax_balance")
    prior_years = detail.get("visible_prior_year_balances") or {}
    receiver = raw.get("shelby_receiver_review") or {}
    recheck = raw.get("trustee_detail_recheck") or {}

    holds: List[Dict[str, str]] = []
    if status["status"] == "unknown":
        holds.append({"why": "The Shelby Trustee sale list could not be read on " + status["checked_on"]
                             + ", so the current status of this tract is unknown.",
                      "check": "Run the Shelby Trustee sale list check again before any spend."})
    if receiver.get("status") == "receiver_eligibility_research":
        cases = receiver.get("cases") or []
        hearings = sorted({c["hearing_listed"] for c in cases if c.get("hearing_listed")})
        warrants = ", ".join(c["warrant"] for c in cases)
        upcoming = [h for h in hearings if h >= day.isoformat()]
        holds.append({"why": "This parcel is also in a Shelby Environmental Court receiver case (warrant "
                             + warrants + "); who can sell is unresolved until the court acts.",
                      "check": ("Check the outcome of the " + upcoming[0] + " Environmental Court hearing on warrant "
                                + warrants + " after it is held (next check " + upcoming[0] + ").") if upcoming else
                               ("Check the Environmental Court roster for the next hearing on warrant " + warrants + ".")})
    if value and (row.get("property_value_source") != "shelby_trustee_assessment_estimate"
                  or detail.get("estimated_appraisal") != value):
        holds.append({"why": "The value on file (source " + str(row.get("property_value_source")) + ") is not the "
                             "county assessment of this parcel; the Trustee shows no current-year assessment for it.",
                      "check": "Confirm on the Shelby Assessor record whether this parcel ID is current or an "
                               "earlier ID of another listed parcel at the same address."})
    if recheck.get("status"):
        holds.append({"why": "The Trustee parcel page did not confirm this parcel and sale together on "
                             + str(recheck.get("checked_on")) + " while the sale list still shows it.",
                      "check": "Open the Trustee parcel page and reconcile the sale ID before any spend."})
    if not owner:
        holds.append({"why": "No owner is shown on the Trustee bill for this parcel.",
                      "check": "Read the current deed at the Shelby Register of Deeds."})
    elif names_heirs(owner, *parties):
        holds.append({"why": "The roll owner or the tax suit publication names an estate or heirs, so the "
                             "owner of record may be deceased and the signer is not established.",
                      "check": "Check Shelby Probate Court for an estate and a named personal representative."})
    elif is_entity(owner):
        holds.append({"why": "The owner of record is an entity; no person with authority to sign is identified.",
                      "check": "Read the Tennessee Secretary of State business record for the entity's status, "
                               "registered agent and officers."})
    if not value:
        holds.append({"why": "No county value is on file for this parcel.",
                      "check": "Recheck the Trustee parcel page for the current assessment."})
    elif value < FIRST_TRACE_VALUE_FLOOR:
        holds.append({"why": "The county estimate is below the 150,000 screening floor used for tax tracts.",
                      "check": "Confirm value from a second source before any spend."})
    if not row.get("property_address"):
        holds.append({"why": "The county list gives no street number for this parcel.",
                      "check": "Confirm the situs and land use on the Shelby Assessor record."})
    if detail and not prior_years:
        holds.append({"why": "The Trustee page shows no unpaid Shelby County bill from a prior year, so the "
                             "basis for the sale listing is not reconciled.",
                      "check": "Reconcile the sale basis and any City of Memphis taxes with the Trustee page."})

    if status["status"] in ("listed", "postponed"):
        what = ("Shelby County delinquent tax sale " + str(sale_id) + ", sale date " + str(sale_date)
                + ". The parcel was on the Trustee's current sale list on " + status["checked_on"] + ".")
        if status["status"] == "postponed":
            what += " The date moved from " + str(status.get("prior_sale_date")) + "."
    else:
        what = "Shelby County delinquent tax sale " + str(sale_id) + ", sale date " + str(sale_date) + ". " + status["note"]
    basis = "Primary Owner on the Shelby County Trustee bill for this parcel"
    if detail.get("checked_on"):
        basis += ", read " + detail["checked_on"]
    basis += ". This is the taxpayer of record, not a deed search."
    if parties:
        named = owner in parties
        basis += (" The tax suit publication names " + "; ".join(parties) + (
            ", which includes the roll owner." if named else ", which does not match the roll owner exactly."))
        if len(parties) > 1:
            basis += " The role of the other named parties is not established."
    else:
        basis += " The taxpayer named in the tax suit has not been read."
    known = []
    if prior_years:
        # Only bills from earlier years are delinquent; the current-year bill is not yet.
        note = "Unpaid Shelby County bills from prior years: " + ", ".join(
            f"{y} ${prior_years[y]:,.2f}" for y in sorted(prior_years)) + "."
        if balance is not None:
            note += f" The Trustee page total, with the current-year bill, is ${balance:,.2f}."
        note += " Excludes city taxes and court costs not yet posted. Not a payoff and not total debt."
        known.append(debt("delinquent_tax", sum(prior_years.values()), detail.get("checked_on"), page, note))
    elif balance is not None:
        known.append(debt("delinquent_tax", balance, detail.get("checked_on"), page,
                          "Total balance on the Trustee parcel page; no prior-year bill is itemized, so the "
                          "delinquent part is not established. Not a payoff and not total debt."))
    evidence = ""
    if status["status"] in ("listed", "postponed"):
        evidence = ("The tax case is still open: the parcel was on the county sale list on " + status["checked_on"]
                    + " and the Trustee still bills " + str(owner) + ". No listing, contract or deed transfer "
                      "search has been run in this lane.")
    return _finish(
        lane_source="shelby_tax_sale", county="shelby", status=status, holds=holds, day=day,
        removed_reason=status["note"],
        event={"what": what, "status": _EVENT[status["status"]], "checked_on": status["checked_on"], "source": listing},
        asset={"description": "Real property, Shelby County parcel " + " ".join(str(parcel).split())
                              + " (alternate ID " + str(raw.get("alt_parcel")) + ")",
               "parcel": parcel, "address": row.get("property_address"),
               "tie": "The Trustee's sale list pairs this parcel ID with sale " + str(sale_id)
                      + ", and the Trustee parcel page shows the same parcel and sale ID.",
               "source": page},
        decider={"name": owner, "role": "owner of record on the county tax roll", "basis": basis,
                 "verified": False, "source": page},
        economics={"deadline": sale_date,
                   "deadline_kind": "Tax sale date. " + REDEMPTION + ".",
                   "value": value,
                   "value_basis": ("County estimate: " + str(detail.get("tax_year") or "current")
                                   + " assessment divided by the class ratio. Not a market value.")
                                  if detail.get("estimated_appraisal") == value else
                                  ("Copied from another record (source " + str(row.get("property_value_source"))
                                   + "). Not an assessment of this parcel."),
                   "value_is_estimate": True, "value_source": page if value else None, "known_debt": known,
                   "unknowns": ["Mortgage payoff and any other recorded liens (no recorded instrument read)",
                                "City taxes and court costs added in the tax suit",
                                "Current deed and everyone required to sign",
                                "Whether the owner means to pay, redeem or sell"]},
        availability={"evidence": evidence, "checked_on": status["checked_on"],
                      "conflicts": [],
                      "first_call_question": "Do you plan to pay the back taxes before the " + str(sale_date)
                                             + " sale, or would selling before the sale help you?",
                      "verify_next": ["Recheck the Trustee sale list and balance on the day contact data is bought.",
                                      "Read the current deed and liens at the Shelby Register of Deeds."]
                                     + (["Read the tax suit publication at " + pub_url + "."] if pub_url else [])},
        owner_names=owner,
    )


CHEATHAM_OFFICE = ("Cheatham County Clerk and Master, 100 Public Square, Suite 106, Ashland City, TN 37015, "
                   "(615) 792-4620")


def fold_parcel(value: Any) -> str:
    """Comparison key for a printed parcel: punctuation and spacing dropped,
    and the letters newspapers print for digits (l, I, O, S) read as digits."""
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper()).translate(str.maketrans("LIOS", "1105"))


def match_tract(raw: Dict[str, Any], notice: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The notice tract for a staged row: same printed parcel, or the same
    tract letter with the same judgment amount. Never the letter alone."""
    if str(raw.get("docket")) != str(notice.get("docket")):
        return None
    want = fold_parcel(raw.get("parcel_printed"))
    hits = [t for t in notice["tracts"] if want and fold_parcel(t["parcel_printed"]) == want]
    if len(hits) == 1:
        return hits[0]
    hits = [t for t in notice["tracts"] if t["letter"] == raw.get("tract")
            and raw.get("published_judgment") is not None
            and abs(t["amount"] - float(raw["published_judgment"])) < 0.005]
    return hits[0] if len(hits) == 1 else None


def cheatham_card(row: Dict[str, Any], status: Dict[str, Any], day: date, notice_url: str,
                  tract: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    raw = row.get("raw_payload") or {}
    roll = raw.get("assessment_review") or raw.get("comptroller_parcel") or {}
    printed_owner = (tract or {}).get("owners") or raw.get("owners_printed") or ""
    unknown_owner = printed_owner.lower().startswith("unknown") or not printed_owner
    roll_owner = roll.get("owner") if str(roll.get("owner") or "").upper() != "UNKNOWN" else None
    name = None if unknown_owner else (roll_owner or printed_owner)
    aka = re.fullmatch(r"(.+?)\s*\(\s*(?:a/?k/?a|aka)\s+(.+?)\)", printed_owner.strip(), re.I)
    aliases = [a.strip(" .") for a in aka.groups()] if aka else []
    if any(len(a.split()) < 2 or " and " in a.lower() for a in aliases):
        aliases = []
    # Both full names of an "aka" owner are kept so two tracts held under either
    # name fall in one owner group; otherwise the county roll name is the key.
    if unknown_owner:
        owner_names = None
    elif aliases:
        owner_names = sorted(set(aliases))
    else:
        owner_names = [roll_owner or printed_owner]
    parcel_id = roll.get("parcelid") or None   # as sourced; the shared property key drops the roll year
    printed = (tract or {}).get("parcel_printed") or raw.get("parcel_printed")
    address = row.get("property_address") or (tract or {}).get("location") or raw.get("location_printed")
    sale_date = status.get("sale_date") or row.get("trustee_sale_date")
    amount = (tract or {}).get("amount") if tract else raw.get("published_judgment")
    posted = source("Notice of Delinquent Tax Sale posted by the Cheatham County Trustee", notice_url,
                    status.get("source_date"))
    roll_src = source("Tennessee Comptroller parcel roll, Cheatham County", roll.get("source_url"), None) if roll else None
    case = "Civil Action No. " + str(raw.get("docket"))
    label = "tract " + str((tract or {}).get("letter") or raw.get("tract"))

    holds: List[Dict[str, str]] = []
    if status["status"] == "unknown":
        holds.append({"why": "The county's posted sale notice could not be read on " + status["checked_on"]
                             + ", so the status of this tract is unknown.",
                      "check": "Ask the " + CHEATHAM_OFFICE + " whether " + label + " in " + case + " is still set for sale."})
    elif status["status"] == "noticed":
        holds.append({"why": "The tract is printed on the county's posted sale notice, but Cheatham County "
                             "publishes no payment or release list, so whether it is still set for sale is unknown.",
                      "check": "Ask the " + CHEATHAM_OFFICE + " whether " + label + " in " + case
                               + " is still set for the " + str(sale_date) + " sale."})
    if unknown_owner:
        holds.append({"why": "The court notice lists the owner as unknown.",
                      "check": "Read the last deed at the Cheatham County Register of Deeds."})
    elif names_heirs(printed_owner, roll_owner):
        holds.append({"why": "The notice names heirs or an estate, so the owner of record is deceased and the "
                             "signers are not established.",
                      "check": "Check Cheatham Chancery probate filings for an estate and a personal representative."})
    elif is_entity(name):
        holds.append({"why": "The owner is an entity; no person with authority to sign is identified.",
                      "check": "Read the Tennessee Secretary of State business record for the entity."})
    if tract and not tract.get("parcel_forms_agree", True):
        holds.append({"why": "The notice prints two parcel numbers for this tract that do not agree ("
                             + str(tract.get("parcel_printed")) + " and " + str(tract.get("parcel_long")) + ").",
                      "check": "Confirm the parcel for " + label + " with the Clerk and Master before relying on the matched roll parcel."})
    if not re.match(r"\d+\s", str(address or "")):
        holds.append({"why": "The notice gives no street number for this tract.",
                      "check": "Confirm the tract location on the county parcel map."})
    if not row.get("property_value"):
        holds.append({"why": "No county appraisal is matched to this tract.",
                      "check": "Match the printed parcel to the county roll."})

    what = ("Cheatham County Chancery delinquent tax sale, " + case + ", " + label + ", advertised for "
            + str(sale_date) + ". " + status["note"])
    basis = "Named as current owner in the court's sale notice (" + printed_owner + ")" if printed_owner else "No owner named"
    if roll_owner:
        basis += "; the county roll for the matched parcel shows " + roll_owner
    basis += ". No deed search has been run."
    known = []
    if amount is not None:
        known.append(debt("delinquent_tax", amount, status.get("source_date"), posted,
                          "Judgment amount printed in the sale notice. Penalties, interest and costs accrue "
                          "on top of it. Not a payoff and not total debt."))
    return _finish(
        lane_source="cheatham_tax_sale", county="cheatham", status=status, holds=holds, day=day,
        removed_reason=status["note"],
        event={"what": what, "status": _EVENT[status["status"]], "checked_on": status["checked_on"], "source": posted},
        asset={"description": "Real property, " + label + " in " + case + ", parcel " + str(printed)
                              + " as printed" + (", county roll parcel " + parcel_id if parcel_id else ""),
               "parcel": parcel_id or printed, "address": address,
               "tie": "The court's sale notice prints this parcel number and location under " + case + ".",
               "source": posted},
        decider={"name": name, "role": "owner named in the tax suit" + (" and on the county roll" if roll_owner else ""),
                 "basis": basis, "verified": False, "source": posted},
        economics={"deadline": sale_date, "deadline_kind": "Tax sale date. " + REDEMPTION + ".",
                   "value": row.get("property_value"),
                   "value_basis": "County appraisal on the Comptroller roll. An assessor's appraisal, not a market value.",
                   "value_is_estimate": True, "value_source": roll_src if row.get("property_value") else None,
                   "known_debt": known,
                   "unknowns": ["Mortgage payoff and any other recorded liens (no recorded instrument read)",
                                "Penalties, interest and costs added to the judgment",
                                "Current deed and everyone required to sign",
                                "Whether the owner means to pay, redeem or sell"]},
        availability={"evidence": "", "checked_on": status["checked_on"], "conflicts": [],
                      "first_call_question": "Do you plan to pay the tax judgment before the " + str(sale_date)
                                             + " sale, or would selling before the sale help you?",
                      "verify_next": ["Read the current deed and liens at the Cheatham County Register of Deeds."]},
        owner_names=owner_names,
    )
