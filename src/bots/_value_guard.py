"""One rule for every property value on a staged row.

A value belongs to a parcel, not to a street. It may be written to a row, or
trusted on a review card, only when it can show all of:

  parcel       the exact county parcel the value was read from
  county       the same county as the row
  unit         the same unit, where the row's address has one
  basis        what the number is (county appraisal, bill assessment divided
               by the class ratio, ...)
  observed_on  the date the county record was read

Three uses:

  require_value_evidence()  gate in front of an enricher's write
  copy_decision()           gate for copying a value between two staged rows
  value_status()            read side: is the value already on a row backed
                            by a parcel record held on that same row?

No network, no database: pure functions.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Set

# source -> plain basis. A source missing here has no stated basis.
BASIS = {
    "comptroller": "Tennessee Comptroller county assessment roll appraisal",
    "hamilton_assessor": "Hamilton County Assessor appraisal",
    "metro_nashville_parcels": "Metro Nashville parcel record total appraised value",
    "davidson_assessor": "Davidson County Assessor total appraised value",
    "rutherford_assessor": "Rutherford County Assessor total appraised value",
    "shelby_assessor": "Shelby County Assessor total appraised value",
    "williamson_assessor": "Williamson County Assessor total appraised value",
    "shelby_trustee_assessment_estimate":
        "Shelby County realty bill assessment divided by the class ratio. An estimate, not an appraisal",
}

# Estimates built from the row's own document, not from a parcel record.
# They carry their own label on the card and are not parcel values.
DOCUMENT_ESTIMATES = {"shelby_loan_hpi_estimate", "montgomery_last_sale", "permit_construction_cost_proxy"}

XREF_SOURCE = "xref_avm_enricher"
XREF_KEY = "property_value_xref"
# States of phone_metadata.property_value_xref written by this rule.
XREF_EXACT = "exact_parcel"
XREF_SETTLED = (XREF_EXACT, "replaced_by_own_parcel_record", "superseded_by_parcel_record", "removed_cross_parcel")

_UNIT = re.compile(r"(?:\b(?:UNIT|APT|APARTMENT|SUITE|STE|BLDG|BUILDING)\b\.?\s*#?\s*|#\s*)([A-Z0-9][A-Z0-9-]*)")


def _d(value: Any) -> Dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _num(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def norm_county(county: Any) -> str:
    return re.sub(r"\s+county$", "", str(county or "").strip().lower())


def iso_day(value: Any) -> Optional[str]:
    """ISO day from a date, datetime or a string that starts with one."""
    if value in (None, ""):
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    text = str(value).strip()[:10]
    try:
        date.fromisoformat(text)
    except ValueError:
        return None
    return text


def address_unit(address: Any) -> Optional[str]:
    """The unit printed in an address ("Unit 4", "Apt B", "#12"), or None."""
    m = _UNIT.search(str(address or "").upper())
    return m.group(1) if m else None


def parcel_forms(parcel: Any, county: Any = None) -> Set[str]:
    """Every spelling one parcel number is compared under.

    Letters and digits only. A Comptroller roll ID ends with the roll year
    ("057 055K E 00100 000 2026"); the year is not part of the parcel. Shelby
    prints one parcel two ways ("D0242N E00083" and "D0242N0E000830"); both
    fold to the same form. Tenant personal-property accounts ("PER-...") are
    not land parcels and give nothing.
    """
    text = str(parcel or "").upper().strip()
    if not text or text.startswith("PER-"):
        return set()
    text = re.sub(r"\s+20\d{2}\s*$", "", text)
    key = re.sub(r"[^A-Z0-9]", "", text)
    if len(key) < 6:
        return set()
    forms = {key}
    if norm_county(county) == "shelby":
        if len(key) == 14:
            forms.add(key[:6] + key[6:8].lstrip("0") + key[8:13] + ("" if key[13] == "0" else key[13]))
    return forms


def _doc_parcels(raw: Dict[str, Any]) -> List[Any]:
    """Parcel numbers printed by the row's own source document."""
    out = [raw.get("parcel"), raw.get("alt_parcel"), raw.get("parcel_id"),
           _d(raw.get("hamilton_roll_valuation")).get("parcel"),
           _d(raw.get("extracted")).get("parcel"),
           _d(raw.get("nashville_codes")).get("Property_APN"),
           _d(raw.get("davidson_demolition_permit")).get("Parcel")]
    return [p for p in out if p]


def value_evidence(row: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Parcel records held on the row that state a value.

    Each: source, parcel, county, value, basis, observed_on, address.
    observed_on is None when the record carries no date of its own.
    """
    raw = _d(row.get("raw_payload"))
    meta = _d(row.get("phone_metadata"))
    county = norm_county(row.get("county"))
    out: List[Dict[str, Any]] = []

    def add(source, parcel, value, on, rec_county=None, address=None, basis=None):
        value = _num(value)
        if parcel and value and value > 0:
            out.append({"source": source, "parcel": str(parcel), "county": norm_county(rec_county) or county,
                        "value": value, "basis": basis or BASIS.get(source), "observed_on": iso_day(on),
                        "address": address})

    comp = _d(raw.get("comptroller_parcel"))
    add("comptroller", comp.get("parcelid"), comp.get("appraisal"), str(comp.get("version") or "")[-10:],
        comp.get("county"), comp.get("address"))
    ham = _d(raw.get("hamilton_assessor"))
    add("hamilton_assessor", ham.get("parcel") or ham.get("parcel_id"), ham.get("appraised"),
        ham.get("file_date") or _d(raw.get("hamilton_roll_valuation")).get("assessor_file_date"),
        "hamilton", ham.get("property_address"))
    metro = _d(raw.get("metro_parcel"))
    add("metro_nashville_parcels", metro.get("parcel") or metro.get("apn"), metro.get("total_appraisal"),
        meta.get("metro_parcel_checked"), "davidson")
    pad = _d(raw.get("padctn"))
    add("davidson_assessor", pad.get("parcel"), pad.get("appraised"), pad.get("checked_at"), "davidson")
    ruth = _d(raw.get("rutherford_arcgis"))
    add("rutherford_assessor", ruth.get("parcel_id"), ruth.get("appraised"), ruth.get("checked_at"),
        "rutherford", ruth.get("property_address"))
    shel = _d(raw.get("shelby_arcgis"))
    add("shelby_assessor", shel.get("parid"), shel.get("appraised"), shel.get("checked_at"),
        "shelby", shel.get("property_address"))
    will = _d(raw.get("williamson_inigo"))
    add("williamson_assessor", will.get("parcel_id"), will.get("appraised"), will.get("checked_at"),
        "williamson", will.get("property_address"))
    # Shelby tax sale rows: the Trustee parcel page for the row's own parcel.
    detail = _d(raw.get("trustee_detail"))
    add("shelby_trustee_assessment_estimate", raw.get("parcel"), detail.get("estimated_appraisal"),
        detail.get("checked_on"), "shelby")
    # Memphis notices: the reviewed realty bill for the noticed parcel.
    review = _d(meta.get("shelby_trustee_value_review"))
    if review.get("status") == "verified_assessment":
        add("shelby_trustee_assessment_estimate", review.get("parcel_id"), review.get("estimated_appraisal"),
            review.get("checked_on"), "shelby", review.get("location"))
    # Pre-foreclosure rows: the county record read in a recorded review.
    seed = _d(raw.get("preforeclosure_evidence"))
    if seed.get("parcel") and seed.get("value_basis"):
        kind = ("shelby_trustee_assessment_estimate" if county == "shelby" and seed.get("value_is_estimate")
                else "metro_nashville_parcels" if county == "davidson" else "county_parcel_review")
        add(kind, seed.get("parcel"), seed.get("value"), seed.get("observed_on"), county, basis=seed.get("value_basis"))
    return out


def row_parcels(row: Mapping[str, Any]) -> Set[str]:
    """Every form of every parcel number the row holds for its own property."""
    raw = _d(row.get("raw_payload"))
    county = row.get("county")
    forms: Set[str] = set()
    for parcel in _doc_parcels(raw):
        forms |= parcel_forms(parcel, county)
    for ev in value_evidence(row):
        if not ev["county"] or not norm_county(county) or ev["county"] == norm_county(county):
            forms |= parcel_forms(ev["parcel"], ev["county"])
    return forms


def _parcel_conflict(row: Mapping[str, Any], parcel: Any, county: Any) -> bool:
    """True when the row's own document prints a parcel in the same format as
    `parcel` and it is a different one. Formats that cannot be compared (a
    notice "055K-E-001.00" against a roll ID) neither confirm nor conflict."""
    theirs = parcel_forms(parcel, county)
    if not theirs:
        return False
    lengths = {len(f) for f in theirs}
    for doc in _doc_parcels(_d(row.get("raw_payload"))):
        mine = parcel_forms(doc, row.get("county") or county)
        if mine & theirs:
            return False
        if any(len(f) in lengths for f in mine):
            return True
    return False


def require_value_evidence(update: Dict[str, Any], row: Mapping[str, Any], *, parcel: Any, county: Any,
                           observed_on: Any, basis: Optional[str] = None,
                           record_address: Any = None) -> Optional[str]:
    """Gate in front of an enricher's write.

    `update` is the patch about to be sent. If it sets property_value and the
    evidence is incomplete, the value and its source are taken out of the
    patch (everything else in it still goes) and the reason is returned.
    If the evidence is complete, property_value_as_of is stamped and None is
    returned. A patch that sets no value is left alone.
    """
    if update.get("property_value") in (None, ""):
        return None
    source = update.get("property_value_source")
    reason = None
    day = iso_day(observed_on)
    if not parcel_forms(parcel, county):
        reason = "no_parcel"
    elif not norm_county(county):
        reason = "no_county"
    elif norm_county(row.get("county")) and norm_county(row.get("county")) != norm_county(county):
        reason = "county_mismatch"
    elif _parcel_conflict(row, parcel, county):
        reason = "parcel_mismatch"
    elif not (basis or BASIS.get(str(source or ""))):
        reason = "no_basis"
    elif not day:
        reason = "no_observation_date"
    else:
        unit = address_unit(row.get("property_address"))
        if unit and address_unit(record_address) != unit:
            # A unit is settled by the unit on the county record, or by the
            # row's own document naming this exact parcel.
            named = {f for doc in _doc_parcels(_d(row.get("raw_payload")))
                     for f in parcel_forms(doc, row.get("county") or county)}
            if not (named & parcel_forms(parcel, county)):
                reason = "unit_not_matched"
    if reason:
        update.pop("property_value", None)
        update.pop("property_value_source", None)
        update.pop("property_value_as_of", None)
        return reason
    update["property_value_as_of"] = day
    return None


def value_status(row: Mapping[str, Any]) -> Dict[str, Any]:
    """Is the value on this row backed by a parcel record on this row?

    status: missing     no value
            verified    a parcel record on the row states this value, in the
                        row's county, with a basis and an observation date
            estimate    built from the row's own document (loan, last sale);
                        labeled as an estimate, not a parcel value
            unverified  anything else; `reason` says what is missing
    """
    value = _num(row.get("property_value"))
    source = str(row.get("property_value_source") or "").strip()
    if not value or value <= 0:
        return {"status": "missing", "reason": "no value", "source": source or None}
    if source in DOCUMENT_ESTIMATES:
        return {"status": "estimate", "reason": "estimate from the record's own document, not a parcel value",
                "source": source}
    county = norm_county(row.get("county"))
    unit = address_unit(row.get("property_address"))
    xref = _d(_d(row.get("phone_metadata")).get(XREF_KEY))
    best: Optional[Dict[str, Any]] = None
    reason = "no parcel record on this row states this value"
    for ev in value_evidence(row):
        if abs(ev["value"] - value) > 1:
            continue
        on = ev["observed_on"] or (iso_day(row.get("property_value_as_of")) if source == ev["source"] else None)
        if county and ev["county"] and ev["county"] != county:
            reason = "the parcel record is in another county"
        elif _parcel_conflict(row, ev["parcel"], ev["county"]):
            reason = "the parcel record is for a different parcel than the one this record names"
        elif not ev["basis"]:
            reason = "the value has no stated basis"
        elif not on:
            reason = "the parcel record has no observation date"
        elif unit and ev.get("address") and address_unit(ev["address"]) not in (None, unit):
            reason = "the parcel record is for a different unit"
        else:
            best = {**ev, "observed_on": on}
            break
    if best is None and xref.get("match") == XREF_EXACT and xref.get("parcel") and iso_day(xref.get("observed_on")):
        # Copied under the exact-parcel rule: the proof travels with the copy.
        if parcel_forms(xref["parcel"], xref.get("county") or county) & row_parcels(row) and (
                not county or norm_county(xref.get("county")) == county):
            best = {"source": xref.get("basis_source") or source, "parcel": xref["parcel"], "county": county,
                    "value": value, "basis": xref.get("basis"), "observed_on": iso_day(xref["observed_on"])}
    if best is not None:
        return {"status": "verified", "reason": None, "source": best["source"], "parcel": best["parcel"],
                "county": best["county"], "basis": best["basis"], "observed_on": best["observed_on"]}
    if source == XREF_SOURCE or (xref and xref.get("match") != XREF_EXACT and xref.get("status") not in XREF_SETTLED):
        if reason.startswith("no parcel record"):
            reason = "copied from another record by street address; no record of this parcel states it"
    return {"status": "unverified", "reason": reason, "source": source or None}


def unverified_label(row: Mapping[str, Any]) -> Optional[str]:
    """Card wording for a value that cannot show its parcel, basis and date.
    None when the value is verified, an estimate from its own document, or
    absent."""
    status = value_status(row)
    if status["status"] != "unverified":
        return None
    return "Unverified value: " + status["reason"] + ". Check the county parcel record before relying on it"


def own_parcel_value(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    """A complete parcel record on the row itself (parcel, same county,
    basis, date), ready to supply the row's value. The newest one wins."""
    county = norm_county(row.get("county"))
    unit = address_unit(row.get("property_address"))
    good = []
    for ev in value_evidence(row):
        if not (ev["basis"] and ev["observed_on"] and ev["county"]):
            continue
        if county and ev["county"] != county:
            continue
        if _parcel_conflict(row, ev["parcel"], ev["county"]):
            continue
        if unit and address_unit(ev.get("address")) != unit:
            named = {f for doc in _doc_parcels(_d(row.get("raw_payload"))) for f in parcel_forms(doc, county)}
            if not (named & parcel_forms(ev["parcel"], ev["county"])):
                continue
        good.append(ev)
    good.sort(key=lambda e: e["observed_on"], reverse=True)
    return good[0] if good else None


def copy_decision(donor: Mapping[str, Any], recipient: Mapping[str, Any]) -> Dict[str, Any]:
    """May the donor's value be written to the recipient? Returns
    {"ok": True, parcel, county, basis, basis_source, observed_on, value} or
    {"ok": False, "reason": ...}."""
    proof = value_status(donor)
    if proof["status"] != "verified":
        return {"ok": False, "reason": "donor_value_" + proof["status"]}
    county = norm_county(recipient.get("county"))
    if not county or county != proof["county"]:
        return {"ok": False, "reason": "county_mismatch" if county else "recipient_has_no_county"}
    mine = row_parcels(recipient)
    if not mine:
        return {"ok": False, "reason": "recipient_has_no_parcel"}
    if not (parcel_forms(proof["parcel"], county) & mine):
        return {"ok": False, "reason": "different_parcel"}
    if address_unit(donor.get("property_address")) != address_unit(recipient.get("property_address")):
        return {"ok": False, "reason": "different_unit"}
    return {"ok": True, "parcel": proof["parcel"], "county": county, "basis": proof["basis"],
            "basis_source": proof["source"], "observed_on": proof["observed_on"],
            "value": _num(donor.get("property_value"))}


def today_iso() -> str:
    """Today's date, for a county record read during this run."""
    return date.today().isoformat()
