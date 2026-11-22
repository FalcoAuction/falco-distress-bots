"""County appraisal binding for Hamilton County Herald foreclosure notices."""
import re
from datetime import timezone
from email.utils import parsedate_to_datetime

from ._base import BotBase, guard_phone_metadata
from ._comptroller import _street_tokens

SOURCE = "hamilton_county_herald"
REVIEW = "herald_assessor_review"
CONTROL = re.compile(r"(\d{3}[A-Z0-9]?)[\s_-]+(?:([A-Z])[\s_-]+)?(\d{3}(?:\.\d+)?)(?!\w|\.\d)", re.I)
LABEL = re.compile(r"\b(?:tax\s+id(?:entification)?(?:\s+number)?|(?:tax\s+map\s+)?parcel\s+(?:id(?:\s+number)?|number)|tax map parcel)\s*[#:]*\s*", re.I)
MULTIPLE = re.compile(r"^\s*(?:,|&|and\b|through\b|to\b|[-/])\s*\d{3}(?!\d)", re.I)


def control(text):
    m = CONTROL.fullmatch(str(text or "").strip().upper())
    return "-".join((m[1], m[2] or "", m[3])) if m else None


def subject(raw):
    body = raw["body"]
    excluded = {control(m[1]) for m in re.finditer(
        r"\bLESS\s+AND\s+EXCEPT\s+PARCEL\s+(" + CONTROL.pattern + ")", body, re.I)}
    controls = set()
    for label in LABEL.finditer(body):
        tail = body[label.end():]
        # "tax identification number, date of birth" is a privacy disclaimer.
        if tail.lower().startswith(", date of birth"):
            continue
        m = CONTROL.match(tail)
        if not m:
            return None, "unparsed_parcel"
        if MULTIPLE.match(tail[m.end():]) or re.match(r"\s+[A-Z]\d{3}\b", tail[m.end():], re.I):
            return None, "multiple_parcels"
        controls.add(control(m[0]))
    if raw.get("parcel"):
        stored = control(raw["parcel"])
        if not stored:
            return None, "unparsed_parcel"
        if stored not in excluded:
            controls.add(stored)
    if (excluded and not controls) or controls & excluded:
        return None, "excluded_parcel"
    if len(controls) > 1 or re.search(r"\b(?:TRACT|PARCEL)\s+(?:NO\.?\s*)?(?:2|II)\b", body, re.I):
        return None, "multiple_parcels"
    return next(iter(controls), None), None


def printed_situs(body, situs):
    # Bind the address to the subject property clause, never to the trustee's office.
    clauses = re.finditer(
        r"(?:also known as|commonly known as|address/description|property address|"
        r"address of the (?:above-described )?property is believed to be)\s*:?\s*(\d+[^,;]{1,100}),"
        r"(?!\s*(?:#|APT\b|UNIT\b|SUITE\b))", body, re.I)
    return any(_street_tokens(m[1]) == _street_tokens(situs) for m in clauses)


def parcel_review(row, parcels, addresses, ambiguous, duplicate_parcels, stamp, url):
    raw = row.get("raw_payload") or {}
    nid = str(raw.get("notice_id") or "")
    addr = row.get("property_address") or ""
    if (raw.get("publication") != SOURCE or row.get("bot_source") != SOURCE or not nid.isdigit()
            or row.get("pipeline_lead_key") != BotBase.make_lead_key(SOURCE, "FL-" + nid)
            or (row.get("county") or "").lower() not in ("hamilton", "hamilton county")
            or not isinstance(raw.get("body"), str) or not re.search(r"\bHamilton County\b", raw["body"], re.I)
            or addr != raw.get("property_address") or not stamp):
        return None
    evidence = dict(notice_id=nid, source_url=url, county_file_stamp=stamp, notice_address=addr)
    street = addr.split(",")[0].strip().upper()
    parcel, hold = subject(raw)
    if not hold:
        if not re.match(r"^\d+\s+\S", street) or re.search(r",\s*(?:#|UNIT\b|APT\b|SUITE\b)", addr, re.I):
            hold = "incomplete_address"
        elif not parcel and street in ambiguous:
            hold = "ambiguous_address"
        else:
            key = parcel or addresses.get(street)
            hit = parcels.get(key)
            if not hit or key in duplicate_parcels:
                hold = "unresolved_parcel"
            else:
                situs = str(hit.get("property_address") or "").upper()
                # Some publisher address fields omit only the final street type.
                # Require both the printed parcel and the complete street in the notice.
                printed = parcel and situs.startswith(street + " ") and situs[len(street)+1:] in (
                    "PL", "LN", "DR", "RD", "ST", "CT", "AVE", "CIR", "WAY", "BLVD", "TER", "PIKE"
                ) and printed_situs(raw["body"], situs)
                if (situs != street and not printed) or hit.get("situs_unit"):
                    hold = "address_conflict"
                elif not parcel and not printed_situs(raw["body"], situs):
                    hold = "unconfirmed_notice_address"
                elif not hit.get("appraised") or not hit.get("owner"):
                    hold = "missing_county_facts"
                else:
                    return dict(evidence, status="exact_parcel_address" if parcel else "unique_county_address",
                                parcel=key, county_address=situs, recorded_owner=hit["owner"],
                                county_appraisal=hit["appraised"])
    return dict(evidence, status=hold)


def review_patch(row, evidence):
    old = row.get("phone_metadata")
    if not evidence or (old is not None and not isinstance(old, dict)):
        return {}
    previous = (old or {}).get(REVIEW) or {}
    if not isinstance(previous, dict):
        return {}
    evidence = dict(evidence)
    patch = {}
    amount = evidence.get("county_appraisal")
    owns_value = (previous.get("supplied_value") is True and row.get("property_value_source") == "hamilton_assessor"
                  and row.get("property_value") == previous.get("county_appraisal"))
    empty_value = row.get("property_value") in (None, 0) and not row.get("property_value_source")
    evidence["supplied_value"] = bool(amount and (empty_value or owns_value))
    if owns_value or (empty_value and amount):
        as_of = None
        if amount:
            try:
                as_of = parsedate_to_datetime(evidence["county_file_stamp"]).astimezone(timezone.utc).date().isoformat()
            except (TypeError, ValueError):
                pass
        values = dict(property_value=amount, property_value_source="hamilton_assessor" if amount else None,
                      property_value_as_of=as_of)
        if any(row.get(k) != v for k, v in values.items()):
            patch.update(values)
    owner = evidence.get("recorded_owner")
    owns_owner = previous.get("supplied_owner") is True and row.get("owner_name_records") == previous.get("recorded_owner")
    evidence["supplied_owner"] = bool(owner and (not row.get("owner_name_records") or owns_owner))
    if (owns_owner or evidence["supplied_owner"]) and row.get("owner_name_records") != owner:
        patch["owner_name_records"] = owner
    if previous != evidence:
        patch["phone_metadata"] = {**(old or {}), REVIEW: evidence}
    return patch


def refresh(client, parcels, addresses, ambiguous, duplicates, stamp, url):
    table = client.table("homeowner_requests_staging")
    cursor = None
    updated = 0
    while True:
        q = table.select("id,bot_source,raw_payload,pipeline_lead_key,county,property_address,phone_metadata,"
                         "staging_status,property_value,property_value_as_of,property_value_source,owner_name_records")
        q = q.eq("bot_source", SOURCE).eq("distress_type", "PRE_FORECLOSURE").in_("staging_status", ["pending","promoted"])
        if cursor:
            q = q.gt("id", cursor)
        rows = q.order("id").limit(80).execute().data
        for row in rows:
            ev = parcel_review(row, parcels, addresses, ambiguous, duplicates, stamp, url)
            patch = review_patch(row, ev)
            if not patch:
                continue
            q = table.update(patch).eq("raw_payload->>notice_id", ev["notice_id"])
            raw = row["raw_payload"]
            for field in ("parcel", "property_address", "publication"):
                name = "raw_payload->>" + field
                q = q.eq(name, raw[field]) if raw.get(field) is not None else q.is_(name, "null")
            for field in ("id","pipeline_lead_key","bot_source","county","property_address","staging_status",
                          "property_value","property_value_source","property_value_as_of","owner_name_records"):
                value = row.get(field)
                q = q.is_(field, "null") if value is None else q.eq(field, value)
            updated += len(guard_phone_metadata(q, row["phone_metadata"]).execute().data or [])
        if len(rows) < 80:
            return updated
        cursor = rows[-1]["id"]
