"""Knox County Trustee current-year real-property roll, keyed by docket CLT."""
import logging
import re
from collections import Counter
from datetime import datetime, timezone

from ._base import BotBase
from ._probate_staging import guard_raw

API = "https://knox-tn.mygovonline.com/api/v2"
PORTAL = "https://propertytax.knoxcountytn.gov/property-tax/bill"
KEY = "knox_assessor"
HEARING = "knoxville_hearing_review"
SOURCE = "knoxville_poh"
STREETS = {"AVENUE": "AVE", "STREET": "ST", "ROAD": "RD", "DRIVE": "DR", "LANE": "LN",
           "BOULEVARD": "BLVD", "NORTH": "N", "SOUTH": "S", "EAST": "E", "WEST": "W"}


def street(value):
    return " ".join(STREETS.get(word, word) for word in (value or "").upper().split())


def lookup(fetch, hearing, today):
    parcel = hearing["parcel_id"]
    match = re.fullmatch(r"(\d{3}[A-Z]{2})(\d{3}(?:\d{2})?)", parcel)
    if not match:
        return None
    account = "-".join(match.groups())
    response = fetch(f"{API}/due/PPT/{account}", params={"detail_level": "public"})
    if response is None or response.status_code != 200:
        raise ValueError("County bill unavailable")
    data = response.json()
    if data["status"] != 1:
        raise ValueError("Invalid county response")
    records = data["due"]["PPT"]
    accounts = records["accounts"]
    if len(accounts) != 1 or accounts[0]["account_number"] != account:
        return None
    year = int(today[:4])
    bills = [b for b in records["bills"] if str(b.get("taxYear")) == str(year)]
    if len(bills) != 1:
        return None
    bill = bills[0]
    info = bill["parcelInformation"]
    value = bill["taxInformation"]["totalPropertyValue"]
    owner = " ".join((info.get("ownerName") or "").split())
    address = hearing["property_address"]
    if (bill.get("account_number") != account or bill.get("parcelId") != accounts[0].get("parcel_id")
            or not bill.get("parcelId") or bill.get("bill_type") != "Real"
            or bill.get("isPreliminary") is not False or bill.get("hideFromOnlineServices") is not False
            or info.get("propertyIdentifier") != "R" or info.get("specialInterest") != "0"
            or not address.endswith(", Knoxville, TN")
            or street(address.removesuffix(", Knoxville, TN")) != street(info.get("propertyAddress"))
            or not owner or owner != " ".join((bill.get("owner") or "").split())
            or re.search(r"\b(UNLISTED|WITHHELD|UNKNOWN|REDACTED)\b", owner, re.I)
            or type(value) not in (int, float) or not 0 < value < float("inf")):
        return None
    return {"case_number": hearing["case_number"], "parcel_id": parcel, "property_address": address,
            "owner": owner, "appraised_value": value, "tax_year": year, "checked_on": today,
            "source_url": f"{PORTAL}/{account}/{year}"}


def refresh(client, reviews, fetch, today=None):
    today = today or datetime.now(timezone.utc).date().isoformat()
    counts = Counter()
    table = client.table("homeowner_requests_staging")
    for key, hearing in reviews.items():
        if (hearing.get("document_kind") != "poh_agenda" or hearing.get("meeting_cancelled") is not False
                or hearing.get("status") != "agenda_outcome_unverified" or hearing.get("checked_on") != today
                or key != BotBase.make_lead_key(SOURCE, f"poh-record-{hearing['case_number']}-{hearing['parcel_id']}")):
            continue
        try:
            rows = (table.select("id,property_address,raw_payload,phone_metadata,staging_status")
                    .eq("bot_source", SOURCE).eq("pipeline_lead_key", key).eq("county", "Knox County")
                    .eq("property_address", hearing["property_address"]).execute().data)
            if len(rows) != 1:
                counts["unmatched"] += 1
                continue
            row = rows[0]
            raw = row.get("raw_payload") or {}
            if ((row.get("phone_metadata") or {}).get(HEARING) != hearing or row.get("staging_status") == "rejected"
                    or any(raw.get(k) != hearing[k] for k in ("case_number", "parcel_id"))):
                counts["protected"] += 1
                continue
            observed = lookup(fetch, hearing, today)
            if not observed:
                counts["unconfirmed"] += 1
                continue
            if raw.get(KEY) == observed:
                counts["unchanged"] += 1
                continue
            q = (table.update({"raw_payload": {**raw, KEY: observed}}).eq("id", row["id"])
                 .eq("bot_source", SOURCE).eq("pipeline_lead_key", key).eq("county", "Knox County")
                 .eq("property_address", hearing["property_address"]).eq("staging_status", row["staging_status"])
                 .eq("phone_metadata->knoxville_hearing_review->>source_sha256", hearing["source_sha256"])
                 .eq("phone_metadata->knoxville_hearing_review->>checked_on", today))
            changed = guard_raw(q, raw).execute().data
            counts["updated" if changed else "conflict"] += 1
        except Exception as exc:
            counts["failed"] += 1
            logging.getLogger(__name__).warning("Knox roll lookup/update failed for %s (%s)", key, type(exc).__name__)
    return dict(counts)
