"""Memphis 311 structural requests. The city records complaints and their disposition."""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from urllib.parse import urlencode

from ._base import BotBase, LeadPayload, _supabase

MEMPHIS_311_ENDPOINT = (
    "https://311.memphistn.gov/server/rest/services/311/"
    "311_Request_Map_PROD/FeatureServer/0/query"
)
HIGH_SIGNAL_REQUEST_TYPES = {
    "CE-Substandard,Derelict Struc", "CE-Dilapidated", "CE-Boarded Vacant",
    "CE-Demolition", "CE-Unsafe Structure", "CE-Unfit", "CE-Open Vacant",
    "CE-Substandard", "CE-Major Building Repair", "CE-Roof", "CE-Foundation",
}
ACTIVE = {"Open", "In Progress", "Pending Litigation"}
FIELDS = ("OBJECTID,INCIDENT_NUMBER,INCIDENT_ID,DEPARTMENT,REQUEST_TYPE,REQUEST_STATUS,"
          "Request_Sub_Status,Location_Address,Unit_Address,CITY,STATE,ZipCode,PARCEL_ID,"
          "REPORTED_DATE,last_edited_date,Closed_Date,RESOLVED_DATE,RESOLUTION_CODE,RESOLUTION_SUMMARY")
REFRESH_LIMIT = 100


def clean(value):
    return " ".join(str(value or "").split())


def case_url(number):
    return MEMPHIS_311_ENDPOINT + "?" + urlencode({
        "where": "INCIDENT_NUMBER='" + number + "'", "outFields": FIELDS, "returnGeometry": "false", "f": "json"})


def request_address(a):
    street = clean(a.get("Location_Address"))
    if not re.match(r"^[0-9]+[A-Za-z]?\s+\S", street):
        return None
    city, state, zipcode = (clean(a.get(k)) for k in ("CITY", "STATE", "ZipCode"))
    if state and state.upper() not in {"TN", "TENNESSEE"}:
        return None
    embedded = re.fullmatch(r"(.+?),\s*([^,]+),\s*(?:TN|Tennessee),?\s+(\d{5})(?:,\s*USA)?", street, re.I)
    if embedded:
        street, place, postal = embedded.groups()
        if city and city.casefold() != place.casefold() or zipcode and zipcode != postal:
            return None
        city, zipcode = place, postal
    elif "," in street or not state:
        return None
    if city.casefold() not in {"memphis", "cordova"} or not re.fullmatch(r"\d{5}", zipcode):
        return None
    unit = clean(a.get("Unit_Address"))
    if unit and unit != street.split()[0] and unit.casefold() != street.casefold():
        if not re.fullmatch(r"(?:apt\.?|unit|ste\.?|suite|#)\s*[a-z0-9-]+", unit, re.I):
            return None
        if not street.casefold().endswith(unit.casefold()):
            if re.search(r"(?:\b(?:unit|apt|suite|ste)\.?\s+|#)\w", street, re.I):
                return None
            street += ", " + unit
    return f"{street}, {city}, TN {zipcode}"


def evidence(number, records, today, prior=None):
    review = {"incident_number": number, "checked_on": today, "source_url": case_url(number)}
    if len(records) != 1:
        return {**review, "finding": "ambiguous" if records else "not_returned"}
    a = records[0]
    if clean(a.get("INCIDENT_NUMBER")) != number or (prior and (any(
            clean(prior.get(k)) and clean(a.get(k)) and clean(prior[k]).casefold() != clean(a[k]).casefold()
            for k in ("INCIDENT_ID", "Location_Address", "PARCEL_ID", "REPORTED_DATE"))
            or clean(prior.get("Unit_Address")).casefold() != clean(a.get("Unit_Address")).casefold())):
        return {**review, "finding": "identity_conflict"}
    status = clean(a.get("REQUEST_STATUS"))
    typ = clean(a.get("REQUEST_TYPE"))
    if status in {"Closed", "Resolved"}:
        finding = "terminal_request"
    elif clean(a.get("DEPARTMENT")) not in {"", "Code Enforcement"} or typ not in HIGH_SIGNAL_REQUEST_TYPES:
        finding = "other_request_type"
    elif status not in ACTIVE:
        finding = "status_review"
    elif any(clean(a.get(k)) for k in ("Closed_Date", "RESOLVED_DATE", "RESOLUTION_CODE", "RESOLUTION_SUMMARY")):
        finding = "disposition_review"
    elif not request_address(a):
        finding = "address_review"
    else:
        finding = "structural_request"
    return {**review, "finding": finding, "request_type": typ, "request_status": status,
            "sub_status": a.get("Request_Sub_Status"), "department": a.get("DEPARTMENT"),
            "reported_at_ms": a.get("REPORTED_DATE"), "last_edited_at_ms": a.get("last_edited_date"),
            "closed_at_ms": a.get("Closed_Date"), "resolved_at_ms": a.get("RESOLVED_DATE"),
            "resolution_code": a.get("RESOLUTION_CODE"), "parcel_as_reported": a.get("PARCEL_ID")}


def sql_list(values):
    return ",".join("'" + value.replace("'", "''") + "'" for value in sorted(values))


class MemphisCodesBot(BotBase):
    name = "memphis_codes"
    description = "Memphis 311 structural service requests"
    throttle_seconds = 0.5
    expected_min_yield = 0

    def _cases(self, where):
        records = defaultdict(list)
        last = -1
        while True:
            params = {"where": f"({where}) AND OBJECTID > {last}", "orderByFields": "OBJECTID ASC",
                      "resultRecordCount": 500, "outFields": FIELDS, "returnGeometry": "false", "f": "json"}
            res = self.fetch(MEMPHIS_311_ENDPOINT, params=params)
            if res is None or res.status_code != 200:
                raise RuntimeError("Memphis 311 query unavailable")
            data = res.json()
            if not isinstance(data.get("features"), list) or "error" in data:
                raise ValueError("Invalid Memphis 311 query result")
            for feature in data["features"]:
                attrs = feature["attributes"]
                oid = attrs["OBJECTID"]
                if not isinstance(oid, int) or oid <= last:
                    raise ValueError("Non-advancing Memphis 311 object IDs")
                last = oid
                number = clean(attrs.get("INCIDENT_NUMBER"))
                if re.fullmatch(r"[0-9]+", number):
                    records[number].append(attrs)
            if not data.get("exceededTransferLimit"):
                return records
            if not data["features"]:
                raise ValueError("Incomplete Memphis 311 page")

    def scrape(self):
        self._complete = False
        cases = self._cases(f"REQUEST_TYPE IN ({sql_list(HIGH_SIGNAL_REQUEST_TYPES)}) "
                            f"AND REQUEST_STATUS IN ({sql_list(ACTIVE)})")
        today = datetime.now(timezone.utc).date().isoformat()
        leads = []
        # A type/status-filtered query cannot reveal a second record with the
        # same incident number that was closed or transferred. Confirm by ID.
        eligible = sorted(number for number, records in cases.items()
                          if evidence(number, records, today)["finding"] == "structural_request")
        exact = {}
        for start in range(0, len(eligible), 40):
            exact.update(self._cases("INCIDENT_NUMBER IN (" + sql_list(eligible[start:start + 40]) + ")"))
        for number in eligible:
            records = exact.get(number, [])
            review = evidence(number, records, today, cases[number][0])
            if review["finding"] == "structural_request":
                attrs = records[0]
                leads.append(LeadPayload(
                    bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, number),
                    property_address=request_address(attrs), county="Shelby County", distress_type="CODE_VIOLATION",
                    admin_notes=f"Memphis 311 request {number}: {review['request_type']} ({review['request_status']})",
                    source_url=review["source_url"], raw_payload={"memphis_311": attrs, "memphis_codes_review": review}))
        self._complete = True
        return leads

    def _write_staging(self, leads):
        if not getattr(self, "_complete", False):
            return 0, 0
        client = _supabase()
        if client is None:
            return 0, 0
        table = client.table("homeowner_requests_staging")
        known = set()
        candidates = []
        today = datetime.now(timezone.utc).date().isoformat()
        last = "00000000-0000-0000-0000-000000000000"
        while True:
            page = (table.select("id,pipeline_lead_key,number:raw_payload->memphis_311->>INCIDENT_NUMBER,"
                                 "checked:raw_payload->memphis_codes_review->>checked_on")
                    .eq("bot_source", self.name).gt("id", last).order("id").limit(250).execute().data)
            for row in page:
                key = row["pipeline_lead_key"]
                known.add(key)
                number = clean(row["number"])
                if (row["checked"] != today and re.fullmatch(r"[0-9]+", number)
                        and key == self.make_lead_key(self.name, number)):
                    candidates.append(row)
            if len(page) < 250:
                break
            last = page[-1]["id"]
        fresh = [lead for lead in leads if lead.pipeline_lead_key not in known]
        staged, dupes = super()._write_staging(fresh) if fresh else (0, 0)
        selected = sorted(candidates, key=lambda r: (r["checked"] or "", r["id"]))[:REFRESH_LIMIT]
        for start in range(0, len(selected), 40):
            chunk = selected[start:start + 40]
            numbers = {r["number"] for r in chunk}
            cases = self._cases("INCIDENT_NUMBER IN (" + sql_list(numbers) + ")")
            current = (table.select("id,pipeline_lead_key,prior:raw_payload->memphis_311,checked:raw_payload->memphis_codes_review->>checked_on").eq("bot_source", self.name)
                       .in_("id", [r["id"] for r in chunk]).execute().data)
            patches = []
            for row in current:
                if (row["checked"] or "") >= today:
                    continue
                prior = row["prior"] or {}
                number = clean(prior.get("INCIDENT_NUMBER"))
                if number not in numbers or row["pipeline_lead_key"] != self.make_lead_key(self.name, number):
                    continue
                review = evidence(number, cases.get(number, []), today, prior)
                patches.append({"id": row["id"], "raw_patch": {"memphis_codes_review": review}})
            # Merge only this collector's field under the DB row lock. Full-payload
            # equality filters put historical JSON in URLs and exceed gateway limits.
            if patches:
                count = client.rpc("apply_staging_enrichment", {"p_rows": patches}).execute().data
                if count != len(patches):
                    raise RuntimeError(f"Memphis review updated {count} of {len(patches)} rows")
        return staged, dupes + len(leads) - len(fresh)


def run() -> dict:
    return MemphisCodesBot().run()


if __name__ == "__main__":
    print(run())
