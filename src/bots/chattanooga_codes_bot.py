"""
Chattanooga code enforcement scraper.

Source: City of Chattanooga open-data CSV published through ArcGIS Online.
Public download, no auth, no WAF. The current CSV is generated from the
CityView code-enforcement system and includes all violations, so this bot
filters to active high-signal structural/dangerous cases.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

import csv
import io
import hashlib
import json
import time
from collections import Counter, defaultdict
from urllib.parse import quote
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload, _supabase, guard_phone_metadata


CHATTANOOGA_CODES_CSV_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/"
    "19f35e4e09c041718905088a1fc7a6bb/data"
)
CHATTANOOGA_CODES_SOURCE_URL = (
    "https://www.arcgis.com/home/item.html?id=19f35e4e09c041718905088a1fc7a6bb"
)

ACTIVE_STATUSES = {"INPR", "O"}

# Keep the real distress and skip the noise: grass, loose litter, and
# inoperable vehicles are common, but structural/dangerous cases are money.
HIGH_SIGNAL_TERMS = {
    "ACCESSORY STRUCTURES",
    "BOARDING",
    "CERTIFICATE OF OCCUPANCY",
    "CONDEMNED",
    "DANGEROUS",
    "DEMOLITION",
    "ELECTRICAL",
    "EXTERIOR OF STRUCTURE",
    "EXTERIOR WALLS",
    "FOUNDATION",
    "INTERIOR",
    "PLUMBING",
    "REPAIR OR DEMOLITION",
    "ROOFS",
    "SANITARY",
    "SMOKE ALARMS",
    "STAIRWAYS",
    "UNFIT",
    "UNSAFE",
    "WINDOWS, DOORS",
}


class ChattanoogaCodesBot(BotBase):
    name = "chattanooga_codes"
    description = "Chattanooga Code Enforcement - active high-signal violations (Hamilton Co.)"
    throttle_seconds = 1.0
    expected_min_yield = 50

    def scrape(self) -> List[LeadPayload]:
        self._started = time.monotonic()
        self._snapshot = None
        metadata_url = CHATTANOOGA_CODES_CSV_URL.rsplit("/", 1)[0]
        first = self.fetch(metadata_url, params={"f": "json"})
        if first is None or first.status_code != 200:
            raise RuntimeError("Chattanooga item metadata unavailable")
        modified = first.json().get("modified")
        if not isinstance(modified, int) or modified <= 0 or modified > time.time() * 1000:
            raise RuntimeError("Invalid source modification date")
        res = self.fetch(CHATTANOOGA_CODES_CSV_URL, timeout=90)
        if res is None or res.status_code != 200:
            raise RuntimeError("Chattanooga CSV unavailable")
        snapshot = parse_snapshot(res.content, modified)
        last = self.fetch(metadata_url, params={"f": "json"})
        if last is None or last.status_code != 200 or last.json().get("modified") != modified:
            raise RuntimeError("Chattanooga publication changed during download")
        self._snapshot = snapshot
        leads = []
        for (case, address), rows in snapshot["groups"].items():
            evidence = case_review(snapshot, case, address)
            if not evidence["current_code_signal"]:
                continue
            active = [r for r in rows if usable_active(r)]
            first_row = active[0]
            group = {"case_number": case, "address": address,
                     "city": "CHATTANOOGA", "state": "TN", "rows": active,
                     "descriptions": list(dict.fromkeys(r["description"] for r in active)),
                     "record_ids": [r["record_id"] for r in active],
                     **{k: first_row.get(k, "") for k in ("date_entered", "latitude", "longitude", "council_district")}}
            leads.append(self._build_lead(group))
        return leads

    def _candidates(self, client):
        last = None
        while True:
            q = client.table("homeowner_requests_staging").select(
                "id,pipeline_lead_key,review:phone_metadata->chattanooga_case_review"
            ).eq("bot_source", self.name).order("id").limit(500)
            if last:
                q = q.gt("id", last)
            rows = q.execute().data or []
            yield from rows
            if len(rows) < 500:
                return
            last = rows[-1]["id"]

    def _write_staging(self, leads):
        snapshot = getattr(self, "_snapshot", None)
        if snapshot is None:
            return 0, 0  # A failed/truncated source never changes existing evidence.
        client = _supabase()
        if client is None:
            return 0, 0
        candidates = list(self._candidates(client))
        known = {r["pipeline_lead_key"] for r in candidates}
        fresh = [lead for lead in leads if lead.pipeline_lead_key not in known]
        staged, _ = super()._write_staging(fresh) if fresh else (0, 0)
        if staged:
            candidates = list(self._candidates(client))
        selected = refresh_candidates(candidates, snapshot)
        self.refresh_counts = Counter(selected=len(selected))
        for offset in range(0, len(selected), 50):
            if time.monotonic() - getattr(self, "_started", time.monotonic()) > 450:
                self.refresh_counts["deferred"] += len(selected) - offset
                break
            chunk = selected[offset:offset + 50]
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                cols = "id,pipeline_lead_key,property_address,county,raw_payload,phone_metadata"
                if table.endswith("_staging"):
                    cols += ",staging_status,rejection_reason"
                q = client.table(table).select(cols)
                q = (q.eq("bot_source", self.name).in_("id", [r["id"] for r in chunk])
                     if table.endswith("_staging") else
                     q.eq("source", "bot").in_("pipeline_lead_key", [r["pipeline_lead_key"] for r in chunk]))
                try:
                    rows = q.execute().data or []
                except Exception as exc:
                    self.refresh_counts["failed"] += len(chunk)
                    self.logger.warning("Chattanooga hydration failed (%s)", type(exc).__name__)
                    continue
                for row in rows:
                    patch = refresh_patch(row, snapshot)
                    if patch is None:
                        self.refresh_counts["unchanged_or_protected"] += 1
                        continue
                    old_pm = row.get("phone_metadata")
                    if max(len(quote(json.dumps(value), safe="")) for value in
                           (old_pm, patch["phone_metadata"])) > MAX_CAS_BYTES:
                        self.refresh_counts["oversized"] += 1
                        continue
                    update = client.table(table).update(patch).eq("id", row["id"]).eq("pipeline_lead_key", row["pipeline_lead_key"])
                    update = update.eq("bot_source", self.name) if table.endswith("_staging") else update.eq("source", "bot")
                    fields = ["property_address", "county"]
                    if table.endswith("_staging"):
                        fields += ["staging_status", "rejection_reason"]
                    for field in fields:
                        update = update.is_(field, "null") if row.get(field) is None else update.eq(field, row[field])
                    try:
                        written = guard_phone_metadata(update, old_pm).execute().data
                        self.refresh_counts["updated" if written else "conflicts"] += 1
                    except Exception as exc:
                        self.refresh_counts["failed"] += 1
                        self.logger.warning("Chattanooga refresh failed id=%s (%s)", row["id"], type(exc).__name__)
        self.logger.info("Chattanooga case refresh %s", dict(self.refresh_counts))
        return staged, len(leads) - len(fresh)

    def _build_lead(self, group: Dict[str, Any]) -> LeadPayload:
        city = self._title_city(group["city"])
        state = group["state"].upper()
        full_address = f"{group['address']}, {city}, {state}"

        date_iso = self._date_to_iso(group["date_entered"])
        descriptions = group["descriptions"][:6]
        notes_parts = [
            f"case {group['case_number']} [HIGH-SIGNAL]",
            f"{len(group['rows'])} active violation row(s)",
        ]
        if descriptions:
            notes_parts.append("violations: " + "; ".join(descriptions))
        if date_iso:
            notes_parts.append(f"entered: {date_iso}")
        if group["council_district"]:
            notes_parts.append(f"district: {group['council_district']}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(
                self.name, f"{group['case_number']}|{group['address'].upper()}"
            ),
            property_address=full_address,
            county="Hamilton County",
            distress_type="CODE_VIOLATION",
            admin_notes=" | ".join(notes_parts),
            source_url=CHATTANOOGA_CODES_SOURCE_URL,
            raw_payload={
                "chattanooga_codes": {
                    "case_number": group["case_number"],
                    "record_ids": group["record_ids"],
                    "date_entered": group["date_entered"],
                    "latitude": group["latitude"],
                    "longitude": group["longitude"],
                    "council_district": group["council_district"],
                    "descriptions": group["descriptions"],
                    "rows": group["rows"],
                }
            },
        )

    def _is_high_signal(self, row: Dict[str, str]) -> bool:
        return serious(row)

    def _build_address(self, row: Dict[str, str]) -> Optional[str]:
        street_number = self._clean(row.get("street_number"))
        street_name = self._clean(row.get("street_name"))
        address = " ".join(part for part in [street_number, street_name] if part).strip()
        return address or None

    @staticmethod
    def _clean(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _date_to_iso(value: str) -> Optional[str]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).date().isoformat()
        except ValueError:
            return value[:10] if len(value) >= 10 else None

    @staticmethod
    def _title_city(value: str) -> str:
        return " ".join(part.capitalize() for part in value.split()) or "Chattanooga"



REVIEW_KEY = "chattanooga_case_review"
INACTIVE_REASON = "CHATTANOOGA_CASE_INACTIVE"
REVIEW_REASON = "CHATTANOOGA_CASE_REVIEW"
REFRESH_ROWS = 400
# Leave room for identity guards below the service's 16KB request-header cap.
MAX_CAS_BYTES = 8000


def norm(value):
    return " ".join(str(value or "").upper().split())


def usable_active(row):
    return row["status"] in ACTIVE_STATUSES and not row.get("date_corrected", "").strip()


def serious(row):
    # Complaint comments are not a citation. Keep exact official description evidence.
    text = " ".join(row.get(k, "") for k in ("description", "description_extended")).upper()
    return row.get("flag_dangerous", "").lower() == "true" or any(term in text for term in HIGH_SIGNAL_TERMS)


def parse_snapshot(content, modified):
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")), strict=True)
    required = {"case_number", "record_id", "status", "street_number", "street_name", "state",
                "description", "description_extended", "flag_dangerous", "date_corrected"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Chattanooga CSV schema missing required fields")
    groups = defaultdict(list)
    cases = set()
    for row in reader:
        if None in row or any(row.get(k) is None for k in required):
            raise ValueError("Malformed/truncated Chattanooga CSV")
        row = {k: str(v or "").strip() for k, v in row.items()}
        if any(not row[k] for k in ("case_number", "record_id", "status", "street_number", "street_name")):
            raise ValueError("Incomplete Chattanooga case identity")
        address = norm(row["street_number"] + " " + row["street_name"])
        groups[(row["case_number"], address)].append(row)
        cases.add(row["case_number"])
    if not groups:
        raise ValueError("Empty Chattanooga CSV is not a valid snapshot")
    return {"groups": groups, "cases": cases, "source_modified_on": datetime.fromtimestamp(modified / 1000, timezone.utc).isoformat(),
            "source_sha256": hashlib.sha256(content).hexdigest(), "checked_on": datetime.now(timezone.utc).date().isoformat()}


def case_review(snapshot, case, address):
    rows = snapshot["groups"].get((case, norm(address)), [])
    by_id = defaultdict(list)
    for row in rows:
        by_id[row["record_id"]].append(row)
    identity_fields = ("status", "date_corrected", "description", "description_extended", "flag_dangerous", "state")
    ambiguous = any(len({tuple(r[k] for k in identity_fields) for r in copies}) > 1 for copies in by_id.values())
    unique = [copies[0] for copies in by_id.values()]
    statuses = sorted({r["status"] for r in unique})
    active = [r for r in unique if usable_active(r)]
    strong = [r for r in active if serious(r)]
    if not rows:
        status = "identity_mismatch" if case in snapshot["cases"] else "not_found"
    elif ambiguous:
        status = "ambiguous"
    elif any(norm(r["state"]) != "TN" for r in unique):
        status = "identity_mismatch"
    elif any(s not in {"INPR", "O", "C", "LIT", "DUP", "OUT"} for s in statuses):
        status = "unknown"
    elif active:
        status = "active"
    elif "LIT" in statuses:
        status = "litigation_review"
    elif set(statuses) <= {"C"}:
        status = "closed"
    elif set(statuses) <= {"DUP"}:
        status = "duplicate"
    elif set(statuses) <= {"OUT"}:
        status = "outside_jurisdiction"
    elif any(s in ACTIVE_STATUSES for s in statuses):
        status = "corrected"
    else:
        status = "unknown"
    signal = status == "active" and bool(strong)
    return {"case_number": case, "property_address": norm(address), "status": status,
            "checked_on": snapshot["checked_on"], "source_modified_on": snapshot["source_modified_on"],
            "source_sha256": snapshot["source_sha256"], "source_url": CHATTANOOGA_CODES_CSV_URL,
            "source_item_url": CHATTANOOGA_CODES_SOURCE_URL, "record_ids": sorted(by_id),
            "official_statuses": statuses, "current_code_signal": signal,
            "serious_active_count": len(strong) if signal else 0,
            "active_descriptions": list(dict.fromkeys(r["description"] for r in active)),
            "serious_active_records": [{k: r.get(k, "") for k in ("record_id", "status", "description", "description_extended", "date_entered", "date_cited", "flag_dangerous")} for r in strong] if signal else [],
            "corrected_count": sum(bool(r.get("date_corrected")) for r in unique),
            "owner_verified": False, "sale_authority_verified": False,
            "property_scope": "reported_address_only", "evidence_scope": "individual_case_not_whole_parcel"}


def refresh_candidates(rows, snapshot):
    rows = sorted(rows, key=lambda r: r["id"])
    if not rows:
        return []
    offset = datetime.fromisoformat(snapshot["checked_on"]).toordinal() * REFRESH_ROWS % len(rows)
    rotated = rows[offset:] + rows[:offset]
    return [r for r in rotated if not isinstance(r.get("review"), dict) or
            r["review"].get("checked_on") != snapshot["checked_on"] or
            r["review"].get("source_sha256") != snapshot["source_sha256"]][:REFRESH_ROWS]


def refresh_patch(row, snapshot):
    raw = row.get("raw_payload") or {}
    if not isinstance(raw, dict):
        return None
    old = raw.get("chattanooga_codes")
    pm = row.get("phone_metadata")
    if not isinstance(old, dict) or (pm is not None and not isinstance(pm, dict)):
        return None
    case = str(old.get("case_number") or "").strip()
    original_rows = old.get("rows") or []
    if not isinstance(original_rows, list) or any(not isinstance(r, dict) or
            str(r.get("case_number") or "").strip() != case for r in original_rows):
        return None
    addresses = {norm(str(r.get("street_number") or "") + " " + str(r.get("street_name") or "")) for r in original_rows if isinstance(r, dict)}
    if len(addresses) != 1 or not case:
        return None
    address = next(iter(addresses))
    if row.get("pipeline_lead_key") != BotBase.make_lead_key("chattanooga_codes", case + "|" + address):
        return None
    previous = (pm or {}).get(REVIEW_KEY)
    if isinstance(previous, dict):
        if previous.get("source_modified_on", "") > snapshot["source_modified_on"] or previous.get("checked_on", "") > snapshot["checked_on"]:
            return None
        if previous.get("source_modified_on") == snapshot["source_modified_on"] and previous.get("source_sha256") != snapshot["source_sha256"]:
            return None  # conflicting bytes for one publication cannot restore eligibility
    evidence = case_review(snapshot, case, address)
    if (norm(row.get("county")).removesuffix(" COUNTY") != "HAMILTON" or
            norm(str(row.get("property_address") or "").split(",")[0]) != address):
        evidence.update(status="identity_mismatch", current_code_signal=False,
                        serious_active_count=0, serious_active_records=[])
    # The CSV names no parcel or owner. Parcel grouping belongs to the independent assessor review.
    evidence["record_count"] = len(evidence["record_ids"])
    evidence["active_description_count"] = len(evidence["active_descriptions"])
    evidence["evidence_truncated"] = False
    metadata = {**(pm or {}), REVIEW_KEY: evidence}
    # Keep the next run's full-metadata CAS below the URL budget too. Counts
    # remain complete; sample IDs/citations are explicitly marked when clipped.
    for field in ("serious_active_records", "active_descriptions", "record_ids"):
        minimum = 1 if field == "serious_active_records" and evidence["current_code_signal"] else 0
        while len(evidence[field]) > minimum and len(quote(json.dumps(metadata), safe="")) > 5500:
            evidence[field].pop()
            evidence["evidence_truncated"] = True
    patch = {"phone_metadata": metadata}
    stage = row.get("staging_status")
    if stage == "pending" and not evidence["current_code_signal"]:
        inactive = evidence["status"] in ("closed", "duplicate", "outside_jurisdiction", "corrected")
        patch.update(staging_status="rejected", rejection_reason=INACTIVE_REASON if inactive else REVIEW_REASON)
    elif stage == "rejected" and row.get("rejection_reason") in (INACTIVE_REASON, REVIEW_REASON) and evidence["current_code_signal"]:
        patch.update(staging_status="pending", rejection_reason=None)
    return None if all(row.get(k) == v for k, v in patch.items()) else patch

def run() -> dict:
    bot = ChattanoogaCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
