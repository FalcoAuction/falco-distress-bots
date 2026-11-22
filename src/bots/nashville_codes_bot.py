"""
Nashville code violations scraper.

Source: data.nashville.gov / ArcGIS Hub publishes Metro Codes property
standards violations (the city housing code enforcement database).
Public ArcGIS REST endpoint, no auth. Open cases are research signals,
not proof of financial distress, current ownership or willingness to sell.
Recurring exact-case queries revisit stored records regardless of age and
retain DONE/CLOSED outcomes. Missing records are unresolved, not closed.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

import re
import json
import time
from urllib.parse import quote
from datetime import datetime, timezone
from typing import List, Optional

from ._base import BotBase, LeadPayload, _supabase


NASHVILLE_CODES_ENDPOINT = (
    "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/"
    "Property_Standards_Violations_2/FeatureServer/0/query"
)
REFRESH_CASES = 200
INACTIVE_REASON = "NASHVILLE_CASE_INACTIVE"
REVIEW_REASON = "NASHVILLE_CASE_REVIEW"


def case_groups(records):
    groups = {}
    for attrs in records:
        case = str(attrs.get("Request_Nbr") or "").strip()
        if not case:
            continue
        group = groups.setdefault(case, [])
        comparable = {k: v for k, v in attrs.items() if k != "ObjectId"}
        if not any({k: v for k, v in prior.items() if k != "ObjectId"} == comparable for prior in group):
            group.append(attrs)
    return groups


def refresh_candidates(rows, today):
    # Rotate the starting point even when writes fail. Same-day successful
    # rows are skipped, so a second run advances through the remaining backlog.
    rows = sorted(rows, key=lambda row: str(row["id"]))
    if not rows:
        return []
    start = datetime.fromisoformat(today).toordinal() * REFRESH_CASES % len(rows)
    return [r for r in rows[start:] + rows[:start]
            if r.get("case_num") and r.get("checked_on") != today][:REFRESH_CASES]


def review(attrs, today):
    from ._cv_filter import is_auctionable_cv
    status = str(attrs.get("Status") or "").upper().strip()
    case = str(attrs.get("Request_Nbr") or "").strip()
    citation = str(attrs.get("Violations_Noted") or "").strip()
    return {"checked_on": today, "case_number": case, "official_status": status,
            "property_apn": attrs.get("Property_APN"), "property_address": attrs.get("Property_Address"),
            "violations_noted": citation,
            "current_code_signal": bool(status == "OPEN" and citation and is_auctionable_cv(citation)[0]),
            "status": "open" if status == "OPEN" else "inactive" if status in ("DONE", "CLOSED") else "unrecognized",
            "source_url": NASHVILLE_CODES_ENDPOINT + "?f=json&where=" + quote("Request_Nbr='" + case.replace("'", "''") + "'", safe=""),
            "last_activity": attrs.get("Last_Activity"), "last_activity_date": attrs.get("Last_Activity_Date"),
            "last_activity_result": attrs.get("Last_Act__Result"),
            "owner_verified": False, "seller_intent": "unknown"}


def refresh_patch(row, official, today):
    """A case-specific status update; preserve manual decisions and enrichment."""
    raw = row.get("raw_payload")
    if not isinstance(raw, dict) or not isinstance(raw.get("nashville_codes"), dict):
        return None
    old = raw["nashville_codes"]
    case = str(old.get("Request_Nbr") or "").strip()
    if not case or row.get("pipeline_lead_key") != BotBase.make_lead_key("nashville_codes", case):
        return None
    records = official.get(case, [])
    norm = lambda value: " ".join(str(value or "").upper().split())
    new = records[0] if len(records) == 1 else None
    evidence = review(new or {"Request_Nbr": case}, today)
    matched = bool(new and norm(old.get("Property_Address")) and
                   norm(old.get("Property_Address")) == norm(new.get("Property_Address")) and
                   (not old.get("Property_APN") or norm(old["Property_APN"]) == norm(new.get("Property_APN"))))
    if not records:
        evidence["status"] = "not_found"
    elif len(records) > 1:
        evidence["status"] = "ambiguous_case"
    elif not matched:
        evidence["status"] = "identity_mismatch"
    owner_changed = bool(new and norm(old.get("Property_Owner")) != norm(new.get("Property_Owner")))
    previous_review = raw.get("nashville_codes_review")
    evidence["owner_changed"] = owner_changed or bool(previous_review.get("owner_changed") if isinstance(previous_review, dict) else False)
    qualifies = bool(matched and evidence["current_code_signal"])
    evidence["current_code_signal"] = qualifies
    payload = {**raw, "nashville_codes_review": evidence}
    if matched:
        payload["nashville_codes"] = new
    changes = {key: {"before": old.get(key), "after": new.get(key)} for key in
               ("Status", "Property_Owner", "Violations_Noted", "Last_Activity_Date")
               if matched and old.get(key) != new.get(key)}
    if changes:
        history = raw.get("nashville_codes_changes")
        payload["nashville_codes_changes"] = ((history if isinstance(history, list) else []) +
                                               [{"checked_on": today, "changes": changes}])[-10:]
    patch = {"raw_payload": payload}
    stage = row.get("staging_status")
    if stage == "pending" and not qualifies:
        patch.update(staging_status="rejected", rejection_reason=INACTIVE_REASON if matched and evidence["status"] == "inactive" else REVIEW_REASON)
    elif stage == "rejected" and row.get("rejection_reason") in (INACTIVE_REASON, REVIEW_REASON) and qualifies:
        patch.update(staging_status="pending", rejection_reason=None)
    return None if all(row.get(k) == v for k, v in patch.items()) else patch

# Higher-signal violation types — properties unlikely to be casually fixed.
# We still scrape everything but tag these as "actionable" in admin_notes
# so Patrick can prioritize.
HIGH_SIGNAL_VIOLATIONS = {
    "UNFIT FOR HABITATION",
    "DEMOLITION ORDER",
    "BLDG MAINTENANCE",
    "STRUCTURAL",
    "ACCUMULATION OF DEBRIS",
    "STANDING WATER",
    "OPEN VACANT BUILDING",
    "VACANT/SUBSTANDARD",
    "ROOF",
    "FOUNDATION",
}


class NashvilleCodesBot(BotBase):
    name = "nashville_codes"
    description = "Nashville Metro Codes — open property standards violations (Davidson Co.)"
    throttle_seconds = 0.5
    expected_min_yield = 100  # typical OPEN backlog is thousands; alert if <100

    def _query(self, where):
        """Keyset pages respect Metro's 1,000-row cap; errors are not emptiness."""
        records = []
        last = -1
        for _ in range(100):
            res = self.fetch(NASHVILLE_CODES_ENDPOINT, timeout=30, params={
                "where": f"({where}) AND ObjectId > {last}", "outFields": "*",
                "returnGeometry": "false", "resultRecordCount": "1000",
                "orderByFields": "ObjectId ASC", "f": "json",
            })
            if res is None or res.status_code != 200:
                raise RuntimeError("Metro case query failed")
            data = res.json()
            if data.get("error") or not isinstance(data.get("features"), list):
                raise RuntimeError("Metro case query returned an error or invalid features")
            batch = [f.get("attributes") for f in data["features"]]
            if any(not isinstance(a, dict) or not isinstance(a.get("ObjectId"), int)
                   or a["ObjectId"] <= last for a in batch):
                raise RuntimeError("Metro case pagination did not advance")
            records.extend(batch)
            if not data.get("exceededTransferLimit"):
                return records
            if not batch:
                raise RuntimeError("Metro returned an incomplete empty page")
            last = max(a["ObjectId"] for a in batch)
        raise RuntimeError("Metro case pagination exceeded safety limit")

    def scrape(self) -> List[LeadPayload]:
        self._started = time.monotonic()
        records = self._query("Status='OPEN'")
        # Conflicting duplicate case numbers cannot be safely keyed to one lead.
        grouped = case_groups(records)
        return [lead for case, values in grouped.items() if len(values) == 1
                if (lead := self._build_lead(values[0])) is not None]

    def _candidates(self, client):
        last = None
        while True:
            q = client.table("homeowner_requests_staging").select(
                "id,pipeline_lead_key,case_num:raw_payload->nashville_codes->>Request_Nbr,"
                "checked_on:raw_payload->nashville_codes_review->>checked_on"
            ).eq("bot_source", self.name).order("id").limit(500)
            if last is not None:
                q = q.gt("id", last)
            rows = q.execute().data or []
            yield from rows
            if len(rows) < 500:
                return
            last = rows[-1]["id"]

    def _write_staging(self, leads):
        client = _supabase()
        if client is None:
            return 0, 0
        candidates = list(self._candidates(client))
        known = {r["pipeline_lead_key"] for r in candidates}
        # Include manual rejections in deduplication; don't recreate a rejected case.
        fresh = [lead for lead in leads if lead.pipeline_lead_key not in known]
        staged, _ = super()._write_staging(fresh) if fresh else (0, 0)
        today = datetime.now(timezone.utc).date().isoformat()
        selected = refresh_candidates(candidates, today)
        self.refresh_counts = {"selected": len(selected), "updated": 0, "failed": 0,
                               "oversized": 0, "conflicts": 0}
        for offset in range(0, len(selected), 50):
            if time.monotonic() - getattr(self, "_started", time.monotonic()) > 450:
                break
            chunk = selected[offset:offset + 50]
            cases = sorted({str(r["case_num"]) for r in chunk})
            where = "Request_Nbr IN (" + ",".join("'" + c.replace("'", "''") + "'" for c in cases) + ")"
            try:
                official = case_groups(self._query(where))
            except Exception as exc:
                self.refresh_counts["failed"] += len(chunk)
                self.logger.warning("Metro refresh batch failed (%s); prior evidence retained", type(exc).__name__)
                continue
            ids = [r["id"] for r in chunk]
            keys = [r["pipeline_lead_key"] for r in chunk]
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                cols = "id,pipeline_lead_key,owner_name_records,raw_payload"
                if table.endswith("_staging"):
                    cols += ",staging_status,rejection_reason"
                q = client.table(table).select(cols)
                q = (q.eq("bot_source", self.name).in_("id", ids) if table.endswith("_staging")
                     else q.eq("source", "bot").in_("pipeline_lead_key", keys))
                try:
                    rows = q.execute().data or []
                except Exception as exc:
                    self.logger.warning("Metro refresh hydration failed %s (%s)", table, type(exc).__name__)
                    self.refresh_counts["failed"] += len(chunk)
                    continue
                for row in rows:
                    if time.monotonic() - getattr(self, "_started", time.monotonic()) > 450:
                        break
                    patch = refresh_patch(row, official, today)
                    if patch is None:
                        continue
                    original = json.dumps(row["raw_payload"], separators=(",", ":"))
                    if len(quote(original, safe="")) > 6000:
                        self.refresh_counts["oversized"] += 1
                        continue
                    update = client.table(table).update(patch).eq("id", row["id"]).eq("raw_payload", original)
                    if table.endswith("_staging"):
                        update = update.eq("staging_status", row["staging_status"])
                        update = (update.is_("rejection_reason", "null") if row.get("rejection_reason") is None
                                  else update.eq("rejection_reason", row["rejection_reason"]))
                    try:
                        if update.execute().data:
                            self.refresh_counts["updated"] += 1
                        else:
                            self.refresh_counts["conflicts"] += 1
                    except Exception as exc:
                        self.refresh_counts["failed"] += 1
                        self.logger.warning("Metro refresh write failed id=%s (%s)", row["id"], type(exc).__name__)
        self.logger.info("Metro refresh %s", self.refresh_counts)
        return staged, len(leads) - len(fresh)

    def _build_lead(self, attrs: dict) -> Optional[LeadPayload]:
        case_num = str(attrs.get("Request_Nbr") or "").strip()
        if attrs.get("Status") != "OPEN":
            return None
        if not case_num:
            return None
        address = str(attrs.get("Property_Address") or "").strip()
        if not address:
            return None
        zip_code = str(attrs.get("ZIP") or "").strip()
        city = str(attrs.get("City") or "Nashville").strip()
        full_address = f"{address}, {city}, TN"
        if zip_code:
            full_address = f"{full_address} {zip_code}"

        owner = str(attrs.get("Property_Owner") or "").strip()
        violation = str(attrs.get("Violations_Noted") or "").strip()
        problem = str(attrs.get("Reported_Problem") or "").strip()

        # Date conversion (epoch ms → ISO)
        date_received = attrs.get("Date_Received")
        date_iso = None
        if isinstance(date_received, (int, float)) and date_received > 0:
            try:
                date_iso = datetime.fromtimestamp(
                    date_received / 1000, tz=timezone.utc
                ).date().isoformat()
            except Exception:
                pass

        # Auctionability gate. Reject leads where every cited violation
        # is in the lawn-only bucket (HIGH WEEDS, TALL GRASS) — those
        # owners just mow and pay the $200 fine, not auction deals. Keep
        # anything with structural / vacant / chronic / dangerous-building
        # codes. The previous filter was passing leads where the COMPLAINT
        # NARRATIVE happened to mention "standing water" but the only
        # cited violation was HIGH WEEDS; we now decide off the actual
        # citation codes (Violations_Noted field).
        from ._cv_filter import is_auctionable_cv
        keep, reason = is_auctionable_cv(violation)
        if not violation or not keep:
            return None
        priority_tag = f" [{reason.upper()[:40]}]"

        notes_parts = [f"case {case_num}"]
        if violation:
            notes_parts.append(f"violation: {violation}")
        elif problem:
            notes_parts.append(f"problem: {problem}")
        if date_iso:
            notes_parts.append(f"received: {date_iso}")
        if priority_tag:
            notes_parts[0] = notes_parts[0] + priority_tag
        admin_notes = " · ".join(notes_parts)

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, case_num),
            property_address=full_address,
            county="Davidson County",
            owner_name_records=owner or None,
            distress_type="CODE_VIOLATION",
            admin_notes=admin_notes,
            source_url=f"https://www.nashville.gov/departments/codes/property-standards/code-enforcement/codes-violation-history-search",
            raw_payload={"nashville_codes": attrs, "nashville_codes_review": review(attrs, datetime.now(timezone.utc).date().isoformat()), **({"condition": cond} if (cond := property_condition(attrs)) else {})},
        )


_CONDITION_RES = {
    # A burned house the owner has not rebuilt: insurance money in hand and
    # no will to start over is a common reason to sell.
    "fire_damage": re.compile(r"\bfire[\s-]*damage|\bburn(?:ed|t)\b|\b(?:house|structure|kitchen)\s+fire\b|\bfire\s+(?:at|in)\s+(?:the\s+)?(?:house|home|property|structure)", re.I),
    "dilapidated": re.compile(r"dilapidated|falling\s+apart|roof\s+(?:is\s+)?caving|collaps", re.I),
    "vacant_open": re.compile(r"\bvacant\b|\babandoned\b|open\s+(?:and|&)\s+unsecured|unsecured\s+(?:structure|building|house)|boarded", re.I),
}


def property_condition(attrs: dict) -> List[str]:
    """Condition tags read from the complaint text and violations noted."""
    text = " ".join(str(attrs.get(k) or "") for k in ("Reported_Problem", "Violations_Noted"))
    return [tag for tag, rx in _CONDITION_RES.items() if rx.search(text)]


def run() -> dict:
    bot = NashvilleCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
