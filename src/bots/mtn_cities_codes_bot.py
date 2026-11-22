"""Mt. Juliet municipal Codes Enforcement complaint research.

The city links SeeClickFix from https://www.mtjuliet-tn.gov/152/Code-Enforcement-Division.
Open and Acknowledged describe an intake request. A verified property violation,
ownership or sale authority requires independent records.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any, Optional

from ._base import BotBase, LeadPayload, _supabase, guard_phone_metadata
from ._address import normalize_address


# ─────────────────────────── Mt. Juliet config ──────────────────────────────

MTJULIET_SCF_PLACE = "mount-juliet"
MTJULIET_SCF_BASE = "https://seeclickfix.com/api/v2/issues"
MTJULIET_HTML_BASE = "https://seeclickfix.com/issues/{id}"

CODES_ORG = "Mt. Juliet, TN - Codes Enforcement"
REVIEW = "mt_juliet_issue_review"
# Require an allegation about a building in the narrative. Intake category names,
# commas in prose, abandoned vehicles and general maintenance are not citations.
BUILDING_CONDITION = re.compile(
    r"\b(?:dilapidated|condemned|fire[- ]damaged) (?:house|home|building|structure)\b"
    r"|\b(?:house|home|building|structure)\b[^.!?\n]{0,65}\b(?:condemned|severely damaged|collapsing|torn down)\b"
    r"|\b(?:roof|foundation|ceilings?|walls?) (?:is |are |has |have )?(?:caving in|collapsing|detaching|collapsed)\b"
    r"|\b(?:house|home|building|structure) (?:was |is )?(?:declared|found) (?:unsafe|dangerous)\b"
    r"|\b(?:structural damage|structural failure|unfit for habitation)\b", re.I,
)


class MtnCitiesCodesBot(BotBase):
    name = "mtn_cities_codes"
    description = "Mt. Juliet Codes Enforcement public complaint research"
    throttle_seconds = 1.5
    expected_min_yield = 1

    def scrape(self):
        self.feed_complete = False
        self.issues = {}
        page = 1
        while page <= 40:
            response = self.fetch(MTJULIET_SCF_BASE, params={
                "place_url": MTJULIET_SCF_PLACE, "per_page": 20, "page": page,
                "status": "Open,Acknowledged", "sort": "created_at", "sort_direction": "DESC",
            })
            if response is None or response.status_code != 200:
                raise RuntimeError(f"Mt. Juliet feed unavailable on page {page}")
            data = response.json()
            pagination = data["metadata"]["pagination"]
            if pagination["page"] != page or not isinstance(data["issues"], list):
                raise ValueError("Unexpected Mt. Juliet feed page")
            for issue in data["issues"]:
                self.issues[issue["id"]] = issue
            if pagination["next_page"] is None:
                if len(self.issues) != pagination["entries"]:
                    raise ValueError("Mt. Juliet feed changed during pagination")
                break
            if pagination["next_page"] != page + 1:
                raise ValueError("Unexpected Mt. Juliet next page")
            page += 1
        else:
            raise RuntimeError("Incomplete Mt. Juliet feed")
        leads = []
        for issue in self.issues.values():
            review = self.review(issue)
            if review and review["finding"] == "reported_building_condition":
                leads.append(LeadPayload(
                    bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, f"MJ-SCF-{issue['id']}"),
                    property_address=review["reported_address"], county="Wilson County", distress_type="CODE_VIOLATION",
                    source_url=review["source_url"], admin_notes="Citizen-reported building condition; enforcement unverified.",
                    raw_payload={"city": "Mt. Juliet", "platform": "seeclickfix", "issue": {"id": issue["id"]}, REVIEW: review},
                ))
        self.feed_complete = True
        return leads

    def review(self, issue):
        rt = issue.get("request_type") or {}
        org = rt.get("organization")
        if org not in (CODES_ORG, "Mt. Juliet, TN") or type(issue.get("id")) is not int:
            return None
        status = issue.get("status")
        address = self._normalize_address(issue.get("address") or "")
        narrative = issue.get("description") or ""
        match = next((m for m in BUILDING_CONDITION.finditer(narrative)
                      if not re.search(r"\b(?:no(?: evidence of)?|not|without)\s+(?:an?\s+)?$", narrative[:m.start()], re.I)), None)
        finding = "unverified"
        if org != CODES_ORG:
            finding = "outside_codes"
        elif status in ("Closed", "Archived"):
            finding = "closed_request"
        elif status in ("Open", "Acknowledged"):
            finding = "other_request"
            if match and address and re.match(r"^[1-9]\d*\s+[A-Za-z]", address) and re.search(
                    r"\b(?:Mt\.?|Mount) Juliet,? (?:TN|Tennessee)\b", address, re.I):
                finding = "reported_building_condition"
        return {"issue_id": issue["id"], "reported_address": address, "request_type": rt.get("title"), "organization": org,
                "source_url": MTJULIET_HTML_BASE.format(id=issue["id"]), "checked_on": datetime.now(timezone.utc).date().isoformat(),
                "reported_on": self._iso_date(issue.get("created_at")), "status": status, "finding": finding,
                "condition_excerpt": match.group() if finding == "reported_building_condition" else None,
                "enforcement_verified": False, "sale_authority_verified": False}

    def existing_review(self, row):
        ident = str(row.get("issue_id") or "")
        if not ident.isdigit() or self.make_lead_key(self.name, "MJ-SCF-" + ident) != row["pipeline_lead_key"]:
            return None
        issue = self.issues.get(int(ident))
        if issue is None:
            response = self.fetch(f"{MTJULIET_SCF_BASE}/{ident}")
            if response is None or response.status_code != 200:
                return None
            issue = response.json()
        review = self.review(issue)
        if not review or str(review["issue_id"]) != ident or row["county"] != "Wilson County":
            return None
        address = review["reported_address"]
        if not address or row["property_address"] not in (address, normalize_address(address).normalized):
            return None
        return review

    def _write_staging(self, leads):
        if not getattr(self, "feed_complete", False):
            return 0, 0
        client = _supabase()
        if client is None:
            return 0, 0
        known = set()
        cursor = None
        while True:
            table = client.table("homeowner_requests_staging")
            query = table.select("id,bot_source,pipeline_lead_key,county,property_address,phone_metadata,issue_id:raw_payload->issue->>id").eq("bot_source", self.name)
            if cursor:
                query = query.gt("id", cursor)
            rows = query.order("id").limit(100).execute().data
            for row in rows:
                known.add(row["pipeline_lead_key"])
                review = self.existing_review(row)
                old = row["phone_metadata"]
                if not review or (old is not None and not isinstance(old, dict)):
                    self.logger.warning("Unverified Mt. Juliet request on staging row %s", row["id"])
                    continue
                if (old or {}).get(REVIEW) == review:
                    continue
                update = table.update({"phone_metadata": {**(old or {}), REVIEW: review}})
                for field in ("id", "pipeline_lead_key", "bot_source", "county", "property_address"):
                    update = update.eq(field, row[field])
                update = update.eq("raw_payload->issue->>id", str(review["issue_id"]))
                if not guard_phone_metadata(update, old).execute().data:
                    self.logger.warning("Concurrent Mt. Juliet update skipped for %s", row["id"])
            if len(rows) < 100:
                break
            cursor = rows[-1]["id"]
        fresh = [lead for lead in leads if lead.pipeline_lead_key not in known]
        staged, dupes = super()._write_staging(fresh) if fresh else (0, 0)
        return staged, dupes + len(leads) - len(fresh)

    @staticmethod
    def _normalize_address(addr: str) -> Optional[str]:
        """SeeClickFix addresses look like
            '1003 Windrush Rd Mt Juliet, TN, 37122, USA'
            '1011 Carriage Trl Mount Juliet, Tennessee, 37122'
        Normalize trailing ', USA', collapse whitespace, force a final
        ', TN <ZIP>' tail for the enrichment matcher."""
        if not addr:
            return None
        a = addr.strip()
        a = re.sub(r",\s*USA\s*$", "", a, flags=re.I)
        a = re.sub(r",\s*Tennessee", ", TN", a, flags=re.I)
        a = re.sub(r"\s+", " ", a).strip(" ,")
        return a or None

    @staticmethod
    def _iso_date(value: Any) -> Optional[str]:
        """SeeClickFix returns ISO 8601 with offset, e.g.
            '2026-05-07T16:53:55-04:00'
        We only want the YYYY-MM-DD."""
        if not value:
            return None
        s = str(value)
        try:
            # Python 3.11+ accepts the offset directly.
            return datetime.fromisoformat(s).date().isoformat()
        except ValueError:
            return None


def run() -> dict:
    bot = MtnCitiesCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
