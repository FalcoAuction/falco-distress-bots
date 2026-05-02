"""
Memphis (Shelby County) code enforcement scraper.

Source: 311 Memphis FeatureServer (ArcGIS REST). Public, no auth, no rate
limit, ~28K Code Enforcement records statewide. Updated daily.

Filter: REQUEST_STATUS='Open' + DEPARTMENT='Code Enforcement', then
narrow to high-signal request types (boarded vacant, dilapidated,
unfit, etc) — not the noise of high weeds / vehicle violations.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from ._base import BotBase, LeadPayload


MEMPHIS_311_ENDPOINT = (
    "https://311.memphistn.gov/server/rest/services/311/"
    "311_Request_Map_PROD/FeatureServer/0/query"
)

# High-signal CE request types — these are properties unlikely to be
# casually fixed and indicate genuine distress / motivation to sell.
HIGH_SIGNAL_REQUEST_TYPES = {
    "CE-Dilapidated",
    "CE-Boarded Vacant",
    "CE-Code Miscellaneous",
    "CE-Demolition",
    "CE-Unsafe Structure",
    "CE-Unfit",
    "CE-Open Vacant",
    "CE-Substandard",
    "CE-Major Building Repair",
    "CE-Roof",
    "CE-Foundation",
}


class MemphisCodesBot(BotBase):
    name = "memphis_codes"
    description = "Memphis 311 Code Enforcement — open high-signal violations (Shelby Co.)"
    throttle_seconds = 0.5
    expected_min_yield = 50

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        offset = 0
        page_size = 3000  # Memphis allows up to 3000/page
        max_pages = 8

        for page in range(max_pages):
            params = {
                "where": "DEPARTMENT='Code Enforcement' AND REQUEST_STATUS='Open'",
                "outFields": "*",
                "returnGeometry": "false",
                "resultRecordCount": str(page_size),
                "resultOffset": str(offset),
                "orderByFields": "REPORTED_DATE DESC",
                "f": "json",
            }

            res = self.fetch(MEMPHIS_311_ENDPOINT, params=params)
            if res is None or res.status_code != 200:
                self.logger.error(f"page {page} fetch failed: {res.status_code if res else 'none'}")
                break

            try:
                data = res.json()
            except Exception as e:
                self.logger.error(f"page {page} JSON parse failed: {e}")
                break

            features = data.get("features") or []
            self.logger.info(f"page {page}: {len(features)} features (offset={offset})")
            if not features:
                break

            for feat in features:
                attrs = feat.get("attributes") or {}
                lead = self._build_lead(attrs)
                if lead is not None:
                    leads.append(lead)

            if not data.get("exceededTransferLimit"):
                break
            offset += page_size

        self.logger.info(f"total high-signal leads built: {len(leads)}")
        return leads

    def _build_lead(self, attrs: dict) -> Optional[LeadPayload]:
        # Filter to high-signal request types only
        request_type = str(attrs.get("REQUEST_TYPE") or "").strip()
        if request_type not in HIGH_SIGNAL_REQUEST_TYPES:
            return None

        incident = str(attrs.get("INCIDENT_NUMBER") or "").strip()
        if not incident:
            return None

        address = str(attrs.get("Location_Address") or "").strip()
        if not address:
            return None

        zip_code = str(attrs.get("ZipCode") or "").strip()
        city = str(attrs.get("CITY") or "Memphis").strip()
        full_address = f"{address}, {city}, TN"
        if zip_code:
            full_address = f"{full_address} {zip_code}"

        owner = str(attrs.get("Owner_Name") or "").strip()
        request_summary = str(attrs.get("REQUEST_SUMMARY") or "").strip()

        date_received = attrs.get("REPORTED_DATE")
        date_iso = None
        if isinstance(date_received, (int, float)) and date_received > 0:
            try:
                date_iso = datetime.fromtimestamp(
                    date_received / 1000, tz=timezone.utc
                ).date().isoformat()
            except Exception:
                pass

        notes_parts = [f"#{incident} [HIGH-SIGNAL]", f"type: {request_type}"]
        if request_summary:
            notes_parts.append(f"summary: {request_summary[:100]}")
        if date_iso:
            notes_parts.append(f"reported: {date_iso}")
        admin_notes = " · ".join(notes_parts)

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, incident),
            property_address=full_address,
            county="Shelby County",
            owner_name_records=owner or None,
            distress_type="CODE_VIOLATION",
            admin_notes=admin_notes,
            source_url="https://311.memphistn.gov/",
            raw_payload={"memphis_311": attrs},
        )


def run() -> dict:
    bot = MemphisCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
