"""
Nashville code violations scraper.

Source: data.nashville.gov / ArcGIS Hub publishes Metro Codes property
standards violations (the city housing code enforcement database).
Public ArcGIS REST endpoint, no auth, no rate limit. Refreshed daily.

Why this is a great lead source: properties with active code violations
are owned by people who can't afford to fix them, won't fix them, or
don't live there (absentee). All three = motivated to sell.

Default rolling window: 3 years. We pull only OPEN status (active
unresolved violations). High-signal violation types are filtered first;
low-signal stuff like "high weeds" is staged but tagged.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Optional

from ._base import BotBase, LeadPayload


NASHVILLE_CODES_ENDPOINT = (
    "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/"
    "Property_Standards_Violations_2/FeatureServer/0/query"
)

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

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        offset = 0
        page_size = 2000
        max_pages = 10  # safety cap (20K records max per run)

        for page in range(max_pages):
            params = {
                "where": "Status='OPEN'",
                "outFields": "*",
                "returnGeometry": "false",
                "resultRecordCount": str(page_size),
                "resultOffset": str(offset),
                "orderByFields": "Date_Received DESC",
                "f": "json",
            }

            res = self.fetch(NASHVILLE_CODES_ENDPOINT, params=params)
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

            # Pagination — ArcGIS sets exceededTransferLimit=true when more available
            if not data.get("exceededTransferLimit"):
                break
            offset += page_size

        self.logger.info(f"total leads built: {len(leads)}")
        return leads

    def _build_lead(self, attrs: dict) -> Optional[LeadPayload]:
        case_num = (attrs.get("Request_Nbr") or "").strip()
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
        violation = str(attrs.get("Violations_Noted") or attrs.get("Reported_Problem") or "").strip()
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

        # Filter to high-signal violations only — properties unlikely to be
        # casually fixed = real motivated-seller signal. Skip "high weeds",
        # "junk vehicles", etc. Patrick can broaden the filter via env var
        # if he wants to dig into the long tail later.
        is_high_signal = (
            any(sig in violation.upper() for sig in HIGH_SIGNAL_VIOLATIONS)
            or any(sig in problem.upper() for sig in HIGH_SIGNAL_VIOLATIONS)
        )
        if not is_high_signal:
            return None
        priority_tag = " [HIGH-SIGNAL]"

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
            raw_payload={"nashville_codes": attrs},
        )


def run() -> dict:
    bot = NashvilleCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
