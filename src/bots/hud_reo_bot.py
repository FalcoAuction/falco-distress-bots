"""
HUD REO bot — pulls FHA-insured foreclosed properties currently held by
HUD for resale, statewide TN.

Source: HUD's public ArcGIS REST endpoint. No auth, no captcha, no rate
limit. Returns ~160 active TN properties at any given time.

Filter: case_step_number IN (1, 2, 3) — these are active/early-lifecycle
listings. Step 4-6 are conveyed/sold.

Limitations of v1:
  - No list price (the price lives on the SPA storefront, would need
    Playwright/Selenium to extract). For homeowner outreach this is
    fine — the property is bank-owned, NOT a homeowner we'd contact.
  - REO leads are different from our typical homeowner-distress flow.
    They go to the AUCTION-PARTNER-ROUTING bucket: Parks could potentially
    bid on or list these for the bank. We stage them so Patrick can
    decide what to do with them.

Distress type: REO
"""

from __future__ import annotations

from typing import List, Optional

from ._base import BotBase, LeadPayload


HUD_REO_ENDPOINT = (
    "https://egis.hud.gov/arcgis/rest/services/gotit/REOProperties/MapServer/0/query"
)


class HudReoBot(BotBase):
    name = "hud_reo"
    description = "HUD-owned FHA REO properties statewide TN"
    throttle_seconds = 0.5  # public ArcGIS, no rate limit issues
    expected_min_yield = 50  # we typically see 80-100 active TN; alert if <50

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []

        params = {
            "where": "STATE_CODE='TN'",
            "outFields": "OBJECTID,CASE_NUM,CASE_STEP_NUMBER,STREET_NUM,DIRECTION_PREFIX,STREET_NAME,CITY,STATE_CODE,DISPLAY_ZIP_CODE,REVITE_NAME",
            "returnGeometry": "false",
            "f": "json",
            "resultRecordCount": "1000",
        }

        res = self.fetch(HUD_REO_ENDPOINT, params=params)
        if res is None or res.status_code != 200:
            self.logger.error(f"HUD ArcGIS endpoint returned {res.status_code if res else 'no-response'}")
            return leads

        try:
            data = res.json()
        except Exception as e:
            self.logger.error(f"failed to parse JSON: {e}")
            return leads

        features = data.get("features") or []
        self.logger.info(f"ArcGIS returned {len(features)} TN records")

        for feat in features:
            attrs = feat.get("attributes") or {}
            case_num = (attrs.get("CASE_NUM") or "").strip()
            if not case_num:
                continue

            step = attrs.get("CASE_STEP_NUMBER")
            # Filter to active lifecycle stages — skip step 4+ (conveyed/sold)
            if step is None or step > 3:
                continue

            address = self._build_address(attrs)
            if not address:
                continue

            city = str(attrs.get("CITY") or "").strip().title()
            zip_code = str(attrs.get("DISPLAY_ZIP_CODE") or "").strip()
            full_address = address
            if city:
                full_address = f"{full_address}, {city}, TN"
                if zip_code:
                    full_address = f"{full_address} {zip_code}"

            lead_key = self.make_lead_key(self.name, case_num)

            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=lead_key,
                property_address=full_address,
                county=None,  # not in ArcGIS feed; can be cross-referenced later
                distress_type="REO",
                admin_notes=f"HUD case {case_num} · step {step} · revite={attrs.get('REVITE_NAME') or 'none'}",
                source_url=f"https://www.hudhomestore.gov/propertydetails?caseNumber={case_num}",
                raw_payload={"hud_arcgis": attrs},
            ))

        return leads

    @staticmethod
    def _build_address(attrs: dict) -> Optional[str]:
        street_num = str(attrs.get("STREET_NUM") or "").strip()
        direction = str(attrs.get("DIRECTION_PREFIX") or "").strip()
        street_name = str(attrs.get("STREET_NAME") or "").strip()
        if not street_num or not street_name:
            return None
        parts = [street_num]
        if direction:
            parts.append(direction)
        parts.append(street_name.title())
        return " ".join(parts)


def run() -> dict:
    """Entry point matching the existing bot convention. Called from run_all.py."""
    bot = HudReoBot()
    return bot.run()


if __name__ == "__main__":
    result = run()
    print(f"\nResult: {result}")
