"""
Davidson County demolition + fire-damage building permits.

Source: data.nashville.gov / ArcGIS Feature Service for "Building Permits
Issued". Public, no auth, no rate limit. Updated continuously.

Why this is a high-leverage lead source: an issued demolition permit
means the owner has paid the city and committed to tearing down the
structure. They've already given up on the property. Same for fire-
damage rehab permits — uninsured or underinsured fires turn into forced
sales when owners can't fund the repair.

This is genuinely free data nobody else in TN distress mining is using.
8 demolition permits filed in Davidson in the last 5 days alone (~30-60/
month run rate).

Endpoint:
  https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/
    Building_Permits_Issued_2/FeatureServer/0/query

Distress type: DEMOLITION (own pipeline category since 2026-05-09).
Maps to the demolition math-sheet scenario which models the owner's
"stay the course" commitment (demo cost + new construction + months
of carry) vs auction-now cash. Different conversation than foreclosure.

Subtype is encoded in admin_notes ("category: teardown" / fire_damage
/ storm_damage / major_rebuild) and drives:
  - SMS opener language (3 variants: teardown, fire, storm)
  - Math sheet defaults (auction clearance + cost legs)
  - Dialer queue prioritization (rep can call HOT subset first)
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import List, Optional

from ._base import BotBase, LeadPayload


ENDPOINT = (
    "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/"
    "Building_Permits_Issued_2/FeatureServer/0/query"
)

# Permit types we ingest. Only the high-distress ones — regular new
# construction / additions / siding aren't lead-worthy.
DISTRESS_PERMIT_TYPES = (
    "Building Demolition Permit",
    "Building Commercial - Fire Damage",
    "Building Residential Rehab Storm Damage",
)

# Window: only pull permits issued in the last 180 days. Older
# demolitions have already happened (or been scheduled) — the
# conversation window is closed by then.
LOOKBACK_DAYS = 180


class DavidsonDemolitionBot(BotBase):
    name = "davidson_demolition"
    description = "Davidson Metro Codes — issued demolition + fire-damage building permits"
    throttle_seconds = 0.5
    expected_min_yield = 5  # weekly cadence: 5 new demolitions in 180d is the floor

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        offset = 0
        page_size = 1000
        max_pages = 5  # 5K cap

        # ArcGIS expects an SQL TIMESTAMP literal for date comparisons —
        # not unix-ms (the latter parses but matches nothing here).
        cutoff = (datetime.now(tz=timezone.utc) - timedelta(days=LOOKBACK_DAYS)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        type_clause = " OR ".join(
            f"Permit_Type_Description='{t}'" for t in DISTRESS_PERMIT_TYPES
        )
        where = f"({type_clause}) AND Date_Issued >= TIMESTAMP '{cutoff}'"

        for page in range(max_pages):
            params = {
                "where": where,
                "outFields": "*",
                "returnGeometry": "false",
                "resultRecordCount": str(page_size),
                "resultOffset": str(offset),
                "orderByFields": "Date_Issued DESC",
                "f": "json",
            }
            res = self.fetch(ENDPOINT, params=params)
            if res is None or res.status_code != 200:
                self.logger.error(
                    f"page {page} fetch failed: {res.status_code if res else 'none'}"
                )
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

        self.logger.info(f"total distress permits built: {len(leads)}")
        return leads

    def _build_lead(self, attrs: dict) -> Optional[LeadPayload]:
        permit_no = str(attrs.get("Permit__") or "").strip()
        if not permit_no:
            return None
        address = str(attrs.get("Address") or "").strip()
        if not address:
            return None
        city = str(attrs.get("City") or "Nashville").strip()
        zip_code = str(attrs.get("ZIP") or "").strip()
        full_address = f"{address}, {city}, TN"
        if zip_code:
            full_address = f"{full_address} {zip_code}"

        permit_type = str(attrs.get("Permit_Type_Description") or "").strip()
        purpose = str(attrs.get("Purpose") or "").strip()
        contact = str(attrs.get("Contact") or "").strip()
        cost = attrs.get("Const_Cost")
        parcel = str(attrs.get("Parcel") or "").strip()

        date_issued = attrs.get("Date_Issued")
        date_iso = None
        if isinstance(date_issued, (int, float)) and date_issued > 0:
            try:
                date_iso = datetime.fromtimestamp(
                    date_issued / 1000, tz=timezone.utc
                ).date().isoformat()
            except Exception:
                pass

        # Subtype categorization. Mirrors classifyDemolitionSubtype()
        # in falco-site/src/app/admin/math-sheet/[id]/demolition-data.ts —
        # keep these in sync.
        cost_num = float(cost) if isinstance(cost, (int, float)) else 0.0
        pt_lower = permit_type.lower()
        if "fire damage" in pt_lower:
            subtype = "fire_damage"
        elif "storm damage" in pt_lower:
            subtype = "storm_damage"
        elif "demolition" in pt_lower:
            subtype = "major_rebuild" if cost_num >= 300_000 else "teardown"
        else:
            subtype = "unknown"

        # Surface short-form purpose / cost / SUBTYPE in admin_notes so
        # the dialer caller knows whether this is a teardown ($5-15K
        # demo permit cost) vs. a major rebuild ($100K+) vs. fire/storm
        # rehab. Subtype drives SMS opener selection + math sheet
        # defaults downstream.
        notes_parts = [
            f"permit {permit_no}",
            f"type: {permit_type}",
            f"category: {subtype}",
        ]
        if cost_num > 0:
            notes_parts.append(f"cost: ${int(cost_num):,}")
        if purpose:
            notes_parts.append(f"purpose: {purpose[:120]}")
        if date_iso:
            notes_parts.append(f"issued: {date_iso}")
        if parcel:
            notes_parts.append(f"parcel: {parcel}")
        admin_notes = " · ".join(notes_parts)

        # Owner_name_records gets the contractor / contact when present;
        # may be the owner directly or the licensed contractor pulling
        # the permit. The skip-trace pass downstream handles owner lookup
        # from the address regardless.
        owner = contact if contact and not contact.lower().startswith("self") else None

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, permit_no),
            property_address=full_address,
            county="Davidson County",
            owner_name_records=owner,
            distress_type="DEMOLITION",
            admin_notes=admin_notes,
            source_url="https://data.nashville.gov/datasets/nashville::building-permits-issued",
            raw_payload={"davidson_demolition_permit": attrs},
        )


def run() -> dict:
    bot = DavidsonDemolitionBot()
    return bot.run()


if __name__ == "__main__":
    import sys
    result = run()
    print(result)
    sys.exit(0 if result.get("status") != "failed" else 1)
