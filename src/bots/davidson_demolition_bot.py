"""
Davidson County demolition + fire-damage building permits.

Source: data.nashville.gov / ArcGIS Feature Service for "Building Permits
Issued". Public, no auth, no rate limit. Updated continuously.

Issued permits document proposed work. Review current county ownership and independent
open structural findings before treating a permit as an auction prospect.

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

from collections import defaultdict

from .stalled_build_bot import PARCELS, STRUCTURAL, PUBLIC_OWNER_RE
from .nashville_codes_bot import NASHVILLE_CODES_ENDPOINT
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


def permit_review(permit, parcel, cases):
    owner = parcel.get("Owner")
    acquired = parcel.get("OwnDate")
    issued = permit.get("Date_Issued")
    findings = []
    for case in cases:
        if (owner and case.get("Property_APN") == parcel.get("APN") and case.get("Status") == "OPEN"
                and case.get("Request_Nbr") and str(case.get("Property_Owner") or "").strip().upper() == owner.strip().upper()
                and acquired and case.get("Date_Received", 0) and case["Date_Received"] >= acquired
                and STRUCTURAL.search(case.get("Violations_Noted") or "")):
            findings.append({"case": case["Request_Nbr"], "finding": case["Violations_Noted"]})
    if not owner:
        status = "owner_unverified"
    elif PUBLIC_OWNER_RE.search(owner):
        status = "public_owner"
    elif acquired and issued and acquired > issued:
        status = "ownership_changed"
    else:
        status = "open_structural_case" if findings else "permit_only"
    return {"status": status, "apn": parcel.get("APN"), "owner": owner,
            "county_appraisal": parcel.get("TotlAppr"), "findings": findings,
            "parcel_source": PARCELS, "case_source": NASHVILLE_CODES_ENDPOINT,
            "checked_at": datetime.now(timezone.utc).date().isoformat()}


class DavidsonDemolitionBot(BotBase):
    name = "davidson_demolition"
    description = "Davidson Metro Codes — issued demolition + fire-damage building permits"
    throttle_seconds = 0.5
    expected_min_yield = 5  # weekly cadence: 5 new demolitions in 180d is the floor

    def _write_staging(self, leads):
        from ._base import _supabase
        from .free_county_completion_bot import apply

        result = super()._write_staging(leads)
        client = _supabase()
        if client is None:
            return result
        keyed = {lead.pipeline_lead_key: lead for lead in leads}
        keys = list(keyed)
        for start in range(0, len(keys), 80):
            rows = (client.table("homeowner_requests_staging")
                    .select("id,pipeline_lead_key,raw_payload,owner_name_records,property_value,property_value_source")
                    .eq("bot_source", self.name).eq("distress_type", "DEMOLITION")
                    .in_("pipeline_lead_key", keys[start:start + 80]).execute().data or [])
            metadata = []
            for row in rows:
                lead = keyed[row["pipeline_lead_key"]]
                review = lead.raw_payload["demolition_review"]
                if (row.get("raw_payload") or {}).get("demolition_review") != review:
                    metadata.append({"id": row["id"], "raw_patch": {"demolition_review": review}})
                patch = {}
                owner = review["owner"]
                contact = lead.raw_payload["davidson_demolition_permit"].get("Contact")
                if row.get("owner_name_records") != owner and (owner or row.get("owner_name_records") == contact):
                    patch["owner_name_records"] = owner
                value = review["county_appraisal"]
                source = row.get("property_value_source")
                if value and (not row.get("property_value") or source in (
                        "xref_avm_enricher", "williamson_assessor", "metro_nashville_parcels", "davidson_assessor")):
                    if source != "metro_nashville_parcels" or float(row.get("property_value") or 0) != value:
                        patch.update(property_value=value, property_value_source="metro_nashville_parcels")
                if patch:
                    apply(client, "homeowner_requests_staging", row, patch)
            if metadata:
                client.rpc("apply_staging_enrichment", {"p_rows": metadata}).execute()
        return result

    def _county_rows(self, endpoint, field, values, fields, extra=""):
        values = sorted({str(v) for v in values if v})
        for start in range(0, len(values), 60):
            quoted = ",".join("'" + v.replace("'", "''") + "'" for v in values[start:start + 60])
            offset = 0
            while True:
                res = self.fetch(endpoint, params={
                    "where": f"{field} IN ({quoted})" + extra, "outFields": fields,
                    "returnGeometry": "false", "resultOffset": offset,
                    "resultRecordCount": 1000, "orderByFields": field, "f": "json",
                }, timeout=60)
                if res is None or res.status_code != 200:
                    raise RuntimeError("County demolition review unavailable")
                body = res.json()
                if "features" not in body:
                    raise RuntimeError("County demolition review missing features")
                features = body["features"]
                yield from (f["attributes"] for f in features)
                if not body.get("exceededTransferLimit"):
                    break
                if not features:
                    raise RuntimeError("Incomplete county demolition review")
                offset += len(features)

    def county_reviews(self, permits):
        by_id, by_address, cases = defaultdict(list), defaultdict(list), defaultdict(list)
        fields = "APN,PropAddr,PropCity,Owner,OwnDate,TotlAppr"
        for row in self._county_rows(PARCELS, "APN", (p.get("Parcel") for p in permits), fields):
            by_id[row["APN"]].append(row)
        for row in self._county_rows(PARCELS, "PropAddr", (p.get("Address") for p in permits if not p.get("Parcel")), fields):
            by_address[(row["PropAddr"], row["PropCity"])].append(row)
        matches = []
        for p in permits:
            rows = (by_id[p["Parcel"]] if p.get("Parcel") else by_address[(p.get("Address"), p.get("City"))])
            matches.append(rows[0] if len(rows) == 1 else {})
        for row in self._county_rows(NASHVILLE_CODES_ENDPOINT, "Property_APN", (m.get("APN") for m in matches),
                                    "Property_APN,Status,Request_Nbr,Property_Owner,Date_Received,Violations_Noted",
                                    " AND Status='OPEN'"):
            cases[row["Property_APN"]].append(row)
        return [permit_review(p, m, cases[m.get("APN")]) for p, m in zip(permits, matches)]

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

        reviews = self.county_reviews([lead.raw_payload["davidson_demolition_permit"] for lead in leads])
        for lead, review in zip(leads, reviews):
            lead.raw_payload["demolition_review"] = review
            lead.owner_name_records = review["owner"]
            lead.admin_notes += f" | County review: {review['status']}"
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

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, permit_no),
            property_address=full_address,
            county="Davidson County",
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
