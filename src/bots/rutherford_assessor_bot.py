"""
Rutherford County (Murfreesboro) Property Assessor enricher.

Rutherford is one of the 9 EXTERNAL TPAD counties — TPAD doesn't
cover it; the county runs its own ArcGIS Online feature service that
exposes the FULL parcel + appraisal + sales database publicly.

Endpoint:
  https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/
  Parcel_Data/FeatureServer/0

Available fields (the richest of all 5 EXTERNAL TPAD county sources):
  ParcelID, Grantee (current owner), Grantor (prior owner),
  FormattedLocation (full property address),
  MailingAddress, MailingCity, MailingState, MailingZipCode,
  LocationCity, LocationZip,
  TotalValue (appraised), TotalLandValue, TotalBuildingValue,
  TotalYardItemValue, TotalAssessedValue, TotalLandValueWithAg,
  TotalArea, TotalAreaUOM,
  SaleDate (epoch ms), SalePrice,
  YearBuilt, BuildingTypeDescription, StoryHeight,
  AppraisalArea, NALCode, AccountType, ImprovedStatus,
  Subdivision, MapBook, Block, Lot, Section,
  Commercial, Condo, Exempt, Lease (boolean flags),
  LegalReference, AgriculturalCredit

Output:
  - property_value (TotalValue)
  - owner_name_records (Grantee)
  - raw_payload.rutherford_arcgis (full record + sale history)

Distress type: N/A (enricher only).
"""

from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field


RUTHERFORD_BASE = (
    "https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/"
    "Parcel_Data/FeatureServer/0/query"
)

# Rutherford-county cities (LocationCity values we filter candidates by)
RUTHERFORD_CITIES = (
    "MURFREESBORO", "SMYRNA", "LA VERGNE", "LAVERGNE", "EAGLEVILLE",
)


def _street_keywords(address: str) -> Optional[Tuple[int, str]]:
    """Extract (street_number, first_street_token) from a full address."""
    if not address:
        return None
    head = address.split(",")[0].strip()
    parts = head.split(None, 1)
    if len(parts) < 2:
        return None
    try:
        number = int(parts[0])
    except ValueError:
        return None
    rest = parts[1]
    tokens = [t for t in rest.split() if t.upper() not in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}]
    if not tokens:
        return None
    return (number, tokens[0].upper())


class RutherfordAssessorBot(BotBase):
    name = "rutherford_assessor"
    description = "Rutherford County (Murfreesboro) ArcGIS-based property assessor enricher"
    throttle_seconds = 0.8
    expected_min_yield = 1

    max_leads_per_run = 200

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "enriched": 0, "skipped": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        enriched = 0
        skipped = 0
        not_found = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} Rutherford candidates lacking property_value")

            for row in candidates[:self.max_leads_per_run]:
                addr = row.get("property_address") or ""
                hit = self._lookup(addr)
                if hit is None:
                    not_found += 1
                    continue

                update: Dict[str, Any] = {}
                # Authoritative — override any prior HMDA-anchored phantom.
                if hit.get("appraised"):
                    update["property_value"] = hit["appraised"]
                    update["property_value_source"] = "rutherford_assessor"
                if hit.get("owner") and not row.get("owner_name_records"):
                    update["owner_name_records"] = hit["owner"]

                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["rutherford_arcgis"] = hit
                update["raw_payload"] = existing_raw

                if not update:
                    skipped += 1
                    continue

                table = row["__table__"]
                try:
                    client.table(table).update(update).eq("id", row["id"]).execute()
                    enriched += 1
                    if table == "homeowner_requests":
                        if "property_value" in update:
                            record_field(client, row["id"], "property_value",
                                          update["property_value"], "rutherford_assessor",
                                          confidence=1.0,
                                          metadata={"parcel_id": hit.get("parcel_id"),
                                                    "year_built": hit.get("year_built")})
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif enriched == 0 and not_found == 0:
            status = "zero_yield"
        elif enriched == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=enriched + skipped + not_found,
            parsed_count=enriched + skipped,
            staged_count=enriched, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(f"enriched={enriched} skipped={skipped} not_found={not_found}")
        return {
            "name": self.name, "status": status,
            "enriched": enriched, "skipped": skipped, "not_found": not_found,
            "error": error_message,
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + skipped + not_found,
        }

    def _candidates(self, client) -> List[Dict[str, Any]]:
        out = []
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                q = (
                    client.table(table)
                    .select("id, property_address, county, owner_name_records, property_value, raw_payload")
                    .or_("county.eq.rutherford,property_address.ilike.%murfreesboro%,property_address.ilike.%smyrna%,property_address.ilike.%la vergne%,property_address.ilike.%lavergne%,property_address.ilike.%eagleville%")
                    .is_("property_value", "null")
                    .limit(500)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                for r in rows:
                    r["__table__"] = table
                    out.append(r)
            except Exception as e:
                self.logger.warning(f"candidates query on {table} failed: {e}")
        return out

    def _lookup(self, address: str) -> Optional[Dict[str, Any]]:
        parts = _street_keywords(address)
        if not parts:
            return None
        number, street = parts

        # Use FormattedLocation LIKE — leading % wildcards confuse URL encoding,
        # so anchor at start with the number then wildcard the rest
        where = f"FormattedLocation LIKE '{number} {street}%'"
        res = self.fetch(
            RUTHERFORD_BASE,
            params={
                "where": where,
                "outFields": ",".join([
                    "ParcelID", "Grantee", "Grantor",
                    "FormattedLocation", "LocationCity", "LocationZip",
                    "MailingAddress", "MailingCity", "MailingState", "MailingZipCode",
                    "TotalValue", "TotalLandValue", "TotalBuildingValue",
                    "TotalAssessedValue",
                    "SaleDate", "SalePrice",
                    "YearBuilt", "BuildingTypeDescription",
                    "AccountType", "ImprovedStatus",
                    "Subdivision", "MapBook",
                ]),
                "returnGeometry": "false",
                "f": "json",
            },
        )
        if res is None or res.status_code != 200:
            return None
        try:
            data = res.json()
        except Exception:
            return None
        features = data.get("features") or []
        if not features:
            return None

        # Pick best match: starts with the street number
        target_prefix = f"{number} "
        best = None
        for feat in features:
            attrs = feat.get("attributes") or {}
            loc = (attrs.get("FormattedLocation") or "").upper()
            if loc.startswith(target_prefix):
                best = attrs
                break
        if best is None:
            best = features[0].get("attributes") or {}

        sale_date_iso = None
        if best.get("SaleDate"):
            try:
                sale_date_iso = datetime.fromtimestamp(
                    best["SaleDate"] / 1000.0, tz=timezone.utc
                ).date().isoformat()
            except Exception:
                pass

        return {
            "parcel_id": best.get("ParcelID"),
            "owner": best.get("Grantee"),
            "prior_owner": best.get("Grantor"),
            "property_address": best.get("FormattedLocation"),
            "property_city": best.get("LocationCity"),
            "property_zip": best.get("LocationZip"),
            "owner_mailing": best.get("MailingAddress"),
            "owner_city": best.get("MailingCity"),
            "owner_state": best.get("MailingState"),
            "owner_zip": best.get("MailingZipCode"),
            "appraised": best.get("TotalValue"),
            "land_value": best.get("TotalLandValue"),
            "building_value": best.get("TotalBuildingValue"),
            "assessed_value": best.get("TotalAssessedValue"),
            "last_sale_date": sale_date_iso,
            "last_sale_price": best.get("SalePrice"),
            "year_built": best.get("YearBuilt"),
            "building_type": best.get("BuildingTypeDescription"),
            "subdivision": best.get("Subdivision"),
        }


def run() -> dict:
    bot = RutherfordAssessorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = RutherfordAssessorBot()
        for addr in sys.argv[1:]:
            print(f"{addr}: {bot._lookup(addr)}")
    else:
        print(run())
