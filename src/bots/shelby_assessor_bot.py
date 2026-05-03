"""
Shelby County (Memphis) Property Assessor enricher.

Shelby is one of the 9 EXTERNAL TPAD counties (the largest TN county
by population). The Shelby County assessor data is exposed publicly
via the Shelby County GIS ArcGIS REST endpoint at
gis.shelbycountytn.gov/arcgis/rest/services/Parcel/. No auth, no
captcha.

Two endpoints used:
  /Parcel/CERT_Parcel/MapServer/0   — parcel polygons + owner +
        mailing address + property class. ~600k Shelby parcels.
  /Parcel/CertParcel_NOAttrib/MapServer/1 — assessment values
        (RTOTAPR = total appraised, APRLAND = land value,
        ASMTLAND/ASMTBLDG = TN-statutory-assessment values)
  /Parcel/CertParcel_NOAttrib/MapServer/8 — sales history with
        SALEDT (epoch millis) + PRICE (dollars) per transaction

Quirks discovered during build:
  - PARID has TWO spaces in middle: "056033  00178" (not one).
    Must URL-encode as "056033%20%2000178".
  - ASSR_ASMT fields APRBLDG + ASMTBLDG return "Invalid connection
    property" — must query around them (RTOTAPR + APRLAND + ASMTLAND
    work fine).
  - PRICE column is dollars-as-integer (no /100 division).
  - SALEDT is epoch milliseconds (UTC).

Output written to:
  - property_value (numeric, dollars)
  - owner_name_records
  - raw_payload.shelby_arcgis (parcel + sale history + assessment
    breakdown)
  - phone_metadata.mortgage_estimate (downstream
    mortgage_estimator_bot picks up last_sale_price + last_sale_date
    from raw_payload to compute current balance)

Distress type: N/A (enricher only).
"""

from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

from ._base import BotBase, _supabase
from ._provenance import record_field


SHELBY_GIS = "https://gis.shelbycountytn.gov/arcgis/rest/services/Parcel"
PARCEL_LAYER = SHELBY_GIS + "/CERT_Parcel/MapServer/0/query"
ASSR_ASMT_LAYER = SHELBY_GIS + "/CertParcel_NOAttrib/MapServer/1/query"
ASSR_SALES_LAYER = SHELBY_GIS + "/CertParcel_NOAttrib/MapServer/8/query"

# Fields that work on ASSR_ASMT (avoiding the broken APRBLDG + ASMTBLDG)
ASSR_FIELDS = "PARID,RTOTAPR,APRLAND,ASMTLAND,CLASS,LUC,RTOTASMT,RTOTGRNAPR"


def _split_address(address: str) -> Optional[Tuple[int, str]]:
    """Extract (street_number, street_keyword) from a full address."""
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


class ShelbyAssessorBot(BotBase):
    name = "shelby_assessor"
    description = "Shelby County (Memphis) GIS-ArcGIS-based property assessor enricher"
    throttle_seconds = 1.0
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
            self.logger.info(f"{len(candidates)} Shelby candidates lacking property_value")

            for row in candidates[:self.max_leads_per_run]:
                addr = row.get("property_address") or ""
                hit = self._lookup(addr)
                if hit is None:
                    not_found += 1
                    continue

                update: Dict[str, Any] = {}
                if hit.get("appraised") and not row.get("property_value"):
                    update["property_value"] = hit["appraised"]
                if hit.get("owner") and not row.get("owner_name_records"):
                    update["owner_name_records"] = hit["owner"]

                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["shelby_arcgis"] = hit
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
                                          update["property_value"], "shelby_assessor",
                                          confidence=1.0,
                                          metadata={"parid": hit.get("parid"),
                                                    "tax_year": hit.get("tax_year")})
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
                    .or_("county.eq.shelby,property_address.ilike.%memphis%,property_address.ilike.%bartlett%,property_address.ilike.%germantown%,property_address.ilike.%collierville%,property_address.ilike.%cordova%,property_address.ilike.%millington%,property_address.ilike.%lakeland%,property_address.ilike.%arlington%")
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
        parts = _split_address(address)
        if not parts:
            return None
        number, street = parts

        # Step 1: find PARID + owner via parcel layer
        where = f"PAR_ADRSTR='{street}' AND PAR_ADRNO={number}"
        res = self.fetch(
            PARCEL_LAYER,
            params={
                "where": where,
                "outFields": "PARID,OWNER,PAR_ADRNO,PAR_ADRSTR,MAP,CLASS,OWN_ADRNO,OWN_ADRSTR,OWN_CITY,OWN_STATE,OWN_ZIP",
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
        f0 = features[0]["attributes"]
        parid = f0.get("PARID")
        if not parid:
            return None

        out: Dict[str, Any] = {
            "parid": parid,
            "owner": f0.get("OWNER"),
            "property_address": f"{f0.get('PAR_ADRNO')} {f0.get('PAR_ADRSTR')}",
            "owner_mailing": " ".join(filter(None, [
                str(f0.get("OWN_ADRNO") or ""),
                f0.get("OWN_ADRSTR") or "",
            ])).strip(),
            "owner_city": f0.get("OWN_CITY"),
            "owner_state": f0.get("OWN_STATE"),
            "owner_zip": f0.get("OWN_ZIP"),
            "class": f0.get("CLASS"),
            "map": f0.get("MAP"),
        }

        # Step 2: pull appraisal data from ASSR_ASMT
        encoded_parid = quote(parid)
        asmt_res = self.fetch(
            ASSR_ASMT_LAYER,
            params={
                "where": f"PARID='{parid}'",
                "outFields": ASSR_FIELDS,
                "returnGeometry": "false",
                "f": "json",
            },
        )
        if asmt_res is not None and asmt_res.status_code == 200:
            try:
                asmt_data = asmt_res.json()
                asmt_features = asmt_data.get("features") or []
                if asmt_features:
                    af = asmt_features[0]["attributes"]
                    out["appraised"] = af.get("RTOTAPR")
                    out["land_value"] = af.get("APRLAND")
                    out["assessed_land"] = af.get("ASMTLAND")
                    out["assessed_total"] = af.get("RTOTASMT")
                    out["asmt_class"] = af.get("CLASS")
                    out["asmt_luc"] = af.get("LUC")
            except Exception:
                pass

        # Step 3: pull most recent sale from ASSR_SALES
        sales_res = self.fetch(
            ASSR_SALES_LAYER,
            params={
                "where": f"PARID='{parid}'",
                "outFields": "PARID,SALEDT,PRICE,SALETYPE,INSTRTYP",
                "returnGeometry": "false",
                "f": "json",
                "orderByFields": "SALEDT DESC",
            },
        )
        if sales_res is not None and sales_res.status_code == 200:
            try:
                sales_data = sales_res.json()
                sales_features = sales_data.get("features") or []
                if sales_features:
                    sales = []
                    for s in sales_features:
                        a = s["attributes"]
                        # Convert SALEDT (epoch ms) to YYYY-MM-DD
                        saledt_iso = None
                        if a.get("SALEDT"):
                            try:
                                saledt_iso = datetime.fromtimestamp(
                                    a["SALEDT"] / 1000.0, tz=timezone.utc
                                ).date().isoformat()
                            except Exception:
                                pass
                        sales.append({
                            "date": saledt_iso,
                            "price": a.get("PRICE"),
                            "saletype": a.get("SALETYPE"),
                            "instrument": a.get("INSTRTYP"),
                        })
                    sales.sort(key=lambda x: x["date"] or "0", reverse=True)
                    out["sales_history"] = sales[:5]
                    if sales:
                        out["last_sale_date"] = sales[0]["date"]
                        out["last_sale_price"] = sales[0]["price"]
            except Exception:
                pass

        return out


def run() -> dict:
    bot = ShelbyAssessorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = ShelbyAssessorBot()
        for addr in sys.argv[1:]:
            print(f"{addr}: {bot._lookup(addr)}")
    else:
        print(run())
