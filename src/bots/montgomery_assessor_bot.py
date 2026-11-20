"""
Montgomery County (Clarksville) parcel enricher.

Montgomery is one of the 9 counties TPAD and the Comptroller files do not
cover. Its old Parcels service now requires a token; the public web-map
layer below is open but blanks every appraisal field ($0). It still carries
the owner, mailing address, building facts and the last sale, so:

  - property_value is set from the last sale only when it is recent
    (SALE_MAX_AGE_YEARS) and arm's length (>= SALE_MIN_PRICE), and is
    labeled property_value_source = "montgomery_last_sale", never as an
    appraisal
  - owner_name_records is filled when missing
  - raw_payload.montgomery_gis keeps the record, or status "not_found", so a
    lead is looked up once

Endpoint:
  https://gis.mcgtn.org/arcgis/rest/services/ParcelMapForWebApplication/MapServer/0

Distress type: N/A (enricher only).
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from .rutherford_assessor_bot import _street_keywords

MONTGOMERY_PARCELS = ("https://gis.mcgtn.org/arcgis/rest/services/"
                      "ParcelMapForWebApplication/MapServer/0/query")
FIELDS = ("ParcelID,Owner1,Owner2,PropertyAddress,PropertyCity,MailingAddress,MailCity,MailState,MailZip,"
          "SalesDate,SalesPrice,YearBuilt,LivingArea,CalcAcreage,AssessmentClass,LandUseDesc,BldgMktValue")
SALE_MAX_AGE_YEARS = 6
SALE_MIN_PRICE = 50_000


def _money(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v) if v > 0 else None
    digits = re.sub(r"[^\d.]", "", str(v))
    try:
        n = int(float(digits)) if digits else 0
    except ValueError:
        return None
    return n if n > 0 else None


def sale_value(price: Any, sale_ms: Any, today: datetime) -> Optional[int]:
    """Last sale as a value only when recent and arm's length."""
    p = _money(price)
    if not p or p < SALE_MIN_PRICE or not sale_ms:
        return None
    try:
        sold = datetime.fromtimestamp(float(sale_ms) / 1000.0, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
    return p if (today - sold).days <= SALE_MAX_AGE_YEARS * 365 else None


class MontgomeryAssessorBot(BotBase):
    name = "montgomery_assessor"
    description = "Montgomery County (Clarksville) open parcel layer: owner, mailing, last sale"
    throttle_seconds = 0.5
    expected_min_yield = 0
    max_leads_per_run = 600

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase", "enriched": 0, "staged": 0,
                    "duplicates": 0, "fetched": 0}
        valued = enriched = not_found = 0
        error_message: Optional[str] = None
        try:
            for row in self._candidates(client)[:self.max_leads_per_run]:
                hit = self._lookup(row.get("property_address") or "")
                if hit == "error":
                    continue  # service down: try again next run
                if hit is None:
                    not_found += 1
                update: Dict[str, Any] = {}
                if hit:
                    value = sale_value(hit.get("last_sale_price_raw"), hit.get("last_sale_ms"), started)
                    if value:
                        update["property_value"] = value
                        update["property_value_source"] = "montgomery_last_sale"
                        valued += 1
                    if hit.get("owner") and not row.get("owner_name_records"):
                        update["owner_name_records"] = hit["owner"]
                raw = self._raw_payload(client, row["__table__"], row["id"])
                if raw is None:
                    continue
                raw["montgomery_gis"] = hit or {"status": "not_found", "checked_at": started.isoformat()}
                update["raw_payload"] = raw
                try:
                    client.table(row["__table__"]).update(update).eq("id", row["id"]).execute()
                    if hit:
                        enriched += 1
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error_message else ("ok" if enriched else ("all_dupes" if not_found else "zero_yield"))
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=enriched + not_found, parsed_count=enriched, staged_count=valued,
                            duplicate_count=not_found, error_message=error_message)
        self.logger.info(f"enriched={enriched} valued={valued} not_found={not_found}")
        return {"name": self.name, "status": status, "enriched": enriched, "valued": valued,
                "not_found": not_found, "error": error_message, "staged": valued,
                "duplicates": not_found, "fetched": enriched + not_found}

    def _candidates(self, client) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                rows = (client.table(table).select("id, property_address, owner_name_records")
                        .ilike("county", "montgomery%").is_("property_value", "null")
                        .not_.is_("property_address", "null").is_("raw_payload->montgomery_gis", "null")
                        .order("id").limit(1000).execute().data) or []
            except Exception as e:
                self.logger.warning(f"candidates query on {table} failed: {e}")
                continue
            for r in rows:
                r["__table__"] = table
                out.append(r)
        self.logger.info(f"{len(out)} Montgomery candidates lacking property_value")
        return out

    @staticmethod
    def _raw_payload(client, table: str, row_id: str) -> Optional[Dict[str, Any]]:
        rows = (client.table(table).select("raw_payload").eq("id", row_id).limit(1).execute().data) or []
        raw = rows[0].get("raw_payload") if rows else None
        if raw is None:
            return {}
        return dict(raw) if isinstance(raw, dict) else None

    def _lookup(self, address: str) -> Any:
        """Parcel record, None when no single parcel matches, or "error"."""
        parts = _street_keywords(address)
        if not parts:
            return None
        number, street = parts
        prefix = f"{number} {street}".replace("'", "''")
        res = self.fetch(MONTGOMERY_PARCELS, params={"where": f"PropertyAddress LIKE '{prefix}%'",
                                                     "outFields": FIELDS, "returnGeometry": "false", "f": "json"})
        if res is None or res.status_code != 200:
            return "error"
        try:
            data = res.json()
        except Exception:
            return "error"
        if data.get("error"):
            return "error"
        # Exact number + street word only; never guess between parcels.
        hits = [f.get("attributes") or {} for f in data.get("features") or []]
        hits = [a for a in hits if (a.get("PropertyAddress") or "").upper().split()[:2] == [str(number), street]]
        if len({a.get("ParcelID") for a in hits}) != 1:
            return None
        a = hits[0]
        sale_iso = None
        if a.get("SalesDate"):
            try:
                sale_iso = datetime.fromtimestamp(a["SalesDate"] / 1000.0, tz=timezone.utc).date().isoformat()
            except (TypeError, ValueError, OSError):
                pass
        owner = " & ".join(x.strip() for x in (a.get("Owner1"), a.get("Owner2")) if x and str(x).strip())
        return {
            "parcel_id": a.get("ParcelID"), "owner": owner or None,
            "property_address": a.get("PropertyAddress"), "property_city": a.get("PropertyCity"),
            "owner_mailing": a.get("MailingAddress"), "owner_city": a.get("MailCity"),
            "owner_state": a.get("MailState"), "owner_zip": a.get("MailZip"),
            "last_sale_date": sale_iso, "last_sale_price": _money(a.get("SalesPrice")),
            "last_sale_price_raw": a.get("SalesPrice"), "last_sale_ms": a.get("SalesDate"),
            "year_built": a.get("YearBuilt"), "living_area": a.get("LivingArea"),
            "acres": a.get("CalcAcreage"), "assessment_class": a.get("AssessmentClass"),
            "land_use": a.get("LandUseDesc"), "building_value": a.get("BldgMktValue"),
            "value_note": "county layer blanks appraisals; value is the last sale when recent",
        }


def run() -> dict:
    return MontgomeryAssessorBot().run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = MontgomeryAssessorBot()
        for addr in sys.argv[1:]:
            print(f"{addr}: {bot._lookup(addr)}")
    else:
        print(run())
