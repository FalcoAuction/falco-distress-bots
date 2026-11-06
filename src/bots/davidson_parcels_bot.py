"""Davidson County values from Metro Nashville's public parcel layer.

Metro Planning publishes every Davidson parcel on a public ArcGIS service
(maps.nashville.gov, Cadastral/Parcels layer 0) with owner, owner mailing
address, property address, land use, acres, total appraisal and last sale.
It accepts batch queries (PropAddr IN (...), up to 2,000 records), so the
whole unvalued Davidson backlog (about 5,000 code, demolition and notice
leads) fills in a few dozen requests. davidson_assessor scrapes the PADCTN
portal one lead at a time, capped at 150 a run, sale dates first, so code
violations never reached the front of its queue.

Match: exact property address (street part before the first comma, upper
case, whitespace collapsed). An address that maps to more than one parcel
with different owners (condo complexes, split lots) is left unvalued.
Writes go through apply_staging_enrichment: the value only when the row has
none, raw_payload.metro_parcel with the parcel facts, and
phone_metadata.metro_parcel_checked so misses are retried after 30 days.

No robots.txt on the host; requests carry the identified bot user agent.
Verified 2026-11-05. Distress type: N/A (enricher).
"""
from __future__ import annotations

import re
import traceback as tb
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase

LAYER = "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query"
FIELDS = "PropAddr,PropCity,PropZip,Owner,OwnAddr1,OwnAddr2,OwnCity,OwnState,OwnZip,TotlAppr,LandAppr,ImprAppr,LUDesc,Acres,SalePrice,OwnDate,STANPAR"
BATCH = 100        # addresses per ArcGIS query
WRITE_BATCH = 200  # rows per apply_staging_enrichment call
PAGE = 1000        # candidate rows per page
RETRY_DAYS = 30


def street_key(address: Optional[str]) -> Optional[str]:
    """'813 Chadwell Dr, Madison, TN 37115' -> '813 CHADWELL DR'."""
    if not address:
        return None
    street = re.sub(r"\s+", " ", address.split(",")[0]).strip().upper()
    if not re.match(r"^\d+[A-Z]?\s+\S", street):
        return None  # no house number: not an address we can match exactly
    return street


def _sql_list(values: List[str]) -> str:
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


def pick(features: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """One parcel per address, or None when several parcels disagree on owner."""
    rows = [f.get("attributes") or {} for f in features]
    if not rows:
        return None
    owners = {(r.get("Owner") or "").strip().upper() for r in rows}
    if len(owners) > 1:
        return None
    return max(rows, key=lambda r: r.get("TotlAppr") or 0)


def parcel_facts(r: Dict[str, Any]) -> Dict[str, Any]:
    own_date = r.get("OwnDate")
    mailing = ", ".join(x for x in [
        " ".join(str(r.get(k) or "").split()) for k in ("OwnAddr1", "OwnAddr2")] if x)
    city = " ".join(x for x in [str(r.get("OwnCity") or "").strip(), str(r.get("OwnState") or "").strip(),
                                str(r.get("OwnZip") or "").strip()] if x)
    return {
        "parcel": r.get("STANPAR"),
        "owner": r.get("Owner"),
        "owner_mailing": ", ".join(x for x in (mailing, city) if x) or None,
        "total_appraisal": r.get("TotlAppr"),
        "land_appraisal": r.get("LandAppr"),
        "improvement_appraisal": r.get("ImprAppr"),
        "land_use": r.get("LUDesc"),
        "acres": r.get("Acres"),
        "last_sale_price": r.get("SalePrice"),
        # Epoch ms, sometimes before 1970; fromtimestamp rejects negatives on Windows.
        "owned_since": (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=own_date)).date().isoformat()
        if isinstance(own_date, (int, float)) else None,
        "source": "metro_nashville_parcels",
    }


class DavidsonParcelsBot(BotBase):
    name = "davidson_parcels"
    description = "Davidson values and owner mailing addresses from Metro's public parcel layer"
    throttle_seconds = 1.0
    expected_min_yield = 0

    def scrape(self) -> List[Any]:
        return []

    def _candidates(self, client) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=RETRY_DAYS)).isoformat()
        out, after = [], "00000000-0000-0000-0000-000000000000"
        while True:
            page = (client.table("homeowner_requests_staging").select("id,property_address,phone_metadata->metro_parcel_checked")
                    .gt("id", after).ilike("county", "davidson%").is_("property_value", "null")
                    .not_.is_("property_address", "null").neq("staging_status", "rejected")
                    .order("id").limit(PAGE).execute().data) or []
            if not page:
                break
            after = page[-1]["id"]
            out.extend(r for r in page if not r.get("metro_parcel_checked") or r["metro_parcel_checked"] < cutoff)
            if len(page) < PAGE:
                break
        return out

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase"}
        valued = missed = skipped = 0
        error = None
        try:
            rows = self._candidates(client)
            keyed: Dict[str, List[str]] = {}
            for r in rows:
                key = street_key(r.get("property_address"))
                if key:
                    keyed.setdefault(key, []).append(r["id"])
                else:
                    skipped += 1
            self.logger.info(f"{len(rows)} unvalued Davidson leads, {len(keyed)} matchable addresses")
            now = started.isoformat()
            updates: List[Dict[str, Any]] = []
            keys = list(keyed)
            for i in range(0, len(keys), BATCH):
                chunk = keys[i:i + BATCH]
                res = self.fetch(LAYER, method="POST", data={
                    "where": f"PropAddr IN ({_sql_list(chunk)})", "outFields": FIELDS,
                    "returnGeometry": "false", "f": "json"})
                if res is None or res.status_code != 200:
                    self.logger.warning(f"batch {i // BATCH}: fetch failed")
                    continue
                body = res.json()
                if body.get("error"):
                    self.logger.warning(f"batch {i // BATCH}: {body['error']}")
                    continue
                by_addr: Dict[str, List[Dict[str, Any]]] = {}
                for f in body.get("features") or []:
                    addr = re.sub(r"\s+", " ", ((f.get("attributes") or {}).get("PropAddr") or "")).strip().upper()
                    by_addr.setdefault(addr, []).append(f)
                for key in chunk:
                    hit = pick(by_addr.get(key, []))
                    for row_id in keyed[key]:
                        entry: Dict[str, Any] = {"id": row_id, "meta_patch": {"metro_parcel_checked": now}}
                        if hit and (hit.get("TotlAppr") or 0) > 0:
                            entry.update({"property_value": hit["TotlAppr"], "property_value_source": "metro_nashville_parcels",
                                          "raw_patch": {"metro_parcel": parcel_facts(hit)}})
                            valued += 1
                        else:
                            missed += 1
                        updates.append(entry)
            for i in range(0, len(updates), WRITE_BATCH):
                client.rpc("apply_staging_enrichment", {"p_rows": updates[i:i + WRITE_BATCH]}).execute()
        except Exception as e:
            error = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error else "ok" if valued else "all_dupes" if missed or skipped else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=valued + missed, parsed_count=valued + missed + skipped,
                            staged_count=valued, duplicate_count=missed, error_message=error)
        self.logger.info(f"valued={valued} missed={missed} skipped={skipped}")
        return {"name": self.name, "status": status, "valued": valued, "missed": missed,
                "skipped": skipped, "staged": valued, "error": error}


def run() -> dict:
    return DavidsonParcelsBot().run()


if __name__ == "__main__":
    print(run())
