"""Stalled new construction in Davidson County: a new home or townhome whose
builder was replaced mid-project ("change of contractor"), still owned by the
person or company that started it.

A spec builder who lost a contractor halfway through is carrying a loan on an
unfinished house; selling it as-is at auction is a clean way out. Both feeds
are Metro Nashville open data, free and public:

  Building Permits Issued (Metro ArcGIS)   change-of-contractor permits on
                                           new construction, $150k+ scope
  Cadastral Parcels (maps.nashville.gov)   owner of record, owner mailing,
                                           appraisal, last sale date

A property that sold after the permit was issued has already changed hands
and is skipped, as are government and housing-authority owners.

Distress type: STALLED_BUILD (owners are usually builder LLCs, so the
ranking's company-owner rule does not apply to this type).

    python -m src.bots.stalled_build_bot
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload

PERMITS = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Building_Permits_Issued_2/FeatureServer/0/query"
PARCELS = "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query"
WHERE = ("Permit_Type_Description like '%Change Contractor%' AND (Purpose like '%CONSTRUCT%' OR Purpose like "
         "'%NEW SINGLE%' OR Purpose like '%NEW %RESIDENCE%') AND Const_Cost >= 150000 AND "
         "(Permit_Subtype_Description like '%Single Family%' OR Permit_Subtype_Description like '%Townhome%' OR "
         "Permit_Subtype_Description like '%Two Family%' OR Permit_Subtype_Description like '%Duplex%')")
PUBLIC_OWNER_RE = re.compile(r"\b(MDHA|METRO(?:POLITAN)?\s+GOV|METROPOLITAN|CITY OF|STATE OF|HOUSING AUTHORITY|BOARD OF EDUCATION)\b", re.I)
BATCH = 100


def _iso(ms: Any) -> str:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).date().isoformat()
    except (TypeError, ValueError, OSError):
        return ""


def stalled(permit: Dict[str, Any], parcel: Dict[str, Any]) -> bool:
    owner = str(parcel.get("Owner") or "")
    if not owner or PUBLIC_OWNER_RE.search(owner):
        return False
    issued, owned = permit.get("Date_Issued"), parcel.get("OwnDate")
    # Sold after the contractor change: someone else already took it over.
    return not (issued and owned and int(owned) > int(issued))


class StalledBuildBot(BotBase):
    name = "stalled_build"
    description = "Davidson new construction with a mid-project contractor change, still held by the original owner"
    throttle_seconds = 0.5
    expected_min_yield = 0

    def scrape(self) -> List[LeadPayload]:
        res = self.fetch(PERMITS, params={"where": WHERE, "outFields": "*", "returnGeometry": "false",
                                          "resultRecordCount": 2000, "orderByFields": "Date_Issued DESC", "f": "json"},
                         timeout=120)
        permits = [f["attributes"] for f in ((res.json().get("features") or []) if res is not None and res.status_code == 200 else [])]
        latest: Dict[str, Dict[str, Any]] = {}
        for p in permits:
            if p.get("Parcel") and p["Parcel"] not in latest:
                latest[p["Parcel"]] = p
        parcels: Dict[str, Dict[str, Any]] = {}
        apns = sorted(latest)
        for i in range(0, len(apns), BATCH):
            ids = ",".join(f"'{a}'" for a in apns[i:i + BATCH])
            r = self.fetch(PARCELS, params={"where": f"APN IN ({ids})", "returnGeometry": "false", "f": "json",
                                            "outFields": "APN,Owner,OwnDate,OwnAddr1,OwnAddr2,OwnCity,OwnState,OwnZip,"
                                                         "PropAddr,PropCity,TotlAppr,LandAppr,Acres,LUDesc,SalePrice"},
                           timeout=120)
            for f in ((r.json().get("features") or []) if r is not None and r.status_code == 200 else []):
                a = f.get("attributes") or {}
                parcels.setdefault(a.get("APN"), a)
        leads: List[LeadPayload] = []
        for apn, p in latest.items():
            parcel = parcels.get(apn)
            if not parcel or not stalled(p, parcel):
                continue
            mailing = ", ".join(x for x in (parcel.get("OwnAddr1"), parcel.get("OwnAddr2"),
                                            " ".join(y for y in (parcel.get("OwnCity"), parcel.get("OwnState"),
                                                                 parcel.get("OwnZip")) if y)) if x)
            addr = ", ".join(x for x in (parcel.get("PropAddr") or p.get("Address"),
                                         parcel.get("PropCity") or p.get("City"), "TN") if x)
            value = max(float(parcel.get("TotlAppr") or 0), float(p.get("Const_Cost") or 0))
            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, f"{apn}|{p.get('Permit__')}"),
                property_address=addr,
                county="davidson",
                owner_name_records=parcel.get("Owner"),
                full_name=parcel.get("Owner"),
                property_value=value or None,
                distress_type="STALLED_BUILD",
                admin_notes=(f"Contractor changed {_iso(p.get('Date_Issued'))} on a ${int(p.get('Const_Cost') or 0):,} "
                             f"new build; owner {parcel.get('Owner')} since {_iso(parcel.get('OwnDate')) or 'n/a'}. "
                             f"Value is the larger of appraisal and permitted scope."),
                source_url="https://www.nashville.gov/departments/codes/construction-and-permits",
                raw_payload={
                    "source": "metro_permits", "state": "TN", "property_lane": "residential",
                    "permit": {k: p.get(k) for k in ("Permit__", "Permit_Subtype_Description", "Const_Cost", "Contact",
                                                     "Purpose")} | {"issued": _iso(p.get("Date_Issued"))},
                    "metro_parcel": {"apn": apn, "owner": parcel.get("Owner"), "owner_mailing": mailing or None,
                                     "appraisal": parcel.get("TotlAppr"), "land_use": parcel.get("LUDesc"),
                                     "owned_since": _iso(parcel.get("OwnDate"))},
                },
            ))
        self.logger.info(f"{len(permits)} contractor-change permits, {len(latest)} parcels, {len(leads)} still held")
        return leads


def run() -> dict:
    return StalledBuildBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
