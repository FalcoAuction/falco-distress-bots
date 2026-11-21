"""Davidson contractor-change watchlist, corroborated with free county records.

A contractor change does not prove construction stopped, financial distress,
or an outstanding construction loan. Keep these as research prospects unless
an independent, open structural enforcement case supports prioritization.
The county appraisal is a screening value; a construction budget is not value.

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
from .nashville_codes_bot import NASHVILLE_CODES_ENDPOINT

PERMITS = "https://services2.arcgis.com/HdTo6HJqh92wn4D8/arcgis/rest/services/Building_Permits_Issued_2/FeatureServer/0/query"
PARCELS = "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query"
WHERE = ("Permit_Type_Description like '%Change Contractor%' AND (Purpose like '%CONSTRUCT%' OR Purpose like "
         "'%NEW SINGLE%' OR Purpose like '%NEW %RESIDENCE%') AND Const_Cost >= 150000 AND "
         "(Permit_Subtype_Description like '%Single Family%' OR Permit_Subtype_Description like '%Townhome%' OR "
         "Permit_Subtype_Description like '%Two Family%' OR Permit_Subtype_Description like '%Duplex%')")
PUBLIC_OWNER_RE = re.compile(r"\b(MDHA|METRO(?:POLITAN)?\s+GOV|METROPOLITAN|CITY OF|STATE OF|HOUSING AUTHORITY|BOARD OF EDUCATION)\b", re.I)
BATCH = 100
STRUCTURAL = re.compile(r"UNFIT FOR HABITATION|DEMOLITION ORDER|STRUCTURAL|OPEN VACANT BUILDING|VACANT/SUBSTANDARD|FAILURE TO SECURE", re.I)


def review(permit: Dict[str, Any], parcel: Dict[str, Any], cases: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Match official findings, never a complainant's allegation or old case."""
    normalize = lambda s: re.sub(r"[^A-Z0-9]", "", str(s or "").upper())
    evidence = []
    for case in cases:
        if (case.get("Property_APN") != parcel.get("APN") or case.get("Status") != "OPEN"
                or not case.get("Request_Nbr") or not parcel.get("Owner")
                or normalize(case.get("Property_Owner")) != normalize(parcel.get("Owner"))
                or not STRUCTURAL.search(str(case.get("Violations_Noted") or ""))):
            continue
        if not case.get("Date_Received") or not parcel.get("OwnDate"):
            continue
        if int(case["Date_Received"]) < int(parcel["OwnDate"]):
            continue
        evidence.append({"case": case["Request_Nbr"], "finding": case["Violations_Noted"],
                         "status": "OPEN", "source_url": NASHVILLE_CODES_ENDPOINT})
    return {"status": "corroborated" if evidence and stalled(permit, parcel) else "watchlist",
            "parcel_id": parcel.get("APN"), "owner": parcel.get("Owner"),
            "permit": permit.get("Permit__"), "evidence": evidence,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "reason": "Open structural enforcement supports review; sale willingness is unknown."
            if evidence else "Contractor change only; no matching open structural finding verified.",
            "value_basis": "County appraisal; construction budget retained separately."}


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
    description = "Davidson contractor-change prospects checked against county appraisal and open structural cases"
    throttle_seconds = 0.5
    expected_min_yield = 0

    def _write_staging(self, leads: List[LeadPayload]) -> tuple[int, int]:
        result = super()._write_staging(leads)
        # Base dedupe only touches last_seen. Refresh this lane's county review
        # as well, otherwise a resolved case could remain corroborated forever.
        from ._base import _supabase
        from .free_county_completion_bot import apply
        client = _supabase()
        if client is None:
            return result
        keyed = {lead.pipeline_lead_key: lead for lead in leads}
        keys = list(keyed)
        for offset in range(0, len(keys), BATCH):
            rows = (client.table("homeowner_requests_staging")
                    .select("id,pipeline_lead_key,raw_payload,property_value,property_value_source")
                    .eq("bot_source", self.name).eq("distress_type", "STALLED_BUILD")
                    .in_("pipeline_lead_key", keys[offset:offset + BATCH]).execute().data) or []
            for row in rows:
                lead = keyed[row["pipeline_lead_key"]]
                raw = dict(row.get("raw_payload") or {})
                raw["stalled_review"] = lead.raw_payload["stalled_review"]
                raw["metro_parcel"] = {**(raw.get("metro_parcel") or {}), **lead.raw_payload["metro_parcel"]}
                patch = {"raw_payload": raw}
                if not row.get("property_value") or row.get("property_value_source") in ("metro_nashville_parcels", "permit_construction_cost_proxy"):
                    patch.update(property_value=lead.property_value, property_value_source="metro_nashville_parcels")
                apply(client, "homeowner_requests_staging", row, patch)
        return result

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
        cases: List[Dict[str, Any]] = []
        apns = sorted(latest)
        for i in range(0, len(apns), BATCH):
            ids = ",".join(f"'{a}'" for a in apns[i:i + BATCH])
            r = self.fetch(PARCELS, params={"where": f"APN IN ({ids})", "returnGeometry": "false", "f": "json",
                                            "outFields": "APN,Owner,OwnDate,OwnAddr1,OwnAddr2,OwnCity,OwnState,OwnZip,"
                                                         "PropAddr,PropCity,TotlAppr,LandAppr,ImprAppr,Acres,LUDesc,SalePrice"},
                           timeout=120)
            for f in ((r.json().get("features") or []) if r is not None and r.status_code == 200 else []):
                a = f.get("attributes") or {}
                parcels.setdefault(a.get("APN"), a)
            offset = 0
            while True:
                # POST keeps the 100-parcel filter below proxy URL limits.
                response = self.fetch(NASHVILLE_CODES_ENDPOINT, method="POST", data={
                    "where": f"Property_APN IN ({ids}) AND Status='OPEN'", "outFields": "*",
                    "returnGeometry": "false", "f": "json", "resultOffset": offset,
                    "resultRecordCount": 1000, "orderByFields": "ObjectId"}, timeout=120)
                if response is None or response.status_code != 200:
                    raise RuntimeError("Cannot verify current construction enforcement cases")
                body = response.json()
                if body.get("error"):
                    raise RuntimeError("County enforcement query failed")
                page = [f["attributes"] for f in body.get("features", [])]
                cases.extend(page)
                if not body.get("exceededTransferLimit"):
                    break
                if not page:
                    raise RuntimeError("County enforcement page was truncated")
                offset += len(page)
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
            value = float(parcel.get("TotlAppr") or 0)
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
                             f"Contractor change alone does not establish distress. Value is county appraisal; "
                             f"construction budget is not a property valuation."),
                source_url="https://www.nashville.gov/departments/codes/construction-and-permits",
                raw_payload={
                    "source": "metro_permits", "state": "TN", "property_lane": "residential",
                    "stalled_review": review(p, parcel, cases),
                    "permit": {k: p.get(k) for k in ("Permit__", "Permit_Subtype_Description", "Const_Cost", "Contact",
                                                     "Purpose")} | {"issued": _iso(p.get("Date_Issued"))},
                    "metro_parcel": {"apn": apn, "owner": parcel.get("Owner"), "owner_mailing": mailing or None,
                                     "appraisal": parcel.get("TotlAppr"), "total_appraisal": parcel.get("TotlAppr"),
                                     "land_appraisal": parcel.get("LandAppr"), "improvement_appraisal": parcel.get("ImprAppr"),
                                     "acres": parcel.get("Acres"), "land_use": parcel.get("LUDesc"),
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
