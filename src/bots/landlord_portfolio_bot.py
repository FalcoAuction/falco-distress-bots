"""Tired-landlord portfolios (Davidson).

Starts from open Codes housing cases (habitability, CA cases, landlord
registration violations) in staging, takes each case's owner of record,
and pulls every parcel that owner holds from Metro Nashville's public
parcel layer. An owner with several residential parcels and repeated
housing cases is a portfolio auction candidate: one sale, several houses.

Writes public.landlord_portfolios (falco-site migration
20261109_landlord_portfolios.sql), one row per owner with at least two
residential parcels, upserted on owner + county. Public agencies and
institutions (housing authority, Metro, churches, universities, banks)
are skipped; small LLCs are kept, since many individual landlords hold
rentals in one.

Read-only against Metro; staging reads select only a few JSON fields.
Distress type: N/A (analysis).
"""
from __future__ import annotations

import re
import traceback as tb
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase

LAYER = "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query"
FIELDS = "Owner,PropAddr,PropCity,LUDesc,TotlAppr,Acres,OwnAddr1,OwnAddr2,OwnCity,OwnState,OwnZip,OwnDate,STANPAR"
HOUSING_SUBTYPES = ("Codes Housing Request", "CA Codes Case", "Landlord Registration Violation")
OWNER_BATCH = 40
PAGE = 500
MAX_RESIDENTIAL = 40          # bigger than this is an institution, not a tired landlord
MAX_RESIDENTIAL_VALUE = 15_000_000

RESIDENTIAL_RE = re.compile(r"SINGLE|DUPLEX|TRIPLEX|QUADPLEX|CONDO|ZERO LOT|MOBILE|RESIDENTIAL|APARTMENT|TOWN", re.I)
INSTITUTION_RE = re.compile(
    r"\b(HOUSING AUTHORITY|MDHA|METROPOLITAN GOV|METRO GOV|CITY OF|STATE OF|COUNTY|UNITED STATES|CHURCH|MINISTR|"
    r"UNIVERSITY|COLLEGE|SCHOOL|BANK|REIT|HABITAT FOR HUMANITY|COMMUNITY ORGANIZATION|FOUNDATION|NEIGHBORHOOD|"
    r"COMMUNITY DEVELOPMENT|CDC|NONPROFIT|NON-PROFIT)\b", re.I)
# "L.P." and "L.L.C." with periods count too (HJL, L.P.).
COMPANY_RE = re.compile(
    r"\b(L\.?L\.?C|L L C|INC|CORP|L\.?L\.?P|L\.?P|LTD|TRUST|PARTNERS|HOLDINGS?|PROPERTIES|INVESTMENTS?|GROUP|VENTURES|REALTY|CAPITAL)\b", re.I)


def _norm(owner: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (owner or "")).strip().upper()


def _sql_list(values: List[str]) -> str:
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


def _epoch_date(ms: Any) -> Optional[date]:
    if not isinstance(ms, (int, float)):
        return None
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)).date()


def build_portfolio(owner: str, parcels: List[Dict[str, Any]], cases: List[Dict[str, Any]],
                    today: Optional[date] = None) -> Optional[Dict[str, Any]]:
    """One landlord_portfolios row, or None when the owner is not a portfolio
    landlord (fewer than 2 or more than MAX_RESIDENTIAL residential parcels,
    an institution, or above the value cap)."""
    today = today or datetime.now(timezone.utc).date()
    if INSTITUTION_RE.search(owner):
        return None
    seen, rows = set(), []
    for p in parcels:
        if p.get("STANPAR") in seen:
            continue
        seen.add(p.get("STANPAR"))
        rows.append(p)
    residential = [p for p in rows if RESIDENTIAL_RE.search(p.get("LUDesc") or "")]
    res_value = sum(p.get("TotlAppr") or 0 for p in residential)
    if not 2 <= len(residential) <= MAX_RESIDENTIAL or res_value > MAX_RESIDENTIAL_VALUE or res_value <= 0:
        return None
    first = rows[0]
    mailing = ", ".join(x for x in [" ".join(str(first.get(k) or "").split()) for k in ("OwnAddr1", "OwnAddr2")] if x)
    city_line = " ".join(x for x in (str(first.get("OwnCity") or "").strip(), str(first.get("OwnState") or "").strip(),
                                     str(first.get("OwnZip") or "").strip()) if x)
    own_dates = [d for d in (_epoch_date(p.get("OwnDate")) for p in rows) if d]
    owned_since = min(own_dates) if own_dates else None
    out_of_area = (str(first.get("OwnState") or "TN").strip().upper() != "TN")
    years = (today - owned_since).days / 365.25 if owned_since else 0
    open_cases = sum(1 for c in cases if (c.get("status") or "").upper() == "OPEN")
    score = round(open_cases * 3 + min(len(residential), 10) + min(years / 5, 4) + (2 if out_of_area else 0), 2)
    return {
        "owner_name": owner,
        "county": "davidson",
        "parcel_count": len(rows),
        "residential_count": len(residential),
        "total_value": round(sum(p.get("TotlAppr") or 0 for p in rows), 2),
        "residential_value": round(res_value, 2),
        "owner_mailing": ", ".join(x for x in (mailing, city_line) if x) or None,
        "out_of_area": out_of_area,
        "owned_since": owned_since.isoformat() if owned_since else None,
        "housing_cases": len(cases),
        "open_cases": open_cases,
        "case_numbers": sorted({c["case"] for c in cases if c.get("case")})[:50],
        "parcels": [{"parcel": p.get("STANPAR"), "address": p.get("PropAddr"), "city": p.get("PropCity"),
                     "land_use": p.get("LUDesc"), "value": p.get("TotlAppr"), "acres": p.get("Acres")}
                    for p in sorted(residential, key=lambda p: -(p.get("TotlAppr") or 0))][:MAX_RESIDENTIAL],
        "is_company": bool(COMPANY_RE.search(owner)),
        "tired_score": score,
        "expected_fee": round(res_value * 0.10 * 0.20, 2),
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }


class LandlordPortfolioBot(BotBase):
    name = "landlord_portfolio"
    description = "Davidson landlords with housing code cases and several rental parcels"
    throttle_seconds = 1.0
    expected_min_yield = 0

    def scrape(self) -> List[Any]:
        return []

    def _cases(self, client) -> Dict[str, List[Dict[str, Any]]]:
        by_owner: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            page = (client.table("homeowner_requests_staging")
                    .select("id,metro_owner:raw_payload->metro_parcel->>owner,codes_owner:raw_payload->nashville_codes->>Property_Owner,"
                            "case:raw_payload->nashville_codes->>Request_Nbr,sub:raw_payload->nashville_codes->>Subtype_Description,"
                            "status:raw_payload->nashville_codes->>Status")
                    .eq("bot_source", "nashville_codes").neq("staging_status", "rejected")
                    .gt("id", after).order("id").limit(PAGE).execute().data) or []
            if not page:
                break
            after = page[-1]["id"]
            for r in page:
                owner = _norm(r.get("metro_owner") or r.get("codes_owner"))
                if owner and r.get("sub") in HOUSING_SUBTYPES:
                    by_owner[owner].append(r)
            if len(page) < PAGE:
                break
        return by_owner

    def _parcels(self, owners: List[str]) -> Dict[str, List[Dict[str, Any]]]:
        out: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        offset = 0
        while True:
            res = self.fetch(LAYER, method="POST", data={
                "where": f"Owner IN ({_sql_list(owners)})", "outFields": FIELDS, "returnGeometry": "false",
                "resultOffset": str(offset), "resultRecordCount": "2000", "orderByFields": "OBJECTID", "f": "json"})
            if res is None or res.status_code != 200:
                raise RuntimeError(f"Metro parcel query failed ({getattr(res, 'status_code', None)})")
            body = res.json()
            if body.get("error"):
                raise RuntimeError(f"Metro parcel query error: {body['error']}")
            feats = body.get("features") or []
            for f in feats:
                a = f.get("attributes") or {}
                out[_norm(a.get("Owner"))].append(a)
            if not body.get("exceededTransferLimit") or not feats:
                break
            offset += len(feats)
        return out

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase"}
        written = owners_checked = 0
        error = None
        try:
            cases = self._cases(client)
            owners = sorted(cases)
            self.logger.info(f"{sum(len(v) for v in cases.values())} housing cases, {len(owners)} owners")
            rows: List[Dict[str, Any]] = []
            for i in range(0, len(owners), OWNER_BATCH):
                batch = owners[i:i + OWNER_BATCH]
                parcels = self._parcels(batch)
                owners_checked += len(batch)
                for owner in batch:
                    row = build_portfolio(owner, parcels.get(owner, []), cases[owner])
                    if row:
                        rows.append(row)
            for i in range(0, len(rows), 200):
                client.table("landlord_portfolios").upsert(rows[i:i + 200], on_conflict="owner_name,county").execute()
            written = len(rows)
        except Exception as e:
            error = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error else "ok" if written else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=owners_checked, parsed_count=owners_checked, staged_count=written,
                            duplicate_count=0, error_message=error)
        self.logger.info(f"owners_checked={owners_checked} portfolios={written}")
        return {"name": self.name, "status": status, "owners_checked": owners_checked, "portfolios": written,
                "staged": written, "error": error}


def run() -> dict:
    return LandlordPortfolioBot().run()


if __name__ == "__main__":
    print(run())
