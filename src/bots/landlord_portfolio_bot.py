"""Landlord portfolios across the Middle TN counties with a free parcel layer.

A portfolio is an owner with 2 to 40 residential parcels. It is worth a
look when the owner has at least one distress signal we already stage at
one of those parcels, or when the owner is an aging passive landlord:

  signals (weight in tired_score)
    probate       6   the owner died; heirs inherit a rental portfolio
    foreclosure   5   lis pendens, trustee notice, pre-foreclosure, REO
    court_sale    5   partition or other court-ordered sale
    tax           4   delinquent tax suit or tax sale list
    bankruptcy    4
    code_housing  3   open Codes housing case (Davidson only), up to 5 counted
  passive: 5+ rentals, owner mails from outside Tennessee, held 15+ years

tired_score = signal weights + rentals (capped at 10) + years owned / 5
(capped at 4) + 2 for out-of-state mailing. A starting judgment, not a
model. expected_fee = residential value x 10% x 20%.

How: staged leads in the county are tied to an owner through the parcel
at their address (Davidson leads already carry Metro's owner), then every
parcel of each owner is pulled in batches. Passive landlords come from a
grouped statistics query (owner, count) on out-of-state mailing. Public
agencies, nonprofits and the national single-family rental funds are
skipped. Rows for a county are replaced on each run.

Counties (free public ArcGIS parcel layers, no token, verified 2026-11-05):
Davidson (Metro), Rutherford, Montgomery, Williamson (countywide layer on
Brentwood's server), Wilson, Maury (City of Columbia's account), Robertson
and Cheatham (hosted by Austin Peay State University). Sumner has no
countywide layer with owner and mailing; Dickson only a consultant's 2023
copy without mailing addresses; both are left out.
Williamson's layer has empty transfer dates, so it yields signal portfolios
only (passive needs 15+ years held).
Writes public.landlord_portfolios. Distress type: N/A (analysis).
"""
from __future__ import annotations

import re
import traceback as tb
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase

OWNER_BATCH = 40
ADDRESS_BATCH = 100
PAGE = 500
LEADS_PAGE = 300
MAX_QUERY_ROWS = 200_000  # guard against a server that ignores resultOffset
MAX_RESIDENTIAL = 40
MAX_RESIDENTIAL_VALUE = 15_000_000
PASSIVE_MIN = 5
PASSIVE_YEARS = 15
HOUSING_SUBTYPES = ("Codes Housing Request", "CA Codes Case", "Landlord Registration Violation")

SIGNAL_WEIGHTS = {"probate": 6, "foreclosure": 5, "court_sale": 5, "tax": 4, "bankruptcy": 4, "code_housing": 3}
SIGNAL_BY_TYPE = {
    "PROBATE": "probate", "PRE_FORECLOSURE": "foreclosure", "TRUSTEE_NOTICE": "foreclosure", "FORECLOSURE": "foreclosure",
    "REO": "foreclosure", "LIS_PENDENS": "foreclosure", "COURT_SALE": "court_sale", "TAX_LIEN": "tax", "BANKRUPTCY": "bankruptcy",
}

DAVIDSON_RESIDENTIAL = re.compile(r"SINGLE|DUPLEX|TRIPLEX|QUADPLEX|CONDO|ZERO LOT|MOBILE|RESIDENTIAL|APARTMENT|TOWN", re.I)
INSTITUTION_RE = re.compile(
    r"\b(HOUSING AUTHORITY|MDHA|METROPOLITAN GOV|METRO GOV|CITY OF|STATE OF|COUNTY|UNITED STATES|CHURCH|MINISTR|"
    r"UNIVERSITY|COLLEGE|SCHOOL|BANK|REIT|HABITAT FOR HUMANITY|COMMUNITY ORGANIZATION|FOUNDATION|NEIGHBORHOOD|"
    r"COMMUNITY DEVELOPMENT|CDC|NONPROFIT|NON-PROFIT|"
    # federal REO holders and condo / homeowner associations
    r"FEDERAL NATIONAL MORTGAGE|FEDERAL HOME LOAN|FANNIE MAE|FREDDIE MAC|SECRETARY OF HOUSING|VETERANS AFFAIRS|"
    r"O\.?I\.?C|OWNERS ASSOC|HOMEOWNERS ASSOC|"
    # national single-family rental funds and their borrower entities
    r"BORROWER|PROGRESS RESIDENTIAL|INVITATION HOMES|AMERICAN HOMES 4 RENT|AMH|TRICON|FIRSTKEY|VINEBROOK|"
    r"MAIN STREET RENEWAL|HOME PARTNERS|SFR|PRETIUM|CERBERUS|RESICAP|ROOFSTOCK|MYND|AH4R|FKH|"
    # home builders holding lot inventory, and infill spec developers
    # holding teardown lots (not rentals)
    r"HOMES OF|HOMES,? INC|HOMES,? LLC|BUILDERS|BUILDS|HOMEBUILDERS|CONSTRUCTION|COMMUNITIES|DEVELOPMENT|DEVELOPERS)\b", re.I)
# "L.P." and "L.L.C." with periods count too (HJL, L.P.).
COMPANY_RE = re.compile(
    r"\b(L\.?L\.?C|L L C|INC|CORP|L\.?L\.?P|L\.?P|LTD|TRUST|PARTNERS|HOLDINGS?|PROPERTIES|INVESTMENTS?|GROUP|VENTURES|REALTY|CAPITAL)\b", re.I)


def _money(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    s = re.sub(r"[^\d.]", "", str(v or ""))
    return float(s) if s else 0.0


def _epoch_date(ms: Any) -> Optional[date]:
    if not isinstance(ms, (int, float)):
        return None
    return (datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=ms)).date()


def _norm(owner: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (owner or "")).strip().upper()


def street_key(address: Optional[str]) -> Optional[str]:
    if not address:
        return None
    street = re.sub(r"\s+", " ", address.split(",")[0]).strip().upper()
    return street if re.match(r"^\d+[A-Z]?\s+\S", street) else None


def _sql_list(values: List[str]) -> str:
    return ",".join("'" + v.replace("'", "''") + "'" for v in values)


# ── County layers ────────────────────────────────────────────────────────
# Each maps the county's fields onto one parcel shape:
#   parcel, owner, address, city, land_use, residential, value, acres,
#   mail_lines, mail_city, mail_state, mail_zip, owned_since

def _davidson(a: Dict[str, Any]) -> Dict[str, Any]:
    return {"parcel": a.get("STANPAR"), "owner": _norm(a.get("Owner")), "address": a.get("PropAddr"), "city": a.get("PropCity"),
            "land_use": a.get("LUDesc"), "residential": bool(DAVIDSON_RESIDENTIAL.search(a.get("LUDesc") or "")),
            "value": _money(a.get("TotlAppr")), "acres": a.get("Acres"), "mail_lines": [a.get("OwnAddr1"), a.get("OwnAddr2")],
            "mail_city": a.get("OwnCity"), "mail_state": a.get("OwnState"), "mail_zip": a.get("OwnZip"),
            "owned_since": _epoch_date(a.get("OwnDate"))}


def _rutherford(a: Dict[str, Any]) -> Dict[str, Any]:
    return {"parcel": a.get("ParcelID"), "owner": _norm(a.get("Owner1")), "address": a.get("FormattedLocation"),
            "city": a.get("LocationCity"), "land_use": a.get("BuildingTypeDescription") or a.get("AccountType"),
            "residential": a.get("AccountType") in ("00 - Residential", "008 - Mult Unit"),
            "value": _money(a.get("TotalValue")), "acres": a.get("CALCACRES"), "mail_lines": [a.get("MailingAddress"), a.get("MailingAddress2")],
            "mail_city": a.get("MailingCity"), "mail_state": a.get("MailingState"), "mail_zip": a.get("MailingZipCode"),
            "owned_since": _epoch_date(a.get("SaleDate"))}


def _montgomery(a: Dict[str, Any]) -> Dict[str, Any]:
    return {"parcel": str(a.get("ParcelID")) if a.get("ParcelID") is not None else None, "owner": _norm(a.get("Owner1")),
            "address": a.get("PropertyAddress"), "city": a.get("PropertyCity"), "land_use": a.get("LandUseDesc"),
            "residential": a.get("AssessmentClass") == "RS", "value": _money(a.get("AppraisedValue")), "acres": a.get("CalcAcreage"),
            "mail_lines": [a.get("MailingAddress")], "mail_city": a.get("MailCity"), "mail_state": a.get("MailState"),
            "mail_zip": a.get("MailZip"), "owned_since": _epoch_date(a.get("SalesDate"))}


def _mdy(value: Any) -> Optional[date]:
    """'06/17/1997' -> date; epoch ms also accepted."""
    if isinstance(value, (int, float)):
        return _epoch_date(value)
    m = re.match(r"^\s*(\d{1,2})/(\d{1,2})/(\d{4})", str(value or ""))
    if not m:
        return None
    try:
        return date(int(m.group(3)), int(m.group(1)), int(m.group(2)))
    except ValueError:
        return None


def flip_to_number_first(address: Optional[str]) -> Optional[str]:
    """'LICK CREEK RD 6825' -> '6825 LICK CREEK RD' (Wilson, Maury, Robertson, Cheatham)."""
    a = re.sub(r"\s+", " ", address or "").strip().upper()
    m = re.match(r"^(.*\D)\s+(\d+[A-Z]?)$", a)
    return f"{m.group(2)} {m.group(1)}" if m else (a or None)


def flip_to_street_first(key: str) -> str:
    """'6825 LICK CREEK RD' -> 'LICK CREEK RD 6825' for querying those layers."""
    num, _, street = key.partition(" ")
    return f"{street} {num}"


def _assessor_std(a: Dict[str, Any], lower: bool = False) -> Dict[str, Any]:
    """Wilson / Maury (upper-case fields) and Robertson / Cheatham (lower-case
    fields) share the state's standard assessment schema."""
    g = (lambda k: a.get(k.lower())) if lower else (lambda k: a.get(k))
    cls = str((g("CLASS") if lower else g("PT")) or "")
    return {"parcel": g("PARCELID") or g("GISLINK"), "owner": _norm(g("OWNER")), "address": flip_to_number_first(g("ADDRESS")),
            "city": None, "land_use": g("LANDUSE") or cls,
            "residential": cls.strip().upper().startswith("00"), "value": _money(g("APPRAISAL")),
            "acres": g("CALC_ACRE") or g("CALCAC") or a.get("Acres"),
            "mail_lines": [g("MAILADDR")], "mail_city": g("MAILCITY"), "mail_state": g("STATE"), "mail_zip": g("ZIP"),
            "owned_since": _mdy(g("SALEDATE"))}


def _williamson(a: Dict[str, Any]) -> Dict[str, Any]:
    # PROP_TYPE is a numeric code; 102500 covers 91k of 107k parcels and is
    # treated as residential. Other codes are left out until a lookup exists.
    return {"parcel": a.get("PARCEL_ID") or a.get("GISLINK"), "owner": _norm(a.get("OWNER1")), "address": a.get("PROP_STREE"),
            "city": None, "land_use": str(a.get("PROP_TYPE") or ""), "residential": a.get("PROP_TYPE") == 102500,
            "value": _money(a.get("TOTAL_VALU")), "acres": a.get("Acreage"), "mail_lines": [a.get("OWN_STREET")],
            "mail_city": a.get("OWN_CITY"), "mail_state": a.get("OWN_STATE"), "mail_zip": a.get("OWN_ZIP"),
            "owned_since": _epoch_date(a.get("PXFER_DATE"))}


STD_FIELDS = "PARCELID,GISLINK,OWNER,ADDRESS,PT,LANDUSE,APPRAISAL,CALC_ACRE,MAILADDR,MAILCITY,STATE,ZIP,SALEDATE"
STD_FIELDS_LOWER = "parcelid,gislink,owner,address,class,landuse,appraisal,calcac,mailaddr,mailcity,state,zip,saledate"

COUNTIES: Dict[str, Dict[str, Any]] = {
    "davidson": {
        "url": "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query",
        "owner": "Owner", "address": "PropAddr", "state": "OwnState", "normalize": _davidson,
        "fields": "STANPAR,Owner,PropAddr,PropCity,LUDesc,TotlAppr,Acres,OwnAddr1,OwnAddr2,OwnCity,OwnState,OwnZip,OwnDate",
        "residential_where": "(" + " OR ".join(f"LUDesc LIKE '%{k}%'" for k in
                                               ("SINGLE", "DUPLEX", "TRIPLEX", "QUADPLEX", "CONDO", "ZERO LOT", "MOBILE", "RESIDENTIAL")) + ")",
    },
    "rutherford": {
        "url": "https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/Parcel_Data/FeatureServer/1/query",
        "owner": "Owner1", "address": "FormattedLocation", "state": "MailingState", "normalize": _rutherford,
        "fields": "ParcelID,Owner1,FormattedLocation,LocationCity,AccountType,BuildingTypeDescription,TotalValue,CALCACRES,"
                  "MailingAddress,MailingAddress2,MailingCity,MailingState,MailingZipCode,SaleDate",
        "residential_where": "AccountType IN ('00 - Residential','008 - Mult Unit')",
    },
    "montgomery": {
        "url": "https://gis.mcgtn.org/arcgis/rest/services/Parcels/MapServer/0/query",
        "owner": "Owner1", "address": "PropertyAddress", "state": "MailState", "normalize": _montgomery,
        "fields": "ParcelID,Owner1,PropertyAddress,PropertyCity,LandUseDesc,AssessmentClass,AppraisedValue,CalcAcreage,"
                  "MailingAddress,MailCity,MailState,MailZip,SalesDate",
        "residential_where": "AssessmentClass = 'RS'",
    },
    "williamson": {
        "url": "https://maps.brentwoodtn.gov/arcgis/rest/services/Datasets/County/MapServer/2/query",
        "owner": "OWNER1", "address": "PROP_STREE", "state": "OWN_STATE", "normalize": _williamson,
        "fields": "PARCEL_ID,GISLINK,OWNER1,PROP_STREE,PROP_TYPE,TOTAL_VALU,Acreage,OWN_STREET,OWN_CITY,OWN_STATE,OWN_ZIP,PXFER_DATE",
        "residential_where": "PROP_TYPE = 102500",
    },
    "wilson": {
        "url": "https://services2.arcgis.com/0lW6bjFI0RIjy5uB/arcgis/rest/services/Wilson_Parcels/FeatureServer/0/query",
        "owner": "OWNER", "address": "ADDRESS", "state": "STATE", "normalize": _assessor_std, "street_first": True,
        "fields": STD_FIELDS, "residential_where": "PT = '00 Residential'",
    },
    "maury": {
        "url": "https://services6.arcgis.com/ubRvNDgGwwJuZMcL/arcgis/rest/services/Parcels/FeatureServer/1/query",
        "owner": "OWNER", "address": "ADDRESS", "state": "STATE", "normalize": _assessor_std, "street_first": True,
        "fields": STD_FIELDS.replace("CALC_ACRE", "Acres"), "residential_where": "PT = '00 Residential'",
    },
    "robertson": {
        "url": "https://apnsgis4.apsu.edu/arcgis/rest/services/Robertson/RobertsonGIS/MapServer/3/query",
        "owner": "owner", "address": "address", "state": "state", "street_first": True,
        "normalize": lambda a: _assessor_std(a, lower=True), "fields": STD_FIELDS_LOWER,
        "residential_where": "class = '00 RESIDENTIAL'",
    },
    "cheatham": {
        "url": "https://apnsgis4.apsu.edu/arcgis/rest/services/Cheatham/Parcels/MapServer/0/query",
        "owner": "owner", "address": "address", "state": "state", "street_first": True,
        "normalize": lambda a: _assessor_std(a, lower=True), "fields": STD_FIELDS_LOWER,
        "residential_where": "class = '00 RESIDENTIAL'",
    },
}


def build_portfolio(county: str, owner: str, parcels: List[Dict[str, Any]], signals: Dict[str, int],
                    cases: List[str], today: Optional[date] = None) -> Optional[Dict[str, Any]]:
    """One landlord_portfolios row from normalized parcels, or None."""
    today = today or datetime.now(timezone.utc).date()
    if not owner or INSTITUTION_RE.search(owner):
        return None
    seen, rows = set(), []
    for p in parcels:
        if p.get("parcel") in seen:
            continue
        seen.add(p.get("parcel"))
        rows.append(p)
    residential = [p for p in rows if p.get("residential")]
    res_value = sum(p.get("value") or 0 for p in residential)
    if not 2 <= len(residential) <= MAX_RESIDENTIAL or not 0 < res_value <= MAX_RESIDENTIAL_VALUE:
        return None
    first = rows[0]
    mailing = ", ".join(" ".join(str(x).split()) for x in first.get("mail_lines") or [] if x and str(x).strip())
    city_line = " ".join(str(x).strip() for x in (first.get("mail_city"), first.get("mail_state"), first.get("mail_zip")) if x)
    state = str(first.get("mail_state") or "TN").strip().upper()
    out_of_area = bool(state) and state != "TN"
    owned = [p["owned_since"] for p in rows if p.get("owned_since")]
    owned_since = min(owned) if owned else None
    years = (today - owned_since).days / 365.25 if owned_since else 0
    signal_score = sum(SIGNAL_WEIGHTS[k] * (min(n, 5) if k == "code_housing" else n) for k, n in signals.items() if k in SIGNAL_WEIGHTS)
    signal_count = sum(signals.values())
    passive = False
    if not signal_count:
        if not (len(residential) >= PASSIVE_MIN and out_of_area and years >= PASSIVE_YEARS):
            return None
        passive = True
    score = round(signal_score + min(len(residential), 10) + min(years / 5, 4) + (2 if out_of_area else 0), 2)
    return {
        "owner_name": owner, "county": county,
        "parcel_count": len(rows), "residential_count": len(residential),
        "total_value": round(sum(p.get("value") or 0 for p in rows), 2), "residential_value": round(res_value, 2),
        "owner_mailing": ", ".join(x for x in (mailing, city_line) if x) or None, "out_of_area": out_of_area,
        "owned_since": owned_since.isoformat() if owned_since else None,
        "housing_cases": signals.get("code_housing", 0), "open_cases": signals.get("code_housing", 0),
        "case_numbers": sorted(set(cases))[:50],
        "parcels": [{"parcel": p.get("parcel"), "address": p.get("address"), "city": p.get("city"), "land_use": p.get("land_use"),
                     "value": p.get("value"), "acres": p.get("acres")}
                    for p in sorted(residential, key=lambda p: -(p.get("value") or 0))][:MAX_RESIDENTIAL],
        "is_company": bool(COMPANY_RE.search(owner)),
        "signals": {k: v for k, v in signals.items() if v}, "signal_count": signal_count, "passive": passive,
        "tired_score": score, "expected_fee": round(res_value * 0.10 * 0.20, 2),
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }


def lead_signal(row: Dict[str, Any]) -> Optional[str]:
    if row.get("bot_source") == "nashville_codes":
        return "code_housing" if row.get("sub") in HOUSING_SUBTYPES and (row.get("status") or "").upper() == "OPEN" else None
    return SIGNAL_BY_TYPE.get((row.get("distress_type") or "").upper())


class LandlordPortfolioBot(BotBase):
    name = "landlord_portfolio"
    description = "Landlord portfolios with distress signals across 8 Middle TN counties"
    throttle_seconds = 1.0
    expected_min_yield = 0

    def scrape(self) -> List[Any]:
        return []

    def _query(self, cfg: Dict[str, Any], data: Dict[str, str]) -> List[Dict[str, Any]]:
        out, offset = [], 0
        while True:
            res = self.fetch(cfg["url"], method="POST", data={**data, "returnGeometry": "false", "f": "json",
                                                             "resultOffset": str(offset), "resultRecordCount": "2000"})
            if res is None or res.status_code != 200:
                raise RuntimeError(f"parcel query failed ({getattr(res, 'status_code', None)})")
            body = res.json()
            if body.get("error"):
                raise RuntimeError(f"parcel query error: {body['error']}")
            feats = body.get("features") or []
            out.extend(f.get("attributes") or {} for f in feats)
            if not body.get("exceededTransferLimit") or not feats:
                return out
            offset += len(feats)
            if offset > MAX_QUERY_ROWS:
                raise RuntimeError(f"parcel query passed {MAX_QUERY_ROWS} rows; server may be ignoring resultOffset")

    def _owner_counts(self, cfg: Dict[str, Any], county: str) -> List[Dict[str, Any]]:
        """Out-of-state owners with their residential parcel counts, largest
        first, in one request. Grouped statistics cannot be paged: Montgomery
        ignores resultOffset (returns the first page forever) and Metro
        rejects it. Montgomery also rejects HAVING, so sort by count and cut
        in Python. No resultRecordCount either (Metro rejects it here); the
        server max of 2,000 rows reaches well below 5 parcels in every county."""
        res = self.fetch(cfg["url"], method="POST", data={
            "where": f"{cfg['residential_where']} AND {cfg['state']} IS NOT NULL AND {cfg['state']} <> 'TN'",
            "groupByFieldsForStatistics": cfg["owner"],
            "outStatistics": f'[{{"statisticType":"count","onStatisticField":"{cfg["owner"]}","outStatisticFieldName":"n"}}]',
            "orderByFields": "n DESC", "returnGeometry": "false", "f": "json"})
        if res is None or res.status_code != 200:
            raise RuntimeError(f"owner count query failed ({getattr(res, 'status_code', None)})")
        body = res.json()
        if body.get("error"):
            raise RuntimeError(f"owner count query error: {body['error']}")
        rows = [f.get("attributes") or {} for f in body.get("features") or []]
        if rows and (rows[-1].get("n") or 0) >= PASSIVE_MIN:
            self.logger.warning(f"{county}: owner counts truncated at {len(rows)} rows, passive list incomplete")
        return rows

    def _staged(self, client, county: str) -> List[Dict[str, Any]]:
        """Signal leads in one county via staged_county_leads (indexed on the
        normalized county, paged by id, only the fields used here)."""
        out: List[Dict[str, Any]] = []
        after = None
        while True:
            # 300 rows a call: extracting JSON fields unpacks each row's full
            # notice payload, about 4.5 ms a row, under the 8 s API timeout.
            page = client.rpc("staged_county_leads", {"p_county": county, "p_after": after, "p_limit": LEADS_PAGE}).execute().data or []
            for r in page:
                r["case"] = r.pop("case_nbr", None)
            out.extend(r for r in page if lead_signal(r))
            if len(page) < LEADS_PAGE:
                return out
            after = page[-1]["id"]

    def county_portfolios(self, client, county: str) -> List[Dict[str, Any]]:
        cfg = COUNTIES[county]
        norm: Callable = cfg["normalize"]
        leads = self._staged(client, county)
        # 1. owner for each signal lead
        owner_of: Dict[str, str] = {}
        need: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for r in leads:
            if r.get("metro_owner"):
                owner_of[r["id"]] = _norm(r["metro_owner"])
            elif street_key(r.get("property_address")):
                need[street_key(r["property_address"])].append(r)
        keys = list(need)
        flip = cfg.get("street_first", False)
        for i in range(0, len(keys), ADDRESS_BATCH):
            chunk = keys[i:i + ADDRESS_BATCH]
            layer_keys = [flip_to_street_first(k) for k in chunk] if flip else chunk
            found: Dict[str, set] = defaultdict(set)
            for a in self._query(cfg, {"where": f"UPPER({cfg['address']}) IN ({_sql_list(layer_keys)})", "outFields": f"{cfg['owner']},{cfg['address']}"}):
                addr = re.sub(r"\s+", " ", str(a.get(cfg["address"]) or "")).strip().upper()
                found[flip_to_number_first(addr) if flip else addr].add(_norm(a.get(cfg["owner"])))
            for key in chunk:
                owners = found.get(key) or set()
                if len(owners) == 1:
                    for r in need[key]:
                        owner_of[r["id"]] = next(iter(owners))
        signals: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        cases: Dict[str, List[str]] = defaultdict(list)
        counted = set()
        for r in leads:
            owner = owner_of.get(r["id"])
            if not owner:
                continue
            sig = lead_signal(r)
            ident = (owner, sig, street_key(r.get("property_address")) or r["id"], r.get("case") or "")
            if ident in counted:
                continue
            counted.add(ident)
            signals[owner][sig] += 1
            if r.get("case"):
                cases[owner].append(r["case"])
        # 2. passive out-of-state landlords
        stats = self._owner_counts(cfg, county)
        passive_owners = {_norm(a.get(cfg["owner"])) for a in stats
                          if a.get(cfg["owner"]) and PASSIVE_MIN <= (a.get("n") or 0) <= MAX_RESIDENTIAL}
        # 3. every parcel of each candidate owner
        owners = sorted(o for o in set(signals) | passive_owners if o and not INSTITUTION_RE.search(o))
        self.logger.info(f"{county}: {len(leads)} signal leads, {len(signals)} owners with signals, "
                         f"{len(passive_owners)} out-of-state 5+ owners, {len(owners)} to check")
        rows = []
        for i in range(0, len(owners), OWNER_BATCH):
            batch = owners[i:i + OWNER_BATCH]
            parcels: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for a in self._query(cfg, {"where": f"UPPER({cfg['owner']}) IN ({_sql_list(batch)})", "outFields": cfg["fields"]}):
                p = norm(a)
                parcels[p["owner"]].append(p)
            for owner in batch:
                row = build_portfolio(county, owner, parcels.get(owner, []), dict(signals.get(owner, {})), cases.get(owner, []))
                if row:
                    rows.append(row)
        return rows

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase"}
        per_county: Dict[str, int] = {}
        errors: List[str] = []
        for county in COUNTIES:
            try:
                rows = self.county_portfolios(client, county)
                client.table("landlord_portfolios").delete().eq("county", county).execute()
                for i in range(0, len(rows), 200):
                    client.table("landlord_portfolios").insert(rows[i:i + 200]).execute()
                per_county[county] = len(rows)
            except Exception as e:
                errors.append(f"{county}: {type(e).__name__}: {e}\n{tb.format_exc()}")
                self.logger.error(f"{county} FAILED: {e}")
        total = sum(per_county.values())
        error = "\n".join(errors) or None
        status = "failed" if errors and not total else "ok" if total else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=total, parsed_count=total, staged_count=total, duplicate_count=0, error_message=error)
        self.logger.info(f"portfolios {per_county}")
        return {"name": self.name, "status": status, "portfolios": per_county, "staged": total, "error": error}


def run() -> dict:
    return LandlordPortfolioBot().run()


if __name__ == "__main__":
    print(run())
