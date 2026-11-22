"""Land lane rows for counties outside the Comptroller roll, from their
public parcel map services (free, no key, 2,000 records per request):

  rutherford  Parcel_Data FeatureServer layer 1 (services5.arcgis.com)
  davidson    Metro Planning Cadastral/Parcels MapServer layer 0

Each row comes back in the Comptroller index shape (owner, owner2, calcac,
appraisal, class, landuse, saledate, state, address, city, parcelid, county,
mailaddr, mailcity, zip, impval, bldgs, price, zoning, imp) so the same
_land.signals rules apply. Williamson (Inigo) and Montgomery have no bulk
parcel service and stay out of this lane.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional

RUTHERFORD = "https://services5.arcgis.com/A5C0MR9xfkxVRwat/arcgis/rest/services/Parcel_Data/FeatureServer/1/query"
DAVIDSON = "https://maps.nashville.gov/arcgis/rest/services/Cadastral/Parcels/MapServer/0/query"
PAGE = 2000
_LAND_WORDS = ("FARM", "RURAL", "VACANT", "SINGLE FAMILY", "MOBILE", "RESIDENTIAL", "DUPLEX", "AGRIC", "FOREST",
               "ZERO LOT", "RESIDENCE")


def _ymd(ms: Any) -> Optional[str]:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime("%Y%m%d") if ms else None
    except (TypeError, ValueError, OSError):
        return None


def rutherford_row(a: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "county": "RUTHERFORD", "parcelid": a.get("ParcelID") or a.get("GISLINK"),
        "owner": a.get("Owner1") or "", "owner2": " ".join(x for x in (a.get("Owner2"), a.get("Owner3")) if x),
        "calcac": a.get("CALCACRES") or a.get("DEEDACRES") or 0, "appraisal": a.get("TotalValue") or 0,
        "class": a.get("AccountType") or "", "landuse": a.get("BuildingTypeDescription") or "",
        "saledate": _ymd(a.get("SaleDate")), "state": a.get("MailingState") or "",
        "address": a.get("FormattedLocation") or a.get("STREETADDRESS") or "",
        "city": a.get("LocationCity") or a.get("CITY") or "",
        "mailaddr": " ".join(x for x in (a.get("MailingAddress"), a.get("MailingAddress2")) if x),
        "mailcity": a.get("MailingCity") or "", "zip": a.get("MailingZipCode") or "",
        "impval": a.get("TotalBuildingValue"), "bldgs": None, "price": a.get("SalePrice"),
        "zoning": a.get("ZONING"), "imp": a.get("BuildingTypeDescription"),
    }


def davidson_row(a: Dict[str, Any]) -> Dict[str, Any]:
    lu = str(a.get("LUDesc") or "").upper()
    return {
        "county": "DAVIDSON", "parcelid": a.get("APN") or str(a.get("ParID") or ""),
        "owner": a.get("Owner") or "", "owner2": "",
        "calcac": a.get("Acres") or a.get("DeededAcreage") or 0, "appraisal": a.get("TotlAppr") or 0,
        # Metro has land use, not the state class: farm, vacant rural and
        # residential uses count as land; stores, offices and industrial do not.
        "class": "AGRICULTURAL" if any(w in lu for w in _LAND_WORDS) else lu,
        "landuse": lu, "saledate": _ymd(a.get("OwnDate")), "state": a.get("OwnState") or "",
        "address": a.get("PropAddr") or "", "city": a.get("PropCity") or "",
        "mailaddr": " ".join(x for x in (a.get("OwnAddr1"), a.get("OwnAddr2")) if x),
        "mailcity": a.get("OwnCity") or "", "zip": a.get("OwnZip") or "",
        "impval": a.get("ImprAppr"), "bldgs": None, "price": a.get("SalePrice"), "zoning": a.get("Zoning"), "imp": lu,
    }


SOURCES = {
    "rutherford": (RUTHERFORD, "CALCACRES >= 20 AND TotalValue >= 150000", rutherford_row),
    "davidson": (DAVIDSON, "Acres >= 20 AND TotlAppr >= 150000", davidson_row),
}


def county_rows(county: str, get: Callable[..., Any]) -> Iterator[Dict[str, Any]]:
    """Every 20+ acre, $150k+ parcel in the county, in index shape. `get`
    is the caller's fetch (url, params=...) returning a response or None."""
    url, where, shape = SOURCES[county]
    offset = 0
    last_id = None
    while True:
        res = get(url, params={"where": where, "outFields": "*", "returnGeometry": "false", "f": "json",
                               "resultOffset": offset, "resultRecordCount": PAGE, "orderByFields": "OBJECTID"})
        if res is None or res.status_code != 200:
            raise ValueError(f"{county} parcel service failed at offset {offset}")
        page = res.json()
        if page.get("error") or not isinstance(page.get("features"), list):
            raise ValueError(f"{county} parcel service returned an invalid page at offset {offset}")
        feats = page["features"]
        for f in feats:
            attrs = f["attributes"]
            oid = attrs.get("OBJECTID")
            if oid is None or (last_id is not None and oid <= last_id):
                raise ValueError(f"{county} parcel page failed to advance at offset {offset}")
            last_id = oid
            yield shape(attrs)
        if not page.get("exceededTransferLimit") and len(feats) < PAGE:
            return
        if not feats:
            raise ValueError(f"{county} parcel service returned an empty intermediate page")
        offset += len(feats)
