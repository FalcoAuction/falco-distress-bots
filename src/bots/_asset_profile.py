"""What a matched owner holds: parcel count, acreage, value, land use, and
a tier that says what kind of auction it would be.

Computed from data the enrichers already store (owner_match parcels, or a
single padctn_owner_match hit); no network. Probate plus land or several
parcels is the strongest estate-auction signal: heirs splitting land rarely
want to hold it, and farms, tracts and multi-parcel estates are what
auction firms sell best.

    profile = asset_profile(raw_payload)   # None when nothing is matched
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

LAND_ACRES = 5.0
_LAND_USE = re.compile(r"FARM|AGRIC|VACANT|ACREAGE|FOREST|GREENBELT|TIMBER|RURAL|PASTURE", re.I)
_COMMERCIAL_USE = re.compile(r"COMMERCIAL|INDUSTRIAL|OFFICE|RETAIL|WAREHOUSE|STORE|SHOP", re.I)


def _num(v: Any) -> Optional[float]:
    try:
        n = float(str(v).replace(",", "").replace("$", "")) if v not in (None, "") else None
    except ValueError:
        return None
    return n if n is not None and n >= 0 else None


def _parcels(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    om = raw.get("owner_match") or {}
    if om.get("status") == "matched" and om.get("parcels"):
        return [{"parcel": p.get("parcel"), "acres": _num(p.get("acres")), "value": _num(p.get("value")),
                 "land_use": p.get("class")} for p in om["parcels"]]
    pm = raw.get("padctn_owner_match")
    if isinstance(pm, dict) and pm.get("parcel"):
        return [{"parcel": pm.get("parcel"), "acres": _num(pm.get("acres")), "value": _num(pm.get("appraised")),
                 "land_use": pm.get("land_use")}]
    return []


def asset_profile(raw_payload: Any) -> Optional[Dict[str, Any]]:
    raw = raw_payload if isinstance(raw_payload, dict) else {}
    parcels = _parcels(raw)
    if not parcels:
        return None
    acres = [p["acres"] for p in parcels if p["acres"] is not None]
    values = [p["value"] for p in parcels if p["value"] is not None]
    uses = sorted({str(p["land_use"]).strip() for p in parcels if p.get("land_use")})
    total_acres = round(sum(acres), 3) if acres else None
    land = (total_acres is not None and total_acres >= LAND_ACRES) or any(_LAND_USE.search(u) for u in uses)
    commercial = any(_COMMERCIAL_USE.search(u) for u in uses)
    multi = len(parcels) >= 2
    tier = "land" if land else "commercial" if commercial else "multi_parcel" if multi else "single_residence"
    return {
        "parcel_count": len(parcels),
        "total_acres": total_acres,
        "total_value": round(sum(values), 2) if values else None,
        "land_uses": uses,
        "is_land": land,
        "is_commercial": commercial,
        "is_multi_parcel": multi,
        "tier": tier,
        "version": "asset-profile-2026-11-05",
    }
