"""Free Davidson parcel evidence for a worksite printed in a WARN notice."""
import re
from urllib.parse import urlencode

from ._land_gis import DAVIDSON


def worksite_parcel(worksite, fetch):
    # Preserve street order and unit identifiers. A tenant's suite must not
    # silently resolve to ownership of an entire building.
    street, _, place = (worksite or "").upper().partition(",")
    if (not re.search(r"\b(?:TN|TENNESSEE)\s+\d{5}(?:-\d{4})?\s*$", place)
            or re.search(r"\b(?:SUITE|STE|UNIT|APT|FLOOR)\b|#", place)
            or not re.fullmatch(r"\d+[A-Z]?\s+[\w .'\-]+", street)):
        return None
    words = street.split()
    suffix = {"STREET": "ST", "ROAD": "RD", "DRIVE": "DR", "AVENUE": "AVE",
              "BOULEVARD": "BLVD", "LANE": "LN", "COURT": "CT", "PARKWAY": "PKWY"}
    words[-1] = suffix.get(words[-1], words[-1])
    target = " ".join(words)
    sql_value = target.replace("'", "''")
    params = {"where": f"UPPER(PropAddr) = '{sql_value}'", "outFields": "APN,PropAddr,Owner,LUDesc,TotlAppr",
              "returnGeometry": "false", "f": "json", "resultRecordCount": 2}
    response = fetch(DAVIDSON, params=params)
    if response is None or response.status_code != 200:
        return None
    try:
        page = response.json()
    except ValueError:
        return None
    if not isinstance(page, dict) or page.get("error") or page.get("exceededTransferLimit"):
        return None
    features = page.get("features")
    if not isinstance(features, list) or len(features) != 1:
        return None
    record = features[0].get("attributes") or {}
    if (str(record.get("PropAddr") or "").strip().upper() != target
            or not record.get("APN") or not record.get("Owner")):
        return None
    return {"county": "davidson", "parcel_id": record["APN"], "property_address": record["PropAddr"],
            "recorded_owner": record["Owner"], "land_use": record.get("LUDesc"),
            "county_appraisal": record.get("TotlAppr"), "source_url": DAVIDSON + "?" + urlencode(params),
            "match_basis": "exact_worksite_address"}
