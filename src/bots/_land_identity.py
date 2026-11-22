"""Provider-scoped Tennessee parcel identity; fixed-width roll controls are significant."""
import re

_ROLL = re.compile(r"([0-9]{3} [0-9]{3}[ A-Z] [ A-Z] [0-9]{5}[ A-Z][0-9]{3}) (20[0-9]{2})")


def parcel_control(source, county, parcel_id):
    if source not in ("comptroller_roll", "county_gis") or not isinstance(parcel_id, str):
        return None
    county = county.strip().lower() if isinstance(county, str) else ""
    county = {"vanburen": "van buren", "de kalb": "dekalb"}.get(county, county)
    if not county or not parcel_id.strip():
        return None
    match = _ROLL.fullmatch(parcel_id) if source == "comptroller_roll" else None
    return source, county, match[1] if match else parcel_id
