"""Research-only identity for church closures, using free official address and parcel layers."""

import re
from datetime import datetime, timezone

import requests

BASE = "https://services1.arcgis.com/YuVBSS7Y1of2Qud1/arcgis/rest/services/"
POINTS = BASE + "Tennessee_NG911_Address_Points/FeatureServer/0/query"
PARCELS = BASE + "Tennessee_Property_Boundaries_Public_Use/FeatureServer/0/query"
SHELBY = "https://services6.arcgis.com/FStub7HlCSCdqoXK/arcgis/rest/services/OpenDataAddress/FeatureServer/0/query"
MEMPHIS = "https://311.memphistn.gov/server/rest/services/311/ParcelCentroids/MapServer/1/query"
ALIASES = dict(ROAD="RD", STREET="ST", LANE="LN", DRIVE="DR", HIGHWAY="HWY", ROUTE="RT",
               NORTH="N", SOUTH="S", EAST="E", WEST="W")


def words(value):
    return " ".join(ALIASES.get(w, w) for w in re.findall(r"[A-Z0-9]+", (value or "").upper()))


def query(url, **params):
    response = requests.get(url, params={"f": "json", "returnGeometry": "false",
                            "resultRecordCount": 501, **params}, timeout=(3, 6))
    response.raise_for_status()
    data = response.json()
    if not isinstance(data.get("features"), list) or data.get("exceededTransferLimit"):
        raise ValueError("incomplete ArcGIS result")
    return [f["attributes"] for f in data["features"]], response.url


def match(closure, lookup=query):
    evidence = {"status": "held", "checked_on": datetime.now(timezone.utc).date().isoformat(),
                "sources": []}

    def read(url, **params):
        records, link = lookup(url, **params)
        evidence["sources"].append(link)
        return records

    def hold(reason):
        return {**evidence, "reason": reason}

    county = (closure.get("county") or "").upper()
    address = words(closure.get("address"))
    number, _, remainder = address.partition(" ")
    if (closure.get("state") != "TN" or not re.fullmatch(r"[A-Z ]{2,30}", county)
            or not re.fullmatch(r"[1-9][0-9]{0,6}", number) or not remainder):
        return hold("incomplete_resolution_address")
    try:
        points = read(POINTS, where=f"County='{county}' AND Add_Number={number}",
                      outFields="OBJECTID,County,Add_Number,AddNum_Pre,AddNum_Suf,Unit,StNam_Full,Post_City,Zip_Code,Longitude,Latitude")
        candidates = []
        for point in points:
            street = words(point["StNam_Full"])
            city = words(point["Post_City"])
            site = re.sub(r"(?: TN| TENNESSEE)?(?: [0-9]{5})?$", "", remainder)
            if city and site.endswith(" " + city):
                site = site[:-len(city)-1]
            missing_suffix = street.rpartition(" ")[0] == site and street.rpartition(" ")[2] in {
                "RD", "LN", "DR", "ST", "HWY", "WAY", "CT", "CIR", "AVE", "BLVD"}
            if (str(point["Add_Number"]) == number and point["County"].upper() == county
                    and street and (site == street or missing_suffix)):
                candidates.append((point, site, street))
        if len(candidates) != 1:
            return hold("ambiguous_address_points" if candidates else "no_exact_address_point")
        p, site, street = candidates[0]
        zipcode = re.search(r" ([0-9]{5})$", address)
        if zipcode and zipcode[1] != str(p.get("Zip_Code") or "").strip():
            return hold("zip_conflict")
        if any((p.get(f) or "").strip() for f in ("AddNum_Pre", "AddNum_Suf", "Unit")):
            return hold("address_subunit")
        lon, lat = float(p["Longitude"]), float(p["Latitude"])
        if not (-91 < lon < -81 and 34 < lat < 37):
            return hold("invalid_point")
        evidence["address_point_id"] = p["OBJECTID"]
        geometry = dict(geometry=f"{lon},{lat}", inSR=4326, geometryType="esriGeometryPoint",
                        spatialRel="esriSpatialRelIntersects")
        if county == "SHELBY":
            exact = f"{number} {street}"
            county_points = read(SHELBY, where=f"ADDRESS='{exact}'", outFields="ADDRESS,CITY,STATUS,STRUCDESC,GISLINK")
            parcels = read(MEMPHIS, **geometry, outFields="PARCELID")
            if len(parcels) != 1 or len(county_points) != 1:
                return hold("county_record_count")
            a = county_points[0]
            parcel = parcels[0]["PARCELID"]
            if not (site == street and parcel and a["GISLINK"] == parcel and a["STATUS"] == 730
                    and a["ADDRESS"] == exact and words(a["CITY"]) == words(p["Post_City"])
                    and a["STRUCDESC"] == "PLACE OF WORSHIP"):
                return hold("county_worship_site_unconfirmed")
            return {**evidence, "status": "parcel_identified", "parcel_id": parcel,
                    "county": county, "basis": "county_worship_address_and_parcel"}
        parcels = read(PARCELS, **geometry, outFields="COUNTY_NAME,PARCELID,OWNER,OWNER2,ADDRESS")
        if len(parcels) != 1:
            return hold("parcel_count")
        parcel = parcels[0]
        if not parcel["PARCELID"] or words(parcel["COUNTY_NAME"]) != words(county):
            return hold("parcel_identity")
        owner = words((parcel["OWNER"] or "") + " " + (parcel["OWNER2"] or ""))
        name = words(closure.get("church")).removesuffix(" UNITED METHODIST CHURCH")
        # A missing road suffix needs agreement on the assessor street and the
        # congregation's distinctive name as well as a unique address point.
        if site != street and name.startswith("NEW ") and name[4:] == site:
            name = name[4:]
        if site != street and (name != site or street != site + " RD"):
            return hold("street_suffix_unconfirmed")
        if (not name or name in {"CHURCH", "METHODIST", "FIRST", "UNITED"}
                or f" {name} " not in f" {owner} " or not {"METHODIST", "CHURCH"} & set(owner.split())
                or "CEMETERY" in owner):
            return hold("county_owner_unconfirmed")
        if words(parcel["ADDRESS"]) not in {street, f"{street} {number}", f"{number} {street}"}:
            return hold("assessor_street_conflict")
        return {**evidence, "status": "parcel_identified", "county": county,
                "parcel_id": parcel["PARCELID"], "assessment_owner": " ".join(v.strip() for v in (parcel["OWNER"], parcel["OWNER2"]) if v and v.strip()),
                "basis": "church_named_assessor_parcel"}
    except (requests.RequestException, ValueError, TypeError, KeyError, AttributeError):
        return hold("source_incomplete")

