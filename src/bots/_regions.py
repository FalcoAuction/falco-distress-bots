"""Shared county sets. Middle Tennessee is the pilot region: all 41
counties of the Middle grand division (T.C.A. 4-1-202)."""

from __future__ import annotations

MIDDLE_TN_COUNTIES = frozenset({
    "bedford", "cannon", "cheatham", "clay", "coffee", "davidson", "dekalb", "dickson", "fentress",
    "franklin", "giles", "grundy", "hickman", "houston", "humphreys", "jackson", "lawrence", "lewis",
    "lincoln", "macon", "marshall", "maury", "montgomery", "moore", "overton", "perry", "pickett",
    "putnam", "robertson", "rutherford", "sequatchie", "smith", "stewart", "sumner", "trousdale",
    "van buren", "warren", "wayne", "white", "williamson", "wilson",
    # spellings some sources use
    "vanburen", "de kalb",
})


MIDDLE_TN_FIPS = {
    # Census FIPS for the 41 Middle TN counties (state 47).
    "bedford": "47003", "cannon": "47015", "cheatham": "47021", "clay": "47027", "coffee": "47031",
    "davidson": "47037", "dekalb": "47041", "dickson": "47043", "fentress": "47049", "franklin": "47051",
    "giles": "47055", "grundy": "47061", "hickman": "47081", "houston": "47083", "humphreys": "47085",
    "jackson": "47087", "lawrence": "47099", "lewis": "47101", "lincoln": "47103", "macon": "47111",
    "marshall": "47117", "maury": "47119", "montgomery": "47125", "moore": "47127", "overton": "47133",
    "perry": "47135", "pickett": "47137", "putnam": "47141", "robertson": "47147", "rutherford": "47149",
    "sequatchie": "47153", "smith": "47159", "stewart": "47161", "sumner": "47165", "trousdale": "47169",
    "van buren": "47175", "warren": "47177", "wayne": "47181", "white": "47185", "williamson": "47187",
    "wilson": "47189",
}
