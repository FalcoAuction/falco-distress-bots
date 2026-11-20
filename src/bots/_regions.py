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


# Census FIPS for all 95 Tennessee counties (state 47; odd numbers in
# alphabetical order, Anderson 001 to Wilson 189).
TN_FIPS = {
    "anderson": "47001", "bedford": "47003", "benton": "47005", "bledsoe": "47007", "blount": "47009",
    "bradley": "47011", "campbell": "47013", "cannon": "47015", "carroll": "47017", "carter": "47019",
    "cheatham": "47021", "chester": "47023", "claiborne": "47025", "clay": "47027", "cocke": "47029",
    "coffee": "47031", "crockett": "47033", "cumberland": "47035", "davidson": "47037", "decatur": "47039",
    "dekalb": "47041", "dickson": "47043", "dyer": "47045", "fayette": "47047", "fentress": "47049",
    "franklin": "47051", "gibson": "47053", "giles": "47055", "grainger": "47057", "greene": "47059",
    "grundy": "47061", "hamblen": "47063", "hamilton": "47065", "hancock": "47067", "hardeman": "47069",
    "hardin": "47071", "hawkins": "47073", "haywood": "47075", "henderson": "47077", "henry": "47079",
    "hickman": "47081", "houston": "47083", "humphreys": "47085", "jackson": "47087", "jefferson": "47089",
    "johnson": "47091", "knox": "47093", "lake": "47095", "lauderdale": "47097", "lawrence": "47099",
    "lewis": "47101", "lincoln": "47103", "loudon": "47105", "mcminn": "47107", "mcnairy": "47109",
    "macon": "47111", "madison": "47113", "marion": "47115", "marshall": "47117", "maury": "47119",
    "meigs": "47121", "monroe": "47123", "montgomery": "47125", "moore": "47127", "morgan": "47129",
    "obion": "47131", "overton": "47133", "perry": "47135", "pickett": "47137", "polk": "47139",
    "putnam": "47141", "rhea": "47143", "roane": "47145", "robertson": "47147", "rutherford": "47149",
    "scott": "47151", "sequatchie": "47153", "sevier": "47155", "shelby": "47157", "smith": "47159",
    "stewart": "47161", "sullivan": "47163", "sumner": "47165", "tipton": "47167", "trousdale": "47169",
    "unicoi": "47171", "union": "47173", "van buren": "47175", "warren": "47177", "washington": "47179",
    "wayne": "47181", "weakley": "47183", "white": "47185", "williamson": "47187", "wilson": "47189",
}
TN_COUNTIES = frozenset(TN_FIPS) | frozenset({"vanburen", "de kalb"})
