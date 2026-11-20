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
