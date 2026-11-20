"""Land lane: Tennessee farm and timber tracts with a reason to sell (statewide).

Reads the Comptroller parcel index (data/comptroller/index.sqlite, refreshed
weekly by comptroller_refresh, free and cleared for commercial use) and stages
every Tennessee tract of 20+ acres whose owner of record is an estate
or heirs, a long-held family group (ETAL), or an out-of-state owner holding
25+ years. Rules in _land.py.

Distress types:
  HEIRS_PROPERTY  heirs or family_etal
  ABSENTEE_LAND   absentee only

The Comptroller roll covers 86 of the 95 counties; Rutherford and
Davidson come from their county parcel map services (_land_gis). Williamson,
Montgomery, Hamilton, Knox, Shelby, Chester and Hickman are not in this lane yet.

    python -m src.bots.land_lane_bot
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ._base import BotBase, LeadPayload
from ._comptroller import _record, county_key
from ._land import lane_record, person_name, signals, street_address
from ._land_gis import SOURCES as GIS_COUNTIES, county_rows
from ._regions import TN_COUNTIES

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "comptroller" / "index.sqlite"
MAX_LEADS = int(os.environ.get("FALCO_LAND_MAX_LEADS", "4000") or 4000)


def tract_leads(con: Optional[sqlite3.Connection], bot: "LandLaneBot", today: date,
                extra_rows: Iterable[Mapping[str, Any]] = ()) -> List[LeadPayload]:
    rows: List[Any] = []
    if con is not None:
        con.row_factory = sqlite3.Row
        counties = sorted({county_key(c) for c in TN_COUNTIES})   # the index keys counties as "VANBUREN"
        marks = ",".join("?" * len(counties))
        rows = con.execute(f"SELECT * FROM parcels WHERE county IN ({marks}) AND calcac >= 20 AND appraisal >= 150000",
                           counties).fetchall()
    out: List[LeadPayload] = []
    for row in list(rows) + list(extra_rows):
        sigs = signals(row, today)
        if not sigs:
            continue
        rec = _record(row)
        rec.update({"method": "land_lane", "county": row["county"].lower()})
        street = street_address(row["address"])
        place = row["city"] or (row["county"].title() + " County")
        address = ", ".join(x for x in (street, place, "TN") if x)
        owner = rec["owner"]
        out.append(LeadPayload(
            bot_source=bot.name,
            pipeline_lead_key=bot.make_lead_key(bot.name, f"{row['county'].lower()}|{row['parcelid']}"),
            property_address=address if street else None,
            county=row["county"].lower(),
            full_name=person_name(row["owner"]),
            owner_name_records=owner,
            property_value=float(row["appraisal"] or 0) or None,
            distress_type="HEIRS_PROPERTY" if ("heirs" in sigs or "family_etal" in sigs) else "ABSENTEE_LAND",
            admin_notes=f"{rec['acres']:.0f} acres, {', '.join(sigs)}; owner {owner}",
            raw_payload={"source": "comptroller_roll" if isinstance(row, sqlite3.Row) else "county_gis", "state": "TN", "property_lane": "land",
                         "land": lane_record(row, sigs, row["county"].lower()), "comptroller_parcel": rec},
        ))
    out.sort(key=lambda l: (0 if l.distress_type == "HEIRS_PROPERTY" else 1, -(l.property_value or 0)))
    return out[:MAX_LEADS]


class LandLaneBot(BotBase):
    name = "land_lane"
    description = "Middle TN farm and timber tracts (20+ acres) owned by heirs, family groups or long-held absentees"
    throttle_seconds = 0
    expected_min_yield = 0

    def scrape(self) -> List[LeadPayload]:
        gis: List[Dict[str, Any]] = []
        for county in GIS_COUNTIES:
            try:
                rows = list(county_rows(county, lambda url, **kw: self.fetch(url, timeout=120, **kw)))
                self.logger.info(f"{county}: {len(rows)} large parcels from the county map service")
                gis += rows
            except Exception as e:  # one county's service down must not cost the rest
                self.logger.warning(f"{county}: parcel service failed: {e}")
        con = sqlite3.connect(DB_PATH) if DB_PATH.exists() else None
        if con is None:
            self.logger.warning("no Comptroller index; comptroller_refresh has not run (county GIS only)")
        try:
            leads = tract_leads(con, self, date.today(), gis)
        finally:
            if con is not None:
                con.close()
        counts: Dict[str, int] = {}
        for l in leads:
            counts[l.distress_type] = counts.get(l.distress_type, 0) + 1
        self.logger.info(f"staging {len(leads)} tract(s): {counts}")
        return leads


def run() -> dict:
    return LandLaneBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
