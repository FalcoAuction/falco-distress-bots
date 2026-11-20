"""Land lane: Middle Tennessee farm and timber tracts with a reason to sell.

Reads the Comptroller parcel index (data/comptroller/index.sqlite, refreshed
weekly by comptroller_refresh, free and cleared for commercial use) and stages
every Middle Tennessee tract of 20+ acres whose owner of record is an estate
or heirs, a long-held family group (ETAL), or an out-of-state owner holding
25+ years. Rules in _land.py.

Distress types:
  HEIRS_PROPERTY  heirs or family_etal
  ABSENTEE_LAND   absentee only

The Comptroller roll covers 37 of the 41 Middle TN counties; Davidson,
Williamson, Rutherford and Montgomery run their own assessors and are not in
this lane yet.

    python -m src.bots.land_lane_bot
"""

from __future__ import annotations

import os
import sqlite3
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload
from ._comptroller import _record, county_key
from ._land import lane_record, person_name, signals, street_address
from ._regions import MIDDLE_TN_COUNTIES

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "comptroller" / "index.sqlite"
MAX_LEADS = int(os.environ.get("FALCO_LAND_MAX_LEADS", "4000") or 4000)


def tract_leads(con: sqlite3.Connection, bot: "LandLaneBot", today: date) -> List[LeadPayload]:
    con.row_factory = sqlite3.Row
    counties = sorted({county_key(c) for c in MIDDLE_TN_COUNTIES})   # the index keys counties as "VANBUREN"
    marks = ",".join("?" * len(counties))
    rows = con.execute(
        f"SELECT * FROM parcels WHERE county IN ({marks}) AND calcac >= 20 AND appraisal >= 150000", counties).fetchall()
    out: List[LeadPayload] = []
    for row in rows:
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
            raw_payload={"source": "comptroller_roll", "state": "TN", "property_lane": "land",
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
        if not DB_PATH.exists():
            self.logger.warning("no Comptroller index; comptroller_refresh has not run")
            return []
        con = sqlite3.connect(DB_PATH)
        try:
            leads = tract_leads(con, self, date.today())
        finally:
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
