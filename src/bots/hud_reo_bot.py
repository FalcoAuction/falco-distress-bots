"""Observe Tennessee HUD HomeStore inventory already marketed through HUD.

The public search contains structured listings. A buyer bid opening is not a
trustee sale or an invitation to provide auction services. Keep observations
in the run receipt so no homeowner or partner lead is staged.
"""

import json
import re
from datetime import datetime, timezone

from bs4 import BeautifulSoup

from ._base import BotBase, BotTimeout


TN_SEARCH = "https://www.hudhomestore.gov/searchresult?citystate=TN"
DETAIL = "https://www.hudhomestore.gov/propertydetails?caseNumber="


class HudReoBot(BotBase):
    name = "hud_reo"
    description = "TN HUD HomeStore inventory under agency marketing"

    def inventory(self) -> list[dict]:
        response = self.fetch(TN_SEARCH)
        if response is None or response.status_code != 200:
            raise ValueError("HUD HomeStore search unavailable")
        page = BeautifulSoup(response.content, "html.parser")
        if not page.select_one('input#cityStateZip[value="TN"]'):
            raise ValueError("HUD search did not confirm Tennessee")
        data = page.select_one("input#available_prop[value]")
        if data is None:
            raise ValueError("HUD listing data missing")
        listings = json.loads(data["value"])
        if not isinstance(listings, list):
            raise ValueError("Invalid HUD listing collection")

        cases = {}
        for row in listings:
            if not isinstance(row, dict):
                raise ValueError("Invalid HUD listing")
            case = row.get("propertyCaseNumber")
            if not isinstance(case, str) or not re.fullmatch(r"[0-9]{3}-[0-9]{6}", case):
                raise ValueError("Invalid HUD case number")
            if row.get("propertyState") != "TN" or not all(
                isinstance(row.get(field), str) and row[field].strip()
                for field in ("propertyAddress", "propertyCity")
            ):
                raise ValueError(f"Invalid Tennessee property identity for {case}")
            if case in cases:
                raise ValueError(f"Duplicate HUD case {case}")
            cases[case] = {
                "case": case, "street": row["propertyAddress"], "city": row["propertyCity"],
                "county": row.get("propertyCounty"), "list_price": row.get("listPrice"),
                "listing_period": row.get("listingPeriod"), "listed": row.get("listDate"),
                "bid_open_date": row.get("bidOpenDate"), "source_url": DETAIL + case,
            }
        return list(cases.values())

    def run(self) -> dict:
        started = datetime.now(timezone.utc)
        self._report_health("running", started, None, 0, 0, 0, 0)
        cases = []
        status = "ok"
        error = None
        try:
            cases = self.inventory()
        except BotTimeout as exc:
            status, error = "timed_out", str(exc)
        except Exception as exc:
            status, error = "failed", str(exc)
        notes = None
        if status == "ok":
            notes = {"source_url": TN_SEARCH, "listings": cases, "disposition": "agency_marketed"}
        self._report_health(status, started, datetime.now(timezone.utc), len(cases),
                            len(cases), 0, 0, error_message=error, notes=notes)
        return {"name": self.name, "run_id": self.run_id, "status": status,
                "fetched": len(cases), "staged": 0, "duplicates": 0, "error": error}


def run() -> dict:
    return HudReoBot().run()


if __name__ == "__main__":
    print(run())
