"""
Chattanooga code enforcement scraper.

Source: City of Chattanooga open-data CSV published through ArcGIS Online.
Public download, no auth, no WAF. The current CSV is generated from the
CityView code-enforcement system and includes all violations, so this bot
filters to active high-signal structural/dangerous cases.

Distress type: CODE_VIOLATION
"""

from __future__ import annotations

import csv
import io
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional

from ._base import BotBase, LeadPayload


CHATTANOOGA_CODES_CSV_URL = (
    "https://www.arcgis.com/sharing/rest/content/items/"
    "19f35e4e09c041718905088a1fc7a6bb/data"
)
CHATTANOOGA_CODES_SOURCE_URL = (
    "https://www.arcgis.com/home/item.html?id=19f35e4e09c041718905088a1fc7a6bb"
)

ACTIVE_STATUSES = {"INPR", "O"}

# Keep the real distress and skip the noise: grass, loose litter, and
# inoperable vehicles are common, but structural/dangerous cases are money.
HIGH_SIGNAL_TERMS = {
    "ACCESSORY STRUCTURES",
    "BOARDING",
    "CERTIFICATE OF OCCUPANCY",
    "CONDEMNED",
    "DANGEROUS",
    "DEMOLITION",
    "ELECTRICAL",
    "EXTERIOR OF STRUCTURE",
    "EXTERIOR WALLS",
    "FOUNDATION",
    "INTERIOR",
    "PLUMBING",
    "REPAIR OR DEMOLITION",
    "ROOFS",
    "SANITARY",
    "SMOKE ALARMS",
    "STAIRWAYS",
    "UNFIT",
    "UNSAFE",
    "WINDOWS, DOORS",
}


class ChattanoogaCodesBot(BotBase):
    name = "chattanooga_codes"
    description = "Chattanooga Code Enforcement - active high-signal violations (Hamilton Co.)"
    throttle_seconds = 1.0
    expected_min_yield = 50

    def scrape(self) -> List[LeadPayload]:
        res = self.fetch(CHATTANOOGA_CODES_CSV_URL, timeout=90)
        if res is None or res.status_code != 200:
            self.logger.error(f"CSV fetch failed: {res.status_code if res else 'none'}")
            return []

        text = res.content.decode(res.encoding or "utf-8-sig", errors="replace")
        reader = csv.DictReader(io.StringIO(text))

        groups: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        active_rows = 0
        for row in reader:
            status = self._clean(row.get("status")).upper()
            if status not in ACTIVE_STATUSES:
                continue

            case_number = self._clean(row.get("case_number"))
            address = self._build_address(row)
            if not case_number or not address:
                continue

            active_rows += 1
            key = f"{case_number}|{address.upper()}"
            group = groups.setdefault(
                key,
                {
                    "case_number": case_number,
                    "address": address,
                    "city": self._clean(row.get("city")) or "CHATTANOOGA",
                    "state": self._clean(row.get("state")) or "TN",
                    "rows": [],
                    "descriptions": [],
                    "record_ids": [],
                    "is_high_signal": False,
                    "date_entered": self._clean(row.get("date_entered")),
                    "latitude": self._clean(row.get("latitude")),
                    "longitude": self._clean(row.get("longitude")),
                    "council_district": self._clean(row.get("council_district")),
                },
            )

            description = self._clean(row.get("description"))
            if description and description not in group["descriptions"]:
                group["descriptions"].append(description)
            record_id = self._clean(row.get("record_id"))
            if record_id:
                group["record_ids"].append(record_id)
            group["rows"].append(row)
            group["is_high_signal"] = group["is_high_signal"] or self._is_high_signal(row)

        leads = [
            self._build_lead(group)
            for group in groups.values()
            if group["is_high_signal"]
        ]
        self.logger.info(
            f"active rows={active_rows}, active case/address groups={len(groups)}, "
            f"high-signal leads={len(leads)}"
        )
        return leads

    def _build_lead(self, group: Dict[str, Any]) -> LeadPayload:
        city = self._title_city(group["city"])
        state = group["state"].upper()
        full_address = f"{group['address']}, {city}, {state}"

        date_iso = self._date_to_iso(group["date_entered"])
        descriptions = group["descriptions"][:6]
        notes_parts = [
            f"case {group['case_number']} [HIGH-SIGNAL]",
            f"{len(group['rows'])} active violation row(s)",
        ]
        if descriptions:
            notes_parts.append("violations: " + "; ".join(descriptions))
        if date_iso:
            notes_parts.append(f"entered: {date_iso}")
        if group["council_district"]:
            notes_parts.append(f"district: {group['council_district']}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(
                self.name, f"{group['case_number']}|{group['address'].upper()}"
            ),
            property_address=full_address,
            county="Hamilton County",
            distress_type="CODE_VIOLATION",
            admin_notes=" | ".join(notes_parts),
            source_url=CHATTANOOGA_CODES_SOURCE_URL,
            raw_payload={
                "chattanooga_codes": {
                    "case_number": group["case_number"],
                    "record_ids": group["record_ids"],
                    "date_entered": group["date_entered"],
                    "latitude": group["latitude"],
                    "longitude": group["longitude"],
                    "council_district": group["council_district"],
                    "descriptions": group["descriptions"],
                    "rows": group["rows"],
                }
            },
        )

    def _is_high_signal(self, row: Dict[str, str]) -> bool:
        flag_dangerous = self._clean(row.get("flag_dangerous")).lower() == "true"
        haystack = " ".join(
            [
                self._clean(row.get("description")),
                self._clean(row.get("description_extended")),
                self._clean(row.get("comments")),
            ]
        ).upper()
        return flag_dangerous or any(term in haystack for term in HIGH_SIGNAL_TERMS)

    def _build_address(self, row: Dict[str, str]) -> Optional[str]:
        street_number = self._clean(row.get("street_number"))
        street_name = self._clean(row.get("street_name"))
        address = " ".join(part for part in [street_number, street_name] if part).strip()
        return address or None

    @staticmethod
    def _clean(value: object) -> str:
        if value is None:
            return ""
        return str(value).strip()

    @staticmethod
    def _date_to_iso(value: str) -> Optional[str]:
        if not value:
            return None
        try:
            return datetime.fromisoformat(value).date().isoformat()
        except ValueError:
            return value[:10] if len(value) >= 10 else None

    @staticmethod
    def _title_city(value: str) -> str:
        return " ".join(part.capitalize() for part in value.split()) or "Chattanooga"


def run() -> dict:
    bot = ChattanoogaCodesBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
