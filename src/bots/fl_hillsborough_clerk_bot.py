"""
Hillsborough County Clerk daily new-case files — free, no login, posted
nightly as plain CSVs on the clerk's public records server:

  https://publicrec.hillsclerk.com/Civil/dailyfilings/CivilFiling_YYYYMMDD.csv
  https://publicrec.hillsclerk.com/Probate/dailyfilings/ProbateFiling_YYYYMMDD.csv

The only one of Florida's ten largest counties that publishes this openly
(the rest sit behind CAPTCHAs, logins or paid FTP), so this is the Florida
foreclosure + probate lane until the other clerks answer ch. 119 requests.

Stages one lead per case:
  LIS_PENDENS  mortgage foreclosure complaints (homestead / non-homestead /
               commercial, with the clerk's value tier) and county-court
               association lien foreclosures (HOA / condo)
  PARTITION    co-owners or heirs suing to force a sale
  PROBATE      formal, ancillary and summary (> $1000) administrations;
               decedent address, date of death, petitioner and attorney

Each run reads the last LOOKBACK_DAYS files from the directory listing, so
a missed night is picked up the next run; lead keys are case numbers, so
re-reads are duplicates, not new rows. Leads carry raw_payload.state = FL;
the fl_roll_enricher attaches the parcel, value and mailing address.

Distress types: LIS_PENDENS, PARTITION, PROBATE
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload
from ._fl_clerk import civil_case, group_cases, probate_case, read_rows

BASE = "https://publicrec.hillsclerk.com"
DIRS = (("Civil/dailyfilings", "CivilFiling_"), ("Probate/dailyfilings", "ProbateFiling_"))
LOOKBACK_DAYS = 7


class FlHillsboroughClerkBot(BotBase):
    name = "fl_hillsborough_clerk"
    description = "Hillsborough FL clerk daily filings: mortgage/HOA foreclosures, partitions, probate (free CSVs)"
    throttle_seconds = 1.0
    expected_min_yield = 0           # weekends and holidays post no files
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        leads: Dict[str, LeadPayload] = {}
        for folder, prefix in DIRS:
            for url in self._recent_files(folder, prefix):
                res = self.fetch(url, timeout=120)
                if res is None or res.status_code != 200:
                    self.logger.warning(f"{url}: {getattr(res, 'status_code', 'no response')}")
                    continue
                res.encoding = res.encoding or "utf-8"
                cases = group_cases(read_rows(res.text))
                kept = 0
                for cn, rows in cases.items():
                    case = probate_case(cn, rows) if prefix.startswith("Probate") else civil_case(cn, rows)
                    if not case:
                        continue
                    lead = self._lead(case, url)
                    if lead.pipeline_lead_key not in leads:
                        leads[lead.pipeline_lead_key] = lead
                        kept += 1
                self.logger.info(f"{url.rsplit('/', 1)[-1]}: {len(cases)} cases, {kept} kept")
        self.logger.info(f"staging {len(leads)} lead(s)")
        return list(leads.values())

    def _recent_files(self, folder: str, prefix: str) -> List[str]:
        res = self.fetch(f"{BASE}/{folder}/", timeout=60)
        if res is None or res.status_code != 200:
            self.logger.warning(f"{folder}: listing unavailable")
            return []
        names = sorted(set(re.findall(rf'{re.escape(prefix)}\d{{8}}\.csv', res.text)))
        return [f"{BASE}/{folder}/{n}" for n in names[-LOOKBACK_DAYS:]]

    def _lead(self, c: Dict[str, Any], url: str) -> LeadPayload:
        key = self.make_lead_key(self.name, c["case_number"])
        raw = {"source": "hillsclerk_daily", "state": "FL", "county": "hillsborough", "file": url, **c}
        if c["kind"] == "probate":
            return LeadPayload(
                bot_source=self.name, pipeline_lead_key=key, distress_type="PROBATE", county="Hillsborough",
                property_address=_fl_only(c["decedent_address"]), full_name=c["petitioner"],
                owner_name_records=c["decedent"], source_url=url, raw_payload=raw,
                admin_notes=(f"Hillsborough probate {c['case_number']} ({c['case_type']}): estate of {c['decedent']}"
                             + (f", died {c['date_of_death']}" if c.get("date_of_death") else "")
                             + (f"; petitioner {c['petitioner']}" if c.get("petitioner") else "")
                             + (f"; attorney {c['attorney']}" if c.get("attorney") else "")),
            )
        label = {"mortgage": "Mortgage foreclosure", "association_lien": "Association lien foreclosure",
                 "partition": "Partition"}[c["kind"]]
        return LeadPayload(
            bot_source=self.name, pipeline_lead_key=key, distress_type=c["distress_type"], county="Hillsborough",
            property_address=_fl_only(c["owner_address"]), full_name=c["owners"][0],
            owner_name_records="; ".join(c["owners"]), source_url=url, raw_payload=raw,
            admin_notes=(f"{label} {c['case_number']} filed {c['filed']}"
                         + (f" ({c['occupancy']}, {c['value_tier']})" if c.get("occupancy") else "")
                         + (f"; plaintiff {c['plaintiff']}" if c.get("plaintiff") else "")),
        )


def _fl_only(addr):
    """Out-of-state party addresses (ancillary estates, absentee owners) are
    mailing addresses, not the Florida property; the roll enricher finds the
    parcel by name instead."""
    return addr if addr and re.search(r"\bFL\b", addr) else None


def run() -> dict:
    return FlHillsboroughClerkBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
