"""
Pinellas County Clerk daily new-case files: free, no login, posted each
morning at https://publicfiles.mypinellasclerk.gov/download/. The Clerk
confirmed in writing on 2026-11-17 (GovQA I196030-111526) that automated
downloading from this site for normal business purposes is fine.

Stages one lead per case, with the same rules as the Hillsborough bot
(parsing shared through _fl_clerk; Pinellas columns translated in
_fl_pinellas):
  LIS_PENDENS  circuit mortgage foreclosures (homestead / non-homestead /
               commercial, with the clerk's value tier; defendants come with
               their mailing address) and county-court foreclosures (claims
               up to $50,000, mostly condominium and HOA liens)
  PARTITION    co-owners or heirs suing to force a sale
  PROBATE      formal, ancillary and summary (> $1000) administrations:
               decedent, petitioner and attorney (no addresses; the DOR roll
               enricher finds the decedent's parcel by name)

The clerk's LIS_PENDENS_DAILY folder stopped updating on 2026-09-30, so
foreclosures come from NEW_CASE_FILINGS_DAILY. Each run reads the last
LOOKBACK_DAYS files of each kind; lead keys are case numbers, so re-reads
are duplicates.

Distress types: LIS_PENDENS, PARTITION, PROBATE
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload
from ._fl_clerk import civil_case, group_cases, probate_case, read_rows
from ._fl_pinellas import civil_rows, file_day, probate_rows, read_probate

BASE = "https://publicfiles.mypinellasclerk.gov/download"
DIRS = (("CIVIL/NEW_CASE_FILINGS_DAILY", "CivilNewFilingsDaily_"),
        ("PROBATE/NEW_ESTATE_CASE_FILINGS_DAILY", "EstateNewCaseFilingsDaily_"))
LOOKBACK_DAYS = 7


def _fl_only(addr):
    """Out-of-state party addresses are mailing addresses, not the Florida
    property; the roll enricher finds the parcel by name instead."""
    return addr if addr and re.search(r"\bFL\b", addr) else None


class FlPinellasClerkBot(BotBase):
    name = "fl_pinellas_clerk"
    description = "Pinellas FL clerk daily filings: mortgage/HOA foreclosures, partitions, probate (free CSVs)"
    throttle_seconds = 1.0
    expected_min_yield = 0           # weekends and holidays post empty files
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        leads: Dict[str, LeadPayload] = {}
        for folder, prefix in DIRS:
            probate = prefix.startswith("Estate")
            for url in self._recent_files(folder, prefix):
                res = self.fetch(url, timeout=120)
                if res is None or res.status_code != 200:
                    self.logger.warning(f"{url}: {getattr(res, 'status_code', 'no response')}")
                    continue
                res.encoding = res.encoding or "utf-8"
                cases = group_cases(probate_rows(read_probate(res.text)) if probate else civil_rows(read_rows(res.text)))
                kept = 0
                for cn, rows in cases.items():
                    case = probate_case(cn, rows) if probate else civil_case(cn, rows)
                    if not case:
                        continue
                    if probate:
                        pet = next((r for r in rows if r.get("PartyType") != "Decedent"), {})
                        case["attorney_firm"] = pet.get("AttorneyFirm") or None
                    else:
                        case["pinellas_case_type"] = rows[0].get("PinellasCaseType")
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
        names = {n for n in re.findall(rf'{re.escape(prefix)}\d{{2}}-\d{{2}}-\d{{4}}\.csv', res.text) if file_day(n)}
        newest = sorted(names, key=file_day)[-LOOKBACK_DAYS:]
        return [f"{BASE}/{folder}/{n}" for n in newest]

    def _lead(self, c: Dict[str, Any], url: str) -> LeadPayload:
        key = self.make_lead_key(self.name, c["case_number"])
        raw = {"source": "pinellas_clerk_daily", "state": "FL", "county": "pinellas", "file": url, **c}
        if c["kind"] == "probate":
            return LeadPayload(
                bot_source=self.name, pipeline_lead_key=key, distress_type="PROBATE", county="Pinellas",
                property_address=_fl_only(c["decedent_address"]), full_name=c["petitioner"],
                owner_name_records=c["decedent"], source_url=url, raw_payload=raw,
                admin_notes=(f"Pinellas probate {c['case_number']} ({c['case_type']}): estate of {c['decedent']}"
                             + (f", died {c['date_of_death']}" if c.get("date_of_death") else "")
                             + (f"; petitioner {c['petitioner']}" if c.get("petitioner") else "")
                             + (f"; attorney {c['attorney']}" if c.get("attorney") else "")
                             + (f" ({c['attorney_firm'].strip()})" if c.get("attorney_firm") else "")),
            )
        label = {"mortgage": "Mortgage foreclosure", "association_lien": "County-court foreclosure (usually an association lien)",
                 "partition": "Partition"}[c["kind"]]
        return LeadPayload(
            bot_source=self.name, pipeline_lead_key=key, distress_type=c["distress_type"], county="Pinellas",
            property_address=_fl_only(c["owner_address"]), full_name=c["owners"][0],
            owner_name_records="; ".join(c["owners"]), source_url=url, raw_payload=raw,
            admin_notes=(f"{label} {c['case_number']} filed {c['filed']}"
                         + (f" ({c['occupancy']}, {c['value_tier']})" if c.get("occupancy") else "")
                         + (f"; plaintiff {c['plaintiff']}" if c.get("plaintiff") else "")),
        )


def run() -> dict:
    return FlPinellasClerkBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
