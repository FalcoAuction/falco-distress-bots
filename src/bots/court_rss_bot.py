"""
Federal court RSS feeds — free, no PACER account, no CourtListener token.

Stages two kinds of high-value-asset leads:

  VESSEL_ARREST  civil cases in FL and TN district courts whose caption
                 names a vessel (lender foreclosing a preferred ship
                 mortgage, marina or crew lien). The boat itself is the
                 asset; the pitch is a private auction instead of the
                 Marshal's sale, to the plaintiff's lawyer.
  BANKRUPTCY     FL bankruptcy debtors, but ONLY when the debtor matches a
                 FL-registered aircraft worth ~$100K+, or is a business
                 whose name says aviation / marine / yacht. Everyone else is
                 dropped in memory so FL names never reach the TN real-estate
                 enrichers.

Feeds cover the last ~24 hours, so this needs to run at least daily (the
pipeline runs twice a day). Leads carry raw_payload.state so the aircraft
matcher and the CRM keep FL and TN apart. Parsing lives in `_court_rss.py`.

Distress types: VESSEL_ARREST, BANKRUPTCY
"""

from __future__ import annotations

import re
import sys
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload
from ._court_rss import debtors, parse_feed, vessel_case

FEED = "https://ecf.{court}.uscourts.gov/cgi-bin/rss_outside.pl"
DISTRICT_COURTS = (("flsd", "FL"), ("flmd", "FL"), ("flnd", "FL"), ("tnmd", "TN"), ("tned", "TN"), ("tnwd", "TN"))
FL_BANKRUPTCY_COURTS = ("flsb", "flmb", "flnb")
TN_BANKRUPTCY_COURTS = ("tnmb", "tneb", "tnwb")
# "Voluntary Petition (Chapter 7)", "Chapter 13 Voluntary Petition (fee) - case
# upload"; not "Post-Petition Mortgage Fees" or amended petitions.
NEW_PETITION = re.compile(r"^(?!.*amended)(?:Chapter \d+ )?(?:Voluntary|Involuntary) Petition\b", re.I)
ASSET_BUSINESS = re.compile(r"\b(YACHTS?|MARINE|BOATS?|BOATING|AVIATION|AIRCRAFT|AIR CHARTERS?|JETS?|AVIONICS|"
                            r"SEAPLANES?|HELICOPTERS?)\b", re.I)
ENTITY = re.compile(r"\b(LLC|L\.L\.C|INC|CORP|CORPORATION|COMPANY|CO|LP|LLP|PLLC|LTD)\b\.?", re.I)


class CourtRssBot(BotBase):
    name = "court_rss"
    description = "Vessel arrests (FL+TN district courts) and FL bankruptcies with aircraft or marine assets, via free court RSS"
    throttle_seconds = 1.0
    expected_min_yield = 0          # vessel arrests are a few a month; zero is a normal day
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        leads: Dict[str, LeadPayload] = {}
        for court, state in DISTRICT_COURTS:
            for item in self._feed(court):
                vc = vessel_case(item)
                if not vc:
                    continue
                key = self.make_lead_key(self.name, f"{court}|{item['case_number']}")
                if key in leads:
                    continue
                label = vc["vessel"] or "unnamed vessel"
                leads[key] = LeadPayload(
                    bot_source=self.name, pipeline_lead_key=key, distress_type="VESSEL_ARREST",
                    full_name=vc["plaintiff"], owner_name_records=vc["defendants"],
                    source_url=item["docket_url"],
                    admin_notes=f"Vessel arrest: {label} ({court} {item['case_number']})",
                    raw_payload={"source": "pacer_rss", "state": state, "court_id": court,
                                 "case_number": item["case_number"], "case_name": item["caption"],
                                 "first_entry": item["entry_type"], "first_seen": item["published"],
                                 "docket_url": item["docket_url"], "vessel_case": vc},
                )
        for lead in self._fl_bankruptcies():
            leads.setdefault(lead.pipeline_lead_key, lead)
        for lead in self._tn_bankruptcies():
            leads.setdefault(lead.pipeline_lead_key, lead)
        self.logger.info(f"staging {len(leads)} lead(s)")
        return list(leads.values())

    def _fl_bankruptcies(self) -> List[LeadPayload]:
        cases: Dict[str, Dict[str, Any]] = {}
        for court in FL_BANKRUPTCY_COURTS:
            for item in self._feed(court):
                cases.setdefault(f"{court}|{item['case_number']}", {**item, "court_id": court})
        if not cases:
            return []
        from .faa_aircraft_bot import FaaAircraftBot   # shares one FAA download per process
        index = FaaAircraftBot()._load_index()
        if index is None:
            self.logger.warning("FAA index unavailable; FL bankruptcy overlay skipped this run")
            return []
        out = []
        for ck, item in cases.items():
            caption = item["caption"]
            names = debtors(caption)
            business = bool(ENTITY.search(caption))
            planes = index.match({"distress_type": "BANKRUPTCY", "full_name": names[0] if names else caption,
                                  "case_name": caption, "court_id": item["court_id"], "state": "FL"})
            asset_business = business and bool(ASSET_BUSINESS.search(caption))
            if not planes and not asset_business:
                continue
            county = planes[0]["registrant_county"] if planes else None
            out.append(LeadPayload(
                bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, ck),
                distress_type="BANKRUPTCY", full_name=caption, owner_name_records=caption,
                county=county.title() if county else None, source_url=item["docket_url"],
                admin_notes=f"FL bankruptcy ({item['court_id']} {item['case_number']}, ch. {item['chapter'] or '?'})"
                            + (f": possible FAA registrant match for {planes[0]['aircraft']} {planes[0]['n_number']}" if planes else ""),
                raw_payload={"source": "pacer_rss", "state": "FL", "court_id": item["court_id"],
                             "case_number": item["case_number"], "case_name": caption, "chapter": item["chapter"],
                             "trustee": item["trustee"], "docket_url": item["docket_url"],
                             "first_seen": item["published"]},
            ))
        return out

    def _tn_bankruptcies(self) -> List[LeadPayload]:
        """New Tennessee bankruptcy petitions. CourtListener (the old source,
        courtlistener_bankruptcy) has 401'd without a token since 2026-10-27;
        the courts' own RSS carries the debtor, chapter and trustee for free.
        Only petitions are staged, not every later docket entry."""
        out = []
        for court in TN_BANKRUPTCY_COURTS:
            for item in self._feed(court):
                if not NEW_PETITION.search(item.get("entry_type") or ""):
                    continue
                names = debtors(item["caption"])
                if not names:
                    continue
                out.append(LeadPayload(
                    bot_source=self.name, pipeline_lead_key=self.make_lead_key(self.name, f"{court}|{item['case_number']}"),
                    distress_type="BANKRUPTCY", full_name=names[0], owner_name_records="; ".join(names),
                    source_url=item["docket_url"],
                    admin_notes=f"TN bankruptcy {court} {item['case_number']}, ch. {item['chapter'] or '?'}"
                                + (f"; trustee {item['trustee']}" if item.get("trustee") else ""),
                    raw_payload={"source": "pacer_rss", "state": "TN", "court_id": court,
                                 "case_number": item["case_number"], "case_name": item["caption"],
                                 "chapter": item["chapter"], "trustee": item["trustee"],
                                 "docket_url": item["docket_url"], "first_entry": item["entry_type"],
                                 "first_seen": item["published"]},
                ))
        return out

    def _feed(self, court: str) -> List[Dict[str, Any]]:
        res = self.fetch(FEED.format(court=court), timeout=60)
        if res is None or res.status_code != 200:
            self.logger.warning(f"{court}: feed unavailable ({getattr(res, 'status_code', 'no response')})")
            return []
        items = parse_feed(res.text)
        self.logger.info(f"{court}: {len(items)} docket entries")
        return items


def run() -> dict:
    return CourtRssBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
