"""
Tennessee bankruptcy events from the courts' free RSS feeds: the docket
entries that put a debtor's house back in play after the filing.

  BK_DISMISSED         chapter 13 case dismissed. The automatic stay ends, so
                       the lender can restart the foreclosure that the filing
                       paused. The best bankruptcy signal FALCO has.
  BK_DISMISSAL_MOTION  the chapter 13 trustee (or a creditor) asked to dismiss,
                       usually for missed plan payments; or an order was entered
                       on that motion and the feed does not say which way.
  BK_STAY_RELIEF       an order on a lender's request to lift the stay for one
                       property: the lender is clearing the way to foreclose.
  BK_ABANDONED         the trustee is giving up a property (notice, motion or
                       order). It leaves the estate and goes back to the debtor,
                       usually with no equity the trustee could use, often
                       with the lender close behind.

Source: https://ecf.{tnmb,tneb,tnwb}.uscourts.gov/cgi-bin/rss_outside.pl
(free, public, no PACER account). Feeds cover about 24 hours, so this must
run at least daily; the pipeline runs twice a day. One lead per case and
event type, so a motion to dismiss and the later dismissal are two leads.

The feed carries the debtor's name, chapter, trustee and docket link, never a
property address. bankruptcy_property_enricher matches debtors to homes
(Middle TN only today). Classification lives in _court_rss.bk_event().

Distress types: BK_DISMISSED, BK_DISMISSAL_MOTION, BK_STAY_RELIEF, BK_ABANDONED
"""

from __future__ import annotations

import sys
from typing import Any, Dict, List

from ._base import BotBase, LeadPayload
from ._court_rss import (BK_ABANDONED, BK_DISMISSAL_MOTION, BK_DISMISSED, BK_STAY_RELIEF,
                         bk_event, debtors, parse_feed)

FEED = "https://ecf.{court}.uscourts.gov/cgi-bin/rss_outside.pl"
TN_BANKRUPTCY_COURTS = {
    "tnmb": "Middle District of Tennessee (Nashville)",
    "tneb": "Eastern District of Tennessee (Knoxville, Chattanooga)",
    "tnwb": "Western District of Tennessee (Memphis, Jackson)",
}

_STATUS_NOTE = {
    "order": "order entered",
    "motion": "motion filed",
    "notice": "notice filed",
    "order_on_motion": "order entered on the motion; open the docket to see if it was granted",
}
_EVENT_NOTE = {
    BK_DISMISSED: "Chapter 13 case dismissed",
    BK_DISMISSAL_MOTION: "Chapter 13 dismissal requested",
    BK_STAY_RELIEF: "Lender relief from stay",
    BK_ABANDONED: "Trustee abandoning property",
}


def build_lead(bot: "BankruptcyEventsBot", court: str, item: Dict[str, Any]) -> LeadPayload | None:
    """One staged lead for a classified docket entry, or None."""
    ev = bk_event(item)
    if not ev:
        return None
    names = debtors(item.get("caption") or "")
    if not names:
        return None
    case = item.get("case_number") or ""
    note = f"{_EVENT_NOTE[ev['type']]} ({court} {case}, ch. {item.get('chapter') or '?'}): {_STATUS_NOTE[ev['status']]}"
    if item.get("trustee"):
        note += f"; trustee {item['trustee']}"
    return LeadPayload(
        bot_source=bot.name,
        pipeline_lead_key=bot.make_lead_key(bot.name, f"{court}|{case}|{ev['type']}"),
        distress_type=ev["type"],
        full_name=names[0],
        owner_name_records="; ".join(names),
        source_url=item.get("docket_url"),
        admin_notes=note,
        raw_payload={
            "source": "pacer_rss", "state": "TN", "court_id": court, "court": TN_BANKRUPTCY_COURTS[court],
            "case_number": case, "case_name": item.get("caption"), "chapter": item.get("chapter"),
            "trustee": item.get("trustee"), "docket_url": item.get("docket_url"),
            "bk_event": ev, "event_seen": item.get("published"),
        },
    )


class BankruptcyEventsBot(BotBase):
    name = "bankruptcy_events"
    description = "TN chapter 13 dismissals, lender stay relief and trustee abandonments, via free court RSS"
    throttle_seconds = 1.0
    # A normal weekday has dozens across the three courts; weekends can be empty.
    expected_min_yield = 0
    frozen_after_runs = 10_000

    def scrape(self) -> List[LeadPayload]:
        leads: Dict[str, LeadPayload] = {}
        for court in TN_BANKRUPTCY_COURTS:
            for item in self._feed(court):
                lead = build_lead(self, court, item)
                if lead:
                    leads.setdefault(lead.pipeline_lead_key, lead)
        counts: Dict[str, int] = {}
        for lead in leads.values():
            counts[lead.distress_type or "?"] = counts.get(lead.distress_type or "?", 0) + 1
        self.logger.info(f"staging {len(leads)} lead(s): {counts}")
        return list(leads.values())

    def _feed(self, court: str) -> List[Dict[str, Any]]:
        res = self.fetch(FEED.format(court=court), timeout=60)
        if res is None or res.status_code != 200:
            self.logger.warning(f"{court}: feed unavailable ({getattr(res, 'status_code', 'no response')})")
            return []
        items = parse_feed(res.text)
        self.logger.info(f"{court}: {len(items)} docket entries")
        return items


def run() -> dict:
    return BankruptcyEventsBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
