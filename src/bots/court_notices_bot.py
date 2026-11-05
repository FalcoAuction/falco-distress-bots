"""Court notices from The Nashville Ledger and Memphis Daily News.

Both papers (The Daily News Publishing Co.) print every court notice in full
on their Notices.aspx edition pages. That includes the probate notices to
creditors that tnpublicnotice.com only shows as truncated excerpts, with the
full text behind a CAPTCHA this bot does not touch.

From each probate notice we stage a PROBATE lead with the decedent as
full_name and the court's county, which is exactly what
probate_property_enricher needs to match the decedent to parcels. The
structured notice goes in raw_payload['court_notice']: docket, date of death,
letters date and type, personal representatives (with addresses where
printed) and the estate's attorney (with office address where printed).

Partition suits and court-ordered sales are staged as COURT_SALE: a direct
"this asset will be sold" signal for the any-asset auction lanes.

Coverage: the Ledger's Friday edition carries Middle Tennessee court notices
(Davidson plus surrounding counties); the Memphis Daily News carries Shelby
and West Tennessee. The Hamilton County Herald uses a different layout and
is not covered here.

Lead key is county + docket, so the same estate republished across weeks or
in both papers stages once.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._base import BotBase, LeadPayload
from . import _court_notices as cn

PUBLICATIONS: List[Dict[str, Any]] = [
    # The Ledger publishes notices weekly, on Fridays.
    {"key": "nashville_ledger", "base": "https://www.tnledger.com", "weekdays": {4}, "lookback_days": 21},
    # The Memphis Daily News publishes notices on weekdays.
    {"key": "memphis_daily_news", "base": "https://www.memphisdailynews.com", "weekdays": {0, 1, 2, 3, 4}, "lookback_days": 10},
]


def edition_dates(today: date, weekdays: Iterable[int], lookback_days: int) -> List[date]:
    """Publication dates within the lookback window, newest first."""
    wanted = set(weekdays)
    return [d for d in (today - timedelta(days=i) for i in range(lookback_days + 1)) if d.weekday() in wanted]


def lead_key(kind: str, county: Optional[str], docket: Optional[str], notice_id: str) -> Tuple[str, str]:
    """Stable identity: county + docket when both exist (an estate is one lead
    however many weeks or papers it runs in), otherwise the notice ID."""
    if county and docket:
        return ("court_notice", f"{kind}|{county}|{docket}")
    return ("court_notice", f"{kind}|notice|{notice_id}")


def build_lead(bot_name: str, publication: str, base: str, pub_date: date,
               row: Dict[str, Any]) -> Optional[LeadPayload]:
    kind = cn.classify(row["lines"])
    if kind == cn.KIND_OTHER:
        return None
    date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
    source_url = f"{base}/Search/Details/ViewNotice.aspx?id={row['notice_id']}&date={date_param}"
    raw: Dict[str, Any] = {
        "publication": publication,
        "notice_id": row["notice_id"],
        "publication_date": pub_date.isoformat(),
        "kind": kind,
        "text": "\n".join(row["lines"]),
    }
    if kind == cn.KIND_PROBATE:
        fields = cn.extract_probate(row["lines"])
        if not fields["decedent"]:
            return None  # a probate lead without a decedent can't be matched to property
        raw["court_notice"] = fields
        reps = ", ".join(f"{r['name']} ({r['role']})" for r in fields["representatives"]) or "none parsed"
        attorney = (", ".join(fields["attorney"]["names"]) or "none parsed").rstrip(".")
        source, ident = lead_key(kind, fields["county"], fields["docket"], row["notice_id"])
        return LeadPayload(
            bot_source=bot_name,
            pipeline_lead_key=BotBase.make_lead_key(source, ident),
            county=fields["county"],
            full_name=fields["decedent"],
            owner_name_records=fields["decedent"],
            distress_type="PROBATE",
            admin_notes=f"Probate {fields['docket'] or '?'} ({fields['county'] or '?'}). "
                        f"Representatives: {reps}. Attorney: {attorney}.",
            raw_payload=raw,
            source_url=source_url,
        )
    fields = cn.extract_court_sale(row["lines"])
    raw["court_notice"] = fields
    source, ident = lead_key(kind, fields["county"], fields["docket"], row["notice_id"])
    return LeadPayload(
        bot_source=bot_name,
        pipeline_lead_key=BotBase.make_lead_key(source, ident),
        county=fields["county"],
        distress_type="COURT_SALE",
        admin_notes=f"Court-ordered sale or partition {fields['docket'] or '?'} ({fields['county'] or '?'}): {fields['headline'][:200]}",
        raw_payload=raw,
        source_url=source_url,
    )


class CourtNoticesBot(BotBase):
    name = "court_notices"
    description = "Probate and court-ordered-sale notices from the Nashville Ledger and Memphis Daily News"
    throttle_seconds = 1.5
    expected_min_yield = 1
    # Weekly Ledger editions repeat for two weeks and estates rarely change,
    # so identical output across a few runs is normal; flag only a long freeze.
    frozen_after_runs = 10

    def scrape(self) -> List[LeadPayload]:
        today = datetime.now(timezone.utc).date()
        leads: Dict[str, LeadPayload] = {}
        empty_editions: List[str] = []
        for pub in PUBLICATIONS:
            for d in edition_dates(today, pub["weekdays"], pub["lookback_days"]):
                res = self.fetch(f"{pub['base']}/Notices.aspx",
                                 params={"noticesDate": f"{d.month}/{d.day}/{d.year}"})
                if res is None or res.status_code != 200:
                    self.logger.warning(f"{pub['key']} {d}: fetch failed")
                    continue
                rows = cn.parse_court_rows(res.text)
                if not rows:
                    empty_editions.append(f"{pub['key']} {d}")
                    continue
                for row in rows:
                    lead = build_lead(self.name, pub["key"], pub["base"], d, row)
                    if lead and lead.pipeline_lead_key not in leads:
                        leads[lead.pipeline_lead_key] = lead
        if empty_editions:
            # A publication day with no court grid at all is either a quiet day
            # or a layout change; the frozen/zero-yield health checks catch a
            # persistent one.
            self.logger.info(f"editions without a court grid: {', '.join(empty_editions)}")
        probate = sum(1 for l in leads.values() if l.distress_type == "PROBATE")
        self.logger.info(f"{probate} probate + {len(leads) - probate} court-sale notices")
        return list(leads.values())


def run() -> dict:
    return CourtNoticesBot().run()


if __name__ == "__main__":
    if os.environ.get("FALCO_COURT_NOTICES_DRY_RUN") == "1":
        bot = CourtNoticesBot()
        found = bot.scrape()
        for lead in found[:10]:
            print(lead.distress_type, lead.county, lead.full_name, "|", (lead.admin_notes or "")[:160])
        print(f"total {len(found)}")
    else:
        print(run())
