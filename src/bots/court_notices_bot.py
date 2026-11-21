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
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._base import BotBase, LeadPayload
from . import _court_notices as cn
from ._court_order import PARTITION_ACTION_RE

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
               row: Dict[str, Any], source_url: Optional[str] = None) -> Optional[LeadPayload]:
    kind = cn.classify(row["lines"])
    if kind == cn.KIND_OTHER:
        return None
    if source_url is None:
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
    partition = bool(PARTITION_ACTION_RE.search(raw["text"]))
    review = {} if partition else cn.court_sale_review(row["lines"])
    if review.get("stage") in {"no_sale_in_notice", "trustee_sale"}:
        return None
    if review:
        raw["court_sale_review"] = review
    fields = cn.extract_court_sale(row["lines"])
    if review.get("docket"):
        fields = {**fields, "reviewed_docket": review["docket"]}
    raw["court_notice"] = fields
    source, ident = lead_key(kind, fields["county"], fields["docket"], row["notice_id"])
    return LeadPayload(
        bot_source=bot_name,
        pipeline_lead_key=BotBase.make_lead_key(source, ident),
        county=fields["county"],
        property_address=review.get("property_address"),
        distress_type="PARTITION" if partition else "COURT_SALE",
        admin_notes=f"{review.get('stage', 'partition')}: {review.get('docket') or fields['docket'] or '?'} ({fields['county'] or '?'}): {fields['headline'][:200]}",
        raw_payload=raw,
        source_url=source_url,
    )


def cheatham_tax_tracts(row, pub_date, today):
    lines = row["lines"]
    if not any(line.upper() == "NOTICE OF DELINQUENT TAX SALE" for line in lines):
        return []
    text = " ".join(lines)
    if not re.search(r"CHANCERY COURT FOR CHEATHAM COUNTY", text, re.I):
        return []
    docket = re.search(r"Civil Action No\.\s*(\d+)\b", text, re.I)
    scheduled = re.search(r"highest and best bidder at\s+\d+:\d+\s*[ap]\.m\. on \w+, (\w+ \d+, \d{4})", text, re.I)
    if not docket or not scheduled:
        return []
    try:
        sale = datetime.strptime(scheduled[1], "%B %d, %Y").date()
    except ValueError:
        return []
    if sale < today:
        return []
    leads = []
    for line in lines:
        tract = re.match(r"([A-Z])\.\s+.+?\bParcel\s+(.+?)\s*\([^)]+\),\s*having an add(?:ress| re s) of (.+?)\s*[,\-]\s*Current Owners?\s*-\s*(.+?)\s*-\s*[$S]([\d,]+\.\d{2})$", line, re.I)
        if not tract:
            continue
        letter, parcel, location, owners, amount = tract.groups()
        owners = owners.split(" - Period of delinquency")[0].removeprefix("Defendant ").strip()
        leads.append(LeadPayload(
            bot_source="court_notices", county="cheatham", distress_type="TAX_LIEN",
            pipeline_lead_key=BotBase.make_lead_key("cheatham_tax", f"{docket[1]}|{row['notice_id']}|{letter}"),
            property_address=location if re.match(r"\d+\s", location) else None,
            owner_name_records=None if owners.lower().startswith("unknown") else owners,
            trustee_sale_date=sale.isoformat(),
            source_url=f"https://www.tnledger.com/Search/Details/ViewNotice.aspx?id={row['notice_id']}&date={pub_date.month}/{pub_date.day}/{pub_date.year}",
            raw_payload={"kind": "cheatham_tax_sale", "docket": docket[1], "notice_id": row["notice_id"],
                         "tract": letter, "parcel_printed": parcel, "location_printed": location,
                         "owners_printed": owners, "published_judgment": float(amount.replace(",", "")),
                         "tract_text": line, "publication_date": pub_date.isoformat()},
        ))
    return leads


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
                    if pub["key"] == "nashville_ledger":
                        tracts = cheatham_tax_tracts(row, d, today)
                        if tracts:
                            for tract in tracts:
                                leads.setdefault(tract.pipeline_lead_key, tract)
                            continue
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
