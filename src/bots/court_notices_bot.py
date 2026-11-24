"""Court notices from the Memphis Daily News, and Cheatham tax sale tracts
from the County Trustee's posted notice.

The Memphis Daily News prints every court notice in full on its daily
/notices/YYYY/Mon/D/ index page, which its robots.txt allows. That includes
the probate notices to creditors that tnpublicnotice.com only shows as
truncated excerpts, with the full text behind a CAPTCHA this bot does not
touch.

The Nashville Ledger (tnledger.com, same publisher) was read here until
2026-11-24. Its robots.txt disallows every path, so it is no longer fetched
(see _source_access.STOPPED). Rows staged from it stay as they are, with
their publication dates. New Davidson and Middle Tennessee court notices are
a coverage gap until a permitted source exists.

From each probate notice we stage a PROBATE lead with the decedent as
full_name and the court's county, which is exactly what
probate_property_enricher needs to match the decedent to parcels. The
structured notice goes in raw_payload['court_notice']: docket, date of death,
letters date and type, personal representatives (with addresses where
printed) and the estate's attorney (with office address where printed).

Partition suits and court-ordered sales are staged as COURT_SALE: a direct
"this asset will be sold" signal for the any-asset auction lanes.

Cheatham County delinquent tax sale tracts used to be read from the Ledger's
copy of the court's notice. They now come from the same notice as the County
Trustee posts it (cheathamcountytrustee.gov, the PDF cheatham_tax_sale already
reads). A case whose tracts are already staged is left to cheatham_tax_sale.

Lead key is county + docket, so the same estate republished across weeks
stages once.
"""

from __future__ import annotations

import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._base import BotBase, LeadPayload
from ._probate_staging import ProbateNoticeBot, remember_notice
from . import _court_notices as cn
from ._court_order import PARTITION_ACTION_RE

MONTHS = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

PUBLICATIONS: List[Dict[str, Any]] = [
    # The Memphis Daily News publishes notices on weekdays. Only the
    # /notices/ index is read: robots.txt (2026-11-24) allows it and
    # disallows /Notices.aspx and /Search/Details/.
    {"key": "memphis_daily_news", "base": "https://www.memphisdailynews.com", "weekdays": {0, 1, 2, 3, 4},
     "lookback_days": 10, "edition_path": "/notices/{year}/{month}/{day}/"},
]
# tnledger.com is not listed: see _source_access.STOPPED["court_notices:nashville_ledger"].


def edition_url(pub: Dict[str, Any], d: date) -> str:
    return pub["base"] + pub["edition_path"].format(year=d.year, month=MONTHS[d.month - 1], day=d.day)


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
    estate_fields = None
    estate_review = None
    if kind == cn.KIND_PROBATE or re.search(r"\bestate of\b", " ".join(row["lines"]), re.I):
        estate_fields = cn.extract_probate(row["lines"])
        estate_review = cn.probate_review(row["lines"], estate_fields)
        if kind == cn.KIND_OTHER and estate_fields["decedent"] and estate_review["stage"] == "appointment_reported":
            kind = cn.KIND_PROBATE
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
        fields = estate_fields
        if not fields["decedent"]:
            return None  # a probate lead without a decedent can't be matched to property
        raw["court_notice"] = fields
        raw["probate_review"] = estate_review
        reps = ", ".join(f"{r['name']} ({r['role']})" for r in fields["representatives"]) or "none parsed"
        attorney = (", ".join(fields["attorney"]["names"]) or "none parsed").rstrip(".")
        source, ident = lead_key(kind, fields["county"], cn.historic_probate_docket(row["lines"]), row["notice_id"])
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
    if estate_review and estate_fields["decedent"] and not partition:
        raw["probate_review"] = estate_review
        raw["court_notice"] = {**estate_fields, **fields, "docket": fields["docket"] or estate_fields["docket"]}
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


def cheatham_trustee_tracts(notice, today, staged_dockets):
    """Tracts from the County Trustee's posted sale notice (as parsed by
    cheatham_tax_sale_bot.read_notice), for a case not staged yet. Same
    record shape as the tracts once read from the newspaper copy."""
    if not notice or str(notice["docket"]) in {str(d) for d in staged_dockets}:
        return []
    if notice["sale_date"] < today.isoformat():
        return []
    leads = []
    for tract in notice["tracts"]:
        location, owners = tract.get("location"), tract.get("owners") or ""
        leads.append(LeadPayload(
            bot_source="court_notices", county="cheatham", distress_type="TAX_LIEN",
            pipeline_lead_key=BotBase.make_lead_key("cheatham_tax", f"{notice['docket']}|trustee_notice|{tract['letter']}"),
            property_address=location if location and re.match(r"\d+\s", location) else None,
            owner_name_records=None if not owners or owners.lower().startswith("unknown") else owners,
            trustee_sale_date=notice["sale_date"],
            source_url=notice["source_url"],
            raw_payload={"kind": "cheatham_tax_sale", "docket": notice["docket"], "notice_id": "trustee_notice",
                         "tract": tract["letter"], "parcel_printed": tract["parcel_printed"],
                         "location_printed": location, "owners_printed": owners,
                         "published_judgment": tract["amount"], "publication": "cheatham_county_trustee",
                         "notice_sha256": notice["sha256"],
                         "publication_date": notice.get("document_date") or today.isoformat()},
        ))
    return leads


class CourtNoticesBot(ProbateNoticeBot):
    name = "court_notices"
    description = ("Probate and court-ordered-sale notices from the Memphis Daily News; "
                   "Cheatham tax sale tracts from the County Trustee's posted notice")
    throttle_seconds = 1.5
    expected_min_yield = 1
    # Estates rarely change, so identical output across a few runs is normal;
    # flag only a long freeze.
    frozen_after_runs = 10

    def _cheatham(self, today: date) -> List[LeadPayload]:
        """New-case tracts from the Trustee's posted notice. An unreadable
        notice or an unreachable staging table stages nothing."""
        from ._base import _supabase
        from .cheatham_tax_sale_bot import NOTICE, read_notice
        client = _supabase()
        if client is None:
            return []
        res = self.fetch(NOTICE)
        if res is None or res.status_code != 200 or not res.content.startswith(b"%PDF"):
            self.logger.warning("Cheatham Trustee notice not read; no tracts staged from it this run")
            return []
        try:
            notice = read_notice(res.content)
            staged = (client.table("homeowner_requests_staging").select("docket:raw_payload->>docket")
                      .eq("bot_source", "court_notices").eq("county", "cheatham").eq("distress_type", "TAX_LIEN")
                      .eq("raw_payload->>kind", "cheatham_tax_sale").limit(1000).execute().data or [])
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Cheatham Trustee notice not usable (%s)", type(exc).__name__)
            return []
        return cheatham_trustee_tracts(notice, today, {r.get("docket") for r in staged})

    def scrape(self) -> List[LeadPayload]:
        today = datetime.now(timezone.utc).date()
        leads: Dict[str, LeadPayload] = {}
        empty_editions: List[str] = []
        for pub in PUBLICATIONS:
            for d in edition_dates(today, pub["weekdays"], pub["lookback_days"]):
                url = edition_url(pub, d)
                res = self.fetch(url)
                if res is None or res.status_code != 200:
                    self.logger.warning(f"{pub['key']} {d}: fetch failed")
                    continue
                rows = cn.parse_court_rows(res.text)
                if not rows:
                    empty_editions.append(f"{pub['key']} {d}")
                    continue
                for row in rows:
                    # The edition page is the page that was read, so it is the source.
                    lead = build_lead(self.name, pub["key"], pub["base"], d, row, source_url=url)
                    if lead:
                        remember_notice(leads, lead)
        for tract in self._cheatham(today):
            leads.setdefault(tract.pipeline_lead_key, tract)
        if empty_editions:
            # A publication day with no court grid at all is either a quiet day
            # or a layout change; the frozen/zero-yield health checks catch a
            # persistent one.
            self.logger.info(f"editions without a court grid: {', '.join(empty_editions)}")
        probate = sum(1 for l in leads.values() if l.distress_type == "PROBATE")
        self.logger.info(f"{probate} probate + {len(leads) - probate} court-sale and tax notices")
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
