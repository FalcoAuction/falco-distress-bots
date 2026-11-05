"""Legal notices from Main Street Media of Tennessee papers (mainstreetmediatn.com).

One publisher prints the legal notices for 12 papers covering 9 of the 10
Middle Tennessee focus counties (all but Williamson). Each weekly issue is one
WordPress post with the notices in full text, one <p> per notice, separated by
<hr>. No login, no CAPTCHA; robots.txt blocks only Meta's crawlers and
/wp-admin/. Verified 2026-11-05.

From each issue:
  - PROBATE: notices to creditors, parsed with the same extractor as the
    Ledger (decedent, docket, representatives, attorney). Staged under
    bot_source "court_notices" with the same county + docket key, so an
    estate printed in both the Ledger and a Main Street paper is one lead.
  - COURT_SALE: Clerk and Master, partition, judicial and special
    commissioner sales.
  - TAX_LIEN: Chancery delinquent-tax suit and tax sale notices. These print
    in three flattened columns, so owners cannot be paired with parcels
    reliably; one lead per map/parcel number is staged with the tax years,
    and owner and address come later from county parcel data.

Trustee (foreclosure) sale notices are left to the trustee and Ledger bots.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload
from . import _court_notices as cn
from .court_notices_bot import build_lead

HUB = "https://mainstreetmediatn.com/bargain-browser/public-notices/"
POST_RE = re.compile(
    r'href="(https://mainstreetmediatn\.com/articles/bargainbrowser/public-notices-([a-z0-9-]+?)-'
    r'(january|february|march|april|may|june|july|august|september|october|november|december)-(\d{2})-(\d{4})/)[^"]*"')
MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}

# Paper slug -> county. Papers outside the 10 focus counties (Hickman,
# Houston, Lawrence, Pulaski) are skipped.
PAPERS: Dict[str, str] = {
    "main-street-nashville": "davidson",
    "murfreesboro-post": "rutherford",
    "gallatin-news": "sumner",
    "hendersonville-standard": "sumner",
    "portland-sun": "sumner",
    "wilson-post": "wilson",
    "chronicle-of-mt-juliet": "wilson",
    "main-street-maury": "maury",
    "main-street-clarksville": "montgomery",
    "cheatham-county-exchange": "cheatham",
    "robertson-county-connection": "robertson",
    "dickson-post": "dickson",
}

LOOKBACK_DAYS = 21

# Tennessee map/parcel numbers as printed: "23-17.01", "111-EB-14.00",
# "82-53.00 001" (special interest), "11B/111G-A-21.00". Requiring a tax
# year right after keeps street numbers and dates out.
PARCEL_YEAR_RE = re.compile(
    r"\b(\d{1,3}[A-Z]?(?:/\d{1,3}[A-Z]?)?-(?:[A-Z]{1,2}-)?\d{1,3}\.\d{2})(?:\s+(\d{3}))?\s+((?:19|20)\d\d)\b")
LABELED_PARCEL_RE = re.compile(
    r"MAP:\s*(\d{1,3}(?:\s?[A-Z])?)\s+(?:GROUP:\s*([A-Z]{1,2})\s+)?PARCEL:\s*(\d{1,3}\.\d{2})", re.I)
TAX_NOTICE_RE = re.compile(
    r"chancery court.*?(?:tax notice|delinquent tax|tax sale|back taxes|delinquent property tax)", re.I | re.S)


def issue_links(hub_html: str, today: date, lookback_days: int = LOOKBACK_DAYS) -> List[Tuple[str, str, date]]:
    """(url, county, issue date) for focus-county issues inside the lookback."""
    out, seen = [], set()
    for url, paper, month, day, year in POST_RE.findall(hub_html):
        county = PAPERS.get(paper)
        if not county or url in seen:
            continue
        try:
            issued = date(int(year), MONTHS[month], int(day))
        except ValueError:
            continue
        if (today - issued).days > lookback_days or issued > today + timedelta(days=7):
            continue
        seen.add(url)
        out.append((url, county, issued))
    return out


def notices(post_html: str) -> List[List[str]]:
    """Each notice as its list of non-empty lines. Notices are separated by
    <hr>; one notice can span several <p> (Robertson judicial sales)."""
    body = BeautifulSoup(post_html, "html.parser").select_one(".entry-content")
    if body is None:
        return []
    out: List[List[str]] = []
    current: List[str] = []
    for el in body.find_all(["p", "hr"]):
        if el.name == "hr":
            if current:
                out.append(current)
            current = []
            continue
        current.extend(ln.strip() for ln in el.get_text("\n").split("\n") if ln.strip())
    if current:
        out.append(current)
    return out


def tax_parcels(lines: List[str]) -> List[Dict[str, Any]]:
    """Map/parcel numbers with their tax years from a chancery tax notice."""
    text = " ".join(lines)
    if not TAX_NOTICE_RE.search(text):
        return []
    found: Dict[str, Dict[str, Any]] = {}
    # Wilson: "MAP: 128 A GROUP: B PARCEL: 21.00" -> "128A-B-21.00" (no tax year printed)
    for map_no, group, parcel in LABELED_PARCEL_RE.findall(text):
        pid = "-".join(x for x in (map_no.replace(" ", ""), group, parcel) if x)
        found.setdefault(pid, {"parcel": pid, "tax_years": []})
    for parcel, special, year in PARCEL_YEAR_RE.findall(text):
        pid = f"{parcel} {special}" if special else parcel
        found.setdefault(pid, {"parcel": pid, "tax_years": []})
        if year not in found[pid]["tax_years"]:
            found[pid]["tax_years"].append(year)
    return list(found.values())


class MainStreetMediaBot(BotBase):
    name = "main_street_media"
    description = "Probate, court-sale and tax-suit notices from Main Street Media papers (9 Middle TN counties)"
    throttle_seconds = 2.0
    expected_min_yield = 1
    frozen_after_runs = 10

    def scrape(self) -> List[LeadPayload]:
        today = datetime.now(timezone.utc).date()
        hub = self.fetch(HUB)
        if hub is None or hub.status_code != 200:
            self.logger.warning("hub fetch failed")
            return []
        leads: Dict[str, LeadPayload] = {}
        counts: Dict[str, int] = {}
        for url, county, issued in issue_links(hub.text, today):
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                self.logger.warning(f"{url}: fetch failed")
                continue
            for i, lines in enumerate(notices(res.text)):
                for lead in self._leads(url, county, issued, i, lines):
                    leads.setdefault(lead.pipeline_lead_key, lead)
                    counts[lead.distress_type] = counts.get(lead.distress_type, 0) + 1
        self.logger.info(f"parsed {len(leads)} leads from Main Street Media: {counts}")
        return list(leads.values())

    def _leads(self, url: str, county: str, issued: date, index: int, lines: List[str]) -> List[LeadPayload]:
        notice_id = f"msm:{url.rstrip('/').rsplit('/', 1)[-1]}:{index}"
        kind = cn.classify(lines)
        if kind != cn.KIND_OTHER:
            lead = build_lead("court_notices", "main_street_media", "", issued,
                              {"notice_id": notice_id, "lines": lines}, source_url=url)
            if lead is None:
                return []
            # The paper's county when the notice text does not name one.
            if not lead.county:
                lead.county = county
                if isinstance(lead.raw_payload.get("court_notice"), dict):
                    lead.raw_payload["court_notice"]["county"] = county
            lead.raw_payload["paper_county"] = county
            return [lead]
        out = []
        for parcel in tax_parcels(lines):
            out.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key("msm_tax", f"{county}|{parcel['parcel']}"),
                county=county,
                distress_type="TAX_LIEN",
                admin_notes=f"Chancery delinquent tax notice, {county.title()} County, parcel {parcel['parcel']}, "
                            f"tax years {', '.join(parcel['tax_years'])}.",
                raw_payload={"publication": "main_street_media", "publication_date": issued.isoformat(),
                             "kind": "chancery_tax_notice", "parcel": parcel["parcel"],
                             "tax_years": parcel["tax_years"], "notice_id": notice_id,
                             "headline": " ".join(lines[:3])[:300]},
                source_url=url,
            ))
        return out


def run() -> dict:
    return MainStreetMediaBot().run()


if __name__ == "__main__":
    print(run())
