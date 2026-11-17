"""Tennessee WARN notices: employers announcing a plant closure or mass layoff
(50+ workers), published by the TN Department of Labor and Workforce
Development as a plain HTML table.

Why: a closing business often liquidates equipment, vehicles, inventory and
sometimes its building, which is auction inventory for the non-real-estate
lanes (business_equipment, vehicles_fleet, commercial_real_property). A
layoff is a weaker signal than a closure; both are staged and the notice
type is kept so a reviewer can tell them apart.

Source: https://www.tn.gov/workforce/general-resources/major-publications0/major-publications-redirect/reports.html
robots.txt allows everything. The site 403s generic library user agents but
serves an identified bot ("compatible; FALCO-Bot"), which is what we send;
nothing here impersonates a browser. Verified 2026-11-05.

Only the 10 Middle Tennessee focus counties are staged. No property address
is published; the company and county are enough for a reviewer to look the
business up. Distress type BUSINESS_CLOSURE.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload

WARN_URL = "https://www.tn.gov/workforce/general-resources/major-publications0/major-publications-redirect/reports.html"
FOCUS_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson",
                  "maury", "montgomery", "cheatham", "robertson", "dickson"}
LOOKBACK_DAYS = 365

_DATE_RE = re.compile(r"(\d{1,2})[/-](\d{1,2})[/-](\d{2,4})")


def _date(value: str) -> Optional[str]:
    m = _DATE_RE.search(value or "")
    if not m:
        return None
    month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if year < 100:
        year += 2000
    try:
        return date(year, month, day).isoformat()
    except ValueError:
        return None


def parse_warn(page_html: str) -> List[Dict[str, Any]]:
    """Every row of every WARN table, keyed by normalized header."""
    soup = BeautifulSoup(page_html, "html.parser")
    out: List[Dict[str, Any]] = []
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if not rows:
            continue
        headers = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)).lower() for c in rows[0].find_all(["th", "td"])]
        if "company" not in headers or "county" not in headers:
            continue
        for tr in rows[1:]:
            cells = [re.sub(r"\s+", " ", c.get_text(" ", strip=True)) for c in tr.find_all(["td", "th"])]
            if len(cells) != len(headers):
                continue
            rec = dict(zip(headers, cells))
            workers = re.sub(r"[^\d]", "", rec.get("affected workers", ""))
            out.append({
                "posted": _date(rec.get("date of posting", "")),
                "company": rec.get("company", "").strip(),
                "county": rec.get("county", "").strip(),
                "workers": int(workers) if workers else None,
                "effective": _date(rec.get("closure/layoff date", "")),
                "effective_raw": rec.get("closure/layoff date", ""),
                "notice": rec.get("notice/type", "").strip(),
            })
    return [r for r in out if r["company"]]


def _counties(value: str) -> List[str]:
    """'Davidson / Williamson' or 'Davidson, Rutherford' -> each county."""
    return [c.strip().lower().replace(" county", "") for c in re.split(r"[/,&]| and ", value) if c.strip()]


class WarnNoticesBot(BotBase):
    name = "warn_notices"
    description = "TN WARN closure and mass-layoff notices, Middle TN focus counties"
    throttle_seconds = 2.0
    user_agent = "Mozilla/5.0 (compatible; FALCO-Bot/1.0; +https://falco.llc)"
    expected_min_yield = 0  # a quarter with no Middle TN notices is normal
    frozen_after_runs = 60

    def scrape(self) -> List[LeadPayload]:
        res = self.fetch(WARN_URL)
        if res is None or res.status_code != 200:
            self.logger.warning(f"WARN fetch failed: {getattr(res, 'status_code', None)}")
            return []
        cutoff = (datetime.now(timezone.utc).date() - timedelta(days=LOOKBACK_DAYS)).isoformat()
        leads: List[LeadPayload] = []
        for rec in parse_warn(res.text):
            if rec["posted"] and rec["posted"] < cutoff:
                continue
            counties = [c for c in _counties(rec["county"]) if c in FOCUS_COUNTIES]
            for county in counties:
                ident = rec["notice"] or f"{rec['company']}|{rec['posted']}"
                leads.append(LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=self.make_lead_key("warn", f"{ident}|{county}"),
                    county=county,
                    full_name=rec["company"],
                    distress_type="BUSINESS_CLOSURE",
                    admin_notes=f"WARN notice {rec['notice'] or '?'}: {rec['company']}, {rec['workers'] or '?'} workers, "
                                f"effective {rec['effective'] or rec['effective_raw'] or '?'} (posted {rec['posted'] or '?'}).",
                    raw_payload={"warn": rec, "source": "tn_dol_warn"},
                    source_url=WARN_URL,
                ))
        self.logger.info(f"{len(leads)} Middle TN WARN notices in the last {LOOKBACK_DAYS} days")
        return leads


def run() -> dict:
    return WarnNoticesBot().run()


if __name__ == "__main__":
    print(run())
