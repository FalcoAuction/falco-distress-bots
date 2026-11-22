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

Tennessee worksites are staged statewide. A worksite alone does not establish ownership.
"""
from __future__ import annotations

import hashlib
import io
import re
from urllib.parse import urljoin, urlsplit
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload, _supabase
from ._warn_letter import letter_facts, review
from ._warn_refresh import refresh

WARN_URL = "https://www.tn.gov/workforce/general-resources/major-publications0/major-publications-redirect/reports.html"
from ._regions import TN_COUNTIES as FOCUS_COUNTIES  # noqa: E402
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
            anchor = tr.find_all(["td", "th"])[headers.index("company")].find("a", href=True)
            url = urljoin(WARN_URL, anchor["href"]) if anchor else ""
            letter = urlsplit(url)
            letter_url = url if letter.scheme == "https" and letter.hostname == "www.tn.gov" and letter.path.lower().endswith(".pdf") else None
            workers = rec.get("affected workers", "").replace(",", "").strip()
            out.append({
                "letter_url": letter_url,
                "posted": _date(rec.get("date of posting", "")),
                "company": rec.get("company", "").strip(),
                "county": rec.get("county", "").strip(),
                "workers": int(workers) if workers.isdigit() else None,
                "effective": _date(rec.get("closure/layoff date", "")),
                "effective_raw": rec.get("closure/layoff date", ""),
                "notice": rec.get("notice/type", "").strip(),
            })
    return [r for r in out if r["company"]]


def letter_review(text: str) -> Dict[str, Any]:
    """Extract employer event and worksite from the state memo, excluding its letterhead."""
    statement, event_type, sites, _ = letter_facts(text)
    category = "closure" if event_type.endswith("closure") else "layoff" if event_type.endswith("layoff") else "review"
    return {"event_type": category, "event_statement": statement,
            "worksite": sites[0] if len(sites) == 1 else None, "asset_disposition": "unverified"}


def _counties(value: str) -> List[str]:
    """'Davidson / Williamson' or 'Davidson, Rutherford' -> each county."""
    return [c.strip().lower().replace(" county", "") for c in re.split(r"[/,&]| and ", value) if c.strip()]


class WarnNoticesBot(BotBase):
    name = "warn_notices"
    description = "TN WARN closure and mass-layoff notices, statewide"
    throttle_seconds = 2.0
    user_agent = "Mozilla/5.0 (compatible; FALCO-Bot/1.0; +https://falco.llc)"
    expected_min_yield = 0  # a quarter with no Middle TN notices is normal
    frozen_after_runs = 60

    def scrape(self) -> List[LeadPayload]:
        self._complete = False
        res = self.fetch(WARN_URL, allow_redirects=False)
        if res is None or res.status_code != 200:
            raise RuntimeError("WARN index unavailable")
        records = parse_warn(res.text)
        today = datetime.now(timezone.utc).date()
        cutoff = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()
        groups = {}
        for rec in records:
            if not rec["posted"] or not cutoff <= rec["posted"] <= today.isoformat():
                continue
            for county in _counties(rec["county"]):
                if county not in FOCUS_COUNTIES:
                    continue
                ident = rec["notice"] or f"{rec['company']}|{rec['posted']}"
                group = groups.setdefault((ident, county), [])
                if rec not in group:
                    group.append(rec)
        if not groups:
            raise RuntimeError("WARN index returned no dated Tennessee notices")
        import pdfplumber
        letters = {}
        leads = []
        for (ident, county), group in groups.items():
            rec = dict(group[0])
            url = rec["letter_url"]
            if len(group) == 1 and url and url not in letters:
                text, digest = "", None
                response = self.fetch(url, timeout=30, allow_redirects=False)
                if response is not None and response.status_code == 200:
                    data = response.content
                    if data.startswith(b"%PDF") and len(data) <= 5_000_000:
                        digest = hashlib.sha256(data).hexdigest()
                        try:
                            with pdfplumber.open(io.BytesIO(data)) as doc:
                                if len(doc.pages) <= 15:
                                    text = " ".join(page.extract_text() or "" for page in doc.pages)
                        except Exception as exc:
                            self.logger.warning("Unreadable WARN letter %s (%s)", ident, type(exc).__name__)
                letters[url] = text, digest
            text, digest = letters.get(url, ("", None)) if len(group) == 1 else ("", None)
            result = review(rec, text, digest, today.isoformat())
            if len(group) > 1:
                result.update(verification="ambiguous_index", index_observations=group)
            category = result["event_type"]
            rec.update(event_type="closure" if category.endswith("closure") else
                       "layoff" if category.endswith("layoff") else "review",
                       event_statement=result["event_statement"], worksite=result["worksite"],
                       asset_disposition="unverified")
            leads.append(LeadPayload(
                bot_source=self.name, pipeline_lead_key=self.make_lead_key("warn", f"{ident}|{county}"),
                county=county, full_name=rec["company"], distress_type="BUSINESS_CLOSURE",
                admin_notes=f"WARN {ident} · {rec['company']} · employment notice posted {rec['posted']}",
                raw_payload={"warn": rec, "warn_review": result, "source": "tn_dol_warn"},
                source_url=url or WARN_URL))
        self._complete = True
        self.logger.info(f"{len(leads)} Tennessee WARN notices in the last {LOOKBACK_DAYS} days")
        return leads

    def _write_staging(self, leads):
        if not getattr(self, "_complete", False):
            return 0, 0
        client = _supabase()
        if client is None:
            raise RuntimeError("WARN staging unavailable")
        return refresh(client, leads, self.fetch, datetime.now(timezone.utc).date(), LOOKBACK_DAYS, self.run_id)


def run() -> dict:
    return WarnNoticesBot().run()


if __name__ == "__main__":
    print(run())
