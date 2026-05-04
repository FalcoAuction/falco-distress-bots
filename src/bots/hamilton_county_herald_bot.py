"""
Hamilton County Herald — Chattanooga's legal-notice publication.

Hamilton is one of the 9 EXTERNAL TPAD counties (no AVM coverage)
and the 4th-largest TN county by population. Hamilton County Herald
is a Catalis-era weekly publication carrying foreclosure +
probate/court notices. Combined with hamilton_tax_delinquent_bot
(which covers tax-lien delinquency), this scraper closes Hamilton's
PRE_FORECLOSURE + PROBATE coverage gap.

Index page: /PublicNotices.aspx (no date param — shows current week)
Index page (historical): /Notices.aspx?date=M/D/YYYY  (FUTURE — same
  pattern as the older URL format in earlier issues, but PublicNotices
  is the current canonical landing)
Detail page: /ViewNotice.aspx?id={NUMERIC}&date=M/D/YYYY

Notable structural difference from Nashville Ledger / Memphis Daily
News (sister Catalis-era pubs):
  - All notices use OpenChild(id, date) NOT OpenChildFT2 — unified
    index without ID-prefix type discrimination
  - Notice IDs are numeric only (e.g. "53233") — no FL/CL/FN/CD prefix
  - Section headers in the index HTML classify by type:
      <h3>Foreclosures</h3>   -> IDs that follow are foreclosures
      <h3>Courts</h3>          -> IDs that follow are court/probate
      <h3>Miscellaneous</h3>   -> bid notices, ordinances, etc (skip)
  - Detail page exposes section title via <span id="lblTitle"> and
    body via <span id="lblBody"> (single span, not the lbl1..lbl11
    structured fields the Nashville Ledger uses)

So we walk the index, group IDs by section, then fetch detail pages
only for Foreclosures + Courts sections. Body parsing reuses the same
regex patterns as Nashville Ledger / Memphis Daily News.

Distress types: PRE_FORECLOSURE + PROBATE (per section)
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload


HCH_BASE = "https://www.hamiltoncountyherald.com"
HCH_INDEX = HCH_BASE + "/PublicNotices.aspx"
HCH_INDEX_DATED = HCH_BASE + "/Notices.aspx"
HCH_DETAIL = HCH_BASE + "/ViewNotice.aspx"

OPENCHILD_RE = re.compile(r"OpenChild\('(\d+)','([^']+)'\)")

# Notice body extraction (same as Nashville Ledger)
LENDER_RE = re.compile(r"payable to the order of\s+([^.]+?)\.", re.IGNORECASE)
LENDER_ALT_RE = re.compile(r"the holder of the (?:note|debt) is\s+([^.,]+)", re.IGNORECASE)
PARCEL_RE = re.compile(
    r"(?:Tax Parcel ID|MAP AND PARCEL|TAX MAP|PARCEL)\s*(?:NO\.?|NUMBER|ID)?[:\s]+([A-Z0-9][A-Z0-9\- \.]*\d[A-Z0-9\-\. ]*)",
    re.IGNORECASE,
)
PRINCIPAL_RE = re.compile(r"original principal (?:amount|sum) of \$?([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
DELINQUENT_AMOUNT_RE = re.compile(r"entire amount delinquent[^$]*\$([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
COMMONLY_KNOWN_RE = re.compile(r"Commonly known as[:\s]+([^<\n]+?)(?:\.|<|\n|$)", re.IGNORECASE)
BORROWER_RE = re.compile(r"executed by\s+([A-Z][A-Za-z. ,'\-]+?),", re.IGNORECASE)

# Probate body extraction
DECEDENT_RE = re.compile(r"Estate of\s+([A-Z][A-Za-z\.\-' ]+?),\s*Deceased", re.IGNORECASE)
DOCKET_RE = re.compile(r"(?:Probate )?Docket No\.?:?\s*([A-Z0-9\-]+)", re.IGNORECASE)
DOD_RE = re.compile(r"who died on\s+([A-Z][a-z]+\.?\s+\d{1,2}[, ]+\d{4}|\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE)


class HamiltonCountyHeraldBot(BotBase):
    name = "hamilton_county_herald"
    description = "Chattanooga Hamilton County Herald foreclosure + probate notices"
    throttle_seconds = 1.5
    expected_min_yield = 3

    weeks_to_scan = 4

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        for pub_date in self._recent_friday_dates(self.weeks_to_scan):
            sections = self._fetch_index_dated(pub_date)
            self.logger.info(
                f"{pub_date.isoformat()}: "
                f"{len(sections.get('Foreclosures', []))} foreclosures, "
                f"{len(sections.get('Courts', []))} court/probate"
            )
            for nid in sections.get("Foreclosures", []):
                if nid in seen_ids:
                    continue
                seen_ids.add(nid)
                lead = self._build_lead(nid, pub_date, "Foreclosures")
                if lead is not None:
                    leads.append(lead)
            for nid in sections.get("Courts", []):
                if nid in seen_ids:
                    continue
                seen_ids.add(nid)
                lead = self._build_lead(nid, pub_date, "Courts")
                if lead is not None:
                    leads.append(lead)
        return leads

    @staticmethod
    def _recent_friday_dates(weeks: int) -> List[date]:
        today = date.today()
        days_since_friday = (today.weekday() - 4) % 7
        latest_friday = today - timedelta(days=days_since_friday)
        return [latest_friday - timedelta(days=7 * i) for i in range(weeks)]

    def _fetch_index_dated(self, pub_date: date) -> Dict[str, List[str]]:
        """GET PublicNotices.aspx (or Notices.aspx?date=...) and split notice
        IDs by <h3> section header.

        BUG-FIX 2026-05-04: HCH's `Notices.aspx?date=...` returns HTTP
        200 with an empty 9KB shell on most queries (probably needs
        cookies / postback state). The bot's old logic never reached the
        fallback because it only triggered on non-200. Now we ALSO fall
        back when the dated response parses to zero sections — recovers
        Hamilton Herald to ~5-10 leads/week.
        """
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        # Try the dated URL first
        res = self.fetch(HCH_INDEX_DATED, params={"date": date_param})
        sections: Dict[str, List[str]] = {}
        if res is not None and res.status_code == 200:
            sections = self._parse_index_sections(res.text)
        # If the dated query returned nothing (empty shell, often the case),
        # fall back to the undated landing page which lists current notices.
        if not sections:
            res2 = self.fetch(HCH_INDEX)
            if res2 is not None and res2.status_code == 200:
                sections = self._parse_index_sections(res2.text)
        return sections

    @staticmethod
    def _parse_index_sections(html: str) -> Dict[str, List[str]]:
        """Walk the HTML, splitting OpenChild IDs by the most-recent <h3>
        section header above them."""
        sections: Dict[str, List[str]] = {}
        # Approach: iterate sections by walking <h3>...</h3>...next <h3> chunks
        # then within each chunk extract OpenChild IDs.
        chunks = re.split(r"<h3[^>]*>([^<]+)</h3>", html)
        # chunks pattern: [pre, header1, body1, header2, body2, ...]
        seen_per_section: Dict[str, set] = {}
        for i in range(1, len(chunks), 2):
            header = chunks[i].strip()
            body = chunks[i + 1] if i + 1 < len(chunks) else ""
            # Normalize headers: "Foreclosures" / "Courts" / "Miscellaneous" / etc
            section_key = header.strip()
            ids: List[str] = []
            seen = seen_per_section.setdefault(section_key, set())
            for m in OPENCHILD_RE.finditer(body):
                nid = m.group(1)
                if nid in seen:
                    continue
                seen.add(nid)
                ids.append(nid)
            if ids:
                sections.setdefault(section_key, []).extend(ids)
        return sections

    def _build_lead(self, nid: str, pub_date: date, section: str) -> Optional[LeadPayload]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self.fetch(HCH_DETAIL, params={"id": nid, "date": date_param})
        if res is None or res.status_code != 200:
            return None
        soup = BeautifulSoup(res.text, "html.parser")
        title_span = soup.select_one("span#lblTitle")
        body_span = soup.select_one("span#lblBody")
        if not body_span:
            return None
        title = title_span.get_text(strip=True) if title_span else section
        body = body_span.get_text(" ", strip=True)
        if not body:
            return None

        if section == "Foreclosures":
            return self._build_foreclosure_lead(nid, pub_date, title, body, res.url)
        elif section == "Courts":
            return self._build_probate_lead(nid, pub_date, title, body, res.url)
        return None

    # ── Foreclosure lead ────────────────────────────────────────────────────

    def _build_foreclosure_lead(self, nid: str, pub_date: date, title: str,
                                  body: str, url: str) -> Optional[LeadPayload]:
        # Borrower
        m = BORROWER_RE.search(body)
        borrower = m.group(1).strip() if m else None
        # Address ("Commonly known as: 7711 E. Village Lane, Hixson, TN 37343")
        m = COMMONLY_KNOWN_RE.search(body)
        property_address = m.group(1).strip() if m else None
        if not property_address:
            return None
        # Parcel
        m = PARCEL_RE.search(body)
        parcel = m.group(1).strip() if m else None
        # Lender
        m = LENDER_RE.search(body) or LENDER_ALT_RE.search(body)
        lender = m.group(1).strip() if m else None
        # Principal
        m = PRINCIPAL_RE.search(body)
        principal = float(m.group(1).replace(",", "")) if m else None
        # Delinquent amount
        m = DELINQUENT_AMOUNT_RE.search(body)
        delinquent = float(m.group(1).replace(",", "")) if m else None

        admin_parts = [f"Hamilton Co Herald {nid}", f"pub {pub_date.isoformat()}"]
        if lender:
            admin_parts.append(f"lender={lender}")
        if parcel:
            admin_parts.append(f"parcel={parcel}")
        if delinquent:
            admin_parts.append(f"delinquent=${delinquent:,.0f}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, f"FL-{nid}"),
            property_address=property_address,
            county="hamilton",
            full_name=borrower,
            owner_name_records=borrower,
            mortgage_balance=principal or delinquent,
            distress_type="PRE_FORECLOSURE",
            admin_notes=" · ".join(admin_parts),
            source_url=url,
            raw_payload={
                "publication": "hamilton_county_herald",
                "title": title,
                "borrower": borrower,
                "property_address": property_address,
                "parcel": parcel,
                "lender": lender,
                "original_principal": principal,
                "delinquent_amount": delinquent,
                "publication_date": pub_date.isoformat(),
                "body": body,
            },
        )

    # ── Probate lead ────────────────────────────────────────────────────────

    def _build_probate_lead(self, nid: str, pub_date: date, title: str,
                             body: str, url: str) -> Optional[LeadPayload]:
        # Skip non-probate court notices (custody, civil suits, etc)
        if not re.search(r"NOTICE TO CREDITORS|Letters of (?:Administration|Testamentary|Authority)",
                          body, re.IGNORECASE):
            return None
        m = DECEDENT_RE.search(body)
        if not m:
            return None
        decedent = m.group(1).strip().title()
        m = DOCKET_RE.search(body)
        docket = m.group(1) if m else None
        m = DOD_RE.search(body)
        dod_raw = m.group(1) if m else None

        admin_parts = [f"Hamilton Co Herald {nid}", f"pub {pub_date.isoformat()}", "court=hamilton"]
        if docket:
            admin_parts.append(f"docket={docket}")
        if dod_raw:
            admin_parts.append(f"dod={dod_raw}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, f"CL-{nid}"),
            full_name=decedent,
            owner_name_records=decedent,
            county="hamilton",
            distress_type="PROBATE",
            admin_notes=" · ".join(admin_parts),
            source_url=url,
            raw_payload={
                "publication": "hamilton_county_herald",
                "title": title,
                "decedent": decedent,
                "county": "hamilton",
                "docket": docket,
                "date_of_death_raw": dod_raw,
                "publication_date": pub_date.isoformat(),
                "body": body,
            },
        )


def run() -> dict:
    bot = HamiltonCountyHeraldBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
