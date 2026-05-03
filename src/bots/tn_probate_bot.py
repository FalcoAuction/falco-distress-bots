"""
TN Probate scraper — walks the Nashville Ledger weekly Court Notice
publication and extracts probate "Notice to Creditors" filings.

Probate is THE missing high-signal lead category: a recently-deceased
homeowner means an executor/heir who often wants the property
liquidated quickly to settle the estate. The Ledger publishes these
notices for Middle TN counties (Davidson, Cheatham, Robertson,
Williamson, Wilson, Rutherford, Sumner, etc) under the same Friday
publication used by the foreclosure scraper.

Detail URL: /Search/Details/ViewNotice.aspx?id=CL{NNNNNN}&date=M/D/YYYY

Field extraction (regex on full notice body):
  - probate court / county
  - docket number (P-NNNN style)
  - decedent name (Estate of {NAME}, Deceased)
  - date of death (when stated)
  - date letters issued
  - personal representative (Administrator / Executor)
  - attorney + their mailing address
  - clerk + chief deputy clerk

The PROPERTY ADDRESS for the decedent's estate is NOT in the notice
itself — that has to be cross-referenced via county assessor lookup
by owner name. davidson_assessor_bot already supports owner search
(SelectedSearch=1); a downstream enricher can wire that connection.
For v1 we stage the lead with full_name=decedent and admin_notes
documenting the probate context.

Distress type: PROBATE
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload


LEDGER_BASE = "https://www.tnledger.com"
INDEX_URL = LEDGER_BASE + "/Notices.aspx"
DETAIL_URL = LEDGER_BASE + "/Search/Details/ViewNotice.aspx"

CL_ID_RE = re.compile(r"OpenChildFT2\('(CL\d+)','([^']+)'\)")

# Body-text patterns
PROBATE_FLAG_RE = re.compile(
    r"(?:NOTICE TO CREDITORS|PROBATE COURT|PROBATE DIVISION|Letters of (?:Administration|Testamentary|Authority))",
    re.IGNORECASE,
)
# Multiple county patterns to cover Davidson + outer-county formats
COUNTY_PATTERNS = (
    re.compile(r"PROBATE COURT OF\s+([A-Z][A-Z]+(?:\s+[A-Z]+)*)\s+COUNTY", re.IGNORECASE),
    re.compile(r"Probate Court of\s+([A-Za-z]+)\s+County", re.IGNORECASE),
    re.compile(r"Circuit Court of\s+([A-Za-z]+)\s+County[^.]*Probate", re.IGNORECASE),
    re.compile(r"([A-Za-z]+)\s+County[, ]+Tennessee[, ]+Probate Division", re.IGNORECASE),
    re.compile(r"Chancery Court of\s+([A-Za-z]+)\s+County", re.IGNORECASE),
)
DOCKET_RE = re.compile(r"(?:Probate )?Docket No\.?:?\s*([A-Z0-9\-]+)", re.IGNORECASE)
DECEDENT_RE = re.compile(r"Estate of\s+([A-Z][A-Za-z\.\-' ]+?),\s*Deceased", re.IGNORECASE)
# Two date styles: "Nov. 26, 2025" or "12/28/2025"
DOD_PATTERNS = (
    re.compile(r"who died on\s+([A-Z][a-z]+\.?\s+\d{1,2}[, ]+\d{4})", re.IGNORECASE),
    re.compile(r"who died on\s+(\d{1,2}/\d{1,2}/\d{4})", re.IGNORECASE),
)
# Davidson omits year inline ("on the 23rd day of April"); the year is in a
# separate "This 23rd day of April, 2026." statement near the end.
LETTERS_PATTERNS = (
    re.compile(
        r"on the\s+(\d{1,2})(?:st|nd|rd|th)?\s+day of\s+([A-Z][a-z]+),?\s+(\d{4}),?\s+Letters of",
        re.IGNORECASE,
    ),
    re.compile(
        r"on the\s+(\d{1,2})(?:st|nd|rd|th)?\s+day of\s+([A-Z][a-z]+),?\s+Letters of",
        re.IGNORECASE,
    ),
)
THIS_DAY_YEAR_RE = re.compile(
    r"This\s+(?:the\s+)?\d{1,2}(?:st|nd|rd|th)?\s+day of\s+[A-Z][a-z]+,?\s+(\d{4})",
    re.IGNORECASE,
)
# Two executor patterns: "{NAME}\nPersonal Representative(s)" (Davidson) or
# "{NAME}\nAdministrator/Executor of the Estate" (outer counties)
EXECUTOR_PATTERNS = (
    re.compile(
        r"([A-Z][A-Za-z\.\-' ]+(?:\s[A-Z][A-Za-z\.\-' ]+)+)\s*\n+(?:[\w ,.]+\n+)*?(?:Personal Representative|Administrator(?:\s+ad\s+Litem)?|Executor|Executrix|Co-?Administrator|Co-?Executor)",
        re.IGNORECASE,
    ),
)
# Davidson: attorney name on a line BEFORE "Attorney for Personal Representative(s)"
# Outer counties: "Attorney: {NAME}" inline
ATTORNEY_PATTERNS = (
    re.compile(r"Attorney:\s+([A-Z][A-Za-z\.\-' ]+(?:\s[A-Z][A-Za-z\.\-' ]+)*)"),
    re.compile(
        r"\n([A-Z][A-Z\.\-' ]+(?:\s[A-Z\.\-' ]+)+)\s*\n+(?:[\w ,.]+\n+)*?Attorney for Personal Representative",
        re.IGNORECASE,
    ),
)


class TnProbateBot(BotBase):
    name = "tn_probate"
    description = "TN probate Notice to Creditors via Nashville Ledger CL notices"
    throttle_seconds = 1.5
    expected_min_yield = 5  # typical Friday: 50-80 court notices, most probate

    weeks_to_scan = 4

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        for pub_date in self._recent_friday_dates(self.weeks_to_scan):
            ids = self._fetch_index(pub_date)
            self.logger.info(f"{pub_date.isoformat()}: {len(ids)} court-notice IDs")
            for cl_id in ids:
                if cl_id in seen_ids:
                    continue
                seen_ids.add(cl_id)
                detail = self._fetch_detail(cl_id, pub_date)
                if detail is None:
                    continue
                lead = self._build_lead(detail, cl_id, pub_date)
                if lead is not None:
                    leads.append(lead)
        return leads

    # ── Index walk ──────────────────────────────────────────────────────────

    @staticmethod
    def _recent_friday_dates(weeks: int) -> List[date]:
        today = date.today()
        days_since_friday = (today.weekday() - 4) % 7
        latest_friday = today - timedelta(days=days_since_friday)
        return [latest_friday - timedelta(days=7 * i) for i in range(weeks)]

    def _fetch_index(self, pub_date: date) -> List[str]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self.fetch(INDEX_URL, params={"noticesDate": date_param})
        if res is None or res.status_code != 200:
            return []
        ids = []
        seen: set[str] = set()
        for m in CL_ID_RE.finditer(res.text):
            cl_id = m.group(1)
            if cl_id not in seen:
                seen.add(cl_id)
                ids.append(cl_id)
        return ids

    # ── Detail fetch ────────────────────────────────────────────────────────

    def _fetch_detail(self, cl_id: str, pub_date: date) -> Optional[Dict]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self.fetch(DETAIL_URL, params={"id": cl_id, "date": date_param})
        if res is None or res.status_code != 200:
            return None
        return self._parse_detail(res.text)

    @staticmethod
    def _parse_detail(html: str) -> Dict:
        soup = BeautifulSoup(html, "html.parser")
        body_paras = soup.select("div#record-details > p")
        body = "\n\n".join(p.get_text(" ", strip=True) for p in body_paras)
        return {"body": body}

    # ── Lead construction ───────────────────────────────────────────────────

    def _build_lead(self, detail: Dict, cl_id: str, pub_date: date) -> Optional[LeadPayload]:
        body = detail.get("body") or ""
        if not PROBATE_FLAG_RE.search(body):
            return None  # skip non-probate court notices (some are guardianship etc)

        decedent_match = DECEDENT_RE.search(body)
        if not decedent_match:
            return None
        decedent = decedent_match.group(1).strip().title()

        # County
        county = None
        for pat in COUNTY_PATTERNS:
            m = pat.search(body)
            if m:
                county = m.group(1).strip().lower()
                break

        # Docket
        m = DOCKET_RE.search(body)
        docket = m.group(1) if m else None

        # Date of death
        dod = self._parse_dod(body)

        # Letters issuance date
        letters_iso = self._parse_letters_date(body)

        # Personal representative
        executor = None
        for pat in EXECUTOR_PATTERNS:
            m = pat.search(body)
            if m:
                executor = m.group(1).strip()
                break

        # Attorney
        attorney = None
        for pat in ATTORNEY_PATTERNS:
            m = pat.search(body)
            if m:
                attorney = m.group(1).strip()
                # Filter out the "for Personal Representative" false match
                if "for personal representative" in attorney.lower():
                    continue
                break

        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        source_url = f"{DETAIL_URL}?id={cl_id}&date={date_param}"

        admin_parts = [f"Nashville Ledger {cl_id}", f"pub {pub_date.isoformat()}"]
        if county:
            admin_parts.append(f"county={county}")
        if docket:
            admin_parts.append(f"docket={docket}")
        if executor:
            admin_parts.append(f"PR={executor}")
        if attorney:
            admin_parts.append(f"atty={attorney}")
        if dod:
            admin_parts.append(f"dod={dod}")
        if letters_iso:
            admin_parts.append(f"letters={letters_iso}")

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, cl_id),
            full_name=decedent,
            owner_name_records=decedent,
            county=county,
            distress_type="PROBATE",
            admin_notes=" · ".join(admin_parts),
            source_url=source_url,
            raw_payload={
                "decedent": decedent,
                "county": county,
                "docket": docket,
                "date_of_death": dod,
                "letters_issued": letters_iso,
                "personal_representative": executor,
                "attorney": attorney,
                "publication_date": pub_date.isoformat(),
                "body": body,
            },
        )

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_dod(body: str) -> Optional[str]:
        for pat in DOD_PATTERNS:
            m = pat.search(body)
            if not m:
                continue
            raw = m.group(1).replace(",", " ").replace(".", "").strip()
            for fmt in ("%b %d %Y", "%B %d %Y", "%m/%d/%Y"):
                try:
                    return datetime.strptime(raw, fmt).date().isoformat()
                except ValueError:
                    continue
        return None

    @staticmethod
    def _parse_letters_date(body: str) -> Optional[str]:
        # Try inline pattern first (year in same sentence)
        m = LETTERS_PATTERNS[0].search(body)
        if m:
            day, month_name, year = m.group(1), m.group(2), m.group(3)
            try:
                return datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y").date().isoformat()
            except ValueError:
                pass
        # Davidson style: year is separately stated in "This {N} day of {M}, {Y}"
        m = LETTERS_PATTERNS[1].search(body)
        year_m = THIS_DAY_YEAR_RE.search(body)
        if m and year_m:
            day, month_name, year = m.group(1), m.group(2), year_m.group(1)
            try:
                return datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y").date().isoformat()
            except ValueError:
                pass
        return None


def run() -> dict:
    bot = TnProbateBot()
    return bot.run()


if __name__ == "__main__":
    result = run()
    print(f"\nResult: {result}")
