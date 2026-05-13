"""Mackie Wolf Zientz & Mann PC — TX-HQ substitute trustee firm with
heavy TN volume (~58 active TN sales as of 2026-05-12).

The firm publishes a "TN Sale Report" PDF at:
  https://mwzmlaw.com/wp-content/uploads/YYYY/MM/TN-Sale-Report-as-of-MM.DD.YYYY.pdf

The PDF refreshes weekly-to-biweekly (not daily despite the page copy).
We walk back from today and pick the most recent one available.

PDF structure (verified 2026-05-12, two-line listings):
  Line 1:  M/D/YYYY  FILE#  STREET   COUNTY   PLATFORM
  Line 2:  CITY TN ZIP

Where PLATFORM is one of:
  AUCTION   -> sale hosted on Auction.com
  HUBZU     -> sale hosted on Altisource's Hubzu
  HUDMARSH  -> sale routed through Hudson Marshall
  MWZM      -> Mackie Wolf in-house / courthouse sale

The platform tag is GOLD — it tells us at scrape-time which leads will
end up on Auction.com vs the courthouse, so we know which need the
auction-overlay framing and which are courthouse-only.

Cost in dialer terms: ~21 of the 58 listings (May 2026 snapshot) are
AUCTION-platform leads — that's roughly the auction.com TN visibility
gap Patrick flagged.

Borrower names / opening bid amounts are NOT exposed in this PDF —
owner enrichment is handled downstream by the assessor bots
(davidson_assessor, williamson_assessor, shelby_assessor, etc.).

Distress type: TRUSTEE_NOTICE.
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from ._base import BotBase, LeadPayload

# pdfplumber is in requirements.txt; lazy-import for clean module-load
# failure mode (so a missing dep doesn't break the whole bot orchestrator)
try:
    import pdfplumber  # type: ignore
except ImportError:
    pdfplumber = None  # type: ignore[assignment]


MWZM_PDF_BASE = "https://mwzmlaw.com/wp-content/uploads"

# TN counties that are multi-word (rare — only Van Buren currently)
# Used so our last-token parsing doesn't split them.
MULTI_WORD_COUNTIES = {"Van Buren"}

# Known platform tags from the PDF's "Sale Trustee" column.
KNOWN_PLATFORMS = {"AUCTION", "HUBZU", "HUDMARSH", "MWZM"}

# First line of a listing: M/D/YYYY  FILE#  STREET   COUNTY   PLATFORM
LISTING_HEAD_RE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{4})\s+(\S+)\s+(.+)$")
# Second line: CITY TN ZIP   (e.g. "Memphis TN 38116" or "East Ridge TN 37412")
ADDR_TAIL_RE = re.compile(r"^(.+?)\s+TN\s+(\d{5})$")


class MackieWolfTrusteeBot(BotBase):
    name = "mackie_wolf_trustee"
    description = (
        "Mackie Wolf Zientz & Mann TN trustee sales — PDF report, "
        "includes Auction.com / Hubzu platform tags."
    )
    throttle_seconds = 1.0
    expected_min_yield = 10  # they typically have 50+ TN listings live

    # How far back to walk looking for the most recent PDF
    max_days_lookback = 21

    def scrape(self) -> List[LeadPayload]:
        if pdfplumber is None:
            self.logger.error(
                "pdfplumber not installed — run: pip install pdfplumber"
            )
            return []

        pdf_url, report_date = self._find_latest_pdf()
        if pdf_url is None:
            self.logger.warning(
                f"no Mackie Wolf TN PDF found in last {self.max_days_lookback} days"
            )
            return []

        self.logger.info(f"using report dated {report_date.isoformat()}: {pdf_url}")
        pdf_bytes = self._download_pdf(pdf_url)
        if pdf_bytes is None:
            return []

        return self._parse_pdf(pdf_bytes, pdf_url, report_date)

    # ── PDF discovery ───────────────────────────────────────────────────────

    def _find_latest_pdf(self) -> Tuple[Optional[str], Optional[date]]:
        """HEAD-probe the predictable URL pattern back N days, return the
        most recent one that returns 200."""
        today = date.today()
        for offset in range(self.max_days_lookback):
            d = today - timedelta(days=offset)
            url = (
                f"{MWZM_PDF_BASE}/{d.strftime('%Y/%m')}/"
                f"TN-Sale-Report-as-of-{d.strftime('%m.%d.%Y')}.pdf"
            )
            res = self.fetch(url, method="HEAD")
            if res is not None and res.status_code == 200:
                return url, d
        return None, None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        res = self.fetch(url, method="GET")
        if res is None or res.status_code != 200:
            self.logger.warning(f"download failed for {url}")
            return None
        if not res.content:
            return None
        return res.content

    # ── PDF parsing ─────────────────────────────────────────────────────────

    def _parse_pdf(
        self, pdf_bytes: bytes, source_url: str, report_date: date,
    ) -> List[LeadPayload]:
        """Extract listings from the report PDF. Each listing spans two
        lines: a header (date + file# + street + county + platform) and
        an address tail (city TN zip)."""
        leads: List[LeadPayload] = []
        seen_keys: set[str] = set()

        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                full_text = "\n".join(
                    (p.extract_text() or "") for p in pdf.pages
                )
        except Exception as e:
            self.logger.warning(f"pdfplumber failed: {e}")
            return []

        lines = full_text.splitlines()
        n = len(lines)
        i = 0
        while i < n:
            line = lines[i].strip()
            m = LISTING_HEAD_RE.match(line)
            if not m:
                i += 1
                continue
            sale_date_raw, file_no, rest = m.group(1), m.group(2), m.group(3).strip()

            # Skip header rows accidentally matching the date pattern
            if file_no.lower() in ("file", "no") or "File" in rest[:8]:
                i += 1
                continue

            # The "rest" segment is "STREET   COUNTY   PLATFORM" with
            # multi-space separators that whitespace-normalize to single
            # spaces in PDF extract. Strategy:
            #   - Last token = platform (validated against known set, else
            #     accept any alphanumeric all-caps 4+ token)
            #   - Second-to-last token(s) = county (1 word, or 2 for
            #     "Van Buren")
            #   - Everything before that = street
            parts = rest.rsplit(None, 1)
            if len(parts) < 2:
                i += 1
                continue
            addr_county, platform = parts[0], parts[1].strip()

            # Validate platform — if it's lowercase or short, this is
            # probably a wrap-around / malformed line; skip.
            if platform not in KNOWN_PLATFORMS:
                # Still accept all-caps alphabetic 4+ chars — Mackie Wolf
                # could add a new platform tag we don't know about yet.
                if not (platform.isupper() and platform.isalpha() and len(platform) >= 4):
                    i += 1
                    continue

            # Pull county — handle multi-word county edge case
            county: Optional[str] = None
            street: Optional[str] = None
            stripped_lower = addr_county.lower()
            matched_multi = False
            for mw in MULTI_WORD_COUNTIES:
                tail = " " + mw.lower()
                if stripped_lower.endswith(tail):
                    county = mw
                    street = addr_county[: -len(tail)].strip()
                    matched_multi = True
                    break
            if not matched_multi:
                ac_parts = addr_county.rsplit(None, 1)
                if len(ac_parts) < 2:
                    i += 1
                    continue
                street = ac_parts[0].strip()
                county = ac_parts[1].strip()

            # The next line is the city/state/zip tail
            next_line = lines[i + 1].strip() if i + 1 < n else ""
            cm = ADDR_TAIL_RE.match(next_line)
            if cm:
                city = cm.group(1).strip()
                zip_code = cm.group(2)
                full_addr = f"{street}, {city}, TN {zip_code}"
                i += 2  # consumed two lines
            else:
                # Sometimes the tail line is missing — keep the street
                # so the lead still has SOMETHING for the assessor lookup
                full_addr = street
                i += 1

            # Parse sale date
            sale_date_iso = self._parse_sale_date(sale_date_raw)

            # Stable lead key: file_no is unique per case
            lead_key = self.make_lead_key("mackie_wolf", file_no)
            if lead_key in seen_keys:
                continue
            seen_keys.add(lead_key)

            notes = (
                f"bot_source=mackie_wolf_trustee · "
                f"file#={file_no} · "
                f"platform={platform} · "
                f"report={report_date.isoformat()}"
            )

            leads.append(LeadPayload(
                bot_source="mackie_wolf_trustee",
                pipeline_lead_key=lead_key,
                property_address=full_addr,
                county=county,
                distress_type="TRUSTEE_NOTICE",
                trustee_sale_date=sale_date_iso,
                admin_notes=notes,
                raw_payload={
                    "file_no": file_no,
                    "platform": platform,
                    "county": county,
                    "report_date": report_date.isoformat(),
                    "scraped_at": datetime.utcnow().isoformat() + "Z",
                },
                source_url=source_url,
            ))

        self.logger.info(f"parsed {len(leads)} listings from PDF")
        return leads

    # ── Helpers ─────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_sale_date(raw: str) -> Optional[str]:
        """'5/14/2026' → '2026-05-14'."""
        if not raw:
            return None
        try:
            return datetime.strptime(raw, "%m/%d/%Y").date().isoformat()
        except ValueError:
            return None


def run() -> dict:
    return MackieWolfTrusteeBot().run()


if __name__ == "__main__":
    print(run())
