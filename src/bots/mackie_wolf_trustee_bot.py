"""Mackie Wolf Zientz & Mann PC — TX-HQ substitute trustee firm with
heavy TN volume.

The firm publishes a "TN Sale Report" PDF at:
  https://mwzmlaw.com/wp-content/uploads/YYYY/MM/TN-Sale-Report-as-of-MM.DD.YYYY.pdf

The PDF refreshes weekly-ish. We walk back from today and pick the
most recent one available.

PDF structure (verified 2026-05-14, new format):
  Header line:   M/D/YYYY  FILE#  BORROWER(s)  STREET  COUNTY
  Continuation:  ADDITIONAL_BORROWER (if borrowers wrap)
  Tail line:     CITY  TN  ZIP

Each borrower is tagged "(Borrower)" — multiple borrowers separated by
comma, wrapping to a second line for long lists. The trailing single
word on the header line is the county.

  5/19/2026 26-000068-505 Harless, Sarah (Borrower) 257 Shipp Springs Road Sullivan
  Kingsport TN 37660

  5/21/2026 26-000028-505 Hitchcock, Martha J. (Borrower), 1306 Coleman Circle Hamilton
  Hitchcock, William E. (Borrower)
  East Ridge TN 37412

OLD FORMAT (pre-2026-05-14) carried a trailing PLATFORM column
(AUCTION/HUBZU/HUDMARSH/MWZM) telling us where each sale was hosted.
That column was removed when MWZ added the borrower column. We lose
the Auction.com / Hubzu visibility from this source — Patrick can
cross-reference Auction.com directly if needed — but we GAIN owner
names on every lead, which were previously filled downstream by the
assessor bots.

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

# Header line of a listing: M/D/YYYY  FILE#  BORROWERS+STREET+COUNTY
# MWZ file numbers have the form "26-000068-505" — strict-ish to avoid
# matching dates/IDs that happen to be space-separated.
LISTING_HEAD_RE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{2}-\d{6}-\d{3})\s+(.+)$")
# Tail line: CITY TN ZIP   (e.g. "Memphis TN 38116" or "East Ridge TN 37412")
ADDR_TAIL_RE = re.compile(r"^(.+?)\s+TN\s+(\d{5})$")
# Borrower marker — appears after each name. Split on this and discard
# the trailing empty segment.
BORROWER_MARKER = "(Borrower)"
# Street-start pattern — typical street: 1-5 digits + space + capital +
# lowercase letter (e.g. "257 Shipp", "1306 Coleman"). Used to split
# borrowers from address when the (Borrower) marker is unreliable
# (multi-borrower records can have the address INSIDE the borrower
# list visually because pdfplumber unwraps PDF columns line-by-line).
STREET_START_RE = re.compile(r"\b\d{1,5}\s+[A-Z][a-zA-Z]")


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
        """Extract listings from the report PDF.

        Each listing is a "record" anchored by a header line matching
        DATE FILE# ... and bounded at the end by a CITY TN ZIP tail
        line. Borrowers can wrap to continuation lines between header
        and tail (long borrower lists trigger this).
        """
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
        # Find record-start indices (lines matching the header pattern)
        record_starts = [
            i for i, l in enumerate(lines)
            if LISTING_HEAD_RE.match(l.strip())
        ]
        if not record_starts:
            self.logger.info("no listing-header lines matched in PDF")
            return []

        for j, start in enumerate(record_starts):
            end = record_starts[j + 1] if j + 1 < len(record_starts) else len(lines)
            record = [lines[k].strip() for k in range(start, end)]

            head_m = LISTING_HEAD_RE.match(record[0])
            if not head_m:
                continue
            sale_date_raw = head_m.group(1)
            file_no = head_m.group(2)
            header_rest = head_m.group(3).strip()

            # Find the CITY/STATE/ZIP tail line (terminates the record).
            tail_idx = None
            tail_match = None
            for k in range(1, len(record)):
                tm = ADDR_TAIL_RE.match(record[k])
                if tm:
                    tail_idx = k
                    tail_match = tm
                    break
            if tail_idx is None or tail_match is None:
                # Malformed record — skip rather than write bad data.
                self.logger.debug(f"no CITY TN ZIP tail for record starting at line {start}")
                continue

            city = tail_match.group(1).strip()
            zip_code = tail_match.group(2)

            # Continuation lines (between header and tail line) — these
            # carry overflow borrowers OR (rarely) the address itself
            # when MWZ wraps a long borrower-name across the column.
            continuation = " ".join(record[1:tail_idx]).strip()

            # Address-split strategy: find the FIRST street-number
            # pattern (digit + capital letter) in header_rest. That's
            # the start of the address. Everything before is
            # borrower-list (line 1 portion). Everything from there
            # through end of header_rest is "STREET COUNTY".
            #
            # Why this works better than rfind("(Borrower)"):
            # multi-borrower records frequently have visual layout like
            #   "Borrower1 (Borrower), 1306 Address County  Borrower2 (Borrower)"
            # where the address sits INSIDE the borrower list (PDF
            # column unwrap artifact). Splitting on "(Borrower)"
            # mis-classifies the address as borrower text.
            street_match = STREET_START_RE.search(header_rest)
            if street_match:
                addr_split = street_match.start()
                borrowers_on_header = header_rest[:addr_split].strip()
                after_text = header_rest[addr_split:].strip()
            else:
                # No street in header — address must be on a
                # continuation line. Look for it there.
                borrowers_on_header = header_rest.strip()
                after_text = ""
                for cont_line in record[1:tail_idx]:
                    cm = STREET_START_RE.search(cont_line)
                    if cm:
                        after_text = cont_line[cm.start():].strip()
                        break
                if not after_text:
                    self.logger.debug(
                        f"no street pattern found for record at line {start}"
                    )
                    continue

            # Trim trailing comma/space from borrower segment (when the
            # last borrower entry ended with ", " before the address)
            borrowers_on_header = borrowers_on_header.rstrip(", ").strip()

            # Combine borrower text from header + continuation lines
            # (continuation might be additional borrowers OR — when the
            # address was on continuation — leftover borrower fragments
            # that we'll silently ignore if they don't have markers).
            all_borrower_text = borrowers_on_header
            if continuation:
                all_borrower_text = (
                    all_borrower_text + " " + continuation
                ).strip() if all_borrower_text else continuation

            # Extract borrower names. Split on the marker, drop the
            # trailing empty segment, strip ", " padding from each.
            owners: List[str] = []
            if BORROWER_MARKER in all_borrower_text:
                raw_segments = all_borrower_text.split(BORROWER_MARKER)
                for seg in raw_segments[:-1]:  # last is post-marker fragment
                    name = seg.strip().strip(",").strip()
                    if name:
                        owners.append(name)
            else:
                # Edge case: header had a borrower NAME but no marker
                # (marker wrapped to the continuation line). Use the
                # cleaned-up borrower-prefix from line 1 as the name.
                if borrowers_on_header:
                    owners.append(borrowers_on_header)
            primary_owner = ", ".join(owners) if owners else None

            # Split "STREET COUNTY" — county is the trailing word(s).
            # Multi-word county edge case ("Van Buren") gets explicit
            # handling.
            county: Optional[str] = None
            street: Optional[str] = None
            after_lower = after_text.lower()
            for mw in MULTI_WORD_COUNTIES:
                tail = " " + mw.lower()
                if after_lower.endswith(tail):
                    county = mw
                    street = after_text[: -len(tail)].strip()
                    break
            if county is None:
                ac_parts = after_text.rsplit(None, 1)
                if len(ac_parts) < 2:
                    continue
                street = ac_parts[0].strip()
                county = ac_parts[1].strip()

            full_addr = f"{street}, {city}, TN {zip_code}"

            # Stable lead key: file_no is unique per case
            lead_key = self.make_lead_key("mackie_wolf", file_no)
            if lead_key in seen_keys:
                continue
            seen_keys.add(lead_key)

            notes = (
                f"bot_source=mackie_wolf_trustee · "
                f"file#={file_no} · "
                f"borrowers={len(owners)} · "
                f"report={report_date.isoformat()}"
            )

            leads.append(LeadPayload(
                bot_source="mackie_wolf_trustee",
                pipeline_lead_key=lead_key,
                property_address=full_addr,
                county=county,
                owner_name_records=primary_owner,
                distress_type="TRUSTEE_NOTICE",
                trustee_sale_date=self._parse_sale_date(sale_date_raw),
                admin_notes=notes,
                raw_payload={
                    "file_no": file_no,
                    "borrowers": owners,
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
