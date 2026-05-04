"""
Nashville Ledger public notices scraper.

The Ledger is a Tennessee newspaper of record that publishes legal
notices for Davidson + surrounding counties. The same notices that
go to foreclosuretennessee.com / tnlegalpub.com — but the Ledger's
detail pages give us BETTER structured fields plus the full notice
body (including original lender, junior lienholders, parcel ID,
recording instrument numbers).

Index page: /Notices.aspx?noticesDate=M/D/YYYY (publishes Fridays)
Detail page: /Search/Details/ViewNotice.aspx?id=FL{NNNNNN}&date=M/D/YYYY

Each detail page yields:
  - Borrower name (legal mortgagor)
  - Property address (street, city, state, zip)
  - Original Trustee + Substitute Trustee + Attorney
  - Instrument No. (recording reference)
  - Advertised Auction Date
  - Date of First Public Notice
  - Trust Date (deed of trust execution date)
  - TDN No. (Ledger internal ID)
  - Full notice body (regex-extractable: lender, parcel, junior liens,
    civil district, plat references)

Why this matters: covers Davidson County (one of the 9 EXTERNAL
counties TPAD doesn't cover). Plus the structured fields are cleaner
than what we extract from base64-encoded PDFs in foreclosure_tennessee_bot.

Distress type: PRE_FORECLOSURE
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

FL_ID_RE = re.compile(r"OpenChildFT2\('(FL\d+)','([^']+)'\)")

# Body-text extraction patterns
LENDER_RE = re.compile(r"payable to the order of\s+([^.]+?)\.", re.IGNORECASE)
LENDER_ALT_RE = re.compile(r"the holder of the (?:note|debt) is\s+([^.,]+)", re.IGNORECASE)
PARCEL_RE = re.compile(
    r"(?:MAP AND PARCEL|TAX MAP|PARCEL)\s*(?:NO\.?|NUMBER|ID)?[:\s]+([A-Z0-9][A-Z0-9\-\.]*\d[A-Z0-9\-\.]*)",
    re.IGNORECASE,
)
COUNTY_RE_FALLBACKS = (
    re.compile(r"Register of Deeds for ([A-Za-z]+) County", re.IGNORECASE),
    re.compile(r"in ([A-Za-z]+) County, Tennessee", re.IGNORECASE),
    re.compile(r"([A-Za-z]+) County[, ]+Tennessee", re.IGNORECASE),
    re.compile(r"recorded in[^.]+?([A-Za-z]+) County", re.IGNORECASE),
)
PRINCIPAL_RE = re.compile(r"original principal (?:amount|sum) of \$?([\d,]+(?:\.\d{2})?)", re.IGNORECASE)
DOT_BOOK_RE = re.compile(
    r"(?:Official Record|Record) Book\s+(?:Volume\s+)?(\d+),?\s+Pages?\s+([\d\-]+)",
    re.IGNORECASE,
)
INTERESTED_PARTIES_RE = re.compile(
    r"OTHER INTERESTED PARTIES:?\s*(.+?)(?:PLEASE TAKE NOTICE|This the|TRUSTEE:)",
    re.IGNORECASE | re.DOTALL,
)


class NashvilleLedgerBot(BotBase):
    name = "nashville_ledger"
    description = "Tennessee Ledger weekly public-notice foreclosure publication"
    throttle_seconds = 1.5  # courtesy delay; ASP.NET site
    expected_min_yield = 5  # typical Friday publishes 30-40 foreclosure notices

    # How many recent Friday publications to walk
    weeks_to_scan = 4

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        for pub_date in self._recent_friday_dates(self.weeks_to_scan):
            ids = self._fetch_index(pub_date)
            self.logger.info(f"{pub_date.isoformat()}: {len(ids)} foreclosure IDs")
            for fl_id in ids:
                if fl_id in seen_ids:
                    continue
                seen_ids.add(fl_id)
                detail = self._fetch_detail(fl_id, pub_date)
                if detail is None:
                    continue
                lead = self._build_lead(detail, fl_id, pub_date)
                if lead is not None:
                    leads.append(lead)
        return leads

    # ── Index walk ──────────────────────────────────────────────────────────

    @staticmethod
    def _recent_friday_dates(weeks: int) -> List[date]:
        """The Ledger publishes Friday. Walk back N most-recent Fridays."""
        today = date.today()
        # Friday is weekday() == 4
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
        for m in FL_ID_RE.finditer(res.text):
            fl_id = m.group(1)
            if fl_id not in seen:
                seen.add(fl_id)
                ids.append(fl_id)
        return ids

    # ── Detail fetch ────────────────────────────────────────────────────────

    def _fetch_detail(self, fl_id: str, pub_date: date) -> Optional[Dict]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self.fetch(DETAIL_URL, params={"id": fl_id, "date": date_param})
        if res is None or res.status_code != 200:
            return None
        return self._parse_detail(res.text)

    @staticmethod
    def _parse_detail(html: str) -> Dict:
        """Extract structured fields + full body text from notice detail HTML."""
        soup = BeautifulSoup(html, "html.parser")
        out: Dict[str, Optional[str]] = {}

        # Structured table — labels + lblN spans
        labels = ("Borrower", "Address", "Original Trustee", "Attorney",
                  "Instrument No.", "Substitute Trustee",
                  "Advertised Auction Date", "Date of First Public Notice",
                  "Trust Date", "TDN No.")
        rows = soup.select("div#pnlSummary tr")
        addr_lines: List[str] = []
        for tr in rows:
            tds = tr.find_all("td")
            if len(tds) < 2:
                continue
            label = tds[0].get_text(strip=True).rstrip(":").strip()
            value = tds[1].get_text(strip=True)
            if not value or value == "N/A":
                continue
            if label in labels:
                key = label.lower().replace(".", "").replace(" ", "_")
                out[key] = value
            elif label in ("", "&nbsp;"):
                # Continuation of address (city/state/zip line)
                if value:
                    addr_lines.append(value)

        if addr_lines:
            out["address_continuation"] = " ".join(addr_lines)

        # Full notice body — paragraphs after the summary panel
        body_paras = soup.select("div#record-details > p")
        body = "\n\n".join(p.get_text(" ", strip=True) for p in body_paras)
        out["body"] = body

        return out

    # ── Lead construction ───────────────────────────────────────────────────

    def _build_lead(self, detail: Dict, fl_id: str, pub_date: date) -> Optional[LeadPayload]:
        borrower = detail.get("borrower")
        street = detail.get("address")
        addr_cont = detail.get("address_continuation") or ""
        if not borrower or not street:
            return None

        full_address = street.strip()
        if addr_cont:
            full_address = f"{full_address}, {addr_cont}".strip().rstrip(",")

        county = self._infer_county(addr_cont, detail.get("body") or "")

        body = detail.get("body") or ""
        lender = self._extract_lender(body)
        parcel = self._extract_parcel(body)
        junior_liens = self._extract_junior_liens(body)
        principal = self._parse_principal(body)
        dot_recording = self._extract_dot_recording(body)

        sale_date = self._iso_date(detail.get("advertised_auction_date"))

        admin_parts = [f"Nashville Ledger {fl_id}", f"pub {pub_date.isoformat()}"]
        if detail.get("substitute_trustee"):
            admin_parts.append(f"trustee={detail['substitute_trustee']}")
        if lender:
            admin_parts.append(f"lender={lender}")
        if parcel:
            admin_parts.append(f"parcel={parcel}")
        if junior_liens:
            admin_parts.append(f"junior={'; '.join(junior_liens[:3])}")

        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        source_url = f"{DETAIL_URL}?id={fl_id}&date={date_param}"

        raw = {
            "structured": {k: detail.get(k) for k in (
                "borrower", "address", "original_trustee", "attorney",
                "instrument_no", "substitute_trustee", "advertised_auction_date",
                "date_of_first_public_notice", "trust_date", "tdn_no",
            )},
            "extracted": {
                "lender": lender,
                "parcel": parcel,
                "junior_liens": junior_liens,
                "original_principal": principal,
                "dot_recording": dot_recording,
            },
            "publication_date": pub_date.isoformat(),
        }

        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, fl_id),
            property_address=full_address,
            county=county,
            full_name=borrower,
            owner_name_records=borrower,
            trustee_sale_date=sale_date,
            distress_type="PRE_FORECLOSURE",
            admin_notes=" · ".join(admin_parts),
            source_url=source_url,
            raw_payload=raw,
        )

    # ── Field extraction helpers ────────────────────────────────────────────

    @staticmethod
    def _extract_lender(body: str) -> Optional[str]:
        m = LENDER_RE.search(body)
        if m:
            return m.group(1).strip()
        m = LENDER_ALT_RE.search(body)
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _extract_parcel(body: str) -> Optional[str]:
        m = PARCEL_RE.search(body)
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _extract_junior_liens(body: str) -> List[str]:
        m = INTERESTED_PARTIES_RE.search(body)
        if not m:
            return []
        chunk = m.group(1)
        # Each interested party block tends to be name then address; split on
        # double-newline or repeated whitespace patterns.
        lines = [ln.strip() for ln in re.split(r"\s{2,}|\n+", chunk) if ln.strip()]
        # Take only the entity-name lines (street numbers + state/zip lines look numeric)
        names: List[str] = []
        for ln in lines:
            if re.match(r"^\d", ln):  # skip address lines starting with street number
                continue
            if re.search(r"\bTN\s+\d{5}\b", ln):  # skip "City, TN 37040" lines
                continue
            if len(ln) < 4 or len(ln) > 80:
                continue
            names.append(ln)
        return names[:5]

    @staticmethod
    def _parse_principal(body: str) -> Optional[float]:
        m = PRINCIPAL_RE.search(body)
        if not m:
            return None
        try:
            return float(m.group(1).replace(",", ""))
        except ValueError:
            return None

    @staticmethod
    def _extract_dot_recording(body: str) -> Optional[str]:
        m = DOT_BOOK_RE.search(body)
        if m:
            return f"Book {m.group(1)} Page {m.group(2)}"
        return None

    @staticmethod
    def _iso_date(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(value.strip(), fmt).date().isoformat()
            except ValueError:
                continue
        return None

    @staticmethod
    def _infer_county(addr_continuation: str, body: str) -> Optional[str]:
        for pat in COUNTY_RE_FALLBACKS:
            m = pat.search(body)
            if m:
                county = m.group(1).strip().lower()
                # Exclude false positives from generic prose
                if county not in {"the", "this", "such", "any", "said"}:
                    return county
        return None


def run() -> dict:
    bot = NashvilleLedgerBot()
    return bot.run()


if __name__ == "__main__":
    result = run()
    print(f"\nResult: {result}")
