"""
Memphis Daily News public notices scraper.

Sister site to The Nashville Ledger (same Catalis-era architecture)
covering Shelby County + West TN. Memphis is one of the 9 EXTERNAL
TPAD counties and Shelby Trustee is Cloudflare-blocked, so this bot
is currently the cleanest public-records foreclosure feed for the
West TN region.

Index page: /notices/{YYYY}/{MMM}/{D}/  (publishes daily, FY URLs use
abbreviated month "Apr"/"May"/etc — different from Ledger's
?noticesDate=M/D/YYYY query-string convention)
Detail page: /Search/Details/ViewNotice.aspx?id={ID}&date=M/D/YYYY
  — IDENTICAL to Nashville Ledger detail-URL pattern, including the
  same lbl1..lbl11 structured-field layout.

ID prefixes:
  FD = Foreclosure (Memphis "Daily News" notation)
  FN = Foreclosure Notice (variant)
  CD = Court (probate) — handled by tn_probate_bot if/when extended
  BN = Bid Notices (commercial RFPs, ignored)

Distress type: PRE_FORECLOSURE (Shelby + West TN counties)
"""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload


MDN_BASE = "https://www.memphisdailynews.com"
INDEX_URL_TEMPLATE = MDN_BASE + "/notices/{year}/{month}/{day}/"
DETAIL_URL = MDN_BASE + "/Search/Details/ViewNotice.aspx"

# Memphis Daily News uses FD/FN prefixes (vs Ledger's FL).
FORECLOSURE_ID_RE = re.compile(r"OpenChildFT2\('(F[DN]\d+)','([^']+)'\)")

# Body-text patterns (same as Nashville Ledger)
LENDER_RE = re.compile(r"payable to the order of\s+([^.]+?)\.", re.IGNORECASE)
LENDER_ALT_RE = re.compile(r"the holder of the (?:note|debt) is\s+([^.,]+)", re.IGNORECASE)
PARCEL_RE = re.compile(
    r"(?:MAP AND PARCEL|TAX MAP|PARCEL)\s*(?:NO\.?|NUMBER|ID)?[:\s]+([A-Z0-9][A-Z0-9\-\.]*\d[A-Z0-9\-\.]*)",
    re.IGNORECASE,
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
COUNTY_RE_FALLBACKS = (
    re.compile(r"Register of Deeds for ([A-Za-z]+) County", re.IGNORECASE),
    re.compile(r"in ([A-Za-z]+) County, Tennessee", re.IGNORECASE),
    re.compile(r"([A-Za-z]+) County[, ]+Tennessee", re.IGNORECASE),
    re.compile(r"recorded in[^.]+?([A-Za-z]+) County", re.IGNORECASE),
)

MONTH_ABBR = ("Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")


class MemphisDailyNewsBot(BotBase):
    name = "memphis_daily_news"
    description = "Memphis Daily News legal-notice publication for Shelby + West TN foreclosures"
    throttle_seconds = 1.5
    expected_min_yield = 5

    days_to_scan = 14  # walk last two weeks (MDN publishes daily, not weekly)

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()

        for pub_date in self._recent_dates(self.days_to_scan):
            ids = self._fetch_index(pub_date)
            if ids:
                self.logger.info(f"{pub_date.isoformat()}: {len(ids)} foreclosure IDs")
            for fid in ids:
                if fid in seen_ids:
                    continue
                seen_ids.add(fid)
                detail = self._fetch_detail(fid, pub_date)
                if detail is None:
                    continue
                lead = self._build_lead(detail, fid, pub_date)
                if lead is not None:
                    leads.append(lead)
        return leads

    @staticmethod
    def _recent_dates(days: int) -> List[date]:
        today = date.today()
        # Skip Sunday (no publication day)
        return [today - timedelta(days=i) for i in range(days)
                if (today - timedelta(days=i)).weekday() != 6]

    def _fetch_index(self, pub_date: date) -> List[str]:
        url = INDEX_URL_TEMPLATE.format(
            year=pub_date.year,
            month=MONTH_ABBR[pub_date.month - 1],
            day=pub_date.day,
        )
        res = self.fetch(url)
        if res is None or res.status_code != 200:
            return []
        ids: List[str] = []
        seen: set[str] = set()
        for m in FORECLOSURE_ID_RE.finditer(res.text):
            fid = m.group(1)
            if fid not in seen:
                seen.add(fid)
                ids.append(fid)
        return ids

    def _fetch_detail(self, fid: str, pub_date: date) -> Optional[Dict]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self.fetch(DETAIL_URL, params={"id": fid, "date": date_param})
        if res is None or res.status_code != 200:
            return None
        return self._parse_detail(res.text)

    @staticmethod
    def _parse_detail(html: str) -> Dict:
        soup = BeautifulSoup(html, "html.parser")
        out: Dict[str, Optional[str]] = {}

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
                if value:
                    addr_lines.append(value)

        if addr_lines:
            out["address_continuation"] = " ".join(addr_lines)

        body_paras = soup.select("div#record-details > p")
        body = "\n\n".join(p.get_text(" ", strip=True) for p in body_paras)
        out["body"] = body
        return out

    def _build_lead(self, detail: Dict, fid: str, pub_date: date) -> Optional[LeadPayload]:
        borrower = detail.get("borrower")
        street = detail.get("address")
        addr_cont = detail.get("address_continuation") or ""
        if not borrower or not street:
            return None
        full_address = street.strip()
        if addr_cont:
            full_address = f"{full_address}, {addr_cont}".strip().rstrip(",")

        county = self._infer_county(detail.get("body") or "")

        body = detail.get("body") or ""
        lender = self._extract_lender(body)
        parcel = self._extract_parcel(body)
        junior_liens = self._extract_junior_liens(body)
        principal = self._parse_principal(body)
        dot_recording = self._extract_dot_recording(body)
        sale_date = self._iso_date(detail.get("advertised_auction_date"))

        admin_parts = [f"Memphis Daily News {fid}", f"pub {pub_date.isoformat()}"]
        if detail.get("substitute_trustee"):
            admin_parts.append(f"trustee={detail['substitute_trustee']}")
        if lender:
            admin_parts.append(f"lender={lender}")
        if parcel:
            admin_parts.append(f"parcel={parcel}")
        if junior_liens:
            admin_parts.append(f"junior={'; '.join(junior_liens[:3])}")

        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        source_url = f"{DETAIL_URL}?id={fid}&date={date_param}"

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
            pipeline_lead_key=self.make_lead_key(self.name, fid),
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
        lines = [ln.strip() for ln in re.split(r"\s{2,}|\n+", chunk) if ln.strip()]
        names: List[str] = []
        for ln in lines:
            if re.match(r"^\d", ln):
                continue
            if re.search(r"\bTN\s+\d{5}\b", ln):
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
    def _infer_county(body: str) -> Optional[str]:
        for pat in COUNTY_RE_FALLBACKS:
            m = pat.search(body)
            if m:
                county = m.group(1).strip().lower()
                if county not in {"the", "this", "such", "any", "said"}:
                    return county
        return None


def run() -> dict:
    bot = MemphisDailyNewsBot()
    return bot.run()


if __name__ == "__main__":
    result = run()
    print(f"\nResult: {result}")
