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

Access (robots.txt read 2026-11-24): /notices/ is allowed, /Search/ is
disallowed. Since 2026-11-24 only the index is read. It prints the notice
ID, borrower, street, advertised sale date and first notice date. The
detail page (substitute trustee, city and ZIP, county, instrument number,
notice text) is no longer fetched; see _source_access.STOPPED. Notices
already staged keep their detail evidence and are still marked as seen each
day they stay on the index. A notice first seen after the stop date is
staged from the index alone, with no county, and is labeled index_only.
The detail parser is kept for a licensed or permitted copy of the page.

ID prefixes:
  FD = Foreclosure (Memphis "Daily News" notation)
  FN = Foreclosure Notice (variant)
  CD = Court (probate) — handled by tn_probate_bot if/when extended
  BN = Bid Notices (commercial RFPs, ignored)

Distress type: PRE_FORECLOSURE (Shelby + West TN counties)
"""

from __future__ import annotations

import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from urllib.parse import unquote

from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

from ._base import BotBase, LeadPayload
from ._memphis_notice_evidence import publisher_evidence
from ._source_access import stopped


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
    # False while the detail path is disallowed by the site's robots.txt.
    read_details = stopped("memphis_daily_news", "detail") is None
    acquisition_seconds = 420  # leave 180s of the runner cap for staging/health

    def __init__(self):
        super().__init__()
        # A single slow page must not consume four 30s attempts plus backoff.
        # Keep the existing UA and throttle; retry on the next scheduled run.
        self.session.mount(MDN_BASE + "/", HTTPAdapter(max_retries=0))

    def _source_fetch(self, url, **kwargs):
        response = self.fetch(url, timeout=(5, 15), **kwargs)
        if response is not None and response.status_code in (403, 429):
            self._source_blocked = True
        return response

    def _report_health(self, **kwargs):
        if kwargs.get("finished_at") is not None:
            kwargs["notes"] = {**(kwargs.get("notes") or {}),
                               "notice_collection": getattr(self, "collection_stats", {})}
        return super()._report_health(**kwargs)

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        seen_ids: set[str] = set()
        candidates = []
        started = time.monotonic()
        self._source_blocked = False
        self.collection_stats = dict(indexes=0, detail_attempts=0, past=0,
                                     invalid=0, failed=0, partial=False)
        if not self.read_details:
            self.collection_stats.update(index_only=0, detail_stopped_on=stopped("memphis_daily_news", "detail")["stopped_on"])
        # Reserve a full request before starting it, not after it has overrun.
        def exhausted():
            return self._source_blocked or time.monotonic() - started >= self.acquisition_seconds - 22

        for pub_date in self._recent_dates(self.days_to_scan):
            if exhausted():
                self.collection_stats["partial"] = True
                break
            entries = self._fetch_index(pub_date)
            self.collection_stats["indexes"] += 1
            for entry in entries:
                if entry["id"] not in seen_ids:
                    seen_ids.add(entry["id"])
                    candidates.append((entry, pub_date))
        # Deduplicate newest issue first, then rotate daily so a slow source
        # cannot repeatedly consume the budget on the same detail-page prefix.
        if candidates:
            offset = date.today().toordinal() % len(candidates)
            candidates = candidates[offset:] + candidates[:offset]
        for entry, pub_date in candidates:
            if exhausted():
                self.collection_stats["partial"] = True
                break
            fid = entry["id"]
            if entry["sale_date"] and entry["sale_date"] < date.today().isoformat():
                self.collection_stats["past"] += 1
                continue
            if not self.read_details:
                lead = self._index_lead(entry, pub_date)
                if lead is not None:
                    self.collection_stats["index_only"] += 1
                    leads.append(lead)
                continue
            self.collection_stats["detail_attempts"] += 1
            detail = self._fetch_detail(fid, pub_date)
            if detail is None:
                self.collection_stats["failed"] += 1
                continue
            # A stale/default detail page must never refresh another notice.
            if (detail.get("tdn_no") != fid
                    or self._identity(detail.get("borrower")) != self._identity(entry["borrower"])
                    or self._identity(detail.get("address")) != self._identity(entry["address"])):
                self.collection_stats["invalid"] += 1
                continue
            lead = self._build_lead(detail, fid, pub_date)
            if lead is not None:
                clause = (lead.raw_payload or {}).get("published_sale_clause")
                if lead.trustee_sale_date != entry["sale_date"] and not (clause and entry["sale_date"] is None):
                    self.collection_stats["invalid"] += 1
                    continue
                if lead.trustee_sale_date and lead.trustee_sale_date < date.today().isoformat():
                    self.collection_stats["past"] += 1
                    continue
                leads.append(lead)
        self.collection_stats["unique_notices"] = len(candidates)
        self.collection_stats["partial"] |= bool(self.collection_stats["failed"] or self.collection_stats["invalid"])
        self.logger.info("Collection coverage: %s; returned=%s", self.collection_stats, len(leads))
        return leads

    @staticmethod
    def _identity(value):
        return " ".join((value or "").casefold().split())

    @staticmethod
    def _recent_dates(days: int) -> List[date]:
        today = date.today()
        # Skip Sunday (no publication day)
        return [today - timedelta(days=i) for i in range(days)
                if (today - timedelta(days=i)).weekday() != 6]

    def _fetch_index(self, pub_date: date) -> List[Dict]:
        url = INDEX_URL_TEMPLATE.format(
            year=pub_date.year,
            month=MONTH_ABBR[pub_date.month - 1],
            day=pub_date.day,
        )
        res = self._source_fetch(url)
        if res is None or res.status_code != 200:
            self.collection_stats["failed"] += 1
            return []
        return self._parse_index(res.text, pub_date)

    @classmethod
    def _parse_index(cls, html: str, pub_date: date) -> List[Dict]:
        entries: List[Dict] = []
        seen: set[str] = set()
        for row in BeautifulSoup(html, "html.parser").select("tr"):
            m = FORECLOSURE_ID_RE.search(str(row))
            if not m:
                continue
            # The URL may silently serve another issue. Bind to the actual link.
            if cls._iso_date(unquote(m.group(2))) != pub_date.isoformat():
                continue
            cells = row.find_all("td")
            if len(cells) != 5:
                continue
            fid = m.group(1)
            if fid not in seen:
                seen.add(fid)
                entries.append({"id": fid, "borrower": cells[1].get_text(" ", strip=True),
                                "address": cells[2].get_text(" ", strip=True),
                                "sale_date": cls._iso_date(cells[3].get_text(strip=True)),
                                "sale_printed": cells[3].get_text(strip=True) or None,
                                "first_notice_printed": cells[4].get_text(strip=True) or None})
        return entries

    def _index_lead(self, entry: Dict, pub_date: date) -> Optional[LeadPayload]:
        """A notice as the allowed index page prints it. For a notice already
        staged this only records today's sighting (same lead key). For a new
        one it stages what the index gives and says what was not collected."""
        fid, borrower, street = entry["id"], entry.get("borrower"), entry.get("address")
        if not borrower or not street:
            return None
        halt = stopped("memphis_daily_news", "detail") or {}
        index_url = INDEX_URL_TEMPLATE.format(year=pub_date.year, month=MONTH_ABBR[pub_date.month - 1], day=pub_date.day)
        raw = {
            "structured": {"borrower": borrower, "address": street, "tdn_no": fid,
                           "advertised_auction_date": entry.get("sale_printed"),
                           "date_of_first_public_notice": entry.get("first_notice_printed")},
            "publication_date": pub_date.isoformat(),
            "notice_id": fid,
            "observed_on": date.today().isoformat(),
            "evidence_type": "published_notice_index",  # not trustee confirmation or sale outcome
            "collection": {"scope": "index_only", "page": index_url, "detail_stopped_on": halt.get("stopped_on"),
                           "not_collected": ["substitute trustee", "city and ZIP", "county", "instrument number",
                                             "notice text"]},
        }
        return LeadPayload(
            bot_source=self.name,
            pipeline_lead_key=self.make_lead_key(self.name, fid),
            property_address=street.strip(),
            county=None,   # the index does not print one; never assumed
            full_name=borrower,
            owner_name_records=borrower,
            trustee_sale_date=entry.get("sale_date"),
            distress_type="PRE_FORECLOSURE",
            admin_notes=f"Memphis Daily News {fid} · pub {pub_date.isoformat()} · index only",
            source_url=index_url,
            raw_payload=raw,
        )

    def _fetch_detail(self, fid: str, pub_date: date) -> Optional[Dict]:
        date_param = f"{pub_date.month}/{pub_date.day}/{pub_date.year}"
        res = self._source_fetch(DETAIL_URL, params={"id": fid, "date": date_param})
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

        evidence = publisher_evidence(detail, county, fid, source_url)
        if "published_sale_clause" in evidence:
            sale_date = evidence["published_sale_clause"]["date"]

        raw = {
            **evidence,
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
            "notice_id": fid,
            "observed_on": date.today().isoformat(),
            "evidence_type": "published_notice",  # not trustee confirmation or sale outcome
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
