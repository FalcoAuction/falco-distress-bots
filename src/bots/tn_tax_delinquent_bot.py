"""
Tennessee delinquent property tax scrapers — multi-county.

Each TN county publishes its delinquent property list differently. This
bot wraps the most accessible (free, no-auth) sources into one scraper
so they all run together.

Sources covered:
  - Montgomery County (Clarksville)  — text-extractable PDF, regex parse
  - Cheatham County                   — HTML table on county site
  - Sumner County                     — scanned-image PDF (skip OCR for v1)

Sources NOT covered (require login, JS rendering, or paid feed):
  - Davidson  — published only in Nashville Ledger newspaper
  - Williamson — JS-rendered Catalis Gov app (needs Playwright)
  - Wilson    — Clerk & Master site, no current sale scheduled
  - Rutherford / Robertson — GovEase login required

Distress type: TAX_LIEN
"""

from __future__ import annotations

import re
from io import BytesIO
from typing import List, Optional

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

from ._base import BotBase, LeadPayload


MONTGOMERY_PDF_URL = "https://montgomerytn.gov/storage/departments/chancery/FinalList.pdf"
CHEATHAM_HTML_URL = "https://www.cheathamcountytn.gov/delinquent_tax_property.html"


# Montgomery County PDF row pattern. Each entry typically has owner,
# parcel ID, situs address, total owed. Format example:
#   Heap, Darrell      041K A 007.00      120 Fairview Lane    $1,303.10
# We use a flexible regex that tolerates whitespace + line wrapping.
_MONTGOMERY_PARCEL_RX = re.compile(
    r"([A-Z][A-Za-z,'\s\-\.&]+?)\s+"           # owner (greedy until parcel)
    r"(\d{2,3}[A-Z]?\s*[A-Z]?\s*[\d.]+(?:\s*[A-Z]?)?)\s+"  # parcel id like "041K A 007.00"
    r"([\d\-]+\s+[A-Z][A-Za-z0-9\s.,'\-]+?)\s+"  # situs address
    r"\$([\d,]+\.\d{2})",                          # amount
    re.MULTILINE,
)


class TnTaxDelinquentBot(BotBase):
    name = "tn_tax_delinquent"
    description = "TN county delinquent property tax lists (Montgomery, Cheatham, Sumner)"
    throttle_seconds = 1.5
    expected_min_yield = 5

    def scrape(self) -> List[LeadPayload]:
        leads: List[LeadPayload] = []
        leads.extend(self._scrape_montgomery())
        leads.extend(self._scrape_cheatham())
        # Sumner left out for v1 — scanned PDFs need OCR
        return leads

    # ── Montgomery (PDF, text-layer) ────────────────────────────────────────

    def _scrape_montgomery(self) -> List[LeadPayload]:
        if pdfplumber is None:
            self.logger.warning("pdfplumber missing — skip Montgomery")
            return []
        self.logger.info(f"Montgomery: fetching {MONTGOMERY_PDF_URL}")
        res = self.fetch(MONTGOMERY_PDF_URL)
        if res is None or res.status_code != 200:
            self.logger.warning(f"Montgomery fetch failed: {res.status_code if res else 'none'}")
            return []
        if "pdf" not in res.headers.get("Content-Type", "").lower():
            self.logger.warning(f"Montgomery: not a PDF response")
            return []

        try:
            with pdfplumber.open(BytesIO(res.content)) as pdf:
                text = "\n".join((p.extract_text() or "") for p in pdf.pages)
        except Exception as e:
            self.logger.warning(f"Montgomery pdfplumber failed: {e}")
            return []

        # Normalize whitespace
        text = re.sub(r"[ \t]+", " ", text)

        leads: List[LeadPayload] = []
        for m in _MONTGOMERY_PARCEL_RX.finditer(text):
            owner = m.group(1).strip().rstrip(",")
            parcel = m.group(2).strip()
            address_raw = m.group(3).strip()
            amount = m.group(4).replace(",", "")

            # Filter junk: owner should look like a name, address should
            # have a street keyword
            if len(owner) < 3 or len(owner) > 80:
                continue
            if not re.search(r"\b(Rd|Road|St|Street|Ave|Avenue|Dr|Drive|Ln|Lane|Blvd|Ct|Cir|Circle|Pl|Way|Hwy|Pkwy|Trl|Trail|Court|Place)\b", address_raw, re.IGNORECASE):
                continue

            full_address = f"{address_raw}, Clarksville, TN"

            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, f"montgomery-{parcel}"),
                property_address=full_address,
                county="Montgomery County",
                owner_name_records=owner,
                distress_type="TAX_LIEN",
                admin_notes=f"Montgomery Co tax sale · parcel {parcel} · ${amount} owed",
                source_url=MONTGOMERY_PDF_URL,
                raw_payload={"county": "Montgomery", "parcel": parcel, "amount": amount},
            ))
        self.logger.info(f"Montgomery: {len(leads)} leads parsed")
        return leads

    # ── Cheatham (HTML table) ───────────────────────────────────────────────

    def _scrape_cheatham(self) -> List[LeadPayload]:
        if BeautifulSoup is None:
            self.logger.warning("bs4 missing — skip Cheatham")
            return []
        self.logger.info(f"Cheatham: fetching {CHEATHAM_HTML_URL}")
        res = self.fetch(CHEATHAM_HTML_URL)
        if res is None or res.status_code != 200:
            self.logger.warning(f"Cheatham fetch failed: {res.status_code if res else 'none'}")
            return []

        soup = BeautifulSoup(res.text, "html.parser")
        leads: List[LeadPayload] = []

        # Table rows — usually columns: Map/Parcel, Min Bid, Details, Address
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue
            for row in rows[1:]:  # skip header
                cells = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cells) < 2:
                    continue
                # Best-effort: find a parcel-looking string + an address-looking string
                parcel = None
                address = None
                for cell in cells:
                    if not parcel and re.match(r"^\d+[A-Z]?\s*[\d\-\.]", cell):
                        parcel = cell
                    if not address and re.search(r"\d+\s+[A-Za-z]+", cell) and re.search(r"(rd|st|ave|dr|ln|blvd|ct|cir|pl|way|hwy)", cell, re.IGNORECASE):
                        address = cell
                if not parcel or not address:
                    continue
                full_address = f"{address}, Cheatham County, TN"
                leads.append(LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=self.make_lead_key(self.name, f"cheatham-{parcel}"),
                    property_address=full_address,
                    county="Cheatham County",
                    distress_type="TAX_LIEN",
                    admin_notes=f"Cheatham Co tax sale · parcel {parcel}",
                    source_url=CHEATHAM_HTML_URL,
                    raw_payload={"county": "Cheatham", "parcel": parcel, "row": cells},
                ))
        self.logger.info(f"Cheatham: {len(leads)} leads parsed")
        return leads


def run() -> dict:
    bot = TnTaxDelinquentBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
