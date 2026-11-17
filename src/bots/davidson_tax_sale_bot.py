"""Davidson County delinquent property tax sale lists (Clerk and Master).

Before each tax sale (monthly in 2026; next 2027-01-20) the Chancery Court
Clerk and Master posts the list of parcels as a text PDF, reissued several
times (_1 ... _5) as owners pay. Each row: owner, map/parcel, deed
book/page or instrument, property address, acres, amount due.

Why: an owner about to lose a property over a few thousand dollars in back
taxes, with equity left, has a deadline and a reason to sell first. After the
sale the owner keeps a one-year right of redemption (TCA 67-5-2701), so a
property on a past list is still worth a look.

Discovery: the site's WordPress media feed lists uploads newest first
(/wp-json/wp/v2/media?search=tax sale). robots.txt allows everything but
/wp-admin/. Only the newest revision of each sale's list is parsed.
Verified 2026-11-05.

Distress type TAX_LIEN; one lead per parcel, keyed by parcel number.
"""
from __future__ import annotations

import io
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pdfplumber

from ._base import BotBase, LeadPayload

BASE = "https://chanceryclerkandmaster.nashville.gov"
MEDIA = f"{BASE}/wp-json/wp/v2/media"
LOOKBACK_DAYS = 120

_REVISION_RE = re.compile(r"^(?P<sale>.+?Tax-Sale\d{4})_(?P<rev>\d+)(?:-\d+)?\.pdf$", re.I)
_SALE_DATE_RE = re.compile(r"\bON\s+[A-Z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.I)
_PARCEL_RE = re.compile(r"^[0-9]{5,}[0-9A-Z]*$")


def newest_revisions(media: List[Dict[str, Any]], today: date, lookback_days: int = LOOKBACK_DAYS) -> List[str]:
    """Newest revision URL of each sale's PDF uploaded within the lookback."""
    best: Dict[str, tuple] = {}
    cutoff = (today - timedelta(days=lookback_days)).isoformat()
    for m in media:
        url = m.get("source_url") or ""
        if m.get("mime_type") != "application/pdf" or (m.get("date") or "")[:10] < cutoff:
            continue
        match = _REVISION_RE.match(url.rsplit("/", 1)[-1])
        if not match:
            continue
        key = match.group("sale").lower()
        rank = (int(match.group("rev")), m.get("date") or "")
        if key not in best or rank > best[key][0]:
            best[key] = (rank, url)
    return [url for _, url in best.values()]


def _money(value: str) -> Optional[float]:
    v = re.sub(r"[^\d.]", "", value or "")
    try:
        return float(v) if v else None
    except ValueError:
        return None


def parse_sale_pdf(data: bytes) -> Dict[str, Any]:
    """{'sale_date': iso | None, 'rows': [{owner, parcel, deed, address, acres, amount}]}"""
    rows: List[Dict[str, Any]] = []
    sale_date = None
    with pdfplumber.open(io.BytesIO(data)) as pdf:
        for i, page in enumerate(pdf.pages):
            if i == 0:
                m = _SALE_DATE_RE.search(page.extract_text() or "")
                if m:
                    try:
                        sale_date = datetime.strptime(m.group(1).replace("  ", " "), "%B %d, %Y").date().isoformat()
                    except ValueError:
                        sale_date = None
            for table in page.extract_tables():
                for cells in table:
                    cells = [re.sub(r"\s+", " ", c or "").strip() for c in cells]
                    if len(cells) < 6 or not _PARCEL_RE.match(cells[1]):
                        continue
                    rows.append({
                        "owner": cells[0], "parcel": cells[1], "deed": cells[2], "address": cells[3],
                        "acres": _money(cells[4]), "amount_due": _money(cells[5]),
                    })
    return {"sale_date": sale_date, "rows": rows}


class DavidsonTaxSaleBot(BotBase):
    name = "davidson_tax_sale"
    description = "Davidson County Clerk and Master delinquent tax sale lists"
    throttle_seconds = 2.0
    expected_min_yield = 0  # months without a sale are normal (Sept-Dec 2026 cancelled)
    frozen_after_runs = 60

    def scrape(self) -> List[LeadPayload]:
        res = self.fetch(MEDIA, params={"search": "tax sale", "per_page": 50, "orderby": "date", "order": "desc",
                                        "_fields": "date,source_url,mime_type"})
        if res is None or res.status_code != 200:
            self.logger.warning("media feed fetch failed")
            return []
        urls = newest_revisions(res.json(), datetime.now(timezone.utc).date())
        leads: Dict[str, LeadPayload] = {}
        for url in urls:
            pdf = self.fetch(url)
            if pdf is None or pdf.status_code != 200:
                self.logger.warning(f"{url}: fetch failed")
                continue
            parsed = parse_sale_pdf(pdf.content)
            self.logger.info(f"{url.rsplit('/', 1)[-1]}: {len(parsed['rows'])} parcels, sale {parsed['sale_date']}")
            for row in parsed["rows"]:
                address = row["address"]
                if address and not re.match(r"^0\s", address):
                    address = f"{address}, NASHVILLE, TN"
                elif address:
                    address = f"{address} (no street number), NASHVILLE, TN"
                key = self.make_lead_key("davidson_tax_sale", row["parcel"])
                leads[key] = LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=key,
                    property_address=address or None,
                    county="davidson",
                    full_name=row["owner"],
                    owner_name_records=row["owner"],
                    distress_type="TAX_LIEN",
                    admin_notes=f"Davidson tax sale {parsed['sale_date'] or '?'}: parcel {row['parcel']}, "
                                f"due ${row['amount_due'] or 0:,.2f}, {row['acres'] or '?'} acres.",
                    raw_payload={"tax_sale": {**row, "sale_date": parsed["sale_date"], "list_url": url}},
                    source_url=url,
                )
        self.logger.info(f"{len(leads)} Davidson tax sale parcels from {len(urls)} list(s)")
        return list(leads.values())


def run() -> dict:
    return DavidsonTaxSaleBot().run()


if __name__ == "__main__":
    print(run())
