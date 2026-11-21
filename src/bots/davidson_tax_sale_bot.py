"""Davidson County delinquent property tax sale lists (Clerk and Master).

Before each tax sale (monthly in 2026; next 2027-01-20) the Chancery Court
Clerk and Master posts the list of parcels as a text PDF, reissued several
times (_1 ... _5) as owners pay. Each row: owner, map/parcel, deed
book/page or instrument, property address, acres, amount due.

Use dated parcel lists for upcoming sales. An expired listing alone does
not establish the sale outcome, current balance or redemption status.

Discovery: the site's WordPress media feed lists uploads newest first
(/wp-json/wp/v2/media?search=tax sale). robots.txt allows everything but
/wp-admin/. Only the newest revision of each sale's list is parsed.
Verified 2026-11-05.

Distress type TAX_LIEN; one lead per parcel, keyed by parcel number.
"""
from __future__ import annotations

import io
import json
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pdfplumber
from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload, _supabase
from ._davidson_tax_sale import final_revisions

BASE = "https://chanceryclerkandmaster.nashville.gov"
MEDIA = f"{BASE}/wp-json/wp/v2/media"
LOOKBACK_DAYS = 120

_SALE_DATE_RE = re.compile(r"\bON\s+[A-Z]+,\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.I)
_PARCEL_RE = re.compile(r"^[0-9]{5,}[0-9A-Z]*$")


def newest_revisions(media: List[Dict[str, Any]], today: date, lookback_days: int = LOOKBACK_DAYS) -> List[str]:
    """Newest revision URL of each sale's PDF uploaded within the lookback."""
    return final_revisions([m for m in media if m.get("mime_type") == "application/pdf"], today, lookback_days)


SCHEDULE = BASE + "/fees/property-tax-schedule/"


def scheduled_sales(html):
    dates = {}
    for heading in BeautifulSoup(html, "html.parser").select("h2"):
        text = " ".join(heading.stripped_strings)
        matches = re.findall(r"[A-Za-z]+\s+\d{1,2},\s*\d{4}", re.sub(r"(20\d)\s+(\d)\b", r"\1\2", text))
        if len(matches) != 2:
            continue
        try:
            day = datetime.strptime(matches[1], "%B %d, %Y").date().isoformat()
        except ValueError:
            continue
        dates[day] = "cancelled" if "CANCEL" in text.upper() else (
            "inactive" if heading.select_one('s,del,strike,[style*="line-through"]') else "scheduled")
    return dates


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
        today = date.today()
        self.schedule = {}
        self.listed = {}
        complete_lists = {}
        page = self.fetch(SCHEDULE)
        self.schedule = scheduled_sales(page.text) if page is not None and page.ok else {}
        if not any(day >= today.isoformat() and status == "scheduled" for day, status in self.schedule.items()):
            return []
        media = []
        for term in ("Tax-Sale", "Tax_Sale"):
            page_no = 1
            while True:
                res = self.fetch(MEDIA, params={"search": term, "per_page": 100, "page": page_no,
                                                "after": (today - timedelta(days=LOOKBACK_DAYS)).isoformat() + "T00:00:00",
                                                "_fields": "date,source_url,mime_type"})
                if res is None or not res.ok:
                    self.logger.warning("Incomplete tax sale media search")
                    return []
                batch = res.json()
                media.extend(batch)
                if len(batch) < 100 or page_no >= int(res.headers.get("X-WP-TotalPages") or page_no + 1):
                    break
                page_no += 1
        urls = newest_revisions(media, today)
        leads: Dict[str, LeadPayload] = {}
        for url in urls:
            pdf = self.fetch(url)
            if pdf is None or pdf.status_code != 200:
                self.logger.warning(f"{url}: fetch failed")
                continue
            parsed = parse_sale_pdf(pdf.content)
            self.logger.info(f"{url.rsplit('/', 1)[-1]}: {len(parsed['rows'])} parcels, sale {parsed['sale_date']}")
            day = parsed["sale_date"]
            if not day or day < today.isoformat() or self.schedule.get(day) != "scheduled" or not parsed["rows"]:
                continue
            complete_lists.setdefault(day, set()).update(row["parcel"] for row in parsed["rows"])
            for row in parsed["rows"]:
                address = row["address"]
                if address and not re.match(r"^0\s", address):
                    address = f"{address}, NASHVILLE, TN"
                elif address:
                    address = f"{address} (no street number), NASHVILLE, TN"
                due = f"${row['amount_due']:,.2f}" if row["amount_due"] is not None else "unverified"
                key = self.make_lead_key(self.name, f"{day}|{row['parcel']}")
                leads[key] = LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=key,
                    property_address=address or None,
                    county="davidson",
                    full_name=row["owner"],
                    owner_name_records=row["owner"],
                    distress_type="TAX_LIEN",
                    trustee_sale_date=day,
                    admin_notes=f"Davidson tax sale {parsed['sale_date'] or '?'}: parcel {row['parcel']}, "
                                f"published amount due {due}, {row['acres'] or '?'} acres.",
                    raw_payload={"tax_sale": {**row, "sale_date": parsed["sale_date"], "list_url": url}},
                    source_url=url,
                )
        self.listed = complete_lists
        self.logger.info(f"{len(leads)} Davidson tax sale parcels from {len(urls)} list(s)")
        return list(leads.values())

    def _write_staging(self, leads):
        result = super()._write_staging(leads)
        client = _supabase()
        if client is None:
            return result
        table = client.table("homeowner_requests_staging")
        current = {(l.trustee_sale_date, l.raw_payload["tax_sale"]["parcel"]): l for l in leads}
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            rows = (table.select("id,raw_payload").eq("bot_source", self.name).eq("distress_type", "TAX_LIEN")
                    .eq("staging_status", "pending").gt("id", after).order("id").limit(100).execute().data or [])
            for row in rows:
                raw = row["raw_payload"]
                facts = raw.get("tax_sale") or {}
                day, parcel = facts.get("sale_date"), facts.get("parcel")
                if not day:
                    continue
                lead = current.get((day, parcel))
                patch = {}
                if lead and lead.raw_payload["tax_sale"] != facts:
                    patch = {"raw_payload": {**raw, **lead.raw_payload}, "admin_notes": lead.admin_notes,
                             "source_url": lead.source_url, "trustee_sale_date": day}
                elif (day < date.today().isoformat() or self.schedule.get(day) in ("cancelled", "inactive")
                      or (day in self.listed and parcel not in self.listed[day])):
                    patch = {"staging_status": "rejected", "rejection_reason": "Tax sale listing no longer current"}
                if patch:
                    table.update(patch).eq("id", row["id"]).eq("staging_status", "pending").filter(
                        "raw_payload", "eq", json.dumps(raw, separators=(",", ":"))).execute()
            if len(rows) < 100:
                break
            after = rows[-1]["id"]
        return result


def run() -> dict:
    return DavidsonTaxSaleBot().run()


if __name__ == "__main__":
    print(run())
