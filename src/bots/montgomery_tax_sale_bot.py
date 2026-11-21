"""Montgomery County Clerk and Master tax sale notices.
Stage upcoming listings. Use completed sale results to retire matching
listings. The county result identifies a buyer and price, but supplies
neither current title nor a parcel-specific redemption deadline.
"""
from __future__ import annotations

import io
import json
import re
import zipfile
import xml.etree.ElementTree as ET
from datetime import date, datetime
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from ._base import BotBase, LeadPayload, _supabase

PAGE = "https://montgomerytn.gov/chancery/tax-sale"
_NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
_LINK_RE = re.compile(r'href="([^"]*/storage/departments/chancery/[^"]*\.xlsx)"', re.I)
_SALE_RE = re.compile(r"(\d{4})\s+Tax\s+Sale\s+(was|will\s+be)\s+(?:held\s+)?on\s+(?:[A-Za-z]+,\s+)?([A-Za-z]+\s+\d{1,2},\s+\d{4})", re.I)


def _col(ref: str) -> int:
    n = 0
    for ch in re.match(r"[A-Z]+", ref).group(0):
        n = n * 26 + ord(ch) - 64
    return n - 1


def read_xlsx(data: bytes) -> List[List[Any]]:
    """Rows of the first sheet as lists (shared strings resolved, numbers as float)."""
    z = zipfile.ZipFile(io.BytesIO(data))
    shared: List[str] = []
    if "xl/sharedStrings.xml" in z.namelist():
        for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", _NS):
            shared.append("".join(t.text or "" for t in si.iter(f"{{{_NS['m']}}}t")))
    sheet = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
    rows: List[List[Any]] = []
    for r in sheet.iter(f"{{{_NS['m']}}}row"):
        cells: Dict[int, Any] = {}
        for c in r.findall("m:c", _NS):
            t, v = c.get("t"), c.find("m:v", _NS)
            if t == "inlineStr":
                val: Any = "".join(x.text or "" for x in c.iter(f"{{{_NS['m']}}}t"))
            elif v is None:
                continue
            elif t == "s":
                val = shared[int(v.text)]
            elif t in ("str", "b", "e"):
                val = v.text
            else:
                try:
                    val = float(v.text)
                except (TypeError, ValueError):
                    val = v.text
            cells[_col(c.get("r"))] = val
        if cells:
            rows.append([cells.get(i) for i in range(max(cells) + 1)])
    return rows


def _money(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    nums = re.findall(r"\d[\d,]*(?:\.\d+)?", str(value or ""))
    return sum(float(n.replace(",", "")) for n in nums) if nums else None


def _text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value if value is not None else "")).strip()


def parse_rows(rows: List[List[Any]]) -> List[Dict[str, Any]]:
    """Header-driven; rows without an owner carry more tax years for the parcel above."""
    header_at = next((i for i, r in enumerate(rows) if any("parcel" in _text(c).lower() for c in r)), None)
    if header_at is None:
        return []
    names = [_text(c).lower() for c in rows[header_at]]

    def idx(*words: str) -> Optional[int]:
        return next((i for i, n in enumerate(names) if all(w in n for w in words)), None)

    i_owner, i_parcel, i_addr = idx("owner"), idx("parcel"), idx("address")
    i_year, i_tax, i_deed = idx("tax", "year"), idx("base", "tax"), idx("deed")
    i_sold, i_buyer = idx("sold"), idx("purchas")

    def get(r: List[Any], i: Optional[int]) -> Any:
        return r[i] if i is not None and i < len(r) else None

    out: List[Dict[str, Any]] = []
    for r in rows[header_at + 1:]:
        owner, parcel = _text(get(r, i_owner)), _text(get(r, i_parcel))
        year, tax = _text(get(r, i_year)), _money(get(r, i_tax))
        if not owner and not parcel:
            if out and (year or tax):
                if year:
                    out[-1]["tax_years"].append(year)
                out[-1]["base_tax"] = round((out[-1]["base_tax"] or 0) + (tax or 0), 2)
            continue
        if not parcel:
            continue
        out.append({"owner": owner or None, "parcel": parcel, "address": _text(get(r, i_addr)) or None,
                    "tax_years": [year] if year else [], "base_tax": tax, "deed": _text(get(r, i_deed)) or None,
                    "sold_for": _money(get(r, i_sold)), "buyer": _text(get(r, i_buyer)) or None})
    return out


def sale_info(html: str) -> Dict[str, Any]:
    """{'sale_date': iso | None, 'sold': bool} from the page text."""
    text = re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", html or ""))
    headings = list(_SALE_RE.finditer(text))
    if len(headings) != 1:
        return {"sale_date": None, "sold": False}
    m = headings[0]
    try:
        d = datetime.strptime(m.group(3), "%B %d, %Y").date().isoformat()
    except ValueError:
        d = None
    return {"sale_date": d, "sold": m.group(2).lower() == "was"}


def listing_status(row, info, today):
    if not info.get("sale_date"):
        return "unknown"
    if info["sold"] and info["sale_date"] <= today and row.get("buyer") and (row.get("sold_for") or 0) > 0:
        return "sold"
    if info["sold"] or row.get("buyer") or row.get("sold_for") or info["sale_date"] < today:
        return "unknown"
    return "upcoming"


class MontgomeryTaxSaleBot(BotBase):
    name = "montgomery_tax_sale"
    description = "Montgomery County upcoming tax listings and completed sale results"
    throttle_seconds = 2.0
    expected_min_yield = 0  # one sale a year; the list can be absent between sales
    frozen_after_runs = 60

    def scrape(self) -> List[LeadPayload]:
        self.sold_rows = {}
        page = self.fetch(PAGE)
        if page is None or page.status_code != 200:
            self.logger.warning("tax sale page fetch failed")
            return []
        links = list(dict.fromkeys(urljoin(PAGE, m.group(1)) for m in _LINK_RE.finditer(page.text)))
        info = sale_info(page.text)
        if len(links) != 1 or not info["sale_date"]:
            self.logger.warning("Cannot associate a single tax workbook with a dated sale")
            return []
        leads: Dict[str, LeadPayload] = {}
        for url in links:
            res = self.fetch(url)
            if res is None or res.status_code != 200:
                self.logger.warning(f"{url}: fetch failed")
                continue
            try:
                rows = parse_rows(read_xlsx(res.content))
            except (zipfile.BadZipFile, KeyError, ET.ParseError) as e:
                self.logger.warning(f"{url}: unreadable workbook ({e})")
                continue
            self.logger.info(f"{url.rsplit('/', 1)[-1]}: {len(rows)} parcels, sale {info['sale_date']}")
            for row in rows:
                status = listing_status(row, info, date.today().isoformat())
                facts = {**row, "sale_date": info["sale_date"], "sold": status == "sold", "list_url": url}
                if status == "sold":
                    self.sold_rows[row["parcel"]] = facts
                if status != "upcoming":
                    continue
                street = row["address"] or ""
                address = f"{street}, Montgomery County, TN" if re.match(r"^\d", street) else None
                key = self.make_lead_key(self.name, f"{info['sale_date']}|{row['parcel']}")
                leads[key] = LeadPayload(
                    bot_source=self.name,
                    pipeline_lead_key=key,
                    property_address=address,
                    county="montgomery",
                    full_name=row["owner"],
                    owner_name_records=row["owner"],
                    distress_type="TAX_LIEN",
                    trustee_sale_date=info["sale_date"],
                    admin_notes=f"Montgomery tax sale {info['sale_date'] or '?'} ({status}): parcel {row['parcel']}, "
                                f"base tax ${row['base_tax'] or 0:,.2f} for {', '.join(row['tax_years']) or '?'}"
                                    + ".",
                    raw_payload={"tax_sale": facts},
                    source_url=url,
                )
        self.logger.info(f"{len(leads)} Montgomery tax sale parcels from {len(links)} list(s)")
        return list(leads.values())

    def _write_staging(self, leads):
        result = super()._write_staging(leads)
        sold = getattr(self, "sold_rows", {})
        if not sold:
            return result
        client = _supabase()
        if client is None:
            return result
        # The original collector keyed rows by parcel alone. Match the sale
        # date as well so a later proceeding on the same parcel stays intact.
        date_key = next(iter(sold.values()))["sale_date"]
        parcels = list(sold)
        table = client.table("homeowner_requests_staging")
        for start in range(0, len(parcels), 50):
            rows = (table.select("id,raw_payload").eq("bot_source", self.name)
                    .eq("distress_type", "TAX_LIEN").eq("staging_status", "pending")
                    .eq("raw_payload->tax_sale->>sale_date", date_key)
                    .in_("raw_payload->tax_sale->>parcel", parcels[start:start + 50]).execute().data or [])
            for row in rows:
                raw = row["raw_payload"]
                facts = sold[raw["tax_sale"]["parcel"]]
                table.update({"raw_payload": {**raw, "tax_sale": facts}, "staging_status": "rejected",
                              "admin_notes": f"County reports parcel {facts['parcel']} sold on {date_key}.",
                              "rejection_reason": "Completed Montgomery County tax sale"}).eq(
                                  "id", row["id"]).eq("staging_status", "pending").filter(
                                      "raw_payload", "eq", json.dumps(raw, separators=(",", ":"))).execute()
        return result


def run() -> dict:
    return MontgomeryTaxSaleBot().run()


if __name__ == "__main__":
    print(run())
