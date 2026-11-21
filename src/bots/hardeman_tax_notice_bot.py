"""Hardeman court tax suits from the Bulletin Times' full legal editions."""
import io
import re
import sqlite3
from datetime import datetime
from urllib.parse import urljoin, urlsplit

import pdfplumber
from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload
from ._bk_match import situs_address
from .comptroller_refresh import BASE, DB_PATH

HUB = "https://www.bulletintimesnews.com/current-legal-notices"
REAL = re.compile(r"^(?:HARDEMAN COUNTY|CITY OF BOLIVAR), TENNESSEE\s*[-–—]\s*REAL PROPERTY TAXES$", re.I)
PARCEL = re.compile(r"\d{2}-(\d{3}[A-Z]?)(?:/([A-Z]))?(?:/(\d{3}[A-Z]))?[/ ](\d{1,3})\.(\d{2})(?:-(\d{3}))?")


def exhibit_parcels(lines):
    """Read only explicitly real-property tables in a single Hardeman tax case."""
    text = "\n".join(lines)
    cases = set(re.findall(r"CIVIL ACTION NO\.\s*(\d+)\b", text, re.I))
    years = set(re.findall(r"certified herein for the year (\d{4})", text, re.I))
    if len(cases) != 1 or len(years) != 1 or "HARDEMAN COUNTY and all MUNICIPALITIES" not in text:
        return {}
    found = {}
    active = False
    for i, line in enumerate(lines):
        if "PROPERTY TAXES" in line.upper() and ("COUNTY" in line or "CITY OF" in line):
            active = bool(REAL.fullmatch(line))
            continue
        if not active:
            continue
        if line.endswith("/") and i + 1 < len(lines):
            line += lines[i + 1]
        m = PARCEL.fullmatch(line)
        if m:
            map_id, group, control, number, fraction, special = m.groups()
            key = " ".join(filter(None, ("035", control or map_id, group,
                                        number.zfill(3) + fraction, special or "000")))
            found[key] = (next(iter(cases)), next(iter(years)), line)
    return found


def pdf_lines(data):
    # These editions print multiple columns. The PDF draws each exhibit cell in
    # reading order; sorting by vertical position would mix unrelated tables.
    lines = []
    with pdfplumber.open(io.BytesIO(data)) as doc:
        for page in doc.pages:
            prev = None
            for ch in page.chars:
                if (prev is None or abs(ch["top"] - prev["top"]) > 2 or
                        ch["x0"] < prev["x0"] - 3 or ch["x0"] - prev["x1"] > 12):
                    lines.append("")
                lines[-1] += ch["text"]
                prev = ch
    return [line.strip() for line in lines if line.strip()]


class HardemanTaxLead(LeadPayload):
    def as_db_row(self, scraper_run_id):
        row = super().as_db_row(scraper_run_id)
        if self.property_value:
            row["property_value_source"] = "comptroller"
        return row


def fill_roll(leads, path, year):
    if not leads:
        return
    con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        records = {}
        for row in con.execute("SELECT * FROM parcels WHERE county='HARDEMAN' AND parcelid LIKE ?",
                               (f"% {year}",)):
            key = " ".join(row["parcelid"].split()[:-1])
            records[key] = None if key in records else row
    finally:
        con.close()
    for lead in leads:
        row = records.get(lead.raw_payload["parcel"])
        if row is None:
            continue
        lead.raw_payload["assessment"] = {"parcel_id": row["parcelid"], "year": year,
                                          "source_url": BASE + "Hardeman.zip"}
        lead.owner_name_records = row["owner"] or None
        lead.property_value = row["appraisal"] if (row["appraisal"] or 0) > 0 else None
        if re.fullmatch(r"(?:STATE |US )?(?:HWY|HIGHWAY|ROUTE)\s+\d+[A-Z]?", (row["address"] or "").strip(), re.I):
            continue  # Highway number only.
        addr = situs_address(row)
        if addr and re.match(r"^[1-9]\d*[A-Z]?\s", addr):
            lead.property_address = addr


class HardemanTaxNoticeBot(BotBase):
    name = "hardeman_tax_notice"
    expected_min_yield = 0

    def scrape(self):
        hub = self.fetch(HUB)
        if hub is None or not hub.ok:
            raise ValueError("Hardeman legal editions unavailable")
        issues = {}
        for a in BeautifulSoup(hub.content, "html.parser").select("a[href]"):
            title = a.get_text(" ", strip=True)
            if not re.fullmatch(r"[A-Za-z]+ \d{1,2}, \d{4} Legal\.pdf", title, re.I):
                continue
            url = urljoin(HUB, a["href"])
            if urlsplit(url).netloc == urlsplit(HUB).netloc:
                issues[url] = datetime.strptime(title.rsplit(" ", 1)[0], "%B %d, %Y").date()
        if not issues:
            raise ValueError("No dated Hardeman legal editions found")
        leads = {}
        for url, issued in sorted(issues.items(), key=lambda item: item[1]):
            pdf = self.fetch(url)
            if pdf is None or not pdf.ok:
                raise ValueError("Hardeman legal edition unavailable")
            for parcel, (case, year, printed) in exhibit_parcels(pdf_lines(pdf.content)).items():
                key = self.make_lead_key(self.name, f"{case}|{parcel}")
                leads[key] = HardemanTaxLead(
                    bot_source=self.name, pipeline_lead_key=key, county="hardeman", distress_type="TAX_LIEN",
                    source_url=url, raw_payload={"case_number": case, "tax_year": year, "parcel": parcel,
                    "printed_parcel": printed, "publication_date": issued.isoformat(), "kind": "tax_collection_suit"})
        result = list(leads.values())
        fill_roll(result, DB_PATH, datetime.now().year)
        return result


def run():
    return HardemanTaxNoticeBot().run()
