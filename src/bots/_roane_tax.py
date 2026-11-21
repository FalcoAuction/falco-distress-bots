"""Recover individual parcels only from identifiable Roane real-property exhibits."""
import re
import sqlite3
from datetime import date

from ._base import BotBase, LeadPayload
from ._bk_match import situs_address
from .comptroller_refresh import BASE, DB_PATH

ROCKWOOD = re.compile(r"^City of Rockwood (\d{4}) Delinquent Real Property Taxes\b", re.I)
COUNTY = re.compile(r"^Exhibit A (\d{4}) Roane County Delinquent Tax .*?Real Property\b", re.I)
CITY_ID = re.compile(r"\b\d{2}-\s*\d{2,3}[A-Z]?\s*-\s*([A-Z]?)\s*-\s*(\d{2,3})([A-Z]?)-(\d{1,3})\.(\d{2})(?![-./\d])", re.I)
COUNTY_ID = re.compile(r"\b\d{1,2}/\d{1,3}[A-Z]?/([A-Z]?)/(\d{1,3})([A-Z]?)/(\d{1,3})(?:\.(\d{1,2}))?/(\d{1,3})(?![/.\d])", re.I)


class RoaneTaxLead(LeadPayload):
    def as_db_row(self, scraper_run_id):
        row = super().as_db_row(scraper_run_id)
        if self.property_value:
            row["property_value_source"] = "comptroller"
        return row


def exhibit_leads(row):
    """None means a different type of notice; an empty list means no complete IDs."""
    text = " ".join((row.get("excerpt") or "").split())
    city = ROCKWOOD.match(text)
    heading = city or COUNTY.match(text)
    if not heading:
        return None
    issuer = "Rockwood" if city else "Roane County"
    real = re.split(r"(?:real and |real & )?personal property tax", text, maxsplit=1, flags=re.I)[0]
    result = {}
    for m in (CITY_ID if city else COUNTY_ID).finditer(real):
        group, sheet, suffix, lot, decimal = m.groups()[:5]
        key = " ".join(filter(None, ["073", sheet.zfill(3) + suffix.upper(), group.upper(),
                                    lot.zfill(3) + (decimal or "").ljust(2, "0"),
                                    "000" if city else m[6].zfill(3)]))
        lead_key = BotBase.make_lead_key("roane_tax_exhibit", f"{issuer}|{heading[1]}|{key}")
        result[key] = RoaneTaxLead(
            bot_source="tn_public_notice", pipeline_lead_key=lead_key, county="roane", distress_type="TAX_LIEN",
            source_url=f"https://www.tnpublicnotice.com/Details.aspx?ID={row['notice_id']}",
            raw_payload={"kind": "roane_tax_exhibit", "issuer": issuer, "tax_year": heading[1],
                         "parcel": key, "printed_parcel": m[0], "publication_date": row.get("pub_date"),
                         "notice_id": row["notice_id"]})
    return list(result.values())


def fill_roll(leads, path=DB_PATH, year=None):
    if not leads or not path.is_file():
        return
    year = year or date.today().year
    wanted = {lead.raw_payload["parcel"] for lead in leads}
    hits = {}
    con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    try:
        for row in con.execute("select * from parcels where county='ROANE' and parcelid like ?", (f"% {year}",)):
            key = " ".join(row["parcelid"].split()[:-1])
            if key in wanted:
                hits[key] = None if key in hits else row
    finally:
        con.close()
    for lead in leads:
        row = hits.get(lead.raw_payload["parcel"])
        if row is None:
            continue
        lead.raw_payload["assessment"] = {"parcel_id": row["parcelid"], "source_url": BASE + "Roane.zip"}
        lead.owner_name_records = row["owner"] or None
        lead.property_value = row["appraisal"] if (row["appraisal"] or 0) > 0 else None
        if not re.fullmatch(r"(?:US |STATE )?(?:HWY|ROUTE|HIGHWAY)\s+\d+[A-Z]?", (row["address"] or "").strip(), re.I):
            addr = situs_address(row)
            if addr and re.match(r"^[1-9]\d*[A-Z]?\s", addr):
                lead.property_address = addr
