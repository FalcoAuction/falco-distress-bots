"""County-specific non-tax headings found while reviewing newspaper tax categories."""
import re
import sqlite3
from datetime import date
from ._base import BotBase, LeadPayload
from ._bk_match import situs_address
from .comptroller_refresh import BASE, DB_PATH


HEADINGS = {
    "sullivan": (
        "FY24 Spent FY25 Adopted FY26 Proposed FY27 Difference Percent",
        "NOTICE OF SALE-JUDICIAL FORECLOSURE In the Matter of: Renasant Bank",
        "BEFORE THE BETTER PROPERTY BOARD OF THE CITY OF BRISTOL",
        "REGULAR MEETING & PUBLIC HEARING Kingsport Board of Zoning Appeals",
        "PUBLIC NOTICE OF ORDER OF THE BUILDING OFFICIAL",
    ),
    "sevier": (
        "TO THE VOTERS OF SEVIER COUNTY", "Sevier County Government Public Meeting Notice",
        "PUBLIC NOTICE PLANNING AND SUBDIVISION REVIEW FOR UNINCORPOR",
        "Sevier County Government Public Notice Notice of Intent to Adopt Prop- erty Tax Rate",
        "NOTICE OF SALE IN THE CHANCERY COURT FOR SEVIER COUNTY, TENNESSEE ENGLISH MOUNTAIN CONDOMINIUM ASSOCIATION",
    ),
    "knox": ("NOTICE Release from the Knox County Property Assessor Phil Ballard", "G26-033 Public Comments"),
    "washington": ("ORDINANCE NO. 4937-26 AN ORDINANCE TO AMEND THE ZONING MAP",
                   "ORDINANCE 4938-26 A-1 AN ORDINANCE TO AMEND THE ZONING MAP"),
    "blount": ("FY 2025 FY 2026 FY 2027 FY 2025 FY 2026",
               "PUBLIC NOTICE - CITY OF ALCOA The City of Alcoa, Tennessee hereby provides certain financial information",
               "REQUEST FOR PROPOSALS PROJECT: Development of a portion of the property located in Laurel Branch Park",
               "NOTICE OF MEETING The Blount County Planning Commission"),
    "henry": ("CITY OF PURYEAR PROPOSED BUDGET",),
    "lawrence": ("PUBLIC NOTICE: CITY OF LAWRENCEBURG, NOTICE OF PROPOSED ZONING",
                 "PUBLIC NOTICE: CITY OF LAWRENCEBURG, NOTICE OF ZONING BOARD",
                 "CITY OF LORETTO, TENNESSEE PUBLIC NOTICE The City of Loretto, Tennessee, hereby provides certain financial information",
                 "IN THE CHANCERY COURT FOR LAWRENCE COUNTY, TENNESSEE IN THE MATTER OF THE ESTATE OF MONETTA SPRINGER"),
    "madison": ("Public Notice A Complaint to Quiet Title TO: ANY UNKNOWN HEIRS OF MARY L. HUNT",),
}


def non_tax_reason(county, excerpt):
    text = " ".join(excerpt.lower().split())
    if any(text.startswith(prefix.lower()) for prefix in HEADINGS.get(county, ())):
        if not re.search(r"\b(?:delinquent (?:property |real property )?tax|tax sale|back taxes)", text):
            return "Municipal, private or non-tax proceeding identified in notice"
    if county in {"mcnairy", "washington", "henry"} and "67-5-508" in text and "assessment records" in text:
        return "General assessment inspection notice"
    if county == "sullivan" and text.startswith("notice of proposed sale") and "acquired by sullivan" in text:
        return "Government resale; county has already acquired the parcel"
    return None


class CountyTaxParcel(LeadPayload):
    def as_db_row(self, scraper_run_id):
        data = super().as_db_row(scraper_run_id)
        if self.property_value:
            data["property_value_source"] = "comptroller"
        return data


def fill_parcels(leads, path=DB_PATH):
    if not leads or not path.is_file():
        return
    wanted = {lead.raw_payload["parcel"] for lead in leads}
    county = leads[0].county
    con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    records = {}
    try:
        for row in con.execute("SELECT * FROM parcels WHERE county=?", (county.upper(),)):
            parts = row["parcelid"].split()
            key, year = " ".join(parts[:-1]), parts[-1]
            if key in wanted and year.isdigit():
                versions = records.setdefault(key, {})
                versions[year] = None if year in versions else row
    finally:
        con.close()
    for lead in leads:
        versions = records.get(lead.raw_payload["parcel"], {})
        if not versions:
            continue
        year = str(date.today().year)
        # Some counties publish only the next year's roll. Preserve its actual year.
        row = versions.get(year if year in versions else max(versions))
        if row is None:
            continue
        lead.raw_payload["assessment"] = {"parcel_id": row["parcelid"], "roll_year": row["parcelid"].split()[-1],
                                          "url": BASE + county.title() + ".zip"}
        lead.owner_name_records = row["owner"] or None
        lead.property_value = row["appraisal"] if (row["appraisal"] or 0) > 0 else None
        if not re.fullmatch(r"(?:STATE |US )?(?:HWY|HIGHWAY|ROUTE)\s+\d+[A-Z]?", (row["address"] or "").strip(), re.I):
            address = situs_address(row)
            if address and re.match(r"^[1-9]\d*[A-Z]?\s", address):
                lead.property_address = address


def parcel_rows(county, row):
    """Only split tables whose complete parcel identifier is printed in the excerpt."""
    text = " ".join((row.get("excerpt") or "").split())
    found = []
    if county == "madison" and text.startswith("Tax yr. Bill # Dist Map# GP CtrlMap Parcel SI"):
        for m in re.finditer(r"\b(20\d\d) (\d+) \d+ (\d+[A-Z]?) (?:([A-Z]) )?(\d+[A-Z]?) "
                             r"(\d{1,3})(?:\.(\d{1,2}))? (\d{1,3}) [^$]+? \$([\d,]+\.\d\d)", text):
            year, bill, _, group, control, lot, fraction, special, debt = m.groups()
            sheet = re.fullmatch(r"(\d+)([A-Z]?)", control)
            key = " ".join(filter(None, ("057", sheet[1].zfill(3) + sheet[2], group,
                                         lot.zfill(3) + (fraction or "").ljust(2, "0"), special.zfill(3))))
            found.append((key, year + "|" + bill, {"tax_year": year, "bill": bill, "published_amount": debt}))
    elif county == "henry" and text.startswith("ALSO, TAKE NOTICE THAT THE FOLLOWING ADDITIONAL PARCELS ARE CURRENTLY DELINQUENT"):
        for m in re.finditer(r"\b\d\d-\d{3}\s*[A-Z]?\s*-\s*([A-Z]?)\s*-\s*(\d{3})\s*([A-Z]?)\s*-\s*(\d{3})\.(\d\d)-\s*-(\d{3})\b", text):
            group, sheet, block, lot, fraction, special = m.groups()
            key = " ".join(filter(None, ("040", sheet + block, group, lot + fraction, special)))
            found.append((key, str(row.get("pub_date") or "")[-4:], {"printed_parcel": m[0]}))
    else:
        return None
    return [CountyTaxParcel(
        bot_source="tn_public_notice", county=county, distress_type="TAX_LIEN",
        pipeline_lead_key=BotBase.make_lead_key("county_tax_table", f"{county}|{identity}|{key}"),
        source_url=f"https://www.tnpublicnotice.com/Details.aspx?ID={row['notice_id']}",
        raw_payload={"kind": "county_tax_table", "parcel": key, "publication_date": row.get("pub_date"),
                     "notice_id": row["notice_id"], **facts})
        for key, identity, facts in found]
