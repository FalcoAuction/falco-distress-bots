"""Property evidence for receiver-roster parcels also advertised by the county Trustee."""
import base64
import json
import math
from urllib.parse import parse_qs, urlsplit

from bs4 import BeautifulSoup

CLASS_RATIOS = {"R": 0.25, "C": 0.40, "I": 0.40}
RATIO_SOURCE = "https://www.shelbycountytrustee.com/208/Calculate-Taxes"
SUFFIX = {"STREET":"ST", "ROAD":"RD", "AVENUE":"AVE", "DRIVE":"DR", "LANE":"LN", "PLACE":"PL",
          "COURT":"CT", "CIRCLE":"CIR", "COVE":"CV", "BOULEVARD":"BLVD", "PARKWAY":"PKWY"}


def street_matches(roster, county):
    """The Trustee usually omits the suffix. Preserve house number, direction and unit."""
    def tokens(s):
        parts = s.upper().replace(".", "").split()
        if len(parts) > 2 and parts[1] == "SAINT":
            parts[1] = "ST"
        if parts:
            parts[-1] = SUFFIX.get(parts[-1], parts[-1])
        return parts
    a, b = tokens(roster), tokens(county)
    return bool(len(b) >= 2 and b[0].isdigit() and
                (a == b or (len(a) == len(b) + 1 and a[-1] in SUFFIX.values() and a[:-1] == b)))


def property_record(html, case, listing, day):
    page = BeautifulSoup(html, "html.parser")
    words = list(page.stripped_strings)
    def field(label):
        if words.count(label) == 1:
            i = words.index(label)
            if i+1 < len(words):
                return words[i+1]
        return ""
    parcel = listing["parcel"]
    location = field("Location")
    evidence = {"checked_on": day.isoformat(), "source_url": listing["source_url"], "parcel": parcel}
    if ("".join(case["parcel"].split()) != "".join(parcel.split()) or field("Parcel ID:") != parcel
            or field("Tax Sale ID:") != listing["sale_id"] or not street_matches(case["address"], location)
            or not street_matches(case["address"], listing["trustee_street"])):
        return {**evidence, "status": "identity_conflict"}
    evidence.update(county_situs=location, status="current_bill_unverified")
    rows = []
    for tr in page.select("tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        if len(cells) == 8 and cells[1:3] == [str(day.year), "Shelby"]:
            rows.append((cells, tr.select_one('a[href*="billNumberData="]')))
    if len(rows) != 1 or not rows[0][1]:
        return evidence
    cells, link = rows[0]
    try:
        encoded, = parse_qs(urlsplit(link["href"]).query)["billNumberData"]
        bill = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
    except (KeyError, ValueError, UnicodeError):
        return evidence
    if (not isinstance(bill, dict) or bill.get("paymentType") != 20 or "REALTY" not in str(bill.get("description", "")).upper()
            or str(bill.get("year")) != cells[1] or str(bill.get("billNumber")) != cells[0]
            or str(bill.get("parcel") or "").strip() != parcel or bill.get("altParcelId") != listing["alt_parcel"]
            or str(bill.get("location") or "").strip() != location):
        return evidence
    evidence.update(status="matched_county_bill", tax_year=day.year, bill_number=cells[0],
                    owner_status="unverified", value_status="assessment_unverified")
    owner = field("Primary Owner")
    if owner and owner.upper() not in {"N/A","UNKNOWN","LOCATION","WITHHELD","REDACTED"} and owner == str(bill.get("primaryName") or "").strip():
        evidence.update(owner_status="trustee_primary_owner", tax_roll_owner=owner)
    assessed = bill.get("assessment")
    if (type(assessed) in (int,float) and math.isfinite(assessed) and assessed > 0 and cells[7] == f"${assessed:,.2f}"):
        code = str(bill.get("classification") or "").strip()
        evidence.update(assessed_value=assessed, tax_class=code, value_status="assessed_only")
        if code in CLASS_RATIOS:
            ratio = CLASS_RATIOS[code]
            evidence.update(assessment_ratio=ratio, estimated_appraisal=round(assessed/ratio),
                            value_status="county_assessment_divided_by_class_ratio", ratio_source_url=RATIO_SOURCE)
    return evidence
