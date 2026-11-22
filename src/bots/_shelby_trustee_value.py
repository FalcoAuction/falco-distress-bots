"""Parcel-bound county tax assessments; derived appraisal is not a market valuation."""
import base64
import json
import re
from datetime import date
from urllib.parse import parse_qs, urlencode, urlparse

from bs4 import BeautifulSoup

HOST = "https://apps2.shelbycountytrustee.com"
REVIEW = "shelby_trustee_value_review"
VALUE_SOURCE = "shelby_trustee_assessment_estimate"
RATIOS = {"R": .25, "C": .40, "I": .40}
SUFFIXES = {"ST", "STREET", "RD", "ROAD", "DR", "DRIVE", "AVE", "AVENUE",
            "BLVD", "BOULEVARD", "CV", "COVE", "LN", "LANE", "CIR", "CIRCLE",
            "CT", "COURT", "WAY", "LNDG", "LANDING", "MALL"}
DIRECTIONS = {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}


def parcel_key(value):
    return re.sub(r"[\s-]", "", str(value or "")).upper()


def street_key(value):
    # The trustee omits suffix/direction and condo unit from billLocation.
    # Units therefore require an independently matching full notice parcel.
    head = str(value or "").upper().split(",")[0]
    head = re.split(r"\b(?:UNIT|APT|SUITE)\b|#", head)[0]
    tokens = head.replace(".", "").split()
    if tokens and tokens[-1] == "STA":
        tokens[-1] = "STATION"
    tokens = [t for t in tokens if t not in DIRECTIONS]
    if tokens and tokens[-1] in SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


def select_parcel(data, address, notice_parcel, as_of):
    """Never choose the first hit: restrict to current realty and exact street."""
    wanted = street_key(address)
    candidates = {}
    for bill in data.get("parcelData", []):
        if (bill.get("billYear") != as_of.year or bill.get("billCategory") != 20
                or "REALTY" not in (bill.get("accountsReceivableCode") or {}).get("description", "")
                or street_key(bill.get("billLocation")) != wanted):
            continue
        key = parcel_key(bill.get("parcel"))
        if key:
            candidates.setdefault(key, []).append(bill)
    notice = parcel_key(notice_parcel)
    full_notice = bool(re.fullmatch(r"[A-Z0-9]{6}[A-Z]?[0-9]{5}", notice))
    if full_notice:
        if notice not in candidates:
            return {"status": "notice_parcel_conflict" if candidates else "no_current_match"}
        candidates = {notice: candidates[notice]}
    elif re.search(r"\b(?:UNIT|APT|SUITE)\b|#", address, re.I):
        return {"status": "unit_requires_parcel"}
    if len(candidates) != 1:
        return {"status": "ambiguous_parcels" if candidates else "no_current_match",
                "candidate_parcels": sorted(candidates)}
    bills = next(iter(candidates.values()))
    identities = {(b.get("billId"), (b.get("customer") or {}).get("primaryName")) for b in bills}
    if len(identities) != 1:
        return {"status": "conflicting_current_bills"}
    bill = bills[0]
    return {"status": "matched", "parcel_id": bill["parcel"].strip(),
            "location": bill["billLocation"], "owner": (bill.get("customer") or {}).get("primaryName"),
            "bill_number": bill["billNumber"], "tax_year": as_of.year,
            "match_method": "notice_parcel_and_street" if full_notice else "unique_county_street"}


def parcel_value(html, match, as_of):
    page = BeautifulSoup(html, "html.parser")
    words = list(page.stripped_strings)
    def field(label):
        return words[words.index(label) + 1] if label in words and words.index(label)+1 < len(words) else ""
    if (parcel_key(field("Parcel ID:")) != parcel_key(match["parcel_id"])
            or street_key(field("Location")) != street_key(match["location"])
            or field("Primary Owner") != match["owner"]):
        return {**match, "status": "detail_identity_conflict"}
    values = []
    for tr in page.select("tr"):
        cells = [td.get_text(strip=True) for td in tr.select("td")]
        link = tr.select_one('a[href*="billNumberData="]')
        if len(cells) != 8 or cells[1:3] != [str(as_of.year), "Shelby"] or not link:
            continue
        try:
            encoded = parse_qs(urlparse(link["href"]).query)["billNumberData"][0]
            bill = json.loads(base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4)))
        except (ValueError, KeyError, UnicodeError):
            continue
        amount = bill.get("assessment")
        code = str(bill.get("classification") or "").strip()
        if (bill.get("paymentType") != 20 or "REALTY" not in str(bill.get("description", ""))
                or parcel_key(bill.get("parcel")) != parcel_key(match["parcel_id"])
                or street_key(bill.get("location")) != street_key(match["location"])
                or bill.get("primaryName") != match["owner"]
                or str(bill.get("billNumber")) != str(match["bill_number"])
                or str(bill.get("year")) != str(as_of.year)
                or type(amount) not in (int, float) or amount <= 0
                or cells[7] != f"${amount:,.2f}" or code not in RATIOS):
            continue
        values.append((amount, code))
    if len(set(values)) != 1:
        return {**match, "status": "assessment_unverified"}
    amount, code = values[0]
    return {**match, "status": "verified_assessment", "assessed_value": amount,
            "tax_class": code, "assessment_ratio": RATIOS[code],
            "estimated_appraisal": round(amount / RATIOS[code]),
            "valuation_method": "county_tax_assessment_divided_by_class_ratio"}


def lookup(bot, row, as_of=None):
    as_of = as_of or date.today()
    address = row.get("property_address") or ""
    raw = row.get("raw_payload") or {}
    notice_parcel = (raw.get("extracted") or {}).get("parcel")
    parts = street_key(address).split()
    evidence = {"checked_on": as_of.isoformat(), "county": "shelby",
                "notice_key": row["pipeline_lead_key"], "notice_address": address}
    if len(parts) < 2 or not parts[0].isdigit():
        return {**evidence, "status": "invalid_address"}
    params = {"type": "property", "location": " ".join(parts[:2])}
    evidence["search_url"] = HOST + "/api/search?" + urlencode(params)
    response = bot.fetch(HOST + "/api/search", params=params, timeout=(5, 15))
    if response is None or response.status_code != 200:
        return {**evidence, "status": "source_unavailable"}
    try:
        data = response.json()
        if not isinstance(data, dict) or not isinstance(data.get("parcelData"), list):
            raise ValueError("Invalid county search")
        match = select_parcel(data, address, notice_parcel, as_of)
    except (ValueError, TypeError):
        return {**evidence, "status": "source_unavailable"}
    if match["status"] != "matched":
        return {**evidence, **match}
    url = HOST + "/Parcel?" + urlencode({"parcel": match["parcel_id"]})
    response = bot.fetch(url, timeout=(5, 15))
    if response is None or response.status_code != 200:
        return {**evidence, **match, "status": "source_unavailable", "source_url": url}
    return {**evidence, **parcel_value(response.text, match, as_of), "source_url": url}
