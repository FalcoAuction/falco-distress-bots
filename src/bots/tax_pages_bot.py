from bs4 import BeautifulSoup

from ..config import SEED_URLS_COUNTY_TAX, TAX_KEYWORDS
from ..utils import (
    fetch, contains_any, find_date_iso, guess_county,
    extract_contact, extract_address, extract_trustee_or_attorney,
    make_lead_key
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..storage import sqlite_store as _store
from ..scoring.days_to_sale import days_to_sale
from ..scoring.detect_risk_flags import detect_risk_flags
from ..scoring.triage import triage
from ..scoring.score_v2 import score_v2
from ..scoring.label import label


def run():
    if not SEED_URLS_COUNTY_TAX:
        return

    for url in SEED_URLS_COUNTY_TAX:
        html = fetch(url)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        if not contains_any(text, TAX_KEYWORDS):
            continue

        distress_type = "Tax"
        sale_date = find_date_iso(text)
        county = guess_county(text)
        contact = extract_contact(text)
        address = extract_address(text)
        trustee = extract_trustee_or_attorney(text)

        flags = detect_risk_flags(text)
        dts = days_to_sale(sale_date)
        override_status, reason = triage(dts, flags)

        if override_status == "KILL":
            status = "KILL"
            score = 0
        elif override_status == "MONITOR":
            score = score_v2(distress_type, county, dts, bool(contact))
            status = "MONITOR"
        else:
            score = score_v2(distress_type, county, dts, bool(contact))
            status = label(distress_type, county, dts, flags, score, bool(contact))

        title = f"{distress_type} ({status}) ({county or 'TN'})"
        lead_key = make_lead_key(distress_type, county, sale_date, address, trustee, url)

        _store.upsert_lead(lead_key, {"address": address or "", "state": "TN"}, county or "")
        _store.insert_ingest_event(lead_key, "TaxPages", url, sale_date, None)

        props = build_properties(
            title=title,
            source="County Page",
            distress_type=distress_type,
            county=county,
            address=address,
            sale_date_iso=sale_date,
            trustee_attorney=trustee,
            contact_info=contact if contact else reason,
            raw_snippet=text[:2000],
            url=url,
            score=score,
            status=status,
            lead_key=lead_key,
        )

        existing_id = find_existing_by_lead_key(lead_key)
        if existing_id:
            update_lead(existing_id, props)
        else:
            create_lead(props)
