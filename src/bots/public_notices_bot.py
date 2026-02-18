from bs4 import BeautifulSoup

from ..config import SEED_URLS_PUBLIC_NOTICES, TRUSTEE_KEYWORDS, ESTATE_KEYWORDS
from ..utils import (
    fetch, contains_any, find_date_iso, guess_county,
    extract_contact, extract_address, extract_trustee_or_attorney,
    make_lead_key
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def run():
    if not SEED_URLS_PUBLIC_NOTICES:
        return

    for url in SEED_URLS_PUBLIC_NOTICES:
        html = fetch(url)
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        if not contains_any(text, TRUSTEE_KEYWORDS + ESTATE_KEYWORDS):
            continue

        distress_type = "Trustee Sale"
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

        props = build_properties(
            title=title,
            source="Public Notice",
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
