from bs4 import BeautifulSoup

from ..config import SEED_URLS_COUNTY_TAX, TAX_KEYWORDS
from ..utils import (
    fetch,
    contains_any,
    find_date_iso,
    guess_county,
    extract_contact,
    extract_address,
    extract_trustee_or_attorney,
)
from ..notion_client import build_properties, create_lead, find_existing_by_url, update_lead
from ..scoring import days_to_sale, detect_risk_flags, hard_kill, score_v2, label


def run():
    if not SEED_URLS_COUNTY_TAX:
        print("[TaxPagesBot] No SEED_URLS_COUNTY_TAX set yet.")
        return

    for url in SEED_URLS_COUNTY_TAX:
        try:
            html = fetch(url)
        except Exception as e:
            print(f"[TaxPagesBot] fetch failed {url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ", strip=True)

        if not contains_any(text, TAX_KEYWORDS):
            print(f"[TaxPagesBot] {url} -> no tax keywords found (still logging).")

        distress_type = "Tax"
        sale_date = find_date_iso(text)
        county = guess_county(text)
        contact = extract_contact(text)
        has_contact = bool(contact)

        flags = detect_risk_flags(text)
        dts = days_to_sale(sale_date)

        killed, kill_reason = hard_kill(dts, flags)
        if killed:
            status = "KILL"
            score = 0
            title = f"Tax (KILL) ({county or 'TN'})"
        else:
            score = score_v2(distress_type, county, dts, has_contact)
            status = label(distress_type, county, dts, flags, score, has_contact)

            # pipeline pages usually MONITOR
            if not sale_date:
                status = "MONITOR"
                score = min(score, 45)

            title = f"Tax ({status}) ({county or 'TN'})"

        # ✅ THESE MUST BE HERE (inside the loop, after title is set)
        address = extract_address(text)
        trustee_attorney = extract_trustee_or_attorney(text)
        raw_snippet = text[:2000]

        props = build_properties(
            title=title,
            source="County Page",
            distress_type=distress_type,
            county=county,
            address=address,
            sale_date_iso=sale_date,
            trustee_attorney=trustee_attorney,
            contact_info=contact if contact else (kill_reason if killed else ""),
            raw_snippet=raw_snippet,
            url=url,
            score=score,
            status=status,
        )

        # ✅ Dedupe (note: URL dedupe is basic; we'll improve later)
        existing_id = find_existing_by_url(url)
        if existing_id:
            update_lead(existing_id, props)
        else:
            create_lead(props)

    print("[TaxPagesBot] Done.")
