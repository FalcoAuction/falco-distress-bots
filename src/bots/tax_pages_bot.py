from bs4 import BeautifulSoup
from ..config import SEED_URLS_COUNTY_TAX, TAX_KEYWORDS
from ..utils import fetch, contains_any, find_date_iso, guess_county, extract_contact
from ..notion_client import create_lead, build_properties
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

        # NOTE: Many county pages are “pipeline pages” not property lists.
        # We'll score them low and label MONITOR unless a sale date is present.
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
            if sale_date == "":
                status = "MONITOR"
                score = min(score, 45)
            title = f"Tax ({status}) ({county or 'TN'})"

        props = build_properties(
            title=title,
            source="County Page",
            distress_type=distress_type,
            county=county,
            sale_date_iso=sale_date,
            contact_info=contact if contact else (kill_reason if killed else ""),
            url=url,
            score=score,
            status=status,
        )

        create_lead(props)

    print("[TaxPagesBot] Done.")
