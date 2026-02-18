from bs4 import BeautifulSoup
from ..config import SEED_URLS_COUNTY_TAX, TAX_KEYWORDS
from ..utils import fetch, contains_any, find_date_iso, guess_county, extract_contact
from ..notion_client import create_lead, build_properties

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
            print(f"[TaxPagesBot] {url} -> no tax keywords found.")

        sale_date = find_date_iso(text)
        county = guess_county(text)
        contact = extract_contact(text)

        title = f"Tax Sale Pipeline ({county or 'TN'})"
        score = 65

        props = build_properties(
            title=title,
            source="County Page",
            distress_type="Tax",
            county=county,
            sale_date_iso=sale_date,
            contact_info=contact,
            url=url,
            score=score,
        )

        create_lead(props)

    print("[TaxPagesBot] Done.")
