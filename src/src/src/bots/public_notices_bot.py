from bs4 import BeautifulSoup
from ..config import SEED_URLS_PUBLIC_NOTICES, TRUSTEE_KEYWORDS, ESTATE_KEYWORDS
from ..utils import fetch, soup_text, contains_any, find_date_iso, guess_county, extract_contact
from ..notion_client import create_lead, build_properties

def run():
    if not SEED_URLS_PUBLIC_NOTICES:
        print("[PublicNoticesBot] No SEED_URLS_PUBLIC_NOTICES yet.")
        return

    for url in SEED_URLS_PUBLIC_NOTICES:
        try:
            html = fetch(url)
        except Exception as e:
            print(f"[PublicNoticesBot] fetch failed {url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        candidates = []

        for node in soup.find_all(["p", "li", "div"]):
            txt = " ".join(node.get_text(" ", strip=True).split())
            if len(txt) < 140:
                continue
            if contains_any(txt, TRUSTEE_KEYWORDS) or contains_any(txt, ESTATE_KEYWORDS):
                candidates.append(txt)

        print(f"[PublicNoticesBot] {url} -> {len(candidates)} candidates")

        for snippet in candidates[:40]:
            is_trustee = contains_any(snippet, TRUSTEE_KEYWORDS)
            is_estate = contains_any(snippet, ESTATE_KEYWORDS)

            distress_type = "Trustee Sale" if is_trustee else ("Estate" if is_estate else "Other")
            sale_date = find_date_iso(snippet)
            county = guess_county(snippet)
            contact = extract_contact(snippet)

            score = 88 if distress_type == "Trustee Sale" else 75

            title = f"{distress_type} Lead ({county or 'TN'})"

            props = build_properties(
                title=title,
                source="Public Notice",
                distress_type=distress_type,
                county=county,
                sale_date_iso=sale_date,
                contact_info=contact,
                url=url,
                score=score,
            )

            create_lead(props)

    print("[PublicNoticesBot] Done.")
