from bs4 import BeautifulSoup

from ..config import SEED_URLS_PUBLIC_NOTICES, TRUSTEE_KEYWORDS, ESTATE_KEYWORDS
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
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def run():
    if not SEED_URLS_PUBLIC_NOTICES:
        print("[PublicNoticesBot] No SEED_URLS_PUBLIC_NOTICES set yet.")
        return

    for url in SEED_URLS_PUBLIC_NOTICES:
        try:
            html = fetch(url)
        except Exception as e:
            print(f"[PublicNoticesBot] fetch failed {url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        full_text = " ".join(soup.get_text(" ", strip=True).split())

        candidates = []

        # Whole-page candidate (helps for portals / index pages)
        if contains_any(full_text, TRUSTEE_KEYWORDS) or contains_any(full_text, ESTATE_KEYWORDS):
            candidates.append(full_text[:4000])

        # Block scan
        for node in soup.find_all(["p", "li", "div", "article", "section"]):
            txt = " ".join(node.get_text(" ", strip=True).split())
            if len(txt) < 80:
                continue
            if contains_any(txt, TRUSTEE_KEYWORDS) or contains_any(txt, ESTATE_KEYWORDS):
                candidates.append(txt[:2000])

        # De-dupe candidates by first 240 chars
        deduped = []
        seen = set()
        for c in candidates:
            k = c[:240]
            if k in seen:
                continue
            seen.add(k)
            deduped.append(c)

        print(f"[PublicNoticesBot] {url} -> {len(deduped)} candidates")

        for snippet in deduped[:60]:
            is_trustee = contains_any(snippet, TRUSTEE_KEYWORDS)
            is_estate = contains_any(snippet, ESTATE_KEYWORDS)

            distress_type = "Trustee Sale" if is_trustee else ("Estate" if is_estate else "Other")

            sale_date = find_date_iso(snippet)
            county = guess_county(snippet)

            contact = extract_contact(snippet)
            has_contact = bool(contact)

            address = extract_address(snippet)
            trustee_attorney = extract_trustee_or_attorney(snippet)

            flags = detect_risk_flags(snippet)
            dts = days_to_sale(sale_date)

            override_status, reason = triage(dts, flags)

            if override_status == "KILL":
                status = "KILL"
                score = 0
                title = f"{distress_type} (KILL) ({county or 'TN'})"
            elif override_status == "MONITOR":
                score = score_v2(distress_type, county, dts, has_contact)
                status = "MONITOR"
                title = f"{distress_type} (MONITOR) ({county or 'TN'})"
            else:
                score = score_v2(distress_type, county, dts, has_contact)
                status = label(distress_type, county, dts, flags, score, has_contact)
                title = f"{distress_type} ({status}) ({county or 'TN'})"

            raw_snippet = snippet[:2000]

            props = build_properties(
                title=title,
                source="Public Notice",
                distress_type=distress_type,
                county=county,
                address=address,
                sale_date_iso=sale_date,
                trustee_attorney=trustee_attorney,
                contact_info=contact if contact else reason,
                raw_snippet=raw_snippet,
                url=url,
                score=score,
                status=status,
            )

            # Basic dedupe by URL (prevents spam but can overwrite if a page has many notices)
            existing_id = find_existing_by_url(url)
            if existing_id:
                update_lead(existing_id, props)
            else:
                create_lead(props)

    print("[PublicNoticesBot] Done.")
