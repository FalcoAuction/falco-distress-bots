import re
from bs4 import BeautifulSoup

from ..config import SEED_URLS_PUBLIC_NOTICES, TRUSTEE_KEYWORDS, ESTATE_KEYWORDS
from ..utils import (
    fetch, contains_any, find_date_iso, guess_county,
    extract_contact, extract_address, extract_trustee_or_attorney,
    make_lead_key
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def _clean(txt: str) -> str:
    return " ".join((txt or "").split())

def _split_into_notice_chunks(text: str) -> list[str]:
    t = _clean(text)
    if len(t) < 200:
        return []

    markers = [
        r"\bSUBSTITUTE\s+TRUSTEE\S*\s+SALE\b",
        r"\bTRUSTEE\S*\s+SALE\b",
        r"\bNOTICE\s+OF\s+FORECLOSURE\b",
        r"\bFORECLOSURE\s+NOTICE\b",
        r"\bNOTICE\s+OF\s+SALE\b",
    ]

    positions = []
    for m in markers:
        for match in re.finditer(m, t, flags=re.IGNORECASE):
            positions.append(match.start())

    positions = sorted(set(positions))
    if not positions:
        # fallback: if page has keywords, treat as one chunk
        return [t[:6000]] if (contains_any(t, TRUSTEE_KEYWORDS) or contains_any(t, ESTATE_KEYWORDS)) else []

    chunks = []
    for i, start in enumerate(positions):
        end = positions[i + 1] if i + 1 < len(positions) else min(len(t), start + 8000)
        chunk = t[start:end].strip()
        if len(chunk) < 200:
            continue
        chunks.append(chunk[:8000])

    # de-dupe
    deduped = []
    seen = set()
    for c in chunks:
        k = c[:220].lower()
        if k in seen:
            continue
        seen.add(k)
        deduped.append(c)

    return deduped[:100]


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
        page_text = _clean(soup.get_text(" ", strip=True))

        if not (contains_any(page_text, TRUSTEE_KEYWORDS) or contains_any(page_text, ESTATE_KEYWORDS)):
            print(f"[PublicNoticesBot] {url} -> no trustee/estate keywords")
            continue

        chunks = _split_into_notice_chunks(page_text)
        print(f"[PublicNoticesBot] {url} -> {len(chunks)} notice chunks")

        for snippet in chunks:
            is_trustee = contains_any(snippet, TRUSTEE_KEYWORDS)
            is_estate = contains_any(snippet, ESTATE_KEYWORDS)
            distress_type = "Trustee Sale" if is_trustee else ("Estate" if is_estate else "Other")

            sale_date = find_date_iso(snippet)
            county = guess_county(snippet)
            contact = extract_contact(snippet)
            has_contact = bool(contact)
            address = extract_address(snippet)
            trustee = extract_trustee_or_attorney(snippet)

            flags = detect_risk_flags(snippet)
            dts = days_to_sale(sale_date)

            # ✅ NEW: Skip expired notices entirely (do not write them)
            if dts is not None and dts < 0:
                continue

            override_status, reason = triage(dts, flags)

            if override_status == "KILL":
                status = "KILL"
                score = 0
            elif override_status == "MONITOR":
                score = score_v2(distress_type, county, dts, has_contact)
                status = "MONITOR"
            else:
                score = score_v2(distress_type, county, dts, has_contact)
                status = label(distress_type, county, dts, flags, score, has_contact)

            title = f"{distress_type} ({status}) ({county or 'TN'})"

            # Notice-specific lead key
            lead_key = make_lead_key(
                distress_type, county, sale_date, address, trustee,
                url + "|" + snippet[:140]
            )

            props = build_properties(
                title=title,
                source="Public Notice",
                distress_type=distress_type,
                county=county,
                address=address,
                sale_date_iso=sale_date,
                trustee_attorney=trustee,
                contact_info=contact if contact else reason,
                raw_snippet=snippet[:2000],
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

    print("[PublicNoticesBot] Done.")
