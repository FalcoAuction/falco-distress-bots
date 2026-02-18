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

    # de-dupe chunks within page
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

    # run-level counters for GitHub Actions logs
    created = 0
    updated = 0
    skipped_no_sale = 0
    skipped_expired = 0
    skipped_lt30 = 0
    skipped_kill = 0
    skipped_short = 0
    seen_keys = set()

    for seed_url in SEED_URLS_PUBLIC_NOTICES:
        try:
            html = fetch(seed_url)
        except Exception as e:
            print(f"[PublicNoticesBot] fetch failed {seed_url}: {e}")
            continue

        soup = BeautifulSoup(html, "html.parser")
        page_text = _clean(soup.get_text(" ", strip=True))

        if not (contains_any(page_text, TRUSTEE_KEYWORDS) or contains_any(page_text, ESTATE_KEYWORDS)):
            print(f"[PublicNoticesBot] {seed_url} -> no trustee/estate keywords")
            continue

        chunks = _split_into_notice_chunks(page_text)
        print(f"[PublicNoticesBot] {seed_url} -> {len(chunks)} notice chunks")

        for snippet in chunks:
            if len(snippet) < 200:
                skipped_short += 1
                continue

            is_trustee = contains_any(snippet, TRUSTEE_KEYWORDS)
            is_estate = contains_any(snippet, ESTATE_KEYWORDS)
            distress_type = "Trustee Sale" if is_trustee else ("Estate" if is_estate else "Other")

            sale_date = find_date_iso(snippet)
            if not sale_date:
                skipped_no_sale += 1
                continue

            county = guess_county(snippet)
            contact = extract_contact(snippet)
            has_contact = bool(contact)
            address = extract_address(snippet)
            trustee = extract_trustee_or_attorney(snippet)

            flags = detect_risk_flags(snippet)
            dts = days_to_sale(sale_date)

            # Skip expired outright
            if dts is not None and dts < 0:
                skipped_expired += 1
                continue

            # Hard filter: require 30+ days-out (signal-only)
            if dts is not None and dts < 30:
                skipped_lt30 += 1
                continue

            override_status, reason = triage(dts, flags)

            # Hard filter: do not write KILLs at all
            if override_status == "KILL":
                skipped_kill += 1
                continue

            # Score + status label
            score = score_v2(distress_type, county, dts, has_contact)
            if override_status == "MONITOR":
                status = "MONITOR"
            else:
                status = label(distress_type, county, dts, flags, score, has_contact)

            title = f"{distress_type} ({status}) ({county or 'TN'})"

            # CRITICAL: stable Lead Key (40-char SHA1) to enable dedupe
            # NOTE: this is still seed-page mode; once we switch to notice-level URLs,
            # replace seed_url with notice_url and remove snippet stabilizer.
            lead_key = make_lead_key(
                "Public Notice",
                distress_type,
                county or "TN",
                sale_date,
                address or "",
                trustee or "",
                seed_url,
                snippet[:200],  # temporary stabilizer; will be replaced by notice_url
            )

            # run-level de-dupe (prevents double writes within one run)
            if lead_key in seen_keys:
                continue
            seen_keys.add(lead_key)

            props = build_properties(
                title=title,
                source="Public Notice",
                distress_type=distress_type,
                county=county,
                address=address,
                sale_date_iso=sale_date,
                trustee_attorney=trustee,
                contact_info=contact if contact else (reason or ""),
                raw_snippet=snippet[:2000],
                url=seed_url,  # will become notice_url in Step B
                score=score,
                status=status,
                lead_key=lead_key,
            )

            existing_id = find_existing_by_lead_key(lead_key)
            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

    print(
        "[PublicNoticesBot] summary "
        f"created={created} updated={updated} "
        f"skipped_no_sale={skipped_no_sale} skipped_expired={skipped_expired} "
        f"skipped_lt30={skipped_lt30} skipped_kill={skipped_kill} skipped_short={skipped_short}"
    )
    print("[PublicNoticesBot] Done.")
