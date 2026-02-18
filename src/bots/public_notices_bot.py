# src/bots/public_notices_bot.py

from urllib.parse import urljoin
from bs4 import BeautifulSoup

from ..config import SEED_URLS_PUBLIC_NOTICES, PUBLIC_NOTICE_MAX_LIST_PAGES
from ..utils import (
    fetch, find_date_iso, guess_county,
    extract_contact, extract_address, extract_trustee_or_attorney,
    make_lead_key
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def _clean(txt: str) -> str:
    return " ".join((txt or "").split())


def _extract_notice_links(list_html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    links: list[str] = []
    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/legal_notice/" in href:
            links.append(urljoin(base_url, href))

    seen = set()
    out: list[str] = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _find_next_page(list_html: str, base_url: str) -> str | None:
    soup = BeautifulSoup(list_html, "html.parser")

    rel_next = soup.select_one('a[rel="next"][href]')
    if rel_next and rel_next.get("href"):
        return urljoin(base_url, rel_next["href"])

    for a in soup.select("a[href]"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if "next" in txt:
            return urljoin(base_url, a["href"])

    return None


def _extract_notice_text(notice_html: str) -> str:
    soup = BeautifulSoup(notice_html, "html.parser")
    return _clean(soup.get_text(" ", strip=True))


def run():
    print(f"[PublicNoticesBot] SEEDS={SEED_URLS_PUBLIC_NOTICES}")

    if not SEED_URLS_PUBLIC_NOTICES:
        print("[PublicNoticesBot] No SEED_URLS_PUBLIC_NOTICES set yet.")
        return

    list_pages_fetched = 0
    notice_links_found = 0
    notice_pages_fetched_ok = 0

    created = 0
    updated = 0

    skipped_short = 0
    skipped_no_sale = 0
    skipped_expired = 0
    skipped_lt30 = 0
    skipped_kill = 0

    wrote_count = 0
    debug_prints_left = 5

    seen_notice_urls = set()

    for seed_url in SEED_URLS_PUBLIC_NOTICES:
        next_url = seed_url
        pages_remaining = PUBLIC_NOTICE_MAX_LIST_PAGES

        while next_url and pages_remaining > 0:
            pages_remaining -= 1

            try:
                list_html = fetch(next_url)
                list_pages_fetched += 1
            except Exception as e:
                print(f"[PublicNoticesBot] listing fetch failed {next_url}: {e}")
                break

            notice_urls = _extract_notice_links(list_html, base_url=next_url)
            notice_links_found += len(notice_urls)
            print(f"[PublicNoticesBot] listing {next_url} -> notices={len(notice_urls)}")

            for notice_url in notice_urls:
                if notice_url in seen_notice_urls:
                    continue
                seen_notice_urls.add(notice_url)

                try:
                    notice_html = fetch(notice_url)
                    notice_pages_fetched_ok += 1
                except Exception as e:
                    print(f"[PublicNoticesBot] notice fetch failed {notice_url}: {e}")
                    continue

                text = _extract_notice_text(notice_html)
                if len(text) < 400:
                    skipped_short += 1
                    continue

                distress_type = "Trustee Sale" if "trustee" in text.lower() else "Foreclosure"

                sale_date = find_date_iso(text)
                if not sale_date:
                    skipped_no_sale += 1
                    continue

                county = guess_county(text)
                contact = extract_contact(text)
                has_contact = bool(contact)
                address = extract_address(text)
                trustee = extract_trustee_or_attorney(text)

                flags = detect_risk_flags(text)
                dts = days_to_sale(sale_date)

                # Skip expired outright
                if dts is not None and dts < 0:
                    skipped_expired += 1
                    continue

                override_status, reason = triage(dts, flags)
                if override_status == "KILL":
                    skipped_kill += 1
                    continue

                score = score_v2(distress_type, county, dts, has_contact)

                # If within 30 days, write MONITOR (do not skip)
                if dts is not None and dts < 30:
                    skipped_lt30 += 1
                    status = "MONITOR"
                else:
                    status = "MONITOR" if override_status == "MONITOR" else label(
                        distress_type, county, dts, flags, score, has_contact
                    )

                title = f"{distress_type} ({status}) ({county or 'TN'})"

                lead_key = make_lead_key(
                    "TNLEGALPUB",
                    notice_url,
                    distress_type,
                    county or "TN",
                    sale_date,
                )

                props = build_properties(
                    title=title,
                    source="TN Legal Pub",
                    distress_type=distress_type,
                    county=county,
                    address=address,
                    sale_date_iso=sale_date,
                    trustee_attorney=trustee,
                    contact_info=contact if contact else (reason or ""),
                    raw_snippet=text[:2000],
                    url=notice_url,
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

                wrote_count += 1

                if debug_prints_left > 0:
                    print(f"[PublicNoticesBot][DEBUG] wrote status={status} sale_date={sale_date} dts={dts} county={county} url={notice_url}")
                    debug_prints_left -= 1

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} notice_links_found={notice_links_found} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} wrote_count={wrote_count} "
        f"created={created} updated={updated} "
        f"skipped_short={skipped_short} skipped_no_sale={skipped_no_sale} "
        f"skipped_expired={skipped_expired} skipped_lt30={skipped_lt30} skipped_kill={skipped_kill}"
    )
    print("[PublicNoticesBot] Done.")
