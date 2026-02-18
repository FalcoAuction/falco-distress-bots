# src/bots/public_notices_bot.py

import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..config import (
    SEED_URLS_PUBLIC_NOTICES,
    TRUSTEE_KEYWORDS,
    ESTATE_KEYWORDS,
    PUBLIC_NOTICES_MAX_LIST_PAGES,
    PUBLIC_NOTICES_DEBUG,
    PUBLIC_NOTICES_MIN_DAYS_OUT,
)
from ..utils import (
    fetch,
    contains_any,
    find_date_iso,
    guess_county,
    extract_contact,
    extract_address,
    extract_trustee_or_attorney,
    make_lead_key,
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def _clean(txt: str) -> str:
    return " ".join((txt or "").split())


def _domain(url: str) -> str:
    try:
        return urlparse(url).netloc.lower()
    except Exception:
        return ""


def _log(msg: str):
    if PUBLIC_NOTICES_DEBUG:
        print(f"[PublicNoticesBot][DEBUG] {msg}")


# ----------------------------
# LINK EXTRACTORS PER SOURCE
# ----------------------------

def _extract_links_tnlegalpub(listing_html: str, listing_url: str) -> list[str]:
    """
    tnlegalpub foreclosure listings are WP-like paged:
    /notice_type/foreclosure/page/2/
    Articles are usually in <h2><a href="...">.
    """
    soup = BeautifulSoup(listing_html, "html.parser")
    links = []
    for a in soup.select("h2 a[href]"):
        href = a.get("href")
        if href:
            links.append(urljoin(listing_url, href))
    # de-dupe
    out = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _listing_pages_tnlegalpub(seed: str) -> list[str]:
    pages = [seed]
    for i in range(2, PUBLIC_NOTICES_MAX_LIST_PAGES + 1):
        pages.append(seed.rstrip("/") + f"/page/{i}/")
    return pages


def _extract_links_generic(seed_html: str, seed_url: str) -> list[str]:
    """
    Generic link extractor: collects links that *look* like notice pages.
    Used for foreclosurestn.com and tnpublicnotice.com (best-effort).
    """
    soup = BeautifulSoup(seed_html, "html.parser")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        href_l = href.lower()

        # Heuristics: keep only likely notice/detail pages
        if any(k in href_l for k in ["notice", "foreclosure", "publicnotice", "view", "listing"]):
            full = urljoin(seed_url, href)
            links.append(full)

    # de-dupe and keep same-domain preference
    seed_dom = _domain(seed_url)
    out = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    # prioritize same-domain links first
    out.sort(key=lambda u: 0 if _domain(u) == seed_dom else 1)
    return out


def _seed_pages_foreclosurestn(seed: str) -> list[str]:
    """
    Best-effort paging attempts. If these 404, we just process page 1.
    (We will refine once we see logs.)
    """
    pages = [seed]
    # try common paging params
    for i in range(2, min(PUBLIC_NOTICES_MAX_LIST_PAGES, 10) + 1):
        pages.append(seed.rstrip("/") + f"/?page={i}")
        pages.append(seed.rstrip("/") + f"/?paged={i}")
        pages.append(seed.rstrip("/") + f"/page/{i}/")
    # de-dupe
    deduped = []
    seen = set()
    for p in pages:
        if p in seen:
            continue
        seen.add(p)
        deduped.append(p)
    return deduped


def _seed_pages_tnpublicnotice(seed: str) -> list[str]:
    """
    tnpublicnotice uses Search.aspx. We start with Search.aspx and (optionally)
    try popular searches by adding querystring filters.

    We do NOT bypass CAPTCHAs or logins.
    """
    base = "https://www.tnpublicnotice.com/Search.aspx"
    pages = [
        base,
        # Best-effort: common filters via query string (safe if ignored)
        base + "?q=foreclosure",
        base + "?q=trustee",
        base + "?q=substitute%20trustee%20sale",
    ]
    # de-dupe
    out, seen = [], set()
    for p in pages:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


# ----------------------------
# NOTICE PARSING + WRITE
# ----------------------------

def _parse_notice_page(notice_url: str, html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    text = _clean(soup.get_text(" ", strip=True))
    if len(text) < 200:
        return None

    # must have at least some signal keywords
    if not (contains_any(text, TRUSTEE_KEYWORDS) or contains_any(text, ESTATE_KEYWORDS)):
        return None

    sale_date = find_date_iso(text)
    county = guess_county(text)
    address = extract_address(text)
    trustee = extract_trustee_or_attorney(text)
    contact = extract_contact(text)

    is_trustee = contains_any(text, TRUSTEE_KEYWORDS)
    is_estate = contains_any(text, ESTATE_KEYWORDS)
    distress_type = "Trustee Sale" if is_trustee else ("Estate" if is_estate else "Other")

    return {
        "notice_url": notice_url,
        "text": text,
        "snippet": text[:2000],
        "sale_date": sale_date,
        "county": county,
        "address": address,
        "trustee": trustee,
        "contact": contact,
        "distress_type": distress_type,
    }


def run():
    if not SEED_URLS_PUBLIC_NOTICES:
        print("[PublicNoticesBot] No SEED_URLS_PUBLIC_NOTICES set.")
        return

    created = updated = 0
    wrote_count = 0

    list_pages_fetched = 0
    notice_links_found = 0
    notice_pages_fetched_ok = 0
    parsed_ok = 0

    skipped_short = 0
    skipped_no_sale = 0
    skipped_expired = 0
    skipped_lt30 = 0
    skipped_kill = 0

    print(f"[PublicNoticesBot] SEEDS={SEED_URLS_PUBLIC_NOTICES}")

    for seed in SEED_URLS_PUBLIC_NOTICES:
        dom = _domain(seed)

        # build list pages per source
        if "tnlegalpub.com" in dom:
            list_pages = _listing_pages_tnlegalpub(seed)
        elif "foreclosurestn.com" in dom:
            list_pages = _seed_pages_foreclosurestn(seed)
        elif "tnpublicnotice.com" in dom:
            list_pages = _seed_pages_tnpublicnotice(seed)
        else:
            list_pages = [seed]

        # gather notice links from list pages
        notice_links: list[str] = []
        seen_links = set()

        for lp in list_pages:
            try:
                _log(f"fetching listing={lp}")
                html = fetch(lp)
                list_pages_fetched += 1
            except Exception as e:
                _log(f"listing fetch failed {lp}: {e}")
                continue

            if "tnlegalpub.com" in dom:
                links = _extract_links_tnlegalpub(html, lp)
            else:
                links = _extract_links_generic(html, lp)

            if links:
                _log(f"listing {lp} -> links={len(links)}")

            for u in links:
                if u in seen_links:
                    continue
                seen_links.add(u)
                notice_links.append(u)

            # keep the crawl bounded
            if len(notice_links) >= 200:
                break

        notice_links_found += len(notice_links)

        # fetch + parse each notice
        for notice_url in notice_links:
            try:
                html = fetch(notice_url)
                notice_pages_fetched_ok += 1
            except Exception as e:
                _log(f"notice fetch failed {notice_url}: {e}")
                continue

            parsed = _parse_notice_page(notice_url, html)
            if not parsed:
                skipped_short += 1
                continue
            parsed_ok += 1

            sale_date = parsed["sale_date"]
            if not sale_date:
                skipped_no_sale += 1
                continue

            dts = days_to_sale(sale_date)
            if dts is not None and dts < 0:
                skipped_expired += 1
                continue

            # optional min-days-out gate
            if dts is not None and dts < int(PUBLIC_NOTICES_MIN_DAYS_OUT):
                skipped_lt30 += 1
                continue

            flags = detect_risk_flags(parsed["text"])
            override_status, reason = triage(dts, flags)

            if override_status == "KILL":
                skipped_kill += 1
                continue

            has_contact = bool(parsed["contact"])
            score = score_v2(parsed["distress_type"], parsed["county"], dts, has_contact)
            status = label(parsed["distress_type"], parsed["county"], dts, flags, score, has_contact)

            title = f"{parsed['distress_type']} ({status}) ({parsed['county'] or 'TN'})"

            lead_key = make_lead_key(
                parsed["distress_type"],
                parsed["county"],
                sale_date,
                parsed["address"],
                parsed["trustee"],
                parsed["notice_url"],
            )

            props = build_properties(
                title=title,
                source="Public Notice",
                distress_type=parsed["distress_type"],
                county=parsed["county"],
                address=parsed["address"],
                sale_date_iso=sale_date,
                trustee_attorney=parsed["trustee"],
                contact_info=(parsed["contact"] or reason),
                raw_snippet=parsed["snippet"],
                url=parsed["notice_url"],     # IMPORTANT: notice-level URL
                score=score,
                status=status,
                lead_key=lead_key,
                days_to_sale_num=dts,
            )

            existing_id = find_existing_by_lead_key(lead_key)
            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

            wrote_count += 1

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} notice_links_found={notice_links_found} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} parsed_ok={parsed_ok} "
        f"wrote_count={wrote_count} created={created} updated={updated} "
        f"skipped_short={skipped_short} skipped_no_sale={skipped_no_sale} "
        f"skipped_expired={skipped_expired} skipped_lt30={skipped_lt30} skipped_kill={skipped_kill}"
    )
    print("[PublicNoticesBot] Done.")
