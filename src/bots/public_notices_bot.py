import os
import re
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..config import (
    SEED_URLS_PUBLIC_NOTICES,
    TRUSTEE_KEYWORDS,
    ESTATE_KEYWORDS,
    PUBLIC_NOTICES_MAX_LIST_PAGES,
    PUBLIC_NOTICES_DEBUG,
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

_ALLOWED_COUNTIES_BASE = {
    c.strip() for c in os.getenv(
        "FALCO_ALLOWED_COUNTIES",
        "Davidson,Williamson,Rutherford,Wilson,Sumner",
    ).split(",") if c.strip()
}
_DTS_MIN = int(os.getenv("FALCO_DTS_MIN", "30"))
_DTS_MAX = int(os.getenv("FALCO_DTS_MAX", "75"))

_ALLOWED_RE = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(_ALLOWED_COUNTIES_BASE, key=len, reverse=True)) + r")\b",
    flags=re.IGNORECASE,
)


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(name.strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


def _infer_allowed_county_from_text(text: str) -> str | None:
    if not text:
        return None
    t = " " + " ".join(text.lower().split()) + " "

    for c in _ALLOWED_COUNTIES_BASE:
        c_l = c.lower()
        if f" {c_l} county " in t:
            return f"{c} County"

    for c in _ALLOWED_COUNTIES_BASE:
        c_l = c.lower()
        if f" county of {c_l} " in t:
            return f"{c} County"

    for c in _ALLOWED_COUNTIES_BASE:
        c_l = c.lower()
        if f" {c_l} " in t:
            return f"{c} County"

    return None


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
# LIST PAGE PARSERS
# ----------------------------

def _extract_items_tnlegalpub(listing_html: str, listing_url: str) -> list[dict]:
    """
    Returns items: {url, title, excerpt}
    """
    soup = BeautifulSoup(listing_html, "html.parser")
    items: list[dict] = []
    for art in soup.select("article.post"):
        a = art.select_one("h2.entry-title a[href]")
        if not a:
            continue
        href = a.get("href")
        title = _clean(a.get_text(" ", strip=True))
        p = art.select_one("p")
        excerpt = _clean(p.get_text(" ", strip=True)) if p else ""
        if href:
            items.append(
                {
                    "url": urljoin(listing_url, href),
                    "title": title,
                    "excerpt": excerpt,
                }
            )

    # de-dupe by url
    out, seen = [], set()
    for it in items:
        u = it["url"]
        if u in seen:
            continue
        seen.add(u)
        out.append(it)
    return out


def _listing_pages_tnlegalpub(seed: str) -> list[str]:
    pages = [seed]
    for i in range(2, PUBLIC_NOTICES_MAX_LIST_PAGES + 1):
        pages.append(seed.rstrip("/") + f"/page/{i}/")
    return pages


def _extract_links_generic(seed_html: str, seed_url: str) -> list[str]:
    soup = BeautifulSoup(seed_html, "html.parser")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        href_l = href.lower()
        if any(k in href_l for k in ["notice", "foreclosure", "publicnotice", "view", "listing"]):
            full = urljoin(seed_url, href)
            links.append(full)

    seed_dom = _domain(seed_url)
    out = []
    seen = set()
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)

    out.sort(key=lambda u: 0 if _domain(u) == seed_dom else 1)
    return out


def _seed_pages_foreclosurestn(seed: str) -> list[str]:
    pages = [seed]
    for i in range(2, min(PUBLIC_NOTICES_MAX_LIST_PAGES, 10) + 1):
        pages.append(seed.rstrip("/") + f"/?page={i}")
        pages.append(seed.rstrip("/") + f"/?paged={i}")
        pages.append(seed.rstrip("/") + f"/page/{i}/")
    out, seen = [], set()
    for p in pages:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _seed_pages_tnpublicnotice(seed: str) -> list[str]:
    base = "https://www.tnpublicnotice.com/Search.aspx"
    pages = [
        base,
        base + "?q=foreclosure",
        base + "?q=trustee",
        base + "?q=substitute%20trustee%20sale",
    ]
    out, seen = [], set()
    for p in pages:
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
    return out


def _listing_item_mentions_allowed(it: dict) -> bool:
    """
    tnlegalpub listing item pre-filter:
    keep only if title OR excerpt mentions one of the allowed counties.
    """
    title = (it.get("title") or "")
    excerpt = (it.get("excerpt") or "")
    blob = f"{title} {excerpt}"
    return bool(_ALLOWED_RE.search(blob))


# ----------------------------
# NOTICE PARSING
# ----------------------------

def _parse_notice_page(notice_url: str, html: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    text = _clean(soup.get_text(" ", strip=True))
    if len(text) < 200:
        return None

    if not (contains_any(text, TRUSTEE_KEYWORDS) or contains_any(text, ESTATE_KEYWORDS)):
        return None

    sale_date = find_date_iso(text)

    county_guess = guess_county(text)
    county = county_guess
    if not _is_allowed_county(county):
        county = _infer_allowed_county_from_text(text)

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
        "county_guess": county_guess,
        "address": address,
        "trustee": trustee,
        "contact": contact,
        "distress_type": distress_type,
    }


def run():
    if not SEED_URLS_PUBLIC_NOTICES:
        print("[PublicNoticesBot] No SEED_URLS_PUBLIC_NOTICES set.")
        return

    print(
        f"[PublicNoticesBot] SEEDS={SEED_URLS_PUBLIC_NOTICES} "
        f"allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]"
    )

    created = 0
    updated = 0

    list_pages_fetched = 0
    notice_links_found = 0
    notice_pages_fetched_ok = 0
    parsed_ok = 0
    filtered_in = 0

    skipped_short = 0
    skipped_no_sale = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_kill = 0

    # new: efficiency counters for tnlegalpub listing pre-filter
    tnlegalpub_items_seen = 0
    tnlegalpub_items_kept = 0

    for seed in SEED_URLS_PUBLIC_NOTICES:
        dom = _domain(seed)

        notice_links: list[str] = []
        seen_links = set()

        # tnlegalpub: parse listing items and pre-filter before fetching notices
        if "tnlegalpub.com" in dom:
            list_pages = _listing_pages_tnlegalpub(seed)

            for lp in list_pages:
                try:
                    html = fetch(lp)
                    list_pages_fetched += 1
                except Exception as e:
                    _log(f"listing fetch failed {lp}: {e}")
                    continue

                items = _extract_items_tnlegalpub(html, lp)
                tnlegalpub_items_seen += len(items)
                kept = [it for it in items if _listing_item_mentions_allowed(it)]
                tnlegalpub_items_kept += len(kept)

                for it in kept:
                    u = it["url"]
                    if u in seen_links:
                        continue
                    seen_links.add(u)
                    notice_links.append(u)

                if len(notice_links) >= 200:
                    break

        els
