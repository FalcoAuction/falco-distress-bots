import os
import re
import hashlib
from datetime import datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from ..utils import fetch
from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale


# -----------------------------
# ENV CONFIG
# -----------------------------
_ALLOWED_COUNTIES_BASE = {
    c.strip() for c in os.getenv(
        "FALCO_ALLOWED_COUNTIES",
        "Davidson,Williamson,Rutherford,Wilson,Sumner",
    ).split(",") if c.strip()
}
_DTS_MIN = int(os.getenv("FALCO_DTS_MIN", "30"))
_DTS_MAX = int(os.getenv("FALCO_DTS_MAX", "75"))

MAX_LIST_PAGES_PER_SOURCE = int(os.getenv("FALCO_PUBLIC_NOTICES_MAX_LIST_PAGES", "15"))
MAX_NOTICE_LINKS_PER_SOURCE = int(os.getenv("FALCO_PUBLIC_NOTICES_MAX_NOTICE_LINKS_PER_SOURCE", "250"))
MAX_NOTICE_TEXT_CHARS = int(os.getenv("FALCO_PUBLIC_NOTICES_MAX_NOTICE_TEXT_CHARS", "9000"))

DEBUG = os.getenv("FALCO_PUBLIC_NOTICES_DEBUG", "0").lower() in {"1", "true", "yes", "y"}


# -----------------------------
# SEEDS
# -----------------------------
SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]


# -----------------------------
# HELPERS
# -----------------------------
def _clean(s: str | None) -> str:
    return " ".join((s or "").replace("\xa0", " ").split()).strip()


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = _clean(name)
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


def _sha1(*parts: str) -> str:
    raw = "|".join([_clean(p).lower() for p in parts if p is not None])
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _safe_fetch(url: str) -> str | None:
    try:
        return fetch(url)
    except Exception:
        return None


def _looks_like_notice_url(href: str) -> bool:
    if not href:
        return False
    u = href.lower()
    # tnlegalpub notices look like /legal_notice/...
    if "/legal_notice/" in u:
        return True
    return False


# -----------------------------
# DATE PARSING
# -----------------------------
_MONTHS = r"(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)"

def _parse_date_from_text(text: str) -> str | None:
    """
    Try multiple patterns. Return ISO date yyyy-mm-dd if found.
    """
    t = _clean(text)
    if not t:
        return None

    # 1) February 25, 2026
    m = re.search(rf"\b{_MONTHS}\s+\d{{1,2}},\s+\d{{4}}\b", t, flags=re.IGNORECASE)
    if m:
        s = m.group(0)
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(s.replace("Sept", "Sep"), fmt).date().isoformat()
            except Exception:
                continue

    # 2) 02/25/2026 or 2/5/26
    m = re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t)
    if m:
        s = m.group(0)
        for fmt in ("%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(s, fmt).date().isoformat()
            except Exception:
                continue

    # 3) Wednesday, February 25, 2026 (drop weekday)
    m = re.search(rf"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+({_MONTHS}\s+\d{{1,2}},\s+\d{{4}})\b", t, flags=re.IGNORECASE)
    if m:
        return _parse_date_from_text(m.group(1))

    return None


# -----------------------------
# COUNTY PARSING (TNLEGALPUB)
# -----------------------------
def _guess_county_from_notice_text(text: str) -> str | None:
    """
    tnlegalpub notices often contain:
      - "Register’s Office for Davidson County, Tennessee"
      - "Shelby County Courthouse"
      - "in the Register of Deeds Office of Bedford County, Tennessee"
    We'll pull the first strong match.
    """
    t = _clean(text)
    if not t:
        return None

    patterns = [
        r"Register[’']s Office for\s+([A-Za-z ]+?)\s+County,\s*Tennessee",
        r"Register of Deeds Office of\s+([A-Za-z ]+?)\s+County,\s*Tennessee",
        r"\b([A-Za-z ]+?)\s+County\s+Courthouse\b",
        r"\b([A-Za-z ]+?)\s+County,\s*Tennessee\b",
    ]
    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            c = _clean(m.group(1))
            # normalize weird casing
            if c:
                return c

    return None


def _extract_title_and_excerpt_from_tnlegalpub_list_item(article: BeautifulSoup):
    a = article.select_one("h2 a")
    if not a or not a.get("href"):
        return None, None, None
    url = a.get("href")
    title = _clean(a.get_text(" ", strip=True))
    p = article.find("p")
    excerpt = _clean(p.get_text(" ", strip=True)) if p else ""
    return url, title, excerpt


def _get_tnlegalpub_list_page_urls(seed: str, max_pages: int):
    # seed ends with /notice_type/foreclosure/
    urls = [seed]
    # pages: /page/2/, /page/3/ ...
    for i in range(2, max_pages + 1):
        urls.append(urljoin(seed, f"page/{i}/"))
    return urls


def _collect_notice_links_tnlegalpub(seed: str):
    """
    Returns list of (notice_url, list_excerpt_text)
    """
    links = []
    seen = set()

    list_pages = _get_tnlegalpub_list_page_urls(seed, MAX_LIST_PAGES_PER_SOURCE)

    pages_fetched = 0
    for page_url in list_pages:
        html = _safe_fetch(page_url)
        if not html:
            continue
        pages_fetched += 1
        soup = BeautifulSoup(html, "html.parser")

        articles = soup.select("div.page-content article.post")
        if not articles:
            # fallback: try generic article selector
            articles = soup.select("article.post")

        for art in articles:
            url, title, excerpt = _extract_title_and_excerpt_from_tnlegalpub_list_item(art)
            if not url:
                continue
            if url in seen:
                continue
            seen.add(url)
            links.append((url, excerpt))

        if len(links) >= MAX_NOTICE_LINKS_PER_SOURCE:
            break

    return pages_fetched, links


def _parse_tnlegalpub_notice(url: str):
    html = _safe_fetch(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    # grab main content text
    # WordPress: often inside <main> or article or .entry-content
    main = soup.select_one("main")
    text = _clean(main.get_text(" ", strip=True)) if main else _clean(soup.get_text(" ", strip=True))
    text = text[:MAX_NOTICE_TEXT_CHARS]

    # sale date
    sale_date_iso = _parse_date_from_text(text)

    # county
    county_guess = _guess_county_from_notice_text(text)

    return {
        "url": url,
        "text": text,
        "sale_date_iso": sale_date_iso,
        "county": county_guess,
    }


def _triage_status_score(dts: int):
    if dts <= 6:
        return "URGENT", 95
    if dts <= 13:
        return "HOT", 80
    return "GREEN", 65


# -----------------------------
# RUN
# -----------------------------
def run():
    print(f"[PublicNoticesBot] SEEDS={SEEDS} allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]")

    list_pages_fetched = 0
    notice_links_found = 0
    notice_pages_fetched_ok = 0
    parsed_ok = 0
    filtered_in = 0
    created = 0
    updated = 0

    skipped_short = 0
    skipped_no_sale = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_kill = 0
    skipped_dup_in_run = 0
    skipped_county_missing = 0

    sample_kept: list[str] = []
    sample_county_missing: list[str] = []

    seen_lead_keys = set()

    for seed in SEEDS:
        domain = urlparse(seed).netloc.lower()

        # For now, we implement tnlegalpub strongly because it's producing real foreclosure notices.
        if "tnlegalpub.com" in domain:
            pages_fetched, links = _collect_notice_links_tnlegalpub(seed)
            list_pages_fetched += pages_fetched
            notice_links_found += len(links)

            # fetch each notice and parse
            for (notice_url, _excerpt) in links[:MAX_NOTICE_LINKS_PER_SOURCE]:
                parsed = _parse_tnlegalpub_notice(notice_url)
                if not parsed:
                    continue
                notice_pages_fetched_ok += 1

                text = parsed.get("text") or ""
                if len(text) < 200:
                    skipped_short += 1
                    continue

                sale_date_iso = parsed.get("sale_date_iso")
                if not sale_date_iso:
                    skipped_no_sale += 1
                    continue

                dts = days_to_sale(sale_date_iso)
                if dts is None or dts < 0:
                    skipped_expired += 1
                    continue

                county = parsed.get("county")
                if not county:
                    skipped_county_missing += 1
                    if len(sample_county_missing) < 10:
                        sample_county_missing.append(f"url={notice_url} guess=")
                    continue

                if not _is_allowed_county(county):
                    skipped_out_of_geo += 1
                    continue

                if not (_DTS_MIN <= dts <= _DTS_MAX):
                    skipped_outside_window += 1
                    continue

                status, score = _triage_status_score(dts)
                if status is None:
                    skipped_kill += 1
                    continue

                lead_key = _sha1("PUBLICNOTICE", "TNLEGALPUB", notice_url, county, sale_date_iso)
                if lead_key in seen_lead_keys:
                    skipped_dup_in_run += 1
                    continue
                seen_lead_keys.add(lead_key)

                parsed_ok += 1

                if len(sample_kept) < 5:
                    sample_kept.append(f"county={_county_base(county)} sale={sale_date_iso} dts={dts} url={notice_url}")

                payload = {
                    "title": f"Foreclosure ({status}) ({county})",
                    "source": "TNLegalPub",
                    "distress_type": "Foreclosure",
                    "county": f"{_county_base(county)} County" if _county_base(county) else county,
                    "address": "",  # tnlegalpub notice parsing for address can be added later
                    "sale_date_iso": sale_date_iso,
                    "trustee_attorney": "",
                    "contact_info": "",
                    "raw_snippet": text[:2000],
                    "url": notice_url,
                    "status": status,
                    "score": score,
                    "lead_key": lead_key,
                    "days_to_sale": dts,
                }

                props = build_properties(payload)

                existing_id = find_existing_by_lead_key(lead_key)
                if existing_id:
                    update_lead(existing_id, props)
                    updated += 1
                else:
                    create_lead(props)
                    created += 1

                filtered_in += 1

        else:
            # Leaving the other seeds as "no-op" for now to avoid low-signal parsing issues.
            # We’ll wire them up after tnlegalpub is producing leads reliably.
            continue

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} notice_links_found={notice_links_found} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} parsed_ok={parsed_ok} "
        f"filtered_in={filtered_in} created={created} updated={updated} "
        f"skipped_short={skipped_short} skipped_no_sale={skipped_no_sale} skipped_expired={skipped_expired} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_outside_window={skipped_outside_window} "
        f"skipped_kill={skipped_kill} skipped_dup_in_run={skipped_dup_in_run} "
        f"skipped_county_missing={skipped_county_missing} "
        f"sample_kept={sample_kept} sample_county_missing={sample_county_missing}"
    )
    print("[PublicNoticesBot] Done.")
