# src/bots/public_notices_bot.py

import os
import re
from datetime import datetime
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from ..config import (
    SEED_URLS_PUBLIC_NOTICES,
    PUBLIC_NOTICES_MAX_LIST_PAGES,
    MAX_NOTICE_LINKS_PER_SOURCE,
    MAX_NOTICE_TEXT_CHARS,
    TARGET_COUNTIES,
)
from ..utils import fetch, make_lead_key
from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale


# -----------------------------
# ENV OVERRIDES (same style as your other bots)
# -----------------------------
_ALLOWED_COUNTIES_BASE = {
    c.strip()
    for c in os.getenv(
        "FALCO_ALLOWED_COUNTIES",
        "Davidson,Williamson,Rutherford,Wilson,Sumner",
    ).split(",")
    if c.strip()
}
_DTS_MIN = int(os.getenv("FALCO_DTS_MIN", "21"))
_DTS_MAX = int(os.getenv("FALCO_DTS_MAX", "90"))

DEBUG = bool(int(os.getenv("FALCO_PUBLIC_DEBUG", "0")))


# -----------------------------
# HELPERS
# -----------------------------
def _log(msg: str):
    if DEBUG:
        print(msg)


def _norm_whitespace(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\r", " ").replace("\n", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(name.strip().split())
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def _normalize_county(name: str | None) -> str | None:
    if not name:
        return None
    n = _norm_whitespace(name)
    if not n:
        return None
    if n.lower().endswith("county"):
        return n if n.lower().endswith(" county") else f"{n[:-6].strip()} County"
    return f"{n} County"


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


def _status_from_dts(dts: int | None):
    if dts is None:
        return None
    if dts < 0:
        return "EXPIRED"
    if dts <= 6:
        return "URGENT"
    if 7 <= dts <= 13:
        return "HOT"
    return "GREEN"


def _score_from_status(dts: int | None, status: str | None) -> int:
    if dts is None or not status:
        return 0
    if status == "URGENT":
        return max(90, min(100, 100 - (dts * 2)))
    if status == "HOT":
        return max(75, min(89, 89 - ((dts - 7) * 2)))
    if status == "GREEN":
        return max(55, min(74, 74 - int((max(14, dts) - 14) / 2)))
    return 0


def _parse_date_flex_any(s: str) -> str | None:
    if not s:
        return None
    s = _norm_whitespace(s)
    if not s or s.lower() in {"tbd", "unknown", "n/a", "-"}:
        return None

    fmts = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d,%Y",
        "%b %d,%Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _extract_all_date_candidates(text: str) -> list[str]:
    """
    TNLegalPub notices can contain multiple dates (publication, postponement, original, new, etc.).
    We want the best FUTURE sale date, so we collect candidates from common patterns.
    """
    t = _norm_whitespace(text)

    candidates: list[str] = []

    # Common written dates: "February 25, 2026" / "Feb 25, 2026"
    for m in re.finditer(r"\b([A-Z][a-z]{2,9}\s+\d{1,2},\s+\d{4})\b", t):
        candidates.append(m.group(1))

    # Numeric: 02/25/2026
    for m in re.finditer(r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b", t):
        candidates.append(m.group(1))

    # Phrases like "sold at foreclosure sale on Wednesday, February 25, 2026"
    for m in re.finditer(
        r"\b(?:sale on|sold on|foreclosure sale on|will be on)\s+(?:[A-Za-z]+,\s+)?([A-Z][a-z]{2,9}\s+\d{1,2},\s+\d{4})\b",
        t,
        flags=re.IGNORECASE,
    ):
        candidates.append(m.group(1))

    # Dedup preserving order
    seen = set()
    out = []
    for c in candidates:
        cc = c.strip()
        if cc not in seen:
            seen.add(cc)
            out.append(cc)
    return out


def _pick_best_sale_date_iso(text: str) -> str | None:
    """
    Choose the earliest FUTURE date found in the notice.
    If none are future, return None.
    """
    cands = _extract_all_date_candidates(text)
    isos = []
    for c in cands:
        iso = _parse_date_flex_any(c)
        if iso:
            isos.append(iso)

    if not isos:
        return None

    # Keep only future-ish (dts >= 0), then pick earliest upcoming
    future = []
    for iso in isos:
        dts = days_to_sale(iso)
        if dts is not None and dts >= 0:
            future.append((dts, iso))

    if not future:
        return None

    future.sort(key=lambda x: x[0])
    return future[0][1]


def _extract_county_from_text(text: str) -> str | None:
    """
    TNLegalPub often contains "... Register’s Office for Sumner County, Tennessee"
    or "Shelby County Courthouse" etc.
    """
    t = _norm_whitespace(text)

    # "for Sumner County"
    m = re.search(r"\bfor\s+([A-Za-z \-]+?)\s+County\b", t, flags=re.IGNORECASE)
    if m:
        return _normalize_county(m.group(1))

    # "Sumner County Courthouse"
    m = re.search(r"\b([A-Za-z \-]+?)\s+County\s+Courthouse\b", t, flags=re.IGNORECASE)
    if m:
        return _normalize_county(m.group(1))

    # "in the Register’s Office for Davidson County"
    m = re.search(r"\bRegister[’']?s Office for\s+([A-Za-z \-]+?)\s+County\b", t, flags=re.IGNORECASE)
    if m:
        return _normalize_county(m.group(1))

    return None


def _extract_address_from_text(text: str) -> str | None:
    if not text:
        return None
    t = _norm_whitespace(text)

    # Strong labeled patterns
    labeled = [
        r"\bProperty Address:\s*([0-9].{10,140}?\b(?:TN|Tennessee)\s+\d{5}\b)",
        r"\bAddress:\s*([0-9].{10,140}?\b(?:TN|Tennessee)\s+\d{5}\b)",
    ]
    for pat in labeled:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            return _norm_whitespace(m.group(1))

    # Common phrasing patterns
    patterns = [
        r"\b(commonly known as|located at|located on)\s+([0-9].{10,140}?\b(?:TN|Tennessee)\s+\d{5}\b)",
        r"\bproperty.*?\blocated\s+at\s+([0-9].{10,140}?\b(?:TN|Tennessee)\s+\d{5}\b)",
        r"\b([0-9]{1,6}\s+[A-Za-z0-9\.\-# ]{4,90}?),\s*([A-Za-z \-]{2,40}?),\s*(?:TN|Tennessee)\s+(\d{5})\b",
    ]

    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if not m:
            continue

        if pat.startswith(r"\b(commonly known as"):
            return _norm_whitespace(m.group(2))

        if pat.startswith(r"\bproperty"):
            return _norm_whitespace(m.group(1))

        # street, city, zip
        if pat.startswith(r"\b([0-9]{1,6}"):
            street = m.group(1)
            city = m.group(2)
            z = m.group(3)
            return _norm_whitespace(f"{street}, {city}, TN {z}")

    return None


def _extract_trustee_or_firm(text: str) -> str | None:
    if not text:
        return None
    t = _norm_whitespace(text)

    # Explicit labels (best case)
    labels = [
        "Substitute Trustee:",
        "Substitute Trustee is",
        "Trustee:",
        "Attorney:",
        "Attorney for",
        "Law Firm:",
        "Firm:",
    ]
    for lab in labels:
        m = re.search(re.escape(lab) + r"\s*([^\.]{3,160})", t, flags=re.IGNORECASE)
        if m:
            cand = _norm_whitespace(m.group(1))
            cand = re.split(
                r"\b(Sale Date|Sale Location|Sale Time|Address|County|Reference|Property)\b",
                cand,
                maxsplit=1,
                flags=re.IGNORECASE,
            )[0]
            cand = _norm_whitespace(cand)
            if cand and len(cand) >= 3:
                return cand

    # Law firm entity fallback
    firm_pat = r"\b([A-Z][A-Za-z0-9&,\.\- ]{6,90}\b(?:LLC|L\.L\.C\.|PC|P\.C\.|PLC|P\.L\.C\.|LLP|L\.L\.P\.))\b"
    m = re.search(firm_pat, t)
    if m:
        return _norm_whitespace(m.group(1))

    # Person-name trustee fallback: "... as Substitute Trustee"
    m = re.search(
        r"\b([A-Z][a-z]+(?:\s+[A-Z]\.)?(?:\s+[A-Z][a-z]+){1,2})\s+(?:as\s+)?Substitute Trustee\b",
        t,
    )
    if m:
        return _norm_whitespace(m.group(1))

    return None


def _trim_snippet(text: str, max_chars: int) -> str:
    t = _norm_whitespace(text)
    if not t:
        return ""
    if len(t) <= max_chars:
        return t

    # Keep the first chunk + a little tail (often contains county/sale lines)
    head = t[: int(max_chars * 0.75)]
    tail = t[-int(max_chars * 0.20) :]
    out = f"{head} ... {tail}"
    return out[:max_chars]


# -----------------------------
# TNLEGALPUB (listing + notice)
# -----------------------------
def _tnlegalpub_listing_urls(seed_url: str, max_pages: int):
    # WordPress taxonomy pagination: /page/2/
    urls = [seed_url]
    for p in range(2, max_pages + 1):
        if seed_url.endswith("/"):
            urls.append(f"{seed_url}page/{p}/")
        else:
            urls.append(f"{seed_url}/page/{p}/")
    return urls


def _tnlegalpub_extract_notice_links(list_html: str, base_url: str):
    soup = BeautifulSoup(list_html, "html.parser")
    links = []
    for a in soup.select("h2.entry-title a[href]"):
        href = a.get("href")
        if not href:
            continue
        u = urljoin(base_url, href)
        if "/legal_notice/" not in u:
            continue
        links.append(u)

    # Dedup preserve order
    seen = set()
    out = []
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def _tnlegalpub_parse_notice_page(notice_url: str, html: str):
    soup = BeautifulSoup(html, "html.parser")

    title = None
    h1 = soup.select_one("h1.entry-title")
    if h1:
        title = _norm_whitespace(h1.get_text(" ", strip=True))

    # Main content: WordPress commonly uses .entry-content, but fall back to body text
    content = ""
    entry = soup.select_one(".entry-content")
    if entry:
        content = entry.get_text(" ", strip=True)
    else:
        # fallback: find the first article text
        art = soup.select_one("article")
        content = art.get_text(" ", strip=True) if art else soup.get_text(" ", strip=True)

    content = _norm_whitespace(content)
    if not content:
        return None

    county = _extract_county_from_text(content)
    sale_date_iso = _pick_best_sale_date_iso(content)
    address = _extract_address_from_text(content)
    trustee = _extract_trustee_or_firm(content)

    return {
        "title": title,
        "county": county,
        "sale_date_iso": sale_date_iso,
        "address": address,
        "trustee": trustee,
        "raw_text": content,
        "url": notice_url,
    }


# -----------------------------
# MAIN BOT
# -----------------------------
def run():
    print(
        f"[PublicNoticesBot] SEEDS={SEED_URLS_PUBLIC_NOTICES} "
        f"allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]"
    )

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

    sample_kept = []
    sample_county_missing = []

    seen_lead_keys = set()

    for seed in SEED_URLS_PUBLIC_NOTICES:
        # Only fully implement tnlegalpub right now
        if "tnlegalpub.com/notice_type/foreclosure" not in seed:
            continue

        base_url = "https://tnlegalpub.com/"

        listing_urls = _tnlegalpub_listing_urls(seed, PUBLIC_NOTICES_MAX_LIST_PAGES)
        all_notice_links = []

        for u in listing_urls:
            try:
                html = fetch(u)
                list_pages_fetched += 1
            except Exception:
                continue

            links = _tnlegalpub_extract_notice_links(html, base_url=base_url)
            all_notice_links.extend(links)

            if len(all_notice_links) >= MAX_NOTICE_LINKS_PER_SOURCE:
                break

        # Dedup
        uniq = []
        seen = set()
        for u in all_notice_links:
            if u not in seen:
                seen.add(u)
                uniq.append(u)
        all_notice_links = uniq[:MAX_NOTICE_LINKS_PER_SOURCE]
        notice_links_found += len(all_notice_links)

        # Fetch + parse each notice page
        for notice_url in all_notice_links:
            try:
                notice_html = fetch(notice_url)
                notice_pages_fetched_ok += 1
            except Exception:
                continue

            parsed = _tnlegalpub_parse_notice_page(notice_url, notice_html)
            if not parsed:
                skipped_short += 1
                continue

            parsed_ok += 1

            county = parsed.get("county")
            sale_date_iso = parsed.get("sale_date_iso")

            if not sale_date_iso:
                skipped_no_sale += 1
                continue

            dts = days_to_sale(sale_date_iso)
            if dts is None:
                skipped_no_sale += 1
                continue
            if dts < 0:
                skipped_expired += 1
                continue

            # County handling
            if not county:
                skipped_county_missing += 1
                sample_county_missing.append(f"url={notice_url} guess=")
                continue

            if not _is_allowed_county(county):
                skipped_out_of_geo += 1
                continue
            if TARGET_COUNTIES and county not in TARGET_COUNTIES:
                skipped_out_of_geo += 1
                continue

            if not (_DTS_MIN <= dts <= _DTS_MAX):
                skipped_outside_window += 1
                continue

            status = _status_from_dts(dts)
            if status in (None, "EXPIRED"):
                skipped_kill += 1
                continue

            address = parsed.get("address") or ""
            trustee = parsed.get("trustee") or ""

            # Lead key
            lead_key = make_lead_key(
                "TNLEGALPUB",
                notice_url,
                county,
                sale_date_iso,
                address or (parsed.get("title") or ""),
            )

            if lead_key in seen_lead_keys:
                skipped_dup_in_run += 1
                continue
            seen_lead_keys.add(lead_key)

            existing_id = find_existing_by_lead_key(lead_key)

            score_for_create = _score_from_status(dts, status)

            title = parsed.get("title") or f"Foreclosure Notice ({status})"
            if address:
                title = f"{address} ({status})"

            raw_snippet = _trim_snippet(parsed.get("raw_text", ""), MAX_NOTICE_TEXT_CHARS)

            props = build_properties(
                title=title,
                source="TNLegalPub",
                distress_type="Foreclosure",
                county=county,
                address=(address if address else None),
                sale_date_iso=sale_date_iso,
                trustee_attorney=(trustee if trustee else None),
                contact_info=(trustee if trustee else None),
                raw_snippet=raw_snippet,
                url=notice_url,
                score=(None if existing_id else score_for_create),
                status=status,
                lead_key=lead_key,
                days_to_sale=dts,
            )

            if existing_id:
                update_lead(existing_id, props)
                updated += 1
            else:
                create_lead(props)
                created += 1

            filtered_in += 1

            if len(sample_kept) < 5:
                sample_kept.append(f"county={county} sale={sale_date_iso} dts={dts} url={notice_url}")

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
