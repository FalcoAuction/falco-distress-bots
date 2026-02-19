# src/bots/public_notices_bot.py

import os
import re
import hashlib
from datetime import datetime, date
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


# ============================================================
# CONFIG VIA ENV (keeps it consistent with other bots)
# ============================================================

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

MAX_NOTICE_TEXT_CHARS = int(os.getenv("FALCO_MAX_NOTICE_TEXT_CHARS", "1200"))  # truncate raw snippet

SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

MAX_LIST_PAGES = int(os.getenv("FALCO_PUBLIC_NOTICES_MAX_LIST_PAGES", "10"))


# ============================================================
# HELPERS
# ============================================================

def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(name.strip().split())
    n = n.replace("\u00a0", " ")
    if n.lower().endswith(" county"):
        n = n[:-7].strip()
    return n


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


def _norm_whitespace(s: str) -> str:
    s = (s or "").replace("\u00a0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _truncate(s: str, n: int) -> str:
    s = s or ""
    if len(s) <= n:
        return s
    return s[: n - 3].rstrip() + "..."


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _make_lead_key(source: str, url: str, county: str | None, sale_date_iso: str | None, address: str | None):
    parts = [
        (source or "").strip().lower(),
        (url or "").strip().lower(),
        (county or "").strip().lower(),
        (sale_date_iso or "").strip().lower(),
        (address or "").strip().lower(),
    ]
    return _sha1("|".join(parts))


def _triage(dts: int):
    if dts <= 7:
        return "URGENT", 95
    if dts <= 14:
        return "HOT", 82
    return "GREEN", 65


def _parse_date_any(text: str) -> str | None:
    """
    Finds a date in formats like:
      - February 25, 2026
      - Feb 25, 2026
      - 02/25/2026
      - 2/25/26
    Returns ISO YYYY-MM-DD.
    """
    if not text:
        return None
    t = _norm_whitespace(text)

    # mm/dd/yyyy or m/d/yy
    m = re.search(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", t)
    if m:
        mm, dd, yy = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if yy < 100:
            yy += 2000
        try:
            return date(yy, mm, dd).isoformat()
        except Exception:
            pass

    # Month name formats
    m = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{1,2}),\s+(\d{4})\b",
        t,
        flags=re.IGNORECASE,
    )
    if m:
        month_raw = m.group(1)
        day = int(m.group(2))
        year = int(m.group(3))
        month_map = {
            "jan": 1, "january": 1,
            "feb": 2, "february": 2,
            "mar": 3, "march": 3,
            "apr": 4, "april": 4,
            "may": 5,
            "jun": 6, "june": 6,
            "jul": 7, "july": 7,
            "aug": 8, "august": 8,
            "sep": 9, "sept": 9, "september": 9,
            "oct": 10, "october": 10,
            "nov": 11, "november": 11,
            "dec": 12, "december": 12,
        }
        key = month_raw.strip().lower()
        key = key[:4] if key.startswith("sept") else key[:3] if len(key) > 3 else key
        # handle both "sept" and "sep"
        if month_raw.strip().lower().startswith("sept"):
            month = 9
        else:
            month = month_map.get(month_raw.strip().lower()[:3], None) or month_map.get(month_raw.strip().lower(), None)

        if month:
            try:
                return date(year, month, day).isoformat()
            except Exception:
                pass

    return None


def _extract_county_from_text(text: str) -> str | None:
    """
    TNLegalPub often contains patterns like:
      - "Register’s Office for Sumner County, Tennessee"
      - "... in the Register of Deeds Office of Bedford County, Tennessee"
      - "... Shelby County Courthouse ..."
    """
    if not text:
        return None
    t = text

    m = re.search(r"\bfor\s+([A-Za-z]+)\s+County,\s*Tennessee\b", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip() + " County"

    m = re.search(r"\bOffice\s+of\s+([A-Za-z]+)\s+County,\s*Tennessee\b", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip() + " County"

    m = re.search(r"\b([A-Za-z]+)\s+County\s+Courthouse\b", t, flags=re.IGNORECASE)
    if m:
        return m.group(1).strip() + " County"

    return None


def _extract_address_from_text(text: str) -> str | None:
    """
    Heuristics:
      - looks for "located at 123 Main St, City, Tennessee 37209"
      - or "commonly known as 2225 Brewers Landing, Memphis, TN 38104"
      - or "Property ... at 3308 Trevor Street, #2, Nashville, Tennessee 37209"
    """
    if not text:
        return None
    t = _norm_whitespace(text)

    patterns = [
        r"\b(commonly known as|located at|located on|property.*?at)\s+([0-9].{10,120}?\b(TN|Tennessee)\s+\d{5}\b)",
        r"\b([0-9]{1,6}\s+[A-Za-z0-9\.\-# ]{4,80}?),\s*([A-Za-z \-]{2,40}?),\s*(TN|Tennessee)\s+(\d{5})\b",
    ]

    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if not m:
            continue

        if len(m.groups()) >= 2 and (m.group(1) or "").lower() in {"commonly known as", "located at", "located on"}:
            addr = m.group(2)
            return _norm_whitespace(addr)

        # second pattern returns pieces
        if pat.startswith(r"\b([0-9]{1,6}"):
            street = m.group(1)
            city = m.group(2)
            st = m.group(3)
            z = m.group(4)
            st = "TN" if st.lower().startswith("tn") else "TN"
            return _norm_whitespace(f"{street}, {city}, {st} {z}")

    return None


def _extract_trustee_or_firm(text: str) -> str | None:
    """
    Heuristics for trustee/attorney/firm:
      - "Substitute Trustee: John Doe"
      - "Trustee: ..."
      - "Law Firm ..." or "P.C." or "LLC"
      - "Auction Vendor:" etc
    """
    if not text:
        return None
    t = _norm_whitespace(text)

    # explicit trustee labels
    for lab in ["Substitute Trustee:", "Trustee:", "Attorney:", "Law Firm:", "Firm:"]:
        m = re.search(re.escape(lab) + r"\s*([^\.]{3,120})", t, flags=re.IGNORECASE)
        if m:
            cand = _norm_whitespace(m.group(1))
            # stop at obvious next labels if they got captured
            cand = re.split(r"\b(Sale Date|Sale Location|Sale Time|Address|County|Reference)\b", cand, maxsplit=1)[0]
            cand = _norm_whitespace(cand)
            if cand:
                return cand

    # firm-ish pattern
    m = re.search(r"\b([A-Z][A-Za-z0-9&,\.\- ]{6,80}\b(?:LLC|L\.L\.C\.|PC|P\.C\.|PLC|P\.L\.C\.|LLP|L\.L\.P\.))\b", t)
    if m:
        return _norm_whitespace(m.group(1))

    return None


# ============================================================
# TNLEGALPUB LIST + NOTICE PARSER
# ============================================================

def _tnlegalpub_list_page_urls(page: int) -> str:
    if page <= 1:
        return "https://tnlegalpub.com/notice_type/foreclosure/"
    return f"https://tnlegalpub.com/notice_type/foreclosure/page/{page}/"


def _tnlegalpub_extract_notice_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("article.post h2.entry-title a[href]"):
        href = a.get("href", "").strip()
        if href and href.startswith("http"):
            links.append(href)
    return links


def _tnlegalpub_parse_notice(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")

    # main article text
    # (TNLegalPub is wordpress; the text usually lives in article / entry-content / page-content)
    container = soup.select_one("main#content") or soup.select_one("div.page-content") or soup.body
    text = _norm_whitespace(container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True))

    # sale date: try to find the first real date
    sale_date_iso = _parse_date_any(text)
    if not sale_date_iso:
        return None

    county = _extract_county_from_text(text)
    address = _extract_address_from_text(text)
    trustee = _extract_trustee_or_firm(text)

    # raw snippet (truncate hard)
    raw_snippet = _truncate(text, MAX_NOTICE_TEXT_CHARS)

    return {
        "source": "TNLegalPub",
        "url": url,
        "sale_date_iso": sale_date_iso,
        "county": county,
        "address": address,
        "trustee_attorney": trustee,
        "raw_snippet": raw_snippet,
    }


# ============================================================
# MAIN RUN
# ============================================================

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

    sample_kept = []
    sample_county_missing = []

    # only implement TNLegalPub cleanly for now (your logs show it's the only one hitting)
    seen_keys = set()

    # ---- LISTING PAGES ----
    all_links: list[str] = []
    for page in range(1, MAX_LIST_PAGES + 1):
        list_url = _tnlegalpub_list_page_urls(page)
        html = None
        try:
            html = fetch(list_url)
            list_pages_fetched += 1
        except Exception:
            break

        links = _tnlegalpub_extract_notice_links(html)
        if not links:
            break

        all_links.extend(links)

        # stop early if the page is mostly duplicates
        if page >= 2 and len(set(links)) <= 2:
            break

    # dedupe links
    all_links = list(dict.fromkeys(all_links))
    notice_links_found = len(all_links)

    # ---- NOTICE PAGES ----
    for notice_url in all_links:
        try:
            html = fetch(notice_url)
            notice_pages_fetched_ok += 1
        except Exception:
            continue

        parsed = _tnlegalpub_parse_notice(html, notice_url)
        if not parsed:
            skipped_no_sale += 1
            continue

        parsed_ok += 1

        sale_date_iso = parsed["sale_date_iso"]
        dts = days_to_sale(sale_date_iso)

        if dts is None:
            skipped_no_sale += 1
            continue
        if dts < 0:
            skipped_expired += 1
            continue
        if not (_DTS_MIN <= dts <= _DTS_MAX):
            skipped_outside_window += 1
            continue

        county = parsed.get("county")
        if not county:
            skipped_county_missing += 1
            sample_county_missing.append(f"url={notice_url} guess=?")
            continue

        if not _is_allowed_county(county):
            skipped_out_of_geo += 1
            continue

        status, score = _triage(dts)

        # lead key uses address if we have it; otherwise still stable by url+county+date
        lead_key = _make_lead_key("TNLEGALPUB", notice_url, county, sale_date_iso, parsed.get("address"))

        # prevent dup in same run
        if lead_key in seen_keys:
            skipped_dup_in_run += 1
            continue
        seen_keys.add(lead_key)

        existing_id = find_existing_by_lead_key(lead_key)

        # IMPORTANT: don't overwrite existing Notion fields with blanks.
        # build payload with only non-empty values.
        payload = {
            "title": f"Foreclosure ({status}) ({county})",
            "source": parsed["source"],
            "distress_type": "Foreclosure",
            "county": county,
            "sale_date_iso": sale_date_iso,
            "status": status,
            "url": notice_url,
            "lead_key": lead_key,
            "days_to_sale": dts,
            "raw_snippet": parsed.get("raw_snippet"),
        }
        if parsed.get("address"):
            payload["address"] = parsed["address"]
        if parsed.get("trustee_attorney"):
            payload["trustee_attorney"] = parsed["trustee_attorney"]
            payload["contact_info"] = parsed["trustee_attorney"]

        # only set score on create
        if not existing_id:
            payload["score"] = score

        props = build_properties(payload)

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
        f"skipped_kill={skipped_kill} skipped_dup_in_run={skipped_dup_in_run} skipped_county_missing={skipped_county_missing} "
        f"sample_kept={sample_kept} sample_county_missing={sample_county_missing}"
    )
    print("[PublicNoticesBot] Done.")
