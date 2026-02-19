# src/bots/public_notices_bot.py

import os
import re
import hashlib
from datetime import date
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
# CONFIG VIA ENV
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

MAX_NOTICE_TEXT_CHARS = int(os.getenv("FALCO_MAX_NOTICE_TEXT_CHARS", "1200"))
MAX_LIST_PAGES = int(os.getenv("FALCO_PUBLIC_NOTICES_MAX_LIST_PAGES", "10"))

SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]


# ============================================================
# HELPERS
# ============================================================

def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = " ".join(name.strip().split()).replace("\u00a0", " ")
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


# ============================================================
# DATE EXTRACTION (TNLEGALPUB NEEDS MULTI-DATE PICKING)
# ============================================================

_MONTH_MAP = {
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


def _iso_from_mdy(mm: int, dd: int, yy: int) -> str | None:
    try:
        if yy < 100:
            yy += 2000
        return date(yy, mm, dd).isoformat()
    except Exception:
        return None


def _extract_all_dates_iso(text: str) -> list[str]:
    """
    Pull ALL date candidates in a notice.
    Returns list of ISO dates (YYYY-MM-DD), de-duped, order preserved.
    """
    if not text:
        return []
    t = _norm_whitespace(text)

    found: list[str] = []

    # numeric dates: 2/25/2026 or 2/25/26
    for m in re.finditer(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b", t):
        iso = _iso_from_mdy(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        if iso and iso not in found:
            found.append(iso)

    # month name dates: February 25, 2026 / Feb 25, 2026
    for m in re.finditer(
        r"\b(January|February|March|April|May|June|July|August|September|October|November|December|"
        r"Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+(\d{1,2}),\s+(\d{4})\b",
        t,
        flags=re.IGNORECASE,
    ):
        mon_raw = m.group(1).strip().lower()
        day = int(m.group(2))
        year = int(m.group(3))

        if mon_raw.startswith("sept"):
            month = 9
        else:
            key3 = mon_raw[:3]
            month = _MONTH_MAP.get(key3) or _MONTH_MAP.get(mon_raw)

        if month:
            try:
                iso = date(year, month, day).isoformat()
                if iso not in found:
                    found.append(iso)
            except Exception:
                pass

    return found


def _pick_best_sale_date_iso(text: str) -> str | None:
    """
    TNLegalPub notices may include:
      - publication dates (expired)
      - original sale dates (expired)
      - postponed/rescheduled dates (future)
    So we select the best sale date by scoring all extracted dates:
      1) prefer dates where DTS is within [_DTS_MIN, _DTS_MAX]
      2) otherwise prefer the nearest FUTURE date (smallest non-negative DTS)
    """
    all_iso = _extract_all_dates_iso(text)
    if not all_iso:
        return None

    scored = []
    for iso in all_iso:
        dts = days_to_sale(iso)
        if dts is None:
            continue
        scored.append((iso, dts))

    if not scored:
        return None

    # Prefer within window
    in_window = [(iso, dts) for (iso, dts) in scored if dts >= 0 and _DTS_MIN <= dts <= _DTS_MAX]
    if in_window:
        # choose earliest upcoming in-window (smallest dts)
        in_window.sort(key=lambda x: x[1])
        return in_window[0][0]

    # Otherwise choose nearest future date
    future = [(iso, dts) for (iso, dts) in scored if dts >= 0]
    if future:
        future.sort(key=lambda x: x[1])
        return future[0][0]

    # If all are past, return None
    return None


# ============================================================
# COUNTY / ADDRESS / TRUSTEE EXTRACTION
# ============================================================

def _extract_county_from_text(text: str) -> str | None:
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
    if not text:
        return None
    t = _norm_whitespace(text)

    patterns = [
        r"\b(commonly known as|located at|located on)\s+([0-9].{10,140}?\b(TN|Tennessee)\s+\d{5}\b)",
        r"\bproperty.*?\blocated\s+at\s+([0-9].{10,140}?\b(TN|Tennessee)\s+\d{5}\b)",
        r"\b([0-9]{1,6}\s+[A-Za-z0-9\.\-# ]{4,90}?),\s*([A-Za-z \-]{2,40}?),\s*(TN|Tennessee)\s+(\d{5})\b",
    ]

    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if not m:
            continue

        if m.lastindex and m.lastindex >= 2 and (m.group(1) or "").lower() in {"commonly known as", "located at", "located on"}:
            return _norm_whitespace(m.group(2))

        if pat.startswith(r"\bproperty"):
            return _norm_whitespace(m.group(1))

        if pat.startswith(r"\b([0-9]{1,6}"):
            street = m.group(1)
            city = m.group(2)
            z = m.group(4)
            return _norm_whitespace(f"{street}, {city}, TN {z}")

    return None


def _extract_trustee_or_firm(text: str) -> str | None:
    if not text:
        return None
    t = _norm_whitespace(text)

    # explicit labels
    for lab in ["Substitute Trustee:", "Trustee:", "Attorney:", "Law Firm:", "Firm:"]:
        m = re.search(re.escape(lab) + r"\s*([^\.]{3,140})", t, flags=re.IGNORECASE)
        if m:
            cand = _norm_whitespace(m.group(1))
            cand = re.split(r"\b(Sale Date|Sale Location|Sale Time|Address|County|Reference)\b", cand, maxsplit=1)[0]
            cand = _norm_whitespace(cand)
            if cand:
                return cand

    # firm-ish fallback
    m = re.search(r"\b([A-Z][A-Za-z0-9&,\.\- ]{6,90}\b(?:LLC|L\.L\.C\.|PC|P\.C\.|PLC|P\.L\.C\.|LLP|L\.L\.P\.))\b", t)
    if m:
        return _norm_whitespace(m.group(1))

    return None


# ============================================================
# TNLEGALPUB LIST + NOTICE PARSER
# ============================================================

def _tnlegalpub_list_page_url(page: int) -> str:
    if page <= 1:
        return "https://tnlegalpub.com/notice_type/foreclosure/"
    return f"https://tnlegalpub.com/notice_type/foreclosure/page/{page}/"


def _tnlegalpub_extract_notice_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.select("article.post h2.entry-title a[href]"):
        href = (a.get("href") or "").strip()
        if href.startswith("http"):
            links.append(href)
    return links


def _tnlegalpub_parse_notice(html: str, url: str) -> dict | None:
    soup = BeautifulSoup(html, "html.parser")
    container = soup.select_one("main#content") or soup.select_one("div.page-content") or soup.body
    text = _norm_whitespace(container.get_text(" ", strip=True) if container else soup.get_text(" ", strip=True))

    sale_date_iso = _pick_best_sale_date_iso(text)
    if not sale_date_iso:
        return None

    county = _extract_county_from_text(text)
    address = _extract_address_from_text(text)
    trustee = _extract_trustee_or_firm(text)

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
# MAIN RUN (TNLEGALPUB ONLY FOR NOW)
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

    seen_keys = set()

    # ---- LIST PAGES ----
    all_links: list[str] = []
    for page in range(1, MAX_LIST_PAGES + 1):
        list_url = _tnlegalpub_list_page_url(page)
        try:
            html = fetch(list_url)
            list_pages_fetched += 1
        except Exception:
            break

        links = _tnlegalpub_extract_notice_links(html)
        if not links:
            break

        all_links.extend(links)

        if page >= 2 and len(set(links)) <= 2:
            break

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

        lead_key = _make_lead_key("TNLEGALPUB", notice_url, county, sale_date_iso, parsed.get("address"))

        if lead_key in seen_keys:
            skipped_dup_in_run += 1
            continue
        seen_keys.add(lead_key)

        existing_id = find_existing_by_lead_key(lead_key)

        # NEVER overwrite fields with blanks.
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
