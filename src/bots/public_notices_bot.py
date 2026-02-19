import os
import re
import hashlib
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale


# ============================================================
# CONFIG (per-bot env overrides)
# ============================================================

SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

HEADERS = {"User-Agent": "Mozilla/5.0"}

_ALLOWED_COUNTIES_BASE = {
    c.strip() for c in os.getenv(
        "FALCO_ALLOWED_COUNTIES",
        "Davidson,Williamson,Rutherford,Wilson,Sumner",
    ).split(",") if c.strip()
}

# Default PublicNoticesBot window is more permissive than the other bots
# so it doesn't silently skip near-term sales.
_DTS_MIN = int(os.getenv("FALCO_PUBLIC_DTS_MIN", os.getenv("FALCO_DTS_MIN", "0")))
_DTS_MAX = int(os.getenv("FALCO_PUBLIC_DTS_MAX", os.getenv("FALCO_DTS_MAX", "120")))

MAX_LIST_PAGES = int(os.getenv("FALCO_PUBLIC_MAX_LIST_PAGES", "8"))
MAX_NOTICE_LINKS = int(os.getenv("FALCO_PUBLIC_MAX_NOTICE_LINKS", "200"))
MAX_SNIPPET_LEN = int(os.getenv("FALCO_PUBLIC_MAX_SNIPPET_LEN", "1200"))

DEBUG = os.getenv("FALCO_PUBLIC_DEBUG", "0") == "1"


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = _norm_ws(name)
    if n.lower().endswith(" county"):
        n = n[:-7]
    return n.strip()


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


# ============================================================
# DATE PARSING
# ============================================================

def _parse_date_flex(s: str) -> str | None:
    if not s:
        return None

    s = _norm_ws(s).rstrip(".,;")

    # Normalize ALL CAPS months into Title Case
    if re.search(r"[A-Za-z]", s):
        s = s.title()

    fmts = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%B %d, %Y",
        "%b %d, %Y",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _extract_sale_date_candidates_with_context(text: str) -> list[str]:
    """
    Priority candidates: dates near "sale", "foreclosure sale", "public auction", "sold at".
    Then fallback: all date-like strings.
    """
    t = _norm_ws(text)

    out = []

    # Context-first patterns (these are the money)
    context_patterns = [
        r"(?:foreclosure\s+sale\s+on|sold\s+at\s+foreclosure\s+sale\s+on|sale\s+at\s+public\s+auction\s+will\s+be\s+on|scheduled\s+to\s+be\s+sold\s+at\s+foreclosure\s+sale\s+on)\s+([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
        r"(?:foreclosure\s+sale\s+on|sale\s+date\s+is|sale\s+at\s+public\s+auction\s+will\s+be\s+on)\s+(\d{1,2}/\d{1,2}/\d{2,4})",
        r"(?:foreclosure\s+sale\s+on|sale\s+date\s+is|sale\s+at\s+public\s+auction\s+will\s+be\s+on)\s+(\d{1,2}-\d{1,2}-\d{2,4})",
    ]

    for pat in context_patterns:
        for m in re.finditer(pat, t, flags=re.IGNORECASE):
            out.append(m.group(1))

    # Fallback patterns (broad)
    month_pat = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}"
    for m in re.finditer(month_pat, t, flags=re.IGNORECASE):
        out.append(m.group(0))
    for m in re.finditer(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t):
        out.append(m.group(0))
    for m in re.finditer(r"\b\d{1,2}-\d{1,2}-\d{2,4}\b", t):
        out.append(m.group(0))

    # De-dupe preserve order
    seen = set()
    uniq = []
    for x in out:
        x = _norm_ws(x)
        if not x or x in seen:
            continue
        seen.add(x)
        uniq.append(x)
    return uniq


def _pick_best_sale_date_iso(text: str) -> tuple[str | None, int | None]:
    """
    Choose a future date that is inside [_DTS_MIN, _DTS_MAX].
    If multiple, choose the closest one (smallest dts).
    """
    candidates = _extract_sale_date_candidates_with_context(text)

    best_iso = None
    best_dts = None

    for cand in candidates:
        iso = _parse_date_flex(cand)
        if not iso:
            continue

        dts = days_to_sale(iso)
        if dts is None:
            continue
        if dts < 0:
            continue

        # Must be inside window
        if not (_DTS_MIN <= dts <= _DTS_MAX):
            continue

        if best_iso is None or dts < best_dts:
            best_iso = iso
            best_dts = dts

    return best_iso, best_dts


# ============================================================
# ADDRESS + TRUSTEE EXTRACTION (basic but useful)
# ============================================================

def _extract_address(text: str) -> str | None:
    t = _norm_ws(text)
    pats = [
        r"Property Address:\s*([0-9].+?TN\s+\d{5})",
        r"Address:\s*([0-9].+?TN\s+\d{5})",
        r"(?:commonly known as|located at|located on)\s+([0-9].+?TN\s+\d{5})",
        r"\b([0-9]{1,6}\s+[A-Za-z0-9\.\-# ]+,\s*[A-Za-z \-]+,\s*TN\s+\d{5})",
    ]
    for pat in pats:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            return _norm_ws(m.group(1))
    return None


def _extract_trustee(text: str) -> str | None:
    t = _norm_ws(text)
    labels = ["Substitute Trustee:", "Trustee:", "Attorney:", "Firm:"]
    for lab in labels:
        m = re.search(re.escape(lab) + r"\s*([^\.]{4,150})", t, flags=re.IGNORECASE)
        if m:
            return _norm_ws(m.group(1))

    firm_pat = r"\b([A-Z][A-Za-z0-9&,\.\- ]{5,80}(?:LLC|LLP|PLC|PC))\b"
    m = re.search(firm_pat, t)
    if m:
        return _norm_ws(m.group(1))
    return None


def _extract_county(text: str) -> str | None:
    t = _norm_ws(text)

    # Common in notices
    m = re.search(r"\b([A-Za-z]+)\s+County,\s*Tennessee\b", t, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1).title()} County"

    # Another common phrasing
    m = re.search(r"\bRegister(?:’|')s\s+Office\s+for\s+([A-Za-z]+)\s+County,\s*Tennessee\b", t, flags=re.IGNORECASE)
    if m:
        return f"{m.group(1).title()} County"

    return None


# ============================================================
# PAGINATION (tnlegalpub specifically)
# ============================================================

def _is_tnlegalpub(url: str) -> bool:
    try:
        return "tnlegalpub.com" in (urlparse(url).netloc or "")
    except Exception:
        return False


def _list_pages_for_seed(seed: str) -> list[str]:
    if _is_tnlegalpub(seed):
        # tnlegalpub uses /page/2/ style
        pages = []
        base = seed.rstrip("/") + "/"
        pages.append(base)
        for i in range(2, MAX_LIST_PAGES + 1):
            pages.append(urljoin(base, f"page/{i}/"))
        return pages

    # Non-tnlegalpub: just 1 page for now
    return [seed]


def run():
    print(f"[PublicNoticesBot] SEEDS={SEEDS} allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]")

    session = requests.Session()

    list_pages_fetched = 0
    notice_links = []
    seen_links = set()

    # --- scrape listing pages
    for seed in SEEDS:
        for list_url in _list_pages_for_seed(seed):
            try:
                r = session.get(list_url, headers=HEADERS, timeout=20)
            except Exception:
                continue
            if r.status_code != 200:
                continue

            list_pages_fetched += 1
            soup = BeautifulSoup(r.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/legal_notice/" in href:
                    full = urljoin(list_url, href)
                    if full not in seen_links:
                        seen_links.add(full)
                        notice_links.append(full)

            if len(notice_links) >= MAX_NOTICE_LINKS:
                break
        if len(notice_links) >= MAX_NOTICE_LINKS:
            break

    # --- counters
    notice_pages_fetched_ok = 0
    parsed_ok = 0
    filtered_in = 0
    created = 0
    updated = 0

    skipped_no_sale = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_kill = 0
    skipped_dup_in_run = 0

    seen_in_run = set()

    # --- scrape notice pages
    for url in notice_links:
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        notice_pages_fetched_ok += 1

        soup = BeautifulSoup(r.text, "html.parser")
        text = _norm_ws(soup.get_text(" "))

        sale_date_iso, dts = _pick_best_sale_date_iso(text)
        if not sale_date_iso:
            # We DID attempt context + fallback and still found nothing in-window
            # Distinguish between "no dates at all" vs "dates but outside window"
            any_dates = _extract_sale_date_candidates_with_context(text)
            if any_dates:
                skipped_outside_window += 1
                if DEBUG:
                    print(f"[PublicNoticesBot] outside_window url={url} sample_dates={any_dates[:5]}")
            else:
                skipped_no_sale += 1
                if DEBUG:
                    print(f"[PublicNoticesBot] no_sale url={url}")
            continue

        if dts is None:
            skipped_no_sale += 1
            continue
        if dts < 0:
            skipped_expired += 1
            continue

        county = _extract_county(text)
        if county and not county.lower().endswith(" county"):
            county = county + " County"

        if not _is_allowed_county(county):
            skipped_out_of_geo += 1
            if DEBUG:
                print(f"[PublicNoticesBot] out_of_geo url={url} county={county}")
            continue

        address = _extract_address(text)
        trustee = _extract_trustee(text)

        lead_key_raw = f"PUBLICNOTICE|{county}|{sale_date_iso}|{address}|{url}"
        lead_key = hashlib.sha1(lead_key_raw.encode("utf-8")).hexdigest()

        if lead_key in seen_in_run:
            skipped_dup_in_run += 1
            continue
        seen_in_run.add(lead_key)

        payload = {
            "title": address or "Foreclosure Notice",
            "source": "PublicNotices",
            "distress_type": "Foreclosure",
            "county": county,
            "address": address,
            "sale_date_iso": sale_date_iso,
            "trustee_attorney": trustee,
            "contact_info": trustee,
            "raw_snippet": text[:MAX_SNIPPET_LEN],
            "url": url,
            "lead_key": lead_key,
            "days_to_sale": dts,
        }

        props = build_properties(payload)

        existing = find_existing_by_lead_key(lead_key)
        if existing:
            update_lead(existing, props)
            updated += 1
        else:
            create_lead(props)
            created += 1

        parsed_ok += 1
        filtered_in += 1

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} "
        f"notice_links_found={len(notice_links)} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} "
        f"parsed_ok={parsed_ok} filtered_in={filtered_in} "
        f"created={created} updated={updated} "
        f"skipped_no_sale={skipped_no_sale} skipped_expired={skipped_expired} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_outside_window={skipped_outside_window} "
        f"skipped_kill={skipped_kill} skipped_dup_in_run={skipped_dup_in_run}"
    )
    print("[PublicNoticesBot] Done.")
