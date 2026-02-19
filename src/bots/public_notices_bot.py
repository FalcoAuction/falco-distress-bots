import os
import re
import hashlib
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
)
from ..scoring import days_to_sale


SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

_ALLOWED_COUNTIES_BASE = {
    c.strip() for c in os.getenv(
        "FALCO_ALLOWED_COUNTIES",
        "Davidson,Williamson,Rutherford,Wilson,Sumner",
    ).split(",") if c.strip()
}

_DTS_MIN = int(os.getenv("FALCO_DTS_MIN", "21"))
_DTS_MAX = int(os.getenv("FALCO_DTS_MAX", "90"))

MAX_SNIPPET_LEN = 1200


def _norm_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _county_base(name: str | None) -> str | None:
    if not name:
        return None
    n = _norm_whitespace(name)
    if n.lower().endswith(" county"):
        n = n[:-7]
    return n.strip()


def _is_allowed_county(county: str | None) -> bool:
    base = _county_base(county)
    if not base:
        return False
    return base in _ALLOWED_COUNTIES_BASE


# ============================================================
# DATE EXTRACTION (ROBUST)
# ============================================================

def _parse_date_flex_any(s: str) -> str | None:
    if not s:
        return None

    s = _norm_whitespace(s)

    if re.search(r"[A-Za-z]", s):
        s = s.title()

    s = s.rstrip(".,;")

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


def _extract_all_date_candidates(text: str) -> list[str]:
    t = _norm_whitespace(text)

    candidates = []

    month_pat = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}"

    for m in re.finditer(month_pat, t, flags=re.IGNORECASE):
        candidates.append(m.group(0))

    for m in re.finditer(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", t):
        candidates.append(m.group(0))

    for m in re.finditer(r"\b\d{1,2}-\d{1,2}-\d{2,4}\b", t):
        candidates.append(m.group(0))

    seen = set()
    out = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)

    return out


# ============================================================
# ADDRESS EXTRACTION
# ============================================================

def _extract_address(text: str) -> str | None:
    t = _norm_whitespace(text)

    patterns = [
        r"Property Address:\s*([0-9].+?TN\s+\d{5})",
        r"Address:\s*([0-9].+?TN\s+\d{5})",
        r"(?:commonly known as|located at|located on)\s+([0-9].+?TN\s+\d{5})",
        r"\b([0-9]{1,6}\s+[A-Za-z0-9\.\-# ]+,\s*[A-Za-z \-]+,\s*TN\s+\d{5})",
    ]

    for pat in patterns:
        m = re.search(pat, t, flags=re.IGNORECASE)
        if m:
            return _norm_whitespace(m.group(1))

    return None


# ============================================================
# TRUSTEE / FIRM EXTRACTION
# ============================================================

def _extract_trustee(text: str) -> str | None:
    t = _norm_whitespace(text)

    labels = [
        "Substitute Trustee:",
        "Trustee:",
        "Attorney:",
        "Firm:",
    ]

    for lab in labels:
        m = re.search(re.escape(lab) + r"\s*([^\.]{4,150})", t, flags=re.IGNORECASE)
        if m:
            return _norm_whitespace(m.group(1))

    firm_pat = r"\b([A-Z][A-Za-z0-9&,\.\- ]{5,80}(?:LLC|LLP|PLC|PC))\b"
    m = re.search(firm_pat, t)
    if m:
        return _norm_whitespace(m.group(1))

    return None


# ============================================================
# MAIN
# ============================================================

def run():
    print(f"[PublicNoticesBot] SEEDS={SEEDS} allowed_counties={sorted(_ALLOWED_COUNTIES_BASE)} dts_window=[{_DTS_MIN},{_DTS_MAX}]")

    session = requests.Session()

    fetched_pages = 0
    notice_links = []
    skipped_dup_in_run = 0

    for seed in SEEDS:
        try:
            r = session.get(seed, headers=HEADERS, timeout=20)
        except Exception:
            continue

        if r.status_code != 200:
            continue

        fetched_pages += 1
        soup = BeautifulSoup(r.text, "html.parser")

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/legal_notice/" in href:
                full = urljoin(seed, href)
                if full not in notice_links:
                    notice_links.append(full)

    created = 0
    updated = 0
    parsed_ok = 0
    filtered_in = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_no_sale = 0

    seen_in_run = set()

    for url in notice_links:
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
        except Exception:
            continue

        if r.status_code != 200:
            continue

        text = _norm_whitespace(BeautifulSoup(r.text, "html.parser").get_text(" "))

        date_candidates = _extract_all_date_candidates(text)

        sale_date_iso = None
        best_dts = None

        for cand in date_candidates:
            iso = _parse_date_flex_any(cand)
            if not iso:
                continue

            dts = days_to_sale(iso)
            if dts is None or dts < 0:
                continue

            if sale_date_iso is None or dts < best_dts:
                sale_date_iso = iso
                best_dts = dts

        if not sale_date_iso:
            skipped_no_sale += 1
            continue

        if not (_DTS_MIN <= best_dts <= _DTS_MAX):
            skipped_outside_window += 1
            continue

        county_match = re.search(r"([A-Za-z]+ County), Tennessee", text)
        county = county_match.group(1) if county_match else None

        if not _is_allowed_county(county):
            skipped_out_of_geo += 1
            continue

        address = _extract_address(text)
        trustee = _extract_trustee(text)

        lead_key_raw = f"PUBLICNOTICE|{county}|{sale_date_iso}|{address}|{url}"
        lead_key = hashlib.sha1(lead_key_raw.encode()).hexdigest()

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
            "days_to_sale": best_dts,
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
        f"list_pages_fetched={fetched_pages} "
        f"notice_links_found={len(notice_links)} "
        f"parsed_ok={parsed_ok} filtered_in={filtered_in} "
        f"created={created} updated={updated} "
        f"skipped_no_sale={skipped_no_sale} "
        f"skipped_expired={skipped_expired} "
        f"skipped_out_of_geo={skipped_out_of_geo} "
        f"skipped_outside_window={skipped_outside_window} "
        f"skipped_dup_in_run={skipped_dup_in_run}"
    )

    print("[PublicNoticesBot] Done.")
