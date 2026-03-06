# src/utils.py
import hashlib
import os
import re
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

from bs4 import BeautifulSoup


# ============================================================
# HTTP
# ============================================================

def fetch(url: str) -> str:
    import requests  # lazy — only imported when an actual HTTP fetch is made
    headers = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


# ============================================================
# TEXT HELPERS
# ============================================================

def contains_any(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any((k or "").lower() in t for k in (keywords or []))


def soup_text(soup: BeautifulSoup) -> str:
    return soup.get_text("\n")


# ============================================================
# DATE HELPERS (legacy)
# ============================================================

def find_date_iso(text: str) -> str:
    """
    Attempts to find a date like:
      January 5, 2026
      01/05/2026
    Returns ISO yyyy-mm-dd or "".
    """
    if not text:
        return ""

    month_pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    match = re.search(month_pattern, text, re.IGNORECASE)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%B %d, %Y")
            return d.date().isoformat()
        except Exception:
            pass

    num_pattern = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
    match = re.search(num_pattern, text)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%m/%d/%Y")
            return d.date().isoformat()
        except Exception:
            pass

    return ""


# ============================================================
# COUNTY / CONTACT / ADDRESS (legacy for TaxPagesBot)
# ============================================================

def guess_county(text: str) -> str:
    """
    Legacy heuristic used by TaxPagesBot.
    Returns base county name (e.g., 'Davidson') or ''.
    """
    if not text:
        return ""
    counties = [
        "Davidson", "Williamson", "Rutherford", "Sumner", "Wilson",
        "Maury", "Montgomery", "Robertson", "Dickson", "Bedford",
        "Putnam", "Shelby", "Hamilton", "Knox", "Bledsoe", "Rhea",
    ]
    low = text.lower()
    for c in counties:
        if c.lower() in low:
            return c
    return ""


def extract_contact(text: str) -> str:
    """
    Legacy: returns first email else first phone else ''.
    """
    if not text:
        return ""
    email_match = re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", text)
    if email_match:
        return email_match.group(0)

    phone_match = re.search(r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b", text)
    if phone_match:
        return phone_match.group(0)

    return ""


def extract_address(text: str) -> str:
    """
    Legacy: very light address sniffing.
    """
    if not text:
        return ""
    addr_pattern = (
        r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+"
        r"(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b"
    )
    match = re.search(addr_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return ""


def extract_trustee_or_attorney(text: str) -> str:
    """
    Legacy fallback used by TaxPagesBot.
    """
    if not text:
        return ""
    trustee_pattern = r"(Substitute Trustee|Substitute Trustees|Trustee|Attorney)[^,\n.]{0,160}"
    match = re.search(trustee_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return ""


# ============================================================
# URL CANONICALIZATION (stability)
# ============================================================

def canonicalize_url(url: str) -> str:
    """
    Remove common tracking params/fragments so lead_key doesn't churn.
    Safe to call everywhere; returns original on parse error.
    """
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        # drop fragment
        fragment = ""
        qs = parse_qsl(parts.query, keep_blank_values=False)
        bad = {
            "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
            "gclid", "fbclid", "ref", "ref_id", "_ga"
        }
        qs2 = [(k, v) for (k, v) in qs if k.lower() not in bad]
        query = urlencode(qs2, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, fragment))
    except Exception:
        return url.strip()


# ============================================================
# LEAD KEY (single stable implementation)
# ============================================================

def _norm_key_part(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def make_lead_key(*parts: str) -> str:
    """
    Stable lead key (SHA1 hex = 40 chars).
    Accepts any number of parts.
    """
    base = "|".join(_norm_key_part(p) for p in parts if p is not None and str(p).strip() != "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def get_current_run_id() -> str:
    """
    Returns current run_id from environment.
    """
    return os.getenv("FALCO_RUN_ID", "unknown_run")
