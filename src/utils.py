# src/utils.py
from __future__ import annotations

import hashlib
import re
from datetime import datetime
from urllib.parse import urlparse, urlunparse

import requests
from bs4 import BeautifulSoup


def fetch(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (Falco Distress Bot)"
    }
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def contains_any(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords)


def find_date_iso(text: str) -> str:
    """Attempts to find a date like:
    - January 5, 2026
    - 01/05/2026
    Returns ISO date (YYYY-MM-DD) or "".
    """
    if not text:
        return ""

    # Month word format
    month_pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    match = re.search(month_pattern, text, re.IGNORECASE)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%B %d, %Y")
            return d.date().isoformat()
        except Exception:
            pass

    # Numeric format
    num_pattern = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
    match = re.search(num_pattern, text)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%m/%d/%Y")
            return d.date().isoformat()
        except Exception:
            pass

    return ""


def guess_county(text: str) -> str:
    """Very light heuristic: returns the first county base name found in text."""
    counties = [
        "Davidson","Williamson","Rutherford","Sumner","Wilson","Maury",
        "Montgomery","Robertson","Dickson","Bedford","Putnam",
        "Shelby","Hamilton","Knox","Bledsoe","Rhea"
    ]
    t = (text or "").lower()
    for c in counties:
        if c.lower() in t:
            return c
    return ""


def extract_contact(text: str) -> str:
    if not text:
        return ""
    email_match = re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", text)
    if email_match:
        return email_match.group(0)

    phone_match = re.search(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", text)
    if phone_match:
        return phone_match.group(0)

    return ""


def extract_address(text: str) -> str:
    """Best-effort street address extraction."""
    if not text:
        return ""
    addr_pattern = (
        r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+"
        r"(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b"
        r"(?:[^\n,]{0,40})"
    )
    match = re.search(addr_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip(" ,;\n\t")
    return ""


def extract_trustee_or_attorney(text: str) -> str:
    """Best-effort trustee/attorney extraction."""
    if not text:
        return ""
    trustee_pattern = r"\b(Substitute\s+Trustee|Trustee|Attorney)\b[^\n,.]{0,140}"
    match = re.search(trustee_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return ""


# ----------------------------
# URL + Lead key
# ----------------------------

def canonicalize_url(url: str) -> str:
    """Remove query/fragment and normalize scheme/netloc for key stability."""
    if not url:
        return ""
    try:
        p = urlparse(url.strip())
        scheme = (p.scheme or "https").lower()
        netloc = (p.netloc or "").lower()
        path = re.sub(r"/+$", "", p.path or "")
        return urlunparse((scheme, netloc, path, "", "", ""))
    except Exception:
        return (url or "").strip()


def _norm_key_part(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def make_lead_key(*parts: str) -> str:
    """Return a deterministic Lead Key (SHA1 hex = 40 chars).

    Backward compatible with previous bots: they pass parts like
      (distress_type, county, sale_date_iso, address, trustee, url)

    Stability hardening:
    - whitespace normalization on every part
    - URL canonicalization (strip query/fragment, normalize host)
    - still omits empty parts (to avoid breaking existing lead_key values already stored in Notion)
    """
    normed: list[str] = []
    for i, p in enumerate(parts):
        if not p:
            continue
        if i == len(parts) - 1 and isinstance(p, str) and (p.startswith("http://") or p.startswith("https://")):
            p = canonicalize_url(p)
        normed.append(_norm_key_part(p))

    base = "|".join(normed)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
