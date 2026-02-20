import re
import requests
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode


def fetch(url: str) -> str:
    headers = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.text


def contains_any(text: str, keywords: list[str]) -> bool:
    t = (text or "").lower()
    return any(k.lower() in t for k in keywords)


def find_date_iso(text: str) -> str:
    """Attempts to find a date like: January 5, 2026 or 01/05/2026."""
    month_pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    match = re.search(month_pattern, text or "", re.IGNORECASE)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%B %d, %Y")
            return d.date().isoformat()
        except Exception:
            pass

    num_pattern = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
    match = re.search(num_pattern, text or "")
    if match:
        try:
            d = datetime.strptime(match.group(0), "%m/%d/%Y")
            return d.date().isoformat()
        except Exception:
            pass

    return ""


def extract_trustee_or_attorney(text: str) -> str:
    trustee_pattern = r"(Substitute Trustee|Substitute Trustees|Trustee|Attorney)[^\n,.]{0,160}"
    match = re.search(trustee_pattern, text or "", re.IGNORECASE)
    if match:
        return match.group(0).strip()
    return ""


def _norm_key_part(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def make_lead_key(*parts: str) -> str:
    """Stable lead key (sha1 hex, 40 chars). Avoids volatility from whitespace and case."""
    base = "|".join(_norm_key_part(p) for p in parts if p is not None and str(p).strip() != "")
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def canonicalize_url(url: str) -> str:
    """Remove common tracking params/fragments so lead_key doesn't churn."""
    if not url:
        return ""
    try:
        parts = urlsplit(url.strip())
        # drop fragment
        fragment = ""
        # keep only stable query params
        qs = parse_qsl(parts.query, keep_blank_values=False)
        bad = {"utm_source","utm_medium","utm_campaign","utm_term","utm_content","gclid","fbclid","ref","ref_id","_ga"}
        qs2 = [(k,v) for (k,v) in qs if k.lower() not in bad]
        query = urlencode(qs2, doseq=True)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, query, fragment))
    except Exception:
        return url.strip()


def soup_text(soup: BeautifulSoup) -> str:
    return soup.get_text("\n")


def parse_date_candidates(text: str) -> list[str]:
    """Return list of ISO date candidates found in text, best-effort."""
    t = text or ""
    cands = []
    # Month name
    for m in re.finditer(r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}", t, re.IGNORECASE):
        iso = find_date_iso(m.group(0))
        if iso:
            cands.append(iso)
    for m in re.finditer(r"\b\d{1,2}/\d{1,2}/\d{4}\b", t):
        iso = find_date_iso(m.group(0))
        if iso:
            cands.append(iso)
    # de-dupe preserve order
    out = []
    seen = set()
    for d in cands:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out
