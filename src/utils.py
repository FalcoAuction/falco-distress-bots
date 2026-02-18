import re
import requests
import hashlib
from bs4 import BeautifulSoup
from datetime import datetime

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
    """
    Attempts to find a date like:
    January 5, 2026
    01/05/2026
    """
    # Month word format
    month_pattern = r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+\d{4}"
    match = re.search(month_pattern, text, re.IGNORECASE)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%B %d, %Y")
            return d.date().isoformat()
        except:
            pass

    # Numeric format
    num_pattern = r"\b\d{1,2}/\d{1,2}/\d{4}\b"
    match = re.search(num_pattern, text)
    if match:
        try:
            d = datetime.strptime(match.group(0), "%m/%d/%Y")
            return d.date().isoformat()
        except:
            pass

    return ""

def guess_county(text: str) -> str:
    counties = [
        "Davidson","Williamson","Rutherford","Sumner","Wilson","Maury",
        "Montgomery","Robertson","Dickson","Bedford","Putnam",
        "Shelby","Hamilton","Knox","Bledsoe","Rhea"
    ]
    for c in counties:
        if c.lower() in text.lower():
            return c
    return ""

def extract_contact(text: str) -> str:
    email_match = re.search(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b", text)
    if email_match:
        return email_match.group(0)

    phone_match = re.search(r"\b\d{3}[-.]?\d{3}[-.]?\d{4}\b", text)
    if phone_match:
        return phone_match.group(0)

    return ""

def extract_address(text: str) -> str:
    addr_pattern = r"\d{1,5}\s+[A-Za-z0-9.\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Court|Ct|Boulevard|Blvd)"
    match = re.search(addr_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0)
    return ""

def extract_trustee_or_attorney(text: str) -> str:
    trustee_pattern = r"(Substitute Trustee|Trustee|Attorney)[^,.]{0,120}"
    match = re.search(trustee_pattern, text, re.IGNORECASE)
    if match:
        return match.group(0)
    return ""

def make_lead_key(distress_type: str, county: str, sale_date_iso: str, address: str, trustee: str, url: str) -> str:
    base = "|".join([
        (distress_type or "").strip().lower(),
        (county or "").strip().lower(),
        (sale_date_iso or "").strip().lower(),
        (address or "").strip().lower(),
        (trustee or "").strip().lower(),
        (url or "").strip().lower(),
    ])
    return hashlib.sha256(base.encode("utf-8")).hexdigest()[:16]
import hashlib
import re


def _norm_key_part(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def make_lead_key(*parts: str) -> str:
    """
    Returns a stable fixed-length Lead Key (SHA1 hex = 40 chars).
    This prevents truncation mismatch and makes dedupe deterministic.
    """
    base = "|".join(_norm_key_part(p) for p in parts if p)
    return hashlib.sha1(base.encode("utf-8")).hexdigest()
