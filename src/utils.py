import re
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

def fetch(url: str) -> str:
    r = requests.get(url, timeout=30, headers={"User-Agent": "FalcoBot/1.0"})
    r.raise_for_status()
    return r.text

def soup_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return re.sub(r"\s+", " ", soup.get_text(" ", strip=True))

def contains_any(text: str, keywords: list[str]) -> bool:
    t = text.lower()
    return any(k.lower() in t for k in keywords)

def find_date_iso(text: str) -> str:
    patterns = [
        r"\b(\d{1,2}/\d{1,2}/\d{2,4})\b",
        r"\b([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})\b",
    ]
    for p in patterns:
        m = re.search(p, text)
        if m:
            try:
                dt = dateparser.parse(m.group(1), fuzzy=True)
                return dt.date().isoformat()
            except Exception:
                pass
    return ""

def guess_county(text: str) -> str:
    m = re.search(r"\b([A-Z][a-z]+)\s+County\b", text)
    return m.group(1) if m else ""
    def extract_address(text: str) -> str:
    """
    Best-effort street address extractor.
    Works for many trustee notices that contain: 'Property located at 123 ...'
    """
    if not text:
        return ""

    t = " ".join(text.split())

    # Common cue phrases
    cues = [
        r"(?:located at|property located at|street address is|commonly known as)\s+(.{10,120})",
        r"(?:situs address|property address)\s*[:\-]\s*(.{10,120})",
    ]

    for cue in cues:
        m = re.search(cue, t, flags=re.IGNORECASE)
        if m:
            chunk = m.group(1)
            # stop at obvious delimiters
            chunk = re.split(r"\s(?:being|more particularly|parcel|map|tax|deed|book|instrument)\b", chunk, maxsplit=1, flags=re.IGNORECASE)[0]
            chunk = chunk.strip(" ,.;")
            return chunk[:180]

    # Generic street pattern (not perfect, but helps)
    street_suffix = r"(Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Court|Ct|Boulevard|Blvd|Way|Pike|Highway|Hwy|Circle|Cir|Trail|Trl)"
    m = re.search(rf"\b(\d{{1,6}}\s+[A-Za-z0-9.\- ]{{2,40}}\s+{street_suffix}\b[^.,;]{{0,40}})", t)
    if m:
        return m.group(1).strip()[:180]

    return ""

def extract_trustee_or_attorney(text: str) -> str:
    """
    Best-effort extraction for trustee/attorney/firm from notices.
    """
    if not text:
        return ""

    t = " ".join(text.split())

    patterns = [
        r"(Substitute Trustee|Trustee)\s*[:\-]\s*([A-Za-z0-9 .,&\-]{3,80})",
        r"(Attorney|Law Firm)\s*[:\-]\s*([A-Za-z0-9 .,&\-]{3,80})",
        r"(?:is|are)\s+the\s+(?:Substitute\s+)?Trustee\s*[:\-]?\s*([A-Za-z0-9 .,&\-]{3,80})",
    ]

    for p in patterns:
        m = re.search(p, t, flags=re.IGNORECASE)
        if m:
            val = m.group(m.lastindex).strip(" ,.;")
            return val[:140]

    return ""


def extract_contact(text: str) -> str:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phones = re.findall(r"\(?\b\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", text)
    out = []
    if emails:
        out.append("Emails: " + ", ".join(sorted(set(emails))[:5]))
    if phones:
        out.append("Phones: " + ", ".join(sorted(set(phones))[:5]))
    return " | ".join(out)
