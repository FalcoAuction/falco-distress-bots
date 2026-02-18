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

def extract_contact(text: str) -> str:
    emails = re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phones = re.findall(r"\(?\b\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b", text)
    out = []
    if emails:
        out.append("Emails: " + ", ".join(sorted(set(emails))[:5]))
    if phones:
        out.append("Phones: " + ", ".join(sorted(set(phones))[:5]))
    return " | ".join(out)
