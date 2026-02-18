# src/bots/public_notices_bot.py

from urllib.parse import urljoin
from bs4 import BeautifulSoup

from ..config import SEED_URLS_PUBLIC_NOTICES, PUBLIC_NOTICE_MAX_LIST_PAGES
from ..utils import (
    fetch, find_date_iso, guess_county,
    extract_contact, extract_address, extract_trustee_or_attorney,
    make_lead_key
)
from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, triage, score_v2, label


def _clean(txt: str) -> str:
    return " ".join((txt or "").split())


def _extract_notice_links(list_html: str, base_url: str) -> list[str]:
    """
    tnlegalpub listing pages link individual notices at /legal_notice/<slug>/
    """
    soup = BeautifulSoup(list_html, "html.parser")
    links: list[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if "/legal_notice/" in href:
            links.append(urljoin(base_url, href))

    # De-dupe while preserving order
    seen = set()
    out: list[str] = []
    for u in links:
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
    return out


def _find_next_page(list_html: str, base_url: str) -> str | None:
    """
    Try rel=next, otherwise any link containing 'next'
    """
    soup = BeautifulSoup(list_html, "html.parser")

    rel_next = soup.select_one('a[rel="next"][href]')
    if rel_next and rel_next.get("href"):
        return urljoin(base_url, rel_next["href"])

    for a in soup.select("a[href]"):
        txt = (a.get_text(" ", strip=True) or "").lower()
        if "next" in txt:
            return urljoin(base_url, a["href"])

    return None


def _extract_notice_text(notice_html: str) -> str:
    soup = BeautifulSoup(notice_html, "html.parser")
    return _clean(soup.get_text(" ", strip=True))


def run():
    print(f"[PublicNoticesBot] SEEDS={SEED_URLS_PUBLIC_NOTIC_
