# src/bots/public_notices_bot.py
"""PublicNoticesBot

Targets:
- tnlegalpub.com (WordPress listing + individual notice pages)
- (light) foreclosurestn.com
- (light) tnpublicnotice.com

Behavior:
- Robust extraction: address, trustee/firm, sale date, county
- Clip raw_snippet (never store full notice body)
- Non-destructive Notion updates (handled centrally in notion_client.update_lead)
- Run-level debug artifacts
"""

from __future__ import annotations

import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..notion_client import build_properties, create_lead, update_lead, find_existing_by_lead_key
from ..scoring import days_to_sale, detect_risk_flags, score_v2, label
from ..settings import (
    clip_raw_snippet,
    county_base,
    get_allowed_counties_base,
    get_dts_window,
    is_allowed_county,
    normalize_county_full,
)
from ..utils import make_lead_key, canonicalize_url


SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}

MAX_LIST_PAGES = int(os.getenv("FALCO_PUBLIC_MAX_LIST_PAGES", "8"))
MAX_NOTICE_LINKS = int(os.getenv("FALCO_PUBLIC_MAX_NOTICE_LINKS", "200"))
MAX_SNIPPET_LEN = int(os.getenv("FALCO_PUBLIC_MAX_SNIPPET_LEN", os.getenv("FALCO_MAX_RAW_SNIPPET_CHARS", "1200")))

DEBUG = os.getenv("FALCO_PUBLIC_DEBUG", "0") == "1"
MAX_ITEMS_PER_RUN = int(os.getenv("FALCO_PUBLIC_MAX_ITEMS_PER_RUN", "0"))  # optional safety; 0=disabled


# ============================================================
# Small text helpers
# ============================================================

def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _clean_lines(s: str) -> List[str]:
    if not s:
        return []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in s.splitlines()]
    return [ln for ln in lines if ln and len(ln) > 2]


def _text_from_main_content(soup: BeautifulSoup) -> str:
    """Primary: the notice body."""
    selectors = [
        "article .entry-content",
        "main .entry-content",
        "div.entry-content",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            txt = node.get_text("\n")
            if txt and len(txt.strip()) > 50:
                return txt
    # fallback
    node = soup.select_one("article") or soup.select_one("main")
    return node.get_text("\n") if node else soup.get_text("\n")


def _text_from_article_all(soup: BeautifulSoup) -> str:
    """Secondary: include the whole article (captures signature/meta blocks missing in entry-content)."""
    node = soup.select_one("article")
    if node:
        return node.get_text("\n")
    node = soup.select_one("main")
    if node:
        return node.get_text("\n")
    return soup.get_text("\n")


# ============================================================
# Sale date extraction (candidate selection)
# ============================================================

_MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

_DATE_PATTERNS = [
    re.compile(rf"\b({_MONTHS})\s+\d{{1,2}}(?:st|nd|rd|th)?[,]?\s+\d{{4}}\b", re.IGNORECASE),
    re.compile(rf"\b({_MONTHS})\s+\d{{1,2}}(?:st|nd|rd|th)?\b", re.IGNORECASE),  # year missing
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
]

_DATE_CONTEXT_TOKENS = [
    "sale", "sold", "auction", "public auction", "foreclosure", "trustee",
    "will be sold", "to be sold", "substitute trustee", "front door",
    "p.m.", "a.m.", "courthouse", "chancery", "circuit"
]


def _parse_date_str(date_str: str, now_year: int) -> Optional[str]:
    s = _norm_ws(date_str)
    s = re.sub(r"(st|nd|rd|th)\b", "", s, flags=re.IGNORECASE).strip()

    fmts = [
        "%B %d, %Y",
        "%b %d, %Y",
        "%B %d %Y",
        "%b %d %Y",
        "%m/%d/%Y",
        "%m/%d/%y",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            d = datetime.strptime(s, fmt).date()
            return d.isoformat()
        except Exception:
            continue

    m = re.match(rf"^({_MONTHS})\s+(\d{{1,2}})$", s, flags=re.IGNORECASE)
    if m:
        month = m.group(1)
        day = m.group(2)
        for year in (now_year, now_year + 1):
            for fmt in ("%B %d %Y", "%b %d %Y"):
                try:
                    d = datetime.strptime(f"{month} {day} {year}", fmt).date()
                    return d.isoformat()
                except Exception:
                    continue
    return None


def _extract_date_candidates(text: str) -> List[Tuple[str, int]]:
    if not text:
        return []
    now_year = datetime.utcnow().year
    candidates: List[Tuple[str, int]] = []

    for pat in _DATE_PATTERNS:
        for m in pat.finditer(text):
            iso = _parse_date_str(m.group(0), now_year)
            if not iso:
                continue

            start = max(0, m.start() - 140)
            end = min(len(text), m.end() + 140)
            ctx = text[start:end].lower()

            score = 1
            for tok in _DATE_CONTEXT_TOKENS:
                if tok in ctx:
                    score += 1
            candidates.append((iso, score))

    best: Dict[str, int] = {}
    for iso, sc in candidates:
        best[iso] = max(best.get(iso, 0), sc)

    return sorted(best.items(), key=lambda x: (-x[1], x[0]))


def _pick_sale_date_in_window(text: str, dts_min: int, dts_max: int) -> Tuple[str, Optional[int], List[Tuple[str, int]]]:
    cands = _extract_date_candidates(text)

    best_iso = ""
    best_dts: Optional[int] = None
    best_score = -1

    for iso, sc in cands:
        dts = days_to_sale(iso)
        if dts is None:
            continue
        if not (dts_min <= dts <= dts_max):
            continue

        if sc > best_score:
            best_score = sc
            best_iso = iso
            best_dts = dts
        elif sc == best_score and best_dts is not None and dts < best_dts:
            best_iso = iso
            best_dts = dts

    if best_iso:
        return best_iso, best_dts, cands
    return "", None, cands


# ============================================================
# County extraction
# ============================================================

_COUNTY_RX = re.compile(r"\b([A-Za-z]+)\s+County\b", re.IGNORECASE)
_IN_COUNTY_RX = re.compile(r"\bCounty\s+of\s+([A-Za-z]+)\b|\bin\s+the\s+County\s+of\s+([A-Za-z]+)\b", re.IGNORECASE)


def _extract_county(text: str) -> str:
    if not text:
        return ""
    m = _COUNTY_RX.search(text)
    if m:
        return normalize_county_full(m.group(1)) or ""
    m2 = _IN_COUNTY_RX.search(text)
    if m2:
        g = m2.group(1) or m2.group(2)
        return normalize_county_full(g) or ""

    allowed = get_allowed_counties_base()
    low = text.lower()
    for base in sorted(allowed, key=len, reverse=True):
        if base and base.lower() in low:
            return f"{base} County"
    return ""


# ============================================================
# Address extraction
# ============================================================

_ADDR_LABEL_RX = re.compile(
    r"\b(Property\s+Address|Street\s+Address|Address\s+of\s+Property|Located\s+at|Property\s+is\s+located\s+at)\b\s*[:\-]?\s*(.+)",
    re.IGNORECASE,
)
_ADDR_TN_ZIP_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b[^\n]{0,60}\bTN\b\.?\s*\d{5}(?:-\d{4})?\b",
    re.IGNORECASE,
)
_ADDR_GENERIC_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b[^\n]{0,40}",
    re.IGNORECASE,
)

_ADDR_LEADING_JUNK = re.compile(
    r"^(the\s+)?(address\s+)?(of\s+)?(the\s+)?(described\s+)?(property\s+)?(is\s+)?(located\s+)?(at\s+)?",
    re.IGNORECASE,
)


def _cleanup_address(cand: str) -> str:
    c = _norm_ws(cand).strip(" ,;\t")
    c = _ADDR_LEADING_JUNK.sub("", c).strip(" ,;\t")
    c = re.sub(r"\bTN\.\s*(\d{5}(?:-\d{4})?)\b", r"TN \1", c, flags=re.IGNORECASE)
    c = re.split(
        r"\b(Parcel|Tax\s+Map|Book\s+and\s+Page|Deed\s+of\s+Trust|Instrument\s+No\.|Being\s+the\s+same\s+property|Assignment)\b",
        c,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip(" ,;\t")
    c = c.rstrip(".").strip()
    return c


def _extract_address(lines: List[str], full_text: str) -> str:
    for ln in lines:
        m = _ADDR_LABEL_RX.search(ln)
        if m:
            cand = _cleanup_address(m.group(2))
            if 8 <= len(cand) <= 180:
                return cand

    m = _ADDR_TN_ZIP_RX.search(full_text)
    if m:
        return _cleanup_address(m.group(0))

    m2 = _ADDR_GENERIC_RX.search(full_text)
    if m2:
        return _cleanup_address(m2.group(0))

    return ""


# ============================================================
# Trustee / firm extraction
# ============================================================

_KNOWN_FIRM_RX = re.compile(
    r"\b("
    r"Wilson\s*&\s*Associates|"
    r"Shapiro\s+Ingrassia|"
    r"McCalla\s+Raymer|"
    r"Barrett\s+Frappier|"
    r"Sirote\s*&\s*Permutt|"
    r"Aldridge\s+Pite|"
    r"Rubin\s+Lublin|"
    r"Padgett\s+Law\s+Group|"
    r"Brock\s*&\s*Scott"
    r")\b",
    re.IGNORECASE,
)

# Only accept explicit labels with delimiters (prevents statute bait)
_TRUSTEE_LABELED_RXES = [
    re.compile(r"\bSubstitute\s+Trustee\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    re.compile(r"\bTrustee\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    re.compile(r"\bAttorney\s+for\s+the\s+Trustee\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    re.compile(r"\bSubstitute\s+Trustee\s+is\s+([A-Za-z0-9&.,'\-\s]{3,180})", re.IGNORECASE),
]

# Added: common inline forms
_TRUSTEE_INLINE_RXES = [
    re.compile(r"\b([A-Z][A-Za-z0-9&.,'\-\s]{3,120}),\s+as\s+Substitute\s+Trustee\b", re.IGNORECASE),
    re.compile(r"\bSubstitute\s+Trustee\s*,\s*([A-Z][A-Za-z0-9&.,'\-\s]{3,120})\b", re.IGNORECASE),
]

_FIRMISH_LINE_RX = re.compile(
    r"\b(PLLC|P\.?L\.?L\.?C\.?|LLC|P\.?C\.?|LLP|L\.?L\.?P\.?|LAW\s+GROUP|ATTORNEYS|ASSOCIATES|COUNSEL)\b",
    re.IGNORECASE,
)

_BAD_TRUSTEE_RX = re.compile(
    r"\b("
    r"party\s+interested|record\s+book|warranty\s+deed|deed\s+of\s+trust|"
    r"tennessee\s+code\s+annotated|§\s*\d+|t\.c\.a\.|"
    r"instrument\s+no\.|parcel|tax\s+map|book\s+and\s+page"
    r")\b",
    re.IGNORECASE,
)


def _sanitize_trustee(cand: str) -> str:
    c = _norm_ws(cand).strip(" ,;\t")
    if not c:
        return ""
    if _BAD_TRUSTEE_RX.search(c):
        return ""
    c = re.split(r"\b(Phone|Tel|Facsimile|Fax|Email|Address|P\.?\s*O\.?\s*Box)\b", c, maxsplit=1, flags=re.IGNORECASE)[0]
    c = c.strip(" ,;\t")
    if len(c) > 160:
        c = c[:160].rstrip()
    return c


def _extract_trustee(lines: List[str], full_text: str) -> str:
    # Tier A: labeled patterns line-by-line
    for ln in lines:
        for rx in _TRUSTEE_LABELED_RXES:
            m = rx.search(ln)
            if m:
                cand = _sanitize_trustee(m.group(1))
                if 3 <= len(cand) <= 160:
                    return cand

    # Tier B: labeled patterns in full text
    for rx in _TRUSTEE_LABELED_RXES:
        m = rx.search(full_text)
        if m:
            cand = _sanitize_trustee(m.group(1))
            if 3 <= len(cand) <= 160:
                return cand

    # Tier C: inline patterns in full text
    for rx in _TRUSTEE_INLINE_RXES:
        m = rx.search(full_text)
        if m:
            cand = _sanitize_trustee(m.group(1))
            if 3 <= len(cand) <= 160:
                return cand

    # Tier D: known firm anywhere
    mfirm = _KNOWN_FIRM_RX.search(full_text)
    if mfirm:
        cand = _sanitize_trustee(mfirm.group(0))
        if 3 <= len(cand) <= 160:
            return cand

    # Tier E: tail heuristic (last ~45 lines)
    tail = lines[-45:] if len(lines) > 45 else lines
    for ln in reversed(tail):
        if _BAD_TRUSTEE_RX.search(ln):
            continue
        low = ln.lower()
        if _FIRMISH_LINE_RX.search(ln) and ("trustee" in low or "sale" in low or "auction" in low):
            cand = _sanitize_trustee(ln)
            if 3 <= len(cand) <= 160:
                return cand

    for ln in reversed(tail):
        if _BAD_TRUSTEE_RX.search(ln):
            continue
        if "attempt to collect a debt" in ln.lower():
            continue
        if _FIRMISH_LINE_RX.search(ln):
            cand = _sanitize_trustee(ln)
            if 3 <= len(cand) <= 160:
                return cand

    return ""


# ============================================================
# Snippet building
# ============================================================

def _build_raw_snippet(
    *,
    sale_date_iso: str,
    county_full: str,
    trustee: str,
    address: str,
    lines: List[str],
    max_chars: int,
) -> str:
    header_lines: List[str] = []
    if sale_date_iso:
        header_lines.append(f"Sale Date: {sale_date_iso}")
    if county_full:
        header_lines.append(f"County: {county_full}")
    if trustee:
        header_lines.append(f"Trustee/Firm: {trustee}")
    if address:
        header_lines.append(f"Address: {address}")

    signal_lines: List[str] = []
    keywords = (
        "sale", "sold", "auction", "public auction", "property address",
        "located at", "substitute trustee", "trustee", "front door", "courthouse"
    )
    for ln in lines:
        low = ln.lower()
        if any(k in low for k in keywords):
            signal_lines.append(ln)
        if len(signal_lines) >= 6:
            break

    body = "\n".join(signal_lines[:6] if signal_lines else lines[:8])
    snippet = "\n".join(header_lines + (["---"] if header_lines else []) + ([body] if body else []))
    return clip_raw_snippet(snippet, max_chars=max_chars)


# ============================================================
# Listing discovery
# ============================================================

def _list_pages_for_seed(seed: str) -> List[str]:
    if "tnlegalpub.com/notice_type/" in seed:
        pages = [seed]
        for i in range(2, MAX_LIST_PAGES + 1):
            pages.append(seed.rstrip("/") + f"/page/{i}/")
        return pages
    return [seed]


def _extract_notice_links(list_url: str, soup: BeautifulSoup) -> List[str]:
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href") or ""
        if "/legal_notice/" in href:
            links.append(urljoin(list_url, href))
    return links


# ============================================================
# Main run
# ============================================================

def run():
    dts_min, dts_max = get_dts_window("PUBLIC_NOTICES")
    allowed = sorted(get_allowed_counties_base())
    print(f"[PublicNoticesBot] SEEDS={SEEDS} allowed_counties={allowed} dts_window=[{dts_min},{dts_max}]")

    session = requests.Session()

    list_pages_fetched = 0
    notice_links: List[str] = []
    seen_links = set()

    for seed in SEEDS:
        for list_url in _list_pages_for_seed(seed):
            try:
                r = session.get(list_url, headers=HEADERS, timeout=25)
            except Exception:
                continue
            if r.status_code != 200:
                continue

            list_pages_fetched += 1
            soup = BeautifulSoup(r.text, "html.parser")
            for link in _extract_notice_links(list_url, soup):
                can = canonicalize_url(link)
                if can and can not in seen_links:
                    seen_links.add(can)
                    notice_links.append(link)

            if len(notice_links) >= MAX_NOTICE_LINKS:
                break
        if len(notice_links) >= MAX_NOTICE_LINKS:
            break

    notice_pages_fetched_ok = 0
    parsed_ok = 0
    filtered_in = 0
    created = 0
    updated = 0

    skipped_no_sale = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_county_missing = 0
    skipped_dup_in_run = 0
    skipped_http = 0

    sample_kept: List[str] = []
    sample_county_missing: List[str] = []
    sample_skipped_reason: Dict[str, List[str]] = {}

    def _sample(reason: str, msg: str):
        arr = sample_skipped_reason.setdefault(reason, [])
        if len(arr) < 5:
            arr.append(msg)

    seen_in_run = set()

    for idx, url in enumerate(notice_links):
        if MAX_ITEMS_PER_RUN and idx >= MAX_ITEMS_PER_RUN:
            break

        try:
            r = session.get(url, headers=HEADERS, timeout=25)
        except Exception:
            skipped_http += 1
            _sample("http_error", f"url={url}")
            continue
        if r.status_code != 200:
            skipped_http += 1
            _sample("http_status", f"status={r.status_code} url={url}")
            continue

        notice_pages_fetched_ok += 1

        soup = BeautifulSoup(r.text, "html.parser")

        # IMPORTANT: use entry-content for snippet/body, but also include full article for trustee extraction.
        body_text = _text_from_main_content(soup)
        article_all_text = _text_from_article_all(soup)

        body_lines = _clean_lines(body_text)
        body_full = "\n".join(body_lines) if body_lines else body_text

        # Combined text improves trustee/firm extraction without polluting the snippet
        combined_text = "\n".join([body_full, article_all_text])
        combined_text_norm = _norm_ws(combined_text)

        sale_date_iso, dts, candidates = _pick_sale_date_in_window(combined_text_norm, dts_min, dts_max)
        if not sale_date_iso:
            if candidates:
                skipped_outside_window += 1
                _sample("outside_window", f"url={url} candidates={candidates[:4]}")
            else:
                skipped_no_sale += 1
                _sample("no_sale", f"url={url}")
            continue

        if dts is None:
            skipped_no_sale += 1
            _sample("no_sale", f"url={url}")
            continue
        if dts < 0:
            skipped_expired += 1
            _sample("expired", f"url={url} sale={sale_date_iso} dts={dts}")
            continue

        county_full = _extract_county(combined_text)
        if not county_full:
            skipped_county_missing += 1
            if len(sample_county_missing) < 5:
                sample_county_missing.append(f"url={url} sale={sale_date_iso}")
            _sample("county_missing", f"url={url}")
            continue

        county_full = normalize_county_full(county_full) or county_full
        if not is_allowed_county(county_full):
            skipped_out_of_geo += 1
            _sample("out_of_geo", f"url={url} county={county_full}")
            continue

        address = _extract_address(body_lines, combined_text)
        trustee = _extract_trustee(_clean_lines(article_all_text), combined_text)

        lead_key = make_lead_key(
            "PublicNotices",
            "Foreclosure",
            county_full,
            sale_date_iso,
            url,
        )

        if lead_key in seen_in_run:
            skipped_dup_in_run += 1
            continue
        seen_in_run.add(lead_key)

        raw_snippet = _build_raw_snippet(
            sale_date_iso=sale_date_iso,
            county_full=county_full,
            trustee=trustee,
            address=address,
            lines=body_lines,  # snippet stays based on the notice body
            max_chars=min(MAX_SNIPPET_LEN, 1900),
        )

        flags = detect_risk_flags(combined_text_norm)
        county_b = county_base(county_full) or ""
        has_contact = bool(trustee.strip())
        score = score_v2("Foreclosure", county_b, dts, has_contact)
        status = label("Foreclosure", county_b, dts, flags, score, has_contact)

        payload = {
            "title": address or "Foreclosure Notice",
            "source": "PublicNotices",
            "distress_type": "Foreclosure",
            "county": county_full,
            "address": address,
            "sale_date_iso": sale_date_iso,
            "trustee_attorney": trustee,
            "contact_info": trustee,
            "status": status,
            "score": score,
            "raw_snippet": raw_snippet,
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

        if len(sample_kept) < 5:
            sample_kept.append(
                f"county={county_full} sale={sale_date_iso} dts={dts} "
                f"addr={address or '[missing]'} trustee={trustee or '[missing]'}"
            )

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} "
        f"notice_links_found={len(notice_links)} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} "
        f"parsed_ok={parsed_ok} filtered_in={filtered_in} "
        f"created={created} updated={updated} "
        f"skipped_no_sale={skipped_no_sale} skipped_expired={skipped_expired} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_outside_window={skipped_outside_window} "
        f"skipped_county_missing={skipped_county_missing} skipped_dup_in_run={skipped_dup_in_run} "
        f"skipped_http={skipped_http} "
        f"sample_kept={sample_kept} "
        f"sample_county_missing={sample_county_missing} "
        f"sample_skipped_reason={sample_skipped_reason}"
    )
    print("[PublicNoticesBot] Done.")
