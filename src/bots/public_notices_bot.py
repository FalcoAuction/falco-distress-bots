# src/bots/public_notices_bot.py
"""PublicNoticesBot

Targets:
- tnlegalpub.com (WordPress listing + individual notice pages)
- (light) foreclosurestn.com
- (light) tnpublicnotice.com

Required behaviors:
- Robust extraction: address, trustee/firm, sale date, county
- Clip raw_snippet (never store full notice body)
- Non-destructive Notion updates (handled centrally in notion_client.update_lead)
- Run-level debug artifacts: skip reason counts + sample_kept + sample_county_missing

Notes:
- This bot intentionally uses a slightly more permissive days-to-sale window by default.
  See src.settings.get_dts_window(source="PUBLIC_NOTICES") and env overrides:
    FALCO_PUBLIC_DTS_MIN / FALCO_PUBLIC_DTS_MAX
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
    """Preserve meaningful lines; remove empty / ultra-short lines."""
    if not s:
        return []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in s.splitlines()]
    out: List[str] = []
    for ln in lines:
        if not ln:
            continue
        if len(ln) <= 2:
            continue
        out.append(ln)
    return out


def _text_from_main_content(soup: BeautifulSoup) -> str:
    """Try to get the notice body without pulling the entire site chrome."""
    selectors = [
        "article .entry-content",
        "main .entry-content",
        "div.entry-content",
        "article",
        "main",
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            # Keep line breaks for later key-line extraction
            txt = node.get_text("\n")
            if txt and len(txt.strip()) > 50:
                return txt
    # Fallback
    return soup.get_text("\n")


# ============================================================
# Sale date extraction
# ============================================================

_MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

_DATE_PATTERNS = [
    # Month name formats (with comma year optional)
    re.compile(rf"\b({_MONTHS})\s+\d{{1,2}}(?:st|nd|rd|th)?[,]?\s+\d{{4}}\b", re.IGNORECASE),
    re.compile(rf"\b({_MONTHS})\s+\d{{1,2}}(?:st|nd|rd|th)?\b", re.IGNORECASE),  # year missing
    # Numeric
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
]

# extra context tokens that often surround the auction date
_DATE_CONTEXT_TOKENS = [
    "sale", "sold", "auction", "public auction", "foreclosure", "trustee", "will be sold", "to be sold",
    "on", "at", "p.m.", "a.m."
]


def _parse_date_str(date_str: str, now_year: int) -> Optional[str]:
    s = _norm_ws(date_str)
    s = re.sub(r"(st|nd|rd|th)\b", "", s, flags=re.IGNORECASE).strip()
    # Normalize month abbreviations for strptime where possible
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

    # If the date is missing a year (common on some snippets), assume current year, then next year if already past
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
    """Return list of (iso_date, score) based on context proximity."""
    if not text:
        return []
    t = text
    now_year = datetime.utcnow().year
    candidates: List[Tuple[str, int]] = []
    for pat in _DATE_PATTERNS:
        for m in pat.finditer(t):
            raw = m.group(0)
            iso = _parse_date_str(raw, now_year)
            if not iso:
                continue
            # score based on context around match
            start = max(0, m.start() - 120)
            end = min(len(t), m.end() + 120)
            ctx = t[start:end].lower()
            score = 1
            for tok in _DATE_CONTEXT_TOKENS:
                if tok in ctx:
                    score += 1
            candidates.append((iso, score))
    # de-dupe by iso keeping max score
    best: Dict[str, int] = {}
    for iso, sc in candidates:
        best[iso] = max(best.get(iso, 0), sc)
    return sorted(best.items(), key=lambda x: (-x[1], x[0]))


def _pick_sale_date_in_window(text: str, dts_min: int, dts_max: int) -> Tuple[str, Optional[int], List[Tuple[str, int]]]:
    """Pick best sale date candidate within window. Returns (sale_date_iso, dts, candidates)."""
    cands = _extract_date_candidates(text)
    for iso, _score in cands:
        dts = days_to_sale(iso)
        if dts is None:
            continue
        if dts_min <= dts <= dts_max:
            return iso, dts, cands
    # If none within window, return empty but still return candidates for debug
    return "", None, cands


# ============================================================
# County extraction
# ============================================================

_COUNTY_RX = re.compile(r"\b([A-Za-z]+)\s+County\b", re.IGNORECASE)
_IN_COUNTY_RX = re.compile(r"\bCounty\s+of\s+([A-Za-z]+)\b|\bin\s+the\s+County\s+of\s+([A-Za-z]+)\b", re.IGNORECASE)


def _extract_county(text: str) -> str:
    if not text:
        return ""
    # Prefer explicit "X County"
    m = _COUNTY_RX.search(text)
    if m:
        return normalize_county_full(m.group(1)) or ""
    m2 = _IN_COUNTY_RX.search(text)
    if m2:
        g = m2.group(1) or m2.group(2)
        return normalize_county_full(g) or ""

    # Fallback: if any allowed county base name appears anywhere, use it
    allowed = get_allowed_counties_base()
    low = text.lower()
    for base in sorted(allowed, key=len, reverse=True):
        if base and base.lower() in low:
            return f"{base} County"
    return ""


# ============================================================
# Address extraction
# ============================================================

_ADDR_LABEL_RX = re.compile(r"\b(Property\s+Address|Street\s+Address|Address\s+of\s+Property|Located\s+at)\b\s*[:\-]?\s*(.+)", re.IGNORECASE)
_ADDR_TN_ZIP_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b[^\n]{0,60}\bTN\b\s*\d{5}(?:-\d{4})?\b",
    re.IGNORECASE,
)
_ADDR_GENERIC_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\s+(Street|St\.?|Avenue|Ave\.?|Road|Rd\.?|Drive|Dr\.?|Lane|Ln\.?|Court|Ct\.?|Boulevard|Blvd\.?|Way|Place|Pl\.?|Circle|Cir\.?|Pike|Hwy|Highway)\b[^\n]{0,40}",
    re.IGNORECASE,
)


def _extract_address(lines: List[str], full_text: str) -> str:
    # 1) Label-based
    for ln in lines:
        m = _ADDR_LABEL_RX.search(ln)
        if m:
            cand = _norm_ws(m.group(2))
            # sometimes label line continues on next line
            if len(cand) < 10:
                continue
            # avoid grabbing a full paragraph; clip at common delimiters
            cand = re.split(r"\b(Parcel|Tax\s+Map|Book\s+and\s+Page|Deed\s+of\s+Trust|Instrument\s+No\.|Being\s+the\s+same\s+property)\b", cand, maxsplit=1)[0]
            cand = cand.strip(" ,;\t")
            if 6 <= len(cand) <= 180:
                return cand

    # 2) Strong TN+ZIP pattern
    m = _ADDR_TN_ZIP_RX.search(full_text)
    if m:
        return _norm_ws(m.group(0)).strip(" ,;")

    # 3) Generic street pattern (may miss city/state)
    m2 = _ADDR_GENERIC_RX.search(full_text)
    if m2:
        return _norm_ws(m2.group(0)).strip(" ,;")

    return ""


# ============================================================
# Trustee / firm extraction
# ============================================================

_TRUSTEE_RXES = [
    re.compile(r"\bSubstitute\s+Trustee\b\s*[:\-]?\s*([^\n]{3,160})", re.IGNORECASE),
    re.compile(r"\bTrustee\b\s*[:\-]?\s*([^\n]{3,160})", re.IGNORECASE),
    re.compile(r"\bAttorney\s+for\s+the\s+Trustee\b\s*[:\-]?\s*([^\n]{3,160})", re.IGNORECASE),
    re.compile(r"\b(Shapiro\s+Ingrassia|Wilson\s+&\s+Associates|Barrett\s+Frappier|McCalla\s+Raymer|Sirote\s+&\s+Permutt|Aldridge\s+Pite|Rubin\s+Lublin)\b[^\n]{0,120}", re.IGNORECASE),
]


def _extract_trustee(lines: List[str], full_text: str) -> str:
    # 1) line-based (avoids grabbing huge paragraphs)
    for ln in lines:
        for rx in _TRUSTEE_RXES:
            m = rx.search(ln)
            if m:
                if m.lastindex:
                    cand = _norm_ws(m.group(1))
                else:
                    cand = _norm_ws(m.group(0))
                cand = cand.strip(" ,;\t")
                if 3 <= len(cand) <= 160:
                    return cand

    # 2) full-text regex
    for rx in _TRUSTEE_RXES:
        m = rx.search(full_text)
        if m:
            cand = _norm_ws(m.group(1) if m.lastindex else m.group(0))
            cand = cand.strip(" ,;\t")
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

    # Choose 2-3 high-signal lines from the notice body
    signal_lines: List[str] = []
    keywords = (
        "sale", "sold", "auction", "public auction", "property address",
        "located at", "substitute trustee", "trustee"
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
        # WordPress pagination: /page/2/
        pages = [seed]
        for i in range(2, MAX_LIST_PAGES + 1):
            pages.append(seed.rstrip("/") + f"/page/{i}/")
        return pages

    # Light/no-op for other sources unless they expose direct links on the page
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

    # --- scrape listing pages
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
    skipped_county_missing = 0
    skipped_dup_in_run = 0
    skipped_http = 0

    # sample artifacts
    sample_kept: List[str] = []
    sample_county_missing: List[str] = []
    sample_skipped_reason: Dict[str, List[str]] = {}

    def _sample(reason: str, msg: str):
        arr = sample_skipped_reason.setdefault(reason, [])
        if len(arr) < 5:
            arr.append(msg)

    seen_in_run = set()

    # --- scrape notice pages
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
        body_text = _text_from_main_content(soup)
        lines = _clean_lines(body_text)
        full_text = "\n".join(lines) if lines else body_text
        full_text_norm = _norm_ws(full_text)

        # ---- sale date (windowed)
        sale_date_iso, dts, candidates = _pick_sale_date_in_window(full_text_norm, dts_min, dts_max)
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

        # ---- county (normalize + allowlist)
        county_full = _extract_county(full_text)
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

        # ---- address + trustee
        address = _extract_address(lines, full_text)
        trustee = _extract_trustee(lines, full_text)

        # ---- lead key (stable + consistent)
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

        # ---- snippet
        raw_snippet = _build_raw_snippet(
            sale_date_iso=sale_date_iso,
            county_full=county_full,
            trustee=trustee,
            address=address,
            lines=lines,
            max_chars=min(MAX_SNIPPET_LEN, 1900),  # Notion rich_text cap handled too
        )

        # ---- scoring + status
        flags = detect_risk_flags(full_text_norm)
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

        # non-destructive updates happen inside notion_client.update_lead via pruning empties
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
            sample_kept.append(f"county={county_full} sale={sale_date_iso} dts={dts} addr={address or '[missing]'} trustee={trustee or '[missing]'}")

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
