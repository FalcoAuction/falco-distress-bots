import os
import re
from datetime import datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..notion_client import (
    build_properties,
    create_lead,
    update_lead,
    find_existing_by_lead_key,
    NOTION_WRITE_ENABLED,
)
from ..gating.convertibility import apply_convertibility_gate
from ..scoring import days_to_sale
from ..settings import (
    get_dts_window,
    is_allowed_county,
    normalize_county_full,
    clip_raw_snippet,
    get_allowed_counties_base,
)
from ..utils import make_lead_key, canonicalize_url


# ============================================================
# CONFIG (per-bot env overrides)
# ============================================================

SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
    "https://www.foreclosurestn.com/",
    "https://www.tnpublicnotice.com/Search.aspx",
]

HEADERS = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}

# Defaults mirror global DTS window unless overridden for this bot
_DTS_MIN_DEFAULT, _DTS_MAX_DEFAULT = get_dts_window("PUBLIC_NOTICES")
_DTS_MIN = int(os.getenv("FALCO_PUBLIC_DTS_MIN", str(_DTS_MIN_DEFAULT)))
_DTS_MAX = int(os.getenv("FALCO_PUBLIC_DTS_MAX", str(_DTS_MAX_DEFAULT)))

MAX_LIST_PAGES = int(os.getenv("FALCO_PUBLIC_MAX_LIST_PAGES", "8"))
MAX_NOTICE_LINKS = int(os.getenv("FALCO_PUBLIC_MAX_NOTICE_LINKS", "200"))
MAX_SNIPPET_LEN = int(os.getenv("FALCO_PUBLIC_MAX_SNIPPET_LEN", os.getenv("FALCO_MAX_RAW_SNIPPET_CHARS", "1200")))

DEBUG = os.getenv("FALCO_PUBLIC_DEBUG", "0") == "1"


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _clean_lines(text: str) -> list[str]:
    if not text:
        return []
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
    return [ln for ln in lines if ln and len(ln) > 2]


# ============================================================
# DATE PARSING
# ============================================================

_MONTHS = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

_DATE_PATTERNS = [
    re.compile(rf"\b({_MONTHS})\s+\d{{1,2}}(?:st|nd|rd|th)?[,]?\s+\d{{4}}\b", re.IGNORECASE),
    re.compile(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b"),
    re.compile(r"\b\d{4}-\d{2}-\d{2}\b"),
]

_CONTEXT_TOKENS = ["sale", "sold", "auction", "will be sold", "to be sold", "trustee", "substitute trustee", "courthouse", "front door"]


def _parse_date_flex(s: str) -> str | None:
    if not s:
        return None
    s = _norm_ws(s).rstrip(".,;")
    s = re.sub(r"(st|nd|rd|th)\b", "", s, flags=re.IGNORECASE).strip()
    if re.search(r"[A-Za-z]", s):
        # normalize ALL CAPS months
        s = s.title()

    fmts = [
        "%m/%d/%Y",
        "%m/%d/%y",
        "%m-%d-%Y",
        "%m-%d-%y",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _date_candidates(text: str) -> list[tuple[str, int]]:
    t = text or ""
    out: list[tuple[str, int]] = []
    for pat in _DATE_PATTERNS:
        for m in pat.finditer(t):
            iso = _parse_date_flex(m.group(0))
            if not iso:
                continue
            start = max(0, m.start() - 120)
            end = min(len(t), m.end() + 120)
            ctx = t[start:end].lower()
            score = 1 + sum(1 for tok in _CONTEXT_TOKENS if tok in ctx)
            out.append((iso, score))
    # dedupe keep best score
    best = {}
    for iso, sc in out:
        best[iso] = max(best.get(iso, 0), sc)
    return sorted(best.items(), key=lambda x: (-x[1], x[0]))


def _pick_best_sale_date_iso(text: str) -> tuple[str | None, int | None, list[tuple[str, int]]]:
    cands = _date_candidates(text)
    best_iso = None
    best_dts = None
    best_score = -1
    for iso, sc in cands:
        dts = days_to_sale(iso)
        if dts is None:
            continue
        if dts < 0:
            continue
        if not (_DTS_MIN <= dts <= _DTS_MAX):
            continue
        if sc > best_score:
            best_score = sc
            best_iso = iso
            best_dts = dts
        elif sc == best_score and best_dts is not None and dts < best_dts:
            best_iso = iso
            best_dts = dts
    return best_iso, best_dts, cands


# ============================================================
# EXTRACTION: county / address / trustee
# ============================================================

_COUNTY_RX = re.compile(r"\b([A-Za-z]+)\s+County\b", re.IGNORECASE)

_ADDR_TN_ZIP_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\b[^\n]{0,60}\bTN\b\.?\s*\d{5}(?:-\d{4})?\b",
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

_FIRMISH_RX = re.compile(r"\b(PLLC|P\.?L\.?L\.?C\.?|LLC|P\.?C\.?|LLP|LAW\s+GROUP|ATTORNEYS|ASSOCIATES|COUNSEL)\b", re.IGNORECASE)


def _extract_county(text: str) -> str | None:
    m = _COUNTY_RX.search(text or "")
    if m:
        return normalize_county_full(m.group(1))
    # fallback: detect allowed base names by substring
    low = (text or "").lower()
    for base in sorted(get_allowed_counties_base(), key=len, reverse=True):
        if base and base.lower() in low:
            return f"{base} County"
    return None


def _extract_address(text: str) -> str | None:
    if not text:
        return None
    m = _ADDR_TN_ZIP_RX.search(text)
    if m:
        s = _norm_ws(m.group(0)).strip(" ,.;")
        # trim leading junk like "of the described property is"
        s = re.sub(r"^(the\s+)?(address\s+)?(of\s+)?(the\s+)?(described\s+)?(property\s+)?(is\s+)?", "", s, flags=re.IGNORECASE).strip()
        return s
    return None


def _sanitize_trustee(s: str) -> str:
    s = _norm_ws(s).strip(" ,.;:-")
    if not s:
        return ""
    if _BAD_TRUSTEE_RX.search(s):
        return ""
    # chop contact tails
    s = re.split(r"\b(Phone|Tel|Facsimile|Fax|Email|Address|P\.?\s*O\.?\s*Box)\b", s, maxsplit=1, flags=re.IGNORECASE)[0].strip(" ,.;:-")
    if len(s) > 160:
        s = s[:160].rstrip()
    return s


def _looks_like_trustee(s: str) -> bool:
    if not s or len(s) < 3:
        return False
    if _BAD_TRUSTEE_RX.search(s):
        return False
    if _FIRMISH_RX.search(s):
        return True
    # person-ish: at least two tokens
    if len(s.split()) >= 2 and not re.fullmatch(r"[0-9\W_]+", s):
        return True
    return False


_TRUSTEE_PATTERNS = [
    # delimiter forms
    re.compile(r"\bSubstitute\s+Trustees?\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    re.compile(r"\bTrustees?\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    re.compile(r"\bAttorney\s+for\s+the\s+Trustee\b\s*[:=\-]\s*([^\n]{3,180})", re.IGNORECASE),
    # "is" form
    re.compile(r"\bSubstitute\s+Trustees?\s+is\s+([^\n]{3,180})", re.IGNORECASE),
    # inline "X, as Substitute Trustee"
    re.compile(r"\b([A-Z][A-Za-z0-9&.,'\-\s]{3,140}),\s+as\s+Substitute\s+Trustees?\b", re.IGNORECASE),
    # no-delimiter form (guarded)
    re.compile(r"\bSubstitute\s+Trustees?\s+([A-Z][A-Za-z0-9&.,'\-\s]{3,160})\b", re.IGNORECASE),
]


def _extract_trustee(text: str) -> str | None:
    t = text or ""
    # try patterns over full text
    for rx in _TRUSTEE_PATTERNS:
        m = rx.search(t)
        if m:
            cand = _sanitize_trustee(m.group(1))
            if _looks_like_trustee(cand):
                return cand

    # Next-line heuristic
    lines = _clean_lines(t)
    for i, ln in enumerate(lines[:-1]):
        low = ln.lower().strip(" .:-")
        if low in ("substitute trustee", "substitute trustees", "trustee", "trustees"):
            cand = _sanitize_trustee(lines[i+1])
            if _looks_like_trustee(cand):
                return cand

    # If word appears, take a window after it
    m2 = re.search(r"\bsubstitute\s+trustees?\b", t, flags=re.IGNORECASE)
    if m2:
        window = t[m2.end(): m2.end()+220]
        window = re.split(r"[\n\.]", window)[0]
        cand = _sanitize_trustee(window)
        if _looks_like_trustee(cand):
            return cand

    return None


def _build_snippet(sale_date_iso: str, county: str, trustee: str | None, address: str | None, body_text: str) -> str:
    lines = _clean_lines(body_text)
    header = []
    if sale_date_iso:
        header.append(f"Sale Date: {sale_date_iso}")
    if county:
        header.append(f"County: {county}")
    if trustee:
        header.append(f"Trustee/Firm: {trustee}")
    if address:
        header.append(f"Address: {address}")
    # pick key lines
    key_lines = []
    for ln in lines:
        low = ln.lower()
        if any(k in low for k in ("sale", "auction", "will be sold", "substitute trustee", "property address", "located at", "courthouse", "front door")):
            key_lines.append(ln)
        if len(key_lines) >= 6:
            break
    body = "\n".join(key_lines if key_lines else lines[:8])
    snippet = "\n".join(header + (["---"] if header else []) + ([body] if body else []))
    return clip_raw_snippet(snippet, max_chars=int(MAX_SNIPPET_LEN))


def _is_tnlegalpub(url: str) -> bool:
    return "tnlegalpub.com" in (url or "")


def _list_pages_for_seed(seed: str) -> list[str]:
    if _is_tnlegalpub(seed):
        pages = []
        base = seed.rstrip("/") + "/"
        pages.append(base)
        for i in range(2, MAX_LIST_PAGES + 1):
            pages.append(urljoin(base, f"page/{i}/"))
        return pages
    return [seed]


def run():
    allowed = sorted(get_allowed_counties_base())
    print(f"[PublicNoticesBot] SEEDS={SEEDS} allowed_counties={allowed} dts_window=[{_DTS_MIN},{_DTS_MAX}]")

    session = requests.Session()

    list_pages_fetched = 0
    notice_links: list[str] = []
    seen_links: set[str] = set()

    # --- scrape listing pages
    for seed in SEEDS:
        for list_url in _list_pages_for_seed(seed):
            try:
                r = session.get(list_url, headers=HEADERS, timeout=20)
            except Exception:
                continue
            if r.status_code != 200:
                continue

            list_pages_fetched += 1
            soup = BeautifulSoup(r.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/legal_notice/" in href:
                    full = canonicalize_url(urljoin(list_url, href))
                    if full and full not in seen_links:
                        seen_links.add(full)
                        notice_links.append(full)

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
    would_create = 0
    would_update = 0

    skipped_no_sale = 0
    skipped_expired = 0
    skipped_out_of_geo = 0
    skipped_outside_window = 0
    skipped_county_missing = 0
    skipped_dup_in_run = 0
    skipped_http = 0

    sample_kept: list[str] = []
    sample_county_missing: list[str] = []
    sample_skipped_reason: dict[str, list[str]] = {"outside_window": [], "out_of_geo": []}

    seen_in_run: set[str] = set()

    # --- scrape notice pages
    for url in notice_links:
        try:
            r = session.get(url, headers=HEADERS, timeout=20)
        except Exception:
            skipped_http += 1
            continue
        if r.status_code != 200:
            skipped_http += 1
            continue
        notice_pages_fetched_ok += 1

        soup = BeautifulSoup(r.text, "html.parser")

        # WordPress: prefer entry-content as "body" for snippet, but parse from full article text for trustee robustness
        entry = soup.select_one("article .entry-content") or soup.select_one(".entry-content")
        body_text = entry.get_text("\n") if entry else soup.get_text("\n")

        article = soup.select_one("article") or soup.select_one("main") or soup
        full_text = article.get_text("\n")

        combined = _norm_ws(body_text + "\n" + full_text)

        sale_date_iso, dts, candidates = _pick_best_sale_date_iso(combined)
        if not sale_date_iso:
            # We DID attempt context + fallback and still found nothing in-window
            if candidates:
                skipped_outside_window += 1
                if len(sample_skipped_reason["outside_window"]) < 5:
                    sample_skipped_reason["outside_window"].append(f"url={url} candidates={candidates[:4]}")
            else:
                skipped_no_sale += 1
            continue

        if dts is None:
            skipped_no_sale += 1
            continue
        if dts < 0:
            skipped_expired += 1
            continue

        county = _extract_county(combined)
        if not county:
            skipped_county_missing += 1
            if len(sample_county_missing) < 5:
                sample_county_missing.append(f"url={url} sale={sale_date_iso}")
            continue

        county = normalize_county_full(county) or county
        if not is_allowed_county(county):
            skipped_out_of_geo += 1
            if len(sample_skipped_reason["out_of_geo"]) < 5:
                sample_skipped_reason["out_of_geo"].append(f"url={url} county={county}")
            continue

        address = _extract_address(combined)
        trustee = _extract_trustee(combined)

        # DEBUG: show trustee contexts when missing (kept items only)
        if DEBUG and not trustee:
            ctxs = []
            for m in re.finditer(r"\bsubstitute\s+trustees?\b|\btrustees?\b", combined, flags=re.IGNORECASE):
                start = max(0, m.start()-120)
                end = min(len(combined), m.end()+180)
                ctxs.append(combined[start:end])
                if len(ctxs) >= 2:
                    break
            if ctxs:
                print(f"[PublicNoticesBot][DEBUG] trustee_missing url={url} ctx={ctxs}")
            else:
                print(f"[PublicNoticesBot][DEBUG] trustee_missing url={url} ctx=[no trustee tokens found]")

        lead_key = make_lead_key("PUBLIC_NOTICES", url, county, sale_date_iso, address or "")

        if lead_key in seen_in_run:
            skipped_dup_in_run += 1
            continue
        seen_in_run.add(lead_key)

        snippet = _build_snippet(sale_date_iso, county, trustee, address, body_text)

        payload = {
            "title": address or "Foreclosure Notice",
            "source": "PublicNotices",
            "county": county,
            "distress_type": "Foreclosure",
            "address": address or "",
            "sale_date_iso": sale_date_iso,
            "trustee_attorney": trustee or "",
            "contact_info": trustee or "",
            "raw_snippet": snippet,
            "url": url,
            "lead_key": lead_key,
            "days_to_sale": dts,
        }
        payload = apply_convertibility_gate(payload)
        props = build_properties(payload)

        existing = find_existing_by_lead_key(lead_key)
        if existing:
            update_lead(existing, props)
            if NOTION_WRITE_ENABLED:
                updated += 1
            else:
                would_update += 1
        else:
            create_lead(props)
            if NOTION_WRITE_ENABLED:
                created += 1
            else:
                would_create += 1

        parsed_ok += 1
        filtered_in += 1

        if len(sample_kept) < 5:
            sample_kept.append(
                f"county={county} sale={sale_date_iso} dts={dts} addr={(address or '[missing]')} trustee={(trustee or '[missing]')}"
            )

    print(
        "[PublicNoticesBot] summary "
        f"list_pages_fetched={list_pages_fetched} "
        f"notice_links_found={len(notice_links)} "
        f"notice_pages_fetched_ok={notice_pages_fetched_ok} "
        f"parsed_ok={parsed_ok} filtered_in={filtered_in} "
        f"created={created} updated={updated} would_create={would_create} would_update={would_update} "
        f"skipped_no_sale={skipped_no_sale} skipped_expired={skipped_expired} "
        f"skipped_out_of_geo={skipped_out_of_geo} skipped_outside_window={skipped_outside_window} "
        f"skipped_county_missing={skipped_county_missing} skipped_dup_in_run={skipped_dup_in_run} "
        f"skipped_http={skipped_http} "
        f"sample_kept={sample_kept} sample_county_missing={sample_county_missing} "
        f"sample_skipped_reason={sample_skipped_reason}"
    )
    print("[PublicNoticesBot] Done.")
