# src/bots/lis_pendens_bot.py
#
# Upstream Lis Pendens ingestion — no sale_date required.
# Stores leads with distress_type="LIS_PENDENS" for later enrichment/scoring.
# Enable with: FALCO_ENABLE_LIS_PENDENS=1
#
# Seeds: tnlegalpub.com LP category pages (same crawl pattern as public_notices_bot).
# Env controls:
#   FALCO_LP_MAX_LIST_PAGES   — max listing pages per seed (default 6)
#   FALCO_LP_MAX_NOTICE_LINKS — max notice detail URLs to fetch (default 150)
#   FALCO_LP_MAX_SNIPPET_LEN  — max raw snippet chars (default 1000)

import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..storage import sqlite_store as _store
from ..utils import make_lead_key, canonicalize_url
from ..settings import (
    get_allowed_counties_base,
    is_allowed_county,
    within_target_counties,
    normalize_county_full,
)

# ============================================================
# CONFIG
# ============================================================

_SEEDS = [
    "https://tnlegalpub.com/notice_type/foreclosure/",
]

_HEADERS = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}

_MAX_LIST_PAGES   = int(os.getenv("FALCO_LP_MAX_LIST_PAGES",   "6"))
_MAX_NOTICE_LINKS = int(os.getenv("FALCO_LP_MAX_NOTICE_LINKS", "150"))
_MAX_SNIPPET_LEN  = int(os.getenv("FALCO_LP_MAX_SNIPPET_LEN",  "1000"))

# Keywords that qualify a notice as upstream LP/SOT (any match accepted)
_LP_KEYWORDS = ("lis pendens", "substitution of trustee", "substitute trustee")

# ============================================================
# HELPERS
# ============================================================

_COUNTY_RX = re.compile(r"\b([A-Za-z]+)\s+County\b", re.IGNORECASE)

_ADDR_RX = re.compile(
    r"\b\d{1,6}\s+[A-Za-z0-9#.,'\-\s]{2,80}\b[^\n]{0,60}\bTN\b\.?\s*\d{5}(?:-\d{4})?\b",
    re.IGNORECASE,
)


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_lp(text: str) -> bool:
    low = (text or "").lower()
    return any(kw in low for kw in _LP_KEYWORDS)


def _extract_county(text: str) -> str | None:
    m = _COUNTY_RX.search(text or "")
    if m:
        return normalize_county_full(m.group(1))
    # fallback: scan for allowed county base names
    low = (text or "").lower()
    for base in sorted(get_allowed_counties_base(), key=len, reverse=True):
        if base and base.lower() in low:
            return f"{base} County"
    return None


def _extract_address(text: str) -> str | None:
    if not text:
        return None
    m = _ADDR_RX.search(text)
    if m:
        s = _norm_ws(m.group(0)).strip(" ,.;")
        s = re.sub(
            r"^(the\s+)?(address\s+)?(of\s+)?(the\s+)?(described\s+)?(property\s+)?(is\s+)?",
            "", s, flags=re.IGNORECASE,
        ).strip()
        return s or None
    return None


def _clean_address(addr: str | None) -> str | None:
    if not addr:
        return addr
    # strip leading zero-padded house numbers e.g. "000 " artifacts
    addr = re.sub(r"^\s*0+\s+", "", addr)
    # strip common label prefixes
    addr = re.sub(r"^(?:Commonly\s+(?:Known\s+As\s+)?)?Property\s+Address\s*:\s*", "", addr, flags=re.IGNORECASE)
    addr = _norm_ws(addr).strip(" ,.;")
    return addr or None


def _clip(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + " …"


def _build_snippet(county: str | None, address: str | None, body_text: str) -> str:
    header = []
    if county:
        header.append(f"County: {county}")
    if address:
        header.append(f"Address: {address}")
    lines = [re.sub(r"\s+", " ", ln).strip() for ln in body_text.splitlines()]
    lines = [ln for ln in lines if ln and len(ln) > 2]
    key_lines = []
    for ln in lines:
        low = ln.lower()
        if any(k in low for k in ("lis pendens", "property", "parcel", "defendant", "plaintiff", "recorded")):
            key_lines.append(ln)
        if len(key_lines) >= 6:
            break
    body = "\n".join(key_lines if key_lines else lines[:6])
    snippet = "\n".join(header + (["---"] if header else []) + ([body] if body else []))
    return _clip(snippet, _MAX_SNIPPET_LEN)


def _list_pages(seed: str) -> list[str]:
    """Expand a seed into paginated list URLs (tnlegalpub WordPress pattern)."""
    pages = [seed.rstrip("/") + "/"]
    for i in range(2, _MAX_LIST_PAGES + 1):
        pages.append(urljoin(pages[0], f"page/{i}/"))
    return pages


# ============================================================
# NOTICE FIELD PARSER
# ============================================================

_SD_LABEL_RX = re.compile(
    r"\bSale\s+Date\s*[:=]\s*([^\n,;]{3,40})",
    re.IGNORECASE,
)
_SD_INLINE_RX = re.compile(
    r"\bwill\s+be\s+(?:held|sold)\s+on\s+([^\n,;]{3,40})",
    re.IGNORECASE,
)
_SD_ORDINAL_RX = re.compile(
    r"\bon\s+the\s+(\d{1,2})(?:st|nd|rd|th)?\s+day\s+of\s+([A-Za-z]+)[,\s]+(\d{4})\b",
    re.IGNORECASE,
)

# "WHEREAS, McPhail Sanchez, LLC has been duly appointed Substitute Trustee"
_TRUSTEE_APPOINTED_RX = re.compile(
    r"(?:(?:WHEREAS|NOW,?\s+THEREFORE)[^,\n]*,\s+)?"
    r"([A-Za-z][\w\s.'&/-]{1,60}"
    r"(?:,\s*(?:LLC|Inc\.?|Corp\.?|PLLC|LLP|P\.?A\.?|P\.?C\.)[\w\s.]{0,15})?)"
    r"\s+has\s+been\s+(?:duly\s+)?appointed\s+(?:as\s+)?Substitute\s+Trustees?",
    re.IGNORECASE,
)
# "Edward D. Russell of The SR Law Group, having been appointed as Substitute Trustee"
_TRUSTEE_HAVING_RX = re.compile(
    r"([A-Za-z][\w\s.'&/-]{1,80}?),?\s+having\s+been\s+appointed\s+(?:as\s+)?Substitute\s+Trustees?",
    re.IGNORECASE,
)
# "Substitute Trustee is X"
_TRUSTEE_IS_RX = re.compile(
    r"\bSubstitute\s+Trustees?\s+is\s+([^\n]{3,160})",
    re.IGNORECASE,
)
# "[Substitute] Trustee(s): X"
_TRUSTEE_LABEL_RX = re.compile(
    r"\b(?:Substitute\s+)?Trustees?\s*(?:\(s\))?\s*[:=\-]\s*([^\n]{3,160})",
    re.IGNORECASE,
)
# "to X [as] Trustee" — original trustee fallback
_TRUSTEE_TO_RX = re.compile(
    r"\bto\s+([A-Za-z][^\n,]{3,80}?)\s+(?:as\s+)?Trustee\b",
    re.IGNORECASE,
)
# "executed X, husband and wife" — borrower names before marital/role phrase
_BORROWER_EXECUTED_RX = re.compile(
    r"\bexecuted\s+([A-Za-z][^\n]{3,140}?),\s*"
    r"(?:husband\s+and\s+wife|his\s+wife|her\s+husband|married|unmarried"
    r"|single\s+(?:person|woman|man)|as\s+\w)",
    re.IGNORECASE,
)
# "Borrower|Grantor|Defendant|Owner: X"
_BORROWER_RX = re.compile(
    r"\b(?:Borrower|Grantor|Defendant|Owner)\s*[:=\-]\s*([^\n]{3,120})",
    re.IGNORECASE,
)

_DATE_FMTS = [
    "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y",
    "%B %d, %Y", "%b %d, %Y", "%Y-%m-%d",
]
_MON_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
    "jan": 1, "feb": 2, "mar": 3, "apr": 4,
    "jun": 6, "jul": 7, "aug": 8, "sep": 9, "sept": 9,
    "oct": 10, "nov": 11, "dec": 12,
}


def _try_parse_date(raw: str) -> str | None:
    s = re.sub(r"(st|nd|rd|th)\b", "", raw, flags=re.IGNORECASE)
    s = re.sub(r"\s+", " ", s).strip(" ,.;")
    if re.search(r"[A-Za-z]", s):
        s = s.title()
    for fmt in _DATE_FMTS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _parse_notice_fields(text: str) -> dict:
    """
    Best-effort extraction of sale_date, trustee, borrower from notice text.
    Returns a dict with whatever was found; all keys optional.
    """
    out: dict = {}

    # Sale date — try labeled form first, then inline, then ordinal
    sale_raw: str | None = None
    for rx in (_SD_LABEL_RX, _SD_INLINE_RX):
        m = rx.search(text)
        if m:
            sale_raw = _norm_ws(m.group(1))
            break
    if not sale_raw:
        m = _SD_ORDINAL_RX.search(text)
        if m:
            day, mon, yr = m.group(1), m.group(2), m.group(3)
            if mon.lower() in _MON_MAP:
                sale_raw = f"{mon} {day}, {yr}"
    if sale_raw:
        out["sale_date_raw"] = sale_raw
        iso = _try_parse_date(sale_raw)
        if iso:
            out["sale_date_iso"] = iso

    # Trustee — priority: appointed/having > "is" form > label form > "to X Trustee" fallback
    trustee_val: str | None = None
    for rx in (_TRUSTEE_APPOINTED_RX, _TRUSTEE_HAVING_RX, _TRUSTEE_IS_RX, _TRUSTEE_LABEL_RX):
        m = rx.search(text)
        if m:
            val = _norm_ws(m.group(1)).strip(" ,.;:-")
            val = re.split(
                r"\b(?:Phone|Tel|Fax|Email|Address)\b",
                val, maxsplit=1, flags=re.IGNORECASE,
            )[0].strip(" ,.;:-")
            if val and len(val) > 2:
                trustee_val = val[:160]
                break
    if not trustee_val:
        m = _TRUSTEE_TO_RX.search(text)
        if m:
            val = _norm_ws(m.group(1)).strip(" ,.;:-")
            if val and len(val) > 2:
                trustee_val = val[:160]
    if trustee_val:
        out["trustee"] = trustee_val

    # Borrower — try "executed X, <marital/role>" first, then label form
    borrower_val: str | None = None
    m = _BORROWER_EXECUTED_RX.search(text)
    if m:
        val = _norm_ws(m.group(1)).strip(" ,.;:-")
        if val and len(val) > 1:
            borrower_val = val[:120]
    if not borrower_val:
        m = _BORROWER_RX.search(text)
        if m:
            val = _norm_ws(m.group(1)).strip(" ,.;:-")
            if val and len(val) > 1:
                borrower_val = val[:120]
    if borrower_val:
        out["borrower"] = borrower_val

    return out


# ============================================================
# MAIN
# ============================================================

def run() -> dict:
    if os.environ.get("FALCO_ENABLE_LIS_PENDENS", "").strip() != "1":
        print("[LisPendensBot] disabled (set FALCO_ENABLE_LIS_PENDENS=1 to enable)")
        return {"status": "disabled"}

    allowed = sorted(get_allowed_counties_base())
    print(f"[LisPendensBot] seeds={_SEEDS} allowed_counties={allowed} "
          f"max_list_pages={_MAX_LIST_PAGES} max_notice_links={_MAX_NOTICE_LINKS}")

    session = requests.Session()

    # ── 1. Collect notice-detail links ───────────────────────────────────────
    notice_links: list[str] = []
    seen_links: set[str] = set()
    list_pages_fetched = 0

    for seed in _SEEDS:
        for list_url in _list_pages(seed):
            if len(notice_links) >= _MAX_NOTICE_LINKS:
                break
            try:
                r = session.get(list_url, headers=_HEADERS, timeout=20)
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
            if len(notice_links) >= _MAX_NOTICE_LINKS:
                break

    # ── 2. Counters ───────────────────────────────────────────────────────────
    notice_pages_fetched = 0
    parsed_ok            = 0
    filtered_in          = 0
    skipped_not_lp       = 0
    skipped_out_of_geo   = 0
    skipped_http         = 0
    skipped_dup_in_run   = 0
    stored_leads         = 0
    stored_ingests       = 0
    stored_artifacts     = 0

    seen_in_run: set[str] = set()
    sample_kept: list[str] = []

    # ── 3. Process each notice page ──────────────────────────────────────────
    for url in notice_links:
        try:
            r = session.get(url, headers=_HEADERS, timeout=20)
        except Exception:
            skipped_http += 1
            continue
        if r.status_code != 200:
            skipped_http += 1
            continue
        notice_pages_fetched += 1

        soup = BeautifulSoup(r.text, "html.parser")
        entry = soup.select_one("article .entry-content") or soup.select_one(".entry-content")
        body_text = entry.get_text("\n") if entry else soup.get_text("\n")
        article = soup.select_one("article") or soup.select_one("main") or soup
        full_text = article.get_text("\n")
        combined = _norm_ws(body_text + "\n" + full_text)

        # Must contain LP keyword to qualify
        if not _is_lp(combined):
            skipped_not_lp += 1
            continue

        county = _extract_county(combined)
        if not county:
            skipped_out_of_geo += 1
            continue

        county_full = normalize_county_full(county) or county
        if (not is_allowed_county(county_full)) or (not within_target_counties(county_full)):
            skipped_out_of_geo += 1
            continue

        address = _clean_address(_extract_address(combined))

        lead_key = make_lead_key("LIS_PENDENS", county_full, address or "")

        if lead_key in seen_in_run:
            skipped_dup_in_run += 1
            continue
        seen_in_run.add(lead_key)

        parsed_ok += 1

        # ── Persist ───────────────────────────────────────────────────────────
        retrieved_at = _now_iso()

        if _store.upsert_lead(
            lead_key,
            {"address": address or "", "state": "TN"},
            county_full,
            distress_type="LIS_PENDENS",
        ):
            stored_leads += 1

        _fields = _parse_notice_fields(body_text)
        _sale_date = _fields.get("sale_date_iso")
        raw_json = json.dumps(_fields, ensure_ascii=False) if _fields else None
        if _store.insert_ingest_event(lead_key, "LIS_PENDENS", url, _sale_date, raw_json):
            stored_ingests += 1

        ok, _ = _store.insert_raw_artifact(
            lead_key,
            "LIS_PENDENS_HTML",
            url,
            retrieved_at,
            "text/html",
            payload_text=body_text,
            notes="LisPendensBot listing HTML",
        )
        if ok:
            stored_artifacts += 1

        filtered_in += 1
        if len(sample_kept) < 5:
            sample_kept.append(
                f"county={county_full} addr={address or '[missing]'} url={url}"
            )

    summary = {
        "leads_found":        len(notice_links),
        "parsed_ok":          parsed_ok,
        "filtered_in":        filtered_in,
        "skipped_out_of_geo": skipped_out_of_geo,
        "skipped_not_lp":     skipped_not_lp,
        "skipped_http":       skipped_http,
        "skipped_dup_in_run": skipped_dup_in_run,
        "stored_leads":       stored_leads,
        "stored_ingests":     stored_ingests,
        "stored_artifacts":   stored_artifacts,
    }

    print(
        "[LisPendensBot] summary "
        f"list_pages_fetched={list_pages_fetched} "
        f"notice_links_found={len(notice_links)} "
        f"notice_pages_fetched={notice_pages_fetched} "
        + " ".join(f"{k}={v}" for k, v in summary.items())
        + f" sample_kept={sample_kept}"
    )
    print("[LisPendensBot] Done.")
    return summary



