"""
Public notice enrichment bot — extracts mortgage data from full notice text.

This is NOT a lead-source bot. It walks existing leads (where source_url
points at a TN trustee-sale notice) and re-fetches the full notice text,
which contains gold-standard public-record mortgage data:

  - Legal borrower name (often more accurate than BatchData skip-trace)
  - Original deed of trust date
  - Original DOT recording instrument number
  - Original lender / note payee
  - Substitute trustee (and original trustee if assigned)
  - Property parcel ID
  - Junior lienholders (HELOCs, judgments mentioned in notice)
  - Original loan amount (when present)
  - Current debt amount (when present, in default-recital section)
  - Sale date, location, time

Result: free equivalent of ATTOM mortgage data PLUS the lien stack
information ATTOM consistently misses.

Distress type: existing lead's distress_type is preserved; this bot
enriches in place.

Writes:
  - Updates homeowner_requests_staging row (or homeowner_requests if
    already promoted) with mortgage_details JSONB
  - Sets owner_name_records to the legal borrower name when extracted
  - Appends to admin_notes
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    import requests
except ImportError:
    requests = None

try:
    from bs4 import BeautifulSoup
except ImportError:
    BeautifulSoup = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    from supabase import create_client, Client
except ImportError:
    print("[notice-enricher] supabase-py not installed", file=sys.stderr)
    raise

from io import BytesIO
from urllib.parse import urljoin

try:
    from ._field_confidence import deep_merge_dict
except ImportError:
    from _field_confidence import deep_merge_dict


# Patterns matching how various scrapers store source URLs in admin_notes.
# We'll regex-extract since the live homeowner_requests table doesn't have
# a dedicated source_url column.
_URL_FROM_NOTES_RX = re.compile(
    r"(?:source\s+url|source_url|src):\s*(https?://\S+)",
    re.IGNORECASE,
)


# ─── Config ─────────────────────────────────────────────────────────────

USER_AGENT = "FALCO-Lead-Research/1.0 (+ops@falco.llc)"
PER_HOST_THROTTLE_S = 1.5  # polite

# Patterns to extract from notice text. Notices follow standardized
# legal language in TN; these patterns hit the variants we've seen.

# Legal phrasings vary across notice formats. tnlegalpub.com uses the
# "WHEREAS, NAME, by Deed of Trust" pattern. ForeclosureTennessee.com PDFs
# (older substitute-trustee-sale style) use "Default having been made...
# Deed of Trust dated DATE, executed by NAME, to TRUSTEE...". We try both.
_PATTERNS = {
    "borrower": [
        re.compile(r"WHEREAS,\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*by Deed of Trust", re.IGNORECASE),
        re.compile(r"executed by\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*(?:to|husband)", re.IGNORECASE),
        re.compile(r"Owner of Property:\s*([A-Z][A-Za-z0-9 .,&'\-/()]+?)(?:\.|\n|Property ID)", re.IGNORECASE),
        re.compile(r"property conveyed to\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),?\s*(?:a married|a single|by Warranty)", re.IGNORECASE),
    ],
    # Disqualifier: phrases that look like a name match but aren't real names
    "_borrower_blacklist": [
        re.compile(r"^the\s+(?:herein|aforementioned|above|named)", re.IGNORECASE),
        re.compile(r"^Grantor\b", re.IGNORECASE),
        re.compile(r"^Trustee\b", re.IGNORECASE),
        re.compile(r"^undersigned\b", re.IGNORECASE),
    ],
    "dot_date": [
        re.compile(r"Deed of Trust[^,]{0,30}dated\s+([A-Z][a-z]+ \d{1,2},\s*\d{4})", re.IGNORECASE),
    ],
    "dot_instrument": [
        re.compile(r"recorded on \w+\s+\d{1,2},\s*\d{4},?\s*in Instrument No\.\s*([A-Z0-9\-]+)", re.IGNORECASE),
        re.compile(r"in Instrument No\.\s*([A-Z0-9\-]+)\s*in the Register", re.IGNORECASE),
    ],
    # ForeclosureTennessee pattern: "recorded in Book NNNN, Page NNNN"
    "dot_book_page": [
        re.compile(r"recorded in Book\s*(\d+),?\s*Page\s*(\d+)", re.IGNORECASE),
    ],
    "lender": [
        # tnlegalpub style: "Note was payable to, LENDER"
        re.compile(r"Note was payable to,?\s*([A-Z][A-Za-z0-9 .,&'\-/()]+?)(?:,\s*which|;|\.|$)", re.IGNORECASE),
        re.compile(r"payable to\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*which the aforementioned", re.IGNORECASE),
        # ForeclosureTennessee style: "indebtedness therein described to LENDER"
        re.compile(r"indebtedness therein described to\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*and", re.IGNORECASE),
        re.compile(r"to secure[^.]{0,100}?(?:to|payable to)\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*and", re.IGNORECASE),
    ],
    "current_holder": [
        re.compile(r"subsequently assigned to\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?)\s*(?:dated|of record)", re.IGNORECASE),
        re.compile(r"lawful owner and holder of said indebtedness[^.]{0,200}?,\s*([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*as substitute", re.IGNORECASE),
    ],
    "substitute_trustee": [
        re.compile(r"WHEREAS,\s+([A-Z][A-Za-z0-9 .,&'\-/()]+?)\s*has been duly appointed Substitute Trustee", re.IGNORECASE),
        # ForeclosureTennessee: "appointed the undersigned, NAME, as substitute trustee"
        re.compile(r"appointed the undersigned,?\s*([A-Z][A-Za-z0-9 .,&'\-/()]+?),\s*as substitute trustee", re.IGNORECASE),
        re.compile(r"original trustee[^.]{0,200}?,\s*([A-Z][A-Za-z0-9 .,&'\-/()]+?),?\s*Trustee", re.IGNORECASE),
    ],
    "parcel_id": [
        re.compile(r"Parcel\s+Number:\s*([A-Z0-9\-\.\s]+?)(?:\s+The|\.|\n)", re.IGNORECASE),
        re.compile(r"Property\s+ID:\s*([A-Z0-9\-\.\s]+?)(?:\s+In|\.|\n)", re.IGNORECASE),
        # ForeclosureTennessee: "TAX MAP-PARCEL NO.: 123M-A-001.01"
        re.compile(r"TAX\s+MAP[-\s]+PARCEL\s+NO\.?:?\s*([A-Z0-9\-\.\s]+?)(?:\s|\(|$)", re.IGNORECASE),
    ],
    "original_principal": [
        re.compile(r"original principal\s+(?:amount|sum)\s+of\s+\$([\d,]+\.\d{2}|\d{1,3}(?:,\d{3})+|\d+)", re.IGNORECASE),
        re.compile(r"in the principal amount of\s+\$([\d,]+\.\d{2}|\d{1,3}(?:,\d{3})+|\d+)", re.IGNORECASE),
    ],
    "default_amount": [
        re.compile(r"in the amount of\s+\$([\d,]+\.\d{2})\s+(?:as of|due|owed)", re.IGNORECASE),
        re.compile(r"default[^.]{0,200}\$([\d,]+\.\d{2})", re.IGNORECASE),
    ],
    "junior_liens": [
        re.compile(r"([A-Z][A-Za-z0-9 .,&'\-/()]+?)\s*,\s*Junior Lienholder", re.IGNORECASE),
    ],
    "property_address_in_notice": [
        re.compile(r"PROPERTY\s+ADDRESS:?\s*([\d][^,\n]+?,\s*[A-Z][A-Za-z]+,?\s*TN\s+\d{5})", re.IGNORECASE),
        re.compile(r"street address[^.]{0,40}is believed to be\s+([\d][^,\n]+?,\s*[A-Z][A-Za-z]+,?\s*TN\s+\d{5})", re.IGNORECASE),
    ],
}


# ─── Supabase client ────────────────────────────────────────────────────

def _supabase() -> Optional[Client]:
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        print("[notice-enricher] missing SUPABASE creds", file=sys.stderr)
        return None
    return create_client(url, key)


# ─── HTTP helpers ───────────────────────────────────────────────────────

_LAST_FETCH: Dict[str, float] = {}


def _polite_get(session: requests.Session, url: str) -> Optional[str]:
    from urllib.parse import urlparse
    host = urlparse(url).netloc
    last = _LAST_FETCH.get(host, 0)
    elapsed = time.time() - last
    if elapsed < PER_HOST_THROTTLE_S:
        time.sleep(PER_HOST_THROTTLE_S - elapsed)
    _LAST_FETCH[host] = time.time()
    try:
        r = session.get(url, timeout=20, allow_redirects=True)
        if r.status_code != 200:
            return None
        return r.text
    except Exception:
        return None


# ─── Extraction ─────────────────────────────────────────────────────────

def extract_notice_text(html: str) -> str:
    """Extract the legal-notice body text from a tnlegalpub or similar
    notice page. Falls back to whole-body text if no obvious article."""
    if not BeautifulSoup:
        return html
    soup = BeautifulSoup(html, "html.parser")
    body = (
        soup.select_one("article")
        or soup.select_one(".post-content")
        or soup.select_one(".entry-content")
        or soup.select_one("main")
        or soup.find("body")
    )
    text = body.get_text(" ", strip=True) if body else soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def extract_mortgage_details(text: str) -> Dict[str, Any]:
    """Extract gold-standard mortgage / lien data from notice text.
    Returns a dict; missing fields omitted."""
    out: Dict[str, Any] = {}
    blacklist = _PATTERNS.get("_borrower_blacklist", [])
    for field, patterns in _PATTERNS.items():
        if field.startswith("_"):
            continue
        if field == "junior_liens":
            # multi-match
            liens = []
            for pat in patterns:
                for m in pat.finditer(text):
                    val = m.group(1).strip(" ,.")
                    if val and val not in liens and len(val) < 120:
                        liens.append(val)
            if liens:
                out["junior_liens"] = liens
            continue
        for pat in patterns:
            m = pat.search(text)
            if m:
                val = m.group(1).strip(" ,.").rstrip()
                # Sanity caps
                if len(val) > 200:
                    val = val[:200].rstrip()
                # Skip blacklisted "borrower"-shaped phrases that aren't names
                if field == "borrower":
                    if any(bl.search(val) for bl in blacklist):
                        continue
                    if len(val) < 4:
                        continue
                out[field] = val
                break
    # Normalize amounts
    for amount_field in ("original_principal", "default_amount"):
        if amount_field in out:
            digits = re.sub(r"[^\d.]", "", out[amount_field])
            try:
                out[amount_field + "_value"] = float(digits)
            except (TypeError, ValueError):
                pass
    return out


# ─── Main ───────────────────────────────────────────────────────────────

def _resolve_source_url(lead: Dict[str, Any]) -> Optional[str]:
    """Find the per-notice URL from any of three places it might live."""
    # 1. Dedicated column (staging table)
    if lead.get("source_url"):
        return lead["source_url"]
    # 2. raw_payload dict (set by some scrapers)
    rp = lead.get("raw_payload") or {}
    if isinstance(rp, dict):
        for key in ("source_url", "url", "notice_url"):
            if rp.get(key):
                return rp[key]
    # 3. admin_notes (where most legacy scrapers stuff it)
    notes = lead.get("admin_notes") or ""
    m = _URL_FROM_NOTES_RX.search(notes)
    if m:
        return m.group(1).rstrip(".,)/")
    return None


def _is_per_notice_url(url: str) -> bool:
    """Skip aggregator/listing pages (e.g. county hub) — only process pages
    that point at a single foreclosure notice."""
    if not url:
        return False
    # tnlegalpub legal_notice/<slug>
    if "tnlegalpub.com/legal_notice/" in url:
        return True
    # ForeclosureTennessee per-listing
    if "foreclosuretennessee.com/Foreclosure/Foreclosure-Listing" in url:
        return True
    # TNForeclosureNotices per-notice (TNFN#NNNN)
    if "tnforeclosurenotices.com/notice/" in url or "tnfn" in url.lower():
        return True
    return False


def _fetch_notice_text(session: requests.Session, url: str) -> Optional[str]:
    """Fetch the per-notice URL and return cleaned notice text. Handles
    both HTML pages (tnlegalpub) and HTML-with-linked-PDF pages
    (ForeclosureTennessee — text actually lives in the PDF)."""
    html = _polite_get(session, url)
    if html is None:
        return None
    if BeautifulSoup is None:
        return html

    soup = BeautifulSoup(html, "html.parser")

    # Did the page link to a PDF that contains the actual notice?
    pdf_link = None
    if "foreclosuretennessee.com" in url:
        # Look for "Download PDF" / .pdf links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            if href.lower().endswith(".pdf") or "download pdf" in text:
                pdf_link = urljoin(url, href)
                break

    if pdf_link and pdfplumber is not None:
        try:
            pdf_bytes: Optional[bytes] = None
            if pdf_link.startswith("data:application/pdf"):
                # Embedded base64 PDF (ForeclosureTennessee.com pattern)
                import base64
                _, _, b64data = pdf_link.partition(",")
                # Strip any URL-encoded padding
                b64data = b64data.split("#")[0]
                try:
                    pdf_bytes = base64.b64decode(b64data + "=" * (-len(b64data) % 4))
                except Exception:
                    pdf_bytes = None
            else:
                r = session.get(pdf_link, timeout=30)
                if r.status_code == 200 and "pdf" in r.headers.get("Content-Type", "").lower():
                    pdf_bytes = r.content
            if pdf_bytes:
                with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
                    pdf_text = "\n".join((p.extract_text() or "") for p in pdf.pages)
                return re.sub(r"\s+", " ", pdf_text)
        except Exception:
            pass
        # Fall through to HTML text if PDF fetch fails

    return extract_notice_text(html)


def enrich_lead(
    client: Client, session: requests.Session, lead: Dict[str, Any]
) -> Tuple[bool, Dict[str, Any]]:
    """Enrich one lead. Returns (success, details_dict)."""
    source_url = _resolve_source_url(lead)
    if not source_url:
        return False, {"error": "no_source_url"}
    if not _is_per_notice_url(source_url):
        return False, {"error": "not_per_notice_url", "url": source_url}

    text = _fetch_notice_text(session, source_url)
    if not text:
        return False, {"error": "fetch_failed", "url": source_url}

    details = extract_mortgage_details(text)
    if not details:
        return False, {"error": "no_extractable_data"}
    details["_source_url"] = source_url
    return True, details


def update_lead(
    client: Client, table: str, lead_id: str, details: Dict[str, Any], lead: Dict[str, Any]
) -> bool:
    """Push enriched data back into the lead row."""
    notes_addition = " · ".join(
        f"{k}={v}"[:100]
        for k, v in details.items()
        if k != "junior_liens" and not k.endswith("_value")
    )
    if details.get("junior_liens"):
        notes_addition += f" · junior_liens=[{', '.join(details['junior_liens'])}]"

    update: Dict[str, Any] = {}
    # Promote borrower to owner_name_records when present and current is empty
    if details.get("borrower") and not lead.get("owner_name_records"):
        update["owner_name_records"] = details["borrower"]
    mortgage_signals: List[Dict[str, Any]] = []
    # Original principal is useful context, but it is not current payoff.
    # Keep the amortized estimate in metadata instead of poisoning the
    # mortgage_balance column that downstream math treats as payoff.
    if "original_principal_value" in details and details.get("dot_date"):
        try:
            dot_dt = datetime.strptime(details["dot_date"], "%B %d, %Y")
            yrs = max(0, (datetime.now() - dot_dt).days / 365.25)
            r = 0.04 / 12
            n = 360
            paid = min(yrs * 12, n)
            remaining = (((1 + r) ** n) - ((1 + r) ** paid)) / (((1 + r) ** n) - 1)
            est_balance = round(details["original_principal_value"] * remaining)
            details["amortized_balance_estimate"] = est_balance
            mortgage_signals.append({
                "source": "notice_enricher",
                "kind": "original_principal",
                "amount": est_balance,
                "confidence": 0.45,
                "original_principal": details["original_principal_value"],
                "dot_date": details.get("dot_date"),
                "note": "Amortized original principal estimate; not verified current payoff.",
            })
        except Exception:
            pass
    # Default/arrears amount is also not payoff. Preserve it as a signal.
    if "default_amount_value" in details:
        mortgage_signals.append({
            "source": "notice_enricher",
            "kind": "default_amount",
            "amount": int(details["default_amount_value"]),
            "confidence": 0.30,
            "note": "Default/arrears amount from notice; not current payoff.",
        })

    if mortgage_signals:
        existing_meta = lead.get("phone_metadata") or {}
        signal = mortgage_signals[0]
        if any(s.get("kind") == "default_amount" for s in mortgage_signals):
            signal = next(s for s in mortgage_signals if s.get("kind") == "default_amount")
        update["phone_metadata"] = deep_merge_dict(existing_meta, {
            "mortgage_signal": signal,
            "mortgage_signals": mortgage_signals,
        })

    # Append to admin_notes — both for the live (no-raw_payload) path
    # and as a human-readable summary for the staging path
    existing_notes = lead.get("admin_notes") or ""
    update["admin_notes"] = (
        existing_notes + (" · " if existing_notes else "") + f"NOTICE-ENRICH: {notes_addition}"
    )[:4000]

    # Live table doesn't have raw_payload column; only set it for staging
    if table == "homeowner_requests_staging":
        rp = lead.get("raw_payload") or {}
        if not isinstance(rp, dict):
            rp = {}
        rp["mortgage_details"] = details
        update["raw_payload"] = rp

    try:
        client.table(table).update(update).eq("id", lead_id).execute()
        return True
    except Exception as e:
        print(f"  update failed: {e}")
        return False


def run() -> Dict[str, Any]:
    if requests is None or BeautifulSoup is None:
        print("[notice-enricher] missing requests or bs4")
        return {"name": "notice_enricher", "status": "missing_deps"}

    client = _supabase()
    if client is None:
        return {"name": "notice_enricher", "status": "no_supabase"}

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    enriched_count = 0
    failed_count = 0
    skipped_count = 0

    # Walk both staging AND live tables. Look for leads with a per-notice URL
    # (in source_url, raw_payload, or admin_notes) and no mortgage_details yet.
    table_specs = [
        # staging has dedicated source_url column
        (
            "homeowner_requests_staging",
            "id, source_url, owner_name_records, admin_notes, raw_payload, "
            "property_address, mortgage_balance, phone_metadata",
        ),
        # live table has no source_url column; URL lives in admin_notes
        (
            "homeowner_requests",
            "id, owner_name_records, admin_notes, "
            "property_address, mortgage_balance, phone_metadata",
        ),
    ]
    for table, columns in table_specs:
        print(f"\n--- {table} ---")
        try:
            res = client.table(table).select(columns).limit(500).execute()
        except Exception as e:
            print(f"  fetch failed: {e}")
            continue

        rows = getattr(res, "data", None) or []
        targets = []
        for r in rows:
            url = _resolve_source_url(r)
            if not url or not _is_per_notice_url(url):
                continue
            # Skip leads we've already enriched
            rp = r.get("raw_payload")
            if isinstance(rp, dict) and rp.get("mortgage_details"):
                continue
            if "NOTICE-ENRICH" in (r.get("admin_notes") or ""):
                continue
            targets.append(r)
        print(f"  {len(targets)} candidate leads with per-notice URLs lacking enrichment")

        for i, lead in enumerate(targets[:50]):  # cap per run
            print(f"  [{i+1}] {lead.get('property_address', '')[:60]}", end=" -> ")
            ok, details = enrich_lead(client, session, lead)
            if not ok:
                print(f"SKIP ({details.get('error', 'unknown')})")
                skipped_count += 1
                continue
            extracted_keys = [k for k in details if not k.endswith("_value")]
            print(f"OK ({len(extracted_keys)} fields: {', '.join(extracted_keys[:5])})")
            if update_lead(client, table, lead["id"], details, lead):
                enriched_count += 1
            else:
                failed_count += 1

    print(f"\nenriched={enriched_count} failed={failed_count} skipped={skipped_count}")
    return {
        "name": "notice_enricher",
        "status": "ok" if enriched_count > 0 else "zero_yield",
        "enriched": enriched_count,
        "failed": failed_count,
        "skipped": skipped_count,
    }


if __name__ == "__main__":
    print(run())
