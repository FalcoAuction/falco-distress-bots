"""
Supabase-backed lead persistence.

Replaces the Notion writes that the bots used to do. Each bot now upserts
its discovered leads directly into the falco-site Supabase
`homeowner_requests` table with `source = 'bot'`. The dialer queue and
/admin Pipeline tab read from this table live, so leads appear within
~minutes of the bot run completing.

Idempotent: upsert on `pipeline_lead_key`. Re-running a bot with the same
leads updates fields rather than creating duplicates.

Required env vars (same as src/sync/site_sync.py):
    NEXT_PUBLIC_SUPABASE_URL
    SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from supabase import create_client, Client
except ImportError:
    print(
        "[supabase_store] ERROR: supabase-py not installed. Run: pip install supabase>=2.0.0",
        file=sys.stderr,
    )
    raise


SUPABASE_TABLE = "homeowner_requests"


# ----------------------------------------------------------------------------
# Address normalization for cross-slug dedup
# ----------------------------------------------------------------------------
# Same property gets imported under different slugs whenever the slug scheme
# changes — historically `<county>-foreclosure-<hex>` then later `sha40` of
# the address. Without normalization we accumulate duplicate rows.
#
# Strategy: derive a stable canonical key from the property address and look
# it up before inserting. If found, update that row's lead_key to the new
# scheme rather than creating a new row. Self-healing dedup over time.

import re

_STREET_SUFFIX_MAP = {
    "street": "st", "str": "st", "st": "st",
    "avenue": "ave", "ave": "ave", "av": "ave",
    "road": "rd", "rd": "rd",
    "drive": "dr", "dr": "dr",
    "lane": "ln", "ln": "ln",
    "boulevard": "blvd", "blvd": "blvd",
    "court": "ct", "ct": "ct",
    "circle": "cir", "cir": "cir",
    "place": "pl", "pl": "pl",
    "terrace": "ter", "ter": "ter",
    "highway": "hwy", "hwy": "hwy",
    "parkway": "pkwy", "pkwy": "pkwy",
    "way": "way",
    "trail": "trl", "trl": "trl",
}

_DIRECTIONAL_MAP = {
    "north": "n", "n": "n",
    "south": "s", "s": "s",
    "east": "e", "e": "e",
    "west": "w", "w": "w",
    "northeast": "ne", "ne": "ne",
    "northwest": "nw", "nw": "nw",
    "southeast": "se", "se": "se",
    "southwest": "sw", "sw": "sw",
}


def _normalize_address(addr: Optional[str]) -> Optional[str]:
    """Derive a canonical key from a free-form address.

    "720 Sweetbrier Rd, Brentwood, TN 37027"
    "720 SWEETBRIER ROAD, Brentwood TN, 37027"
    "720 Sweetbrier Road"
        → all yield "720 sweetbrier rd|brentwood|37027"

    Returns None if the input is too sparse to dedup on (no number+street).
    """
    if not addr:
        return None
    s = str(addr).strip().lower()
    # Strip everything in parens and double spaces
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Split into street / city / state-zip components by commas
    parts = [p.strip() for p in s.split(",") if p.strip()]
    if not parts:
        return None

    # Tokenize the street portion (first comma-segment)
    street_tokens = re.findall(r"[a-z0-9]+", parts[0])
    if not street_tokens:
        return None
    # Require at least a number (or alphanumeric like "1A") + something else
    if len(street_tokens) < 2:
        return None

    norm_tokens = []
    for tok in street_tokens:
        if tok in _STREET_SUFFIX_MAP:
            norm_tokens.append(_STREET_SUFFIX_MAP[tok])
        elif tok in _DIRECTIONAL_MAP:
            norm_tokens.append(_DIRECTIONAL_MAP[tok])
        else:
            norm_tokens.append(tok)
    street_key = " ".join(norm_tokens)

    # City — second segment if present
    city_key = ""
    if len(parts) >= 2:
        city_tokens = re.findall(r"[a-z]+", parts[1])
        # Strip trailing state if it slipped into city ("brentwood tn")
        if city_tokens and city_tokens[-1] in {"tn", "tennessee", "ky", "ms", "ga", "al", "nc", "va", "ar", "mo"}:
            city_tokens = city_tokens[:-1]
        city_key = " ".join(city_tokens)

    # ZIP — find any 5-digit run anywhere
    zip_match = re.search(r"\b(\d{5})(?:-\d{4})?\b", s)
    zip_key = zip_match.group(1) if zip_match else ""

    return f"{street_key}|{city_key}|{zip_key}"

# Track whether we've warned about missing creds this process
_WARNED_MISSING = False
# Cache the client per process to avoid re-creating on every upsert
_CLIENT: Optional[Client] = None


def _client() -> Optional[Client]:
    """Build the Supabase client lazily. Returns None and warns once if
    creds are missing — bots can then no-op rather than crashing."""
    global _CLIENT, _WARNED_MISSING
    if _CLIENT is not None:
        return _CLIENT
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or os.environ.get("SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        if not _WARNED_MISSING:
            _WARNED_MISSING = True
            print(
                "[supabase_store] WARNING: Missing NEXT_PUBLIC_SUPABASE_URL or "
                "SUPABASE_SERVICE_ROLE_KEY. Lead writes will be no-ops this run."
            )
        return None
    _CLIENT = create_client(url, key)
    return _CLIENT


# ----------------------------------------------------------------------------
# Distress type normalization
# ----------------------------------------------------------------------------
# Bots emit human-readable strings ("Foreclosure", "Lis Pendens").
# Supabase stores canonical uppercase tags consistent with the rest of the
# pipeline (TRUSTEE_NOTICE, LIS_PENDENS, NOD, etc.).
# ----------------------------------------------------------------------------

_DISTRESS_MAP = {
    # Trustee / foreclosure
    "foreclosure": "TRUSTEE_NOTICE",
    "trustee notice": "TRUSTEE_NOTICE",
    "trustee_notice": "TRUSTEE_NOTICE",
    "trustee sale": "TRUSTEE_NOTICE",
    # Lis pendens
    "lis pendens": "LIS_PENDENS",
    "lis_pendens": "LIS_PENDENS",
    # Pre-foreclosure / NOD
    "preforeclosure": "PREFORECLOSURE",
    "pre-foreclosure": "PREFORECLOSURE",
    "pre_foreclosure": "PREFORECLOSURE",
    "nod": "NOD",
    "notice of default": "NOD",
    "sot": "SUBSTITUTION_OF_TRUSTEE",
    "substitution of trustee": "SUBSTITUTION_OF_TRUSTEE",
    # FSBO / tax / probate
    "fsbo": "FSBO",
    "tax sale": "TAX_LIEN",
    "tax lien": "TAX_LIEN",
    "tax delinquent": "TAX_LIEN",
    "probate": "PROBATE",
}


def _normalize_distress(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = str(value).strip().lower()
    if not key:
        return None
    if key in _DISTRESS_MAP:
        return _DISTRESS_MAP[key]
    # Default: uppercase + underscore-replace, capped at 60 chars
    return key.upper().replace(" ", "_").replace("-", "_")[:60]


# ----------------------------------------------------------------------------
# Payload normalization
# ----------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_row(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a bot payload dict into a homeowner_requests row.

    Bot payloads vary slightly across scrapers; normalize the common shape:
        title, source, distress_type, county, address, sale_date_iso,
        trustee_attorney, contact_info, raw_snippet, url, lead_key, days_to_sale
    """
    lead_key = (payload.get("lead_key") or "").strip()
    if not lead_key:
        raise ValueError("upsert_lead requires payload['lead_key']")

    address = payload.get("address") or payload.get("title") or "(no address)"
    raw_snippet = payload.get("raw_snippet") or ""
    trustee = payload.get("trustee_attorney") or payload.get("contact_info") or ""
    bot_source = payload.get("source") or ""
    url = payload.get("url") or ""
    sale_date = payload.get("sale_date_iso") or None

    # Build a structured admin_notes blob — preserves the bot context
    # without requiring schema changes for things like trustee name.
    notes_lines = []
    if bot_source:
        notes_lines.append(f"source: {bot_source}")
    if trustee:
        notes_lines.append(f"trustee/contact: {trustee}")
    if url:
        notes_lines.append(f"source url: {url}")
    if raw_snippet:
        notes_lines.append(f"raw: {raw_snippet}")
    admin_notes = "\n".join(notes_lines) if notes_lines else ""

    row: Dict[str, Any] = {
        "pipeline_lead_key": lead_key,
        "source": "bot",
        "status": "new",
        "property_address": address,
        "county": payload.get("county") or None,
        "distress_type": _normalize_distress(payload.get("distress_type")),
        "trustee_sale_date": sale_date,
        "admin_notes": admin_notes,
        "submitted_at": _now_iso(),
        "updated_at": _now_iso(),
    }
    return {k: v for k, v in row.items() if v is not None or k in (
        "source", "status", "admin_notes", "pipeline_lead_key",
    )}


# ----------------------------------------------------------------------------
# Public API
# ----------------------------------------------------------------------------

def upsert_lead(payload: Dict[str, Any]) -> str:
    """Upsert a single bot-discovered lead into homeowner_requests.

    The unique index on pipeline_lead_key is PARTIAL
    (WHERE pipeline_lead_key IS NOT NULL), which Supabase's upsert API
    can't target via on_conflict. So we do explicit existence-check then
    insert-or-update.

    Returns:
        "inserted" - new row created
        "updated"  - existing row updated
        "noop"     - Supabase not configured; no write performed
        "error"    - write failed (logged but not raised)
    """
    client = _client()
    if client is None:
        return "noop"

    try:
        row = _build_row(payload)
    except ValueError as e:
        print(f"[supabase_store] payload validation error: {e}")
        return "error"

    lead_key = row["pipeline_lead_key"]

    try:
        # Step 1: look up by pipeline_lead_key (exact match).
        existing = (
            client.table(SUPABASE_TABLE)
            .select("id")
            .eq("pipeline_lead_key", lead_key)
            .eq("source", "bot")
            .limit(1)
            .execute()
        )
        rows = getattr(existing, "data", None) or []
        if rows:
            update_payload = {k: v for k, v in row.items() if k != "submitted_at"}
            client.table(SUPABASE_TABLE).update(update_payload).eq(
                "id", rows[0]["id"]
            ).execute()
            return "updated"

        # Step 2: address-based dedup. The slug scheme has changed mid-stream
        # (legacy `<county>-foreclosure-<hex>` → newer sha40 keys), which
        # produced 10+ duplicate addresses in production. Before inserting,
        # check whether ANY existing row matches this property by canonical
        # address. If so, claim that row by updating its lead_key to the
        # current one — self-healing dedup over time.
        addr_key = _normalize_address(row.get("property_address"))
        if addr_key:
            # Pull all bot rows in the same county to keep the search bounded;
            # then match on normalized address client-side. Cheap (N is small).
            county = row.get("county")
            q = client.table(SUPABASE_TABLE).select(
                "id, property_address"
            ).eq("source", "bot")
            if county:
                q = q.eq("county", county)
            else:
                # Without county we have to scan; still bounded by source='bot'.
                pass
            scan = q.limit(2000).execute()
            scan_rows = getattr(scan, "data", None) or []
            match_id = None
            for sr in scan_rows:
                if _normalize_address(sr.get("property_address")) == addr_key:
                    match_id = sr.get("id")
                    break
            if match_id:
                update_payload = {
                    k: v for k, v in row.items() if k != "submitted_at"
                }
                client.table(SUPABASE_TABLE).update(update_payload).eq(
                    "id", match_id
                ).execute()
                return "updated"

        # Step 3: genuinely new — INSERT.
        client.table(SUPABASE_TABLE).insert(row).execute()
        return "inserted"
    except Exception as e:
        print(f"[supabase_store] write failed for {lead_key}: {e}")
        return "error"


def find_existing_by_lead_key(lead_key: str) -> Optional[Dict[str, Any]]:
    """Compatibility shim for bots that still call this. Returns the row
    if it exists, None otherwise. Bots used this to decide insert vs
    update — with upsert this is no longer strictly needed, but kept for
    backwards compat with older bot code paths."""
    client = _client()
    if client is None or not lead_key:
        return None
    try:
        result = (
            client.table(SUPABASE_TABLE)
            .select("id, pipeline_lead_key, status, property_address")
            .eq("pipeline_lead_key", lead_key)
            .eq("source", "bot")
            .limit(1)
            .execute()
        )
        if hasattr(result, "data") and result.data:
            return dict(result.data[0])
        return None
    except Exception as e:
        print(f"[supabase_store] find_existing_by_lead_key failed: {e}")
        return None
