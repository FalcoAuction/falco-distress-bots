# src/enrichment/contact_enricher.py
#
# Contact acquisition waterfall for FALCO.
#
# Tier 1 — notice-native extraction (notice_extractor / notice_pdf_extractor)
#           Already run upstream; fields live in lead_field_provenance.
#           This module reads them to inform the contact_ready flag.
#
# Tier 2 — public trustee / law-firm phone lookup (TableTrusteePhoneProvider)
#           Writes: trustee_phone_public
#
# Tier 3 — owner skip trace adapter (NullSkipTraceProvider by default)
#           Writes: owner_phone_primary, owner_phone_secondary,
#                   owner_phone_source, owner_phone_confidence,
#                   owner_phone_dnc_primary, owner_phone_dnc_secondary,
#                   owner_phone_dnc_status
#
# contact_ready (computed flag, refreshed every run)
#           "1" when any tier produced an actionable phone or name.
#
# Entry point
# -----------
#   enrich_contact_data(lead_key, fields, cur) -> Dict[str, int]
#       Mutates `fields` in-place with newly discovered values.
#       Writes provenance rows via `cur`.
#       Returns summary: {"t2_written": N, "t3_written": N, "errors": N}

from __future__ import annotations

import os
import re
import sqlite3
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .providers.trustee_phone_provider import get_trustee_phone_provider
from .providers.skip_trace_provider import get_skip_trace_provider


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _sanitize_phone(s: Optional[str]) -> Optional[str]:
    """Reject garbage phone patterns (mirrors pdf_builder._sanitize_phone)."""
    if not s:
        return None
    digits = re.sub(r"\D", "", s)
    if len(digits) != 10:
        return None
    area, exch = digits[:3], digits[3:6]
    if area == exch:                        # 722-722-xxxx — repeated-pattern garbage
        return None
    if len(set(digits)) == 1:              # all same digit
        return None
    if area[0] in ("0", "1") or exch[0] in ("0", "1"):
        return None
    return s.strip()


def _has_prov_field(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> bool:
    """Return True if a non-empty provenance row already exists."""
    try:
        row = cur.execute(
            """
            SELECT 1 FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
            LIMIT 1
            """,
            (lead_key, field_name),
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _prov_field_fresh(
    cur: sqlite3.Cursor, lead_key: str, field_name: str, max_age_days: int = 30
) -> bool:
    """Return True if a non-empty provenance row exists AND is younger than max_age_days."""
    try:
        row = cur.execute(
            """
            SELECT 1 FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
              AND created_at > datetime('now', ?)
            LIMIT 1
            """,
            (lead_key, field_name, f"-{max_age_days} days"),
        ).fetchone()
        return row is not None
    except Exception:
        return False


def _read_prov_field(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> Optional[str]:
    """Return the latest non-empty provenance value, or None."""
    try:
        row = cur.execute(
            """
            SELECT field_value_text FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
            ORDER BY created_at DESC LIMIT 1
            """,
            (lead_key, field_name),
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception:
        return None


def _write_prov_field(
    cur: sqlite3.Cursor,
    lead_key: str,
    field_name: str,
    value: str,
    source_channel: str,
    run_id: Optional[str],
    created_at: str,
) -> bool:
    """
    Insert a synthetic provenance row (artifact_id=NULL — no raw artifact).
    Returns True if inserted successfully.
    """
    try:
        cur.execute(
            """
            INSERT INTO lead_field_provenance
                (lead_key, field_name, value_type, field_value_text,
                 units, confidence, source_channel, artifact_id,
                 retrieved_at, run_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lead_key, field_name, "raw", value,
                None, None, source_channel, None,
                created_at, run_id, created_at,
            ),
        )
        return True
    except Exception:
        return False


def _get_best_firm(fields: Dict[str, Any]) -> Optional[str]:
    """
    Return the best trustee firm name from already-hydrated fields.
    Priority: ft_trustee_firm > notice_trustee_firm > ft_trustee_name_raw > notice_trustee_name_raw
    """
    for k in (
        "ft_trustee_firm", "notice_trustee_firm",
        "ft_trustee_name_raw", "notice_trustee_name_raw",
        "trustee_attorney",
    ):
        v = (fields.get(k) or "").strip()
        if v:
            return v
    return None


def _get_owner_address(fields: Dict[str, Any]) -> Optional[str]:
    """Build a full address string suitable for skip trace."""
    addr  = (fields.get("address") or "").strip()
    if not addr:
        return None
    state  = (fields.get("state")  or "TN").strip()
    if re.search(r"\b[A-Z]{2}\b", addr) and re.search(r"\b\d{5}(?:-\d{4})?\b", addr):
        return addr
    return f"{addr}, {state}"


def _write_optional_prov_field(
    cur: sqlite3.Cursor,
    lead_key: str,
    field_name: str,
    value: Optional[str],
    source_channel: str,
    run_id: Optional[str],
    created_at: str,
) -> bool:
    if value is None or str(value).strip() == "":
        return False
    return _write_prov_field(cur, lead_key, field_name, str(value).strip(), source_channel, run_id, created_at)


def _latest_attom_raw_json(cur: sqlite3.Cursor, lead_key: str) -> Optional[str]:
    try:
        row = cur.execute(
            """
            SELECT attom_raw_json
            FROM attom_enrichments
            WHERE lead_key=? AND attom_raw_json IS NOT NULL
            ORDER BY enriched_at DESC, id DESC
            LIMIT 1
            """,
            (lead_key,),
        ).fetchone()
        return str(row[0]) if row and row[0] else None
    except Exception:
        return None


def _attom_owner_context(raw_json: Optional[str]) -> Dict[str, Optional[str]]:
    if not raw_json:
        return {}
    try:
        blob = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
        if not isinstance(blob, dict):
            return {}
    except Exception:
        return {}

    owner_blob = blob.get("owner")
    owner_name = None
    owner_mail = None
    subject_one_line = None
    if isinstance(owner_blob, dict):
        owner_core = owner_blob.get("owner") or owner_blob
        if isinstance(owner_core, dict):
            owner1 = owner_core.get("owner1") or {}
            if isinstance(owner1, dict):
                owner_name = (
                    owner1.get("fullname")
                    or owner1.get("fullName")
                    or " ".join(
                        part for part in [
                            owner1.get("firstnameandmi") or owner1.get("firstName"),
                            owner1.get("lastname") or owner1.get("lastName"),
                        ]
                        if part
                    ).strip()
                    or None
                )
            owner_mail = (
                owner_core.get("mailingaddressoneline")
                or ((owner_core.get("mailAddress") or {}).get("oneLine") if isinstance(owner_core.get("mailAddress"), dict) else None)
                or owner_blob.get("mailingaddressoneline")
            )

    address_blob = blob.get("address")
    if isinstance(address_blob, dict):
        subject_one_line = address_blob.get("oneLine")
        if not subject_one_line:
            line1 = str(address_blob.get("line1") or "").strip()
            line2 = str(address_blob.get("line2") or "").strip()
            if line1 and line2:
                subject_one_line = f"{line1}, {line2}"
            elif line1:
                subject_one_line = line1

    return {
        "owner_name": str(owner_name).strip() if owner_name else None,
        "owner_mail": str(owner_mail).strip() if owner_mail else None,
        "subject_address": str(subject_one_line).strip() if subject_one_line else None,
    }


def _normalized_sale_status(fields: Dict[str, Any]) -> str:
    return str(fields.get("sale_status") or "").strip().lower()


def _is_homeowner_contact_lane(fields: Dict[str, Any]) -> bool:
    sale_status = _normalized_sale_status(fields)
    distress_type = str(fields.get("distress_type") or "").strip().upper()
    return sale_status == "pre_foreclosure" or distress_type == "FSBO"


def _refresh_contact_lane_fields(
    cur: sqlite3.Cursor,
    lead_key: str,
    fields: Dict[str, Any],
    run_id: Optional[str],
    created_at: str,
) -> None:
    homeowner_lane = _is_homeowner_contact_lane(fields)
    notice_phone = _sanitize_phone(
        _read_prov_field(cur, lead_key, "notice_phone") or (fields.get("notice_phone") or None)
    )
    trustee_phone = _sanitize_phone(
        _read_prov_field(cur, lead_key, "trustee_phone_public") or (fields.get("trustee_phone_public") or None)
    )
    sale_controller_name = _get_best_firm(fields) or _read_prov_field(cur, lead_key, "notice_law_firm")
    sale_controller_primary = notice_phone or trustee_phone
    sale_controller_secondary = None
    if notice_phone and trustee_phone and trustee_phone != notice_phone:
        sale_controller_secondary = trustee_phone

    contact_target_role = "HOMEOWNER" if homeowner_lane else "SALE_CONTROLLER"
    fields["contact_target_role"] = contact_target_role
    _write_prov_field(cur, lead_key, "contact_target_role", contact_target_role, "ContactEnricher", run_id, created_at)

    if sale_controller_name:
        fields["sale_controller_contact_name"] = sale_controller_name
        _write_prov_field(
            cur, lead_key, "sale_controller_contact_name", sale_controller_name, "ContactEnricher", run_id, created_at
        )
    if sale_controller_primary:
        fields["sale_controller_phone_primary"] = sale_controller_primary
        _write_prov_field(
            cur, lead_key, "sale_controller_phone_primary", sale_controller_primary, "ContactEnricher", run_id, created_at
        )
    if sale_controller_secondary:
        fields["sale_controller_phone_secondary"] = sale_controller_secondary
        _write_prov_field(
            cur, lead_key, "sale_controller_phone_secondary", sale_controller_secondary, "ContactEnricher", run_id, created_at
        )
    if sale_controller_primary:
        source_bits = []
        if notice_phone:
            source_bits.append("notice_phone")
        if trustee_phone:
            source_bits.append("trustee_phone_public")
        sale_controller_source = "+".join(source_bits) if source_bits else "sale_controller"
        fields["sale_controller_contact_source"] = sale_controller_source
        _write_prov_field(
            cur, lead_key, "sale_controller_contact_source", sale_controller_source, "ContactEnricher", run_id, created_at
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def enrich_contact_data(
    lead_key: str,
    fields: Dict[str, Any],
    cur: sqlite3.Cursor,
) -> Dict[str, int]:
    """
    Run Tier-2 and Tier-3 contact enrichment for a single lead.

    Mutates `fields` in-place so callers can immediately use new values.
    Writes results to lead_field_provenance via `cur` (caller must commit).
    Returns {"t2_written": N, "t3_written": N, "errors": N}.
    """
    run_id     = os.environ.get("FALCO_RUN_ID")
    created_at = _now_iso()
    summary: Dict[str, int] = {"t2_written": 0, "t3_written": 0, "errors": 0}

    # ── Tier 2: Trustee firm public phone ────────────────────────────────────
    if not _has_prov_field(cur, lead_key, "trustee_phone_public"):
        try:
            firm = _get_best_firm(fields)
            if firm:
                provider  = get_trustee_phone_provider()
                raw_phone = provider.lookup(firm)
                clean     = _sanitize_phone(raw_phone) if raw_phone else None
                if clean:
                    if _write_prov_field(
                        cur, lead_key, "trustee_phone_public", clean,
                        "TrusteePhoneTable", run_id, created_at,
                    ):
                        summary["t2_written"] += 1
                    if _write_prov_field(
                        cur, lead_key, "trustee_phone_source", "firm_lookup",
                        "TrusteePhoneTable", run_id, created_at,
                    ):
                        pass  # metadata field — not counted separately
                    fields["trustee_phone_public"] = clean
                    fields["trustee_phone_source"] = "firm_lookup"
        except Exception:
            summary["errors"] += 1
    else:
        # Already written — load into fields for downstream use
        v = _read_prov_field(cur, lead_key, "trustee_phone_public")
        if v:
            fields.setdefault("trustee_phone_public", v)

    # ── Tier 3: Owner skip trace ──────────────────────────────────────────────
    # Re-trace if phone data is older than 30 days (stale numbers erode trust)
    has_owner_phone = _prov_field_fresh(cur, lead_key, "owner_phone_primary", max_age_days=30)
    has_owner_dnc = _prov_field_fresh(cur, lead_key, "owner_phone_dnc_status", max_age_days=30)
    if not has_owner_phone or not has_owner_dnc:
        try:
            attom_owner = _attom_owner_context(_latest_attom_raw_json(cur, lead_key))
            if not fields.get("owner_name") and attom_owner.get("owner_name"):
                fields["owner_name"] = attom_owner["owner_name"]
                _write_optional_prov_field(
                    cur, lead_key, "owner_name", attom_owner["owner_name"], "ATTOMOwner", run_id, created_at
                )
            if not fields.get("owner_mail") and attom_owner.get("owner_mail"):
                fields["owner_mail"] = attom_owner["owner_mail"]
                _write_optional_prov_field(
                    cur, lead_key, "owner_mail", attom_owner["owner_mail"], "ATTOMOwner", run_id, created_at
                )

            owner_name = (fields.get("owner_name") or attom_owner.get("owner_name") or None)
            address = _get_owner_address(fields) or attom_owner.get("subject_address")
            mailing_address = str(fields.get("owner_mail") or attom_owner.get("owner_mail") or "").strip() or None
            if address:
                provider = get_skip_trace_provider()
                result   = provider.trace(address, owner_name=owner_name)
                if (
                    not result.owner_phone_primary
                    and not result.owner_phone_secondary
                    and not result.owner_phone_dnc_status
                    and mailing_address
                    and mailing_address != address
                ):
                    result = provider.trace(mailing_address, owner_name=owner_name)
                _t3_any  = False

                if result.owner_phone_primary:
                    clean = _sanitize_phone(result.owner_phone_primary)
                    if clean:
                        if _write_prov_field(
                            cur, lead_key, "owner_phone_primary", clean,
                            result.owner_phone_source or "SkipTrace", run_id, created_at,
                        ):
                            summary["t3_written"] += 1
                        fields["owner_phone_primary"] = clean
                        _t3_any = True

                if result.owner_phone_secondary:
                    clean2 = _sanitize_phone(result.owner_phone_secondary)
                    if clean2:
                        if _write_prov_field(
                            cur, lead_key, "owner_phone_secondary", clean2,
                            result.owner_phone_source or "SkipTrace", run_id, created_at,
                        ):
                            summary["t3_written"] += 1
                        fields["owner_phone_secondary"] = clean2

                src = result.owner_phone_source or "SkipTrace"
                if _t3_any or result.owner_phone_dnc_status:
                    _write_prov_field(
                        cur, lead_key, "owner_phone_source", src,
                        src, run_id, created_at,
                    )
                    if result.owner_phone_confidence:
                        _write_prov_field(
                            cur, lead_key, "owner_phone_confidence",
                            result.owner_phone_confidence,
                            src, run_id, created_at,
                        )
                    fields["owner_phone_source"] = src
                if result.owner_phone_primary_dnc is not None:
                    _write_optional_prov_field(
                        cur,
                        lead_key,
                        "owner_phone_dnc_primary",
                        "1" if result.owner_phone_primary_dnc else "0",
                        src,
                        run_id,
                        created_at,
                    )
                    fields["owner_phone_dnc_primary"] = "1" if result.owner_phone_primary_dnc else "0"
                if result.owner_phone_secondary_dnc is not None:
                    _write_optional_prov_field(
                        cur,
                        lead_key,
                        "owner_phone_dnc_secondary",
                        "1" if result.owner_phone_secondary_dnc else "0",
                        src,
                        run_id,
                        created_at,
                    )
                    fields["owner_phone_dnc_secondary"] = "1" if result.owner_phone_secondary_dnc else "0"
                if result.owner_phone_dnc_status:
                    _write_optional_prov_field(
                        cur,
                        lead_key,
                        "owner_phone_dnc_status",
                        result.owner_phone_dnc_status,
                        src,
                        run_id,
                        created_at,
                    )
                    fields["owner_phone_dnc_status"] = result.owner_phone_dnc_status
        except Exception:
            summary["errors"] += 1
    else:
        # Already written — load into fields
        for k in (
            "owner_phone_primary",
            "owner_phone_secondary",
            "owner_phone_source",
            "owner_phone_confidence",
            "owner_phone_dnc_primary",
            "owner_phone_dnc_secondary",
            "owner_phone_dnc_status",
        ):
            v = _read_prov_field(cur, lead_key, k)
            if v:
                fields.setdefault(k, v)

    _refresh_contact_lane_fields(cur, lead_key, fields, run_id, created_at)

    # ── contact_ready flag (computed — overwritten every run) ─────────────────
    # A lead is contact_ready ONLY when at least one usable phone exists.
    # Trustee name / firm are useful for packet display but are NOT sufficient.
    _notice_phone_ok = bool(
        _sanitize_phone(_read_prov_field(cur, lead_key, "notice_phone"))
    )
    _t2_phone_ok = bool(
        _sanitize_phone((fields.get("trustee_phone_public") or "").strip() or None)
    )
    _t3_primary_ok = bool(
        _sanitize_phone((fields.get("owner_phone_primary") or "").strip() or None)
    )
    _t3_secondary_ok = bool(
        _sanitize_phone((fields.get("owner_phone_secondary") or "").strip() or None)
    )

    _sale_controller_primary_ok = bool(
        _sanitize_phone((fields.get("sale_controller_phone_primary") or "").strip() or None)
    )
    _sale_controller_secondary_ok = bool(
        _sanitize_phone((fields.get("sale_controller_phone_secondary") or "").strip() or None)
    )

    homeowner_lane = _is_homeowner_contact_lane(fields)
    if homeowner_lane:
        contact_ready = _t3_primary_ok or _t3_secondary_ok
    else:
        contact_ready = (
            _sale_controller_primary_ok
            or _sale_controller_secondary_ok
            or _notice_phone_ok
            or _t2_phone_ok
        )
    contact_ready_val = "1" if contact_ready else "0"

    try:
        # Remove stale value then re-insert (computed field — always fresh)
        cur.execute(
            """
            DELETE FROM lead_field_provenance
            WHERE lead_key=? AND field_name='contact_ready' AND source_channel='ContactEnricher'
            """,
            (lead_key,),
        )
        _write_prov_field(
            cur, lead_key, "contact_ready", contact_ready_val,
            "ContactEnricher", run_id, created_at,
        )
        fields["contact_ready"] = contact_ready
    except Exception:
        # Failed to refresh — be conservative: mark as NOT ready rather than
        # preserving a potentially stale True value.
        fields["contact_ready"] = False
        summary["errors"] += 1

    return summary
