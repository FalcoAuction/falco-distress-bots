# src/bots/propstream_bot.py
#
# PropStream CSV-export ingestion bot.
#
# PropStream aggregates pre-foreclosure, auction, tax lien, and REO data
# across the US. This bot ingests CSV exports from a PropStream subscription,
# mapping columns to FALCO schema and storing rich provenance.
#
# Usage:
#   1. Export a filtered list from PropStream as CSV
#   2. Set FALCO_ENABLE_PROPSTREAM=1
#   3. Set FALCO_PROPSTREAM_SEED_FILE=/path/to/export.csv
#   4. Run pipeline
#
# The bot handles PropStream's varying column-name conventions across export
# versions by using fuzzy header matching.

from __future__ import annotations

import csv
import json
import os
from datetime import datetime, timezone
from typing import Any, Optional

from ..gating.convertibility import apply_convertibility_gate
from ..scoring.days_to_sale import days_to_sale
from ..settings import get_dts_window, is_allowed_county, normalize_county_full, within_target_counties
from ..storage import sqlite_store as _store
from ..utils import make_lead_key
from .record_seed_utils import parse_date_flex


SOURCE = "PROPSTREAM"

# PropStream DTS window is wide — many rows are pre-foreclosure without
# an imminent sale date.  Env-overridable via FALCO_PROPSTREAM_DTS_MIN/MAX.
_DTS_MIN = int(os.getenv("FALCO_PROPSTREAM_DTS_MIN", "0"))
_DTS_MAX = int(os.getenv("FALCO_PROPSTREAM_DTS_MAX", "180"))


# ---------------------------------------------------------------------------
# Column mapping — PropStream exports use varying header names
# ---------------------------------------------------------------------------

# Maps FALCO field name -> list of possible CSV header labels (case-insensitive)
_COLUMN_ALIASES: dict[str, list[str]] = {
    "address":              ["address", "property address", "site address", "street address"],
    "city":                 ["city", "property city", "site city"],
    "state":                ["state", "property state", "site state", "st"],
    "zip":                  ["zip", "property zip", "site zip", "zip code", "zipcode"],
    "county":               ["county", "property county"],
    "property_type":        ["property type", "prop type", "type"],
    "beds":                 ["beds", "bedrooms", "br", "bed"],
    "baths":                ["baths", "bathrooms", "ba", "bath"],
    "sqft":                 ["sqft", "sq ft", "building sq ft", "living sq ft", "square feet",
                             "building area", "gross living area"],
    "year_built":           ["year built", "yr built", "yearbuilt"],
    "est_value":            ["est. value", "est value", "estimated value", "avm", "value"],
    "owner_name":           ["owner name", "owner 1 name", "owner", "owner 1 first"],
    "owner_mailing":        ["mailing address", "owner mailing address", "mail address"],
    "owner_mailing_city":   ["mailing city", "owner mailing city", "mail city"],
    "owner_mailing_state":  ["mailing state", "owner mailing state", "mail state"],
    "owner_mailing_zip":    ["mailing zip", "owner mailing zip", "mail zip"],
    "foreclosure_status":   ["foreclosure status", "fc status", "pre-foreclosure status",
                             "foreclosure"],
    "sale_date":            ["auction date", "sale date", "foreclosure sale date",
                             "trustee sale date"],
    "mortgage_balance":     ["mortgage balance", "est. mortgage balance", "est mortgage balance",
                             "loan balance", "1st mortgage balance"],
    "equity_pct":           ["equity %", "equity percent", "est equity %", "est. equity %",
                             "equity"],
    "phone":                ["phone", "owner phone", "phone 1"],
    "last_sale_date":       ["last sale date", "last transfer date"],
    "last_sale_price":      ["last sale price", "last transfer amount"],
    "tax_delinquent":       ["tax delinquent", "tax delinquent flag", "delinquent taxes"],
    "vacant":               ["vacant", "vacancy", "vacant flag"],
    "absentee_owner":       ["absentee owner", "absentee", "owner occupied"],
    "lender":               ["lender", "mortgage lender", "1st mortgage lender"],
}


def _detect_columns(headers: list[str]) -> dict[str, int]:
    """Match actual CSV headers to FALCO fields. Returns {falco_field: column_index}."""
    mapping: dict[str, int] = {}
    headers_lower = [h.strip().lower() for h in headers]

    for falco_field, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias in headers_lower:
                mapping[falco_field] = headers_lower.index(alias)
                break

    return mapping


def _get_field(row: list[str], col_map: dict[str, int], field: str) -> str:
    """Safely get a field from a row using the column mapping."""
    idx = col_map.get(field)
    if idx is None or idx >= len(row):
        return ""
    return row[idx].strip()


def _parse_float(s: str) -> Optional[float]:
    """Parse a numeric string, handling currency symbols and commas."""
    if not s:
        return None
    cleaned = s.replace("$", "").replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def _determine_distress_type(row: list[str], col_map: dict[str, int]) -> str:
    """Determine distress type from PropStream status fields."""
    fc_status = _get_field(row, col_map, "foreclosure_status").lower()
    tax_flag = _get_field(row, col_map, "tax_delinquent").lower()

    if fc_status and ("pre-foreclosure" in fc_status or "pre foreclosure" in fc_status
                      or "notice of default" in fc_status or "lis pendens" in fc_status):
        return "PRE_FORECLOSURE"
    if fc_status and ("auction" in fc_status or "scheduled" in fc_status
                      or "notice of sale" in fc_status):
        return "FORECLOSURE"
    if tax_flag in ("yes", "true", "1", "y"):
        return "TAX_DELINQUENT"
    return "PROPSTREAM"


def _build_full_address(row: list[str], col_map: dict[str, int]) -> str:
    """Build a full address string from component fields."""
    parts = []
    addr = _get_field(row, col_map, "address")
    city = _get_field(row, col_map, "city")
    state = _get_field(row, col_map, "state")
    zip_code = _get_field(row, col_map, "zip")

    if addr:
        parts.append(addr)
    if city:
        parts.append(city)
    if state:
        parts.append(state)
    if zip_code:
        parts.append(zip_code)

    return ", ".join(parts) if parts else ""


def _write_provenance(
    lead_key: str,
    row: list[str],
    col_map: dict[str, int],
    retrieved_at: str,
) -> None:
    """Store rich provenance from PropStream data."""
    # Numeric fields
    for field, prov_name, units in [
        ("beds", "beds", None),
        ("baths", "baths", None),
        ("sqft", "building_area_sqft", "sqft"),
        ("year_built", "year_built", None),
        ("est_value", "propstream_est_value", "USD"),
        ("mortgage_balance", "propstream_mortgage_balance", "USD"),
        ("equity_pct", "propstream_equity_pct", "pct"),
        ("last_sale_price", "last_sale_price", "USD"),
    ]:
        val = _parse_float(_get_field(row, col_map, field))
        if val is not None:
            _store.insert_provenance_num(
                lead_key, prov_name, val, units, 0.85, SOURCE, None, retrieved_at,
            )

    # Text fields
    for field, prov_name, confidence in [
        ("owner_name", "owner_name", 0.85),
        ("property_type", "property_type", 0.9),
        ("lender", "mortgage_lender", 0.8),
        ("phone", "owner_phone_primary", 0.7),
        ("foreclosure_status", "propstream_fc_status", 0.9),
    ]:
        val = _get_field(row, col_map, field)
        if val:
            _store.insert_provenance_text(
                lead_key, prov_name, val, SOURCE,
                retrieved_at=retrieved_at, artifact_id=None, confidence=confidence,
            )

    # Mailing address (combine parts)
    mail_parts = []
    for f in ("owner_mailing", "owner_mailing_city", "owner_mailing_state", "owner_mailing_zip"):
        v = _get_field(row, col_map, f)
        if v:
            mail_parts.append(v)
    if mail_parts:
        _store.insert_provenance_text(
            lead_key, "owner_mail", ", ".join(mail_parts), SOURCE,
            retrieved_at=retrieved_at, artifact_id=None, confidence=0.85,
        )

    # Last sale date
    last_sale = _get_field(row, col_map, "last_sale_date")
    if last_sale:
        parsed = parse_date_flex(last_sale)
        if parsed:
            _store.insert_provenance_text(
                lead_key, "last_sale_date", parsed, SOURCE,
                retrieved_at=retrieved_at, artifact_id=None, confidence=0.85,
            )

    # Contact-ready flag if we have a phone
    if _get_field(row, col_map, "phone"):
        _store.insert_provenance_text(
            lead_key, "contact_ready", "true", SOURCE,
            retrieved_at=retrieved_at, artifact_id=None, confidence=0.7,
        )

    # Distress signal flags
    vacant_flag = _get_field(row, col_map, "vacant").lower()
    if vacant_flag in ("yes", "true", "1", "y"):
        _store.insert_provenance_text(
            lead_key, "propstream_vacant", "true", SOURCE,
            retrieved_at=retrieved_at, artifact_id=None, confidence=0.8,
        )

    absentee_flag = _get_field(row, col_map, "absentee_owner").lower()
    if absentee_flag in ("yes", "true", "1", "y", "absentee"):
        _store.insert_provenance_text(
            lead_key, "propstream_absentee", "true", SOURCE,
            retrieved_at=retrieved_at, artifact_id=None, confidence=0.8,
        )


def run():
    enabled = os.getenv("FALCO_ENABLE_PROPSTREAM", "0").strip() == "1"
    if not enabled:
        print("[PropStreamBot] Disabled — set FALCO_ENABLE_PROPSTREAM=1 and provide a CSV seed file.")
        return {"status": "disabled"}

    seed_file = os.environ.get("FALCO_PROPSTREAM_SEED_FILE")
    if not seed_file or not os.path.isfile(seed_file):
        print(f"[PropStreamBot] No seed file found — set FALCO_PROPSTREAM_SEED_FILE. Got: {seed_file}")
        return {"status": "no_seed_file"}

    # Read CSV
    with open(seed_file, "r", encoding="utf-8-sig") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            print("[PropStreamBot] CSV has no headers.")
            return {"status": "empty_csv"}
        rows = list(reader)

    col_map = _detect_columns(headers)
    mapped_fields = sorted(col_map.keys())
    print(f"[PropStreamBot] CSV has {len(rows)} rows, {len(headers)} columns.")
    print(f"[PropStreamBot] Mapped fields: {mapped_fields}")

    if "address" not in col_map:
        print("[PropStreamBot] ERROR: Could not find an address column in CSV headers.")
        print(f"[PropStreamBot] Available headers: {headers}")
        return {"status": "no_address_column", "headers": headers}

    retrieved_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    # Counters
    valid_rows = 0
    no_address = 0
    geo_skipped = 0
    dts_skipped = 0
    gate_skipped = 0
    dedupe_skipped = 0
    stored_leads = 0
    stored_ingests = 0
    provenance_rows = 0
    seen_in_run: set[str] = set()
    distress_type_counts: dict[str, int] = {}

    for row in rows:
        address = _get_field(row, col_map, "address")
        if not address:
            no_address += 1
            continue

        # Build full address
        full_address = _build_full_address(row, col_map)
        county_raw = _get_field(row, col_map, "county")
        county = normalize_county_full(county_raw) if county_raw else None
        state = _get_field(row, col_map, "state") or "TN"

        if not county:
            # Try to proceed without county — some PropStream exports omit it
            no_address += 1
            continue

        valid_rows += 1

        # Geography gate
        if not is_allowed_county(county) or not within_target_counties(county):
            geo_skipped += 1
            continue

        # DTS gate — only enforce if sale_date is present
        sale_date_raw = _get_field(row, col_map, "sale_date")
        sale_date = parse_date_flex(sale_date_raw) if sale_date_raw else None

        if sale_date:
            dts = days_to_sale(sale_date)
            if dts is not None and (dts < _DTS_MIN or dts > _DTS_MAX):
                dts_skipped += 1
                continue

        # Determine distress type
        distress_type = _determine_distress_type(row, col_map)
        distress_type_counts[distress_type] = distress_type_counts.get(distress_type, 0) + 1

        # Convertibility gate
        payload = {
            "address": full_address,
            "county": county,
            "state": state,
            "distress_type": distress_type,
            "source": SOURCE,
            "raw": {headers[i]: row[i] for i in range(min(len(headers), len(row)))},
        }

        decision = apply_convertibility_gate(payload)
        if isinstance(decision, tuple):
            keep = bool(decision[0])
        elif isinstance(decision, dict) and "keep" in decision:
            keep = bool(decision["keep"])
        else:
            keep = bool(decision) if isinstance(decision, bool) else True
        if not keep:
            gate_skipped += 1
            continue

        # Dedupe
        lead_key = make_lead_key(SOURCE, county, address)
        if lead_key in seen_in_run:
            dedupe_skipped += 1
            continue
        seen_in_run.add(lead_key)

        # Store lead
        lead_attrs = {"address": full_address, "state": state}
        if _store.upsert_lead(lead_key, lead_attrs, county, distress_type=distress_type):
            stored_leads += 1

        # Store ingest event
        ingest_payload = {
            "address": full_address,
            "county": county,
            "state": state,
            "sale_date": sale_date,
            "distress_type": distress_type,
            "channel": SOURCE,
        }
        if _store.insert_ingest_event(lead_key, SOURCE, None, sale_date, json.dumps(ingest_payload)):
            stored_ingests += 1

        # Write rich provenance
        _write_provenance(lead_key, row, col_map, retrieved_at)
        provenance_rows += 1

    summary = {
        "seed_rows": len(rows),
        "valid_rows": valid_rows,
        "no_address": no_address,
        "geo_skipped": geo_skipped,
        "dts_skipped": dts_skipped,
        "gate_skipped": gate_skipped,
        "dedupe_skipped": dedupe_skipped,
        "stored_leads": stored_leads,
        "stored_ingests": stored_ingests,
        "provenance_rows": provenance_rows,
        "distress_types": distress_type_counts,
        "mapped_columns": len(col_map),
    }
    print(f"[PropStreamBot] summary {summary}")
    return summary
