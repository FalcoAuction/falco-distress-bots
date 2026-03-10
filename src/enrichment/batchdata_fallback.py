from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from .contact_enricher import enrich_contact_data
from ..packaging.data_quality import assess_packet_data
from ..settings import is_allowed_county, within_target_counties
from ..storage import sqlite_store as _store

_VERIFY_URL = "https://api.batchdata.com/api/v1/address/verify"
_LOOKUP_URL = "https://api.batchdata.com/api/v1/property/lookup/all-attributes"
_FALLBACK_SOURCE = "BATCHDATA"
_TARGET_FIELDS = (
    "owner_name",
    "owner_mail",
    "last_sale_date",
    "mortgage_lender",
    "property_identifier",
    "year_built",
    "building_area_sqft",
    "beds",
    "baths",
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in ("", "None", "null", "Unavailable", "—")
    return True


def _load_json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _extract_attom_fields(raw_json: Any) -> Dict[str, Any]:
    blob = _load_json(raw_json)
    out: Dict[str, Any] = {}

    owner_blob = blob.get("owner")
    if isinstance(owner_blob, dict):
        owner = owner_blob.get("owner") or {}
        if isinstance(owner, dict):
            owner1 = owner.get("owner1") or {}
            if isinstance(owner1, dict):
                parts = [
                    str(owner1.get(k) or "").strip()
                    for k in ("firstnameandmi", "lastname")
                    if str(owner1.get(k) or "").strip()
                ]
                out["owner_name"] = " ".join(parts) or None
            out["owner_mail"] = owner.get("mailingaddressoneline") or None
        sale = owner_blob.get("sale") or {}
        if isinstance(sale, dict):
            out["last_sale_date"] = sale.get("saleTransDate") or None

    mortgage_blob = blob.get("mortgage")
    if isinstance(mortgage_blob, dict):
        lender = mortgage_blob.get("lender") or {}
        if isinstance(lender, dict):
            out["mortgage_lender"] = lender.get("name") or None

    detail = blob.get("detail") if isinstance(blob.get("detail"), dict) else blob
    if isinstance(detail, dict):
        ident = detail.get("identifier") or {}
        building = detail.get("building") or {}
        vintage = detail.get("vintage") or {}
        rooms = (building.get("rooms") if isinstance(building, dict) else {}) or {}
        size = (building.get("size") if isinstance(building, dict) else {}) or {}
        out["property_identifier"] = ident.get("apn") or ident.get("attomId") or ident.get("fips")
        out["year_built"] = vintage.get("yearBuilt")
        out["building_area_sqft"] = size.get("livingSize") or size.get("bldgSize")
        out["beds"] = rooms.get("beds") or rooms.get("bedsCount")
        out["baths"] = rooms.get("bathsTotal") or rooms.get("bathsFull")

    return out


def _latest_prov_map(cur: sqlite3.Cursor, lead_key: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for field_name in _TARGET_FIELDS:
        row = cur.execute(
            """
            SELECT field_value_text, field_value_num
            FROM lead_field_provenance
            WHERE lead_key=? AND field_name=?
            ORDER BY created_at DESC, prov_id DESC
            LIMIT 1
            """,
            (lead_key, field_name),
        ).fetchone()
        if not row:
            continue
        if row[0] is not None:
            out[field_name] = row[0]
        elif row[1] is not None:
            out[field_name] = row[1]
    return out


def _latest_text_field(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> Optional[str]:
    row = cur.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    if not row or row[0] is None:
        return None
    return str(row[0]).strip() or None


def _hydrate_contact_fields(cur: sqlite3.Cursor, lead_key: str, fields: Dict[str, Any]) -> None:
    for field_name in (
        "notice_phone",
        "ft_trustee_firm",
        "ft_trustee_name_raw",
        "notice_trustee_firm",
        "notice_trustee_name_raw",
        "trustee_attorney",
        "trustee_phone_public",
        "owner_phone_primary",
        "owner_phone_secondary",
        "contact_ready",
    ):
        value = _latest_text_field(cur, lead_key, field_name)
        if value and not _truthy(fields.get(field_name)):
            fields[field_name] = value

    rows = cur.execute(
        """
        SELECT raw_json
        FROM ingest_events
        WHERE lead_key=? AND raw_json IS NOT NULL
        ORDER BY id DESC
        LIMIT 5
        """,
        (lead_key,),
    ).fetchall()
    for row in rows:
        raw_json = row[0]
        if not raw_json:
            continue
        try:
            blob = json.loads(raw_json)
        except Exception:
            continue
        if not isinstance(blob, dict):
            continue
        for source_key, target_key in (
            ("trustee_firm", "ft_trustee_firm"),
            ("trustee_attorney", "trustee_attorney"),
            ("trustee", "ft_trustee_name_raw"),
            ("contact_info", "notice_trustee_name_raw"),
            ("phone", "notice_phone"),
        ):
            value = blob.get(source_key)
            if _truthy(value) and not _truthy(fields.get(target_key)):
                fields[target_key] = str(value).strip()


def _first_obj(payload: Dict[str, Any]) -> Dict[str, Any]:
    results = payload.get("results")
    if isinstance(results, dict):
        properties = results.get("properties")
        if isinstance(properties, list) and properties and isinstance(properties[0], dict):
            return properties[0]
        prop = results.get("property")
        if isinstance(prop, dict):
            return prop
    for key in ("data", "properties", "property"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0]
        if isinstance(value, dict):
            return value
    return payload


def _coerce_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    if len(s) == 8 and s.isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s


def _coerce_num(value: Any) -> Optional[float]:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, (int, float)):
            return float(value)
        return float(str(value).replace(",", "").replace("$", "").strip())
    except Exception:
        return None


def _extract_batchdata_fields(payload: Dict[str, Any]) -> Dict[str, Any]:
    item = _first_obj(payload)
    fields: Dict[str, Any] = {}

    owner_name = (
        _dig(item, "owner", "name")
        or _dig(item, "owner", "fullName")
        or item.get("ownerName")
        or _dig(item, "primaryOwner", "name")
    )
    owner_mail = (
        _dig(item, "owner", "mailingAddress", "full")
        or _format_address(_dig(item, "owner", "mailingAddress"))
        or _dig(item, "owner", "mailingAddress")
        or item.get("mailingAddress")
        or _format_address(_dig(item, "mailingAddress"))
    )
    last_sale_date = (
        _dig(item, "lastSale", "date")
        or _dig(item, "sale", "lastTransfer", "saleDate")
        or _dig(item, "sale", "lastSale", "saleDate")
        or item.get("lastSaleDate")
        or _dig(item, "saleHistory", "lastSaleDate")
        or _dig(item, "deed", "recordingDate")
        or _dig(item, "deedHistory", "recordingDate")
    )
    mortgage_lender = (
        _dig(item, "mortgage", "lenderName")
        or _dig(item, "mortgage", "lender", "name")
        or item.get("mortgageLender")
        or _dig(item, "openMortgage", "lenderName")
        or _dig(item, "openLien", "mortgages", "lenderName")
        or _dig(item, "openLien", "mortgages", 0, "lenderName")
        or _dig(item, "sale", "lastSale", "mortgages", 0, "lenderName")
        or _dig(item, "sale", "lastTransfer", "mortgages", 0, "lenderName")
    )
    property_identifier = (
        _dig(item, "parcel", "apn")
        or _dig(item, "ids", "apn")
        or item.get("apn")
        or item.get("parcelId")
        or item.get("assessorParcelNumber")
    )
    year_built = (
        item.get("yearBuilt")
        or _dig(item, "building", "yearBuilt")
        or _dig(item, "building", "effectiveYearBuilt")
        or _dig(item, "listing", "yearBuilt")
    )
    building_area_sqft = (
        item.get("buildingArea")
        or item.get("livingArea")
        or _dig(item, "building", "sqft")
        or _dig(item, "building", "livingArea")
        or _dig(item, "building", "totalBuildingAreaSquareFeet")
        or _dig(item, "listing", "totalBuildingAreaSquareFeet")
        or _dig(item, "listing", "livingArea")
    )
    beds = (
        item.get("beds")
        or _dig(item, "building", "beds")
        or _dig(item, "building", "bedroomCount")
        or _dig(item, "listing", "bedroomCount")
    )
    baths = (
        item.get("baths")
        or _dig(item, "building", "baths")
        or _dig(item, "building", "bathroomCount")
        or _dig(item, "building", "calculatedBathroomCount")
        or _dig(item, "listing", "bathroomCount")
    )

    if _truthy(owner_name):
        fields["owner_name"] = str(owner_name).strip()
    if _truthy(owner_mail):
        fields["owner_mail"] = str(owner_mail).strip()
    if _truthy(last_sale_date):
        fields["last_sale_date"] = _coerce_date(last_sale_date)
    if _truthy(mortgage_lender):
        fields["mortgage_lender"] = str(mortgage_lender).strip()
    if _truthy(property_identifier):
        fields["property_identifier"] = str(property_identifier).strip()

    for key, value in (
        ("year_built", year_built),
        ("building_area_sqft", building_area_sqft),
        ("beds", beds),
        ("baths", baths),
    ):
        numeric = _coerce_num(value)
        if numeric is not None:
            fields[key] = numeric

    return fields


def _dig(obj: Any, *path: Any) -> Any:
    cur = obj
    for key in path:
        if isinstance(key, int):
            if not isinstance(cur, list) or key >= len(cur):
                return None
            cur = cur[key]
            continue
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _format_address(value: Any) -> Optional[str]:
    if isinstance(value, str):
        return value.strip() or None
    if not isinstance(value, dict):
        return None
    parts = [
        str(value.get("street") or "").strip(),
        str(value.get("city") or "").strip(),
        str(value.get("state") or "").strip(),
        str(value.get("zip") or "").strip(),
    ]
    line1 = parts[0]
    locality = " ".join(p for p in parts[1:] if p)
    out = ", ".join(p for p in (line1, locality) if p)
    return out or None


def _split_address(raw: str, fallback_state: str) -> Dict[str, str]:
    text = " ".join((raw or "").strip().split()).rstrip(",")
    if not text:
        return {}
    match = re.match(
        r"^(?P<street>.+?),\s*(?P<city>[^,]+),\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        text,
    )
    if match:
        return {k: v.strip() for k, v in match.groupdict().items()}

    match = re.match(
        r"^(?P<street>.+?)\s+(?P<city>[A-Za-z .'-]+),\s*(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        text,
    )
    if match:
        return {k: v.strip() for k, v in match.groupdict().items()}

    match = re.match(
        r"^(?P<street>.+?)\s+(?P<city>[A-Za-z .'-]+)\s+(?P<state>[A-Z]{2})\s+(?P<zip>\d{5}(?:-\d{4})?)$",
        text,
    )
    if match:
        return {k: v.strip() for k, v in match.groupdict().items()}

    return {
        "street": text,
        "city": "",
        "state": (fallback_state or "TN").strip() or "TN",
        "zip": "",
    }


def _write_field(lead_key: str, field_name: str, value: Any, artifact_id: Optional[str], retrieved_at: str) -> bool:
    if isinstance(value, (int, float)):
        return _store.insert_provenance_num(
            lead_key=lead_key,
            field_name=field_name,
            value_num=float(value),
            units="count" if field_name in {"beds", "baths"} else None,
            confidence=None,
            source_channel=_FALLBACK_SOURCE,
            artifact_id=artifact_id,
            retrieved_at=retrieved_at,
        )
    return _store.insert_provenance_text(
        lead_key=lead_key,
        field_name=field_name,
        value_text=str(value),
        source_channel=_FALLBACK_SOURCE,
        retrieved_at=retrieved_at,
        artifact_id=artifact_id,
        confidence=None,
    )


def _call_batchdata(address: str, county: str, state: str) -> Dict[str, Any]:
    api_key = os.environ.get("FALCO_BATCHDATA_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Missing FALCO_BATCHDATA_API_KEY")

    parts = _split_address(address, state)
    if not parts.get("street"):
        raise RuntimeError("Missing structured street for BatchData lookup")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    verify_response = requests.post(
        os.environ.get("FALCO_BATCHDATA_VERIFY_URL", _VERIFY_URL).strip() or _VERIFY_URL,
        headers=headers,
        json={"requests": [parts]},
        timeout=25,
    )
    verify_response.raise_for_status()
    verify_payload = verify_response.json()
    addresses = _dig(verify_payload, "results", "addresses") or []
    if not isinstance(addresses, list) or not addresses:
        raise RuntimeError("BatchData address verify returned no addresses")
    verified = addresses[0] if isinstance(addresses[0], dict) else {}
    address_hash = str(verified.get("hash") or "").strip()
    if not address_hash:
        error = str(verified.get("error") or "BatchData address verify failed").strip()
        raise RuntimeError(error)

    lookup_response = requests.post(
        os.environ.get("FALCO_BATCHDATA_PROPERTY_URL", _LOOKUP_URL).strip() or _LOOKUP_URL,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={"requests": [{"hash": address_hash}]},
        timeout=25,
    )
    lookup_response.raise_for_status()
    lookup_payload = lookup_response.json()
    lookup_payload["_batchdata_verify"] = verify_payload
    return lookup_payload


def run() -> Dict[str, int]:
    enabled = os.environ.get("FALCO_ENABLE_BATCHDATA_FALLBACK", "").strip().lower() in {"1", "true", "yes", "y"}
    if not enabled:
        return {
            "requested": 0,
            "enriched_count": 0,
            "skipped_missing_address": 0,
            "skipped_already_complete": 0,
            "skipped_not_worth_it": 0,
            "errors": 0,
            "enabled": 0,
        }

    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    limit = int(os.environ.get("FALCO_MAX_BATCHDATA_ENRICH_PER_RUN", "6"))
    dts_min = int(os.environ.get("FALCO_DTS_MIN", "21"))
    dts_max = int(os.environ.get("FALCO_DTS_MAX", "90"))
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    rows = cur.execute(
        """
        WITH latest_attom AS (
          SELECT
            lead_key,
            attom_raw_json,
            avm_value,
            avm_low,
            avm_high,
            ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY enriched_at DESC, id DESC) AS rn
          FROM attom_enrichments
        )
        SELECT
          l.lead_key,
          l.address,
          l.county,
          l.state,
          l.distress_type,
          l.falco_score_internal,
          l.auction_readiness,
          l.equity_band,
          l.dts_days,
          la.attom_raw_json,
          la.avm_value,
          la.avm_low,
          la.avm_high
        FROM leads l
        LEFT JOIN latest_attom la
          ON la.lead_key = l.lead_key AND la.rn = 1
        WHERE l.dts_days IS NOT NULL
          AND l.dts_days BETWEEN ? AND ?
        ORDER BY
          CASE COALESCE(l.auction_readiness, '')
            WHEN 'GREEN' THEN 1
            WHEN 'YELLOW' THEN 2
            ELSE 3
          END,
          COALESCE(l.falco_score_internal, 0) DESC,
          l.dts_days ASC
        LIMIT ?
        """,
        (dts_min, dts_max, limit * 4),
    ).fetchall()

    summary = {
        "requested": 0,
        "enriched_count": 0,
        "contact_enriched_count": 0,
        "skipped_missing_address": 0,
        "skipped_already_complete": 0,
        "skipped_not_worth_it": 0,
        "errors": 0,
        "enabled": 1,
    }

    for row in rows:
        if summary["requested"] >= limit:
            break

        lead_key = str(row["lead_key"] or "").strip()
        address = str(row["address"] or "").strip()
        county = str(row["county"] or "").strip()
        state = str(row["state"] or "TN").strip() or "TN"
        if not address:
            summary["skipped_missing_address"] += 1
            continue
        if county and (not is_allowed_county(county) or not within_target_counties(county)):
            summary["skipped_not_worth_it"] += 1
            continue

        score = float(row["falco_score_internal"] or 0)
        readiness = str(row["auction_readiness"] or "").upper()
        if readiness not in {"GREEN", "YELLOW"} and score < 70:
            summary["skipped_not_worth_it"] += 1
            continue

        fields = dict(row)
        fields.update(_extract_attom_fields(row["attom_raw_json"]))
        fields.update(_latest_prov_map(cur, lead_key))
        _hydrate_contact_fields(cur, lead_key, fields)
        quality = assess_packet_data(fields)
        targets = quality["batchdata_fallback_targets"]
        needs_contact = "Actionable outreach path missing" in quality["execution_blockers"]
        if not targets and not needs_contact:
            summary["skipped_already_complete"] += 1
            continue

        try:
            retrieved_at = _now_iso()
            wrote_any = False
            artifact_id = None

            if targets:
                summary["requested"] += 1
                payload = _call_batchdata(address, county, state)
                artifact_ok, artifact_id = _store.insert_raw_artifact(
                    lead_key=lead_key,
                    channel=_FALLBACK_SOURCE,
                    source_url=os.environ.get("FALCO_BATCHDATA_PROPERTY_URL", _LOOKUP_URL),
                    retrieved_at=retrieved_at,
                    content_type="application/json",
                    payload_text=json.dumps(payload, ensure_ascii=False),
                    notes="fallback_property_detail",
                )
                if not artifact_ok:
                    artifact_id = None

                extracted = _extract_batchdata_fields(payload)
                for field_name in targets:
                    value = extracted.get(field_name)
                    if not _truthy(value):
                        continue
                    if _write_field(lead_key, field_name, value, artifact_id, retrieved_at):
                        wrote_any = True
                        fields[field_name] = value

            contact_written = False
            if needs_contact:
                contact_summary = enrich_contact_data(lead_key, fields, cur)
                if contact_summary.get("t2_written") or contact_summary.get("t3_written"):
                    contact_written = True
                    summary["contact_enriched_count"] += 1

            if wrote_any or contact_written:
                con.commit()
            if wrote_any:
                summary["enriched_count"] += 1
        except Exception as exc:
            print(f"[BatchDataFallback][WARN] lead_key={lead_key!r} error={type(exc).__name__}: {exc}")
            summary["errors"] += 1

    con.close()
    return summary
