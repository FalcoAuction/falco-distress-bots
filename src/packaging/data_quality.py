import json
from typing import Any, Dict, List, Optional


_PACKET_CRITICAL_FIELDS = {
    "address": "Address missing",
    "county": "County missing",
}

_VALUE_FIELDS = ("value_anchor_low", "value_anchor_mid", "value_anchor_high")

_VAULT_SIGNAL_FIELDS = {
    "falco_score_internal": "Falco score missing",
    "auction_readiness": "Auction readiness missing",
    "equity_band": "Equity band missing",
}

_PROPERTY_SNAPSHOT_FIELDS = {
    "property_type": "Property type missing",
    "property_identifier": "Parcel / APN missing",
    "city": "City missing",
    "zip": "ZIP missing",
    "year_built": "Year built missing",
    "building_area_sqft": "Living area missing",
    "beds": "Beds missing",
    "baths": "Baths missing",
}

_OWNERSHIP_FIELDS = {
    "owner_name": "Owner name missing",
    "owner_mail": "Owner mailing address missing",
    "last_sale_date": "Last transfer date missing",
    "mortgage_lender": "Mortgage lender missing",
}

_OUTREACH_FIELDS = {
    "trustee_phone_public": "Trustee phone missing",
    "owner_phone_primary": "Owner phone missing",
}

_EXECUTION_REQUIRED_FIELDS = {
    "owner_name": "Owner name missing",
    "owner_mail": "Owner mailing address missing",
    "last_sale_date": "Last transfer date missing",
    "mortgage_lender": "Mortgage lender missing",
}

_BATCHDATA_TARGET_FIELDS = frozenset({
    "owner_name",
    "owner_mail",
    "last_sale_date",
    "mortgage_lender",
    "property_identifier",
    "year_built",
    "building_area_sqft",
    "beds",
    "baths",
})


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in ("", "None", "null", "Unavailable", "—")
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _load_raw_json(raw_json: Any) -> Dict[str, Any]:
    if isinstance(raw_json, dict):
        return raw_json
    if not raw_json:
        return {}
    try:
        obj = json.loads(raw_json)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _extract_owner_mortgage(raw_json: Any) -> Dict[str, Optional[str]]:
    blob = _load_raw_json(raw_json)
    out: Dict[str, Optional[str]] = {
        "owner_name": None,
        "owner_mail": None,
        "last_sale_date": None,
        "mortgage_lender": None,
    }

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
            out["owner_mail"] = (
                owner.get("mailingaddressoneline")
                or (owner.get("mailAddress") or {}).get("oneLine")
                or None
            )

        sale = owner_blob.get("sale") or {}
        if isinstance(sale, dict):
            out["last_sale_date"] = sale.get("saleTransDate") or None
            if not out["last_sale_date"]:
                hist = sale.get("salesHistory")
                if isinstance(hist, list) and hist:
                    out["last_sale_date"] = (hist[0] or {}).get("saleRecDate") or None

    mortgage_blob = blob.get("mortgage")
    if isinstance(mortgage_blob, dict):
        lender = mortgage_blob.get("lender") or {}
        if isinstance(lender, dict):
            out["mortgage_lender"] = lender.get("name") or None

    return out


def _extract_property_detail(raw_json: Any) -> Dict[str, Any]:
    blob = _load_raw_json(raw_json)
    detail = blob.get("detail") if isinstance(blob.get("detail"), dict) else blob
    if not isinstance(detail, dict):
        return {}

    ident = detail.get("identifier") or {}
    summary = detail.get("summary") or {}
    building = detail.get("building") or {}
    vintage = detail.get("vintage") or {}
    address = detail.get("address") or {}
    rooms = (building.get("rooms") if isinstance(building, dict) else {}) or {}
    size = (building.get("size") if isinstance(building, dict) else {}) or {}

    return {
        "property_identifier": ident.get("apn") or ident.get("attomId") or ident.get("fips"),
        "property_type": summary.get("proptype") or summary.get("propClass"),
        "city": address.get("locality") or address.get("city"),
        "zip": address.get("postal1") or address.get("zip"),
        "year_built": vintage.get("yearBuilt"),
        "building_area_sqft": size.get("livingSize") or size.get("bldgSize"),
        "beds": (rooms or {}).get("beds") or (rooms or {}).get("bedsCount"),
        "baths": (rooms or {}).get("bathsTotal") or (rooms or {}).get("bathsFull"),
    }


def _truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _has_actionable_outreach(enriched: Dict[str, Any]) -> bool:
    if _truthy_flag(enriched.get("contact_ready")):
        return True

    return any(
        _present(enriched.get(key))
        for key in ("notice_phone", "trustee_phone_public", "owner_phone_primary", "owner_phone_secondary")
    )


def assess_packet_data(fields: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(fields)
    for key, value in _extract_property_detail(fields.get("attom_raw_json")).items():
        if not _present(enriched.get(key)) and _present(value):
            enriched[key] = value
    for key, value in _extract_owner_mortgage(fields.get("attom_raw_json")).items():
        if not _present(enriched.get(key)) and _present(value):
            enriched[key] = value

    critical_missing: List[str] = []
    for key, label in _PACKET_CRITICAL_FIELDS.items():
        if key == "sale_date" and (_present(enriched.get("sale_date")) or _present(enriched.get("sale_date_iso"))):
            continue
        if not _present(enriched.get(key)):
            critical_missing.append(label)

    if not (_present(enriched.get("dts_days")) or _present(enriched.get("sale_date")) or _present(enriched.get("sale_date_iso"))):
        critical_missing.append("Sale timing missing")

    distress_type = str(enriched.get("distress_type") or "").upper().strip()
    if distress_type != "LIS_PENDENS" and not any(_present(enriched.get(key)) for key in _VALUE_FIELDS):
        critical_missing.append("Valuation anchors missing")

    vault_signal_missing = [
        label for key, label in _VAULT_SIGNAL_FIELDS.items() if not _present(enriched.get(key))
    ]
    property_snapshot_missing = [
        label for key, label in _PROPERTY_SNAPSHOT_FIELDS.items() if not _present(enriched.get(key))
    ]
    ownership_missing = [
        label for key, label in _OWNERSHIP_FIELDS.items() if not _present(enriched.get(key))
    ]
    outreach_missing = [
        label for key, label in _OUTREACH_FIELDS.items() if not _present(enriched.get(key))
    ]
    execution_blockers = [
        label for key, label in _EXECUTION_REQUIRED_FIELDS.items() if not _present(enriched.get(key))
    ]

    distress_type = str(enriched.get("distress_type") or "").upper().strip()
    if distress_type in ("LIS_PENDENS", "FORECLOSURE", "FORECLOSURE_TN", "SOT", "SUBSTITUTION_OF_TRUSTEE"):
        if not _has_actionable_outreach(enriched):
            execution_blockers.append("Actionable outreach path missing")

    batchdata_targets = [
        key for key in _BATCHDATA_TARGET_FIELDS
        if not _present(enriched.get(key))
    ]

    total_checks = (
        len(_PACKET_CRITICAL_FIELDS)
        + 1
        + len(_VAULT_SIGNAL_FIELDS)
        + len(_PROPERTY_SNAPSHOT_FIELDS)
        + len(_OWNERSHIP_FIELDS)
    )
    missing_count = (
        len(critical_missing)
        + len(vault_signal_missing)
        + len(property_snapshot_missing)
        + len(ownership_missing)
    )
    completeness = max(0, round(((total_checks - missing_count) / total_checks) * 100))

    vault_blockers = critical_missing + vault_signal_missing + execution_blockers
    top_tier_ready = (
        len(vault_blockers) == 0
        and str(enriched.get("auction_readiness") or "").upper() == "GREEN"
        and _has_actionable_outreach(enriched)
        and not any(
            not _present(enriched.get(key))
            for key in ("property_identifier", "year_built", "building_area_sqft", "baths")
        )
    )

    return {
        "packet_completeness_pct": completeness,
        "vault_publish_ready": len(vault_blockers) == 0,
        "vault_publish_blockers": vault_blockers,
        "execution_blockers": execution_blockers,
        "top_tier_ready": top_tier_ready,
        "critical_missing": critical_missing,
        "vault_signal_missing": vault_signal_missing,
        "property_snapshot_missing": property_snapshot_missing,
        "ownership_missing": ownership_missing,
        "outreach_missing": outreach_missing,
        "batchdata_fallback_targets": batchdata_targets,
    }
