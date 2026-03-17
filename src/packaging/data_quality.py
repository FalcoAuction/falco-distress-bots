import json
from typing import Any, Dict, List, Optional

from ..automation.prefc_policy import (
    prefc_county_is_active,
    prefc_county_is_watch,
    prefc_county_tier,
    prefc_source_priority,
)


_PACKET_CRITICAL_FIELDS = {
    "address": "Address missing",
    "county": "County missing",
}

_VALUE_FIELDS = ("value_anchor_low", "value_anchor_mid", "value_anchor_high")

_VAULT_SIGNAL_FIELDS = {
    "falco_score_internal": "Falco score missing",
    "auction_readiness": "Auction readiness missing",
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
    "mortgage_amount": "Original loan amount missing",
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
    "mortgage_amount": "Original loan amount missing",
}

_PRE_FORECLOSURE_REQUIRED_FIELDS = {
    "owner_name": "Owner name missing",
    "owner_mail": "Owner mailing address missing",
    "last_sale_date": "Last transfer date missing",
    "mortgage_lender": "Mortgage lender missing",
    "mortgage_amount": "Original loan amount missing",
    "property_identifier": "Parcel / APN missing",
}

_BATCHDATA_TARGET_FIELDS = frozenset({
    "owner_name",
    "owner_mail",
    "last_sale_date",
    "mortgage_lender",
    "mortgage_amount",
    "mortgage_date",
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
        "mortgage_amount": None,
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

        mortgage = mortgage_blob.get("mortgage") or {}
        if isinstance(mortgage, dict):
            out["mortgage_amount"] = (
                mortgage.get("amount")
                or mortgage.get("loanAmount")
                or mortgage.get("originationAmount")
                or None
            )
        if not out["mortgage_amount"]:
            out["mortgage_amount"] = (
                mortgage_blob.get("amount")
                or mortgage_blob.get("loanAmount")
                or mortgage_blob.get("originationAmount")
                or None
            )

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


def _prefc_debt_proxy_ready(enriched: Dict[str, Any]) -> bool:
    sale_status = str(enriched.get("sale_status") or "").strip().lower()
    distress_type = str(enriched.get("distress_type") or "").strip().upper()
    is_pre_foreclosure = sale_status == "pre_foreclosure" or distress_type in {"LIS_PENDENS", "SOT", "SUBSTITUTION_OF_TRUSTEE"}

    if not is_pre_foreclosure or not prefc_county_is_active(enriched.get("county")):
        return False

    has_lender = _present(enriched.get("mortgage_lender"))
    has_amount = _present(enriched.get("mortgage_amount"))
    has_last_sale = _present(enriched.get("last_sale_date"))
    has_owner_profile = _present(enriched.get("owner_name")) and _present(enriched.get("owner_mail"))
    has_value = any(_present(enriched.get(key)) for key in _VALUE_FIELDS)

    return (
        has_lender
        and not has_amount
        and has_last_sale
        and has_owner_profile
        and has_value
        and _has_actionable_outreach(enriched)
    )


def _debt_confidence(enriched: Dict[str, Any]) -> str:
    has_lender = _present(enriched.get("mortgage_lender"))
    has_amount = _present(enriched.get("mortgage_amount"))
    has_transfer_support = _present(enriched.get("last_sale_date")) or _present(enriched.get("mortgage_date"))

    if has_lender and has_amount and has_transfer_support:
        return "FULL"
    if has_lender and has_amount:
        return "PARTIAL"
    if _prefc_debt_proxy_ready(enriched):
        return "PROXY"
    if has_lender or has_amount:
        return "THIN"
    return "NONE"


def _int_or_none(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _derive_execution_reality(enriched: Dict[str, Any]) -> Dict[str, Any]:
    is_pre_foreclosure = str(enriched.get("sale_status") or "").strip().lower() == "pre_foreclosure"
    owner_contact = any(
        _present(enriched.get(key))
        for key in ("owner_phone_primary", "owner_phone_secondary")
    )
    sale_status_contact = any(
        _present(enriched.get(key))
        for key in ("notice_phone", "trustee_phone_public")
    )
    owner_profile = _present(enriched.get("owner_name")) and _present(enriched.get("owner_mail"))
    debt_context_strict = (
        _present(enriched.get("mortgage_lender"))
        and _present(enriched.get("mortgage_amount"))
        and _present(enriched.get("last_sale_date"))
    )
    value_context = any(_present(enriched.get(key)) for key in _VALUE_FIELDS)
    debt_context_proxy = _prefc_debt_proxy_ready(enriched)
    debt_context = debt_context_strict or debt_context_proxy
    dts_days = _int_or_none(enriched.get("dts_days"))

    if owner_contact and sale_status_contact:
        contact_path_quality = "STRONG"
    elif owner_contact and owner_profile:
        contact_path_quality = "GOOD"
    elif owner_contact or sale_status_contact or owner_profile:
        contact_path_quality = "PARTIAL"
    else:
        contact_path_quality = "THIN"

    if dts_days is None:
        intervention_window = "WIDE" if is_pre_foreclosure else "MODERATE"
    elif dts_days > 45:
        intervention_window = "WIDE"
    elif dts_days >= 21:
        intervention_window = "MODERATE"
    elif dts_days >= 7:
        intervention_window = "TIGHT"
    else:
        intervention_window = "COMPRESSED"

    if owner_contact and sale_status_contact:
        control_party = "MIXED"
    elif owner_contact and owner_profile and (dts_days is None or dts_days > 14):
        control_party = "OWNER"
    elif sale_status_contact:
        control_party = "LENDER / TRUSTEE"
    elif owner_contact:
        control_party = "OWNER"
    else:
        control_party = "UNCLEAR"

    if control_party == "LENDER / TRUSTEE" and sale_status_contact and intervention_window in {"TIGHT", "COMPRESSED"}:
        lender_control_intensity = "HIGH"
    elif control_party in {"LENDER / TRUSTEE", "MIXED"} or sale_status_contact:
        lender_control_intensity = "MEDIUM"
    else:
        lender_control_intensity = "LOW"

    if (
        owner_contact
        and owner_profile
        and control_party in {"OWNER", "MIXED"}
        and intervention_window in {"WIDE", "MODERATE"}
        and lender_control_intensity != "HIGH"
    ):
        owner_agency = "HIGH"
    elif owner_contact or (owner_profile and control_party != "LENDER / TRUSTEE"):
        owner_agency = "MEDIUM"
    else:
        owner_agency = "LOW"

    if owner_agency == "HIGH" and intervention_window in {"WIDE", "MODERATE"}:
        execution_posture = "OWNER ACTIONABLE"
    elif control_party == "LENDER / TRUSTEE" and lender_control_intensity == "HIGH":
        execution_posture = "LATE-STAGE / LENDER-CONTROLLED"
    elif control_party == "LENDER / TRUSTEE" and sale_status_contact:
        execution_posture = "AUCTION EXECUTION"
    elif control_party == "MIXED" and (owner_contact or sale_status_contact):
        execution_posture = "MIXED / OPERATOR REVIEW"
    else:
        execution_posture = "NEEDS MORE CONTROL CLARITY"

    if (
        value_context
        and owner_profile
        and debt_context
        and (owner_contact or sale_status_contact)
        and owner_agency in {"HIGH", "MEDIUM"}
        and intervention_window in {"WIDE", "MODERATE"}
        and lender_control_intensity != "HIGH"
        and dts_days is not None
        and 0 <= dts_days <= 60
    ):
        workability_band = "STRONG"
    elif (
        value_context
        and owner_profile
        and debt_context
        and (owner_contact or sale_status_contact)
        and intervention_window != "COMPRESSED"
    ):
        workability_band = "MODERATE"
    elif is_pre_foreclosure and owner_profile and debt_context and owner_contact:
        workability_band = "MODERATE"
    else:
        workability_band = "LIMITED"

    if (
        owner_agency == "HIGH"
        and intervention_window in {"WIDE", "MODERATE"}
        and lender_control_intensity != "HIGH"
        and workability_band in {"STRONG", "MODERATE"}
    ):
        influenceability = "HIGH"
    elif (
        owner_agency in {"HIGH", "MEDIUM"}
        and intervention_window != "COMPRESSED"
        and workability_band != "LIMITED"
    ):
        influenceability = "MEDIUM"
    else:
        influenceability = "LOW"

    notes: List[str] = []
    if owner_agency == "HIGH":
        notes.append("Owner still appears to have enough flexibility to influence the outcome")
    elif owner_agency == "MEDIUM":
        notes.append("Owner may still have some room to act, but the file needs operator confirmation")
    else:
        notes.append("Owner agency appears limited at the current stage")

    if lender_control_intensity == "HIGH":
        notes.append("Lender or trustee posture appears to be dictating the file")
    elif lender_control_intensity == "MEDIUM":
        notes.append("Lender or trustee involvement is meaningful, but may not fully control the file yet")

    if intervention_window == "WIDE":
        notes.append("There is still meaningful time to shape outcome before hard sale pressure")
    elif intervention_window == "MODERATE":
        notes.append("There is still some intervention runway, but timing should be taken seriously")
    elif intervention_window == "TIGHT":
        notes.append("Timing is tightening and operator room is narrowing")
    else:
        notes.append("The intervention window is highly compressed")

    if control_party == "LENDER / TRUSTEE":
        notes.append("Owner may not control the outcome at this stage")
    elif control_party == "MIXED":
        notes.append("Owner contact exists, but sale-status control still appears shared")

    if execution_posture == "NEEDS MORE CONTROL CLARITY":
        notes.append("Control path still needs clarification before execution")

    if workability_band == "MODERATE":
        notes.append("Execution path is credible but not yet fully turn-key")
    elif workability_band == "LIMITED":
        notes.append("Execution path remains thin relative to timing and debt context")
    if debt_context_proxy:
        notes.append("Original loan amount remains unconfirmed, but lender and debt path are credible enough for early review")

    return {
        "owner_contact_available": owner_contact,
        "sale_status_contact_available": sale_status_contact,
        "contact_path_quality": contact_path_quality,
        "control_party": control_party,
        "owner_agency": owner_agency,
        "intervention_window": intervention_window,
        "lender_control_intensity": lender_control_intensity,
        "influenceability": influenceability,
        "execution_posture": execution_posture,
        "workability_band": workability_band,
        "debt_proxy_ready": debt_context_proxy,
        "notes": notes,
    }


def _degrade_confidence(confidence: str) -> str:
    if confidence == "HIGH":
        return "MEDIUM"
    if confidence == "MEDIUM":
        return "LOW"
    return "LOW"


def _derive_lane_suggestion(
    enriched: Dict[str, Any],
    execution_reality: Dict[str, Any],
) -> Dict[str, Any]:
    owner_contact = bool(execution_reality.get("owner_contact_available"))
    sale_status_contact = bool(execution_reality.get("sale_status_contact_available"))
    control_party = str(execution_reality.get("control_party") or "UNCLEAR")
    workability_band = str(execution_reality.get("workability_band") or "LIMITED")
    owner_agency = str(execution_reality.get("owner_agency") or "LOW")
    intervention_window = str(execution_reality.get("intervention_window") or "COMPRESSED")
    lender_control_intensity = str(execution_reality.get("lender_control_intensity") or "HIGH")
    influenceability = str(execution_reality.get("influenceability") or "LOW")
    owner_profile = _present(enriched.get("owner_name")) and _present(enriched.get("owner_mail"))
    debt_context = _present(enriched.get("mortgage_lender")) and _present(enriched.get("last_sale_date"))
    dts_days = _int_or_none(enriched.get("dts_days"))

    lane = "unclear"
    confidence = "LOW"
    reasons: List[str] = []

    if influenceability == "HIGH" and owner_agency == "HIGH":
        lane = "borrower_side"
        confidence = "HIGH" if intervention_window in {"WIDE", "MODERATE"} else "MEDIUM"
        reasons.append("Owner still appears reachable, influenceable, and able to shape the file")
        if intervention_window in {"WIDE", "MODERATE"}:
            reasons.append("There is enough runway for operator help before lender control hardens")
    elif owner_contact and sale_status_contact:
        lane = "mixed"
        confidence = "MEDIUM"
        reasons.append("Owner and sale-status contact paths are both present")
        if dts_days is not None and dts_days > 14:
            reasons.append("There is still time to test borrower-side control before sale")
        elif dts_days is not None:
            reasons.append("Timing is tight enough that sale mechanics still matter")
    elif owner_contact and owner_profile and control_party == "OWNER":
        lane = "borrower_side"
        confidence = "MEDIUM"
        reasons.append("Owner contact path exists and control currently appears owner-side")
        if dts_days is None or dts_days > 21:
            confidence = "HIGH"
            reasons.append("Timing leaves room for borrower-side intervention")
    elif sale_status_contact and control_party == "LENDER / TRUSTEE":
        lane = "lender_trustee"
        confidence = "MEDIUM"
        reasons.append("Sale-status contact is stronger than owner-side control")
        if dts_days is not None and dts_days <= 21:
            confidence = "HIGH"
            reasons.append("Sale is close enough that lender/trustee dynamics likely dominate")
    elif sale_status_contact and not owner_contact:
        lane = "auction_only"
        confidence = "MEDIUM"
        reasons.append("No owner contact path is present, but sale-status contact is available")
        if dts_days is not None and dts_days <= 14:
            confidence = "HIGH"
            reasons.append("Very tight timing suggests auction execution is the most realistic lane")
    elif owner_contact:
        lane = "borrower_side"
        confidence = "LOW"
        reasons.append("Owner contact exists, but sale-status control is still unclear")
    elif sale_status_contact:
        lane = "lender_trustee"
        confidence = "LOW"
        reasons.append("Sale-status contact exists, but owner control remains unclear")
    else:
        reasons.append("No clear owner or sale-status contact path is present yet")

    if not owner_profile and lane in {"borrower_side", "mixed"}:
        confidence = _degrade_confidence(confidence)
        reasons.append("Owner identity or mailing profile is still incomplete")

    if not debt_context and lane in {"lender_trustee", "mixed", "auction_only"}:
        confidence = _degrade_confidence(confidence)
        reasons.append("Debt context is still incomplete")

    if owner_agency == "LOW" and lane in {"borrower_side", "mixed"}:
        confidence = _degrade_confidence(confidence)
        reasons.append("Owner agency currently appears limited")

    if intervention_window in {"TIGHT", "COMPRESSED"} and lane in {"borrower_side", "mixed"}:
        confidence = _degrade_confidence(confidence)
        reasons.append("Timing is late enough that borrower-side influence may be limited")

    if lender_control_intensity == "HIGH" and lane in {"borrower_side", "mixed"}:
        confidence = _degrade_confidence(confidence)
        reasons.append("Lender control intensity appears high at this stage")

    if workability_band == "LIMITED":
        confidence = _degrade_confidence(confidence)
        reasons.append("Overall workability remains limited")
    elif workability_band == "STRONG":
        reasons.append("Underlying workability profile is strong")

    if influenceability == "HIGH":
        reasons.append("The file still appears influenceable rather than fully controlled")
    elif influenceability == "LOW":
        confidence = _degrade_confidence(confidence)
        reasons.append("The file currently looks real but not very influenceable")

    if control_party == "UNCLEAR":
        confidence = _degrade_confidence(confidence)
        reasons.append("True control party still needs operator confirmation")

    return {
        "suggested_execution_lane": lane,
        "confidence": confidence,
        "reasons": reasons[:4],
    }


def assess_packet_data(fields: Dict[str, Any]) -> Dict[str, Any]:
    enriched = dict(fields)
    for key, value in _extract_property_detail(fields.get("attom_raw_json")).items():
        if not _present(enriched.get(key)) and _present(value):
            enriched[key] = value
    for key, value in _extract_owner_mortgage(fields.get("attom_raw_json")).items():
        if not _present(enriched.get(key)) and _present(value):
            enriched[key] = value

    sale_status = str(enriched.get("sale_status") or "").lower().strip()
    distress_type = str(enriched.get("distress_type") or "").upper().strip()
    is_pre_foreclosure = sale_status == "pre_foreclosure" or distress_type in {"LIS_PENDENS", "SOT", "SUBSTITUTION_OF_TRUSTEE"}
    county_tier = prefc_county_tier(enriched.get("county"))
    source_priority = prefc_source_priority(distress_type)

    critical_missing: List[str] = []
    for key, label in _PACKET_CRITICAL_FIELDS.items():
        if key == "sale_date" and (_present(enriched.get("sale_date")) or _present(enriched.get("sale_date_iso"))):
            continue
        if not _present(enriched.get(key)):
            critical_missing.append(label)

    if (
        not is_pre_foreclosure
        and not (
            _present(enriched.get("dts_days"))
            or _present(enriched.get("sale_date"))
            or _present(enriched.get("sale_date_iso"))
        )
    ):
        critical_missing.append("Sale timing missing")

    if distress_type != "LIS_PENDENS" and not any(_present(enriched.get(key)) for key in _VALUE_FIELDS):
        critical_missing.append("Valuation anchors missing")

    vault_signal_missing = [
        label for key, label in _VAULT_SIGNAL_FIELDS.items() if not _present(enriched.get(key))
    ]
    equity_band = str(enriched.get("equity_band") or "").strip().upper()
    if equity_band in {"", "UNKNOWN"}:
        vault_signal_missing.append("Equity band missing")
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

    if distress_type in ("LIS_PENDENS", "FORECLOSURE", "FORECLOSURE_TN", "SOT", "SUBSTITUTION_OF_TRUSTEE"):
        if not _has_actionable_outreach(enriched):
            execution_blockers.append("Actionable outreach path missing")

    pre_foreclosure_blockers = [
        label for key, label in _PRE_FORECLOSURE_REQUIRED_FIELDS.items() if not _present(enriched.get(key))
    ]
    debt_proxy_ready = _prefc_debt_proxy_ready(enriched)
    if debt_proxy_ready and "Original loan amount missing" in pre_foreclosure_blockers:
        pre_foreclosure_blockers = [
            blocker for blocker in pre_foreclosure_blockers if blocker != "Original loan amount missing"
        ]
    if is_pre_foreclosure and not prefc_county_is_active(enriched.get("county")):
        if prefc_county_is_watch(enriched.get("county")):
            pre_foreclosure_blockers.append("County remains in watch lane")
        else:
            pre_foreclosure_blockers.append("County is not in the active pre-foreclosure lane")
    if not any(_present(enriched.get(key)) for key in _VALUE_FIELDS):
        pre_foreclosure_blockers.append("Valuation anchors missing")
    if distress_type in ("LIS_PENDENS", "SOT", "SUBSTITUTION_OF_TRUSTEE") and not _has_actionable_outreach(enriched):
        pre_foreclosure_blockers.append("Actionable outreach path missing")

    batchdata_targets = [
        key for key in _BATCHDATA_TARGET_FIELDS
        if not _present(enriched.get(key))
    ]
    execution_reality = _derive_execution_reality(enriched)
    lane_suggestion = _derive_lane_suggestion(enriched, execution_reality)
    debt_confidence = _debt_confidence(enriched)
    equity_is_strong_enough = equity_band in {"MED", "HIGH"}

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
        and execution_reality["contact_path_quality"] != "THIN"
        and execution_reality["control_party"] != "UNCLEAR"
        and execution_reality["owner_agency"] in {"HIGH", "MEDIUM"}
        and execution_reality["intervention_window"] in {"WIDE", "MODERATE"}
        and execution_reality["lender_control_intensity"] != "HIGH"
        and execution_reality["influenceability"] == "HIGH"
        and execution_reality["execution_posture"] != "NEEDS MORE CONTROL CLARITY"
        and execution_reality["workability_band"] == "STRONG"
        and not any(
            not _present(enriched.get(key))
            for key in (
                "owner_name",
                "owner_mail",
                "last_sale_date",
                "mortgage_lender",
                "mortgage_amount",
                "property_identifier",
                "year_built",
                "building_area_sqft",
                "beds",
                "baths",
            )
        )
    )

    pre_foreclosure_review_ready = (
        is_pre_foreclosure
        and len(pre_foreclosure_blockers) == 0
        and prefc_county_is_active(enriched.get("county"))
        and debt_confidence == "FULL"
        and equity_is_strong_enough
        and execution_reality["owner_contact_available"]
        and execution_reality["contact_path_quality"] in {"STRONG", "GOOD"}
        and execution_reality["control_party"] in {"OWNER", "MIXED"}
        and execution_reality["owner_agency"] in {"HIGH", "MEDIUM"}
        and execution_reality["intervention_window"] in {"WIDE", "MODERATE"}
        and execution_reality["lender_control_intensity"] == "LOW"
        and execution_reality["influenceability"] == "HIGH"
        and execution_reality["execution_posture"] in {"OWNER ACTIONABLE", "MIXED / OPERATOR REVIEW"}
        and execution_reality["workability_band"] in {"STRONG", "MODERATE"}
        and str(enriched.get("auction_readiness") or "").upper() in {"GREEN", "YELLOW", "PARTIAL"}
    )
    weak_live_prefc_reasons: List[str] = []
    if is_pre_foreclosure:
        if debt_confidence != "FULL":
            weak_live_prefc_reasons.append("Debt confidence is below full")
        if not equity_is_strong_enough:
            weak_live_prefc_reasons.append("Equity band is too weak for live pre-foreclosure")
        if execution_reality["contact_path_quality"] not in {"STRONG", "GOOD"}:
            weak_live_prefc_reasons.append("Contact path is not strong enough")
        if execution_reality["owner_agency"] not in {"HIGH", "MEDIUM"}:
            weak_live_prefc_reasons.append("Owner agency remains too limited")
        if execution_reality["intervention_window"] not in {"WIDE", "MODERATE"}:
            weak_live_prefc_reasons.append("Intervention window is too compressed")
        if execution_reality["lender_control_intensity"] != "LOW":
            weak_live_prefc_reasons.append("Lender control is too strong")
        if execution_reality["influenceability"] != "HIGH":
            weak_live_prefc_reasons.append("Influenceability is not high enough")
        if execution_reality["execution_posture"] not in {"OWNER ACTIONABLE", "MIXED / OPERATOR REVIEW"}:
            weak_live_prefc_reasons.append("Execution posture is not strong enough")
        if execution_reality["workability_band"] not in {"STRONG", "MODERATE"}:
            weak_live_prefc_reasons.append("Workability is too limited")

    prefc_live_quality = is_pre_foreclosure and len(weak_live_prefc_reasons) == 0

    return {
        "enriched_fields": {
            "property_identifier": enriched.get("property_identifier"),
            "owner_name": enriched.get("owner_name"),
            "owner_mail": enriched.get("owner_mail"),
            "last_sale_date": enriched.get("last_sale_date"),
            "mortgage_date": enriched.get("mortgage_date"),
            "mortgage_lender": enriched.get("mortgage_lender"),
            "mortgage_amount": enriched.get("mortgage_amount"),
            "year_built": enriched.get("year_built"),
            "building_area_sqft": enriched.get("building_area_sqft"),
            "beds": enriched.get("beds"),
            "baths": enriched.get("baths"),
        },
        "packet_completeness_pct": completeness,
        "vault_publish_ready": len(vault_blockers) == 0,
        "vault_publish_blockers": vault_blockers,
        "pre_foreclosure_review_ready": pre_foreclosure_review_ready,
        "pre_foreclosure_review_blockers": pre_foreclosure_blockers,
        "debt_confidence": debt_confidence,
        "prefc_debt_proxy_ready": debt_proxy_ready,
        "prefc_live_quality": prefc_live_quality,
        "prefc_live_review_reasons": weak_live_prefc_reasons[:5],
        "prefc_county_tier": county_tier,
        "prefc_source_priority": source_priority,
        "execution_blockers": execution_blockers,
        "execution_reality": execution_reality,
        "lane_suggestion": lane_suggestion,
        "execution_notes": execution_reality["notes"],
        "top_tier_ready": top_tier_ready,
        "critical_missing": critical_missing,
        "vault_signal_missing": vault_signal_missing,
        "property_snapshot_missing": property_snapshot_missing,
        "ownership_missing": ownership_missing,
        "outreach_missing": outreach_missing,
        "batchdata_fallback_targets": batchdata_targets,
    }
