"""Conservative repairs and reuse of evidence already held by FALCO."""
from __future__ import annotations

from typing import Any
import re


def tracked_run(name, operation, write=True):
    """Give free enrichers the same run-health record as existing bots."""
    from datetime import datetime, timezone
    from ._base import BotBase
    reporter = BotBase()
    reporter.name = name
    started = datetime.now(timezone.utc)
    def report(status, result=None, error=None):
        if not write:
            return
        result = result or {}
        reporter._report_health(status=status, started_at=started,
            finished_at=None if status == "running" else datetime.now(timezone.utc),
            fetched_count=result.get("fetched", 0), parsed_count=result.get("scanned", 0),
            staged_count=result.get("staged", result.get("written", 0)), duplicate_count=0,
            error_message=error)
    report("running")
    try:
        result = operation()
    except Exception as exc:
        report("failed", error=type(exc).__name__)
        raise
    report("ok" if result.get("status") == "ok" else "failed", result, result.get("error"))
    return result


def hmda_principal_repair(row: dict) -> dict:
    pm = row.get("phone_metadata") or {}
    sig = pm.get("mortgage_signal") or {}
    if sig.get("source") != "hmda_match" or pm.get("rod_lookup") or pm.get("mortgage_estimate"):
        return {}
    principal = sig.get("original_principal") or sig.get("principal") or sig.get("amount")
    try:
        matches = float(row.get("mortgage_balance")) == float(principal)
    except (TypeError, ValueError):
        return {}
    if not matches:
        return {}
    return {"mortgage_balance": None, "phone_metadata": {
        **pm, "mortgage_signal": {**sig, "kind": "original_principal"},
        "free_data_repair": {**(pm.get("free_data_repair") or {}),
                             "removed_hmda_principal": row["mortgage_balance"]}}}


def johnson_board_research(row: dict) -> bool:
    source = row.get("bot_source") or row.get("source")
    if source == "johnson_city_bdsr":
        return True
    # Promoted bot copies retain the collector payload under a generic source.
    raw = row.get("raw_payload")
    return (source in (None, "", "bot") and isinstance(raw, dict)
            and isinstance(raw.get("johnson_city_bdsr"), dict))


def entity_eligible(row: dict) -> bool:
    raw = row.get("raw_payload") or {}
    lane = raw.get("property_lane") or {}
    if isinstance(lane, dict) and lane.get("exclude_reason"):
        return False
    if row.get("distress_type") in {"CHURCH_CLOSURE", "STALLED_BUILD"}:
        return True
    return (isinstance(lane, dict) and lane.get("lane") in {"commercial", "land"}
            and (lane.get("readiness") or {}).get("ready") is True)


def professional_contact_ready(row: dict) -> bool:
    raw = row.get("raw_payload") or {}
    groups = [raw.get("b2b_contacts") or {}, (raw.get("personal_property") or {}).get("contacts") or {}]
    for contacts in groups:
        for role in ("trustee", "counsel", "attorney", "executor", "receiver", "business_principal"):
            values = contacts.get(role) or []
            if isinstance(values, dict):
                values = [values]
            if not isinstance(values, list):
                continue
            if any(isinstance(c, dict) and c.get("name") and (c.get("phone") or c.get("email"))
                   for c in values):
                return True
    return False


def county_patch(row: dict) -> dict[str, Any]:
    """Reuse a matched county record; never upgrade an ambiguous name match."""
    raw = row.get("raw_payload") or {}
    rec = raw.get("comptroller_parcel") or {}
    source = "comptroller"
    if rec:
        method = rec.get("method")
        if method not in {"address", "land_lane"} and not (
                method == "debtor_name" and rec.get("confidence") in {"joint", "middle_initial"}):
            return {}
        if method == "land_lane" and raw.get("source") == "county_gis":
            source = "county_gis"
    else:
        rec = raw.get("metro_parcel") or {}
        source = "metro_nashville_parcels"
        if not rec:
            for key, label in (("rutherford_arcgis", "rutherford_assessor"),
                               ("hamilton_assessor", "hamilton_assessor"),
                               ("williamson_inigo", "williamson_assessor"),
                               ("padctn", "davidson_assessor"),
                               ("montgomery_gis", "montgomery_last_sale")):
                candidate = raw.get(key)
                if isinstance(candidate, dict) and candidate and candidate.get("status") not in {
                        "not_found", "wrong_parcel", "ambiguous", "error"}:
                    rec, source = candidate, label
                    break
    if not isinstance(rec, dict) or not rec:
        return {}
    patch: dict[str, Any] = {}
    owner = rec.get("owner")
    if owner and not row.get("owner_name_records"):
        patch["owner_name_records"] = owner
    try:
        value = float(rec.get("appraisal") or rec.get("total_appraisal") or rec.get("appraised") or 0)
        current = float(row.get("property_value") or 0)
    except (TypeError, ValueError):
        value = current = 0
    if value > 0 and not current:
        patch.update(property_value=round(value), property_value_source=source)
    elif value > 0 and abs(value - current) < 1 and not row.get("property_value_source"):
        patch["property_value_source"] = source
    # Do not label a construction budget as a county appraisal.
    elif row.get("bot_source") == "stalled_build" and not row.get("property_value_source"):
        cost = (raw.get("permit") or {}).get("Const_Cost")
        if cost and current == float(cost):
            patch["property_value_source"] = "permit_construction_cost_proxy"
    try:
        year = int(rec.get("year_built") or 0)
        if 1600 <= year <= 2100 and not row.get("year_built"):
            patch["year_built"] = year
    except (ValueError, TypeError):
        pass
    return patch


def notice_phone_repair(row: dict) -> dict:
    """Move only demonstrably resolver-sourced numbers, retaining other evidence."""
    pm = row.get("phone_metadata") or {}
    resolver = pm.get("phone_resolver") or {}
    unsafe = {"notice_body", "courtlistener_case_text", "existing_field_admin_notes"}
    if resolver.get("source") not in unsafe:
        return {}
    digits = lambda p: re.sub(r"\D", "", str(p or ""))[-10:]
    if not row.get("phone") or digits(row["phone"]) != digits(resolver.get("phone")):
        return {}
    # Do not erase a phone independently supplied by a contact provider or user.
    if any(pm.get(k) for k in ("batchdata_skip_trace", "enformion", "enformion_skip_trace", "dialer_feedback")):
        return {}
    raw = dict(row.get("raw_payload") or {})
    contacts = list(raw.get("public_contact_candidates") or [])
    candidate = {"phone": row["phone"], "source": resolver["source"], "role": "unverified_notice_contact"}
    if candidate not in contacts:
        contacts.append(candidate)
    raw["public_contact_candidates"] = contacts
    return {"phone": None, "raw_payload": raw, "phone_metadata": {
        **pm, "phone_resolver": {**resolver, "role": "unverified_notice_contact", "primary_removed": True}}}
