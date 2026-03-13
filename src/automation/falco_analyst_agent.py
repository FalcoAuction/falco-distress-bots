from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..packaging.data_quality import assess_packet_data


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _reports_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "out" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return value.strip() not in {"", "null", "None", "Unavailable", "—", "â€”"}
    if isinstance(value, (list, dict)):
        return bool(value)
    return True


def _latest_attom_map(con: sqlite3.Connection) -> Dict[str, sqlite3.Row]:
    rows = con.execute(
        """
        WITH latest AS (
          SELECT
            lead_key,
            attom_raw_json,
            avm_value,
            avm_low,
            avm_high,
            ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY enriched_at DESC, id DESC) AS rn
          FROM attom_enrichments
        )
        SELECT lead_key, attom_raw_json, avm_value, avm_low, avm_high
        FROM latest
        WHERE rn = 1
        """
    ).fetchall()
    return {str(row["lead_key"]): row for row in rows}


def _latest_text_field_map(con: sqlite3.Connection, field_name: str) -> Dict[str, str]:
    rows = con.execute(
        """
        SELECT lead_key, field_value_text
        FROM lead_field_provenance
        WHERE field_name = ?
          AND field_value_text IS NOT NULL
          AND TRIM(field_value_text) != ''
        ORDER BY created_at DESC, prov_id DESC
        """,
        (field_name,),
    ).fetchall()
    out: Dict[str, str] = {}
    for row in rows:
        lead_key = str(row["lead_key"])
        if lead_key in out:
            continue
        out[lead_key] = str(row["field_value_text"])
    return out


def _latest_num_field_map(con: sqlite3.Connection, field_name: str) -> Dict[str, float]:
    rows = con.execute(
        """
        SELECT lead_key, field_value_num
        FROM lead_field_provenance
        WHERE field_name = ?
          AND field_value_num IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        """,
        (field_name,),
    ).fetchall()
    out: Dict[str, float] = {}
    for row in rows:
        lead_key = str(row["lead_key"])
        if lead_key in out:
            continue
        out[lead_key] = float(row["field_value_num"])
    return out


def _contact_ready_map(con: sqlite3.Connection) -> Dict[str, bool]:
    rows = con.execute(
        """
        SELECT lead_key, field_value_text
        FROM lead_field_provenance
        WHERE field_name = 'contact_ready'
        ORDER BY created_at DESC, prov_id DESC
        """
    ).fetchall()
    out: Dict[str, bool] = {}
    for row in rows:
        lead_key = str(row["lead_key"])
        if lead_key in out:
            continue
        out[lead_key] = str(row["field_value_text"] or "").strip().lower() in {"1", "true", "yes", "y"}
    return out


def _enriched_fields_for_lead(
    lead: Dict[str, Any],
    attom_map: Dict[str, sqlite3.Row],
    text_maps: Dict[str, Dict[str, str]],
    num_maps: Dict[str, Dict[str, float]],
    contact_ready: Dict[str, bool],
) -> Dict[str, Any]:
    lead_key = str(lead.get("lead_key") or "")
    fields = dict(lead)
    attom = attom_map.get(lead_key)
    fields["contact_ready"] = contact_ready.get(lead_key, False)
    fields["attom_raw_json"] = attom["attom_raw_json"] if attom else None
    fields["value_anchor_mid"] = attom["avm_value"] if attom else None
    fields["value_anchor_low"] = attom["avm_low"] if attom else None
    fields["value_anchor_high"] = attom["avm_high"] if attom else None

    for field_name, field_map in text_maps.items():
        value = field_map.get(lead_key)
        if _present(value):
            fields[field_name] = value

    for field_name, field_map in num_maps.items():
        value = field_map.get(lead_key)
        if value is not None:
            fields[field_name] = value

    return fields


def _urgency_for_lead(fields: Dict[str, Any]) -> str:
    sale_status = str(fields.get("sale_status") or "").lower()
    distress_type = str(fields.get("distress_type") or "").upper()
    try:
        dts_days = int(float(fields.get("dts_days"))) if fields.get("dts_days") is not None else None
    except Exception:
        dts_days = None

    if sale_status == "pre_foreclosure" or distress_type in {"SUBSTITUTION_OF_TRUSTEE", "LIS_PENDENS"}:
        return "watch"
    if dts_days is not None and dts_days <= 21:
        return "now"
    if dts_days is not None and dts_days <= 45:
        return "this_week"
    return "monitor"


def _summary_for_priority(fields: Dict[str, Any], quality: Dict[str, Any]) -> str:
    lane = quality["lane_suggestion"]["suggested_execution_lane"].replace("_", " ")
    control = quality["execution_reality"]["control_party"]
    workability = quality["execution_reality"]["workability_band"].lower()
    return (
        f"Screened candidate with a {workability} workability profile. "
        f"Suggested lane is {lane}, with likely control leaning {control.lower()}."
    )


def _summary_for_enrichment(fields: Dict[str, Any], quality: Dict[str, Any]) -> str:
    blockers = quality.get("execution_blockers") or []
    blocker_text = ", ".join(blockers[:2]) if blockers else "key execution context missing"
    return (
        "The file looks directionally interesting, but it is still blocked by "
        f"{blocker_text.lower()}."
    )


def _summary_for_watch(fields: Dict[str, Any], quality: Dict[str, Any]) -> str:
    distress_type = str(fields.get("distress_type") or "pre-foreclosure").replace("_", " ").title()
    county = str(fields.get("county") or "Unknown county")
    return (
        f"Very early {distress_type} signal in {county}. "
        "Good for lifecycle tracking, but too early to claim execution readiness."
    )


def _recommended_action(fields: Dict[str, Any], quality: Dict[str, Any]) -> str:
    sale_status = str(fields.get("sale_status") or "").lower()
    lane = quality["lane_suggestion"]["suggested_execution_lane"]
    blockers = quality.get("execution_blockers") or []

    if quality.get("top_tier_ready"):
        return "Send to licensed operator for execution validation"
    if quality.get("vault_publish_ready"):
        return "Keep on review shelf and request operator lane confirmation"
    if sale_status == "pre_foreclosure":
        return "Enrich now and monitor for foreclosure progression"
    if "Actionable outreach path missing" in blockers:
        return "Repair contact path before escalating"
    if any("Mortgage" in blocker or "loan" in blocker.lower() for blocker in blockers):
        return "Run deeper enterprise enrichment before operator review"
    if lane in {"borrower_side", "lender_trustee", "mixed"}:
        return "Hold for additional validation and missing-field repair"
    return "Monitor and do not escalate yet"


def _is_high_confidence_operator_candidate(fields: Dict[str, Any], quality: Dict[str, Any]) -> bool:
    sale_status = str(fields.get("sale_status") or "").strip().lower()
    lane = str(quality["lane_suggestion"]["suggested_execution_lane"] or "unclear")
    confidence = str(quality["lane_suggestion"]["confidence"] or "LOW").upper()
    execution_reality = quality["execution_reality"]
    blockers = quality.get("execution_blockers") or []

    if lane == "unclear" or confidence == "LOW":
        return False
    if str(execution_reality.get("contact_path_quality") or "THIN").upper() == "THIN":
        return False
    if str(execution_reality.get("control_party") or "UNCLEAR").upper() == "UNCLEAR":
        return False
    if str(execution_reality.get("execution_posture") or "NEEDS MORE CONTROL CLARITY").upper() == "NEEDS MORE CONTROL CLARITY":
        return False

    if sale_status == "pre_foreclosure":
        return bool(
            quality.get("pre_foreclosure_review_ready")
            and str(execution_reality.get("workability_band") or "LIMITED").upper() in {"STRONG", "MODERATE"}
            and len(blockers) <= 2
        )

    return bool(
        quality.get("vault_publish_ready")
        and quality.get("top_tier_ready")
        and str(execution_reality.get("workability_band") or "LIMITED").upper() == "STRONG"
    )


def _analysis_bucket(fields: Dict[str, Any], quality: Dict[str, Any]) -> str:
    sale_status = str(fields.get("sale_status") or "").lower()
    if sale_status == "pre_foreclosure":
        if _is_high_confidence_operator_candidate(fields, quality):
            return "operator_review_candidate"
        return "watch_and_enrich"
    if quality.get("top_tier_ready") and _is_high_confidence_operator_candidate(fields, quality):
        return "priority_review"
    if quality.get("vault_publish_ready") and _is_high_confidence_operator_candidate(fields, quality):
        return "operator_review_candidate"
    blockers = quality.get("execution_blockers") or []
    if blockers:
        return "repair_and_retry"
    return "monitor"


def _confidence(quality: Dict[str, Any], bucket: str) -> str:
    confidence = str(quality["lane_suggestion"]["confidence"] or "LOW").upper()
    if bucket == "priority_review":
        return "HIGH"
    if bucket == "operator_review_candidate" and confidence == "HIGH":
        return "MEDIUM"
    if bucket == "watch_and_enrich" and confidence == "LOW":
        return "MEDIUM"
    return confidence


def _analyst_entry(fields: Dict[str, Any], quality: Dict[str, Any]) -> Dict[str, Any]:
    bucket = _analysis_bucket(fields, quality)
    if bucket == "priority_review":
        summary = _summary_for_priority(fields, quality)
    elif bucket == "operator_review_candidate":
        summary = _summary_for_priority(fields, quality)
    elif bucket == "watch_and_enrich":
        summary = _summary_for_watch(fields, quality)
    else:
        summary = _summary_for_enrichment(fields, quality)

    lane = quality["lane_suggestion"]["suggested_execution_lane"]
    return {
        "lead_key": fields.get("lead_key"),
        "address": fields.get("address"),
        "county": fields.get("county"),
        "distress_type": fields.get("distress_type"),
        "sale_status": fields.get("sale_status"),
        "dts_days": fields.get("dts_days"),
        "analysis_bucket": bucket,
        "confidence": _confidence(quality, bucket),
        "urgency": _urgency_for_lead(fields),
        "suggested_execution_lane": lane,
        "suggested_lane_reasons": quality["lane_suggestion"]["reasons"],
        "control_party": quality["execution_reality"]["control_party"],
        "contact_path_quality": quality["execution_reality"]["contact_path_quality"],
        "execution_posture": quality["execution_reality"]["execution_posture"],
        "workability_band": quality["execution_reality"]["workability_band"],
        "recommended_action": _recommended_action(fields, quality),
        "summary": summary,
        "execution_blockers": quality.get("execution_blockers") or [],
        "missing_fields": quality.get("batchdata_fallback_targets") or [],
        "operator_validation_required": True,
        "top_tier_ready": bool(quality.get("top_tier_ready")),
        "vault_publish_ready": bool(quality.get("vault_publish_ready")),
    }


def build_falco_analyst_report(run_summary: Dict[str, Any], limit: int = 12) -> Dict[str, Any]:
    quality = run_summary.get("quality") if isinstance(run_summary.get("quality"), dict) else {}
    leads = quality.get("leads") if isinstance(quality.get("leads"), list) else []

    con = _connect()
    try:
        attom_map = _latest_attom_map(con)
        text_maps = {
            field_name: _latest_text_field_map(con, field_name)
            for field_name in (
                "trustee_phone_public",
                "owner_phone_primary",
                "owner_phone_secondary",
                "notice_phone",
                "owner_name",
                "owner_mail",
                "last_sale_date",
                "mortgage_lender",
                "mortgage_amount",
                "property_identifier",
            )
        }
        num_maps = {
            field_name: _latest_num_field_map(con, field_name)
            for field_name in ("year_built", "building_area_sqft", "beds", "baths")
        }
        contact_ready = _contact_ready_map(con)

        analyzed: List[Dict[str, Any]] = []
        for row in leads:
            fields = _enriched_fields_for_lead(dict(row), attom_map, text_maps, num_maps, contact_ready)
            quality_result = assess_packet_data(fields)
            analyzed.append(_analyst_entry(fields, quality_result))

        pre_foreclosure_rows = [
            dict(row)
            for row in con.execute(
                """
                SELECT
                  lead_key,
                  address,
                  county,
                  distress_type,
                  sale_status,
                  dts_days
                FROM leads
                WHERE sale_status = 'pre_foreclosure'
                ORDER BY COALESCE(last_seen_at, first_seen_at) DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        ]
    finally:
        con.close()

    buckets = {
        "priority_review": [],
        "operator_review_candidate": [],
        "repair_and_retry": [],
        "watch_and_enrich": [],
        "monitor": [],
    }
    for row in analyzed:
        buckets[row["analysis_bucket"]].append(row)

    overview = {
        "priority_review_count": len(buckets["priority_review"]),
        "operator_review_candidate_count": len(buckets["operator_review_candidate"]),
        "repair_and_retry_count": len(buckets["repair_and_retry"]),
        "watch_and_enrich_count": len(buckets["watch_and_enrich"]),
        "monitor_count": len(buckets["monitor"]),
        "pre_foreclosure_watch_count": len(pre_foreclosure_rows),
    }

    strategic_notes: List[str] = []
    if overview["priority_review_count"]:
        strategic_notes.append("A small set of files is strong enough for immediate licensed/operator review.")
    if overview["repair_and_retry_count"]:
        strategic_notes.append("Most near misses are blocked by missing execution context rather than bad property signal.")
    if overview["pre_foreclosure_watch_count"]:
        strategic_notes.append("Pre-foreclosure watch is feeding the top of funnel earlier than sale-scheduled notices.")

    return {
        "agent": "falco_analyst",
        "generated_at": _utc_now(),
        "run_id": run_summary.get("run_id"),
        "overview": overview,
        "strategic_notes": strategic_notes,
        "priority_review": buckets["priority_review"][:limit],
        "operator_review_candidates": buckets["operator_review_candidate"][:limit],
        "repair_and_retry": buckets["repair_and_retry"][:limit],
        "watch_and_enrich": buckets["watch_and_enrich"][:limit],
        "monitor": buckets["monitor"][:limit],
        "pre_foreclosure_watch": pre_foreclosure_rows,
    }


def write_falco_analyst_report(run_id: str, run_summary: Dict[str, Any]) -> Dict[str, Any]:
    report = build_falco_analyst_report(run_summary)
    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_falco_analyst.json"
    latest_path = reports_dir / "latest_falco_analyst.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    return {
        "ok": True,
        "path": str(run_path),
        "priority_review_count": report["overview"]["priority_review_count"],
        "watch_count": report["overview"]["watch_and_enrich_count"],
        "pre_foreclosure_watch_count": report["overview"]["pre_foreclosure_watch_count"],
    }
