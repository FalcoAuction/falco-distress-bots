from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from ..packaging.data_quality import assess_packet_data
from .prefc_policy import (
    prefc_county_is_active,
    prefc_county_priority,
    prefc_county_tier,
    prefc_is_special_situation,
    prefc_overlap_priority,
    prefc_source_priority,
)


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _reports_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "out" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _load_latest_run_summary() -> dict[str, Any] | None:
    path = _reports_dir() / "latest_run_summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _site_vault_file() -> Path:
    return Path(__file__).resolve().parents[3] / "falco-site" / "data" / "vault_listings.ndjson"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


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


def _latest_text_field(con: sqlite3.Connection, lead_key: str, field_name: str) -> str | None:
    row = con.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ? AND field_value_text IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return str(row[0]).strip() if row and row[0] is not None and str(row[0]).strip() else None


def _latest_num_field(con: sqlite3.Connection, lead_key: str, field_name: str) -> float | None:
    row = con.execute(
        """
        SELECT field_value_num
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ? AND field_value_num IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _contact_ready(con: sqlite3.Connection, lead_key: str) -> bool:
    row = con.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = 'contact_ready'
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    if not row:
        return False
    return str(row[0] or "").strip().lower() in {"1", "true", "yes", "y"}


def _source_set(con: sqlite3.Connection, lead_key: str) -> set[str]:
    rows = con.execute(
        """
        SELECT DISTINCT UPPER(COALESCE(source, ''))
        FROM ingest_events
        WHERE lead_key = ?
        """,
        (lead_key,),
    ).fetchall()
    return {str(row[0] or "").strip().upper() for row in rows if str(row[0] or "").strip()}


def _overlap_signals(lead: sqlite3.Row, con: sqlite3.Connection) -> List[str]:
    signals: List[str] = []
    sources = _source_set(con, str(lead["lead_key"] or ""))
    if "SUBSTITUTION_OF_TRUSTEE" in sources and "LIS_PENDENS" in sources:
        signals.append("stacked_notice_path")
    if sources.intersection({"API_TAX", "OFFICIAL_TAX_SALE", "TAXPAGES"}):
        signals.append("tax_overlap")
    current_sale_date = str(lead["current_sale_date"] or "").strip()
    original_sale_date = str(lead["original_sale_date"] or "").strip()
    if current_sale_date and original_sale_date and current_sale_date != original_sale_date:
        signals.append("reopened_timing")
    return signals


def _load_live_rows() -> list[dict[str, Any]]:
    path = _site_vault_file()
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict) and row.get("status") == "active":
            rows.append(row)
    return rows


def _hydrate_fields(con: sqlite3.Connection, lead: sqlite3.Row, attom_map: Dict[str, sqlite3.Row]) -> Dict[str, Any]:
    lead_key = str(lead["lead_key"] or "")
    attom = attom_map.get(lead_key)
    fields: Dict[str, Any] = dict(lead)
    fields["contact_ready"] = _contact_ready(con, lead_key)
    fields["attom_raw_json"] = attom["attom_raw_json"] if attom else None
    fields["value_anchor_mid"] = attom["avm_value"] if attom else None
    fields["value_anchor_low"] = attom["avm_low"] if attom else None
    fields["value_anchor_high"] = attom["avm_high"] if attom else None
    for field_name in (
        "owner_name",
        "owner_mail",
        "owner_phone_primary",
        "owner_phone_secondary",
        "trustee_phone_public",
        "notice_phone",
        "last_sale_date",
        "mortgage_date",
        "mortgage_lender",
        "property_identifier",
        "mortgage_record_book",
        "mortgage_record_page",
        "mortgage_record_instrument",
        "debt_reconstruction_confidence",
        "debt_reconstruction_source_mix",
        "debt_reconstruction_missing_reason",
        "debt_reconstruction_blocker_type",
        "debt_reconstruction_summary",
        "county_record_lookup_status",
        "county_record_lookup_provider",
        "county_record_lookup_url",
        "county_record_lookup_hint",
        "county_record_lookup_refs",
        "fsbo_listing_title",
        "fsbo_listing_description",
        "fsbo_signal_labels",
        "fsbo_listing_source",
    ):
        value = _latest_text_field(con, lead_key, field_name)
        if value:
            fields[field_name] = value
    for field_name in ("mortgage_amount", "year_built", "building_area_sqft", "beds", "baths", "list_price", "fsbo_signal_score"):
        value = _latest_num_field(con, lead_key, field_name)
        if value is not None:
            fields[field_name] = value
    return fields


def _build_market_allocation(run_summary: dict[str, Any] | None) -> dict[str, Any]:
    county_rows = ((run_summary or {}).get("county_hit_rates") or {}).get("counties") or []
    county_actions: list[dict[str, Any]] = []
    for row in county_rows:
        county = str(row.get("county") or "")
        strong_live = int(row.get("strongLivePrefc") or 0)
        prefc_tracked = int(row.get("preForeclosureTracked") or 0)
        packet_rate = float(row.get("packetRate") or 0)
        live_rate = float(row.get("liveRate") or 0)
        active_lane = bool(row.get("activeLane"))
        if active_lane and strong_live > 0:
            directive = "push_harder"
            reason = "Already producing strong live pre-foreclosure. Increase effort here first."
        elif active_lane and prefc_tracked >= 2 and packet_rate >= 10:
            directive = "hold_steady"
            reason = "Still productive, but quality needs reconstruction work more than volume."
        elif active_lane:
            directive = "hold_steady"
            reason = "Active lane, but not yet proving enough strong-live conversion to widen aggressively."
        elif prefc_tracked > 0 or live_rate > 0:
            directive = "watch"
            reason = "Watch lane only. Keep limited coverage until conversion improves."
        else:
            directive = "deprioritize"
            reason = "Low current evidence of strong output."
        county_actions.append(
            {
                **row,
                "countyTier": prefc_county_tier(county),
                "directive": directive,
                "reason": reason,
            }
        )

    source_actions = [
        {
            "source": "SOT",
            "directive": "push_harder",
            "reason": "Best current upstream pre-foreclosure signal.",
        },
        {
            "source": "LIS_PENDENS",
            "directive": "hold_steady",
            "reason": "Good secondary early signal, especially when paired with SOT or tax overlap.",
        },
        {
            "source": "TAX_OVERLAP",
            "directive": "expand_selectively",
            "reason": "Special-situations path with better upside than generic new-county expansion.",
        },
        {
            "source": "FSBO",
            "directive": "expand_selectively",
            "reason": "Seller-direct lane is worth pushing only when direct contact and pricing dislocation are both present.",
        },
        {
            "source": "FORECLOSURE_NOTICE",
            "directive": "deprioritize",
            "reason": "Later-stage signal. Useful for lifecycle, weaker for upstream quality creation.",
        },
    ]

    return {
        "generated_at": _utc_now(),
        "counties": county_actions[:10],
        "sources": source_actions,
    }


def _lead_next_action(lead: sqlite3.Row, quality: dict[str, Any], overlap_signals: List[str], live_lookup: set[str]) -> tuple[str, str, List[str]]:
    lead_key = str(lead["lead_key"] or "")
    sale_status = str(lead["sale_status"] or "").strip().lower()
    distress_type = str(lead["distress_type"] or "").strip().upper()
    execution = quality.get("execution_reality") or {}
    packetability_band = str(quality.get("packetability_band") or "LOW").upper()
    packetability_score = int(quality.get("packetability_score") or 0)
    reasons: List[str] = []

    if distress_type == "FSBO":
        if lead_key in live_lookup and not bool(quality.get("fsbo_vault_ready")):
            reasons.append("Seller-direct opportunity no longer clears the live bar")
            return "remove_from_vault", "high", reasons
        if bool(quality.get("fsbo_vault_ready")):
            reasons.append("Clears seller-direct live bar with direct contact and actionable pricing")
            return "publish", "high", reasons
        if bool(quality.get("fsbo_review_ready")):
            reasons.extend((quality.get("fsbo_actionability_reasons") or ["Seller-direct file is worth operator review"])[:2])
            return "monitor", "medium", reasons
        if str(execution.get("contact_path_quality") or "THIN").upper() == "THIN":
            reasons.append("Direct seller contact is missing")
            return "suppress", "medium", reasons
        reasons.extend((quality.get("vault_publish_blockers") or quality.get("execution_blockers") or ["Seller-direct opportunity is not actionable enough yet"])[:2])
        return "suppress", "low", reasons

    if lead_key in live_lookup and sale_status == "pre_foreclosure" and not bool(quality.get("prefc_live_quality")):
        reasons.extend(quality.get("prefc_live_review_reasons") or ["Live quality slipped below target"])
        return "remove_from_vault", "high", reasons

    if sale_status == "pre_foreclosure" and bool(quality.get("prefc_live_quality")) and str(quality.get("debt_confidence") or "").upper() == "FULL":
        reasons.append("Clears strong live pre-foreclosure bar")
        return "publish", "high", reasons

    if sale_status == "scheduled" and bool(quality.get("vault_publish_ready")):
        contact_quality = str(execution.get("contact_path_quality") or "THIN").upper()
        workability = str(execution.get("workability_band") or "LIMITED").upper()
        equity_band = str(lead.get("equity_band") or "").upper()
        debt_confidence = str(quality.get("debt_confidence") or "").upper()
        if (
            debt_confidence == "FULL"
            and equity_band in {"MED", "HIGH"}
            and contact_quality in {"GOOD", "STRONG"}
            and workability in {"MODERATE", "STRONG", "LIMITED"}
        ):
            reasons.append("Clears scheduled foreclosure publish bar")
            return "publish", "high", reasons

    if bool(quality.get("suppress_early")):
        reasons.extend((quality.get("early_noise_reasons") or ["Lead is low-signal noise relative to the current lane"])[:2])
        return "suppress", "high", reasons

    has_record_refs = bool(
        lead.get("mortgage_record_book")
        or lead.get("mortgage_record_page")
        or lead.get("mortgage_record_instrument")
    )
    debt_missing_reason = str(lead.get("debt_reconstruction_missing_reason") or "").strip()
    blocker_type = str(lead.get("debt_reconstruction_blocker_type") or "").strip().lower()
    recoverable_partial = bool(quality.get("recoverable_partial"))
    recoverable_next_step = str(quality.get("recoverable_partial_next_step") or "").strip().lower()

    if prefc_is_special_situation(overlap_signals):
        reasons.append("Overlap signals increase upside versus ordinary notice flow")
        if not lead.get("mortgage_lender") or lead.get("mortgage_amount") is None:
            if has_record_refs:
                reasons.append("Recorded debt refs are present, but loan amount is still missing")
                if debt_missing_reason:
                    reasons.append(debt_missing_reason)
                return "county_record_lookup", "high", reasons
            reasons.append("Debt stack still needs reconstruction before it can convert")
            return "reconstruct_debt", "high", reasons
        if not lead.get("last_sale_date") and not lead.get("mortgage_date"):
            reasons.append("Transfer support still missing on a special-situations lead")
            return "reconstruct_transfer", "high", reasons
        return "special_situations_review", "high", reasons

    if recoverable_partial and recoverable_next_step:
        reasons.extend((quality.get("recoverable_partial_reasons") or ["Lead is close enough to justify another recovery pass"])[:2])
        return recoverable_next_step, "high" if packetability_band == "HIGH" else "medium", reasons

    if not lead.get("mortgage_lender") or lead.get("mortgage_amount") is None:
        if has_record_refs:
            reasons.append("County record refs are present but the debt amount is still missing")
            if debt_missing_reason:
                reasons.append(debt_missing_reason)
            return "county_record_lookup", "high", reasons
        if blocker_type == "missing_amount_notice":
            reasons.append("Notice-derived debt clues are present but still incomplete")
            if debt_missing_reason:
                reasons.append(debt_missing_reason)
            return "reconstruct_debt", "high", reasons
        reasons.append("Debt picture is still incomplete")
        return "reconstruct_debt", "high", reasons

    if not lead.get("last_sale_date") and not lead.get("mortgage_date"):
        reasons.append("Transfer support is still missing")
        return "reconstruct_transfer", "high", reasons

    if str(execution.get("contact_path_quality") or "THIN").upper() == "THIN":
        reasons.append("Actionable contact path is missing")
        return "enrich_contact", "medium", reasons

    if prefc_is_special_situation(overlap_signals):
        reasons.append("Special-situations overlap makes this worth a tighter review path")
        return "special_situations_review", "medium", reasons

    if str(execution.get("lender_control_intensity") or "HIGH").upper() == "HIGH" or str(lead.get("equity_band") or "").upper() == "LOW":
        reasons.append("Control or equity makes the file weak for live vault")
        return "monitor", "medium", reasons

    if packetability_band == "HIGH" or packetability_score >= 10:
        reasons.extend((quality.get("packetability_reasons") or ["This file is close enough to stay in the active review queue"])[:2])
        return "monitor", "medium", reasons

    reasons.append("Keep tracking automatically after the current pass")
    return "monitor", "low", reasons


def determine_lead_action(
    lead: sqlite3.Row | dict[str, Any],
    quality: dict[str, Any],
    overlap_signals: List[str],
    live_rows: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    lead_dict = dict(lead)
    live_lookup = {
        str(row.get("sourceLeadKey") or "").strip()
        for row in (live_rows or [])
        if str(row.get("sourceLeadKey") or "").strip()
    }
    action, priority, reasons = _lead_next_action(lead_dict, quality, overlap_signals, live_lookup)
    return {
        "next_action": action,
        "priority": priority,
        "reasons": reasons,
    }


def _build_lead_actions(con: sqlite3.Connection, live_rows: list[dict[str, Any]], limit: int = 14) -> dict[str, Any]:
    live_lookup = {str(row.get("sourceLeadKey") or "").strip() for row in live_rows if str(row.get("sourceLeadKey") or "").strip()}
    attom_map = _latest_attom_map(con)
    rows = con.execute(
        """
        SELECT
          lead_key,
          address,
          county,
          distress_type,
          sale_status,
          falco_score_internal,
          auction_readiness,
          equity_band,
          current_sale_date,
          original_sale_date,
          first_seen_at,
          last_seen_at,
          score_updated_at
        FROM leads
        WHERE sale_status='pre_foreclosure'
           OR UPPER(COALESCE(distress_type, '')) = 'FSBO'
        ORDER BY COALESCE(falco_score_internal, 0) DESC, COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
        LIMIT 40
        """
    ).fetchall()

    actions: list[dict[str, Any]] = []
    for lead in rows:
        fields = _hydrate_fields(con, lead, attom_map)
        quality = assess_packet_data(fields)
        overlaps = _overlap_signals(lead, con)
        next_action, priority, reasons = _lead_next_action(fields, quality, overlaps, live_lookup)
        execution = quality.get("execution_reality") or {}
        actions.append(
            {
                "lead_key": lead["lead_key"],
                "address": lead["address"],
                "county": lead["county"],
                "distress_type": lead["distress_type"],
                "sale_status": lead["sale_status"],
                "falco_score_internal": lead["falco_score_internal"],
                "equity_band": lead["equity_band"],
                "debt_confidence": quality.get("debt_confidence"),
                "debt_reconstruction_confidence": fields.get("debt_reconstruction_confidence"),
                "debt_reconstruction_source_mix": fields.get("debt_reconstruction_source_mix"),
                "debt_reconstruction_missing_reason": fields.get("debt_reconstruction_missing_reason"),
                "debt_reconstruction_blocker_type": fields.get("debt_reconstruction_blocker_type"),
                "debt_reconstruction_summary": fields.get("debt_reconstruction_summary"),
                "packetability_score": quality.get("packetability_score"),
                "packetability_band": quality.get("packetability_band"),
                "recoverable_partial": bool(quality.get("recoverable_partial")),
                "recoverable_partial_next_step": quality.get("recoverable_partial_next_step"),
                "suppress_early": bool(quality.get("suppress_early")),
                "county_record_lookup_status": fields.get("county_record_lookup_status"),
                "county_record_lookup_provider": fields.get("county_record_lookup_provider"),
                "county_record_lookup_url": fields.get("county_record_lookup_url"),
                "county_record_lookup_hint": fields.get("county_record_lookup_hint"),
                "county_record_lookup_refs": fields.get("county_record_lookup_refs"),
                "mortgage_record_book": fields.get("mortgage_record_book"),
                "mortgage_record_page": fields.get("mortgage_record_page"),
                "mortgage_record_instrument": fields.get("mortgage_record_instrument"),
                "prefc_live_quality": bool(quality.get("prefc_live_quality")),
                "contact_path_quality": execution.get("contact_path_quality"),
                "owner_agency": execution.get("owner_agency"),
                "intervention_window": execution.get("intervention_window"),
                "lender_control_intensity": execution.get("lender_control_intensity"),
                "influenceability": execution.get("influenceability"),
                "workability_band": execution.get("workability_band"),
                "overlap_signals": overlaps,
                "next_action": next_action,
                "priority": priority,
                "reasons": reasons,
            }
        )

    actions.sort(
        key=lambda row: (
            {"high": 0, "medium": 1, "low": 2}.get(str(row["priority"]), 3),
            0 if row["next_action"] in {"publish", "remove_from_vault", "county_record_lookup", "reconstruct_debt", "reconstruct_transfer"} else 1,
            prefc_overlap_priority(row["overlap_signals"]),
            prefc_county_priority(row["county"]),
            prefc_source_priority(row["distress_type"]),
            -float(row["falco_score_internal"] or 0),
        )
    )
    return {
        "generated_at": _utc_now(),
        "actions": actions[:limit],
    }


def _build_vault_quality(live_rows: list[dict[str, Any]]) -> dict[str, Any]:
    review: list[dict[str, Any]] = []
    keep_count = 0
    watch_count = 0
    remove_count = 0
    for row in live_rows:
        sale_status = str(row.get("saleStatus") or "").strip().lower()
        reasons = list(row.get("prefcLiveReviewReasons") or [])

        if sale_status == "pre_foreclosure":
            if bool(row.get("prefcLiveQuality")) and str(row.get("debtConfidence") or "").upper() == "FULL":
                decision = "keep_live"
                keep_count += 1
            elif str(row.get("equityBand") or "").upper() == "LOW" or str(row.get("debtConfidence") or "").upper() != "FULL":
                decision = "remove_from_vault"
                remove_count += 1
                if not reasons:
                    reasons = ["Live pre-foreclosure no longer clears the strong-live bar"]
            else:
                decision = "watch_live"
                watch_count += 1
                if not reasons:
                    reasons = ["Needs operator watch until the next quality refresh"]
        elif sale_status == "scheduled":
            top_tier = bool(row.get("topTierReady"))
            readiness = str(row.get("auctionReadiness") or "").strip().upper()
            equity_band = str(row.get("equityBand") or "").strip().upper()
            workability = str(row.get("workabilityBand") or "").strip().upper()
            contact_quality = str(row.get("contactPathQuality") or "").strip().upper()
            if top_tier:
                decision = "keep_live"
                keep_count += 1
            elif readiness != "GREEN" and (
                equity_band in {"LOW", "UNKNOWN", ""}
                or workability not in {"STRONG"}
                or contact_quality not in {"GOOD", "STRONG"}
            ):
                decision = "remove_from_vault"
                remove_count += 1
                reasons = ["Live foreclosure no longer clears the stronger vault bar"]
            else:
                decision = "watch_live"
                watch_count += 1
                reasons = ["Scheduled foreclosure is usable, but not strong enough to be treated as top shelf"]
        else:
            continue

        review.append(
            {
                "slug": row.get("slug"),
                "lead_key": row.get("sourceLeadKey"),
                "title": row.get("title"),
                "county": row.get("county"),
                "equityBand": row.get("equityBand"),
                "debtConfidence": row.get("debtConfidence"),
                "prefcLiveQuality": bool(row.get("prefcLiveQuality")),
                "workabilityBand": row.get("workabilityBand"),
                "decision": decision,
                "reasons": reasons,
            }
        )

    review.sort(
        key=lambda row: (
            {"remove_from_vault": 0, "watch_live": 1, "keep_live": 2}.get(str(row["decision"]), 3),
            prefc_county_priority(row["county"]),
            0 if str(row.get("equityBand") or "").upper() in {"MED", "HIGH"} else 1,
            0 if bool(row.get("topTierReady")) else 1,
        )
    )

    return {
        "generated_at": _utc_now(),
        "keepCount": keep_count,
        "watchCount": watch_count,
        "removeCount": remove_count,
        "liveReview": review,
    }


def build_autonomy_report(run_id: str | None = None, run_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    if run_summary is None:
        run_summary = _load_latest_run_summary()
    live_rows = _load_live_rows()
    con = _connect()
    try:
        market_allocation = _build_market_allocation(run_summary)
        lead_actions = _build_lead_actions(con, live_rows)
        vault_quality = _build_vault_quality(live_rows)
    finally:
        con.close()

    return {
        "agent": "falco_autonomy",
        "generated_at": _utc_now(),
        "run_id": run_id,
        "objective": "Increase strong live opportunities while keeping the vault limited to commercially credible files.",
        "marketAllocation": market_allocation,
        "leadActions": lead_actions,
        "vaultQuality": vault_quality,
    }


def write_autonomy_report(run_id: str | None = None, run_summary: dict[str, Any] | None = None) -> dict[str, Any]:
    report = build_autonomy_report(run_id=run_id, run_summary=run_summary)
    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_autonomy.json" if run_id else reports_dir / "run_manual_autonomy.json"
    latest_path = reports_dir / "latest_autonomy.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    print(f"[AutonomyAgents] wrote {run_path}")
    return {"ok": True, "path": str(run_path), "report": report}
