from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from ..enrichment.attom_enricher import run as run_attom_enrichment
from ..enrichment.batchdata_fallback import run as run_batchdata_fallback
from ..enrichment.county_record_lookup import run as run_county_record_lookup
from ..enrichment.debt_reconstruction import run as run_debt_reconstruction
from ..packaging.data_quality import assess_packet_data
from ..packaging.packager import run as run_packager
from ..scoring.scorer import score_leads_by_keys
from .autonomy_agents import determine_lead_action
from .prefc_policy import (
    prefc_county_is_active,
    prefc_county_priority,
    prefc_is_special_situation,
    prefc_overlap_priority,
    prefc_source_priority,
)
from .site_publish import _load_env_file, _run_command
from .site_snapshots import (
    SITE_REPO,
    SITE_VAULT_LISTINGS,
    _build_publish_candidates,
    _connect,
    _hydrate_quality_fields,
    _load_live_slugs,
)

_PUSH_HARDER_BUDGET_COUNTIES = {"rutherford county", "davidson county"}
_HIGH_QUALITY_SOURCE_TYPES = {"SOT", "SUBSTITUTION_OF_TRUSTEE", "LIS_PENDENS"}


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _county_budget_boost(county: str | None) -> int:
    normalized = str(county or "").strip().lower()
    if normalized in _PUSH_HARDER_BUDGET_COUNTIES:
        return 2
    return 0


def _source_quality_boost(source_value: str | None) -> int:
    normalized = str(source_value or "").strip().upper()
    if normalized in _HIGH_QUALITY_SOURCE_TYPES:
        return 2
    if normalized in {"FORECLOSURE", "FORECLOSURE_TN"}:
        return 0
    return 1


def _candidate_publish_issues(payload: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    sale_status = str(payload.get("saleStatus") or "").strip().lower()
    equity_band = str(payload.get("equityBand") or "").strip().upper()
    if not str(payload.get("ownerName") or "").strip():
        issues.append("owner")
    if not str(payload.get("ownerMail") or "").strip():
        issues.append("mailing")
    if not str(payload.get("mortgageLender") or "").strip():
        issues.append("lender")
    mortgage_amount = payload.get("mortgageAmount")
    if not isinstance(mortgage_amount, (int, float)):
        issues.append("loan amount")

    has_contact = any(
        str(payload.get(key) or "").strip()
        for key in ("ownerPhonePrimary", "ownerPhoneSecondary", "trusteePhonePublic", "noticePhone")
    )
    if not has_contact:
        issues.append("contact path")
    if sale_status == "pre_foreclosure":
        if not equity_band or equity_band == "UNKNOWN":
            issues.append("equity / valuation")
        elif equity_band == "LOW":
            issues.append("equity risk")

    return issues


def _load_existing_site_rows() -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    if not SITE_VAULT_LISTINGS.exists():
        return rows

    for raw_line in SITE_VAULT_LISTINGS.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict) and row.get("slug"):
            rows[str(row["slug"])] = row
    return rows


def _write_site_rows(rows: dict[str, dict[str, Any]]) -> None:
    ordered = sorted(rows.values(), key=lambda row: str(row.get("createdAt") or ""))
    payload = "\n".join(json.dumps(row, ensure_ascii=False) for row in ordered)
    SITE_VAULT_LISTINGS.write_text(payload + ("\n" if payload else ""), encoding="utf-8")


def _has_hard_contact_fields(payload: dict[str, Any]) -> bool:
    return any(
        str(payload.get(key) or "").strip()
        for key in ("ownerPhonePrimary", "ownerPhoneSecondary", "trusteePhonePublic", "noticePhone")
    )


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


def _overlap_signals(con: sqlite3.Connection, lead: sqlite3.Row | dict[str, Any]) -> list[str]:
    lead_key = str((dict(lead) if not isinstance(lead, dict) else lead).get("lead_key") or "").strip()
    if not lead_key:
        return []
    sources = _source_set(con, lead_key)
    signals: list[str] = []
    if "SUBSTITUTION_OF_TRUSTEE" in sources and "LIS_PENDENS" in sources:
        signals.append("stacked_notice_path")
    if sources.intersection({"API_TAX", "OFFICIAL_TAX_SALE", "TAXPAGES"}):
        signals.append("tax_overlap")
    current_sale_date = str((dict(lead) if not isinstance(lead, dict) else lead).get("current_sale_date") or "").strip()
    original_sale_date = str((dict(lead) if not isinstance(lead, dict) else lead).get("original_sale_date") or "").strip()
    if current_sale_date and original_sale_date and current_sale_date != original_sale_date:
        signals.append("reopened_timing")
    return signals


def _prefc_retry_targets(limit: int) -> list[dict[str, Any]]:
    targets: list[dict[str, Any]] = []
    with _connect() as con:
        live_slugs = _load_live_slugs()
        attom_map: dict[str, dict[str, Any]] = {}
        for row in con.execute(
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
            SELECT lead_key, attom_raw_json, avm_value, avm_low, avm_high
            FROM latest_attom
            WHERE rn = 1
            """
        ).fetchall():
            attom_map[str(row["lead_key"])] = dict(row)

        lead_rows = con.execute(
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
              dts_days,
              first_seen_at,
              last_seen_at,
              score_updated_at
            FROM leads
            WHERE sale_status='pre_foreclosure'
            ORDER BY COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
            LIMIT 80
            """
        ).fetchall()

        for lead in lead_rows:
            lead_key = str(lead["lead_key"] or "").strip()
            if not lead_key:
                continue

            hydrated = _hydrate_quality_fields(con, lead, attom_map)
            quality = assess_packet_data(hydrated)
            execution_reality = quality.get("execution_reality") or {}
            lane_suggestion = quality.get("lane_suggestion") or {}
            county = str(lead["county"] or "").strip()
            overlap_signals = _overlap_signals(con, lead)
            special_situation = prefc_is_special_situation(overlap_signals)

            if not prefc_county_is_active(county):
                continue

            prefix = lead_key[:8].lower()
            matched = next((slug for slug in live_slugs if slug.lower().endswith(prefix)), None)
            if matched:
                continue

            owner_agency = str(execution_reality.get("owner_agency") or "LOW").upper()
            intervention_window = str(execution_reality.get("intervention_window") or "COMPRESSED").upper()
            lender_control = str(execution_reality.get("lender_control_intensity") or "HIGH").upper()
            influenceability = str(execution_reality.get("influenceability") or "LOW").upper()
            contact_path = str(execution_reality.get("contact_path_quality") or "THIN").upper()
            lane = str(lane_suggestion.get("suggested_execution_lane") or "unclear").lower()
            confidence = str(lane_suggestion.get("confidence") or "LOW").upper()
            blockers = list(quality.get("pre_foreclosure_review_blockers") or [])
            batchdata_targets = list(quality.get("batchdata_fallback_targets") or [])
            packetability_band = str(quality.get("packetability_band") or "LOW").upper()
            packetability_score = int(quality.get("packetability_score") or 0)
            recoverable_partial = bool(quality.get("recoverable_partial"))
            recoverable_next_step = str(quality.get("recoverable_partial_next_step") or "").strip().lower()
            suppress_early = bool(quality.get("suppress_early"))
            missing_valuation = "Valuation anchors missing" in blockers or str(lead["equity_band"] or "").upper() in {"", "UNKNOWN"}
            debt_ready = bool(
                hydrated.get("mortgage_lender")
                and hydrated.get("mortgage_amount") is not None
                and hydrated.get("last_sale_date")
            )
            hard_contact_gap = not _has_hard_contact_fields(
                {
                    "ownerPhonePrimary": hydrated.get("owner_phone_primary"),
                    "ownerPhoneSecondary": hydrated.get("owner_phone_secondary"),
                    "trusteePhonePublic": hydrated.get("trustee_phone_public"),
                    "noticePhone": hydrated.get("notice_phone"),
                }
            )
            contact_gap = (
                "Actionable outreach path missing" in blockers
                or contact_path not in {"GOOD", "STRONG"}
                or hard_contact_gap
            )

            strong_staged_contact_retry = bool(
                quality.get("pre_foreclosure_review_ready")
                and owner_agency in {"HIGH", "MEDIUM"}
                and intervention_window in {"WIDE", "MODERATE"}
                and lender_control == "LOW"
                and influenceability == "HIGH"
                and lane != "unclear"
                and confidence == "HIGH"
                and hard_contact_gap
            )

            if quality.get("pre_foreclosure_review_ready") and not strong_staged_contact_retry and not recoverable_partial:
                continue
            if suppress_early and not recoverable_partial and not special_situation:
                continue
            if owner_agency == "LOW" and not recoverable_partial:
                continue
            if intervention_window == "COMPRESSED" and lender_control == "HIGH" and not recoverable_partial:
                continue
            if (influenceability == "LOW" or lane == "unclear" or confidence == "LOW") and not recoverable_partial and not special_situation:
                continue
            if len(blockers) > 5:
                continue
            if packetability_band == "LOW" and packetability_score < 6 and not recoverable_partial:
                continue
            if not (missing_valuation or batchdata_targets or contact_gap or special_situation or recoverable_partial):
                continue

            decision = determine_lead_action(hydrated, quality, overlap_signals, [])
            next_action = str(decision.get("next_action") or "").strip().lower()

            targets.append(
                {
                    "lead_key": lead_key,
                    "county": county,
                    "needs_attom": bool(missing_valuation and debt_ready),
                    "needs_batchdata": bool(batchdata_targets or contact_gap),
                    "needs_debt_reconstruction": next_action in {"reconstruct_debt", "county_record_lookup"},
                    "needs_transfer_reconstruction": next_action == "reconstruct_transfer",
                    "needs_contact_recovery": next_action == "enrich_contact" or hard_contact_gap,
                    "next_action": next_action,
                    "blocker_type": str(hydrated.get("debt_reconstruction_blocker_type") or "").strip().lower(),
                    "packetability_band": packetability_band,
                    "packetability_score": packetability_score,
                    "recoverable_partial": recoverable_partial,
                    "recoverable_next_step": recoverable_next_step,
                    "score": float(lead["falco_score_internal"] or 0),
                    "confidence": confidence,
                    "owner_agency": owner_agency,
                    "intervention_window": intervention_window,
                    "lender_control": lender_control,
                    "hard_contact_gap": hard_contact_gap,
                    "staged_contact_retry": strong_staged_contact_retry,
                    "special_situation": special_situation,
                    "overlap_signals": overlap_signals,
                    "county_priority": prefc_county_priority(county),
                    "source_priority": prefc_source_priority(str(lead["distress_type"] or "")),
                }
            )

    targets.sort(
        key=lambda row: (
            -_county_budget_boost(row.get("county")),
            -_source_quality_boost(row.get("distress_type")),
            0 if row["next_action"] in {"county_record_lookup", "reconstruct_debt"} else 1,
            0 if row["next_action"] == "reconstruct_transfer" else 1,
            0 if row["recoverable_partial"] else 1,
            row["county_priority"],
            prefc_overlap_priority(row["overlap_signals"]),
            0 if row["special_situation"] else 1,
            0 if row["blocker_type"] == "missing_amount_with_refs" else 1,
            0 if row["packetability_band"] == "HIGH" else 1 if row["packetability_band"] == "MEDIUM" else 2,
            -row["packetability_score"],
            0 if row["needs_attom"] else 1,
            row["source_priority"],
            0 if row["confidence"] == "HIGH" else 1,
            -row["score"],
            0 if row["owner_agency"] == "HIGH" else 1,
        )
    )
    return targets[:limit]


def _apply_recovery_budget(targets: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    if not targets or limit <= 0:
        return []

    county_caps = {0: 4, 1: 3, 2: 2, 3: 1}
    selected: list[dict[str, Any]] = []
    county_counts: dict[str, int] = {}
    action_counts: dict[str, int] = {}
    action_caps = {
        "county_record_lookup": max(2, limit // 2),
        "reconstruct_debt": max(3, limit),
        "reconstruct_transfer": max(2, limit // 2),
        "enrich_contact": max(2, limit // 2),
        "special_situations_review": max(2, limit // 3),
    }

    for row in targets:
        county = str(row.get("county") or "")
        county_priority = int(row.get("county_priority") or 3)
        county_cap = county_caps.get(county_priority, 1)
        county_cap += _county_budget_boost(county)
        if _source_quality_boost(row.get("distress_type")) >= 2:
            county_cap += 1
        if row.get("special_situation") or row.get("source_priority") == 0:
            county_cap += 1
        if county_counts.get(county, 0) >= county_cap:
            continue

        next_action = str(row.get("next_action") or "")
        if next_action and action_counts.get(next_action, 0) >= action_caps.get(next_action, limit):
            continue

        selected.append(row)
        county_counts[county] = county_counts.get(county, 0) + 1
        if next_action:
            action_counts[next_action] = action_counts.get(next_action, 0) + 1
        if len(selected) >= limit:
            break

    return selected


def _prune_weak_live_prefc(limit: int) -> dict[str, Any]:
    if limit <= 0:
        return {"attempted": True, "pruned": 0, "slugs": []}

    existing = _load_existing_site_rows()
    prune_slugs: list[str] = []
    for slug, row in existing.items():
        if str(row.get("saleStatus") or "").strip().lower() != "pre_foreclosure":
            continue
        if str(row.get("status") or "").strip().lower() != "active":
            continue
        debt_confidence = str(row.get("debtConfidence") or "").strip().upper()
        live_quality = bool(row.get("prefcLiveQuality"))
        equity_band = str(row.get("equityBand") or "").strip().upper()
        if debt_confidence != "FULL" or not live_quality or equity_band == "LOW":
            prune_slugs.append(slug)

    if not prune_slugs:
        return {"attempted": True, "pruned": 0, "slugs": []}

    prune_slugs = prune_slugs[:limit]
    for slug in prune_slugs:
        existing.pop(slug, None)
    _write_site_rows(existing)
    return {"attempted": True, "pruned": len(prune_slugs), "slugs": prune_slugs}


def _prune_moderate_live_foreclosures(limit: int) -> dict[str, Any]:
    if limit <= 0:
        return {"attempted": True, "pruned": 0, "slugs": []}

    existing = _load_existing_site_rows()
    prune_slugs: list[str] = []
    for slug, row in existing.items():
        if str(row.get("saleStatus") or "").strip().lower() != "scheduled":
            continue
        if str(row.get("status") or "").strip().lower() != "active":
            continue
        if bool(row.get("topTierReady")):
            continue

        readiness = str(row.get("auctionReadiness") or "").strip().upper()
        equity_band = str(row.get("equityBand") or "").strip().upper()
        workability = str(row.get("workabilityBand") or "").strip().upper()
        contact_quality = str(row.get("contactPathQuality") or "").strip().upper()

        if (
            readiness != "GREEN"
            and (
                equity_band in {"LOW", "UNKNOWN", ""}
                or workability not in {"STRONG"}
                or contact_quality not in {"GOOD", "STRONG"}
            )
        ):
            prune_slugs.append(slug)

    if not prune_slugs:
        return {"attempted": True, "pruned": 0, "slugs": []}

    prune_slugs = prune_slugs[:limit]
    for slug in prune_slugs:
        existing.pop(slug, None)
    _write_site_rows(existing)
    return {"attempted": True, "pruned": len(prune_slugs), "slugs": prune_slugs}


def _run_targeted_enrichment(run_id: str) -> dict[str, Any]:
    if not _truthy(os.environ.get("FALCO_AUTO_PREFC_ENRICH", "1")):
        return {"attempted": False, "enabled": False, "reason": "FALCO_AUTO_PREFC_ENRICH disabled"}

    limit = max(int(os.environ.get("FALCO_AUTO_PREFC_ENRICH_LIMIT", "10")), 0)
    targets = _apply_recovery_budget(_prefc_retry_targets(limit * 3), limit)
    if not targets:
        return {"attempted": True, "enabled": True, "requested": 0, "processed": 0, "publishedCandidates": 0}

    attom_keys = [row["lead_key"] for row in targets if row["needs_attom"]]
    batchdata_keys = [row["lead_key"] for row in targets if row["needs_batchdata"] or row["needs_contact_recovery"]]
    debt_recon_keys = [row["lead_key"] for row in targets if row["needs_debt_reconstruction"] or row["needs_transfer_reconstruction"]]
    all_keys = sorted({row["lead_key"] for row in targets})

    env_backup = {
        key: os.environ.get(key)
        for key in (
            "FALCO_STAGE2_SOURCE",
            "FALCO_ATTOM_TARGET_LEAD_KEYS",
            "FALCO_ATTOM_MAX_ENRICH",
            "FALCO_MAX_ATTOM_CALLS_PER_RUN",
            "FALCO_BATCHDATA_TARGET_LEAD_KEYS",
            "FALCO_DEBT_RECON_TARGET_LEAD_KEYS",
            "FALCO_COUNTY_LOOKUP_TARGET_LEAD_KEYS",
        )
    }

    attom_result: dict[str, Any] | None = None
    batchdata_result: dict[str, Any] | None = None
    county_lookup_result: dict[str, Any] | None = None
    try:
        if attom_keys:
            os.environ["FALCO_STAGE2_SOURCE"] = "sqlite"
            os.environ["FALCO_ATTOM_TARGET_LEAD_KEYS"] = ",".join(attom_keys)
            os.environ["FALCO_ATTOM_MAX_ENRICH"] = str(max(len(attom_keys), 1))
            os.environ["FALCO_MAX_ATTOM_CALLS_PER_RUN"] = str(max(len(attom_keys) * 4, 4))
            attom_result = run_attom_enrichment()

        if batchdata_keys:
            os.environ["FALCO_BATCHDATA_TARGET_LEAD_KEYS"] = ",".join(batchdata_keys)
            batchdata_result = run_batchdata_fallback()

        county_lookup_keys = [row["lead_key"] for row in targets if row["next_action"] == "county_record_lookup"]
        if county_lookup_keys:
            os.environ["FALCO_COUNTY_LOOKUP_TARGET_LEAD_KEYS"] = ",".join(sorted(set(county_lookup_keys)))
            county_lookup_result = run_county_record_lookup()

        if debt_recon_keys:
            os.environ["FALCO_DEBT_RECON_TARGET_LEAD_KEYS"] = ",".join(sorted(set(debt_recon_keys)))
            run_debt_reconstruction()
        elif all_keys:
            os.environ["FALCO_DEBT_RECON_TARGET_LEAD_KEYS"] = ",".join(all_keys)
            run_debt_reconstruction()
        if all_keys:
            score_leads_by_keys(all_keys, run_id=f"{run_id}_auto_prefc")
            for lead_key in all_keys:
                os.environ["FALCO_REPACK_LEAD_KEY"] = lead_key
                run_packager()
    finally:
        os.environ.pop("FALCO_REPACK_LEAD_KEY", None)
        for key, value in env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    return {
        "attempted": True,
        "enabled": True,
        "requested": len(all_keys),
        "processed": len(all_keys),
        "attomTargets": attom_keys,
        "batchdataTargets": batchdata_keys,
        "debtReconTargets": debt_recon_keys,
        "countyLookup": county_lookup_result,
        "targetActions": [
            {
                "lead_key": row["lead_key"],
                "county": row["county"],
                "next_action": row["next_action"],
                "packetability_band": row["packetability_band"],
                "recoverable_partial": row["recoverable_partial"],
            }
            for row in targets
        ],
        "attom": attom_result,
        "batchdata": batchdata_result,
    }


def _strict_prefc_publish_candidates(limit: int) -> list[dict[str, Any]]:
    with _connect() as con:
        live_slugs = _load_live_slugs()
        candidates = _build_publish_candidates(con, live_slugs, limit=max(limit * 3, 24))

    filtered: list[dict[str, Any]] = []
    for candidate in candidates:
        payload = candidate.get("listingPayload") or {}
        if str(payload.get("saleStatus") or "").strip().lower() != "pre_foreclosure":
            continue
        if not prefc_county_is_active(str(payload.get("county") or "")):
            continue
        if _candidate_publish_issues(payload):
            continue
        if not bool(payload.get("preForeclosureReviewReady")):
            continue
        if not bool(payload.get("prefcLiveQuality")):
            continue
        if str(payload.get("debtConfidence") or "").upper() != "FULL":
            continue
        if str(payload.get("suggestedLaneConfidence") or "").upper() != "HIGH":
            continue
        if str(payload.get("contactPathQuality") or "").upper() not in {"GOOD", "STRONG"}:
            continue
        if str(payload.get("ownerAgency") or "").upper() not in {"HIGH", "MEDIUM"}:
            continue
        if str(payload.get("interventionWindow") or "").upper() not in {"WIDE", "MODERATE"}:
            continue
        if str(payload.get("lenderControlIntensity") or "").upper() != "LOW":
            continue
        if str(payload.get("influenceability") or "").upper() != "HIGH":
            continue
        if str(payload.get("executionPosture") or "").upper() not in {"OWNER ACTIONABLE", "MIXED / OPERATOR REVIEW"}:
            continue
        filtered.append(candidate)

    filtered.sort(
        key=lambda row: (
            -_county_budget_boost((row.get("listingPayload") or {}).get("county")),
            prefc_county_priority(str((row.get("listingPayload") or {}).get("county") or "")),
            prefc_overlap_priority((row.get("listingPayload") or {}).get("overlapSignals") or []),
            0 if bool((row.get("listingPayload") or {}).get("specialSituation")) else 1,
            prefc_source_priority(str((row.get("listingPayload") or {}).get("distressType") or "")),
            0 if str((row.get("listingPayload") or {}).get("ownerAgency") or "").upper() == "HIGH" else 1,
            -float(((row.get("listingPayload") or {}).get("falcoScore") or 0)),
            int(((row.get("listingPayload") or {}).get("dtsDays") or 9999)),
        )
    )
    return filtered[:limit]


def _publish_candidates(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    if not candidates:
        return {"attempted": True, "published": 0, "slugs": []}

    SITE_VAULT_LISTINGS.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_existing_site_rows()
    for candidate in candidates:
        payload = candidate.get("listingPayload")
        if isinstance(payload, dict) and payload.get("slug"):
            existing[str(payload["slug"])] = payload
    _write_site_rows(existing)

    site_env = os.environ.copy()
    site_env.update(_load_env_file(SITE_REPO / ".env.local"))
    import_result = _run_command(
        ["node", str(SITE_REPO / "scripts" / "import-vault-listings.mjs")],
        SITE_REPO,
        site_env,
    )
    if not import_result.get("ok"):
        return {
            "attempted": True,
            "published": 0,
            "slugs": [],
            "import": import_result,
            "ok": False,
        }

    return {
        "attempted": True,
        "published": len(candidates),
        "slugs": [str((candidate.get("listingPayload") or {}).get("slug") or "") for candidate in candidates],
        "import": import_result,
        "ok": True,
    }


def run(run_id: str) -> dict[str, Any]:
    enrichment_result = _run_targeted_enrichment(run_id)
    prune_limit = max(int(os.environ.get("FALCO_AUTO_PREFC_PRUNE_LIMIT", "3")), 0)
    prune_result = _prune_weak_live_prefc(prune_limit)
    foreclosure_prune_limit = max(int(os.environ.get("FALCO_AUTO_FORECLOSURE_PRUNE_LIMIT", "2")), 0)
    foreclosure_prune_result = _prune_moderate_live_foreclosures(foreclosure_prune_limit)

    publish_enabled = _truthy(os.environ.get("FALCO_AUTO_PUBLISH_VAULT", "1"))
    if not publish_enabled:
        return {
            "ok": True,
            "enrichment": enrichment_result,
            "prune": prune_result,
            "foreclosurePrune": foreclosure_prune_result,
            "publish": {
                "attempted": False,
                "enabled": False,
                "reason": "FALCO_AUTO_PUBLISH_VAULT not enabled",
            },
        }

    publish_limit = max(int(os.environ.get("FALCO_AUTO_PREFC_PUBLISH_LIMIT", "4")), 0)
    candidates = _strict_prefc_publish_candidates(publish_limit)
    publish_result = _publish_candidates(candidates)
    return {
        "ok": bool(publish_result.get("ok", True)),
        "enrichment": enrichment_result,
        "prune": prune_result,
        "foreclosurePrune": foreclosure_prune_result,
        "publish": {
            **publish_result,
            "enabled": True,
            "candidateCount": len(candidates),
        },
    }
