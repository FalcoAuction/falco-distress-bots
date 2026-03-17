from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from ..enrichment.attom_enricher import run as run_attom_enrichment
from ..enrichment.batchdata_fallback import run as run_batchdata_fallback
from ..enrichment.debt_reconstruction import run as run_debt_reconstruction
from ..packaging.data_quality import assess_packet_data
from ..packaging.packager import run as run_packager
from ..scoring.scorer import score_leads_by_keys
from .prefc_policy import prefc_county_is_active, prefc_county_priority, prefc_source_priority
from .site_publish import _load_env_file, _run_command
from .site_snapshots import (
    SITE_REPO,
    SITE_VAULT_LISTINGS,
    _build_publish_candidates,
    _connect,
    _hydrate_quality_fields,
    _load_live_slugs,
)


def _truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


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
            LIMIT 40
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

            if quality.get("pre_foreclosure_review_ready") and not strong_staged_contact_retry:
                continue
            if owner_agency == "LOW" or intervention_window == "COMPRESSED" or lender_control == "HIGH":
                continue
            if influenceability == "LOW" or lane == "unclear" or confidence == "LOW":
                continue
            if len(blockers) > 5:
                continue
            if not (missing_valuation or batchdata_targets or contact_gap):
                continue

            targets.append(
                {
                    "lead_key": lead_key,
                    "needs_attom": bool(missing_valuation and debt_ready),
                    "needs_batchdata": bool(batchdata_targets or contact_gap),
                    "score": float(lead["falco_score_internal"] or 0),
                    "confidence": confidence,
                    "owner_agency": owner_agency,
                    "intervention_window": intervention_window,
                    "lender_control": lender_control,
                    "hard_contact_gap": hard_contact_gap,
                    "staged_contact_retry": strong_staged_contact_retry,
                    "county_priority": prefc_county_priority(county),
                    "source_priority": prefc_source_priority(str(lead["distress_type"] or "")),
                }
            )

    targets.sort(
        key=lambda row: (
            row["county_priority"],
            0 if row["needs_attom"] else 1,
            row["source_priority"],
            0 if row["confidence"] == "HIGH" else 1,
            -row["score"],
            0 if row["owner_agency"] == "HIGH" else 1,
        )
    )
    return targets[:limit]


def _run_targeted_enrichment(run_id: str) -> dict[str, Any]:
    if not _truthy(os.environ.get("FALCO_AUTO_PREFC_ENRICH", "1")):
        return {"attempted": False, "enabled": False, "reason": "FALCO_AUTO_PREFC_ENRICH disabled"}

    limit = max(int(os.environ.get("FALCO_AUTO_PREFC_ENRICH_LIMIT", "6")), 0)
    targets = _prefc_retry_targets(limit)
    if not targets:
        return {"attempted": True, "enabled": True, "requested": 0, "processed": 0, "publishedCandidates": 0}

    attom_keys = [row["lead_key"] for row in targets if row["needs_attom"]]
    batchdata_keys = [row["lead_key"] for row in targets if row["needs_batchdata"]]
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
        )
    }

    attom_result: dict[str, Any] | None = None
    batchdata_result: dict[str, Any] | None = None
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

        if all_keys:
            os.environ["FALCO_DEBT_RECON_TARGET_LEAD_KEYS"] = ",".join(all_keys)
            run_debt_reconstruction()
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
            prefc_county_priority(str((row.get("listingPayload") or {}).get("county") or "")),
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

    publish_enabled = _truthy(os.environ.get("FALCO_AUTO_PUBLISH_VAULT"))
    if not publish_enabled:
        return {
            "ok": True,
            "enrichment": enrichment_result,
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
        "publish": {
            **publish_result,
            "enabled": True,
            "candidateCount": len(candidates),
        },
    }
