from __future__ import annotations

import json
import os
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

from ..packaging.data_quality import assess_packet_data
from ..storage.sqlite_store import init_db
from .prefc_policy import prefc_county_priority, prefc_county_tier, prefc_source_priority

ROOT = Path(__file__).resolve().parents[2]
SITE_REPO = ROOT.parent / "falco-site"
SITE_DATA_DIR = SITE_REPO / "data"
SITE_OPERATOR_DIR = SITE_DATA_DIR / "operator"
SITE_OUTREACH_DIR = SITE_DATA_DIR / "outreach"
SITE_VAULT_LISTINGS = SITE_DATA_DIR / "vault_listings.ndjson"
SITE_PRIVATE_PACKET_DIR = SITE_REPO / "private" / "vault" / "packets"
OUTREACH_DIR = ROOT / "out" / "outreach"
REPORTS_DIR = ROOT / "out" / "reports"
PACKETS_ROOT = ROOT / "out" / "packets"
SYSTEM_STATE_COMPANY = "__falco_system_state__"


def _db_path() -> Path:
    return Path(os.environ.get("FALCO_SQLITE_PATH", "data/falco.db"))


def _connect() -> sqlite3.Connection:
    init_db()
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_live_slugs() -> list[str]:
    if not SITE_VAULT_LISTINGS.exists():
        return []

    slugs: list[str] = []
    for line in SITE_VAULT_LISTINGS.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict):
            continue
        if row.get("status") != "active":
            continue
        slug = str(row.get("slug") or "").strip()
        if slug:
            slugs.append(slug)
    return slugs


def _lead_key_prefix(lead_key: str) -> str:
    return (lead_key or "")[:8].lower()


def _prefc_strength_sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        prefc_county_priority(row.get("county")),
        prefc_source_priority(row.get("distress_type") or row.get("distressType") or ""),
        0 if bool(row.get("prefcLiveQuality")) else 1,
        0 if str(row.get("debtConfidence") or "").upper() == "FULL" else 1,
        0 if str(row.get("equity_band") or row.get("equityBand") or "").upper() in {"HIGH", "MED"} else 1,
        0 if str(row.get("contactPathQuality") or "").upper() in {"STRONG", "GOOD"} else 1,
        0 if str(row.get("ownerAgency") or "").upper() == "HIGH" else 1,
        -float(row.get("falco_score_internal") or row.get("falcoScore") or 0),
        int(row.get("dts_days") or row.get("dtsDays") or 9999),
    )


def _slugify(text: str) -> str:
    value = (text or "").lower().strip()
    out: list[str] = []
    prev_dash = False
    for ch in value:
        if ch.isalnum():
            out.append(ch)
            prev_dash = False
        else:
            if not prev_dash:
                out.append("-")
                prev_dash = True
    slug = "".join(out).strip("-")
    return slug or "listing"


def _packet_for_lead(lead_key: str) -> Path | None:
    candidates = sorted(
        PACKETS_ROOT.rglob(f"{lead_key}.pdf"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None

    repack_candidates = [p for p in candidates if "unknown_run" in p.parts]
    if repack_candidates:
        return repack_candidates[0]

    return candidates[0]


def _masked_title(county: str, distress_type: str) -> str:
    county_text = county or "Target County"
    distress_text = distress_type or "Distress Opportunity"
    return f"{county_text} {distress_text}"


def _build_summary(
    county: str,
    distress_type: str,
    dts_days: int | None,
    readiness: str,
    contact_ready: bool,
) -> str:
    dts_text = f"{int(dts_days)} days" if dts_days is not None else "early-stage timing"
    contact_text = "contact ready" if contact_ready else "contact pending"
    return (
        f"{distress_type or 'Distress'} opportunity in {county or 'target market'} with "
        f"{readiness or 'unknown'} readiness, {contact_text}, and auction timing of {dts_text}."
    )


def _build_teaser(county: str, readiness: str, dts_days: int | None) -> str:
    parts = [
        f"County: {county or 'Unknown'}",
        f"Readiness: {readiness or 'Unknown'}",
    ]
    if dts_days is not None:
        parts.append(f"Auction In: {int(dts_days)} days")
    return " • ".join(parts)


def _attach_vault_state(rows: list[dict[str, Any]], live_slugs: list[str]) -> list[dict[str, Any]]:
    attached: list[dict[str, Any]] = []
    for row in rows:
        prefix = _lead_key_prefix(str(row.get("lead_key") or ""))
        matched = next((slug for slug in live_slugs if slug.lower().endswith(prefix)), None)
        attached.append(
            {
                **row,
                "vaultLive": bool(matched),
                "vaultSlug": matched,
            }
        )
    return attached


def _latest_foreclosure_recorded_at(con: sqlite3.Connection, lead_key: str) -> str | None:
    row = con.execute(
        """
        SELECT recorded_at
        FROM foreclosure_events
        WHERE lead_key = ? AND recorded_at IS NOT NULL
        ORDER BY COALESCE(event_at, recorded_at) DESC, event_key DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    return row["recorded_at"] if row and row["recorded_at"] is not None else None


def _meets_high_confidence_review_bar(quality: dict[str, Any], sale_status: str) -> bool:
    lane = str((quality.get("lane_suggestion") or {}).get("suggested_execution_lane") or "unclear")
    confidence = str((quality.get("lane_suggestion") or {}).get("confidence") or "LOW").upper()
    execution_reality = quality.get("execution_reality") or {}
    contact_path_quality = str(execution_reality.get("contact_path_quality") or "THIN").upper()
    control_party = str(execution_reality.get("control_party") or "UNCLEAR").upper()
    owner_agency = str(execution_reality.get("owner_agency") or "LOW").upper()
    intervention_window = str(execution_reality.get("intervention_window") or "COMPRESSED").upper()
    lender_control_intensity = str(execution_reality.get("lender_control_intensity") or "HIGH").upper()
    influenceability = str(execution_reality.get("influenceability") or "LOW").upper()
    execution_posture = str(execution_reality.get("execution_posture") or "NEEDS MORE CONTROL CLARITY").upper()
    workability_band = str(execution_reality.get("workability_band") or "LIMITED").upper()
    blockers = quality.get("execution_blockers") or []
    normalized_status = str(sale_status or "").strip().lower()

    if lane == "unclear" or confidence == "LOW":
        return False
    if contact_path_quality == "THIN":
        return False
    if control_party == "UNCLEAR":
        return False
    if owner_agency == "LOW":
        return False
    if intervention_window == "COMPRESSED":
        return False
    if lender_control_intensity == "HIGH":
        return False
    if influenceability == "LOW":
        return False
    if execution_posture == "NEEDS MORE CONTROL CLARITY":
        return False

    if normalized_status == "pre_foreclosure":
        return bool(
            quality.get("pre_foreclosure_review_ready")
            and workability_band in {"STRONG", "MODERATE"}
            and len(blockers) <= 2
        )

    return bool(
        quality.get("top_tier_ready")
        and quality.get("vault_publish_ready")
        and workability_band == "STRONG"
    )


def _build_vault_candidates(
    con: sqlite3.Connection,
    live_slugs: list[str],
    limit: int = 12,
) -> list[dict[str, Any]]:
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
        attom_map[row["lead_key"]] = dict(row)

    candidates: list[dict[str, Any]] = []
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
          COALESCE(uw_ready, 0) AS uw_ready
        FROM leads
        WHERE dts_days IS NOT NULL
        ORDER BY COALESCE(dts_days, 9999) ASC, COALESCE(falco_score_internal, 0) DESC
        LIMIT 50
        """
    ).fetchall()

    for lead in lead_rows:
        lead_key = str(lead["lead_key"] or "")
        prefix = _lead_key_prefix(lead_key)
        matched = next((slug for slug in live_slugs if slug.lower().endswith(prefix)), None)
        if matched:
            continue

        attom = attom_map.get(lead_key) or {}
        fields: dict[str, Any] = dict(lead)
        fields["contact_ready"] = _fetch_scalar(
            con.cursor(),
            """
            SELECT COUNT(*)
            FROM lead_field_provenance
            WHERE lead_key=?
              AND field_name='contact_ready'
              AND field_value_text='1'
            """,
            (lead_key,),
        ) > 0
        fields["attom_raw_json"] = attom.get("attom_raw_json")
        fields["value_anchor_mid"] = attom.get("avm_value")
        fields["value_anchor_low"] = attom.get("avm_low")
        fields["value_anchor_high"] = attom.get("avm_high")
        for field_name in (
            "trustee_phone_public",
            "owner_phone_primary",
            "owner_phone_secondary",
            "notice_phone",
            "owner_name",
            "owner_mail",
            "last_sale_date",
            "mortgage_lender",
            "property_identifier",
        ):
            row = con.execute(
                """
                SELECT field_value_text
                FROM lead_field_provenance
                WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
                ORDER BY created_at DESC, prov_id DESC
                LIMIT 1
                """,
                (lead_key, field_name),
            ).fetchone()
            if row and row[0]:
                fields[field_name] = row[0]
        for field_name in ("year_built", "building_area_sqft", "beds", "baths", "mortgage_amount"):
            row = con.execute(
                """
                SELECT field_value_num
                FROM lead_field_provenance
                WHERE lead_key=? AND field_name=? AND field_value_num IS NOT NULL
                ORDER BY created_at DESC, prov_id DESC
                LIMIT 1
                """,
                (lead_key, field_name),
            ).fetchone()
            if row and row[0] is not None:
                fields[field_name] = float(row[0])

        quality = assess_packet_data(fields)
        readiness = str(lead["auction_readiness"] or "").upper()
        publish_ready = bool(quality["vault_publish_ready"])
        if not publish_ready:
            continue
        if not _meets_high_confidence_review_bar(quality, str(dict(lead).get("sale_status") or "")):
            continue

        candidates.append(
            {
                "lead_key": lead_key,
                "address": lead["address"],
                "county": lead["county"],
                "distress_type": lead["distress_type"],
                "sale_status": lead["sale_status"],
                "falco_score_internal": lead["falco_score_internal"],
                "auction_readiness": lead["auction_readiness"],
                "equity_band": lead["equity_band"],
                "dts_days": lead["dts_days"],
                "uw_ready": lead["uw_ready"],
                "vaultLive": False,
                "vaultSlug": None,
                "vaultPublishReady": publish_ready,
                "preForeclosureReviewReady": bool(quality.get("pre_foreclosure_review_ready")),
                "topTierReady": bool(quality["top_tier_ready"]),
                "packetCompletenessPct": quality["packet_completeness_pct"],
                "executionBlockers": quality["execution_blockers"],
                "suggestedExecutionLane": quality["lane_suggestion"]["suggested_execution_lane"],
                "suggestedLaneConfidence": quality["lane_suggestion"]["confidence"],
                "contactPathQuality": quality["execution_reality"]["contact_path_quality"],
                "controlParty": quality["execution_reality"]["control_party"],
                "ownerAgency": quality["execution_reality"]["owner_agency"],
                "interventionWindow": quality["execution_reality"]["intervention_window"],
                "lenderControlIntensity": quality["execution_reality"]["lender_control_intensity"],
                "influenceability": quality["execution_reality"]["influenceability"],
                "executionPosture": quality["execution_reality"]["execution_posture"],
                "workabilityBand": quality["execution_reality"]["workability_band"],
            }
        )

    candidates.sort(
        key=lambda row: (
            0 if row["vaultPublishReady"] else 1,
            0 if str(row.get("suggestedLaneConfidence") or "").upper() == "HIGH" else 1,
            0 if str(row.get("auction_readiness") or "").upper() == "GREEN" else 1,
            -(row.get("falco_score_internal") or 0),
            row.get("dts_days") or 9999,
        )
    )
    return candidates[:limit]


def _hydrate_quality_fields(
    con: sqlite3.Connection,
    lead_row: sqlite3.Row | dict[str, Any],
    attom_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    lead_key = str(lead_row["lead_key"] or "")
    fields: dict[str, Any] = dict(lead_row)
    fields["contact_ready"] = _fetch_scalar(
        con.cursor(),
        """
        SELECT COUNT(*)
        FROM lead_field_provenance
        WHERE lead_key=?
          AND field_name='contact_ready'
          AND field_value_text='1'
        """,
        (lead_key,),
    ) > 0
    attom = attom_map.get(lead_key) or {}
    fields["attom_raw_json"] = attom.get("attom_raw_json")
    fields["value_anchor_mid"] = attom.get("avm_value")
    fields["value_anchor_low"] = attom.get("avm_low")
    fields["value_anchor_high"] = attom.get("avm_high")

    for field_name in (
        "trustee_phone_public",
        "owner_phone_primary",
        "owner_phone_secondary",
        "notice_phone",
        "owner_name",
        "owner_mail",
        "last_sale_date",
        "mortgage_date",
        "mortgage_lender",
        "property_identifier",
    ):
        row = con.execute(
            """
            SELECT field_value_text
            FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
            ORDER BY created_at DESC, prov_id DESC
            LIMIT 1
            """,
            (lead_key, field_name),
        ).fetchone()
        if row and row[0]:
            fields[field_name] = row[0]

    for field_name in ("year_built", "building_area_sqft", "beds", "baths", "mortgage_amount"):
        row = con.execute(
            """
            SELECT field_value_num
            FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_num IS NOT NULL
            ORDER BY created_at DESC, prov_id DESC
            LIMIT 1
            """,
            (lead_key, field_name),
        ).fetchone()
        if row and row[0] is not None:
            fields[field_name] = float(row[0])

    return fields


def _build_candidate_listing_payload(
    con: sqlite3.Connection,
    lead: sqlite3.Row | dict[str, Any],
    quality: dict[str, Any],
    packet_file_name: str,
) -> dict[str, Any]:
    lead_data = dict(lead)
    sale_status = str(lead_data.get("sale_status") or "")
    county = str(lead_data.get("county") or "")
    state = "TN"
    distress_type = str(lead_data.get("distress_type") or "")
    display_distress_type = (
        "Pre-Foreclosure Review" if sale_status == "pre_foreclosure" else (distress_type or "Distress Opportunity")
    )
    title = _masked_title(county, display_distress_type)
    slug = f"{_slugify(title)}-{str(lead['lead_key'] or '')[:8]}"
    enriched_fields = quality.get("enriched_fields", {})
    readiness = str(lead["auction_readiness"] or "")
    if readiness == "GREEN" and not quality["top_tier_ready"]:
        readiness = "YELLOW"
    if sale_status == "pre_foreclosure" and readiness not in {"GREEN", "YELLOW", "PARTIAL"}:
        readiness = "PARTIAL"
    dts_days = int(lead_data["dts_days"]) if lead_data.get("dts_days") is not None else None
    contact_ready = bool(quality.get("contact_ready"))
    created_at = str(lead_data.get("score_updated_at") or lead_data.get("last_seen_at") or lead_data.get("first_seen_at") or _utc_now())
    distress_recorded_at = _latest_foreclosure_recorded_at(con, str(lead_data.get("lead_key") or ""))

    def _field(name: str) -> Any:
        return enriched_fields.get(name) or lead_data.get(name)

    return {
        "slug": slug,
        "title": title,
        "market": f"{county or 'Unknown County'}, {state}",
        "county": county,
        "state": state,
        "status": "active",
        "distressType": display_distress_type,
        "auctionWindow": "Pre-Foreclosure" if sale_status == "pre_foreclosure" else (f"{dts_days} Days" if dts_days is not None else "Confidential"),
        "summary": _build_summary(county, display_distress_type, dts_days, readiness, contact_ready),
        "publicTeaser": _build_teaser(county, readiness, dts_days),
        "packetUrl": f"/api/vault/packet?slug={slug}",
        "packetLabel": "Pre-Foreclosure Review Brief" if sale_status == "pre_foreclosure" else "Auction Opportunity Brief",
        "packetFileName": packet_file_name,
        "sourceLeadKey": lead_data["lead_key"],
        "createdAt": created_at,
        "expiresAt": "",
        "claimedAt": "",
        "claimedBy": "",
        "falcoScore": float(lead_data["falco_score_internal"]) if lead_data.get("falco_score_internal") is not None else None,
        "auctionReadiness": readiness,
        "equityBand": lead_data.get("equity_band") or "",
        "dtsDays": dts_days,
        "currentSaleDate": lead_data.get("current_sale_date") or "",
        "originalSaleDate": lead_data.get("original_sale_date") or "",
        "distressRecordedAt": distress_recorded_at or "",
        "contactReady": contact_ready,
        "propertyIdentifier": _field("property_identifier"),
        "ownerName": _field("owner_name"),
        "ownerMail": _field("owner_mail"),
        "ownerPhonePrimary": _field("owner_phone_primary"),
        "ownerPhoneSecondary": _field("owner_phone_secondary"),
        "trusteePhonePublic": _field("trustee_phone_public"),
        "noticePhone": _field("notice_phone"),
        "lastSaleDate": _field("last_sale_date"),
        "mortgageDate": _field("mortgage_date"),
        "mortgageLender": _field("mortgage_lender"),
        "mortgageAmount": _field("mortgage_amount"),
        "yearBuilt": _field("year_built"),
        "buildingAreaSqft": _field("building_area_sqft"),
        "beds": _field("beds"),
        "baths": _field("baths"),
        "contactPathQuality": quality["execution_reality"]["contact_path_quality"],
        "controlParty": quality["execution_reality"]["control_party"],
        "ownerAgency": quality["execution_reality"]["owner_agency"],
        "interventionWindow": quality["execution_reality"]["intervention_window"],
        "lenderControlIntensity": quality["execution_reality"]["lender_control_intensity"],
        "influenceability": quality["execution_reality"]["influenceability"],
        "executionPosture": quality["execution_reality"]["execution_posture"],
        "workabilityBand": quality["execution_reality"]["workability_band"],
        "suggestedExecutionLane": quality["lane_suggestion"]["suggested_execution_lane"],
        "suggestedLaneConfidence": quality["lane_suggestion"]["confidence"],
        "suggestedLaneReasons": quality["lane_suggestion"]["reasons"],
        "topTierReady": bool(quality["top_tier_ready"]),
        "vaultPublishReady": bool(quality["vault_publish_ready"]),
        "preForeclosureReviewReady": bool(quality.get("pre_foreclosure_review_ready")),
        "debtConfidence": quality.get("debt_confidence") or "",
        "prefcDebtProxyReady": bool(quality.get("prefc_debt_proxy_ready")),
        "prefcLiveQuality": bool(quality.get("prefc_live_quality")),
        "prefcLiveReviewReasons": quality.get("prefc_live_review_reasons") or [],
        "saleStatus": sale_status,
        "dataNotes": (
            (quality.get("pre_foreclosure_review_blockers") if sale_status == "pre_foreclosure" else quality["vault_publish_blockers"])
            + quality["execution_notes"]
        )[:4],
    }


def _build_publish_candidates(
    con: sqlite3.Connection,
    live_slugs: list[str],
    limit: int = 24,
) -> list[dict[str, Any]]:
    SITE_PRIVATE_PACKET_DIR.mkdir(parents=True, exist_ok=True)
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
        attom_map[row["lead_key"]] = dict(row)

    candidates: list[dict[str, Any]] = []
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
          COALESCE(uw_ready, 0) AS uw_ready,
          canonical_property_key,
          first_seen_at,
          last_seen_at,
          score_updated_at,
          current_sale_date,
          original_sale_date
        FROM leads
        WHERE COALESCE(dts_days, 9999) <= 90
           OR sale_status = 'pre_foreclosure'
        ORDER BY
          CASE WHEN sale_status = 'pre_foreclosure' THEN 1 ELSE 0 END,
          COALESCE(dts_days, 9999) ASC,
          COALESCE(falco_score_internal, 0) DESC
        LIMIT 120
        """
    ).fetchall()

    for lead in lead_rows:
        lead_key = str(lead["lead_key"] or "")
        prefix = _lead_key_prefix(lead_key)
        matched = next((slug for slug in live_slugs if slug.lower().endswith(prefix)), None)
        if matched:
            continue

        hydrated = _hydrate_quality_fields(con, lead, attom_map)
        quality = assess_packet_data(hydrated)
        publish_ready = bool(quality["vault_publish_ready"])
        if not publish_ready:
            continue
        if str(dict(lead).get("sale_status") or "").strip().lower() == "pre_foreclosure" and not bool(quality.get("prefc_live_quality")):
            continue
        if not _meets_high_confidence_review_bar(quality, str(dict(lead).get("sale_status") or "")):
            continue

        packet_path = _packet_for_lead(lead_key)
        if not packet_path:
            continue

        packet_file_name = f"{_slugify(_masked_title(str(lead['county'] or ''), 'Pre-Foreclosure Review' if str(lead['sale_status'] or '') == 'pre_foreclosure' else str(lead['distress_type'] or 'Distress Opportunity')))}-{lead_key[:8]}.pdf"
        staged_packet_path = SITE_PRIVATE_PACKET_DIR / packet_file_name
        shutil.copy2(packet_path, staged_packet_path)

        listing_payload = _build_candidate_listing_payload(con, hydrated, quality, packet_file_name)
        candidates.append(
            {
                "leadKey": lead_key,
                "address": lead["address"],
                "county": lead["county"],
                "distressType": lead["distress_type"],
                "saleStatus": lead["sale_status"],
                "canonicalPropertyKey": lead["canonical_property_key"],
                "slug": listing_payload["slug"],
                "packetFileName": packet_file_name,
                "listingPayload": listing_payload,
                "supabaseRow": {
                    "slug": listing_payload["slug"],
                    "title": listing_payload["title"],
                    "county": listing_payload["county"] or None,
                    "state": listing_payload["state"] or "TN",
                    "falco_score": listing_payload["falcoScore"],
                    "auction_readiness": listing_payload["auctionReadiness"] or None,
                    "equity_band": listing_payload["equityBand"] or None,
                    "dts_days": listing_payload["dtsDays"],
                    "packet_path": packet_file_name,
                    "is_active": True,
                },
            }
        )

    candidates.sort(
        key=lambda row: (
            0 if row["listingPayload"].get("topTierReady") else 1,
            0 if str(row["listingPayload"].get("auctionReadiness") or "").upper() == "GREEN" else 1,
            -(row["listingPayload"].get("falcoScore") or 0),
            row["listingPayload"].get("dtsDays") or 9999,
        )
    )
    return candidates[:limit]


def _build_pre_foreclosure_promotion(
    con: sqlite3.Connection,
    live_slugs: list[str],
    limit: int = 12,
) -> dict[str, Any]:
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
        attom_map[row["lead_key"]] = dict(row)

    ready_for_review: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    blocker_counts: dict[str, int] = {}

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
          COALESCE(uw_ready, 0) AS uw_ready,
          first_seen_at,
          last_seen_at,
          score_updated_at,
          current_sale_date,
          original_sale_date
        FROM leads
        WHERE sale_status='pre_foreclosure'
        ORDER BY COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
        LIMIT 30
        """
    ).fetchall()

    for lead in lead_rows:
        hydrated = _hydrate_quality_fields(con, lead, attom_map)
        quality = assess_packet_data(hydrated)
        prefix = _lead_key_prefix(str(lead["lead_key"] or ""))
        matched = next((slug for slug in live_slugs if slug.lower().endswith(prefix)), None)
        row = {
            **dict(lead),
            "vaultLive": bool(matched),
            "vaultSlug": matched,
            "preForeclosureReviewReady": bool(quality.get("pre_foreclosure_review_ready")),
            "vaultPublishReady": bool(quality["vault_publish_ready"]),
            "topTierReady": bool(quality["top_tier_ready"]),
            "prefcDebtProxyReady": bool(quality.get("prefc_debt_proxy_ready")),
            "debtConfidence": quality.get("debt_confidence") or "",
            "prefcLiveQuality": bool(quality.get("prefc_live_quality")),
            "prefcLiveReviewReasons": quality.get("prefc_live_review_reasons") or [],
            "packetCompletenessPct": quality["packet_completeness_pct"],
            "executionBlockers": quality["execution_blockers"],
        }

        row.update(
            {
                "suggestedExecutionLane": quality["lane_suggestion"]["suggested_execution_lane"],
                "suggestedLaneConfidence": quality["lane_suggestion"]["confidence"],
                "prefcCountyTier": quality.get("prefc_county_tier") or prefc_county_tier(lead["county"]),
                "prefcSourcePriority": quality.get("prefc_source_priority"),
                "contactPathQuality": quality["execution_reality"]["contact_path_quality"],
                "controlParty": quality["execution_reality"]["control_party"],
                "ownerAgency": quality["execution_reality"]["owner_agency"],
                "interventionWindow": quality["execution_reality"]["intervention_window"],
                "lenderControlIntensity": quality["execution_reality"]["lender_control_intensity"],
                "influenceability": quality["execution_reality"]["influenceability"],
                "executionPosture": quality["execution_reality"]["execution_posture"],
                "workabilityBand": quality["execution_reality"]["workability_band"],
                "ownerName": hydrated.get("owner_name"),
                "ownerMail": hydrated.get("owner_mail"),
                "mortgageDate": hydrated.get("mortgage_date"),
                "mortgageLender": hydrated.get("mortgage_lender"),
                "mortgageAmount": hydrated.get("mortgage_amount"),
                "propertyIdentifier": hydrated.get("property_identifier"),
                "ownerPhonePrimary": hydrated.get("owner_phone_primary"),
                "ownerPhoneSecondary": hydrated.get("owner_phone_secondary"),
                "trusteePhonePublic": hydrated.get("trustee_phone_public"),
                "noticePhone": hydrated.get("notice_phone"),
            }
        )

        if row["preForeclosureReviewReady"] and not row["vaultLive"] and _meets_high_confidence_review_bar(quality, "pre_foreclosure"):
            ready_for_review.append(row)
        else:
            blocked.append(row)
            for blocker in row["executionBlockers"]:
                blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1

    blocked.sort(
        key=lambda row: (
            0 if bool(row.get("vaultLive")) and not bool(row.get("prefcLiveQuality")) else 1,
            _prefc_strength_sort_key(row),
            len(row.get("executionBlockers") or []),
        )
    )
    ready_for_review.sort(key=_prefc_strength_sort_key)

    strongest_candidates = [
        row
        for row in ready_for_review
        if bool(row.get("prefcLiveQuality")) and str(row.get("debtConfidence") or "").upper() == "FULL"
    ]
    weak_live_review = [
        row
        for row in blocked
        if bool(row.get("vaultLive")) and not bool(row.get("prefcLiveQuality"))
    ]

    return {
        "readyCount": len(ready_for_review),
        "blockedCount": len(blocked),
        "readyForReview": ready_for_review[:limit],
        "blocked": blocked[:limit],
        "strongestCandidates": strongest_candidates[:limit],
        "weakLiveReview": weak_live_review[:limit],
        "blockerCounts": [
            {"label": label, "count": count}
            for label, count in sorted(blocker_counts.items(), key=lambda item: (-item[1], item[0]))
        ][:8],
    }


def _build_lifecycle_events(
    con: sqlite3.Connection,
    live_slugs: list[str],
    limit: int = 16,
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        SELECT
          e.event_key,
          e.lead_key,
          e.canonical_property_key,
          e.source,
          e.source_url,
          e.event_type,
          e.sale_date,
          e.derived_status,
          e.event_at,
          l.address,
          l.county,
          l.distress_type,
          l.current_sale_date,
          l.original_sale_date,
          l.sale_status
        FROM foreclosure_events e
        LEFT JOIN leads l ON l.lead_key = e.lead_key
        ORDER BY COALESCE(e.event_at, e.recorded_at) DESC, e.event_key DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()

    return _attach_vault_state([dict(row) for row in rows], live_slugs)


def _fetch_scalar(cur: sqlite3.Cursor, sql: str, params: tuple[Any, ...] = ()) -> int:
    row = cur.execute(sql, params).fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def _operator_snapshot() -> dict[str, Any]:
    con = _connect()
    try:
        cur = con.cursor()

        total_leads = _fetch_scalar(cur, "SELECT COUNT(*) FROM leads")
        green_ready = _fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM leads WHERE UPPER(COALESCE(auction_readiness, '')) = 'GREEN'",
        )
        uw_ready = _fetch_scalar(cur, "SELECT COUNT(*) FROM leads WHERE COALESCE(uw_ready, 0) = 1")
        packeted = _fetch_scalar(cur, "SELECT COUNT(DISTINCT lead_key) FROM packets")
        contact_ready = _fetch_scalar(
            cur,
            """
            SELECT COUNT(DISTINCT lead_key)
            FROM lead_field_provenance
            WHERE field_name IN ('trustee_phone_public', 'owner_phone_primary')
              AND field_value_text IS NOT NULL
              AND TRIM(field_value_text) != ''
            """,
        )

        recent_leads = [
            dict(row)
            for row in cur.execute(
                """
                SELECT
                  lead_key,
                  address,
                  county,
                  distress_type,
                  falco_score_internal,
                  auction_readiness,
                  equity_band,
                  dts_days,
                  COALESCE(uw_ready, 0) AS uw_ready,
                  first_seen_at,
                  last_seen_at,
                  score_updated_at,
                  current_sale_date,
                  original_sale_date,
                  sale_status
                FROM leads
                ORDER BY COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
                LIMIT 12
                """
            ).fetchall()
        ]

        top_candidates = [
            dict(row)
            for row in cur.execute(
                """
                SELECT
                  l.lead_key,
                  l.address,
                  l.county,
                  l.distress_type,
                  l.falco_score_internal,
                  l.auction_readiness,
                  l.equity_band,
                  l.dts_days,
                  COALESCE(l.uw_ready, 0) AS uw_ready,
                  MAX(p.created_at) AS latest_packet_at
                FROM leads l
                LEFT JOIN packets p ON p.lead_key = l.lead_key
                WHERE UPPER(COALESCE(l.auction_readiness, '')) = 'GREEN'
                GROUP BY
                  l.lead_key, l.address, l.county, l.distress_type,
                  l.falco_score_internal, l.auction_readiness, l.equity_band,
                  l.dts_days, l.uw_ready
                ORDER BY
                  COALESCE(l.dts_days, 9999) ASC,
                  COALESCE(l.falco_score_internal, 0) DESC,
                  COALESCE(MAX(p.created_at), '') DESC
                LIMIT 10
                """
            ).fetchall()
        ]

        recent_packets = [
            dict(row)
            for row in cur.execute(
                """
                SELECT
                  p.lead_key,
                  p.run_id,
                  p.pdf_path,
                  p.bytes,
                  p.created_at,
                  l.address,
                  l.county,
                  l.falco_score_internal,
                  l.auction_readiness,
                  l.dts_days
                FROM packets p
                LEFT JOIN leads l ON l.lead_key = p.lead_key
                ORDER BY p.created_at DESC
                LIMIT 12
                """
            ).fetchall()
        ]
        live_slugs = _load_live_slugs()
        vault_candidates = _build_vault_candidates(con, live_slugs)
        pre_foreclosure_promotion = _build_pre_foreclosure_promotion(con, live_slugs)
        lifecycle_events = _build_lifecycle_events(con, live_slugs)

        foreclosure_overview = dict(
            cur.execute(
                """
                SELECT
                  SUM(CASE WHEN sale_status='pre_foreclosure' THEN 1 ELSE 0 END) AS pre_foreclosure_count,
                  SUM(CASE WHEN sale_status='scheduled' THEN 1 ELSE 0 END) AS scheduled_count,
                  SUM(CASE WHEN sale_status='rescheduled' THEN 1 ELSE 0 END) AS rescheduled_count,
                  SUM(CASE WHEN sale_status='expired' THEN 1 ELSE 0 END) AS expired_count
                FROM leads
                """
            ).fetchone()
        )

        pre_foreclosure = [
            dict(row)
            for row in cur.execute(
                """
                SELECT
                  lead_key,
                  address,
                  county,
                  distress_type,
                  falco_score_internal,
                  auction_readiness,
                  equity_band,
                  dts_days,
                  COALESCE(uw_ready, 0) AS uw_ready,
                  first_seen_at,
                  last_seen_at,
                  score_updated_at,
                  current_sale_date,
                  original_sale_date,
                  sale_status
                FROM leads
                WHERE sale_status='pre_foreclosure'
                ORDER BY COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
                LIMIT 10
                """
            ).fetchall()
        ]

        status_changes = [
            dict(row)
            for row in cur.execute(
                """
                SELECT
                  lead_key,
                  address,
                  county,
                  distress_type,
                  falco_score_internal,
                  auction_readiness,
                  equity_band,
                  dts_days,
                  COALESCE(uw_ready, 0) AS uw_ready,
                  first_seen_at,
                  last_seen_at,
                  score_updated_at,
                  current_sale_date,
                  original_sale_date,
                  sale_status
                FROM leads
                WHERE sale_status IN ('scheduled', 'rescheduled', 'expired')
                ORDER BY COALESCE(sale_date_updated_at, score_updated_at, last_seen_at, first_seen_at) DESC
                LIMIT 12
                """
            ).fetchall()
        ]
    finally:
        con.close()

    return {
        "generatedAt": _utc_now(),
        "dbPath": str(_db_path()),
        "sourceMode": "snapshot",
        "sourceNote": "Hosted operator snapshot generated from the upstream bots database and current site vault registry.",
        "overview": {
            "totalLeads": total_leads,
            "greenReady": green_ready,
            "uwReady": uw_ready,
            "packeted": packeted,
            "contactReady": contact_ready,
            "vaultLive": len(live_slugs),
            "vaultQueue": len(vault_candidates),
            "pendingApprovals": 0,
        },
        "recentLeads": _attach_vault_state(recent_leads, live_slugs),
        "topCandidates": _attach_vault_state(top_candidates, live_slugs),
        "recentPackets": _attach_vault_state(recent_packets, live_slugs),
        "vaultCandidates": vault_candidates,
        "foreclosureIntake": {
            "preForeclosureCount": int(foreclosure_overview.get("pre_foreclosure_count") or 0),
            "scheduledCount": int(foreclosure_overview.get("scheduled_count") or 0),
            "rescheduledCount": int(foreclosure_overview.get("rescheduled_count") or 0),
            "expiredCount": int(foreclosure_overview.get("expired_count") or 0),
            "preForeclosure": _attach_vault_state(pre_foreclosure, live_slugs),
            "statusChanges": _attach_vault_state(status_changes, live_slugs),
            "recentEvents": lifecycle_events,
        },
        "preForeclosurePromotion": {
            **pre_foreclosure_promotion,
        },
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def _read_site_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_path = SITE_REPO / ".env.local"
    if not env_path.exists():
        return env

    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        env[key.strip()] = value.strip()

    return env


def _site_supabase_config() -> tuple[str | None, str | None]:
    env_file = _read_site_env()
    url = (
        os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
        or os.environ.get("FALCO_SITE_SUPABASE_URL")
        or env_file.get("NEXT_PUBLIC_SUPABASE_URL")
    )
    service_role_key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or os.environ.get("FALCO_SITE_SUPABASE_SERVICE_ROLE_KEY")
        or env_file.get("SUPABASE_SERVICE_ROLE_KEY")
    )
    return url, service_role_key


def _system_state_email(key: str) -> str:
    return f"state+{key}@falco.local"


def _supabase_rest_request(
    method: str,
    url: str,
    service_role_key: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    body = None if payload is None else json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(url, data=body, method=method)
    req.add_header("apikey", service_role_key)
    req.add_header("Authorization", f"Bearer {service_role_key}")
    req.add_header("Accept", "application/json")
    if payload is not None:
        req.add_header("Content-Type", "application/json")
        req.add_header("Prefer", "return=representation")

    with urllib_request.urlopen(req, timeout=20) as response:
        raw = response.read().decode("utf-8").strip()
        return json.loads(raw) if raw else None


def _publish_system_state(key: str, payload: dict[str, Any]) -> None:
    supabase_url, service_role_key = _site_supabase_config()
    if not supabase_url or not service_role_key:
        return

    envelope = {
        "version": 1,
        "key": key,
        "updatedAt": _utc_now(),
        "payload": payload,
    }
    email = _system_state_email(key)
    base_url = supabase_url.rstrip("/") + "/rest/v1/partner_access_requests"

    query = urllib_parse.urlencode(
        {
            "select": "id",
            "company": f"eq.{SYSTEM_STATE_COMPANY}",
            "status": "eq.state_snapshot",
            "email": f"eq.{email}",
            "order": "created_at.desc",
            "limit": "10",
        }
    )

    try:
        existing_rows = _supabase_rest_request(
            "GET",
            f"{base_url}?{query}",
            service_role_key,
        )
        rows = existing_rows if isinstance(existing_rows, list) else []
        latest_id = str(rows[0].get("id") or "").strip() if rows else ""

        state_row = {
            "email": email,
            "full_name": key,
            "company": SYSTEM_STATE_COMPANY,
            "notes": json.dumps(envelope, ensure_ascii=False),
            "status": "state_snapshot",
        }

        if latest_id:
            _supabase_rest_request(
                "PATCH",
                f"{base_url}?id=eq.{urllib_parse.quote(latest_id)}",
                service_role_key,
                state_row,
            )

            duplicate_ids = [str(row.get("id") or "").strip() for row in rows[1:]]
            duplicate_ids = [row_id for row_id in duplicate_ids if row_id]
            if duplicate_ids:
                _supabase_rest_request(
                    "DELETE",
                    f"{base_url}?id=in.({','.join(urllib_parse.quote(row_id) for row_id in duplicate_ids)})",
                    service_role_key,
                )
        else:
            _supabase_rest_request("POST", base_url, service_role_key, state_row)
    except urllib_error.URLError as exc:
        print(f"[site_snapshots] system state publish skipped for {key}: {exc}")
    except Exception as exc:
        print(f"[site_snapshots] system state publish failed for {key}: {exc}")


def _load_latest_analyst_snapshot() -> dict[str, Any] | None:
    path = REPORTS_DIR / "latest_falco_analyst.json"
    if not path.exists():
        return None

    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _refresh_outreach_snapshots() -> dict[str, str | None]:
    SITE_OUTREACH_DIR.mkdir(parents=True, exist_ok=True)
    results: dict[str, str | None] = {}

    for track in ("auction_partner", "principal_broker"):
        latest = sorted(OUTREACH_DIR.glob(f"{track}_*.json"))
        destination = SITE_OUTREACH_DIR / f"{track}.json"
        if not latest:
            results[track] = None
            continue
        payload = json.loads(latest[-1].read_text(encoding="utf-8"))
        _write_json(destination, payload)
        results[track] = str(destination)

    return results


def write_site_snapshots() -> dict[str, Any]:
    SITE_OPERATOR_DIR.mkdir(parents=True, exist_ok=True)
    operator_path = SITE_OPERATOR_DIR / "report.json"
    candidates_path = SITE_OPERATOR_DIR / "vault_candidates.json"
    operator_payload = _operator_snapshot()
    operator_payload["analyst"] = _load_latest_analyst_snapshot()
    _write_json(operator_path, operator_payload)
    with _connect() as con:
        candidate_payload = {
            "generatedAt": _utc_now(),
            "count": 0,
            "candidates": [],
        }
        try:
            live_slugs = _load_live_slugs()
            publish_candidates = _build_publish_candidates(con, live_slugs)
            candidate_payload = {
                "generatedAt": _utc_now(),
                "count": len(publish_candidates),
                "candidates": publish_candidates,
            }
        finally:
            _write_json(candidates_path, candidate_payload)
    operator_payload["overview"]["vaultQueue"] = int(candidate_payload.get("count") or 0)
    outreach_paths = _refresh_outreach_snapshots()
    _write_json(operator_path, operator_payload)
    _publish_system_state("operator_report", operator_payload)
    _publish_system_state("vault_candidates", candidate_payload)

    return {
        "ok": True,
        "operator": str(operator_path),
        "vaultCandidates": str(candidates_path),
        "outreach": outreach_paths,
    }
