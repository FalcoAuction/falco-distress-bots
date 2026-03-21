from __future__ import annotations

import os
import sqlite3
from typing import Any

from ..automation.autonomy_agents import determine_lead_action
from ..enrichment.attom_enricher import run as run_attom_enrichment
from ..enrichment.batchdata_fallback import run as run_batchdata_fallback
from ..enrichment.debt_reconstruction import run as run_debt_reconstruction
from ..packaging.data_quality import assess_packet_data
from ..packaging.packager import run as run_packager
from ..scoring.scorer import score_leads_by_keys

_EXPANSION_COUNTIES = (
    "Williamson County",
    "Wilson County",
    "Sumner County",
    "Maury County",
    "Cheatham County",
    "Robertson County",
    "Dickson County",
    "Blount County",
    "Sevier County",
    "Washington County",
    "Cumberland County",
    "Putnam County",
    "Sullivan County",
)
_CORE_HIGH_SIGNAL_COUNTIES = (
    "Rutherford County",
    "Davidson County",
    "Montgomery County",
)
_BACKFILL_COUNTIES = _CORE_HIGH_SIGNAL_COUNTIES + _EXPANSION_COUNTIES


def _connect() -> sqlite3.Connection:
    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _target_keys(limit: int) -> list[dict[str, Any]]:
    with _connect() as con:
        placeholders = ",".join("?" for _ in _BACKFILL_COUNTIES)
        rows = con.execute(
            f"""
            SELECT
              lead_key,
              county,
              address,
              distress_type,
              sale_status,
              COALESCE(equity_band, '') AS equity_band,
              COALESCE(falco_score_internal, 0) AS falco_score_internal,
              COALESCE(last_seen_at, first_seen_at, '') AS freshness
            FROM leads
            WHERE county IN ({placeholders})
              AND sale_status IN ('pre_foreclosure', 'scheduled')
              AND address IS NOT NULL
              AND TRIM(address) <> ''
              AND (
                    UPPER(COALESCE(equity_band, '')) IN ('', 'UNKNOWN')
                 OR sale_status = 'pre_foreclosure'
                 OR (
                      sale_status = 'scheduled'
                      AND UPPER(COALESCE(equity_band, '')) IN ('MED', 'HIGH')
                    )
              )
            ORDER BY
              CASE county
                WHEN 'Rutherford County' THEN 0
                WHEN 'Davidson County' THEN 1
                WHEN 'Montgomery County' THEN 2
                WHEN 'Williamson County' THEN 3
                WHEN 'Wilson County' THEN 4
                WHEN 'Sumner County' THEN 5
                WHEN 'Maury County' THEN 6
                WHEN 'Cheatham County' THEN 7
                WHEN 'Robertson County' THEN 8
                WHEN 'Dickson County' THEN 9
                WHEN 'Cumberland County' THEN 10
                WHEN 'Putnam County' THEN 11
                WHEN 'Sullivan County' THEN 12
                ELSE 13
              END,
              CASE
                WHEN sale_status = 'scheduled'
                     AND UPPER(COALESCE(equity_band, '')) IN ('MED', 'HIGH') THEN 0
                WHEN sale_status = 'pre_foreclosure' THEN 1
                ELSE 2
              END,
              freshness DESC,
              falco_score_internal DESC
            LIMIT ?
            """,
            (*_BACKFILL_COUNTIES, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def _latest_text(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> str | None:
    row = cur.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ?
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def _latest_num(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> float | None:
    row = cur.execute(
        """
        SELECT field_value_num
        FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ?
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _latest_attom(cur: sqlite3.Cursor, lead_key: str) -> dict[str, Any]:
    row = cur.execute(
        """
        SELECT attom_raw_json, avm_value, avm_low, avm_high
        FROM attom_enrichments
        WHERE lead_key = ?
        ORDER BY enriched_at DESC, id DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    return {
        "attom_raw_json": row[0] if row and row[0] is not None else None,
        "value_anchor_mid": float(row[1]) if row and row[1] is not None else None,
        "value_anchor_low": float(row[2]) if row and row[2] is not None else None,
        "value_anchor_high": float(row[3]) if row and row[3] is not None else None,
    }


def _publishable_pack_targets(lead_keys: list[str]) -> list[str]:
    if not lead_keys:
        return []
    with _connect() as con:
        placeholders = ",".join("?" for _ in lead_keys)
        rows = con.execute(
            f"""
            SELECT lead_key, address, county, distress_type, falco_score_internal,
                   auction_readiness, equity_band, dts_days, sale_status,
                   current_sale_date, original_sale_date, first_seen_at, last_seen_at
            FROM leads
            WHERE lead_key IN ({placeholders})
            """,
            lead_keys,
        ).fetchall()
        out: list[str] = []
        for row in rows:
            lead = dict(row)
            lead_key = str(lead.get("lead_key") or "").strip()
            if not lead_key:
                continue
            attom = _latest_attom(con.cursor(), lead_key)
            lead_fields = {
                **lead,
                "contact_ready": _latest_text(con.cursor(), lead_key, "contact_ready") == "1",
                "attom_raw_json": attom["attom_raw_json"],
                "value_anchor_mid": attom["value_anchor_mid"],
                "value_anchor_low": attom["value_anchor_low"],
                "value_anchor_high": attom["value_anchor_high"],
                "property_identifier": _latest_text(con.cursor(), lead_key, "property_identifier"),
                "owner_name": _latest_text(con.cursor(), lead_key, "owner_name"),
                "owner_mail": _latest_text(con.cursor(), lead_key, "owner_mail"),
                "last_sale_date": _latest_text(con.cursor(), lead_key, "last_sale_date"),
                "mortgage_date": _latest_text(con.cursor(), lead_key, "mortgage_date"),
                "mortgage_lender": _latest_text(con.cursor(), lead_key, "mortgage_lender"),
                "mortgage_amount": _latest_num(con.cursor(), lead_key, "mortgage_amount"),
                "trustee_phone_public": _latest_text(con.cursor(), lead_key, "trustee_phone_public"),
                "owner_phone_primary": _latest_text(con.cursor(), lead_key, "owner_phone_primary"),
                "owner_phone_secondary": _latest_text(con.cursor(), lead_key, "owner_phone_secondary"),
                "notice_phone": _latest_text(con.cursor(), lead_key, "notice_phone"),
            }
            quality = assess_packet_data(lead_fields)
            decision = determine_lead_action(lead_fields, quality, [], [])
            sale_status = str(lead.get("sale_status") or "").strip().lower()
            if sale_status == "pre_foreclosure":
                if bool(quality.get("prefc_live_quality")) and decision.get("next_action") == "publish":
                    out.append(lead_key)
            elif bool(quality.get("vault_publish_ready")) and decision.get("next_action") == "publish":
                out.append(lead_key)
        return out


def run(run_id: str) -> dict[str, Any]:
    limit = max(int(os.environ.get("FALCO_EXPANSION_ATTOM_LIMIT", "32")), 0)
    targets = _target_keys(limit)
    if not targets:
        return {
            "attempted": True,
            "requested": 0,
            "processed": 0,
            "counties": list(_BACKFILL_COUNTIES),
        }

    lead_keys = [str(row["lead_key"]) for row in targets if str(row.get("lead_key") or "").strip()]
    debt_keys = [
        str(row["lead_key"])
        for row in targets
        if str(row.get("sale_status") or "").strip().lower() in {"pre_foreclosure", "scheduled"}
    ]

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
    packaged_keys: list[str] = []
    try:
        os.environ["FALCO_STAGE2_SOURCE"] = "sqlite"
        os.environ["FALCO_ATTOM_TARGET_LEAD_KEYS"] = ",".join(lead_keys)
        os.environ["FALCO_ATTOM_MAX_ENRICH"] = str(max(len(lead_keys), 1))
        os.environ["FALCO_MAX_ATTOM_CALLS_PER_RUN"] = str(max(len(lead_keys) * 4, 4))
        attom_result = run_attom_enrichment()

        if debt_keys and os.environ.get("FALCO_BATCHDATA_API_KEY", "").strip():
            os.environ["FALCO_ENABLE_BATCHDATA_FALLBACK"] = "1"
            os.environ["FALCO_BATCHDATA_TARGET_LEAD_KEYS"] = ",".join(debt_keys)
            batchdata_result = run_batchdata_fallback()
        if debt_keys:
            os.environ["FALCO_DEBT_RECON_TARGET_LEAD_KEYS"] = ",".join(debt_keys)
            run_debt_reconstruction()

        score_leads_by_keys(lead_keys, run_id=f"{run_id}_expansion")
        packaged_keys = _publishable_pack_targets(lead_keys)
        for lead_key in packaged_keys:
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
        "requested": len(lead_keys),
        "processed": len(lead_keys),
        "counties": list(_BACKFILL_COUNTIES),
        "targets": targets,
        "attom": attom_result,
        "batchdata": batchdata_result,
        "packaged": packaged_keys,
    }
