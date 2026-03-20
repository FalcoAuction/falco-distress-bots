from __future__ import annotations

import os
import sqlite3
from typing import Any

from ..enrichment.attom_enricher import run as run_attom_enrichment
from ..enrichment.batchdata_fallback import run as run_batchdata_fallback
from ..enrichment.debt_reconstruction import run as run_debt_reconstruction
from ..scoring.scorer import score_leads_by_keys

_EXPANSION_COUNTIES = (
    "Williamson County",
    "Wilson County",
    "Sumner County",
    "Maury County",
    "Cheatham County",
    "Robertson County",
    "Dickson County",
)


def _connect() -> sqlite3.Connection:
    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _target_keys(limit: int) -> list[dict[str, Any]]:
    with _connect() as con:
        placeholders = ",".join("?" for _ in _EXPANSION_COUNTIES)
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
              AND UPPER(COALESCE(equity_band, '')) IN ('', 'UNKNOWN')
            ORDER BY
              CASE county
                WHEN 'Williamson County' THEN 0
                WHEN 'Wilson County' THEN 1
                WHEN 'Sumner County' THEN 2
                WHEN 'Maury County' THEN 3
                WHEN 'Cheatham County' THEN 4
                WHEN 'Robertson County' THEN 5
                WHEN 'Dickson County' THEN 6
                ELSE 7
              END,
              CASE sale_status WHEN 'pre_foreclosure' THEN 0 ELSE 1 END,
              freshness DESC,
              falco_score_internal DESC
            LIMIT ?
            """,
            (*_EXPANSION_COUNTIES, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def run(run_id: str) -> dict[str, Any]:
    limit = max(int(os.environ.get("FALCO_EXPANSION_ATTOM_LIMIT", "12")), 0)
    targets = _target_keys(limit)
    if not targets:
        return {
            "attempted": True,
            "requested": 0,
            "processed": 0,
            "counties": list(_EXPANSION_COUNTIES),
        }

    lead_keys = [str(row["lead_key"]) for row in targets if str(row.get("lead_key") or "").strip()]
    prefc_keys = [str(row["lead_key"]) for row in targets if str(row.get("sale_status") or "").strip().lower() == "pre_foreclosure"]

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
        os.environ["FALCO_STAGE2_SOURCE"] = "sqlite"
        os.environ["FALCO_ATTOM_TARGET_LEAD_KEYS"] = ",".join(lead_keys)
        os.environ["FALCO_ATTOM_MAX_ENRICH"] = str(max(len(lead_keys), 1))
        os.environ["FALCO_MAX_ATTOM_CALLS_PER_RUN"] = str(max(len(lead_keys) * 4, 4))
        attom_result = run_attom_enrichment()

        if prefc_keys:
            os.environ["FALCO_BATCHDATA_TARGET_LEAD_KEYS"] = ",".join(prefc_keys)
            batchdata_result = run_batchdata_fallback()
            os.environ["FALCO_DEBT_RECON_TARGET_LEAD_KEYS"] = ",".join(prefc_keys)
            run_debt_reconstruction()

        score_leads_by_keys(lead_keys, run_id=f"{run_id}_expansion")
    finally:
        for key, value in env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    return {
        "attempted": True,
        "requested": len(lead_keys),
        "processed": len(lead_keys),
        "counties": list(_EXPANSION_COUNTIES),
        "targets": targets,
        "attom": attom_result,
        "batchdata": batchdata_result,
    }
