import json
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..packaging.data_quality import assess_packet_data
from ..settings import get_dts_window


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
    return {row["lead_key"]: row for row in rows}


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
        lead_key = row["lead_key"]
        if lead_key in out:
            continue
        out[lead_key] = str(row["field_value_text"] or "").strip().lower() in {"1", "true", "yes", "y"}
    return out


def _latest_field_map(con: sqlite3.Connection, field_name: str) -> Dict[str, str]:
    rows = con.execute(
        """
        SELECT lead_key, field_value_text
        FROM lead_field_provenance
        WHERE field_name = ?
        ORDER BY created_at DESC, prov_id DESC
        """,
        (field_name,),
    ).fetchall()
    out: Dict[str, str] = {}
    for row in rows:
        lead_key = row["lead_key"]
        if lead_key in out:
            continue
        value = row["field_value_text"]
        if value is not None:
            out[lead_key] = value
    return out


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _build_packet_quality_snapshot(con: sqlite3.Connection, limit: int = 25) -> Dict[str, Any]:
    dts_min, dts_max = get_dts_window("RUN_SUMMARY")
    attom_map = _latest_attom_map(con)
    contact_map = _contact_ready_map(con)
    trustee_phone_map = _latest_field_map(con, "trustee_phone_public")
    owner_phone_primary_map = _latest_field_map(con, "owner_phone_primary")
    owner_phone_secondary_map = _latest_field_map(con, "owner_phone_secondary")
    notice_phone_map = _latest_field_map(con, "notice_phone")

    leads = con.execute(
        """
        SELECT
          lead_key,
          address,
          county,
          state,
          distress_type,
          falco_score_internal,
          auction_readiness,
          equity_band,
          dts_days
        FROM leads
        WHERE dts_days IS NOT NULL
          AND dts_days BETWEEN ? AND ?
        ORDER BY dts_days ASC, lead_key ASC
        LIMIT ?
        """,
        (dts_min, dts_max, limit),
    ).fetchall()

    blocker_counts: Counter[str] = Counter()
    batchdata_targets: Counter[str] = Counter()
    readiness_counts: Counter[str] = Counter()
    reviewed: List[Dict[str, Any]] = []

    for lead in leads:
        lead_key = lead["lead_key"]
        attom = attom_map.get(lead_key)
        fields = dict(lead)
        fields["contact_ready"] = contact_map.get(lead_key, False)
        fields["attom_raw_json"] = attom["attom_raw_json"] if attom else None
        fields["value_anchor_mid"] = attom["avm_value"] if attom else None
        fields["value_anchor_low"] = attom["avm_low"] if attom else None
        fields["value_anchor_high"] = attom["avm_high"] if attom else None
        fields["trustee_phone_public"] = trustee_phone_map.get(lead_key)
        fields["owner_phone_primary"] = owner_phone_primary_map.get(lead_key)
        fields["owner_phone_secondary"] = owner_phone_secondary_map.get(lead_key)
        fields["notice_phone"] = notice_phone_map.get(lead_key)

        quality = assess_packet_data(fields)
        readiness = str(fields.get("auction_readiness") or "UNKNOWN").upper()
        readiness_counts[readiness] += 1
        blocker_counts.update(quality["vault_publish_blockers"])
        batchdata_targets.update(quality["batchdata_fallback_targets"])
        reviewed.append(
            {
                "lead_key": lead_key,
                "county": fields.get("county"),
                "distress_type": fields.get("distress_type"),
                "auction_readiness": readiness,
                "dts_days": fields.get("dts_days"),
                "packet_completeness_pct": quality["packet_completeness_pct"],
                "vault_publish_ready": quality["vault_publish_ready"],
                "top_tier_ready": quality["top_tier_ready"],
                "execution_blockers": quality["execution_blockers"],
                "batchdata_fallback_targets": quality["batchdata_fallback_targets"],
            }
        )

    return {
        "generated_at": _utc_now(),
        "dts_window": {"min": dts_min, "max": dts_max},
        "lead_count": len(reviewed),
        "vault_ready_count": sum(1 for row in reviewed if row["vault_publish_ready"]),
        "top_tier_ready_count": sum(1 for row in reviewed if row["top_tier_ready"]),
        "readiness_counts": dict(readiness_counts),
        "top_blockers": blocker_counts.most_common(10),
        "top_batchdata_targets": batchdata_targets.most_common(10),
        "leads": reviewed,
    }


def _build_ingest_snapshot(con: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    by_source = con.execute(
        """
        SELECT COALESCE(source, 'UNKNOWN') AS source, COUNT(*) AS event_count
        FROM ingest_events
        WHERE run_id = ?
        GROUP BY COALESCE(source, 'UNKNOWN')
        ORDER BY event_count DESC, source ASC
        """,
        (run_id,),
    ).fetchall()
    lead_count_row = con.execute(
        """
        SELECT COUNT(DISTINCT lead_key) AS lead_count
        FROM ingest_events
        WHERE run_id = ?
        """,
        (run_id,),
    ).fetchone()
    return {
        "event_count": sum(int(row["event_count"]) for row in by_source),
        "lead_count": int(lead_count_row["lead_count"] if lead_count_row else 0),
        "sources": [{"source": row["source"], "event_count": int(row["event_count"])} for row in by_source],
    }


def _build_packet_snapshot(con: sqlite3.Connection, run_id: str) -> Dict[str, Any]:
    rows = con.execute(
        """
        SELECT lead_key, pdf_path, created_at
        FROM packets
        WHERE run_id = ?
        ORDER BY created_at DESC, lead_key ASC
        """,
        (run_id,),
    ).fetchall()
    return {
        "packet_count": len(rows),
        "packets": [
            {
                "lead_key": row["lead_key"],
                "pdf_path": row["pdf_path"],
                "created_at": row["created_at"],
            }
            for row in rows
        ],
    }


def write_run_summary(
    run_id: str,
    utc_start: str,
    utc_end: str,
    stage_results: Iterable[Dict[str, Any]],
    publish_result: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    con = _connect()
    try:
        report = {
            "run_id": run_id,
            "utc_start": utc_start,
            "utc_end": utc_end,
            "db_path": _db_path(),
            "stage_results": list(stage_results),
            "ingest": _build_ingest_snapshot(con, run_id),
            "packets": _build_packet_snapshot(con, run_id),
            "quality": _build_packet_quality_snapshot(con),
            "publish": publish_result or {"attempted": False},
        }
    finally:
        con.close()

    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_summary.json"
    latest_path = reports_dir / "latest_run_summary.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False, default=_json_ready) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    print(f"[RunSummary] wrote {run_path}")
    return {
        "ok": True,
        "path": str(run_path),
        "ingest_events": report["ingest"]["event_count"],
        "packets_created": report["packets"]["packet_count"],
        "vault_ready_count": report["quality"]["vault_ready_count"],
        "top_tier_ready_count": report["quality"]["top_tier_ready_count"],
    }
