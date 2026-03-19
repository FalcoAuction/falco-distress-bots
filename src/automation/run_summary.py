import json
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

from ..automation.prefc_policy import prefc_county_is_active, prefc_county_priority
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
        lead_key = row["lead_key"]
        if lead_key in out:
            continue
        out[lead_key] = float(row["field_value_num"])
    return out


def _json_ready(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _load_live_vault_lead_keys() -> set[str]:
    site_file = Path(__file__).resolve().parents[3] / "falco-site" / "data" / "vault_listings.ndjson"
    if not site_file.exists():
        return set()
    live: set[str] = set()
    for line in site_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except Exception:
            continue
        if not isinstance(row, dict) or row.get("status") != "active":
            continue
        lead_key = str(row.get("sourceLeadKey") or "").strip()
        if lead_key:
            live.add(lead_key)
    return live


def _build_packet_quality_snapshot(con: sqlite3.Connection, limit: int = 25) -> Dict[str, Any]:
    dts_min, dts_max = get_dts_window("RUN_SUMMARY")
    attom_map = _latest_attom_map(con)
    contact_map = _contact_ready_map(con)
    trustee_phone_map = _latest_field_map(con, "trustee_phone_public")
    owner_phone_primary_map = _latest_field_map(con, "owner_phone_primary")
    owner_phone_secondary_map = _latest_field_map(con, "owner_phone_secondary")
    notice_phone_map = _latest_field_map(con, "notice_phone")
    owner_name_map = _latest_field_map(con, "owner_name")
    owner_mail_map = _latest_field_map(con, "owner_mail")
    last_sale_map = _latest_field_map(con, "last_sale_date")
    mortgage_map = _latest_field_map(con, "mortgage_lender")
    property_identifier_map = _latest_field_map(con, "property_identifier")
    year_built_map = _latest_num_field_map(con, "year_built")
    building_area_map = _latest_num_field_map(con, "building_area_sqft")
    beds_map = _latest_num_field_map(con, "beds")
    baths_map = _latest_num_field_map(con, "baths")

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
    packetability_counts: Counter[str] = Counter()
    recoverable_counts: Counter[str] = Counter()
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
        fields["owner_name"] = owner_name_map.get(lead_key)
        fields["owner_mail"] = owner_mail_map.get(lead_key)
        fields["last_sale_date"] = last_sale_map.get(lead_key)
        fields["mortgage_lender"] = mortgage_map.get(lead_key)
        fields["property_identifier"] = property_identifier_map.get(lead_key)
        fields["year_built"] = year_built_map.get(lead_key)
        fields["building_area_sqft"] = building_area_map.get(lead_key)
        fields["beds"] = beds_map.get(lead_key)
        fields["baths"] = baths_map.get(lead_key)

        quality = assess_packet_data(fields)
        readiness = str(fields.get("auction_readiness") or "UNKNOWN").upper()
        readiness_counts[readiness] += 1
        packetability_counts[str(quality.get("packetability_band") or "UNKNOWN").upper()] += 1
        if bool(quality.get("recoverable_partial")):
            recoverable_counts[str(quality.get("recoverable_partial_next_step") or "review").lower()] += 1
        blocker_counts.update(quality["vault_publish_blockers"])
        batchdata_targets.update(quality["batchdata_fallback_targets"])
        reviewed.append(
            {
                "lead_key": lead_key,
                "address": fields.get("address"),
                "county": fields.get("county"),
                "distress_type": fields.get("distress_type"),
                "falco_score_internal": fields.get("falco_score_internal"),
                "auction_readiness": readiness,
                "dts_days": fields.get("dts_days"),
                "packet_completeness_pct": quality["packet_completeness_pct"],
                "packetability_band": quality.get("packetability_band"),
                "packetability_score": quality.get("packetability_score"),
                "recoverable_partial": bool(quality.get("recoverable_partial")),
                "recoverable_partial_next_step": quality.get("recoverable_partial_next_step"),
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
        "packetability_counts": dict(packetability_counts),
        "recoverable_partial_counts": dict(recoverable_counts),
        "top_blockers": blocker_counts.most_common(10),
        "top_batchdata_targets": batchdata_targets.most_common(10),
        "leads": reviewed,
    }


def _build_county_hit_rate_snapshot(con: sqlite3.Connection) -> Dict[str, Any]:
    live_lead_keys = _load_live_vault_lead_keys()
    packeted = {
        str(row[0] or "")
        for row in con.execute("SELECT DISTINCT lead_key FROM packets").fetchall()
        if str(row[0] or "").strip()
    }
    rows = con.execute(
        """
        SELECT
          l.lead_key,
          l.county,
          l.distress_type,
          l.sale_status,
          l.auction_readiness,
          l.equity_band,
          l.falco_score_internal,
          l.current_sale_date,
          l.original_sale_date
        FROM leads l
        WHERE l.county IS NOT NULL
          AND TRIM(l.county) != ''
        """
    ).fetchall()

    county_rollup: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        county = str(row["county"] or "").strip()
        if not county:
            continue
        bucket = county_rollup.setdefault(
            county,
            {
                "county": county,
                "activeLane": prefc_county_is_active(county),
                "tracked": 0,
                "preForeclosureTracked": 0,
                "packeted": 0,
                "live": 0,
                "strongLivePrefc": 0,
                "sourceMix": Counter(),
            },
        )
        lead_key = str(row["lead_key"] or "").strip()
        bucket["tracked"] += 1
        bucket["sourceMix"][str(row["distress_type"] or "UNKNOWN")] += 1
        if str(row["sale_status"] or "").strip().lower() == "pre_foreclosure":
            bucket["preForeclosureTracked"] += 1
        if lead_key in packeted:
            bucket["packeted"] += 1
        if lead_key in live_lead_keys:
            bucket["live"] += 1
            if (
                str(row["sale_status"] or "").strip().lower() == "pre_foreclosure"
                and str(row["equity_band"] or "").upper() in {"MED", "HIGH"}
            ):
                bucket["strongLivePrefc"] += 1

    ranked = []
    for bucket in county_rollup.values():
        tracked = bucket["tracked"] or 1
        packeted_count = bucket["packeted"]
        live_count = bucket["live"]
        bucket["packetRate"] = round((packeted_count / tracked) * 100, 1)
        bucket["liveRate"] = round((live_count / tracked) * 100, 1)
        bucket["sourceMix"] = dict(bucket["sourceMix"])
        ranked.append(bucket)

    ranked.sort(
        key=lambda row: (
            prefc_county_priority(row["county"]),
            -row["strongLivePrefc"],
            -row["live"],
            -row["packeted"],
            row["county"],
        )
    )
    return {
        "generated_at": _utc_now(),
        "counties": ranked[:12],
        "focusCounties": [row for row in ranked if row["activeLane"]][:6],
    }


def _build_special_situations_snapshot(con: sqlite3.Connection) -> Dict[str, Any]:
    rows = con.execute(
        """
        WITH source_rollup AS (
          SELECT
            lead_key,
            GROUP_CONCAT(DISTINCT UPPER(COALESCE(source, 'UNKNOWN'))) AS source_mix
          FROM ingest_events
          GROUP BY lead_key
        )
        SELECT
          l.lead_key,
          l.address,
          l.county,
          l.distress_type,
          l.sale_status,
          l.equity_band,
          l.falco_score_internal,
          l.current_sale_date,
          l.original_sale_date,
          COALESCE(sr.source_mix, '') AS source_mix
        FROM leads l
        LEFT JOIN source_rollup sr ON sr.lead_key = l.lead_key
        WHERE l.sale_status = 'pre_foreclosure'
           OR COALESCE(sr.source_mix, '') LIKE '%API_TAX%'
           OR COALESCE(sr.source_mix, '') LIKE '%OFFICIAL_TAX_SALE%'
           OR COALESCE(sr.source_mix, '') LIKE '%TAXPAGES%'
        ORDER BY COALESCE(l.falco_score_internal, 0) DESC, COALESCE(l.last_seen_at, l.first_seen_at) DESC
        LIMIT 80
        """
    ).fetchall()

    candidates: list[Dict[str, Any]] = []
    for row in rows:
        source_mix = [part for part in str(row["source_mix"] or "").split(",") if part]
        overlap_signals: list[str] = []
        if "SUBSTITUTION_OF_TRUSTEE" in source_mix and "LIS_PENDENS" in source_mix:
            overlap_signals.append("stacked_notice_path")
        if any(source in source_mix for source in ("API_TAX", "OFFICIAL_TAX_SALE", "TAXPAGES")):
            overlap_signals.append("tax_overlap")
        if row["current_sale_date"] and row["original_sale_date"] and row["current_sale_date"] != row["original_sale_date"]:
            overlap_signals.append("reopened_timing")
        if not overlap_signals:
            continue
        candidates.append(
            {
                "lead_key": row["lead_key"],
                "address": row["address"],
                "county": row["county"],
                "sale_status": row["sale_status"],
                "equity_band": row["equity_band"],
                "falco_score_internal": row["falco_score_internal"],
                "overlap_signals": overlap_signals,
                "source_mix": source_mix,
            }
        )

    candidates.sort(
        key=lambda row: (
            prefc_county_priority(row.get("county")),
            0 if "tax_overlap" in row["overlap_signals"] else 1,
            -float(row.get("falco_score_internal") or 0),
        )
    )
    return {
        "generated_at": _utc_now(),
        "candidate_count": len(candidates),
        "candidates": candidates[:12],
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
            "county_hit_rates": _build_county_hit_rate_snapshot(con),
            "special_situations": _build_special_situations_snapshot(con),
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
        "report": report,
    }
