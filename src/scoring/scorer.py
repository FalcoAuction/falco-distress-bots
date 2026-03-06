import os
import sqlite3
from datetime import datetime, timezone, date
from typing import Optional

DB_PATH_DEFAULT = os.path.join("data", "falco.db")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def db_path() -> str:
    return os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)

def score_dts(dts: int) -> int:
    if 21 <= dts <= 45:
        return 40
    if 46 <= dts <= 60:
        return 30
    if 61 <= dts <= 75:
        return 20
    if 76 <= dts <= 90:
        return 10
    return 0

def score_equity(avm_low: Optional[float], avm_high: Optional[float]) -> (int, str):
    if not avm_low or not avm_high:
        return 10, "UNKNOWN"

    spread = (avm_high - avm_low) / avm_high if avm_high else 0

    if spread < 0.08:
        return 40, "HIGH"
    if spread < 0.15:
        return 30, "MED"
    return 20, "LOW"

def score_leads_for_run(run_id: str):
    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT l.lead_key, l.address, l.county,
               ie.sale_date,
               ae.avm_low, ae.avm_high
        FROM leads l
        JOIN (
            SELECT lead_key, MAX(id) AS max_ie_id
            FROM ingest_events
            WHERE run_id = ?
            GROUP BY lead_key
        ) latest_ie ON latest_ie.lead_key = l.lead_key
        JOIN ingest_events ie ON ie.id = latest_ie.max_ie_id
        LEFT JOIN (
            SELECT lead_key, MAX(id) AS max_ae_id
            FROM attom_enrichments
            GROUP BY lead_key
        ) latest_ae ON latest_ae.lead_key = l.lead_key
        LEFT JOIN attom_enrichments ae ON ae.id = latest_ae.max_ae_id
        WHERE ie.sale_date IS NOT NULL
    """, (run_id,)).fetchall()

    print(f"[SCORING] run_id={run_id} scoring_rows={len(rows)}")

    today = date.today()

    for r in rows:
        sale_date = date.fromisoformat(r["sale_date"])
        dts = (sale_date - today).days

        dts_score = score_dts(dts)
        equity_score, equity_band = score_equity(r["avm_low"], r["avm_high"])

        completeness_score = 20 if r["address"] and r["county"] else 0

        total_score = dts_score + equity_score + completeness_score

        if total_score >= 75:
            readiness = "GREEN"
        elif total_score >= 50:
            readiness = "YELLOW"
        else:
            readiness = "RED"

        _scored_at = utc_now_iso()
        conn.execute("""
            UPDATE leads
            SET falco_score_internal=?,
                auction_readiness=?,
                equity_band=?,
                dts_days=?,
                score_updated_at=?
            WHERE lead_key=?
        """, (
            total_score,
            readiness,
            equity_band,
            dts,
            _scored_at,
            r["lead_key"],
        ))

        # Provenance: record scoring outputs for this lead
        try:
            _prov_rows = [
                # (lead_key, field_name, value_type, field_value_text, field_value_num,
                #  field_value_json, units, confidence, source_channel,
                #  retrieved_at, run_id, created_at)
                (r["lead_key"], "dts_days",             "derived", None,        float(dts),         None, "days", None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "falco_score_internal", "derived", None,        float(total_score), None, None,   None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "equity_band",          "derived", equity_band, None,               None, None,   None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "auction_readiness",    "derived", readiness,   None,               None, None,   None, "SCORING", _scored_at, run_id, _scored_at),
            ]
            conn.executemany("""
                INSERT INTO lead_field_provenance
                    (lead_key, field_name, value_type,
                     field_value_text, field_value_num, field_value_json,
                     units, confidence,
                     source_channel, retrieved_at, run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, _prov_rows)
        except Exception:
            pass  # provenance failure never aborts scoring

    conn.commit()
    conn.close()
