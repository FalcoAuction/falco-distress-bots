import os
import sqlite3
import json
from datetime import datetime, timezone, date
from typing import Optional

from ..packaging.data_quality import assess_packet_data

DB_PATH_DEFAULT = os.path.join("data", "falco.db")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def db_path() -> str:
    return os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)

def score_dts(dts: int) -> int:
    if 21 <= dts <= 45:
        return 25
    if 46 <= dts <= 60:
        return 18
    if 61 <= dts <= 75:
        return 10
    if 76 <= dts <= 90:
        return 4
    return 0

def score_equity(avm_low: Optional[float], avm_high: Optional[float]) -> (int, str):
    if not avm_low or not avm_high:
        return 0, "UNKNOWN"

    spread = (avm_high - avm_low) / avm_high if avm_high else 0

    if spread < 0.08:
        return 18, "HIGH"
    if spread < 0.15:
        return 12, "MED"
    return 6, "LOW"

def _load_raw_json(raw_json: Optional[str]) -> dict:
    if not raw_json:
        return {}
    try:
        parsed = json.loads(raw_json)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}

def _extract_property_detail(raw_json: Optional[str]) -> dict:
    blob = _load_raw_json(raw_json)
    detail = blob.get("detail") if isinstance(blob.get("detail"), dict) else blob
    if not isinstance(detail, dict):
        return {}

    ident = detail.get("identifier") or {}
    summary = detail.get("summary") or {}
    address = detail.get("address") or {}

    return {
        "property_identifier": ident.get("apn") or ident.get("attomId") or ident.get("fips"),
        "property_type": summary.get("proptype") or summary.get("propClass"),
        "city": address.get("locality") or address.get("city"),
        "zip": address.get("postal1") or address.get("zip"),
    }

def _extract_owner_mortgage(raw_json: Optional[str]) -> dict:
    blob = _load_raw_json(raw_json)
    out = {
        "owner_name": None,
        "owner_mail": None,
        "last_sale_date": None,
        "mortgage_lender": None,
    }

    owner_blob = blob.get("owner")
    if isinstance(owner_blob, dict):
        owner = owner_blob.get("owner") or {}
        if isinstance(owner, dict):
            owner1 = owner.get("owner1") or {}
            if isinstance(owner1, dict):
                parts = [
                    str(owner1.get(k) or "").strip()
                    for k in ("firstnameandmi", "lastname")
                    if str(owner1.get(k) or "").strip()
                ]
                out["owner_name"] = " ".join(parts) or None
            out["owner_mail"] = (
                owner.get("mailingaddressoneline")
                or (owner.get("mailAddress") or {}).get("oneLine")
                or None
            )

        sale = owner_blob.get("sale") or {}
        if isinstance(sale, dict):
            out["last_sale_date"] = sale.get("saleTransDate") or None

    mortgage_blob = blob.get("mortgage")
    if isinstance(mortgage_blob, dict):
        lender = mortgage_blob.get("lender") or {}
        if isinstance(lender, dict):
            out["mortgage_lender"] = lender.get("name") or None

    return out

def _truthy_flag(value: Optional[str]) -> bool:
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _latest_prov_text(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> Optional[str]:
    row = cur.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key=? AND field_name=?
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return str(row[0]).strip() if row and row[0] is not None else None


def _latest_prov_num(cur: sqlite3.Cursor, lead_key: str, field_name: str) -> Optional[float]:
    row = cur.execute(
        """
        SELECT field_value_num
        FROM lead_field_provenance
        WHERE lead_key=? AND field_name=?
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    try:
        return float(row[0]) if row and row[0] is not None else None
    except Exception:
        return None

def score_leads_for_run(run_id: str):
    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT l.lead_key, l.address, l.county,
               ie.sale_date,
               ae.avm_low, ae.avm_high, ae.attom_raw_json,
               cp.field_value_text AS contact_ready
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
        LEFT JOIN (
            SELECT lead_key, field_value_text
            FROM (
                SELECT
                    lead_key,
                    field_value_text,
                    ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY created_at DESC) AS rn
                FROM lead_field_provenance
                WHERE field_name = 'contact_ready'
            )
            WHERE rn = 1
        ) cp ON cp.lead_key = l.lead_key
        WHERE ie.sale_date IS NOT NULL
    """, (run_id,)).fetchall()

    print(f"[SCORING] run_id={run_id} scoring_rows={len(rows)}")

    today = date.today()

    for r in rows:
        sale_date = date.fromisoformat(r["sale_date"])
        dts = (sale_date - today).days

        dts_score = score_dts(dts)
        equity_score, equity_band = score_equity(r["avm_low"], r["avm_high"])
        property_detail = _extract_property_detail(r["attom_raw_json"])
        owner_mortgage = _extract_owner_mortgage(r["attom_raw_json"])
        contact_ready = _truthy_flag(r["contact_ready"])

        for key in ("owner_name", "owner_mail", "last_sale_date", "mortgage_lender"):
            prov_value = _latest_prov_text(conn, r["lead_key"], key)
            if prov_value:
                owner_mortgage[key] = prov_value

        for key in ("property_identifier",):
            prov_value = _latest_prov_text(conn, r["lead_key"], key)
            if prov_value:
                property_detail[key] = prov_value

        for key in ("year_built", "building_area_sqft", "beds", "baths"):
            prov_value = _latest_prov_num(conn, r["lead_key"], key)
            if prov_value is not None:
                property_detail[key] = prov_value

        quality = assess_packet_data({
            "address": r["address"],
            "county": r["county"],
            "dts_days": dts,
            "attom_raw_json": r["attom_raw_json"],
            "value_anchor_low": r["avm_low"],
            "value_anchor_high": r["avm_high"],
            "auction_readiness": None,
            "equity_band": equity_band,
            "contact_ready": contact_ready,
            "property_identifier": property_detail.get("property_identifier"),
            "property_type": property_detail.get("property_type"),
            "city": property_detail.get("city"),
            "zip": property_detail.get("zip"),
            "owner_name": owner_mortgage.get("owner_name"),
            "owner_mail": owner_mortgage.get("owner_mail"),
            "last_sale_date": owner_mortgage.get("last_sale_date"),
            "mortgage_lender": owner_mortgage.get("mortgage_lender"),
            "year_built": property_detail.get("year_built"),
            "building_area_sqft": property_detail.get("building_area_sqft"),
            "beds": property_detail.get("beds"),
            "baths": property_detail.get("baths"),
            "notice_phone": _latest_prov_text(conn, r["lead_key"], "notice_phone"),
            "trustee_phone_public": _latest_prov_text(conn, r["lead_key"], "trustee_phone_public"),
            "owner_phone_primary": _latest_prov_text(conn, r["lead_key"], "owner_phone_primary"),
            "owner_phone_secondary": _latest_prov_text(conn, r["lead_key"], "owner_phone_secondary"),
        })
        execution_reality = quality["execution_reality"]

        completeness_score = 8 if r["address"] and r["county"] else 0
        property_score = 12 if (
            property_detail.get("property_type")
            and property_detail.get("property_identifier")
            and property_detail.get("city")
            and property_detail.get("zip")
        ) else 6 if (
            property_detail.get("property_type")
            or property_detail.get("property_identifier")
        ) else 0
        ownership_score = 14 if (
            owner_mortgage.get("owner_name") and owner_mortgage.get("owner_mail")
        ) else 7 if (
            owner_mortgage.get("owner_name") or owner_mortgage.get("owner_mail")
        ) else 0
        debt_history_score = 18 if (
            owner_mortgage.get("mortgage_lender") and owner_mortgage.get("last_sale_date")
        ) else 9 if (
            owner_mortgage.get("mortgage_lender") or owner_mortgage.get("last_sale_date")
        ) else 0
        property_depth_score = 10 if (
            property_detail.get("year_built") is not None
            and property_detail.get("building_area_sqft") is not None
            and property_detail.get("baths") is not None
        ) else 5 if (
            property_detail.get("year_built") is not None
            or property_detail.get("building_area_sqft") is not None
            or property_detail.get("beds") is not None
            or property_detail.get("baths") is not None
        ) else 0
        contact_score = (
            12 if execution_reality["contact_path_quality"] == "STRONG"
            else 6 if execution_reality["contact_path_quality"] == "PARTIAL"
            else 0
        )
        control_score = 6 if execution_reality["control_party"] != "UNCLEAR" else 0
        execution_score = (
            12 if execution_reality["workability_band"] == "STRONG"
            else 6 if execution_reality["workability_band"] == "MODERATE"
            else 0
        )

        raw_score = (
            dts_score
            + equity_score
            + completeness_score
            + property_score
            + property_depth_score
            + ownership_score
            + debt_history_score
            + contact_score
            + control_score
            + execution_score
        )
        total_score = max(0, min(100, raw_score))

        has_valuation = bool(r["avm_low"] and r["avm_high"])
        has_owner_pair = bool(owner_mortgage.get("owner_name") and owner_mortgage.get("owner_mail"))
        has_debt_pair = bool(owner_mortgage.get("mortgage_lender") and owner_mortgage.get("last_sale_date"))
        inside_green_window = 21 <= dts <= 60

        if (
            total_score >= 80
            and has_valuation
            and has_owner_pair
            and has_debt_pair
            and execution_reality["contact_path_quality"] != "THIN"
            and inside_green_window
            and execution_reality["workability_band"] == "STRONG"
            and execution_reality["execution_posture"] != "NEEDS MORE CONTROL CLARITY"
        ):
            readiness = "GREEN"
        elif (
            total_score >= 55
            and has_valuation
            and dts <= 75
            and execution_reality["workability_band"] != "LIMITED"
        ):
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
                (r["lead_key"], "contact_path_quality", "derived", execution_reality["contact_path_quality"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "control_party",        "derived", execution_reality["control_party"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "execution_posture",    "derived", execution_reality["execution_posture"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "workability_band",     "derived", execution_reality["workability_band"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
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
