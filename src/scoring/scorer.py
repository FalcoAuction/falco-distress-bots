import os
import sqlite3
import json
from datetime import datetime, timezone, date
from typing import Optional

from ..automation.prefc_policy import prefc_county_is_active, prefc_source_priority
from ..packaging.data_quality import assess_packet_data

DB_PATH_DEFAULT = os.path.join("data", "falco.db")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def db_path() -> str:
    return os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)


def _source_set(conn: sqlite3.Connection, lead_key: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT DISTINCT UPPER(COALESCE(source, ''))
        FROM ingest_events
        WHERE lead_key=?
        """,
        (lead_key,),
    ).fetchall()
    return {str(row[0] or "").strip().upper() for row in rows if str(row[0] or "").strip()}

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
    if spread < 0.25:
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
        "mortgage_amount": None,
        "mortgage_date": None,
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
        mortgage = mortgage_blob.get("mortgage") or {}
        if isinstance(mortgage, dict):
            out["mortgage_amount"] = (
                mortgage.get("amount")
                or mortgage.get("loanAmount")
                or mortgage.get("originationAmount")
                or None
            )
            out["mortgage_date"] = (
                mortgage.get("recordingDate")
                or mortgage.get("documentDate")
                or mortgage.get("loanDate")
                or None
            )
        if not out["mortgage_amount"]:
            out["mortgage_amount"] = (
                mortgage_blob.get("amount")
                or mortgage_blob.get("loanAmount")
                or mortgage_blob.get("originationAmount")
                or None
            )
        if not out["mortgage_date"]:
            out["mortgage_date"] = (
                mortgage_blob.get("recordingDate")
                or mortgage_blob.get("documentDate")
                or mortgage_blob.get("loanDate")
                or None
            )

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

def _load_rows_for_run(conn: sqlite3.Connection, run_id: str):
    return conn.execute("""
        SELECT l.lead_key, l.address, l.county, l.distress_type, l.sale_status,
               COALESCE(l.current_sale_date, ie.sale_date) AS sale_date,
               l.current_sale_date,
               l.original_sale_date,
               ae.avm_low, ae.avm_high, ae.attom_raw_json,
               cp.field_value_text AS contact_ready
        FROM leads l
        JOIN (
            SELECT DISTINCT lead_key
            FROM ingest_events
            WHERE run_id = ?
        ) touched ON touched.lead_key = l.lead_key
        LEFT JOIN (
            SELECT lead_key, MAX(id) AS max_ie_id
            FROM ingest_events
            GROUP BY lead_key
        ) latest_ie ON latest_ie.lead_key = l.lead_key
        LEFT JOIN ingest_events ie ON ie.id = latest_ie.max_ie_id
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
    """, (run_id,)).fetchall()


def _load_rows_for_lead_keys(conn: sqlite3.Connection, lead_keys: list[str]):
    if not lead_keys:
        return []

    placeholders = ",".join("?" for _ in lead_keys)
    return conn.execute(f"""
        SELECT l.lead_key, l.address, l.county, l.distress_type, l.sale_status,
               COALESCE(l.current_sale_date, ie.sale_date) AS sale_date,
               l.current_sale_date,
               l.original_sale_date,
               ae.avm_low, ae.avm_high, ae.attom_raw_json,
               cp.field_value_text AS contact_ready
        FROM leads l
        LEFT JOIN (
            SELECT lead_key, MAX(id) AS max_ie_id
            FROM ingest_events
            GROUP BY lead_key
        ) latest_ie ON latest_ie.lead_key = l.lead_key
        LEFT JOIN ingest_events ie ON ie.id = latest_ie.max_ie_id
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
        WHERE l.lead_key IN ({placeholders})
    """, tuple(lead_keys)).fetchall()


def _score_rows(conn: sqlite3.Connection, rows, run_id: str):
    print(f"[SCORING] run_id={run_id} scoring_rows={len(rows)}")

    today = date.today()

    for r in rows:
        sale_date_raw = r["sale_date"]
        dts = None
        if sale_date_raw:
            sale_date = date.fromisoformat(sale_date_raw)
            dts = (sale_date - today).days

        dts_score = score_dts(dts) if dts is not None else 0
        equity_score, equity_band = score_equity(r["avm_low"], r["avm_high"])
        property_detail = _extract_property_detail(r["attom_raw_json"])
        owner_mortgage = _extract_owner_mortgage(r["attom_raw_json"])
        contact_ready = _truthy_flag(r["contact_ready"])

        for key in ("owner_name", "owner_mail", "last_sale_date", "mortgage_lender"):
            prov_value = _latest_prov_text(conn, r["lead_key"], key)
            if prov_value:
                owner_mortgage[key] = prov_value
        prov_mortgage_date = _latest_prov_text(conn, r["lead_key"], "mortgage_date")
        if prov_mortgage_date:
            owner_mortgage["mortgage_date"] = prov_mortgage_date
        prov_amount = _latest_prov_num(conn, r["lead_key"], "mortgage_amount")
        if prov_amount is not None:
            owner_mortgage["mortgage_amount"] = prov_amount

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
            "distress_type": r["distress_type"],
            "sale_status": r["sale_status"],
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
            "mortgage_date": owner_mortgage.get("mortgage_date"),
            "mortgage_lender": owner_mortgage.get("mortgage_lender"),
            "mortgage_amount": owner_mortgage.get("mortgage_amount"),
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
            owner_mortgage.get("mortgage_lender")
            and (owner_mortgage.get("last_sale_date") or owner_mortgage.get("mortgage_date"))
        ) else 9 if (
            owner_mortgage.get("mortgage_lender") or owner_mortgage.get("last_sale_date") or owner_mortgage.get("mortgage_date")
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
        owner_agency_score = (
            10 if execution_reality["owner_agency"] == "HIGH"
            else 5 if execution_reality["owner_agency"] == "MEDIUM"
            else 0
        )
        intervention_window_score = (
            10 if execution_reality["intervention_window"] == "WIDE"
            else 6 if execution_reality["intervention_window"] == "MODERATE"
            else 2 if execution_reality["intervention_window"] == "TIGHT"
            else 0
        )
        lender_control_penalty = (
            8 if execution_reality["lender_control_intensity"] == "HIGH"
            else 3 if execution_reality["lender_control_intensity"] == "MEDIUM"
            else 0
        )
        influenceability_score = (
            12 if execution_reality["influenceability"] == "HIGH"
            else 6 if execution_reality["influenceability"] == "MEDIUM"
            else 0
        )
        sources = _source_set(conn, r["lead_key"])
        overlap_bonus = 0
        if "SUBSTITUTION_OF_TRUSTEE" in sources and "LIS_PENDENS" in sources:
            overlap_bonus += 5
        if sources.intersection({"API_TAX", "TAXPAGES", "OFFICIAL_TAX_SALE"}):
            overlap_bonus += 3
        if r["current_sale_date"] and r["original_sale_date"] and r["current_sale_date"] != r["original_sale_date"]:
            overlap_bonus += 3
        county_source_bonus = 0
        if str(r["sale_status"] or "").strip().lower() == "pre_foreclosure" and prefc_county_is_active(r["county"]):
            county_source_bonus += 4
            county_source_bonus += max(0, 2 - prefc_source_priority(r["distress_type"]))
        debt_confidence_bonus = 4 if str(quality.get("debt_confidence") or "").upper() == "FULL" else 0

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
            + owner_agency_score
            + intervention_window_score
            + influenceability_score
            + county_source_bonus
            + overlap_bonus
            + debt_confidence_bonus
            - lender_control_penalty
        )
        total_score = max(0, min(100, raw_score))

        has_valuation = bool(r["avm_low"] and r["avm_high"])
        has_owner_pair = bool(owner_mortgage.get("owner_name") and owner_mortgage.get("owner_mail"))
        is_pre_foreclosure = str(r["sale_status"] or "").strip().lower() == "pre_foreclosure"
        has_debt_pair = bool(
            owner_mortgage.get("mortgage_lender")
            and (owner_mortgage.get("last_sale_date") or owner_mortgage.get("mortgage_date"))
            and owner_mortgage.get("mortgage_amount") is not None
        )
        has_debt_proxy = bool(
            is_pre_foreclosure
            and quality.get("prefc_debt_proxy_ready")
            and owner_mortgage.get("mortgage_lender")
            and (owner_mortgage.get("last_sale_date") or owner_mortgage.get("mortgage_date"))
            and has_owner_pair
            and has_valuation
        )
        inside_green_window = dts is not None and 21 <= dts <= 60

        # ── Readiness tiers (investor-facing action labels) ──────────
        # READY_TO_CALL: has phone + AVM + debt + active sale date + not expired
        # REVIEW_FIRST:  has phone + AVM + not expired, OR pre-foreclosure with phone
        # EARLY_STAGE:   pre-foreclosure + has phone or mailing, no sale date yet
        # MONITOR:       missing phone, missing enrichment, or expired

        has_phone = contact_ready
        not_expired = dts is None or dts >= 0
        has_sale_date = dts is not None

        if (
            has_phone
            and has_valuation
            and (has_debt_pair or has_debt_proxy)
            and has_sale_date
            and not_expired
        ):
            readiness = "READY_TO_CALL"
        elif (
            has_phone
            and has_valuation
            and not_expired
        ):
            readiness = "REVIEW_FIRST"
        elif (
            is_pre_foreclosure
            and (has_phone or has_owner_pair)
        ):
            readiness = "EARLY_STAGE"
        else:
            readiness = "MONITOR"

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
                (r["lead_key"], "owner_agency",         "derived", execution_reality["owner_agency"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "intervention_window",  "derived", execution_reality["intervention_window"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "lender_control_intensity", "derived", execution_reality["lender_control_intensity"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
                (r["lead_key"], "influenceability",     "derived", execution_reality["influenceability"], None, None, None, None, "SCORING", _scored_at, run_id, _scored_at),
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


def score_leads_for_run(run_id: str):
    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row
    try:
        rows = _load_rows_for_run(conn, run_id)
        _score_rows(conn, rows, run_id)
    finally:
        conn.close()


def score_leads_by_keys(lead_keys: list[str], run_id: Optional[str] = None):
    if not lead_keys:
        return

    conn = sqlite3.connect(db_path())
    conn.row_factory = sqlite3.Row
    try:
        rows = _load_rows_for_lead_keys(conn, lead_keys)
        _score_rows(conn, rows, run_id or utc_now_iso())
    finally:
        conn.close()
