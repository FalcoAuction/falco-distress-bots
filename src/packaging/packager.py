import json
import os
import sqlite3
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

from ..settings import get_dts_window, is_allowed_county, within_target_counties
from .pdf_builder import build_pdf_packet
from .data_quality import assess_packet_data
from .drive_uploader import upload_pdf, have_drive_creds
from ..gating.convertibility import is_institutional, apply_convertibility_gate
from ..utils import get_current_run_id
from ..enrichment.contact_enricher import enrich_contact_data
from ..uw.auto_underwrite import auto_underwrite, to_uw_json_payload, persist_auto_uw


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _sha256_file(path: str) -> Tuple[str, int]:
    h = hashlib.sha256()
    size = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            size += len(chunk)
            h.update(chunk)
    return h.hexdigest(), size


def _canonical_pdf_path(out_dir: str, lead_key: str) -> str:
    return os.path.join(out_dir, f"{lead_key}.pdf")


def _quality_sidecar_path(out_dir: str, lead_key: str) -> str:
    return os.path.join(out_dir, f"{lead_key}.quality.json")


def _write_json(path: str, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _hydrate_trustee_from_provenance(cur, lead_key: str) -> dict:
    want = ("ft_trustee_firm", "ft_trustee_name_raw", "notice_trustee_firm", "notice_trustee_name_raw")
    out = {}
    for k in want:
        row = cur.execute(
            """
            SELECT field_value_text
            FROM lead_field_provenance
            WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (lead_key, k),
        ).fetchone()
        if row and row[0]:
            out[k] = row[0]
    return out

def _hydrate_trustee_from_ingest_events(cur, lead_key: str) -> dict:
    out = {}
    try:
        rows = cur.execute(
            "SELECT raw_json FROM ingest_events WHERE lead_key=? AND raw_json IS NOT NULL ORDER BY id DESC LIMIT 5",
            (lead_key,),
        ).fetchall()
    except Exception:
        return out

    for row in rows:
        try:
            blob = json.loads(row[0] or "{}")
        except Exception:
            continue

        for key in ("trustee_attorney", "trustee_firm", "trustee", "contact_info"):
            value = blob.get(key)
            if isinstance(value, str) and value.strip():
                out.setdefault("trustee_attorney", value.strip())
                return out

    return out


def _hydrate_fallback_fields(cur, lead_key: str) -> dict:
    want_text = (
        "owner_name",
        "owner_mail",
        "last_sale_date",
        "mortgage_lender",
        "mortgage_lender_current",
        "mortgage_lender_original",
        "mortgage_lender_notice_holder",
        "mortgage_date",
        "mortgage_date_current",
        "property_identifier",
        "owner_phone_primary",
        "owner_phone_secondary",
        "notice_phone",
        "trustee_phone_public",
        "fsbo_listing_title",
        "fsbo_listing_description",
        "fsbo_signal_labels",
        "fsbo_listing_source",
    )
    want_num = (
        "mortgage_amount",
        "year_built",
        "building_area_sqft",
        "beds",
        "baths",
        "list_price",
        "fsbo_signal_score",
    )
    out = {}
    for field_name in want_text:
        row = cur.execute(
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
            out[field_name] = row[0]
    for field_name in want_num:
        row = cur.execute(
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
            out[field_name] = row[0]
    return out


def _latest_foreclosure_recorded_at(cur: sqlite3.Cursor, lead_key: str) -> Optional[str]:
    row = cur.execute(
        """
        SELECT recorded_at
        FROM foreclosure_events
        WHERE lead_key=? AND recorded_at IS NOT NULL
        ORDER BY COALESCE(event_at, recorded_at) DESC, event_key DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


# Distress types where a foreclosure contact is expected and required for outreach
_FORECLOSURE_DISTRESS_TYPES = frozenset({
    "LIS_PENDENS", "FORECLOSURE", "FORECLOSURE_TN",
    "SOT", "SUBSTITUTION_OF_TRUSTEE",
})


def _has_foreclosure_contact(cur: sqlite3.Cursor, lead_key: str, fields: Dict[str, Any]) -> bool:
    """
    Return True when at least one outreach-ready foreclosure contact signal is present.

    Signals checked (any one suffices):
      - trustee name  : ft_trustee_name_raw / notice_trustee_name_raw (already in fields)
      - trustee firm  : ft_trustee_firm / notice_trustee_firm (already in fields)
      - phone         : notice_phone in lead_field_provenance
      - sale location : notice_trustee_address in lead_field_provenance
                        OR sale_location in ingest_events raw_json
    """
    # Name, firm, and enriched phones loaded into fields by hydration / enrich_contact_data
    for k in (
        "ft_trustee_name_raw", "notice_trustee_name_raw",
        "ft_trustee_firm",     "notice_trustee_firm",
        "trustee_phone_public", "owner_phone_primary",
    ):
        if (fields.get(k) or "").strip():
            return True

    # Phone / address / enriched phone fallback via provenance
    for fname in ("notice_phone", "notice_trustee_address",
                  "trustee_phone_public", "owner_phone_primary"):
        try:
            row = cur.execute(
                """
                SELECT field_value_text FROM lead_field_provenance
                WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
                ORDER BY created_at DESC LIMIT 1
                """,
                (lead_key, fname),
            ).fetchone()
            if row and (row[0] or "").strip():
                return True
        except Exception:
            pass

    # Fallback: check ingest_events raw_json for sale_location / trustee / phone
    try:
        ie_rows = cur.execute(
            "SELECT raw_json FROM ingest_events WHERE lead_key=? AND raw_json IS NOT NULL ORDER BY id DESC LIMIT 5",
            (lead_key,),
        ).fetchall()
        for ie_row in ie_rows:
            try:
                blob = json.loads(ie_row[0] or "{}")
                for k in (
                    "sale_location", "trustee", "trustee_firm", "phone",
                    "trustee_attorney", "contact_info",  # keys used by TNForeclosureNotices
                ):
                    if (blob.get(k) or "").strip():
                        return True
            except Exception:
                pass
    except Exception:
        pass

    return False


def _fetch_internal_comps(
    cur: sqlite3.Cursor,
    subject_lead_key: str,
    county: str,
    subject_avm: float,
    subject_zip: Optional[str] = None,
    subject_city: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return up to 6 leads ordered by AVM proximity to subject_avm.
    Geo priority: same ZIP > same city > same county.
    AVM band: ±25% first pass; widens to ±40% if first pass returns nothing.
    Never raises; returns [] on any error or missing data.
    """
    _COMPS_SQL = """
        WITH latest_ae AS (
          SELECT
            lead_key,
            attom_raw_json,
            COALESCE(avm_value, avm_low) AS avm_anchor,
            avm_value,
            avm_low,
            avm_high,
            ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY enriched_at DESC) AS rn
          FROM attom_enrichments
          WHERE COALESCE(avm_value, avm_low) IS NOT NULL
        ),
        latest_ie AS (
          SELECT
            lead_key,
            sale_date,
            ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY id DESC) AS rn
          FROM ingest_events
        )
        SELECT
          l.lead_key,
          l.address,
          l.dts_days,
          latest_ie.sale_date,
          ae.avm_value,
          ae.avm_low,
          ae.avm_high
        FROM leads l
        JOIN latest_ae ae ON ae.lead_key = l.lead_key AND ae.rn = 1
        LEFT JOIN latest_ie ON latest_ie.lead_key = l.lead_key AND latest_ie.rn = 1
        WHERE l.lead_key != ?
          AND ABS(ae.avm_anchor - ?) <= ? * {band}
          AND {geo_filter}
        ORDER BY ABS(ae.avm_anchor - ?) ASC
        LIMIT 6
    """

    def _run(geo_filter: str, params: tuple, band: float) -> List:
        try:
            return cur.execute(
                _COMPS_SQL.format(geo_filter=geo_filter, band=band),
                params,
            ).fetchall()
        except Exception:
            return []

    def _geo_pass(band: float) -> List:
        avm_band = (subject_lead_key, subject_avm, subject_avm, subject_avm)
        rows: List = []
        if subject_zip and subject_zip.strip():
            rows = _run(
                "json_extract(ae.attom_raw_json, '$.detail.address.postal1') = ?",
                avm_band + (subject_zip.strip(),),
                band,
            )
        if not rows and subject_city and subject_city.strip():
            rows = _run(
                "json_extract(ae.attom_raw_json, '$.detail.address.locality') = ?",
                avm_band + (subject_city.strip(),),
                band,
            )
        if not rows and county:
            rows = _run(
                "l.county = ?",
                avm_band + (county,),
                band,
            )
        return rows

    rows = _geo_pass(0.25)
    if not rows:
        rows = _geo_pass(0.40)

    comps: List[Dict[str, Any]] = []
    for row in rows:
        try:
            comps.append({
                "address":   row["address"],
                "sale_date": row["sale_date"],
                "dts":       row["dts_days"],
                "avm_value": row["avm_value"],
                "avm_low":   row["avm_low"],
                "avm_high":  row["avm_high"],
            })
        except Exception:
            continue
    return comps


_LEAD_COLS = """
        WITH latest AS (
          SELECT
            lead_key,
            avm_value,
            avm_low,
            avm_high,
            confidence,
            status,
            enriched_at,
            attom_raw_json,
            ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY enriched_at DESC) AS rn
          FROM attom_enrichments
        )
        SELECT
          l.lead_key,
          l.address,
          l.county,
          l.state,
          l.distress_type,
          l.sale_status,
          l.current_sale_date,
          l.original_sale_date,
          l.first_seen_at,
          l.last_seen_at,
          l.falco_score_internal,
          l.auction_readiness,
          l.equity_band,
          l.dts_days,
          l.uw_ready,
          l.uw_json,
          le.avm_value,
          le.avm_low,
          le.avm_high,
          le.confidence,
          le.status AS attom_status,
          le.enriched_at,
          le.attom_raw_json AS attom_raw_json
        FROM leads l
        LEFT JOIN latest le
          ON le.lead_key = l.lead_key AND le.rn = 1
"""


def run() -> Dict[str, int]:
    # ── Repack mode: if set, process exactly one lead_key end-to-end ──────────
    repack_key = os.getenv("FALCO_REPACK_LEAD_KEY", "").strip()
    is_repack  = bool(repack_key)

    # Windowing stays deterministic + shared with the rest of the system
    dts_min, dts_max = get_dts_window("PACKAGER")
    max_packets = 1 if is_repack else int(os.getenv("FALCO_MAX_PACKETS_PER_RUN", "10"))

    force_repackage = os.getenv("FALCO_FORCE_REPACKAGE", "").strip() == "1"

    run_id  = get_current_run_id()
    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")

    require_uw = os.getenv("FALCO_REQUIRE_UW", "1").strip() != "0"

    packaged = 0
    skipped_missing = 0
    skipped_already = 0
    skipped_due_to_uw = 0
    auto_uw_generated = 0
    upload_failures = 0
    skipped_upload_missing_creds = 0
    skipped_institutional = 0
    skipped_missing_contact = 0
    vault_ready = 0
    batchdata_candidate_leads = 0
    quality_rows: List[Dict[str, Any]] = []

    drive_enabled = have_drive_creds()

    # Deterministic output layout
    out_dir = os.path.join(os.getcwd(), "out", "packets", run_id)
    os.makedirs(out_dir, exist_ok=True)

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    if is_repack:
        print(f"[REPACK] Mode active — targeting lead_key={repack_key!r} (run_id={run_id})")
        rows = cur.execute(
            _LEAD_COLS + "WHERE l.lead_key = ?",
            (repack_key,),
        ).fetchall()
        if not rows:
            print(f"[REPACK] lead_key={repack_key!r} not found in database — aborting")
            con.close()
            return {
                "packaged_count": 0,
                "repack_lead_key": repack_key,
                "repack_status": "not_found",
            }
    else:
        where_clause = """
        WHERE (
          (l.dts_days IS NOT NULL
           AND l.dts_days >= ?
           AND l.dts_days <= ?)
          OR l.sale_status = 'pre_foreclosure'
          OR UPPER(COALESCE(l.distress_type, '')) = 'FSBO'
        )
        """

        rows = cur.execute(
            _LEAD_COLS + where_clause + """
        ORDER BY
            CASE WHEN l.sale_status = 'pre_foreclosure' THEN 0 ELSE 1 END ASC,
            CASE WHEN UPPER(COALESCE(l.distress_type, '')) = 'FSBO' THEN 1 ELSE 2 END ASC,
            CASE COALESCE(l.auction_readiness, '')
                WHEN 'GREEN' THEN 1
                WHEN 'YELLOW' THEN 2
                WHEN 'PARTIAL' THEN 3
                ELSE 4
            END,
            COALESCE(l.falco_score_internal, 0) DESC,
            COALESCE(l.dts_days, 9999) ASC,
            l.lead_key ASC
        LIMIT ?
            """,
            (dts_min, dts_max, max_packets * 8),
        ).fetchall()

    for r in rows:
        if packaged >= max_packets:
            break

        lead_key = (r["lead_key"] or "").strip()
        if not lead_key:
            skipped_missing += 1
            continue

        fields: Dict[str, Any] = dict(r)

        # ── Compute derived enrichment fields ──────────────────────────────────
        # Parse attom_raw_json into avm / detail objects
        _raw = fields.get("attom_raw_json") or ""
        _attom_detail: Optional[Dict[str, Any]] = None
        _attom_avm_obj: Optional[Dict[str, Any]] = None
        if _raw:
            try:
                _parsed = json.loads(_raw) if isinstance(_raw, str) else _raw
                if isinstance(_parsed, dict):
                    if "avm" in _parsed and "detail" in _parsed:
                        _attom_avm_obj = _parsed.get("avm") or None
                        _attom_detail  = _parsed.get("detail") or None
                    elif "eventDate" in _parsed or "amount" in _parsed:
                        # Legacy AVM-only blob
                        _attom_avm_obj = _parsed
            except Exception:
                pass
        fields["attom_detail"]  = _attom_detail
        fields["attom_avm_obj"] = _attom_avm_obj

        # Value anchors
        _avm_low  = fields.get("avm_low")
        _avm_mid  = fields.get("avm_value")
        _avm_high = fields.get("avm_high")
        fields["value_anchor_low"]  = float(_avm_low)  if _avm_low  is not None else None
        fields["value_anchor_mid"]  = float(_avm_mid)  if _avm_mid  is not None else None
        fields["value_anchor_high"] = float(_avm_high) if _avm_high is not None else None

        # Spread
        _spread_pct  = None
        _spread_band = "UNKNOWN"
        if _avm_low is not None and float(_avm_low) > 0 and _avm_high is not None:
            _spread_pct  = (float(_avm_high) - float(_avm_low)) / float(_avm_low)
            _spread_band = "TIGHT" if _spread_pct <= 0.12 else "NORMAL" if _spread_pct <= 0.18 else "WIDE"
        fields["spread_pct"]  = _spread_pct
        fields["spread_band"] = _spread_band

        # Diamond proxy
        _dts_val    = fields.get("dts_days")
        _readiness  = (fields.get("auction_readiness") or "").upper()
        fields["diamond_proxy"] = bool(
            fields.get("attom_status") == "enriched"
            and _readiness == "GREEN"
            and _dts_val is not None and 21 <= int(_dts_val) <= 60
            and _avm_low is not None and float(_avm_low) >= 300_000
            and _spread_pct is not None and _spread_pct <= 0.18
        )

        # Extract detail summary fields (safe key access throughout)
        if _attom_detail and isinstance(_attom_detail, dict):
            _ident  = _attom_detail.get("identifier") or {}
            _summ   = _attom_detail.get("summary")    or {}
            _bldg   = _attom_detail.get("building")   or {}
            _vint   = _attom_detail.get("vintage")     or {}
            _lot    = _attom_detail.get("lot")         or {}
            _addrd  = _attom_detail.get("address")     or {}
            _rooms  = (_bldg.get("rooms")        if isinstance(_bldg, dict) else None) or {}
            _sized  = (_bldg.get("size")         if isinstance(_bldg, dict) else None) or {}
            _constr = (_bldg.get("construction") if isinstance(_bldg, dict) else None) or {}
            fields["property_identifier"] = (_ident.get("attomId") or _ident.get("fips")) if isinstance(_ident, dict) else None
            fields["property_type"]       = (_summ.get("proptype") or _summ.get("propClass"))    if isinstance(_summ, dict) else None
            fields["land_use"]            = _summ.get("propLandUse")                             if isinstance(_summ, dict) else None
            fields["year_built"]          = _vint.get("yearBuilt")                               if isinstance(_vint, dict) else None
            fields["building_area_sqft"]  = (_sized.get("livingSize") or _sized.get("bldgSize")) if isinstance(_sized, dict) else None
            fields["lot_size"]            = (_lot.get("lotSize1") or _lot.get("lotSizeAcres"))   if isinstance(_lot, dict) else None
            fields["beds"]                = (_rooms.get("beds") or _rooms.get("bedsCount"))       if isinstance(_rooms, dict) else None
            fields["baths"]               = (_rooms.get("bathsTotal") or _rooms.get("bathsFull")) if isinstance(_rooms, dict) else None
            fields["construction_type"]   = (_constr.get("frameType") or _constr.get("constructionType")) if isinstance(_constr, dict) else None
            fields["city"]                = (_addrd.get("locality") or _addrd.get("city"))        if isinstance(_addrd, dict) else None
            fields["zip"]                 = (_addrd.get("postal1") or _addrd.get("zip"))          if isinstance(_addrd, dict) else None
        # ── End computed fields ────────────────────────────────────────────────

        fields.update(_hydrate_trustee_from_provenance(cur, lead_key))
        for _k, _v in _hydrate_trustee_from_ingest_events(cur, lead_key).items():
            if _v and not (fields.get(_k) or "").strip():
                fields[_k] = _v
        for _k, _v in _hydrate_fallback_fields(cur, lead_key).items():
            if _v is not None:
                fields[_k] = _v
        if fields.get("mortgage_lender_current"):
            fields["mortgage_lender"] = fields.get("mortgage_lender_current")
        if fields.get("mortgage_date_current"):
            fields["mortgage_date"] = fields.get("mortgage_date_current")
        if fields.get("current_sale_date") and not fields.get("sale_date_iso"):
            fields["sale_date_iso"] = fields.get("current_sale_date")
        if fields.get("current_sale_date") and not fields.get("sale_date"):
            fields["sale_date"] = fields.get("current_sale_date")
        fields["distress_recorded_at"] = _latest_foreclosure_recorded_at(cur, lead_key)

        try:
            fields = apply_convertibility_gate(fields)
        except Exception:
            pass

        if is_institutional(fields):
            skipped_institutional += 1
            continue

        # ── Tier-2 / Tier-3 contact enrichment ────────────────────────────────
        # Runs after the institutional gate so we don't waste lookups on skipped leads.
        # Mutates `fields` and writes to lead_field_provenance; commit immediately
        # so provenance rows are durable even if this lead is later skipped.
        try:
            _ce_summary = enrich_contact_data(lead_key, fields, cur)
            if _ce_summary.get("t2_written") or _ce_summary.get("t3_written"):
                con.commit()
                print(
                    f"[CONTACT] lead_key={lead_key!r}"
                    f" t2={_ce_summary['t2_written']}"
                    f" t3={_ce_summary['t3_written']}"
                )
        except Exception as _ce_exc:
            print(f"[CONTACT][WARN] enrich_contact_data failed lead_key={lead_key!r}: {_ce_exc}")

        _is_pre_foreclosure = str(fields.get("sale_status") or "").strip().lower() == "pre_foreclosure"
        _is_fsbo = (fields.get("distress_type") or "").upper() == "FSBO"

        # UW gate — attempt auto-underwriting when uw_ready is missing.
        # Manual UW in manual_underwriting table is authoritative and is not overwritten.
        # In repack mode: auto-UW is still attempted so the packet gets UW data;
        #   if it fails, repack proceeds anyway (sparse UW section).
        _uw_ready_raw = int(fields.get("uw_ready") or 0)
        if _is_pre_foreclosure:
            fields.setdefault("uw_json", "")
            fields["uw_ready"] = 1
        elif _is_fsbo:
            fields.setdefault("uw_json", "")
            fields["uw_ready"] = 1
        elif _uw_ready_raw != 1:
            _auto_uw = auto_underwrite(fields)
            if _auto_uw["uw_ready"] == 1:
                fields["uw_json"]  = json.dumps(
                    to_uw_json_payload(_auto_uw, fields), ensure_ascii=False
                )
                fields["uw_ready"] = 1
                persist_auto_uw(lead_key, _auto_uw, fields, cur, con)
                auto_uw_generated += 1
                print(
                    f"[AUTO_UW] generated lead_key={lead_key!r}"
                    f" ready=1 blocker=none"
                    f" bid_cap={_auto_uw.get('manual_bid_cap')}"
                )
            elif not is_repack and require_uw:
                print(
                    f"[AUTO_UW] skipped lead_key={lead_key!r}"
                    f" blocker={_auto_uw.get('uw_blocker')!r}"
                )
                skipped_due_to_uw += 1
                continue
            # repack + auto-UW failed → fall through; packet builds with sparse UW

        if is_repack or force_repackage:
            # Force-rebuild: bypass the "already packaged this run" guard
            if is_repack:
                print(f"[REPACK] Building packet for lead_key={lead_key!r}")
        else:
            # already packaged for THIS run_id — skip only if path is already canonical
            existing = cur.execute(
                "SELECT pdf_path FROM packets WHERE run_id=? AND lead_key=? LIMIT 1",
                (run_id, lead_key),
            ).fetchone()
            if existing:
                existing_path = existing["pdf_path"] or ""
                if "falco_packet_" not in existing_path:
                    skipped_already += 1
                    continue
                # legacy path — fall through to rebuild and update

        # required inputs
        _is_lp   = (fields.get("distress_type") or "").upper() == "LIS_PENDENS"
        _has_avm = fields.get("avm_low") is not None and fields.get("avm_high") is not None
        if not fields.get("address") or (not _is_lp and not _is_pre_foreclosure and not _is_fsbo and not _has_avm):
            skipped_missing += 1
            continue
        # LP leads without AVM proceed but default to YELLOW readiness pending enrichment
        if _is_lp and not _has_avm:
            fields.setdefault("auction_readiness", "YELLOW")
        if _is_pre_foreclosure:
            if str(fields.get("auction_readiness") or "").upper() == "RED":
                fields["auction_readiness"] = "PARTIAL"
            else:
                fields.setdefault("auction_readiness", "PARTIAL")

        # HARD GEO GATE (WAR-PLAN)
        _county_gate = (fields.get("county") or "").strip()
        if _county_gate and (not is_allowed_county(_county_gate) or not within_target_counties(_county_gate)):
            skipped_missing += 1
            continue

        # Foreclosure contact gate — skip if no outreach signal exists
        _distress_upper = (fields.get("distress_type") or "").upper()
        if _distress_upper in _FORECLOSURE_DISTRESS_TYPES:
            if not _has_foreclosure_contact(cur, lead_key, fields):
                print(f"[PACKAGER] skipped lead_key={lead_key!r} reason=missing_foreclosure_contact")
                skipped_missing_contact += 1
                continue

        # Internal comps proxy (no new APIs)
        _subject_avm  = fields.get("avm_value") or fields.get("avm_low")
        _county       = fields.get("county") or ""
        _subject_zip  = (fields.get("zip") or "").strip() or None
        _subject_city = (fields.get("city") or "").strip() or None

        # Fallback: parse zip/city from address string if still missing
        if not _subject_zip or not _subject_city:
            _addr_str = fields.get("address") or ""
            if isinstance(_addr_str, str) and _addr_str.strip():
                import re as _re
                if not _subject_zip:
                    _zip_m = _re.search(r"\b(\d{5})\b", _addr_str)
                    if _zip_m:
                        _subject_zip = _zip_m.group(1)
                if not _subject_city:
                    _city_m = _re.search(
                        r",\s*([^,]+?)\s*,\s*[A-Z]{2}\b", _addr_str
                    )
                    if _city_m:
                        _subject_city = _city_m.group(1).strip() or None
        if _subject_avm is not None and (_subject_zip or _subject_city or _county):
            try:
                fields["internal_comps"] = _fetch_internal_comps(
                    cur, lead_key, _county, float(_subject_avm),
                    subject_zip=_subject_zip,
                    subject_city=_subject_city,
                )
            except Exception:
                fields["internal_comps"] = []
        else:
            fields["internal_comps"] = []

        quality = assess_packet_data(fields)
        if _is_fsbo and not quality.get("fsbo_review_ready"):
            skipped_missing += 1
            continue
        fields["packet_quality"] = quality
        fields["packet_completeness_pct"] = quality["packet_completeness_pct"]
        fields["vault_publish_ready"] = quality["vault_publish_ready"]
        fields["pre_foreclosure_review_ready"] = quality.get("pre_foreclosure_review_ready", False)
        fields["fsbo_review_ready"] = quality.get("fsbo_review_ready", False)
        fields["fsbo_actionability_band"] = quality.get("fsbo_actionability_band")
        fields["fsbo_actionability_reasons"] = quality.get("fsbo_actionability_reasons", [])
        fields["fsbo_vault_ready"] = quality.get("fsbo_vault_ready", False)
        fields["fsbo_price_gap_pct"] = quality.get("fsbo_price_gap_pct")
        fields["fsbo_days_tracked"] = quality.get("fsbo_days_tracked")
        fields["distress_lane"] = "Seller-Direct Opportunity" if _is_fsbo else fields.get("distress_lane")
        fields["vault_publish_blockers"] = quality["vault_publish_blockers"]
        fields["batchdata_fallback_targets"] = quality["batchdata_fallback_targets"]

        if _is_fsbo:
            actionability_band = str(quality.get("fsbo_actionability_band") or "").upper()
            if actionability_band == "ACTIONABLE_NOW":
                fields["auction_readiness"] = "GREEN"
            elif actionability_band == "REVIEW":
                fields["auction_readiness"] = "YELLOW"
            else:
                fields["auction_readiness"] = "PARTIAL"

        if quality["vault_publish_ready"] or quality.get("pre_foreclosure_review_ready"):
            vault_ready += 1
        if quality["batchdata_fallback_targets"]:
            batchdata_candidate_leads += 1

        # Build deterministic pdf path (pdf_builder should accept out_dir)
        try:
            local_path = build_pdf_packet(fields, out_dir=out_dir)
        except Exception as _pdf_exc:
            import traceback as _tb
            print(f"[PACKAGER][ERROR] build_pdf_packet failed lead_key={lead_key}: {type(_pdf_exc).__name__}: {_pdf_exc}")
            _tb.print_exc()
            skipped_missing += 1
            continue

        # Registry: sha + bytes + path
        sha, nbytes = _sha256_file(local_path)
        cur.execute(
            """
            INSERT INTO packets (run_id, lead_key, pdf_path, sha256, bytes, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id, lead_key) DO UPDATE SET
                pdf_path   = excluded.pdf_path,
                sha256     = excluded.sha256,
                bytes      = excluded.bytes,
                created_at = excluded.created_at
            """,
            (run_id, lead_key, local_path, sha, nbytes, _utc_now_iso()),
        )
        con.commit()

        # Upload optional (kept)
        pdf_url = None
        if drive_enabled:
            pdf_url = upload_pdf(local_pdf_path=local_path, filename=os.path.basename(local_path))
            if not pdf_url:
                upload_failures += 1
        else:
            skipped_upload_missing_creds += 1

        if is_repack:
            print(f"[REPACK] Packet written: {local_path}")

        sidecar = {
            "lead_key": lead_key,
            "address": fields.get("address"),
            "county": fields.get("county"),
            "distress_type": fields.get("distress_type"),
            "auction_readiness": fields.get("auction_readiness"),
            "falco_score_internal": fields.get("falco_score_internal"),
            "packet_pdf_path": local_path,
            **quality,
        }
        _write_json(_quality_sidecar_path(out_dir, lead_key), sidecar)
        quality_rows.append(sidecar)
        packaged += 1

    con.close()

    if quality_rows:
        blocker_counts: Dict[str, int] = {}
        for row in quality_rows:
            for blocker in row.get("vault_publish_blockers", []):
                blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
        report = {
            "run_id": run_id,
            "generated_at": _utc_now_iso(),
            "packaged_count": packaged,
            "vault_ready_count": vault_ready,
            "batchdata_candidate_leads": batchdata_candidate_leads,
            "top_blockers": sorted(
                blocker_counts.items(),
                key=lambda item: (-item[1], item[0]),
            ),
            "leads": quality_rows,
        }
        _write_json(os.path.join(out_dir, "vault_readiness_report.json"), report)

    print(
        f"[PACKAGER] packaged={packaged}"
        f" auto_uw={auto_uw_generated}"
        f" skipped_uw={skipped_due_to_uw}"
        f" skipped_missing={skipped_missing}"
        f" skipped_already={skipped_already}"
        f" skipped_institutional={skipped_institutional}"
        f" skipped_missing_contact={skipped_missing_contact}"
        f" vault_ready={vault_ready}"
        f" batchdata_candidates={batchdata_candidate_leads}"
    )

    summary: Dict[str, Any] = {
        "packaged_count":    packaged,
        "auto_uw_generated": auto_uw_generated,
        "skipped_due_to_uw": skipped_due_to_uw,
        "skipped_packaging_missing_fields": skipped_missing,
        "skipped_packaging_already_done": skipped_already,
        "upload_failures": upload_failures,
        "skipped_packaging_institutional": skipped_institutional,
        "skipped_missing_contact": skipped_missing_contact,
        "vault_ready_count": vault_ready,
        "batchdata_candidate_leads": batchdata_candidate_leads,
    }
    if not drive_enabled:
        summary["skipped_upload_missing_creds"] = skipped_upload_missing_creds
    if is_repack:
        summary["repack_lead_key"] = repack_key
        summary["repack_status"]   = "ok" if packaged else "failed"

    return summary
