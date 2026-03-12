from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..packaging.data_quality import assess_packet_data

ROOT = Path(__file__).resolve().parents[2]
SITE_REPO = ROOT.parent / "falco-site"
SITE_DATA_DIR = SITE_REPO / "data"
SITE_OPERATOR_DIR = SITE_DATA_DIR / "operator"
SITE_OUTREACH_DIR = SITE_DATA_DIR / "outreach"
SITE_VAULT_LISTINGS = SITE_DATA_DIR / "vault_listings.ndjson"
OUTREACH_DIR = ROOT / "out" / "outreach"
REPORTS_DIR = ROOT / "out" / "reports"


def _db_path() -> Path:
    return Path(os.environ.get("FALCO_SQLITE_PATH", "data/falco.db"))


def _connect() -> sqlite3.Connection:
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
        for field_name in ("year_built", "building_area_sqft", "beds", "baths"):
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
        if not quality["vault_publish_ready"]:
            continue

        candidates.append(
            {
                "lead_key": lead_key,
                "address": lead["address"],
                "county": lead["county"],
                "distress_type": lead["distress_type"],
                "falco_score_internal": lead["falco_score_internal"],
                "auction_readiness": lead["auction_readiness"],
                "equity_band": lead["equity_band"],
                "dts_days": lead["dts_days"],
                "uw_ready": lead["uw_ready"],
                "vaultLive": False,
                "vaultSlug": None,
                "vaultPublishReady": bool(quality["vault_publish_ready"]),
                "topTierReady": bool(quality["top_tier_ready"]),
                "packetCompletenessPct": quality["packet_completeness_pct"],
                "executionBlockers": quality["execution_blockers"],
            }
        )

    candidates.sort(
        key=lambda row: (
            0 if row["vaultPublishReady"] else 1,
            0 if str(row.get("auction_readiness") or "").upper() == "GREEN" else 1,
            -(row.get("falco_score_internal") or 0),
            row.get("dts_days") or 9999,
        )
    )
    return candidates[:limit]


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
        },
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


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
    operator_payload = _operator_snapshot()
    operator_payload["analyst"] = _load_latest_analyst_snapshot()
    _write_json(operator_path, operator_payload)
    outreach_paths = _refresh_outreach_snapshots()

    return {
        "ok": True,
        "operator": str(operator_path),
        "outreach": outreach_paths,
    }
