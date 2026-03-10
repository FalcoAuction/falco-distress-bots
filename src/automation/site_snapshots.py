from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
SITE_REPO = ROOT.parent / "falco-site"
SITE_DATA_DIR = SITE_REPO / "data"
SITE_OPERATOR_DIR = SITE_DATA_DIR / "operator"
SITE_OUTREACH_DIR = SITE_DATA_DIR / "outreach"
SITE_VAULT_LISTINGS = SITE_DATA_DIR / "vault_listings.ndjson"
OUTREACH_DIR = ROOT / "out" / "outreach"


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
                  score_updated_at
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
    finally:
        con.close()

    live_slugs = _load_live_slugs()

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
            "pendingApprovals": 0,
        },
        "recentLeads": _attach_vault_state(recent_leads, live_slugs),
        "topCandidates": _attach_vault_state(top_candidates, live_slugs),
        "recentPackets": _attach_vault_state(recent_packets, live_slugs),
    }


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


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
    _write_json(operator_path, operator_payload)
    outreach_paths = _refresh_outreach_snapshots()

    return {
        "ok": True,
        "operator": str(operator_path),
        "outreach": outreach_paths,
    }
