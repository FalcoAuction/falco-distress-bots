from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


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


def write_source_watch_report(run_id: str, stage_results: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    con = _connect()
    try:
        recent_sources = con.execute(
            """
            SELECT COALESCE(source, 'UNKNOWN') AS source, COUNT(*) AS event_count
            FROM ingest_events
            WHERE datetime(ingested_at) >= datetime('now', '-7 days')
            GROUP BY COALESCE(source, 'UNKNOWN')
            ORDER BY event_count DESC, source ASC
            """
        ).fetchall()
        recent_counties = con.execute(
            """
            SELECT COALESCE(county, 'UNKNOWN') AS county, COUNT(*) AS lead_count
            FROM leads
            WHERE datetime(first_seen_at) >= datetime('now', '-7 days')
            GROUP BY COALESCE(county, 'UNKNOWN')
            ORDER BY lead_count DESC, county ASC
            """
        ).fetchall()
    finally:
        con.close()

    live_stage = []
    alerts: list[str] = []
    source_counter = Counter({row["source"]: int(row["event_count"]) for row in recent_sources})

    if source_counter:
        top_source, top_count = source_counter.most_common(1)[0]
        total = sum(source_counter.values())
        if total and top_count / total >= 0.8:
            alerts.append(
                f"Source concentration is high: {top_source} produced {top_count}/{total} recent ingest events."
            )

    for stage in stage_results:
        name = str(stage.get("name") or "")
        if not name.endswith("Bot"):
            continue
        result = stage.get("result") if isinstance(stage.get("result"), dict) else {}
        entry = {
            "name": name,
            "ok": bool(stage.get("ok")),
            "stored_leads": int(result.get("stored_leads", 0) or 0),
            "live_rows": int(result.get("live_rows", 0) or 0),
            "valid_rows": int(result.get("valid_rows", 0) or 0),
            "dts_skipped": int(result.get("dts_skipped", 0) or 0),
            "geo_skipped": int(result.get("geo_skipped", 0) or 0),
            "gate_skipped": int(result.get("gate_skipped", 0) or 0),
        }
        live_stage.append(entry)
        if name in {"OfficialTaxSalesBot", "SheriffSalesBot", "ClerkMasterSalesBot"} and entry["stored_leads"] == 0:
            alerts.append(
                f"{name} is wired but produced 0 stored leads on the latest run."
            )

    report = {
        "agent": "source_watch",
        "generated_at": _utc_now(),
        "run_id": run_id,
        "db_path": _db_path(),
        "recent_sources_7d": [
            {"source": row["source"], "event_count": int(row["event_count"])}
            for row in recent_sources
        ],
        "recent_counties_7d": [
            {"county": row["county"], "lead_count": int(row["lead_count"])}
            for row in recent_counties
        ],
        "latest_stage_yield": live_stage,
        "alerts": alerts,
    }

    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_source_watch.json"
    latest_path = reports_dir / "latest_source_watch.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    return {
        "ok": True,
        "path": str(run_path),
        "alert_count": len(alerts),
    }
