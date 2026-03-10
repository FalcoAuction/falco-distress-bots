from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _reports_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "out" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def write_enrichment_trigger_report(run_id: str, run_summary: Dict[str, Any]) -> Dict[str, Any]:
    quality = run_summary.get("quality") if isinstance(run_summary.get("quality"), dict) else {}
    leads = quality.get("leads") if isinstance(quality.get("leads"), list) else []

    trigger_rows: List[Dict[str, Any]] = []
    field_counter: Counter[str] = Counter()

    for row in leads:
        targets = list(row.get("batchdata_fallback_targets") or [])
        readiness = str(row.get("auction_readiness") or "").upper()
        if not targets:
            continue
        if readiness not in {"GREEN", "YELLOW"}:
            continue

        field_counter.update(targets)
        trigger_rows.append(
            {
                "lead_key": row.get("lead_key"),
                "address": row.get("address") or row.get("lead_key"),
                "county": row.get("county"),
                "score": row.get("falco_score_internal"),
                "readiness": readiness,
                "dts_days": row.get("dts_days"),
                "batchdata_targets": targets,
                "execution_blockers": row.get("execution_blockers") or [],
            }
        )

    report = {
        "agent": "enrichment_trigger",
        "generated_at": _utc_now(),
        "run_id": run_id,
        "trigger_count": len(trigger_rows),
        "top_field_targets": field_counter.most_common(10),
        "batchdata_retry_candidates": trigger_rows[:15],
        "operator_notes": [
            "These are the leads closest to improvement via selective fallback enrichment.",
            "If trigger_count is low, BatchData is already catching up or the remaining leads are not worth the spend.",
        ],
    }

    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_enrichment_trigger.json"
    latest_path = reports_dir / "latest_enrichment_trigger.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    return {
        "ok": True,
        "path": str(run_path),
        "trigger_count": len(trigger_rows),
    }
