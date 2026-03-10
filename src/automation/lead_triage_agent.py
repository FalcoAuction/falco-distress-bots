from __future__ import annotations

import json
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


def _lead_label(row: Dict[str, Any]) -> str:
    return str(row.get("address") or row.get("lead_key") or "Unknown lead")


def write_lead_triage_report(run_id: str, run_summary: Dict[str, Any]) -> Dict[str, Any]:
    quality = run_summary.get("quality") if isinstance(run_summary.get("quality"), dict) else {}
    leads = quality.get("leads") if isinstance(quality.get("leads"), list) else []

    top_now: List[Dict[str, Any]] = []
    vault_candidates: List[Dict[str, Any]] = []
    near_misses: List[Dict[str, Any]] = []
    blocked: List[Dict[str, Any]] = []

    for row in leads:
        entry = {
            "lead_key": row.get("lead_key"),
            "address": _lead_label(row),
            "county": row.get("county"),
            "score": row.get("falco_score_internal"),
            "readiness": row.get("auction_readiness"),
            "dts_days": row.get("dts_days"),
            "packet_completeness_pct": row.get("packet_completeness_pct"),
            "vault_publish_ready": row.get("vault_publish_ready"),
            "top_tier_ready": row.get("top_tier_ready"),
            "execution_blockers": row.get("execution_blockers") or [],
            "batchdata_targets": row.get("batchdata_fallback_targets") or [],
        }

        if row.get("top_tier_ready"):
            top_now.append(entry)
        elif row.get("vault_publish_ready"):
            vault_candidates.append(entry)
        elif str(row.get("auction_readiness") or "").upper() in {"GREEN", "YELLOW"} and len(entry["execution_blockers"]) <= 2:
            near_misses.append(entry)
        else:
            blocked.append(entry)

    report = {
        "agent": "lead_triage",
        "generated_at": _utc_now(),
        "run_id": run_id,
        "overview": {
            "top_now_count": len(top_now),
            "vault_candidate_count": len(vault_candidates),
            "near_miss_count": len(near_misses),
            "blocked_count": len(blocked),
        },
        "top_now": top_now[:10],
        "vault_candidates": vault_candidates[:10],
        "near_misses": near_misses[:10],
        "blocked": blocked[:10],
        "operator_notes": [
            "Top now = true turnkey leads worth partner-facing routing now.",
            "Vault candidates = live-worthy but not top shelf.",
            "Near misses = leads worth targeted enrichment or contact-path cleanup.",
        ],
    }

    reports_dir = _reports_dir()
    run_path = reports_dir / f"run_{run_id}_lead_triage.json"
    latest_path = reports_dir / "latest_lead_triage.json"
    payload = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    run_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    return {
        "ok": True,
        "path": str(run_path),
        "top_now_count": len(top_now),
        "near_miss_count": len(near_misses),
    }
