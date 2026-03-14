from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib import parse as urllib_parse

from ..enrichment.attom_enricher import run as run_attom_enrichment
from ..packaging.packager import run as run_packager
from ..scoring.scorer import score_leads_by_keys
from .site_snapshots import _site_supabase_config, _supabase_rest_request

OPERATOR_ENRICHMENT_COMPANY = "__falco_operator_enrichment__"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_notes(notes: str | None) -> dict[str, Any] | None:
    if not notes:
        return None
    try:
        parsed = json.loads(notes)
        if not isinstance(parsed, dict):
            return None
        if parsed.get("version") != 1 or parsed.get("type") != "operator_enrichment":
            return None
        if not isinstance(parsed.get("leadKey"), str):
            return None
        return parsed
    except Exception:
        return None


def _build_notes(
    lead_key: str,
    note: str,
    requested_by: str,
    result_message: str,
) -> str:
    return json.dumps(
        {
            "version": 1,
            "type": "operator_enrichment",
            "leadKey": lead_key,
            "note": note,
            "requestedBy": requested_by,
            "resultMessage": result_message,
            "updatedAt": _utc_now(),
        },
        ensure_ascii=False,
    )


def _list_requests() -> tuple[str | None, str | None, list[dict[str, Any]]]:
    supabase_url, service_role_key = _site_supabase_config()
    if not supabase_url or not service_role_key:
      return None, None, []

    base_url = supabase_url.rstrip("/") + "/rest/v1/partner_access_requests"
    query = urllib_parse.urlencode(
        {
            "select": "id,email,full_name,company,notes,status,created_at",
            "company": f"eq.{OPERATOR_ENRICHMENT_COMPANY}",
            "status": "in.(enrichment_requested,enrichment_processing)",
            "order": "created_at.asc",
            "limit": "25",
        }
    )
    rows = _supabase_rest_request("GET", f"{base_url}?{query}", service_role_key)
    return base_url, service_role_key, rows if isinstance(rows, list) else []


def _patch_request(
    base_url: str,
    service_role_key: str,
    row_id: str,
    status: str,
    notes: str,
) -> None:
    _supabase_rest_request(
        "PATCH",
        f"{base_url}?id=eq.{urllib_parse.quote(row_id)}",
        service_role_key,
        {
            "status": status,
            "notes": notes,
        },
    )


def process_operator_enrichment_requests(run_id: str) -> dict[str, Any]:
    base_url, service_role_key, rows = _list_requests()
    if not base_url or not service_role_key:
        return {
            "ok": True,
            "processed": 0,
            "requested": 0,
            "reason": "Supabase operator enrichment queue unavailable",
        }

    parsed_rows: list[dict[str, Any]] = []
    for row in rows:
        notes = _parse_notes(row.get("notes"))
        lead_key = str((notes or {}).get("leadKey") or row.get("full_name") or "").strip()
        if not lead_key:
            continue
        parsed_rows.append(
            {
                "id": str(row.get("id") or "").strip(),
                "lead_key": lead_key,
                "note": str((notes or {}).get("note") or "").strip(),
                "requested_by": str((notes or {}).get("requestedBy") or "").strip(),
            }
        )

    if not parsed_rows:
        return {"ok": True, "processed": 0, "requested": 0}

    lead_keys = []
    seen = set()
    for row in parsed_rows:
        if row["lead_key"] in seen:
            continue
        seen.add(row["lead_key"])
        lead_keys.append(row["lead_key"])

    for row in parsed_rows:
        if row["id"]:
            _patch_request(
                base_url,
                service_role_key,
                row["id"],
                "enrichment_processing",
                _build_notes(
                    row["lead_key"],
                    row["note"],
                    row["requested_by"] or "FALCO Operator",
                    "Enrichment request is running.",
                ),
            )

    env_backup = {key: os.environ.get(key) for key in (
        "FALCO_STAGE2_SOURCE",
        "FALCO_ATTOM_TARGET_LEAD_KEYS",
        "FALCO_ATTOM_MAX_ENRICH",
        "FALCO_MAX_ATTOM_CALLS_PER_RUN",
    )}

    processed = 0
    failed = 0
    try:
        os.environ["FALCO_STAGE2_SOURCE"] = "sqlite"
        os.environ["FALCO_ATTOM_TARGET_LEAD_KEYS"] = ",".join(lead_keys)
        os.environ["FALCO_ATTOM_MAX_ENRICH"] = str(max(len(lead_keys), 1))
        os.environ["FALCO_MAX_ATTOM_CALLS_PER_RUN"] = str(max(len(lead_keys) * 4, 4))

        run_attom_enrichment()
        score_leads_by_keys(lead_keys, run_id=f"{run_id}_operator_enrichment")

        for lead_key in lead_keys:
            os.environ["FALCO_REPACK_LEAD_KEY"] = lead_key
            run_packager()
            processed += 1
    except Exception as exc:
        failed = len(lead_keys)
        for row in parsed_rows:
            if row["id"]:
                _patch_request(
                    base_url,
                    service_role_key,
                    row["id"],
                    "enrichment_failed",
                    _build_notes(
                        row["lead_key"],
                        row["note"],
                        row["requested_by"] or "FALCO Operator",
                        f"Enrichment refresh failed: {type(exc).__name__}: {exc}",
                    ),
                )
        raise
    finally:
        os.environ.pop("FALCO_REPACK_LEAD_KEY", None)
        for key, value in env_backup.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    for row in parsed_rows:
        if row["id"]:
            _patch_request(
                base_url,
                service_role_key,
                row["id"],
                "enrichment_completed",
                _build_notes(
                    row["lead_key"],
                    row["note"],
                    row["requested_by"] or "FALCO Operator",
                    "Enrichment refresh completed. Reload operator review for updated valuation, contact, and packet data.",
                ),
            )

    return {
        "ok": True,
        "requested": len(lead_keys),
        "processed": processed,
        "failed": failed,
        "lead_keys": lead_keys,
    }
