from __future__ import annotations

import json
import os
import re
import sqlite3

from ..storage import sqlite_store as _store
from ..utils import canonicalize_url
from ..bots.record_seed_utils import default_seed_path, iter_normalized_rows, load_seed_rows, match_lead_key

_PROBATE_KEYWORDS = ("estate of", "probate", "executor", "administrator", "letters testamentary")


def _scan_live_rows(con: sqlite3.Connection) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    cur = con.cursor()
    db_rows = cur.execute(
        """
        SELECT l.lead_key, l.address, l.county, ie.source_url, ie.raw_json, ie.sale_date, ie.ingested_at
        FROM leads l
        JOIN ingest_events ie ON ie.lead_key = l.lead_key
        ORDER BY ie.id DESC
        LIMIT 500
        """
    ).fetchall()
    for lead_key, address, county, source_url, raw_json, sale_date, ingested_at in db_rows:
        text = (raw_json or "")
        lower = text.lower()
        if not any(keyword in lower for keyword in _PROBATE_KEYWORDS):
            continue
        if lead_key in seen:
            continue
        seen.add(lead_key)
        estate_match = re.search(r"(Estate of [A-Za-z ,.'-]{3,80})", text, re.IGNORECASE)
        rows.append(
            {
                "lead_key": lead_key,
                "address": address or "",
                "county": county or "",
                "case_number": "",
                "filed_at": sale_date or ingested_at or "",
                "estate_name": estate_match.group(1).strip() if estate_match else "",
                "contact_name": "",
                "status": "keyword_hit",
                "source_url": source_url or "",
            }
        )
    return rows


def run():
    seed_file = os.environ.get("FALCO_PROBATE_SEED_FILE") or default_seed_path("probate_overlay.csv")
    con = sqlite3.connect(os.environ.get("FALCO_SQLITE_PATH", "data/falco.db"))
    seed_rows = load_seed_rows(seed_file) if os.path.isfile(seed_file) else []
    live_rows = _scan_live_rows(con)
    rows = [*live_rows, *seed_rows]
    matched = 0
    unmatched = 0
    written = 0

    try:
        for row in iter_normalized_rows(rows):
            lead_key = match_lead_key(con, row["lead_key"], row["address"], row["county"])
            if not lead_key:
                unmatched += 1
                continue

            matched += 1
            source_url = canonicalize_url(row["source_url"] or "")
            retrieved_at = row["filed_at"] or None
            artifact_ok, artifact_id = _store.insert_raw_artifact(
                lead_key,
                "PROBATE_OVERLAY",
                source_url or None,
                retrieved_at,
                "application/json",
                payload_text=json.dumps(row),
                notes=row["estate_name"] or "probate overlay row",
            )
            artifact_ref = artifact_id if artifact_ok else None

            for field_name, value in (
                ("probate_case_number", row["case_number"]),
                ("probate_filed_at", row["filed_at"]),
                ("probate_estate_name", row["estate_name"]),
                ("probate_contact_name", row["contact_name"]),
                ("probate_status", row["status"]),
                ("probate_source_url", source_url or None),
                ("probate_flag", "1"),
            ):
                if not value:
                    continue
                if _store.insert_provenance_text(
                    lead_key,
                    field_name,
                    value,
                    "PROBATE_OVERLAY",
                    retrieved_at=retrieved_at,
                    artifact_id=artifact_ref,
                ):
                    written += 1
    finally:
        con.close()

    summary = {
        "seed_rows": len(seed_rows),
        "live_rows": len(live_rows),
        "matched": matched,
        "unmatched": unmatched,
        "written": written,
    }
    print(f"[ProbateOverlay] summary {summary}")
    return summary
