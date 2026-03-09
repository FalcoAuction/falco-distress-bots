from __future__ import annotations

import json
import os
import sqlite3

from ..storage import sqlite_store as _store
from ..utils import canonicalize_url
from ..bots.record_seed_utils import iter_normalized_rows, load_seed_rows, match_lead_key


def run():
    seed_file = os.environ.get("FALCO_PROBATE_SEED_FILE")
    if not seed_file:
        print("[ProbateOverlay] No seed file configured - skipping.")
        return {}
    if not os.path.isfile(seed_file):
        print("[ProbateOverlay] Seed file not found.")
        return {}

    rows = load_seed_rows(seed_file)
    con = sqlite3.connect(os.environ.get("FALCO_SQLITE_PATH", "data/falco.db"))
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
        "seed_rows": len(rows),
        "matched": matched,
        "unmatched": unmatched,
        "written": written,
    }
    print(f"[ProbateOverlay] summary {summary}")
    return summary
