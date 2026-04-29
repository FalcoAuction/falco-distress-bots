import json
import os

from src.gating.convertibility import apply_convertibility_gate
from ..utils import make_lead_key
from ..storage.supabase_store import upsert_lead, find_existing_by_lead_key


def _normalize_gate_decision(decision, payload):
    """Normalize gate return to (keep: bool, reason: str | None)."""
    if isinstance(decision, bool):
        return decision, None
    if isinstance(decision, tuple):
        keep = decision[0]
        reason = decision[1] if len(decision) > 1 else None
        return keep, reason
    if isinstance(decision, dict):
        if "keep" in decision:
            return decision["keep"], decision.get("reason")
        # Gate returned the mutated payload — infer from status_flag
        flag = decision.get("status_flag")
        return flag != "INSTITUTIONAL", flag
    return True, None


def run():
    seed_file = os.environ.get("FALCO_TAX_API_SEED_FILE")

    if not seed_file:
        print("[API_TaxDelinquentBot] No seed file configured — skipping.")
        return {}

    if not os.path.isfile(seed_file):
        print("[API_TaxDelinquentBot] Seed file not found.")
        return {}

    with open(seed_file, "r", encoding="utf-8") as f:
        raw_lines = [line for line in f if line.strip()]

    seed_rows = len(raw_lines)
    print(f"[API_TaxDelinquentBot] Loaded {seed_rows} seed rows.")

    invalid_rows = 0
    valid_rows = 0
    gated_kept = 0
    gated_skipped_institutional = 0
    gated_skipped_other = 0
    dedupe_kept = 0
    dedupe_skipped_in_run = 0
    seen_in_run = set()
    created = 0
    updated = 0
    would_create = 0
    would_update = 0

    for line in raw_lines:
        row = json.loads(line)
        if not row.get("address") or not row.get("county"):
            invalid_rows += 1
            continue

        valid_rows += 1
        payload = {
            "address": row["address"],
            "county": row["county"],
            "state": row.get("state"),
            "distress_type": "TAX_DELINQUENT",
            "source": "API_TAX",
            "external_id": row.get("external_id"),
            "raw": row,
        }

        decision = apply_convertibility_gate(payload)
        keep, reason = _normalize_gate_decision(decision, payload)

        if keep:
            gated_kept += 1
        elif reason == "INSTITUTIONAL":
            gated_skipped_institutional += 1
            continue
        else:
            gated_skipped_other += 1
            continue

        lead_key = make_lead_key("API_TAX", payload["address"], payload["county"])
        payload["lead_key"] = lead_key

        if lead_key in seen_in_run:
            dedupe_skipped_in_run += 1
            continue

        seen_in_run.add(lead_key)
        dedupe_kept += 1

        existing = find_existing_by_lead_key(payload["lead_key"])
        result = upsert_lead(payload)
        if existing:
            updated += 1 if result == "inserted" else 0
        else:
            created += 1 if result == "inserted" else 0

    print(f"[API_TaxDelinquentBot] Valid rows: {valid_rows} | Invalid rows: {invalid_rows}")
    print(
        f"[API_TaxDelinquentBot] Gate kept: {gated_kept} | "
        f"Institutional skipped: {gated_skipped_institutional} | "
        f"Other skipped: {gated_skipped_other}"
    )
    print(f"[API_TaxDelinquentBot] Dedupe kept: {dedupe_kept} | Skipped in-run: {dedupe_skipped_in_run}")
    print(
        f"[API_TaxDelinquentBot] created={created} updated={updated} "
        f"would_create={would_create} would_update={would_update}"
    )

    return {
        "seed_rows": seed_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
        "gated_kept": gated_kept,
        "gated_skipped_institutional": gated_skipped_institutional,
        "gated_skipped_other": gated_skipped_other,
        "dedupe_kept": dedupe_kept,
        "dedupe_skipped_in_run": dedupe_skipped_in_run,
        "created": created,
        "updated": updated,
        "would_create": would_create,
        "would_update": would_update,
    }
