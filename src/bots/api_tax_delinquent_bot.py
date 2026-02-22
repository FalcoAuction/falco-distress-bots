import json
import os


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

    valid = []
    invalid_rows = 0

    for line in raw_lines:
        row = json.loads(line)
        if not row.get("address") or not row.get("county"):
            invalid_rows += 1
            continue
        valid.append({
            "address": row["address"],
            "county": row["county"],
            "state": row.get("state"),
            "distress_type": "TAX_DELINQUENT",
            "source": "API_TAX",
            "external_id": row.get("external_id"),
            "raw": row,
        })

    valid_rows = len(valid)
    print(f"[API_TaxDelinquentBot] Valid rows: {valid_rows} | Invalid rows: {invalid_rows}")

    return {
        "seed_rows": seed_rows,
        "valid_rows": valid_rows,
        "invalid_rows": invalid_rows,
    }
