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
        rows = [line for line in f if line.strip()]

    count = len(rows)
    print(f"[API_TaxDelinquentBot] Loaded {count} seed rows.")
    return {"seed_rows": count}
