# Operational Seed Files

These are the live default seed-file locations used by the new expansion phases.

Default files:
- [official_tax_sales.csv](C:\code\falco-distress-bots\data\seeds\official_tax_sales.csv)
- [sheriff_sales.csv](C:\code\falco-distress-bots\data\seeds\sheriff_sales.csv)
- [bankruptcy_overlay.csv](C:\code\falco-distress-bots\data\seeds\bankruptcy_overlay.csv)
- [probate_overlay.csv](C:\code\falco-distress-bots\data\seeds\probate_overlay.csv)

Behavior:
- If these files exist, the matching phase runs.
- If they contain only headers, the phase runs and ingests zero rows safely.
- Env vars still override these defaults if you want to point at a different file.

Override env vars:
- `FALCO_TAX_SALE_SEED_FILE`
- `FALCO_SHERIFF_SALE_SEED_FILE`
- `FALCO_BANKRUPTCY_SEED_FILE`
- `FALCO_PROBATE_SEED_FILE`
