# FALCO Seed Templates

These templates are the operator handoff for the new expansion phases.

Supported formats:
- `.csv`
- `.json`
- `.jsonl`
- `.ndjson`

For the fastest path, use the CSV templates in this folder.

## Phase 1: Official Tax Sales

Template:
- [official_tax_sales_template.csv](C:\code\falco-distress-bots\scripts\seed_templates\official_tax_sales_template.csv)

Env var:
- `FALCO_TAX_SALE_SEED_FILE`

Required fields:
- `address`
- `county`
- `sale_date`

Optional fields:
- `source_url`
- `notes`

## Phase 2: Sheriff Sales

Template:
- [sheriff_sales_template.csv](C:\code\falco-distress-bots\scripts\seed_templates\sheriff_sales_template.csv)

Env var:
- `FALCO_SHERIFF_SALE_SEED_FILE`

Required fields:
- `address`
- `county`
- `sale_date`

Optional fields:
- `source_url`
- `notes`

## Phase 3: Bankruptcy Overlay

Template:
- [bankruptcy_overlay_template.csv](C:\code\falco-distress-bots\scripts\seed_templates\bankruptcy_overlay_template.csv)

Env var:
- `FALCO_BANKRUPTCY_SEED_FILE`

Match priority:
1. `lead_key`
2. exact `address + county`

Recommended fields:
- `lead_key` or `address` + `county`
- `case_number`
- `chapter`
- `filed_at`
- `status`
- `source_url`

## Phase 4: Probate Overlay

Template:
- [probate_overlay_template.csv](C:\code\falco-distress-bots\scripts\seed_templates\probate_overlay_template.csv)

Env var:
- `FALCO_PROBATE_SEED_FILE`

Match priority:
1. `lead_key`
2. exact `address + county`

Recommended fields:
- `lead_key` or `address` + `county`
- `case_number`
- `filed_at`
- `estate_name`
- `contact_name`
- `status`
- `source_url`

## Quick Use

Example PowerShell:

```powershell
$env:FALCO_TAX_SALE_SEED_FILE="C:\code\falco-distress-bots\scripts\seed_templates\official_tax_sales_template.csv"
$env:FALCO_SHERIFF_SALE_SEED_FILE="C:\code\falco-distress-bots\scripts\seed_templates\sheriff_sales_template.csv"
$env:FALCO_BANKRUPTCY_SEED_FILE="C:\code\falco-distress-bots\scripts\seed_templates\bankruptcy_overlay_template.csv"
$env:FALCO_PROBATE_SEED_FILE="C:\code\falco-distress-bots\scripts\seed_templates\probate_overlay_template.csv"
python -m src.run_all
```

Replace the example paths with real operator files before running.
