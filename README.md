# FALCO Distress Bots

Distress asset intelligence and auction deal origination engine for Tennessee foreclosure/distress notices.

---

## Windows Setup

### 1. Prerequisites

- Python 3.11+ (verify: `py -3 --version`)
- Git

### 2. Clone the repository

```powershell
git clone <repo-url>
cd falco-distress-bots
```

### 3. Create the virtual environment

```powershell
py -3 -m venv .venv
```

### 4. Activate the virtual environment

```powershell
.venv\Scripts\Activate.ps1
```

> If you get an execution policy error, run this first:
> `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

### 5. Install dependencies

```powershell
py -3 -m pip install -r requirements.txt
```

---

## Environment Variables

### Option A: Set for the current session (PowerShell)

```powershell
$env:NOTION_API_KEY       = "secret_..."
$env:NOTION_DATABASE_ID   = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
$env:FALCO_ATTOM_API_KEY  = "your_attom_key_here"   # optional — skipped if not set
```

### Option B: Use a .env file (manual load)

Copy `.env.example` to `.env` and fill in your values:

```powershell
Copy-Item .env.example .env
notepad .env
```

Then load it for your session:

```powershell
Get-Content .env | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]+)=(.*)$') {
        [System.Environment]::SetEnvironmentVariable($matches[1].Trim(), $matches[2].Trim(), 'Process')
    }
}
```

### Required Variables

| Variable | Description |
|---|---|
| `NOTION_API_KEY` | Notion integration secret (starts with `secret_`) |
| `NOTION_DATABASE_ID` | ID of the target Notion database |

### Optional Variables

| Variable | Default | Description |
|---|---|---|
| `FALCO_NOTION_WRITE` | `0` | Set to `1` to enable Notion create/update (default: writes disabled) |
| `FALCO_ATTOM_API_KEY` | _(none)_ | ATTOM API key — enrichment skipped if not set |
| `FALCO_ALLOWED_COUNTIES` | `Davidson,Williamson,Rutherford,Wilson,Sumner` | Comma-separated counties to target |
| `FALCO_DTS_MIN` | `21` | Minimum days to sale |
| `FALCO_DTS_MAX` | `90` | Maximum days to sale |
| `FALCO_MAX_ENRICH_PER_RUN` | `10` | Max records enriched per run |
| `FALCO_ENRICH_DEBUG` | `0` | Set to `1` for verbose enrichment logs |
| `FALCO_MAX_PACKETS_PER_RUN` | `10` | Max PDF packets generated per run |

---

## Running the Engine

### Safe mode (default — no Notion writes)

Scrapes and processes leads but does **not** write anything to Notion.
Use this for testing, dry runs, or any time you want to verify output before committing.

```powershell
py -3 -m src.run_all
```

### Live mode (writes enabled)

Set `FALCO_NOTION_WRITE=1` to allow create/update calls to reach Notion.

```powershell
$env:FALCO_NOTION_WRITE = "1"
py -3 -m src.run_all
```

This runs all four stages in order:

| Stage | What it does |
|---|---|
| **Stage 1** | Scrapes foreclosure/distress notices (4 bots) |
| **Stage 2** | ATTOM enrichment + comps (skipped if no API key) |
| **Stage 3** | Auction fit grading + PDF packaging |

Running twice is safe — deduplication via Lead Key prevents duplicate Notion entries.

---

## Project Structure

```
src/
  run_all.py              # Orchestrator — run this
  config.py               # Static config (counties, DTS windows, keywords)
  settings.py             # Env-driven runtime settings
  notion_client.py        # Notion API wrapper
  scoring.py              # Falco score computation
  utils.py                # Shared utilities
  bots/
    foreclosure_tennessee_bot.py
    tn_foreclosure_notices_bot.py
    public_notices_bot.py
    tax_pages_bot.py
  enrichment/
    attom_enricher.py     # ATTOM property data enrichment
    attom_client.py       # ATTOM HTTP client
    comps.py              # Comparable sales engine
    propstream_enricher.py
  grading/
    grade.py              # Auction fit grading
  packaging/
    packager.py           # PDF packet orchestration
    pdf_builder.py        # PDF generation (reportlab)
    drive_uploader.py     # Google Drive upload (optional)
```

---

## Notes

- **No .env auto-loading**: Set env vars manually per session (see above) or use a tool like `python-dotenv`.
- **ATTOM is feature-flagged**: If `FALCO_ATTOM_API_KEY` is not set, enrichment is skipped silently — the system still runs fully through Stage 1.
- **Notion creds missing**: If Notion creds are absent, writes are skipped with a warning — scraping still runs.
