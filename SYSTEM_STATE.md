# SYSTEM_STATE.md (FALCO)

## Environment
- OS: Windows
- Repo: C:\code\falco-distress-bots
- Shell: PowerShell
- Python: venv at .\.venv
- DB: data/falco.db (FALCO_SQLITE_PATH)

## Operating rules
- Give me ONE step at a time.
- Provide COPY/PASTE PowerShell commands.
- Use Claude for small patches; never giant dumps.
- Avoid multiline python inside PS; write temp .py files and run them.

## Pipeline
Stage 1: Ingest
- src/bots/foreclosure_tennessee_bot.py
- src/bots/tn_foreclosure_notices_bot.py
- src/bots/public_notices_bot.py
- src/bots/tax_pages_bot.py
- src/bots/propstream_bot.py
- src/bots/api_tax_delinquent_bot.py

Stage 2: Enrich
- ATTOM enrichment + raw_artifacts/provenance writes
- Comps engine

Stage 2.5: Score
- src/scoring/scorer.py

Stage 3: Grade + Package
- src/grading/grade.py
- src/packaging/packager.py
- Packager UW gate: FALCO_REQUIRE_UW=1 (default)

Manual Underwriting
- CLI: src/uw/submit_cli.py
- Web UI: src/uw/uw_web.py (http://127.0.0.1:8787/)
- Saves uw_ready + uw_json into DB
- Packager REPACK works end-to-end

## Recent wins (today)
- UW web UI loads by querystring: /?lead_key=<lead_key>
- UW save persists to DB and repack generates PDF
- Provenance tables exist:
  - lead_field_provenance
  - raw_artifacts (channels include ATTOM, NOTICE_HTML, NOTICE_PDF)
- Notice contact extraction is functioning (phones); trustee/address/email label extraction added

## Current objective (War Plan)
- Produce 3–4 ultra-clean AUCTION-VIABLE opportunities from:
  Davidson / Williamson / Rutherford
- Brutal filtering + institutional exclusion
- Upstream lane next: Lis Pendens + Substitution of Trustee ingest
- Revenue: route packets to auction partners; brokerage layer for commission splits

## Next tasks
1) Fix + finalize notice contact extraction (trustee/address/email reliability)
2) Institutional gating improvements (trustee firm detection)
3) Add upstream ingest: Lis Pendens + Substitution of Trustee (target counties)
4) Tighten UW gate + auto “UW candidate queue”
5) Contact enrichment (owner/borrower) workflow + legality
6) Produce 3 outreach-ready Diamond packets with source-stamped provenance

# FALCO SYSTEM STATE
Date: 2026-03-06

Repo:
C:\code\falco-distress-bots

Environment:
Windows + PowerShell
Python venv at .venv
Claude used for code patches
ChatGPT used for systems architecture and step-by-step execution

Pipeline Status:
✔ Distress ingest working
✔ Leads stored in SQLite (data/falco.db)
✔ UW web UI functional (127.0.0.1:8787)
✔ Manual underwriting saved to DB
✔ Packet repack mode working
✔ Street View API integrated
✔ Google Street View caching active
✔ Multi-page Diamond Acquisition Dossier PDF built successfully

Packet Pages:
1 Executive Summary
2 Property Snapshot (Street View image)
3 Valuation Analysis
4 Property Facts
5 Timeline & Risk
6–7 Foreclosure Notice
8 Internal Scoring Appendix
9 Methodology

Key Files:
src/packaging/pdf_builder.py
src/enrichment/streetview.py
src/packaging/packager.py

Important Commands:
python -m src.run_all
repack mode via env vars
UW UI: http://127.0.0.1:8787/

Current Focus:
Improve packet quality and expand enrichment.

# FALCO SYSTEM STATE
Date: 2026-03-06

Repo Root
C:\code\falco-distress-bots

Environment
Windows 11
PowerShell
Python venv (.venv)
SQLite database: data/falco.db

AI Workflow
Claude used for code patches
ChatGPT used for architecture and execution guidance
User executes commands manually in PowerShell

Core System Status
✔ Distress ingest pipeline operational
✔ Leads stored in SQLite
✔ UW web interface working (127.0.0.1:8787)
✔ Manual underwriting saved to DB
✔ Packet repack system working
✔ Street View API integrated
✔ Street View caching functional
✔ Foreclosure notice parsing working
✔ Diamond Acquisition Dossier PDF builder operational

Packet Layout
Page 1 — Executive Summary
Page 2 — Property Snapshot (Street View)
Page 3 — Valuation Analysis
Page 4 — Property Facts
Page 5 — Timeline & Risk
Page 6–7 — Foreclosure Notice
Page 8 — Internal Scoring Appendix
Page 9 — Methodology

Key Files
src/packaging/pdf_builder.py
src/enrichment/streetview.py
src/packaging/packager.py

Commands
python -m src.run_all

Repack Mode
FALCO_FORCE_REPACKAGE=1
FALCO_REPACK_LEAD_KEY=<lead_key>

UW UI
http://127.0.0.1:8787/

Current Goal
Finalize Diamond packet quality and expand enrichment.