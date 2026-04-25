# falco-distress-bots Project Context

Python pipeline for distressed property intelligence. Pulls from public records, enriches, scores, packages, and syncs to falco-site Supabase.

## Tech stack

- **Language:** Python 3.x
- **DB:** SQLite (local pipeline state) + Supabase (sync target for live leads)
- **External APIs:** ATTOM Data (expired 4/23/2026), BatchData (active), Propstream (intermittent)
- **Scheduling:** Local cron / GitHub Actions (now disabled for site_sync — runs locally only)

## Repo layout

```
src/
  bots/             — scrapers per data source (foreclosure, tax delinquent, public notices)
  enrichment/       — ATTOM, BatchData, Propstream enrichment + comp generation
  scoring/          — scorer.py, label.py, days-to-sale, risk flags, triage
  gating/           — convertibility gates (filter unactionable leads)
  grading/          — lead grading post-scoring
  intelligence/     — higher-order analysis layer
  packaging/        — packet builder, PDF generator, Drive uploader
  routing/          — auction firm routing logic
  automation/       — pipeline orchestration, run summaries
  storage/          — SQLite store, lead state persistence
  sync/             — site sync (Supabase upsert)
  db/               — schema + migrations
  core/             — shared utilities, models
  config.py         — env vars, paths
  run_all.py        — main pipeline entrypoint
  notion_client.py  — Notion sync (legacy)
sync_to_vault.py    — local sync script (vault deprecated, but file may still drive Supabase sync)
data/falco.db       — local SQLite, NOT committed
```

## Pipeline flow

1. **Bots** (`src/bots/`) scrape public foreclosure / tax delinquent records → land in SQLite as raw leads
2. **Enrichment** (`src/enrichment/`) hits ATTOM/BatchData/Propstream for AVM, owner contact, distress detail
3. **Scoring** (`src/scoring/`) computes FALCO score, risk flags, days-to-sale
4. **Gating** (`src/gating/`) filters out unactionable leads (institutional, no equity, etc.)
5. **Grading** (`src/grading/`) assigns final grade
6. **Sync** (`src/sync/`) upserts to falco-site Supabase `homeowner_requests` table with `source='bot'`

## Commands

```bash
# Run full pipeline
python -m src.run_all

# Sync to Supabase (local only, NOT in CI)
python sync_to_vault.py

# Test imports
python -c "from src import run_all; print('imports OK')"
```

## Worktrees in use

- `falco-distress-bots/` — main branch, active development
- `falco-distress-bots-mainmerge/` — separate branch (codex/pdf-skimming) for PDF rendering experiments
- `falco-distress-bots-batchprep/` — separate branch (codex/batchdata-prep) for BatchData integration work

## Critical state (as of 2026-04-24)

- **ATTOM Data API expired 4/23/2026.** Pipeline currently fails enrichment for ATTOM-dependent leads. BatchData is the fallback.
- **Site sync removed from CI** (commit a54ce5c) because GitHub Actions can't persist SQLite state across runs. Sync is local-only.
- **143 bot leads** currently in falco-site Supabase (52 NDJSON-derived + 91 from dialer inventory snapshot). 83 have AVM data, 60 need re-enrichment via BatchData.

## Active backlog

Per the audit plan at `~/.claude/plans/enumerated-sauteeing-dawn.md`:

1. DTS null scoring fix — `src/scoring/scorer.py` line 278 (score 0 not 8 for null DTS)
2. Vault agreement audit bug (CROSS-REPO — touches falco-site/src/lib/vault-agreements.ts)
3. contact_ready stale persistence on enrichment error
4. Skip-trace TTL — re-trace after 30 days
5. Distress type upgrade on upsert (lis pendens → foreclosure escalation)
6. Atomic vault sync writes
7. Bot health monitoring — detect dead sources
8. Skip institutional gate for upstream distress (LIS_PENDENS, SOT, NOD)
9. Provenance query tiebreaker in packager
10. Trustee phone word-boundary matching
11. Run summary sampling limit (25 → 200)

## Conventions

- **Idempotent operations.** Bots must be re-runnable without producing duplicates.
- **Provenance tracking.** Every enriched field has a `lead_field_provenance` record (source, confidence, timestamp).
- **Atomic file writes.** Use temp file + os.replace for files that other processes read.
- **Defensive enrichment.** External APIs fail; always have fallback or graceful degradation.
- **No emojis.** No new docs unless explicitly requested.

## Known issues

- ATTOM expired — needs fallback strategy (BatchData primary, Zillow Zestimate secondary)
- ON CONFLICT issues with partial unique indexes (must include WHERE clause matching index predicate)
- jsonb_array_elements collapses with LIMIT — use temp table workaround when expanding JSON arrays
