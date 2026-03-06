-- Run-level telemetry: one row per run_all invocation (or per stage run)
CREATE TABLE IF NOT EXISTS run_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL UNIQUE,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL, -- started|success|failed
  summary_json TEXT NOT NULL DEFAULT '{}',
  error_text TEXT
);

CREATE INDEX IF NOT EXISTS idx_run_events_started_at ON run_events(started_at);

-- Stage2 gating telemetry: one row per lead evaluated for ATTOM enrichment eligibility
CREATE TABLE IF NOT EXISTS stage2_gating_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id TEXT NOT NULL,
  lead_key TEXT NOT NULL,
  gating_result TEXT NOT NULL, -- eligible|skipped
  skip_reason TEXT,            -- e.g. NO_SALE_DATE|TAX_SOURCE|OUT_OF_GEO|MISSING_ADDRESS|TERMINAL_STATUS|COOLDOWN
  evaluated_at TEXT NOT NULL,
  meta_json TEXT NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_s2_gate_run_id ON stage2_gating_events(run_id);
CREATE INDEX IF NOT EXISTS idx_s2_gate_lead_key ON stage2_gating_events(lead_key);
CREATE INDEX IF NOT EXISTS idx_s2_gate_reason ON stage2_gating_events(skip_reason);
