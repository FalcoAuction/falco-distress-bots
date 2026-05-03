-- 0042_lead_field_provenance.sql
--
-- Adds the lead_field_provenance table per priority list architectural
-- item. Tracks per-field source/timestamp/confidence for every enriched
-- field on a lead, so the dialer can show Chris which source filled
-- which value and operators can compare paid-vs-free source quality
-- over time.
--
-- Key design choices:
--   * lead_id references homeowner_requests.id directly (live table only;
--     staging records get rewritten when promoted, so we don't track
--     provenance until promotion).
--   * (lead_id, field_name, source) is the natural key — same field
--     can have multiple provenances if multiple bots wrote it. The
--     LATEST provenance wins for the dialer's "primary" view via
--     ORDER BY fetched_at DESC LIMIT 1.
--   * confidence is 0.0..1.0 inclusive; bot writers populate this based
--     on their own match-quality heuristics (strict-1-match enrichers
--     write 1.0, multi-source agreement raises it, ambiguous matches
--     don't write at all).
--   * `value` stored as text — enrichers serialize whatever they wrote
--     (numeric → str(price), JSON → json.dumps, etc) for audit trail.

CREATE TABLE IF NOT EXISTS lead_field_provenance (
    id BIGSERIAL PRIMARY KEY,
    lead_id UUID NOT NULL REFERENCES homeowner_requests(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    value TEXT,
    source TEXT NOT NULL,                  -- e.g., 'davidson_assessor',
                                            --        'tpad_enricher',
                                            --        'nashville_ledger',
                                            --        'batchdata' (legacy)
    confidence DOUBLE PRECISION DEFAULT 1.0
        CHECK (confidence >= 0.0 AND confidence <= 1.0),
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB                          -- bot-specific context (parcel,
                                            -- account_id, search query, etc)
);

CREATE INDEX IF NOT EXISTS lead_field_provenance_lead_idx
    ON lead_field_provenance(lead_id);
CREATE INDEX IF NOT EXISTS lead_field_provenance_field_idx
    ON lead_field_provenance(lead_id, field_name);
CREATE INDEX IF NOT EXISTS lead_field_provenance_source_idx
    ON lead_field_provenance(source);
CREATE INDEX IF NOT EXISTS lead_field_provenance_fetched_at_idx
    ON lead_field_provenance(fetched_at DESC);

-- Convenience view: latest provenance per (lead, field) — used by dialer
-- to display the primary source/confidence for each enriched field.
CREATE OR REPLACE VIEW lead_field_provenance_latest AS
SELECT DISTINCT ON (lead_id, field_name)
    lead_id, field_name, value, source, confidence, fetched_at, metadata
FROM lead_field_provenance
ORDER BY lead_id, field_name, fetched_at DESC;

COMMENT ON TABLE lead_field_provenance IS
    'Per-field source/timestamp/confidence audit trail for every enriched homeowner_requests field. Multi-source agreement increases confidence; ambiguous-match enrichers (strict-1-match policy) do not write provenance for skipped leads.';

COMMENT ON COLUMN lead_field_provenance.confidence IS
    '0.0..1.0. Strict-match enrichers write 1.0. Heuristic enrichers (excerpt-based parse, owner-classifier) write 0.5..0.9. Unverified/legacy sources write 0.3..0.5.';
