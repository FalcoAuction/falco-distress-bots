CREATE TABLE IF NOT EXISTS run_metadata (
    run_id      TEXT PRIMARY KEY,
    created_at  TEXT NOT NULL,
    config_json TEXT NOT NULL
);
