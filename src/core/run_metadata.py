# src/core/run_metadata.py
#
# Writes a single config-snapshot row to run_metadata for each run_id.
# Uses INSERT OR IGNORE — safe to call from multiple entry points; only first write wins.

import json
import os
import sqlite3
from datetime import datetime, timezone

_CONFIG_KEYS = (
    "FALCO_STAGE2_SOURCE",
    "FALCO_ATTOM_TTL_MODE",
    "FALCO_ATTOM_TTL_DAYS",
    "FALCO_ATTOM_TTL_BY_STATUS",
    "FALCO_ATTOM_REFRESH_LIMIT",
    "FALCO_MAX_ATTOM_CALLS_PER_RUN",
    "FALCO_SQLITE_PATH",
    "FALCO_NOTION_WRITE",
    "FALCO_DRY_RUN",
    "FALCO_AUTO_PUBLISH_VAULT",
    "FALCO_SITE_REPO",
)


def store_run_metadata(run_id: str) -> None:
    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    d = os.path.dirname(db_path)
    if d:
        os.makedirs(d, exist_ok=True)
    config = {k: os.environ.get(k) for k in _CONFIG_KEYS}
    config_json = json.dumps(config, separators=(",", ":"))
    created_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS run_metadata (
                run_id      TEXT PRIMARY KEY,
                created_at  TEXT NOT NULL,
                config_json TEXT NOT NULL
            )
            """
        )
        con.execute(
            "INSERT OR IGNORE INTO run_metadata (run_id, created_at, config_json) VALUES (?, ?, ?)",
            (run_id, created_at, config_json),
        )
        con.commit()
    finally:
        con.close()
    print(f"[RUN_META] stored run_id={run_id} bytes={len(config_json)}")
