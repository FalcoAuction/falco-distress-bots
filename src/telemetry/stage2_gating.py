import os, json, sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

DB_PATH_DEFAULT = os.path.join("data", "falco.db")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def db_path() -> str:
    return os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)

def run_id() -> str:
    return os.environ.get("FALCO_RUN_ID", "unknown")

def write_gating_event(lead_key: str, gating_result: str, skip_reason: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    conn = sqlite3.connect(db_path())
    try:
        conn.execute(
            """
            INSERT INTO stage2_gating_events (run_id, lead_key, gating_result, skip_reason, evaluated_at, meta_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (run_id(), lead_key, gating_result, skip_reason, utc_now_iso(), json.dumps(meta or {}, ensure_ascii=False)),
        )
        conn.commit()
    finally:
        conn.close()
