import os
import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

DB_PATH_DEFAULT = os.path.join("data", "falco.db")

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def get_db_path() -> str:
    return os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)

def open_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn

def start_run(run_id: str) -> None:
    conn = open_conn()
    try:
        conn.execute(
            """
            INSERT INTO run_events (run_id, started_at, status, summary_json)
            VALUES (?, ?, 'started', '{}')
            """,
            (run_id, utc_now_iso()),
        )
        conn.commit()
    finally:
        conn.close()

def finish_run_success(run_id: str, summary: Dict[str, Any]) -> None:
    conn = open_conn()
    try:
        conn.execute(
            """
            UPDATE run_events
            SET finished_at = ?, status = 'success', summary_json = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), json.dumps(summary, ensure_ascii=False), run_id),
        )
        conn.commit()
    finally:
        conn.close()

def finish_run_failed(run_id: str, error_text: str, summary: Optional[Dict[str, Any]] = None) -> None:
    conn = open_conn()
    try:
        conn.execute(
            """
            UPDATE run_events
            SET finished_at = ?, status = 'failed', error_text = ?, summary_json = ?
            WHERE run_id = ?
            """,
            (utc_now_iso(), error_text, json.dumps(summary or {}, ensure_ascii=False), run_id),
        )
        conn.commit()
    finally:
        conn.close()
