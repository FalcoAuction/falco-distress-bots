# tools/uw_set.py
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def main() -> int:
    if len(sys.argv) < 2:
        print("USAGE: python tools/uw_set.py <lead_key> [json_file]")
        print("  json_file optional: defaults to stdin if '-' else None")
        return 2

    lead_key = sys.argv[1].strip()
    if not lead_key:
        print("ERROR: lead_key empty")
        return 2

    json_file = sys.argv[2].strip() if len(sys.argv) >= 3 else None

    # Load UW payload
    if json_file == "-":
        raw = sys.stdin.read()
    elif json_file:
        with open(json_file, "r", encoding="utf-8") as f:
            raw = f.read()
    else:
        print("ERROR: provide json_file path or '-' for stdin")
        return 2

    # PowerShell can write UTF-8 with BOM; strip it if present
    raw = raw.lstrip("\ufeff")
    try:
        uw: Dict[str, Any] = json.loads(raw)
    except Exception as e:
        print(f"ERROR: invalid JSON: {e}")
        return 2

    # Source-stamp (minimal required fields)
    uw.setdefault("_meta", {})
    uw["_meta"].setdefault("updated_at", _now_iso())
    uw["_meta"].setdefault("updated_by", os.getenv("USERNAME") or "unknown")
    uw["_meta"].setdefault("source", "MANUAL_UW")

    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    con = sqlite3.connect(db_path)
    try:
        cur = con.cursor()
        exists = cur.execute("SELECT 1 FROM leads WHERE lead_key=? LIMIT 1", (lead_key,)).fetchone()
        if not exists:
            print("ERROR: lead_key not found")
            return 1

        cur.execute(
            """
            UPDATE leads
            SET uw_ready=1,
                uw_json=?,
                last_seen_at=COALESCE(last_seen_at, ?)
            WHERE lead_key=?
            """,
            (json.dumps(uw, ensure_ascii=False), _now_iso(), lead_key),
        )
        con.commit()

        r = cur.execute(
            "SELECT lead_key, uw_ready, LENGTH(uw_json) FROM leads WHERE lead_key=?",
            (lead_key,),
        ).fetchone()
        print("UPDATED_ROW=", r)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
