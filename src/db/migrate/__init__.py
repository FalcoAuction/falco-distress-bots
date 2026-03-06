import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

DB_PATH_DEFAULT = os.path.join("data", "falco.db")
MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_schema_migrations(conn: sqlite3.Connection) -> None:
    conn.execute(
        '''
        CREATE TABLE IF NOT EXISTS schema_migrations (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          filename TEXT NOT NULL UNIQUE,
          applied_at TEXT NOT NULL
        );
        '''
    )
    conn.commit()

def get_applied(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute("SELECT filename FROM schema_migrations").fetchall()
    return {r["filename"] for r in rows}

def apply_migration(conn: sqlite3.Connection, filename: str, sql_text: str) -> None:
    conn.executescript(sql_text)
    conn.execute(
        "INSERT INTO schema_migrations (filename, applied_at) VALUES (?, ?)",
        (filename, utc_now_iso()),
    )
    conn.commit()

def main() -> int:
    db_path = os.environ.get("FALCO_DB_PATH", DB_PATH_DEFAULT)
    if not MIGRATIONS_DIR.exists():
        raise FileNotFoundError(f"Missing migrations dir: {MIGRATIONS_DIR}")

    conn = connect(db_path)
    try:
        ensure_schema_migrations(conn)
        applied = get_applied(conn)

        migration_files = sorted(
            [p for p in MIGRATIONS_DIR.glob("*.sql") if p.is_file()],
            key=lambda p: p.name,
        )

        to_apply = [p for p in migration_files if p.name not in applied]

        print(f"[MIGRATE] db_path={db_path}")
        print(f"[MIGRATE] applied={len(applied)} pending={len(to_apply)}")

        for p in to_apply:
            sql_text = p.read_text(encoding="utf-8")
            print(f"[MIGRATE] applying {p.name} ...")
            apply_migration(conn, p.name, sql_text)

        print("[MIGRATE] done")
        return 0
    finally:
        conn.close()

if __name__ == "__main__":
    raise SystemExit(main())
