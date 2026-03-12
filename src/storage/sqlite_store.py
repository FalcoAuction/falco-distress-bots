# src/storage/sqlite_store.py
#
# Lightweight "write-a-copy" persistence layer.
# All public functions return True on success, False on failure — never raise.
# The Notion + PDF flow is not affected.
#
# Config:
#   FALCO_SQLITE_PATH  — path to the SQLite file (default: data/falco.db)

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Optional, Tuple


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_INITIALIZED: bool = False


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


@contextmanager
def _connect():
    path = _db_path()
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    con = sqlite3.connect(path)
    try:
        yield con
        con.commit()
    except Exception:
        con.rollback()
        raise
    finally:
        con.close()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ensure_init() -> None:
    global _INITIALIZED
    if not _INITIALIZED:
        init_db()


def _table_columns(con: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = con.execute(f"PRAGMA table_info({table})").fetchall()
        cols = {r[1] for r in rows if len(r) >= 2}
        return cols
    except Exception:
        return set()


def _ensure_column(con: sqlite3.Connection, table: str, col: str, ddl_type: str) -> None:
    """
    Best-effort "add column if missing" for existing DBs.
    Safe to call repeatedly.
    """
    cols = _table_columns(con, table)
    if col in cols:
        return
    # SQLite supports: ALTER TABLE ... ADD COLUMN ...
    con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl_type}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def init_db() -> None:
    """
    Create tables and indexes if they do not exist.
    Also performs best-effort schema alignment on existing DBs by adding missing columns.
    Safe to call repeatedly.
    """
    global _INITIALIZED
    with _connect() as con:
        # 1) Base tables (latest expected schema)
        con.executescript("""
            CREATE TABLE IF NOT EXISTS leads (
                lead_key            TEXT PRIMARY KEY,
                address             TEXT,
                county              TEXT,
                state               TEXT,
                first_seen_at       TEXT NOT NULL,
                last_seen_at        TEXT NOT NULL,
                -- fields used by scoring/packaging (may be populated by other stages)
                dts_days            INTEGER,
                current_sale_date   TEXT,
                original_sale_date  TEXT,
                sale_status         TEXT,
                sale_date_updated_at TEXT,
                falco_score_internal REAL,
                score_updated_at    TEXT,
                auction_readiness   TEXT,
                equity_band         TEXT
            );

            CREATE TABLE IF NOT EXISTS ingest_events (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_key    TEXT NOT NULL,
                source      TEXT,
                source_url  TEXT,
                sale_date   TEXT,
                raw_json    TEXT,
                ingested_at TEXT NOT NULL,
                run_id      TEXT
            );

            CREATE TABLE IF NOT EXISTS attom_enrichments (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_key       TEXT NOT NULL,
                status         TEXT,
                attom_raw_json TEXT,
                avm_value      REAL,
                avm_low        REAL,
                avm_high       REAL,
                confidence     REAL,
                enriched_at    TEXT NOT NULL
            );

            -- Packager registry (expected by src/packaging/packager.py)
            CREATE TABLE IF NOT EXISTS packets (
                run_id      TEXT NOT NULL,
                lead_key    TEXT NOT NULL,
                pdf_path    TEXT,
                sha256      TEXT,
                bytes       INTEGER,
                created_at  TEXT NOT NULL,
                PRIMARY KEY (run_id, lead_key)
            );

            CREATE INDEX IF NOT EXISTS idx_ingest_lead_key
                ON ingest_events (lead_key);

            CREATE INDEX IF NOT EXISTS idx_ingest_sale_date
                ON ingest_events (sale_date);

            CREATE INDEX IF NOT EXISTS idx_attom_lead_key
                ON attom_enrichments (lead_key);

            CREATE INDEX IF NOT EXISTS idx_packets_lead_key
                ON packets (lead_key);

            -- Provenance tables (20260303_01_add_field_provenance)
            CREATE TABLE IF NOT EXISTS raw_artifacts (
                artifact_id    TEXT PRIMARY KEY,
                lead_key       TEXT NOT NULL,
                channel        TEXT NOT NULL,
                source_url     TEXT,
                retrieved_at   TEXT NOT NULL,
                content_type   TEXT NOT NULL,
                content_sha256 TEXT,
                storage_mode   TEXT NOT NULL,
                payload        TEXT,
                file_path      TEXT,
                notes          TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_raw_artifacts_lead
                ON raw_artifacts (lead_key);
            CREATE INDEX IF NOT EXISTS idx_raw_artifacts_retrieved
                ON raw_artifacts (retrieved_at DESC);
            CREATE INDEX IF NOT EXISTS idx_raw_artifacts_sha
                ON raw_artifacts (content_sha256);

            CREATE TABLE IF NOT EXISTS compute_runs (
                run_id                TEXT PRIMARY KEY,
                run_type              TEXT NOT NULL,
                scoring_model_version TEXT,
                git_commit            TEXT,
                created_at            TEXT NOT NULL,
                notes                 TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_compute_runs_type
                ON compute_runs (run_type);

            CREATE TABLE IF NOT EXISTS lead_field_provenance (
                prov_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_key         TEXT NOT NULL,
                field_name       TEXT NOT NULL,
                value_type       TEXT NOT NULL,
                field_value_text TEXT,
                field_value_num  REAL,
                field_value_json TEXT,
                units            TEXT,
                confidence       REAL,
                source_channel   TEXT,
                source_url       TEXT,
                artifact_id      TEXT,
                retrieved_at     TEXT,
                formula          TEXT,
                inputs_json      TEXT,
                run_id           TEXT,
                created_at       TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_prov_lead_field
                ON lead_field_provenance (lead_key, field_name);
            CREATE INDEX IF NOT EXISTS idx_prov_created
                ON lead_field_provenance (created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_prov_run
                ON lead_field_provenance (run_id);
        """)

        # 2) Self-heal older DBs that were created before these columns existed
        # ingest_events: run_id
        try:
            _ensure_column(con, "ingest_events", "run_id", "TEXT")
        except Exception:
            pass

        # leads: columns required by packager / candidate loader
        for col, typ in (
            ("dts_days", "INTEGER"),
            ("current_sale_date", "TEXT"),
            ("original_sale_date", "TEXT"),
            ("sale_status", "TEXT"),
            ("sale_date_updated_at", "TEXT"),
            ("falco_score_internal", "REAL"),
            ("score_updated_at", "TEXT"),
            ("auction_readiness", "TEXT"),
            ("equity_band", "TEXT"),
            ("distress_type", "TEXT"),
            ("uw_ready", "INTEGER"),
            ("uw_json", "TEXT"),
        ):
            try:
                _ensure_column(con, "leads", col, typ)
            except Exception:
                pass

    _INITIALIZED = True


def upsert_lead(
    lead_key: str,
    normalized_address_fields: dict[str, Any],
    county: str,
    distress_type: Optional[str] = None,
) -> bool:
    """
    Insert or update the leads table.
    normalized_address_fields must contain at least 'address'; optionally 'state'.
    Returns True on success, False on any error.
    """
    try:
        _ensure_init()
        # HARD GEO GATE + NORMALIZATION (single choke point)
        from ..settings import normalize_county_full, is_allowed_county, within_target_counties

        county_norm = normalize_county_full(county)
        if county_norm:
            if (not is_allowed_county(county_norm)) or (not within_target_counties(county_norm)):
                return False
        else:
            # unknown county -> reject
            return False

        address = (
            normalized_address_fields.get("address")
            or normalized_address_fields.get("title")
            or ""
        )
        state = normalized_address_fields.get("state") or "TN"
        now = _now()
        with _connect() as con:
            con.execute(
                """
                INSERT INTO leads (lead_key, address, county, state, first_seen_at, last_seen_at, distress_type)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(lead_key) DO UPDATE SET
                    address       = excluded.address,
                    county        = excluded.county,
                    state         = excluded.state,
                    distress_type = COALESCE(excluded.distress_type, leads.distress_type),
                    last_seen_at  = excluded.last_seen_at
                """,
                (lead_key, address, county_norm or "", state, now, now, distress_type),
            )
        return True
    except Exception:
        return False


def insert_ingest_event(
    lead_key: str,
    source: str,
    source_url: Optional[str],
    sale_date: Optional[str],
    raw_json: Optional[str],
) -> bool:
    """
    Append a scrape observation to ingest_events.
    Returns True on success, False on any error.
    """
    try:
        _ensure_init()
        with _connect() as con:
            con.execute(
                """
                INSERT INTO ingest_events
                    (lead_key, source, source_url, sale_date, raw_json, ingested_at, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead_key,
                    source or "",
                    source_url,
                    sale_date,
                    raw_json,
                    _now(),
                    os.getenv("FALCO_RUN_ID"),
                ),
            )
        return True
    except Exception:
        return False


def insert_attom_enrichment(
    lead_key: str,
    status: str,
    attom_raw_json: Optional[str],
    avm_value: Optional[float],
    avm_low: Optional[float],
    avm_high: Optional[float],
    confidence: Optional[float],
) -> bool:
    """
    Store an ATTOM enrichment result (success, no_result, or error).
    Returns True on success, False on any error.
    """
    try:
        import hashlib as _hashlib
        _ensure_init()
        enriched_at = _now()
        _run_id = os.getenv("FALCO_RUN_ID")
        with _connect() as con:
            con.execute(
                """
                INSERT INTO attom_enrichments
                    (lead_key, status, attom_raw_json, avm_value, avm_low, avm_high,
                     confidence, enriched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead_key,
                    status or "",
                    attom_raw_json,
                    avm_value,
                    avm_low,
                    avm_high,
                    confidence,
                    enriched_at,
                ),
            )
            if attom_raw_json is not None:
                try:
                    _sha = _hashlib.sha256(
                        attom_raw_json.encode("utf-8", errors="replace")
                    ).hexdigest()
                    con.execute(
                        """
                        INSERT OR IGNORE INTO raw_artifacts
                            (artifact_id, lead_key, channel, source_url, retrieved_at,
                             content_type, content_sha256, storage_mode, payload, file_path, notes)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (_sha, lead_key, "ATTOM", None, enriched_at,
                         "application/json", _sha, "db", attom_raw_json, None, status or ""),
                    )
                    for _fname, _fval in (
                        ("avm_point_usd", avm_value),
                        ("avm_low_usd",   avm_low),
                        ("avm_high_usd",  avm_high),
                    ):
                        if _fval is None:
                            continue
                        con.execute(
                            """
                            INSERT INTO lead_field_provenance
                                (lead_key, field_name, value_type,
                                 field_value_num, units, confidence,
                                 source_channel, artifact_id,
                                 retrieved_at, run_id, created_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (lead_key, _fname, "raw",
                             float(_fval), "USD", confidence,
                             "ATTOM", _sha,
                             enriched_at, _run_id, enriched_at),
                        )
                except Exception:
                    pass  # provenance failure never aborts the enrichment write
        return True
    except Exception:
        return False


def insert_raw_artifact(
    lead_key: str,
    channel: str,
    source_url: Optional[str],
    retrieved_at: Optional[str],
    content_type: str,
    payload_bytes: Optional[bytes] = None,
    payload_text: Optional[str] = None,
    file_path: Optional[str] = None,
    notes: Optional[str] = None,
) -> Tuple[bool, Optional[str]]:
    """
    Insert OR IGNORE a row into raw_artifacts.
    Caller is responsible for writing any file; pass file_path if storage_mode should be "file".
    sha256 is computed from payload_bytes if provided, else from payload_text utf-8.
    Returns (True, artifact_id) on success, (False, None) on any error. Never raises.
    """
    try:
        import hashlib as _hashlib
        _ensure_init()
        _at = retrieved_at or _now()

        if payload_bytes is not None:
            _sha = _hashlib.sha256(payload_bytes).hexdigest()
        elif payload_text is not None:
            _sha = _hashlib.sha256(
                payload_text.encode("utf-8", errors="replace")
            ).hexdigest()
        else:
            return (False, None)

        _mode = "file" if file_path else "db"
        _payload_db = payload_text if _mode == "db" else None
        _fp = file_path if _mode == "file" else None

        with _connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO raw_artifacts
                    (artifact_id, lead_key, channel, source_url, retrieved_at,
                     content_type, content_sha256, storage_mode, payload, file_path, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (_sha, lead_key, channel, source_url, _at,
                 content_type, _sha, _mode, _payload_db, _fp, notes or ""),
            )
        return (True, _sha)
    except Exception:
        return (False, None)


def insert_provenance_num(
    lead_key: str,
    field_name: str,
    value_num: float,
    units: Optional[str],
    confidence: Optional[float],
    source_channel: str,
    artifact_id: Optional[str],
    retrieved_at: str,
) -> bool:
    """
    Insert a single numeric provenance row into lead_field_provenance. Never raises.
    """
    try:
        _ensure_init()
        _run_id = os.getenv("FALCO_RUN_ID")
        with _connect() as con:
            con.execute(
                """
                INSERT INTO lead_field_provenance
                    (lead_key, field_name, value_type,
                     field_value_num, units, confidence,
                     source_channel, artifact_id,
                     retrieved_at, run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (lead_key, field_name, "raw",
                 float(value_num), units, confidence,
                 source_channel, artifact_id,
                 retrieved_at, _run_id, retrieved_at),
            )
        return True
    except Exception:
        return False


def insert_provenance_text(
    lead_key: str,
    field_name: str,
    value_text: str,
    source_channel: str,
    retrieved_at: Optional[str] = None,
    artifact_id: Optional[str] = None,
    confidence: Optional[float] = None,
) -> bool:
    """
    Insert a single text provenance row into lead_field_provenance. Never raises.
    """
    try:
        _ensure_init()
        _run_id = os.getenv("FALCO_RUN_ID")
        _at = retrieved_at or _now()
        with _connect() as con:
            con.execute(
                """
                INSERT INTO lead_field_provenance
                    (lead_key, field_name, value_type,
                     field_value_text, confidence,
                     source_channel, artifact_id,
                     retrieved_at, run_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lead_key,
                    field_name,
                    "raw",
                    value_text,
                    confidence,
                    source_channel,
                    artifact_id,
                    _at,
                    _run_id,
                    _at,
                ),
            )
        return True
    except Exception:
        return False
