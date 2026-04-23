"""
src/sync/site_sync.py
=====================

Push bot-discovered TN distress leads from local SQLite (falco.db) up to the
falco.llc Supabase `homeowner_requests` table. Powers the /admin lead
inbox + math-sheet auto-population for the FALCO closer (Chris).

Run this after the main pipeline (src.run_all) finishes. Idempotent —
upserts on `pipeline_lead_key` so a property already synced will only
update changed fields, never duplicate.

USAGE
-----
    python -m src.sync.site_sync                  # sync all eligible leads
    python -m src.sync.site_sync --limit 50       # cap per run
    python -m src.sync.site_sync --dry-run        # show what would sync, no writes
    python -m src.sync.site_sync --since "2026-04-01"   # only leads first seen on/after a date

REQUIRED ENV VARS
-----------------
    NEXT_PUBLIC_SUPABASE_URL    same as the falco-site project
    SUPABASE_SERVICE_ROLE_KEY   same as the falco-site project (service role,
                                NOT anon — needed to bypass RLS for inserts)

OPTIONAL ENV VARS
-----------------
    FALCO_SQLITE_PATH           override default `data/falco.db`
    FALCO_SYNC_MIN_AVM          skip leads with no AVM or AVM < this (default 50000)
    FALCO_SYNC_DRY_RUN          set to "1" to force dry-run regardless of CLI flag

DEPENDENCY
----------
    pip install supabase>=2.0.0    (add to requirements.txt)
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

try:
    from supabase import create_client, Client
except ImportError:
    print(
        "[site_sync] ERROR: supabase-py not installed. Run: pip install supabase>=2.0.0",
        file=sys.stderr,
    )
    sys.exit(2)


# ----------------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------------

DEFAULT_LIMIT = 200
DEFAULT_MIN_AVM = 50_000
SUPABASE_TABLE = "homeowner_requests"


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _supabase() -> Client:
    url = (os.environ.get("NEXT_PUBLIC_SUPABASE_URL") or "").strip()
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not url or not key:
        raise RuntimeError(
            "site_sync needs NEXT_PUBLIC_SUPABASE_URL and "
            "SUPABASE_SERVICE_ROLE_KEY set in the environment."
        )
    return create_client(url, key)


@contextmanager
def _local_db():
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    try:
        yield con
    finally:
        con.close()


# ----------------------------------------------------------------------------
# Reading from local SQLite
# ----------------------------------------------------------------------------


def _fetch_eligible_leads(
    con: sqlite3.Connection,
    *,
    limit: int,
    since: Optional[str],
    min_avm: float,
) -> list[dict]:
    """
    Pull TN distress leads with their latest ATTOM enrichment, filtered to
    "ready to call" candidates: have an address, in TN, AVM above threshold.
    """
    where = ["l.state = 'TN' OR l.state IS NULL", "l.address IS NOT NULL", "l.address <> ''"]
    params: list = []
    if since:
        where.append("l.first_seen_at >= ?")
        params.append(since)

    sql = f"""
        SELECT
            l.lead_key,
            l.address,
            l.county,
            l.state,
            l.current_sale_date,
            l.original_sale_date,
            l.sale_status,
            l.falco_score_internal,
            l.auction_readiness,
            l.equity_band,
            l.first_seen_at,
            l.last_seen_at,
            l.dts_days,
            l.canonical_property_key,
            -- Latest ATTOM enrichment per lead
            (
                SELECT a.avm_value
                FROM attom_enrichments a
                WHERE a.lead_key = l.lead_key
                ORDER BY a.enriched_at DESC
                LIMIT 1
            ) AS avm_value,
            (
                SELECT a.confidence
                FROM attom_enrichments a
                WHERE a.lead_key = l.lead_key
                ORDER BY a.enriched_at DESC
                LIMIT 1
            ) AS avm_confidence,
            (
                SELECT a.attom_raw_json
                FROM attom_enrichments a
                WHERE a.lead_key = l.lead_key
                ORDER BY a.enriched_at DESC
                LIMIT 1
            ) AS attom_raw_json,
            -- Latest ingest event for distress_type + source
            (
                SELECT i.source
                FROM ingest_events i
                WHERE i.lead_key = l.lead_key
                ORDER BY i.ingested_at DESC
                LIMIT 1
            ) AS ingest_source
        FROM leads l
        WHERE {" AND ".join(where)}
        ORDER BY
            -- Most actionable first: trustee sale soonest, then highest score
            COALESCE(l.dts_days, 999999) ASC,
            COALESCE(l.falco_score_internal, 0) DESC,
            l.last_seen_at DESC
        LIMIT ?
    """
    params.append(limit)
    rows = [dict(r) for r in con.execute(sql, params).fetchall()]

    # Filter by min AVM (None counts as 0)
    return [r for r in rows if (r.get("avm_value") or 0) >= min_avm]


# ----------------------------------------------------------------------------
# Mapping local row → Supabase upsert payload
# ----------------------------------------------------------------------------


def _parse_attom_raw(raw_json: Optional[str]) -> dict:
    """Best-effort extraction of beds/baths/sqft/year/last-sale from ATTOM blob."""
    if not raw_json:
        return {}
    try:
        data = json.loads(raw_json)
    except Exception:
        return {}
    out: dict = {}
    # ATTOM property data is typically nested under 'property'[0]
    prop = (
        data.get("property", [{}])[0] if isinstance(data.get("property"), list) else {}
    )
    if not isinstance(prop, dict):
        return out
    bldg = prop.get("building") or {}
    rooms = (bldg.get("rooms") or {}) if isinstance(bldg, dict) else {}
    size = (bldg.get("size") or {}) if isinstance(bldg, dict) else {}
    summary = prop.get("summary") or {}
    saleinfo = prop.get("sale") or {}
    saleamt = (saleinfo.get("amount") or {}) if isinstance(saleinfo, dict) else {}
    out["beds"] = rooms.get("beds")
    out["baths"] = rooms.get("bathstotal")
    out["sqft"] = size.get("livingsize") or size.get("universalsize")
    out["year_built"] = summary.get("yearbuilt")
    last_sale_date = saleinfo.get("salesearchdate") or saleinfo.get("saleTransDate")
    out["last_sale_date"] = last_sale_date if last_sale_date else None
    out["last_sale_price"] = saleamt.get("saleamt") if isinstance(saleamt, dict) else None
    return out


def _normalize_distress_type(ingest_source: Optional[str]) -> Optional[str]:
    if not ingest_source:
        return None
    s = ingest_source.lower()
    if "trustee" in s or "foreclosure" in s:
        return "TRUSTEE_NOTICE"
    if "lis" in s and "pendens" in s:
        return "LIS_PENDENS"
    if "tax" in s and ("delinq" in s or "lien" in s):
        return "TAX_LIEN"
    if "probate" in s:
        return "PROBATE"
    return ingest_source.upper()[:60]


def _to_iso_date(value: Any) -> Optional[str]:
    """Normalize a date-ish value to YYYY-MM-DD or None."""
    if not value:
        return None
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return None
        # ISO datetime → date
        if "T" in v:
            return v.split("T", 1)[0]
        # Already YYYY-MM-DD or similar
        return v[:10]
    return None


def _to_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        n = int(float(value))
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        n = float(value)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _build_payload(row: dict) -> dict:
    attom = _parse_attom_raw(row.get("attom_raw_json"))
    payload = {
        "source": "bot",
        "pipeline_lead_key": row["lead_key"],
        "owner_name_records": None,  # not currently tracked at the lead level
        "property_address": row.get("address") or "",
        "county": row.get("county") or "",
        "trustee_sale_date": _to_iso_date(row.get("current_sale_date")),
        "distress_type": _normalize_distress_type(row.get("ingest_source")),
        "property_value": _to_int(row.get("avm_value")),
        "property_value_source": "ATTOM_AVM" if row.get("avm_value") else None,
        "property_value_as_of": _to_iso_date(row.get("last_seen_at")),
        "beds": _to_int(attom.get("beds")),
        "baths": _to_float(attom.get("baths")),
        "sqft": _to_int(attom.get("sqft")),
        "year_built": _to_int(attom.get("year_built")),
        "last_sale_date": _to_iso_date(attom.get("last_sale_date")),
        "last_sale_price": _to_int(attom.get("last_sale_price")),
        "pipeline_score": _to_int(row.get("falco_score_internal")),
        # workflow fields default at the DB level — don't clobber on resync
        # (status defaults to 'new', notes default to '')
    }
    return {k: v for k, v in payload.items() if v is not None or k in ("source", "pipeline_lead_key")}


# ----------------------------------------------------------------------------
# Upsert to Supabase
# ----------------------------------------------------------------------------


def _upsert_batch(client: Client, payloads: list[dict]) -> tuple[int, int]:
    """Upsert in chunks. Returns (inserted_or_updated, errors)."""
    if not payloads:
        return 0, 0
    ok = 0
    errs = 0
    # supabase-py supports list upsert with on_conflict
    chunk_size = 50
    for i in range(0, len(payloads), chunk_size):
        chunk = payloads[i : i + chunk_size]
        try:
            res = (
                client.table(SUPABASE_TABLE)
                .upsert(chunk, on_conflict="pipeline_lead_key")
                .execute()
            )
            ok += len(res.data or chunk)
        except Exception as e:
            print(f"[site_sync] chunk upsert failed: {e}", file=sys.stderr)
            errs += len(chunk)
    return ok, errs


# ----------------------------------------------------------------------------
# CLI entry
# ----------------------------------------------------------------------------


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Sync TN distress leads → falco-site Supabase.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Max leads to sync this run.")
    parser.add_argument("--since", type=str, default=None, help="Only leads first seen on/after this date (YYYY-MM-DD).")
    parser.add_argument("--min-avm", type=float, default=DEFAULT_MIN_AVM, help="Skip leads with AVM below this dollar amount.")
    parser.add_argument("--dry-run", action="store_true", help="Don't write — just print what would sync.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    dry_run = args.dry_run or os.environ.get("FALCO_SYNC_DRY_RUN", "").strip() == "1"

    started = datetime.now(timezone.utc)
    print(f"[site_sync] starting at {started.isoformat()}")
    print(f"[site_sync] db={_db_path()}  limit={args.limit}  min_avm={args.min_avm}  dry_run={dry_run}")

    # Read local
    try:
        with _local_db() as con:
            rows = _fetch_eligible_leads(
                con,
                limit=args.limit,
                since=args.since,
                min_avm=args.min_avm,
            )
    except Exception as e:
        print(f"[site_sync] FATAL reading local DB: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1

    print(f"[site_sync] fetched {len(rows)} eligible leads from {_db_path()}")
    if not rows:
        print("[site_sync] nothing to sync. Exiting clean.")
        return 0

    payloads = [_build_payload(r) for r in rows]

    # Show a preview
    print(f"[site_sync] sample payload: {json.dumps(payloads[0], indent=2, default=str)[:600]}...")

    if dry_run:
        print(f"[site_sync] DRY RUN — would upsert {len(payloads)} rows. No writes.")
        return 0

    # Upsert
    try:
        client = _supabase()
    except RuntimeError as e:
        print(f"[site_sync] FATAL: {e}", file=sys.stderr)
        return 1

    ok, errs = _upsert_batch(client, payloads)
    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"[site_sync] done in {elapsed:.1f}s — upserted={ok}, errors={errs}")

    return 0 if errs == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
