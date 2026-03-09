from __future__ import annotations

import csv
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..settings import normalize_county_full


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_seed_path(filename: str) -> str:
    return str(repo_root() / "data" / "seeds" / filename)


def load_seed_rows(path: str) -> list[dict[str, Any]]:
    ext = os.path.splitext(path)[1].lower()
    with open(path, "r", encoding="utf-8-sig") as f:
        if ext in {".jsonl", ".ndjson"}:
            return [json.loads(line) for line in f if line.strip()]
        if ext == ".json":
            payload = json.load(f)
            if isinstance(payload, list):
                return [row for row in payload if isinstance(row, dict)]
            if isinstance(payload, dict):
                rows = payload.get("rows")
                if isinstance(rows, list):
                    return [row for row in rows if isinstance(row, dict)]
            return []
        if ext == ".csv":
            return list(csv.DictReader(f))
    return []


def pick_first(row: dict[str, Any], *keys: str) -> Optional[str]:
    for key in keys:
        value = row.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def normalize_seed_row(row: dict[str, Any]) -> dict[str, Optional[str]]:
    county = normalize_county_full(
        pick_first(row, "county", "County", "county_name", "market_county")
    )
    return {
        "address": pick_first(row, "address", "Address", "property_address", "site_address"),
        "county": county,
        "sale_date": pick_first(row, "sale_date", "sale_date_iso", "saleDate", "auction_date"),
        "source_url": pick_first(row, "source_url", "sourceUrl", "url", "link"),
        "notes": pick_first(row, "notes", "summary", "description"),
        "lead_key": pick_first(row, "lead_key", "leadKey"),
        "case_number": pick_first(row, "case_number", "caseNumber", "docket_number"),
        "status": pick_first(row, "status", "case_status"),
        "chapter": pick_first(row, "chapter", "bankruptcy_chapter"),
        "filed_at": pick_first(row, "filed_at", "filedAt", "filed_date"),
        "estate_name": pick_first(row, "estate_name", "estateName", "decedent_name"),
        "contact_name": pick_first(row, "contact_name", "executor_name", "administrator_name"),
    }


def iter_normalized_rows(rows: Iterable[dict[str, Any]]) -> Iterable[dict[str, Optional[str]]]:
    for row in rows:
        if not isinstance(row, dict):
            continue
        yield normalize_seed_row(row)


def match_lead_key(
    con: sqlite3.Connection,
    explicit_lead_key: Optional[str],
    address: Optional[str],
    county: Optional[str],
) -> Optional[str]:
    cur = con.cursor()
    if explicit_lead_key:
        row = cur.execute(
            "SELECT lead_key FROM leads WHERE lead_key = ? LIMIT 1",
            (explicit_lead_key,),
        ).fetchone()
        if row:
            return str(row[0])

    if address and county:
        row = cur.execute(
            """
            SELECT lead_key
            FROM leads
            WHERE lower(trim(address)) = lower(trim(?))
              AND lower(trim(county)) = lower(trim(?))
            ORDER BY last_seen_at DESC
            LIMIT 1
            """,
            (address, county),
        ).fetchone()
        if row:
            return str(row[0])
    return None
