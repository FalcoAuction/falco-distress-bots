from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import URLError, HTTPError

from ..automation.prefc_policy import prefc_county_is_active
from ..storage import sqlite_store as _store

_SOURCE_CHANNEL = "COUNTY_RECORD_LOOKUP"

_COUNTY_LOOKUP_CONFIG: dict[str, dict[str, str]] = {
    "DAVIDSON COUNTY": {
        "provider": "Davidson Register Of Deeds",
        "official_url": "https://www.nashville.gov/departments/register-deeds",
        "search_url": "https://www.davidsonportal.com/",
        "notes": "Search by instrument number first, then book/page if needed.",
    },
    "HAMILTON COUNTY": {
        "provider": "Hamilton Register Of Deeds",
        "official_url": "https://register.hamiltontn.gov/",
        "search_url": "https://register.hamiltontn.gov/OnlineRecordSearch/Home.aspx",
        "notes": "Online record search supports instrument lookup and deed references.",
    },
    "MONTGOMERY COUNTY": {
        "provider": "Montgomery Register Of Deeds",
        "official_url": "https://www.montgomerytn.gov/",
        "search_url": "https://www.ustitlesearch.net/",
        "notes": "Use instrument first when available; county directs public records search through US Title Search.",
    },
    "RUTHERFORD COUNTY": {
        "provider": "Rutherford Register Of Deeds",
        "official_url": "https://rutherfordcountytn.gov/register-of-deeds",
        "search_url": "https://rutherfordcountytn.gov/register-of-deeds",
        "notes": "Start from the register portal and search using instrument or book/page references.",
    },
    "SUMNER COUNTY": {
        "provider": "Sumner Register Of Deeds",
        "official_url": "https://sumnercountytn.gov/departments/register_of_deeds",
        "search_url": "https://www.ustitlesearch.net/",
        "notes": "County records search is typically routed through US Title Search.",
    },
    "WILSON COUNTY": {
        "provider": "Wilson Register Of Deeds",
        "official_url": "https://wilsoncountytn.gov/departments/register_of_deeds/index.php",
        "search_url": "https://wilsoncountytn.gov/departments/register_of_deeds/index.php",
        "notes": "Use register office search guidance and recorded references from the notice.",
    },
}

_USTITLESEARCH_LOGIN_URL = "https://www.ustitlesearch.net/logon.asp"


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _reports_dir() -> Path:
    root = Path(__file__).resolve().parents[2]
    out_dir = root / "out" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path())
    con.row_factory = sqlite3.Row
    return con


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _latest_text(con: sqlite3.Connection, lead_key: str, field_name: str) -> str | None:
    row = con.execute(
        """
        SELECT field_value_text
        FROM lead_field_provenance
        WHERE lead_key=? AND field_name=? AND field_value_text IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return str(row[0]).strip() if row and row[0] is not None and str(row[0]).strip() else None


def _county_config(county: str) -> dict[str, str]:
    normalized = str(county or "").strip().upper()
    return _COUNTY_LOOKUP_CONFIG.get(
        normalized,
        {
            "provider": "County Register Of Deeds",
            "official_url": "",
            "search_url": "",
            "notes": "Use county register or recorder search with instrument or book/page refs.",
        },
    )


def _target_keys() -> set[str] | None:
    raw = str(os.environ.get("FALCO_COUNTY_LOOKUP_TARGET_LEAD_KEYS") or "").strip()
    if not raw:
        return None
    return {part.strip() for part in raw.split(",") if part.strip()}


def _ustsn_username() -> str:
    return str(
        os.environ.get("FALCO_USTITLESEARCH_USERNAME")
        or os.environ.get("USTITLESEARCH_USERNAME")
        or ""
    ).strip()


def _ustsn_password() -> str:
    return str(
        os.environ.get("FALCO_USTITLESEARCH_PASSWORD")
        or os.environ.get("USTITLESEARCH_PASSWORD")
        or ""
    ).strip()


def _login_ustsn() -> tuple[bool, str]:
    username = _ustsn_username()
    password = _ustsn_password()
    if not username or not password:
        return False, "auth_required"

    qs = urllib_parse.urlencode(
        {
            "AAABBBCCC": "123",
            "action": "logon",
            "username": username,
            "password": password,
            "savepassword": "false",
        }
    )
    url = f"{_USTITLESEARCH_LOGIN_URL}?{qs}"
    req = urllib_request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib_request.urlopen(req, timeout=20) as response:
            body = response.read().decode("cp1252", errors="ignore")
    except (HTTPError, URLError):
        return False, "site_unavailable"

    lowered = body.lower()
    if "invalid login" in lowered:
        return False, "auth_failed"
    if "logon to the us title search network" in lowered and "invalid login" not in lowered:
        return False, "auth_uncertain"
    return True, "authenticated"


def _build_lookup_task(con: sqlite3.Connection, lead: sqlite3.Row, login_status: str) -> dict[str, Any] | None:
    lead_key = str(lead["lead_key"] or "").strip()
    county = str(lead["county"] or "").strip()
    if not lead_key or not county:
        return None
    if not prefc_county_is_active(county):
        return None

    blocker_type = str(_latest_text(con, lead_key, "debt_reconstruction_blocker_type") or "").strip().lower()
    if blocker_type != "missing_amount_with_refs":
        return None

    book = _latest_text(con, lead_key, "mortgage_record_book")
    page = _latest_text(con, lead_key, "mortgage_record_page")
    instrument = _latest_text(con, lead_key, "mortgage_record_instrument")
    if not any((book, page, instrument)):
        return None

    config = _county_config(county)
    lender = _latest_text(con, lead_key, "mortgage_lender") or ""
    owner_name = _latest_text(con, lead_key, "owner_name") or ""
    debt_reason = _latest_text(con, lead_key, "debt_reconstruction_missing_reason") or ""
    summary = _latest_text(con, lead_key, "debt_reconstruction_summary") or ""

    ref_parts = []
    if instrument:
        ref_parts.append(f"Instrument {instrument}")
    if book or page:
        ref_parts.append(f"Book {book or '?'} Page {page or '?'}")
    ref_text = " | ".join(ref_parts)

    lookup_hint = "Search instrument first, then book/page"
    if instrument and not (book or page):
        lookup_hint = "Search instrument number directly"
    elif (book or page) and not instrument:
        lookup_hint = "Search book/page directly"

    task = {
        "lead_key": lead_key,
        "county": county,
        "address": str(lead["address"] or "").strip(),
        "provider": config["provider"],
        "official_url": config["official_url"],
        "search_url": config["search_url"],
        "lookup_hint": lookup_hint,
        "record_refs": ref_text,
        "instrument": instrument or "",
        "book": book or "",
        "page": page or "",
        "mortgage_lender": lender,
        "owner_name": owner_name,
        "debt_blocker": debt_reason,
        "debt_summary": summary,
        "status": "queued" if login_status == "authenticated" else login_status,
        "notes": config["notes"],
        "generated_at": _now_iso(),
    }
    return task


def _persist_task(task: dict[str, Any]) -> None:
    lead_key = str(task["lead_key"])
    retrieved_at = str(task["generated_at"])
    artifact_payload = json.dumps(task, ensure_ascii=False).encode("utf-8")
    ok, artifact_id = _store.insert_raw_artifact(
        lead_key=lead_key,
        channel=_SOURCE_CHANNEL,
        source_url=str(task.get("search_url") or task.get("official_url") or ""),
        retrieved_at=retrieved_at,
        content_type="application/json",
        payload_bytes=artifact_payload,
        notes="Automated county-record lookup task generated for recoverable partial debt lead.",
    )
    artifact_ref = artifact_id if ok else None
    _store.insert_provenance_text(lead_key, "county_record_lookup_status", str(task["status"]), _SOURCE_CHANNEL, retrieved_at, artifact_ref, 0.95)
    _store.insert_provenance_text(lead_key, "county_record_lookup_provider", str(task["provider"]), _SOURCE_CHANNEL, retrieved_at, artifact_ref, 0.95)
    _store.insert_provenance_text(lead_key, "county_record_lookup_url", str(task.get("search_url") or task.get("official_url") or ""), _SOURCE_CHANNEL, retrieved_at, artifact_ref, 0.95)
    _store.insert_provenance_text(lead_key, "county_record_lookup_hint", str(task["lookup_hint"]), _SOURCE_CHANNEL, retrieved_at, artifact_ref, 0.95)
    _store.insert_provenance_text(lead_key, "county_record_lookup_refs", str(task["record_refs"]), _SOURCE_CHANNEL, retrieved_at, artifact_ref, 0.95)


def run() -> dict[str, Any]:
    targets = _target_keys()
    queued: list[dict[str, Any]] = []
    login_ok, login_status = _login_ustsn()

    with _connect() as con:
        rows = con.execute(
            """
            SELECT lead_key, address, county, sale_status, distress_type
            FROM leads
            WHERE sale_status='pre_foreclosure'
            ORDER BY COALESCE(score_updated_at, last_seen_at, first_seen_at) DESC
            LIMIT 60
            """
        ).fetchall()

        for lead in rows:
            lead_key = str(lead["lead_key"] or "").strip()
            if targets is not None and lead_key not in targets:
                continue
            task = _build_lookup_task(con, lead, "authenticated" if login_ok else login_status)
            if not task:
                continue
            _persist_task(task)
            queued.append(task)

    report = {
        "generated_at": _now_iso(),
        "login_status": "authenticated" if login_ok else login_status,
        "queued_count": len(queued),
        "tasks": queued,
    }
    report_path = _reports_dir() / "county_record_lookup_queue.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return {
        "ok": True,
        "login_status": "authenticated" if login_ok else login_status,
        "queued": len(queued),
        "path": str(report_path),
    }
