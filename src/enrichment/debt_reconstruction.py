from __future__ import annotations

import json
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ..automation.prefc_policy import prefc_county_is_active, prefc_county_priority, prefc_source_priority
from ..storage import sqlite_store as _store

_SOURCE_CHANNEL = "DEBT_RECONSTRUCTION"
_BOOK_RX = re.compile(r"\bbook\s+([A-Za-z0-9-]+)\b", re.IGNORECASE)
_PAGE_RX = re.compile(r"\bpage\s+([A-Za-z0-9-]+)\b", re.IGNORECASE)
_INSTRUMENT_RX = re.compile(r"\b(?:instrument|document)\s*(?:#|number|no\.?)?\s*([A-Za-z0-9-]+)\b", re.IGNORECASE)
_DATE_RX = re.compile(
    r"\b(?:dated|recorded(?:\s+on)?)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|20\d{2}-\d{2}-\d{2})",
    re.IGNORECASE,
)
_CLAUSE_SPLIT_RX = re.compile(
    r"\b(?:which the aforementioned|dated|recorded|by instrument|book\s+[A-Za-z0-9-]+|page\s+[A-Za-z0-9-]+|instrument\s*#?|document\s*#?)\b",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _coerce_date(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip(" ,.;")
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        return text[:10]
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).date().isoformat()
        except Exception:
            continue
    return None


def _clean_party_name(value: Any) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "").strip(" ,.;:-"))
    if not text:
        return None
    text = _CLAUSE_SPLIT_RX.split(text, maxsplit=1)[0].strip(" ,.;:-")
    text = re.sub(r"\s*\((?:the\s+)?holder\)\s*$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s{2,}", " ", text).strip(" ,.;:-")
    if len(text) < 3:
        return None
    return text[:180]


def _coerce_num(value: Any) -> float | None:
    if value is None:
        return None
    try:
        text = str(value).replace(",", "").replace("$", "").strip()
        if not text:
            return None
        amount = float(text)
        return amount if amount > 0 else None
    except Exception:
        return None


def _first_obj(payload: Dict[str, Any]) -> Dict[str, Any]:
    results = payload.get("results")
    if isinstance(results, dict):
        properties = results.get("properties")
        if isinstance(properties, list) and properties and isinstance(properties[0], dict):
            return properties[0]
        prop = results.get("property")
        if isinstance(prop, dict):
            return prop
    for key in ("data", "properties", "property"):
        value = payload.get(key)
        if isinstance(value, list) and value and isinstance(value[0], dict):
            return value[0]
        if isinstance(value, dict):
            return value
    return payload


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


def _latest_num(con: sqlite3.Connection, lead_key: str, field_name: str) -> float | None:
    row = con.execute(
        """
        SELECT field_value_num
        FROM lead_field_provenance
        WHERE lead_key=? AND field_name=? AND field_value_num IS NOT NULL
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key, field_name),
    ).fetchone()
    return float(row[0]) if row and row[0] is not None else None


def _latest_batchdata_artifact(con: sqlite3.Connection, lead_key: str) -> tuple[dict[str, Any] | None, str | None, str]:
    row = con.execute(
        """
        SELECT artifact_id, payload, retrieved_at
        FROM raw_artifacts
        WHERE lead_key=? AND channel='BATCHDATA' AND payload IS NOT NULL
        ORDER BY retrieved_at DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    if not row or not row[1]:
        return None, None, _now_iso()
    try:
        payload = json.loads(row[1])
    except Exception:
        return None, str(row[0] or ""), str(row[2] or _now_iso())
    return payload, str(row[0] or ""), str(row[2] or _now_iso())


def _parse_notice_chain(raw: str | None) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    parts = [_clean_party_name(part) for part in text.split("->")]
    parts = [part for part in parts if part]
    out: dict[str, Any] = {}
    if parts:
        out["current_holder"] = parts[0]
    if len(parts) > 1:
        out["original_lender"] = parts[-1]
    date_match = _DATE_RX.search(text)
    if date_match:
        out["assignment_recorded_at"] = _coerce_date(date_match.group(1))
    book_match = _BOOK_RX.search(text)
    if book_match:
        out["record_book"] = book_match.group(1)
    page_match = _PAGE_RX.search(text)
    if page_match:
        out["record_page"] = page_match.group(1)
    instrument_match = _INSTRUMENT_RX.search(text)
    if instrument_match:
        out["record_instrument"] = instrument_match.group(1)
    return out


def _extract_batchdata_reconstruction(payload: dict[str, Any]) -> dict[str, Any]:
    item = _first_obj(payload)
    foreclosure = item.get("foreclosure") if isinstance(item.get("foreclosure"), dict) else {}
    history = item.get("mortgageHistory") if isinstance(item.get("mortgageHistory"), list) else []
    history = [candidate for candidate in history if isinstance(candidate, dict)]
    history.sort(
        key=lambda candidate: str(
            candidate.get("recordingDate")
            or candidate.get("documentDate")
            or candidate.get("loanDate")
            or candidate.get("saleDate")
            or ""
        ),
        reverse=True,
    )

    latest = history[0] if history else {}
    earliest = history[-1] if history else {}

    out: dict[str, Any] = {}
    current_lender = _clean_party_name(
        foreclosure.get("currentLenderName")
        or latest.get("lenderName")
        or item.get("mortgageLender")
    )
    original_lender = _clean_party_name(earliest.get("lenderName"))
    mortgage_amount = _coerce_num(
        latest.get("loanAmount")
        or latest.get("amount")
        or foreclosure.get("loanAmount")
        or foreclosure.get("amount")
    )
    mortgage_date = _coerce_date(
        latest.get("recordingDate")
        or latest.get("documentDate")
        or latest.get("loanDate")
        or foreclosure.get("recordingDate")
        or foreclosure.get("filingDate")
    )
    if current_lender:
        out["current_holder"] = current_lender
    if original_lender:
        out["original_lender"] = original_lender
    if mortgage_amount is not None:
        out["mortgage_amount"] = mortgage_amount
    if mortgage_date:
        out["mortgage_date"] = mortgage_date
    if foreclosure.get("recordingDate"):
        out["assignment_recorded_at"] = _coerce_date(foreclosure.get("recordingDate"))
    if foreclosure.get("bookNumber"):
        out["record_book"] = str(foreclosure.get("bookNumber")).strip()
    if foreclosure.get("pageNumber"):
        out["record_page"] = str(foreclosure.get("pageNumber")).strip()
    if foreclosure.get("documentNumber"):
        out["record_instrument"] = str(foreclosure.get("documentNumber")).strip()
    return out


def reconstruct_debt_for_lead(con: sqlite3.Connection, lead_key: str) -> dict[str, Any]:
    current_lender = _clean_party_name(_latest_text(con, lead_key, "mortgage_lender"))
    mortgage_amount = _latest_num(con, lead_key, "mortgage_amount")
    mortgage_date = _coerce_date(_latest_text(con, lead_key, "mortgage_date"))
    last_sale_date = _coerce_date(_latest_text(con, lead_key, "last_sale_date"))
    chain_info = _parse_notice_chain(_latest_text(con, lead_key, "mortgage_chain_notice"))
    payload, artifact_id, retrieved_at = _latest_batchdata_artifact(con, lead_key)
    batchdata_info = _extract_batchdata_reconstruction(payload or {}) if payload else {}

    canonical_lender = (
        batchdata_info.get("current_holder")
        or chain_info.get("current_holder")
        or current_lender
    )
    original_lender = (
        chain_info.get("original_lender")
        or batchdata_info.get("original_lender")
        or current_lender
    )
    canonical_mortgage_date = (
        mortgage_date
        or batchdata_info.get("mortgage_date")
        or chain_info.get("assignment_recorded_at")
    )
    canonical_amount = mortgage_amount if mortgage_amount is not None else batchdata_info.get("mortgage_amount")
    record_book = chain_info.get("record_book") or batchdata_info.get("record_book")
    record_page = chain_info.get("record_page") or batchdata_info.get("record_page")
    record_instrument = chain_info.get("record_instrument") or batchdata_info.get("record_instrument")
    assignment_recorded_at = chain_info.get("assignment_recorded_at") or batchdata_info.get("assignment_recorded_at")

    confidence = "FULL"
    if not canonical_lender and canonical_amount is None:
        confidence = "THIN"
    elif canonical_amount is None:
        confidence = "PARTIAL"
    elif not canonical_lender:
        confidence = "PARTIAL"

    summary_parts = []
    if canonical_lender:
        summary_parts.append(f"Current holder: {canonical_lender}")
    if original_lender and original_lender != canonical_lender:
        summary_parts.append(f"Original lender: {original_lender}")
    if canonical_amount is not None:
        summary_parts.append(f"Loan amount: {int(canonical_amount):,}")
    if canonical_mortgage_date:
        summary_parts.append(f"Recording support: {canonical_mortgage_date}")
    if record_book or record_page or record_instrument:
        ref = " ".join(part for part in (
            f"Book {record_book}" if record_book else "",
            f"Page {record_page}" if record_page else "",
            f"Instrument {record_instrument}" if record_instrument else "",
        ) if part)
        if ref:
            summary_parts.append(ref)

    return {
        "artifact_id": artifact_id,
        "retrieved_at": retrieved_at,
        "mortgage_lender": canonical_lender,
        "mortgage_lender_original": original_lender,
        "mortgage_amount": canonical_amount,
        "mortgage_date": canonical_mortgage_date,
        "last_sale_date": last_sale_date,
        "mortgage_assignment_recorded_at": assignment_recorded_at,
        "mortgage_record_book": record_book,
        "mortgage_record_page": record_page,
        "mortgage_record_instrument": record_instrument,
        "debt_reconstruction_confidence": confidence,
        "debt_reconstruction_summary": " | ".join(summary_parts) if summary_parts else "No durable debt reconstruction found",
    }


def _write_field(lead_key: str, field_name: str, value: Any, artifact_id: str | None, retrieved_at: str) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return _store.insert_provenance_num(
            lead_key=lead_key,
            field_name=field_name,
            value_num=float(value),
            units="USD" if field_name == "mortgage_amount" else None,
            confidence=None,
            source_channel=_SOURCE_CHANNEL,
            artifact_id=artifact_id,
            retrieved_at=retrieved_at,
        )
    return _store.insert_provenance_text(
        lead_key=lead_key,
        field_name=field_name,
        value_text=str(value),
        source_channel=_SOURCE_CHANNEL,
        retrieved_at=retrieved_at,
        artifact_id=artifact_id,
        confidence=None,
    )


def run() -> dict[str, int]:
    con = sqlite3.connect(os.environ.get("FALCO_SQLITE_PATH", "data/falco.db"))
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    limit = max(int(os.environ.get("FALCO_DEBT_RECON_LIMIT", "20")), 0)
    target_keys = {
        item.strip()
        for item in os.environ.get("FALCO_DEBT_RECON_TARGET_LEAD_KEYS", "").split(",")
        if item.strip()
    }

    rows = cur.execute(
        """
        SELECT lead_key, county, distress_type, falco_score_internal
        FROM leads
        WHERE sale_status='pre_foreclosure'
        ORDER BY COALESCE(falco_score_internal, 0) DESC, lead_key ASC
        LIMIT 80
        """
    ).fetchall()
    rows = [row for row in rows if not target_keys or str(row["lead_key"] or "").strip() in target_keys]
    rows.sort(
        key=lambda row: (
            prefc_county_priority(row["county"]),
            prefc_source_priority(row["distress_type"]),
            -float(row["falco_score_internal"] or 0),
            str(row["lead_key"] or ""),
        )
    )

    summary = {
        "requested": 0,
        "reconstructed": 0,
        "missing_amount": 0,
        "errors": 0,
        "active_county_focus": 0,
    }

    try:
        for row in rows[:limit]:
            lead_key = str(row["lead_key"] or "").strip()
            if not lead_key:
                continue
            summary["requested"] += 1
            if prefc_county_is_active(row["county"]):
                summary["active_county_focus"] += 1
            try:
                reconstructed = reconstruct_debt_for_lead(con, lead_key)
                artifact_id = reconstructed.pop("artifact_id", None)
                retrieved_at = reconstructed.pop("retrieved_at", _now_iso())
                wrote_any = False
                for field_name, value in reconstructed.items():
                    if _write_field(lead_key, field_name, value, artifact_id, retrieved_at):
                        wrote_any = True
                if reconstructed.get("mortgage_amount") is None:
                    summary["missing_amount"] += 1
                if wrote_any:
                    summary["reconstructed"] += 1
            except Exception:
                summary["errors"] += 1
        con.commit()
    finally:
        con.close()

    return summary
