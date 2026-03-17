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
_VOL_RX = re.compile(r"\bvol\.?\s+([A-Za-z0-9-]+)\b", re.IGNORECASE)
_IMAGE_RX = re.compile(r"\bimage\s+([A-Za-z0-9-]+)\b", re.IGNORECASE)
_INSTRUMENT_RX = re.compile(r"\b(?:instrument|document)\s*(?:#|number|no\.?)?\s*([A-Za-z0-9-]+)\b", re.IGNORECASE)
_DATE_RX = re.compile(
    r"\b(?:dated|recorded(?:\s+on)?)\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}|20\d{2}-\d{2}-\d{2})",
    re.IGNORECASE,
)
_CURRENCY_RX = re.compile(r"\$\s*([0-9][0-9,]+(?:\.\d{2})?)")
_CLAUSE_SPLIT_RX = re.compile(
    r"\b(?:which the aforementioned|dated|recorded|by instrument|book\s+[A-Za-z0-9-]+|page\s+[A-Za-z0-9-]+|instrument\s*#?|document\s*#?)\b",
    re.IGNORECASE,
)
_DOT_DEED_RX = re.compile(
    r"deed of trust dated\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4}).{0,240}?for the benefit of\s+(.+?)(?:, of record|;| and )",
    re.IGNORECASE | re.DOTALL,
)
_LAST_ASSIGNED_RX = re.compile(
    r"was last assigned to\s+(.+?)(?:\(|, c/o|, its attorney|, the entire indebtedness|;|\n)",
    re.IGNORECASE | re.DOTALL,
)
_PROPERTY_CONVEYED_RX = re.compile(
    r"same property conveyed .*? by deed dated\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})\s+recorded\s+([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{2,4})\s+in\s+(?:book|vol\.?)\s+([A-Za-z0-9-]+),\s*(?:page|image)\s+([A-Za-z0-9-]+)",
    re.IGNORECASE | re.DOTALL,
)
_PRINCIPAL_AMOUNT_RX = re.compile(
    r"(?:original\s+principal\s+amount|principal\s+sum|indebtedness\s+in\s+the\s+principal\s+sum|note\s+in\s+the\s+original\s+principal\s+amount)\D{0,20}\$\s*([0-9][0-9,]+(?:\.\d{2})?)",
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


def _latest_notice_artifact(con: sqlite3.Connection, lead_key: str) -> tuple[str | None, str | None, str]:
    prov = con.execute(
        """
        SELECT artifact_id, source_url, retrieved_at
        FROM lead_field_provenance
        WHERE lead_key=? AND source_channel NOT IN ('BATCHDATA', 'ATTOM', 'DEBT_RECONSTRUCTION')
          AND (artifact_id IS NOT NULL OR source_url IS NOT NULL)
        ORDER BY created_at DESC, prov_id DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    if prov:
        artifact_id = str(prov[0] or "").strip() or None
        source_url = str(prov[1] or "").strip() or None
        retrieved_at = str(prov[2] or _now_iso())
        if artifact_id:
            row = con.execute(
                """
                SELECT payload
                FROM raw_artifacts
                WHERE artifact_id=? AND payload IS NOT NULL
                LIMIT 1
                """,
                (artifact_id,),
            ).fetchone()
            if row and row[0]:
                return str(row[0]), artifact_id, retrieved_at
        if source_url:
            row = con.execute(
                """
                SELECT artifact_id, payload, retrieved_at
                FROM raw_artifacts
                WHERE source_url=? AND payload IS NOT NULL AND channel NOT IN ('BATCHDATA', 'ATTOM')
                ORDER BY retrieved_at DESC
                LIMIT 1
                """,
                (source_url,),
            ).fetchone()
            if row and row[1]:
                return str(row[1]), str(row[0] or ""), str(row[2] or retrieved_at)

    row = con.execute(
        """
        SELECT artifact_id, payload, retrieved_at
        FROM raw_artifacts
        WHERE lead_key=? AND payload IS NOT NULL AND channel NOT IN ('BATCHDATA', 'ATTOM')
        ORDER BY retrieved_at DESC
        LIMIT 1
        """,
        (lead_key,),
    ).fetchone()
    if not row or not row[1]:
        return None, None, _now_iso()
    return str(row[1]), str(row[0] or ""), str(row[2] or _now_iso())


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


def _parse_notice_debt_details(raw: str | None) -> dict[str, Any]:
    text = re.sub(r"\s+", " ", str(raw or "")).strip()
    if not text:
        return {}

    out: dict[str, Any] = {}

    dot_match = _DOT_DEED_RX.search(text)
    if dot_match:
        deed_date = _coerce_date(dot_match.group(1))
        original_lender = _clean_party_name(dot_match.group(2))
        if deed_date:
            out["mortgage_date"] = deed_date
        if original_lender:
            out["original_lender"] = original_lender

    assigned_match = _LAST_ASSIGNED_RX.search(text)
    if assigned_match:
        current_holder = _clean_party_name(assigned_match.group(1))
        if current_holder:
            out["current_holder"] = current_holder

    conveyed_match = _PROPERTY_CONVEYED_RX.search(text)
    if conveyed_match:
        deed_date = _coerce_date(conveyed_match.group(1))
        recorded_date = _coerce_date(conveyed_match.group(2))
        if recorded_date or deed_date:
            out["last_sale_date"] = recorded_date or deed_date
        out.setdefault("transfer_record_book", conveyed_match.group(3))
        out.setdefault("transfer_record_page", conveyed_match.group(4))

    principal_match = _PRINCIPAL_AMOUNT_RX.search(text)
    if principal_match:
        amount = _coerce_num(principal_match.group(1))
        if amount is not None:
            out["mortgage_amount"] = amount
    elif "deed of trust" in text.lower():
        money_matches = [_coerce_num(match.group(1)) for match in _CURRENCY_RX.finditer(text)]
        money_values = [value for value in money_matches if value is not None]
        if len(money_values) == 1:
            out["mortgage_amount"] = money_values[0]

    if "record_book" not in out:
        vol_match = _VOL_RX.search(text) or _BOOK_RX.search(text)
        if vol_match:
            out["record_book"] = vol_match.group(1)
    if "record_page" not in out:
        image_match = _IMAGE_RX.search(text) or _PAGE_RX.search(text)
        if image_match:
            out["record_page"] = image_match.group(1)
    if "record_instrument" not in out:
        instrument_match = _INSTRUMENT_RX.search(text)
        if instrument_match:
            out["record_instrument"] = instrument_match.group(1)
    return out


def _extract_batchdata_reconstruction(payload: dict[str, Any]) -> dict[str, Any]:
    item = _first_obj(payload)
    foreclosure = item.get("foreclosure") if isinstance(item.get("foreclosure"), dict) else {}
    history = item.get("mortgageHistory") if isinstance(item.get("mortgageHistory"), list) else []
    history = [candidate for candidate in history if isinstance(candidate, dict)]
    current_lender_name = _clean_party_name(foreclosure.get("currentLenderName"))
    history.sort(
        key=lambda candidate: str(
            candidate.get("recordingDate")
            or candidate.get("documentDate")
            or candidate.get("loanDate")
            or candidate.get("saleDate")
            or ""
        )
    )

    matched_history: list[dict[str, Any]] = []
    if current_lender_name:
        matched_history = [
            candidate
            for candidate in history
            if _clean_party_name(candidate.get("lenderName")) == current_lender_name
        ]

    latest = (matched_history[-1] if matched_history else (history[-1] if history else {}))
    earliest = history[0] if history else {}
    amount_candidate = next(
        (
            candidate
            for candidate in reversed(matched_history or history)
            if _coerce_num(candidate.get("loanAmount") or candidate.get("amount")) is not None
        ),
        {},
    )

    out: dict[str, Any] = {}
    current_lender = _clean_party_name(
        current_lender_name
        or latest.get("lenderName")
        or item.get("mortgageLender")
    )
    original_lender = _clean_party_name(earliest.get("lenderName"))
    mortgage_amount = _coerce_num(
        amount_candidate.get("loanAmount")
        or amount_candidate.get("amount")
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
    notice_payload, notice_artifact_id, notice_retrieved_at = _latest_notice_artifact(con, lead_key)
    notice_details = _parse_notice_debt_details(notice_payload)

    canonical_lender = (
        notice_details.get("current_holder")
        or
        batchdata_info.get("current_holder")
        or chain_info.get("current_holder")
        or current_lender
    )
    original_lender = (
        notice_details.get("original_lender")
        or
        chain_info.get("original_lender")
        or batchdata_info.get("original_lender")
        or current_lender
    )
    canonical_mortgage_date = (
        mortgage_date
        or notice_details.get("mortgage_date")
        or batchdata_info.get("mortgage_date")
        or chain_info.get("assignment_recorded_at")
    )
    canonical_amount = mortgage_amount if mortgage_amount is not None else (
        notice_details.get("mortgage_amount") if notice_details.get("mortgage_amount") is not None else batchdata_info.get("mortgage_amount")
    )
    record_book = chain_info.get("record_book") or notice_details.get("record_book") or batchdata_info.get("record_book")
    record_page = chain_info.get("record_page") or notice_details.get("record_page") or batchdata_info.get("record_page")
    record_instrument = chain_info.get("record_instrument") or notice_details.get("record_instrument") or batchdata_info.get("record_instrument")
    assignment_recorded_at = chain_info.get("assignment_recorded_at") or batchdata_info.get("assignment_recorded_at")
    canonical_last_sale_date = last_sale_date or notice_details.get("last_sale_date")
    source_mix = [
        label
        for label, present in (
            ("NOTICE", bool(notice_payload)),
            ("BATCHDATA", bool(payload)),
            ("CHAIN", bool(chain_info)),
        )
        if present
    ]

    confidence = "FULL"
    if not canonical_lender and canonical_amount is None:
        confidence = "THIN"
    elif canonical_amount is None:
        confidence = "PARTIAL"
    elif not canonical_lender:
        confidence = "PARTIAL"

    missing_reason = ""
    if canonical_amount is None:
        if payload and not batchdata_info.get("mortgage_amount"):
            missing_reason = "No loan amount in BatchData debt fields"
        elif notice_payload and not notice_details.get("mortgage_amount"):
            missing_reason = "No principal amount stated in notice text"
        else:
            missing_reason = "Loan amount could not be reconstructed from current sources"
    elif not canonical_lender:
        missing_reason = "Current lender could not be reconstructed from current sources"

    summary_parts = []
    if canonical_lender:
        summary_parts.append(f"Current holder: {canonical_lender}")
    if original_lender and original_lender != canonical_lender:
        summary_parts.append(f"Original lender: {original_lender}")
    if canonical_amount is not None:
        summary_parts.append(f"Loan amount: {int(canonical_amount):,}")
    if canonical_mortgage_date:
        summary_parts.append(f"Recording support: {canonical_mortgage_date}")
    if canonical_last_sale_date:
        summary_parts.append(f"Transfer support: {canonical_last_sale_date}")
    if record_book or record_page or record_instrument:
        ref = " ".join(part for part in (
            f"Book {record_book}" if record_book else "",
            f"Page {record_page}" if record_page else "",
            f"Instrument {record_instrument}" if record_instrument else "",
        ) if part)
        if ref:
            summary_parts.append(ref)
    if missing_reason:
        summary_parts.append(f"Blocker: {missing_reason}")

    return {
        "artifact_id": artifact_id or notice_artifact_id,
        "retrieved_at": retrieved_at if artifact_id else notice_retrieved_at,
        "mortgage_lender": canonical_lender,
        "mortgage_lender_original": original_lender,
        "mortgage_amount": canonical_amount,
        "mortgage_date": canonical_mortgage_date,
        "last_sale_date": canonical_last_sale_date,
        "mortgage_assignment_recorded_at": assignment_recorded_at,
        "mortgage_record_book": record_book,
        "mortgage_record_page": record_page,
        "mortgage_record_instrument": record_instrument,
        "debt_reconstruction_confidence": confidence,
        "debt_reconstruction_source_mix": ", ".join(source_mix) if source_mix else "NONE",
        "debt_reconstruction_missing_reason": missing_reason,
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
