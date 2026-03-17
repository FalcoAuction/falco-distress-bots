# src/bots/substitution_of_trustee_bot.py
# Substitution of Trustee ingestion bot.
# Enabled by default. Set FALCO_ENABLE_SOT=0 to disable.

import os
import re
import sqlite3
from typing import Dict

_TAG_RX = re.compile(r"<[^>]+>")
_NOTE_PAYABLE_TO_RX = re.compile(r"note\s+(?:was\s+)?payable\s+to,?\s*([^;.\n\r]+)", re.IGNORECASE)
_FOR_BENEFIT_OF_RX = re.compile(r"for\s+the\s+benefit\s+of\s+([^;.\n\r]+)", re.IGNORECASE)
_LAST_ASSIGNED_TO_RX = re.compile(r"last\s+assigned\s+to\s+([^;\n\r]+?)(?:\(|,?\s+c/o|\s+of\s+record|\s*,\s*the\s+entire|\s+the\s+entire)", re.IGNORECASE)
_HOLDER_RX = re.compile(r"owner\s+and\s+holder\s+of\s+the\s+note", re.IGNORECASE)
_PRINCIPAL_AMOUNT_RX = re.compile(r"principal\s+sum\s+of\s+\$?\s*([0-9][0-9,]*(?:\.\d{2})?)", re.IGNORECASE)


def _clean_notice_text(raw_html: str) -> str:
    text = _TAG_RX.sub(" ", raw_html or "")
    text = (
        text.replace("&nbsp;", " ")
        .replace("\xa0", " ")
        .replace("\r", " ")
        .replace("\n", " ")
    )
    return re.sub(r"\s+", " ", text).strip()


def _clean_party_name(value: str) -> str:
    text = re.sub(r"\s+", " ", (value or "").strip(" ,;:."))
    text = text.strip("()[]\"' ")
    return text


def _parse_money_amount(value: str) -> float | None:
    cleaned = (value or "").replace(",", "").replace("$", "").strip()
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def _parse_notice_fields(raw_html: str) -> Dict[str, object]:
    text = _clean_notice_text(raw_html)
    out: Dict[str, object] = {}

    lender_val = None
    for rx in (_LAST_ASSIGNED_TO_RX, _FOR_BENEFIT_OF_RX, _NOTE_PAYABLE_TO_RX):
        match = rx.search(text)
        if match:
            candidate = _clean_party_name(match.group(1))
            if candidate:
                lender_val = candidate
                break

    if lender_val:
        out["mortgage_lender"] = lender_val

    amount_match = _PRINCIPAL_AMOUNT_RX.search(text)
    if amount_match:
        amount = _parse_money_amount(amount_match.group(1))
        if amount is not None and amount > 0:
            out["mortgage_amount"] = amount

    if _HOLDER_RX.search(text):
        out["holder_reference_present"] = True

    return out


def run() -> Dict[str, int]:
    if os.environ.get("FALCO_ENABLE_SOT", "1").strip() == "0":
        print("[SubstitutionOfTrusteeBot] disabled (set FALCO_ENABLE_SOT=1 to enable)")
        return {"status": "disabled"}

    db = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    summary: Dict[str, int] = {
        "candidates_seen": 0,
        "leads_updated":   0,
        "missing_leads":   0,
        "notice_fields_written": 0,
    }

    try:
        con = sqlite3.connect(db)
        try:
            rows = con.execute(
                """
                SELECT lead_key, source_url
                FROM (
                    SELECT
                        lead_key,
                        source_url,
                        ROW_NUMBER() OVER (PARTITION BY lead_key ORDER BY id DESC) AS rn
                    FROM ingest_events
                    WHERE source = 'SUBSTITUTION_OF_TRUSTEE'
                )
                WHERE rn = 1
                LIMIT 200
                """
            ).fetchall()

            summary["candidates_seen"] = len(rows)

            for row in rows:
                lead_key = row[0]
                source_url = row[1]
                exists = con.execute(
                    "SELECT 1 FROM leads WHERE lead_key = ? LIMIT 1", (lead_key,)
                ).fetchone()
                if not exists:
                    summary["missing_leads"] += 1
                    continue
                con.execute(
                    """
                    UPDATE leads
                    SET distress_type      = 'SOT',
                        auction_readiness  = COALESCE(auction_readiness, 'UPSTREAM')
                    WHERE lead_key = ?
                    """,
                    (lead_key,),
                )
                summary["leads_updated"] += 1

                if source_url:
                    artifact = con.execute(
                        """
                        SELECT artifact_id, payload, retrieved_at
                        FROM raw_artifacts
                        WHERE source_url = ? AND payload IS NOT NULL
                        ORDER BY retrieved_at DESC
                        LIMIT 1
                        """,
                        (source_url,),
                    ).fetchone()
                    if artifact and artifact[1]:
                        parsed = _parse_notice_fields(str(artifact[1]))
                        created_at = artifact[2] or con.execute("SELECT CURRENT_TIMESTAMP").fetchone()[0]
                        if parsed.get("mortgage_lender"):
                            con.execute(
                                """
                                INSERT INTO lead_field_provenance
                                    (lead_key, field_name, value_type,
                                     field_value_text,
                                     units, confidence,
                                     source_channel, source_url, artifact_id,
                                     retrieved_at, run_id, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    lead_key,
                                    "mortgage_lender",
                                    "raw",
                                    str(parsed["mortgage_lender"]),
                                    None,
                                    0.72,
                                    "SUBSTITUTION_OF_TRUSTEE",
                                    source_url,
                                    artifact[0],
                                    artifact[2],
                                    os.environ.get("FALCO_RUN_ID"),
                                    created_at,
                                ),
                            )
                            summary["notice_fields_written"] += 1

                        if parsed.get("mortgage_amount") is not None:
                            con.execute(
                                """
                                INSERT INTO lead_field_provenance
                                    (lead_key, field_name, value_type,
                                     field_value_num,
                                     units, confidence,
                                     source_channel, source_url, artifact_id,
                                     retrieved_at, run_id, created_at)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    lead_key,
                                    "mortgage_amount",
                                    "raw",
                                    float(parsed["mortgage_amount"]),
                                    "USD",
                                    0.7,
                                    "SUBSTITUTION_OF_TRUSTEE",
                                    source_url,
                                    artifact[0],
                                    artifact[2],
                                    os.environ.get("FALCO_RUN_ID"),
                                    created_at,
                                ),
                            )
                            summary["notice_fields_written"] += 1

            con.commit()
        finally:
            con.close()
    except Exception as e:
        print(f"[SubstitutionOfTrusteeBot] ERROR {type(e).__name__}: {e}")

    print(
        f"[SubstitutionOfTrusteeBot] summary "
        f"candidates_seen={summary['candidates_seen']} "
        f"leads_updated={summary['leads_updated']} "
        f"missing_leads={summary['missing_leads']} "
        f"notice_fields_written={summary['notice_fields_written']}"
    )
    return summary
