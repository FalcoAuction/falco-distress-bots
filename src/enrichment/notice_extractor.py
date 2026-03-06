# src/enrichment/notice_extractor.py
#
# Parses ForeclosureTennessee listing HTML stored in raw_artifacts (channel=NOTICE_HTML)
# and writes extracted fields to lead_field_provenance.
# No network calls. No new deps. Deterministic.

import html as _html_mod
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Dict


# ---------------------------------------------------------------------------
# Label → field_name mapping (order matters for trustee split below)
# ---------------------------------------------------------------------------
_LABEL_MAP = {
    "Posted":            "ft_posted_date",
    "Sale Date":         "ft_sale_date",
    "Continuance Date":  "ft_continuance_date",
    "Trustee Name":      "ft_trustee_name_raw",
    "Property Address":  "ft_property_address",
    "City/State":        "ft_city_state",
    "Zipcode":           "ft_zip",
    "County":            "ft_county",
}

_TAG_RX = re.compile(r"<[^>]+>")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _strip_tags(s: str) -> str:
    return _TAG_RX.sub("", s).strip()


def _extract_fields(html_text: str) -> Dict[str, str]:
    """
    Parse a ForeclosureTennessee listing HTML and return a dict of
    field_name -> value_text for all recognised labels found.
    Derived fields (ft_trustee_firm, ft_trustee_person) are also added
    when ft_trustee_name_raw contains a '/'.
    """
    fields: Dict[str, str] = {}

    for label, field_name in _LABEL_MAP.items():
        rx = re.compile(
            r"<strong>\s*" + re.escape(label) + r"\s*:\s*</strong>\s*(.*?)\s*</p>",
            re.IGNORECASE | re.DOTALL,
        )
        m = rx.search(html_text)
        if m:
            val = _strip_tags(_html_mod.unescape(m.group(1))).strip()
            if val:
                fields[field_name] = val

    # Derived: split trustee raw into firm + person on first '/'
    raw_trustee = fields.get("ft_trustee_name_raw", "")
    if "/" in raw_trustee:
        firm, _, person = raw_trustee.partition("/")
        firm = firm.strip()
        person = person.strip()
        if firm:
            fields["ft_trustee_firm"] = firm
        if person:
            fields["ft_trustee_person"] = person

    return fields


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run() -> Dict[str, int]:
    """
    Iterate newest NOTICE_HTML artifacts, extract fields, write provenance rows.
    Returns summary dict with html_rows_seen / fields_written / skipped_existing / errors.
    """
    db = _db_path()
    max_rows = int(os.environ.get("FALCO_NOTICE_EXTRACT_MAX", "50"))
    run_id = os.environ.get("FALCO_RUN_ID")

    summary: Dict[str, int] = {
        "html_rows_seen": 0,
        "fields_written": 0,
        "skipped_existing": 0,
        "errors": 0,
    }

    try:
        con = sqlite3.connect(db)
        con.row_factory = sqlite3.Row
    except Exception:
        summary["errors"] += 1
        return summary

    try:
        try:
            artifacts = con.execute(
                """
                SELECT artifact_id, lead_key, source_url, retrieved_at, payload
                FROM raw_artifacts
                WHERE channel = 'NOTICE_HTML' AND payload IS NOT NULL
                ORDER BY retrieved_at DESC
                LIMIT ?
                """,
                (max_rows,),
            ).fetchall()
        except Exception:
            summary["errors"] += 1
            return summary

        summary["html_rows_seen"] = len(artifacts)
        _created_at = _now()

        for art in artifacts:
            artifact_id = art["artifact_id"]
            lead_key    = art["lead_key"]
            retrieved_at = art["retrieved_at"] or _created_at
            payload      = art["payload"]

            try:
                fields = _extract_fields(payload)
            except Exception:
                summary["errors"] += 1
                continue

            for field_name, value_text in fields.items():
                try:
                    exists = con.execute(
                        """
                        SELECT 1 FROM lead_field_provenance
                        WHERE lead_key = ? AND field_name = ? AND artifact_id = ?
                        LIMIT 1
                        """,
                        (lead_key, field_name, artifact_id),
                    ).fetchone()

                    if exists:
                        summary["skipped_existing"] += 1
                        continue

                    con.execute(
                        """
                        INSERT INTO lead_field_provenance
                            (lead_key, field_name, value_type,
                             field_value_text,
                             units, confidence,
                             source_channel, artifact_id,
                             retrieved_at, run_id, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            lead_key, field_name, "raw",
                            value_text,
                            None, None,
                            "ForeclosureTennessee", artifact_id,
                            retrieved_at, run_id, _created_at,
                        ),
                    )
                    summary["fields_written"] += 1

                except Exception:
                    summary["errors"] += 1

        con.commit()

    except Exception:
        summary["errors"] += 1
    finally:
        try:
            con.close()
        except Exception:
            pass

    return summary
