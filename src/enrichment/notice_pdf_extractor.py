# src/enrichment/notice_pdf_extractor.py
#
# Best-effort phone/email scanner for ForeclosureTennessee NOTICE_PDF artifacts.
# No OCR. Uses pypdf text extraction when available; else falls back to latin-1 decode.
# No new deps. Deterministic. Never raises.

import io
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

# Optional dependency (recommended): pip install pypdf
try:
    from pypdf import PdfReader  # type: ignore
    _HAVE_PYPDF = True
except Exception:
    PdfReader = None  # type: ignore
    _HAVE_PYPDF = False


def _pdf_text(pdf_bytes: bytes) -> str:
    """Extract text via pypdf when available; else raw latin-1 decode. Never raises."""
    if not pdf_bytes:
        return ""
    if _HAVE_PYPDF:
        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            parts = []
            for p in reader.pages:
                try:
                    t = p.extract_text() or ""
                except Exception:
                    t = ""
                if t:
                    parts.append(t)
            return "\n".join(parts)
        except Exception:
            pass
    try:
        return pdf_bytes.decode("latin-1", errors="ignore")
    except Exception:
        return ""

# ---------------------------------------------------------------------------
# Regexes
# ---------------------------------------------------------------------------

_EMAIL_RX = re.compile(
    r"[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}",
    re.IGNORECASE,
)

# Formatted phone: requires at least one separator character between groups.
# Matches: (615) 555-1234  /  615-555-1234  /  615.555.1234  /  +1 615 555 1234
_PHONE_FMT_RX = re.compile(
    r"(?:\+?1[\s\-\.])?(?:\(\d{3}\)|\d{3})[\s\-\.]?\d{3}[\s\-\.]\d{4}"
)

# Bare 10-digit run (no separators) — only accepted near phone-context keywords.
_PHONE_DIGITS_RX = re.compile(r"\b\d{10}\b")

# Context keywords that make a bare digit run credible as a phone number.
_PHONE_CTX_RX = re.compile(
    r"\b(?:phone|tel|telephone|call|contact|attorney|trustee|fax|email)\b",
    re.IGNORECASE,
)

_PHONE_CTX_WINDOW = 80  # characters on either side

_REPEATED_DIGITS = {str(d) * 10 for d in range(10)}

# Labels used for trustee/attorney structured extraction
_TRUSTEE_LABELS = [
    "Trustee:",
    "Substitute Trustee:",
    "Successor Trustee:",
    "Trustee/Attorney:",
    "Attorney for Trustee:",
]
_ADDRESS_LABELS = [
    "Address:",
    "Mailing Address:",
    "Notice Address:",
    "Send Notice To:",
    "Mail To:",
    "Return To:",
]
_EMAIL_LABEL_LABELS = [
    "Email:",
    "E-mail:",
    "Email Address:",
]

# Fallback trustee extraction — firm-ish keyword detector
_FIRM_RX = re.compile(
    r"\b(ASSOCIATES|LAW|LEGAL|PLLC|LLP|LLC|P\.?C\.?|ATTORNEYS?|COUNSEL|TRUSTEE|SERVICES|GROUP|FIRM)\b",
    re.IGNORECASE,
)

# Address-line heuristics for fallback
_ADDR_STREET_RX = re.compile(
    r"\b\d+\b.{0,60}\b(ST|STREET|AVE|AVENUE|RD|ROAD|DR|DRIVE|BLVD|BOULEVARD|"
    r"LN|LANE|CT|COURT|WAY|PIKE|HWY|HIGHWAY|PL|PLACE|CIR|CIRCLE|PKWY|PARKWAY)\b",
    re.IGNORECASE,
)
_ADDR_ZIP_STATE_RX = re.compile(
    r"\bTN\b.{0,20}\b\d{5}\b|\b\d{5}\b.{0,20}\bTN\b",
    re.IGNORECASE,
)
_ADDR_SUITE_RX = re.compile(
    r"\b(Suite|Ste\.?|STE|P\.?\s*O\.?\s*Box|PO\s+Box)\b",
    re.IGNORECASE,
)


def _clean_value(s: str) -> str:
    """Normalize whitespace, strip leading/trailing junk."""
    s = re.sub(r"[\r\n\t]+", " ", s)
    s = re.sub(r"\s{2,}", " ", s)
    return s.strip()


def _is_printable_text(s: str) -> bool:
    """
    Return True only if s looks like human-readable text.
    Rejects strings that contain C0 control chars (except space),
    C1 control chars (0x80-0x9F), or the Unicode replacement character —
    all of which appear when binary PDF stream bytes are decoded as latin-1.
    Requires at least one ASCII letter.
    """
    if not s:
        return False
    if "\ufffd" in s:
        return False
    for c in s:
        o = ord(c)
        if o < 0x20 and c not in (" ", "\t"):
            return False
        if 0x7F <= o <= 0x9F:
            return False
    return bool(re.search(r"[A-Za-z]", s))


def _extract_labeled_value(text: str, labels: List[str]) -> Optional[str]:
    """Find first label in text; capture up to 160 chars or end-of-line."""
    for label in labels:
        rx = re.compile(re.escape(label) + r"[ \t]*(.{1,160}?)(?:\n|$)", re.IGNORECASE)
        m = rx.search(text)
        if m:
            val = _clean_value(m.group(1))
            if val:
                return val
    return None


def _is_addr_line(line: str) -> bool:
    """Return True if a line looks like part of a mailing address block."""
    return bool(
        _ADDR_STREET_RX.search(line)
        or _ADDR_ZIP_STATE_RX.search(line)
        or _ADDR_SUITE_RX.search(line)
    )


def _fallback_trustee(lines: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Heuristic scan when label-based extraction found nothing.
    Finds the first firm-ish line (contains _FIRM_RX keyword, length 6-120,
    has at least one letter), then scans the next 12 lines for an address block
    (up to 4 address-like lines joined with ", ").
    Returns (firm_line, address_string) — either may be None.
    Never raises.
    """
    firm_line: Optional[str] = None
    firm_idx: int = -1

    for i, line in enumerate(lines):
        if not (6 <= len(line) <= 120):
            continue
        if not re.search(r"[A-Za-z]", line):
            continue
        if _FIRM_RX.search(line):
            firm_line = line
            firm_idx = i
            break

    if firm_line is None:
        return None, None

    addr_parts: List[str] = []
    for line in lines[firm_idx + 1 : firm_idx + 13]:
        if not line:
            continue
        if _is_addr_line(line):
            addr_parts.append(line)
            if len(addr_parts) >= 4:
                break

    address = ", ".join(addr_parts) if addr_parts else None
    return firm_line, address


# ---------------------------------------------------------------------------
# Phone helpers
# ---------------------------------------------------------------------------

def _looks_like_date_id_10d(digits: str) -> bool:
    # Reject 10-digit sequences that look like YYYYMMDDxx (common in PDFs as dates/ids)
    # e.g. 2026012312, 2025102008
    if len(digits) != 10:
        return False
    if digits.startswith("19") or digits.startswith("20"):
        yyyy = int(digits[:4])
        if 1900 <= yyyy <= 2099:
            mm = int(digits[4:6])
            dd = int(digits[6:8])
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                return True
    return False


def _normalize_us_phone(s: str) -> Optional[str]:
    """
    Normalize a raw phone string to NXX-NXX-XXXX format.
    Returns None if the string is not a valid US phone number.
    """
    digits = re.sub(r"\D", "", s)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    if _looks_like_date_id_10d(digits):
        return None
    # Additional business-validity rejects: PDF binary regions often contain
    # year-like sequences (20260123xx), trailing-zero IDs, and date stamps that
    # pass digit-count checks but are not real phone numbers.
    if digits.startswith("19") or digits.startswith("20"):
        return None
    if digits[-4:] == "0000":
        return None
    if re.match(r"^(19|20)\d{2}\d{2}\d{2}\d{2}$", digits):
        return None
    area = digits[:3]
    exch = digits[3:6]
    line = digits[6:]
    if area[0] in ("0", "1") or exch[0] in ("0", "1"):
        return None
    if digits in _REPEATED_DIGITS:
        return None
    return f"{area}-{exch}-{line}"


def _has_phone_context(text: str, match_start: int, match_end: int) -> bool:
    """Return True if a phone-context keyword appears within the context window."""
    lo = max(0, match_start - _PHONE_CTX_WINDOW)
    hi = min(len(text), match_end + _PHONE_CTX_WINDOW)
    return bool(_PHONE_CTX_RX.search(text[lo:hi]))


def _extract_phones(text: str) -> List[str]:
    """
    Return deduplicated list of raw phone-like strings found in text.
    Formatted matches are always accepted.
    Bare 10-digit runs are only accepted when near a phone-context keyword.
    """
    seen: set = set()
    phones: List[str] = []

    for m in _PHONE_FMT_RX.finditer(text):
        raw = m.group(0).strip()
        if raw and raw not in seen:
            seen.add(raw)
            phones.append(raw)

    for m in _PHONE_DIGITS_RX.finditer(text):
        raw = m.group(0).strip()
        if raw in seen:
            continue
        if not _has_phone_context(text, m.start(), m.end()):
            continue
        seen.add(raw)
        phones.append(raw)

    return phones


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------

def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_pdf_bytes(row) -> Optional[bytes]:
    """Return raw PDF bytes from a raw_artifacts row, or None if unavailable."""
    storage_mode = row["storage_mode"] or ""
    payload      = row["payload"]
    file_path    = row["file_path"]

    if storage_mode == "db" and payload is not None:
        if isinstance(payload, bytes):
            return payload
        s = str(payload)
        return s.encode("latin-1", errors="ignore")

    if file_path:
        fp = os.path.normpath(file_path)
        if not os.path.isabs(fp):
            fp = os.path.join(os.getcwd(), fp)
        try:
            with open(fp, "rb") as fh:
                return fh.read()
        except OSError:
            return None

    return None


def _scan_bytes(pdf_bytes: bytes) -> Tuple[List[str], List[str]]:
    """
    Decode pdf_bytes as latin-1, extract phones and emails.
    Returns (phones, emails) — each deduplicated within this call.

    Phone acceptance rules:
      - If the match contains any separator character ( ) - . or space → accept.
      - If the match is digits-only (len 10) → only accept if a context keyword
        appears within _PHONE_CTX_WINDOW characters on either side.
    """
    text = _pdf_text(pdf_bytes)
    _SEPARATORS = frozenset("()-. ")

    seen_phones: set = set()
    phones: List[str] = []

    # Formatted phones — contain at least one required separator, always accept.
    for m in _PHONE_FMT_RX.finditer(text):
        raw = m.group(0).strip()
        if not raw or raw in seen_phones:
            continue
        seen_phones.add(raw)
        phones.append(raw)

    # Bare 10-digit runs — accept only if near a context keyword.
    for m in _PHONE_DIGITS_RX.finditer(text):
        raw = m.group(0).strip()
        if not raw or raw in seen_phones:
            continue
        lo = max(0, m.start() - _PHONE_CTX_WINDOW)
        hi = min(len(text), m.end() + _PHONE_CTX_WINDOW)
        if not _PHONE_CTX_RX.search(text[lo:hi]):
            continue
        seen_phones.add(raw)
        phones.append(raw)

    emails: List[str] = []
    seen_emails: set = set()
    for m in _EMAIL_RX.finditer(text):
        val = m.group(0).lower()
        if val not in seen_emails:
            seen_emails.add(val)
            emails.append(val)

    return phones, emails


# ---------------------------------------------------------------------------
# Provenance insert
# ---------------------------------------------------------------------------

def _write_prov(
    con: sqlite3.Connection,
    lead_key: str,
    field_name: str,
    value_text: str,
    artifact_id: str,
    retrieved_at: str,
    run_id: Optional[str],
    created_at: str,
) -> int:
    """Insert provenance row if not already present. Returns 1 written, 0 skipped."""
    exists = con.execute(
        """
        SELECT 1 FROM lead_field_provenance
        WHERE lead_key = ? AND field_name = ? AND artifact_id = ?
        LIMIT 1
        """,
        (lead_key, field_name, artifact_id),
    ).fetchone()
    if exists:
        return 0

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
            retrieved_at, run_id, created_at,
        ),
    )
    return 1


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run() -> Dict[str, int]:
    db       = _db_path()
    max_rows = int(os.environ.get("FALCO_NOTICE_PDF_EXTRACT_MAX", "50"))
    run_id   = os.environ.get("FALCO_RUN_ID")

    summary: Dict[str, int] = {
        "pdf_rows_seen":          0,
        "bytes_read":             0,
        "phones_raw_written":     0,
        "phones_norm_written":    0,
        "emails_written":         0,
        "trustee_fields_written":  0,
        "trustee_fallback_written": 0,
        "skipped_missing_bytes":   0,
        "skipped_existing":       0,
        "errors":                 0,
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
                SELECT artifact_id, lead_key, source_url, retrieved_at,
                       content_type, storage_mode, payload, file_path
                FROM raw_artifacts
                WHERE channel = 'NOTICE_PDF'
                ORDER BY retrieved_at DESC
                LIMIT ?
                """,
                (max_rows,),
            ).fetchall()
        except Exception:
            summary["errors"] += 1
            return summary

        summary["pdf_rows_seen"] = len(artifacts)
        _created_at = _now()

        for art in artifacts:
            artifact_id  = art["artifact_id"]
            lead_key     = art["lead_key"]
            retrieved_at = art["retrieved_at"] or _created_at

            try:
                pdf_bytes = _load_pdf_bytes(art)
            except Exception:
                summary["errors"] += 1
                continue

            if pdf_bytes is None:
                summary["skipped_missing_bytes"] += 1
                continue

            summary["bytes_read"] += len(pdf_bytes)

            try:
                phones, emails = _scan_bytes(pdf_bytes)
            except Exception:
                summary["errors"] += 1
                continue

            for phone_raw in phones:
                try:
                    written = _write_prov(
                        con, lead_key, "notice_phone_raw", phone_raw,
                        artifact_id, retrieved_at, run_id, _created_at,
                    )
                    if written:
                        summary["phones_raw_written"] += 1
                    else:
                        summary["skipped_existing"] += 1

                    phone_norm = _normalize_us_phone(phone_raw)
                    if phone_norm is not None:
                        written = _write_prov(
                            con, lead_key, "notice_phone", phone_norm,
                            artifact_id, retrieved_at, run_id, _created_at,
                        )
                        if written:
                            summary["phones_norm_written"] += 1
                        else:
                            summary["skipped_existing"] += 1
                except Exception:
                    summary["errors"] += 1

            for email in emails:
                try:
                    written = _write_prov(
                        con, lead_key, "notice_email", email,
                        artifact_id, retrieved_at, run_id, _created_at,
                    )
                    if written:
                        summary["emails_written"] += 1
                    else:
                        summary["skipped_existing"] += 1
                except Exception:
                    summary["errors"] += 1

            # --- Label-based trustee/attorney extraction ---
            try:
                _text = _pdf_text(pdf_bytes)

                _trustee_raw = _extract_labeled_value(_text, _TRUSTEE_LABELS)
                if _trustee_raw and _is_printable_text(_trustee_raw):
                    _w = _write_prov(
                        con, lead_key, "notice_trustee_name_raw", _trustee_raw,
                        artifact_id, retrieved_at, run_id, _created_at,
                    )
                    if _w:
                        summary["trustee_fields_written"] += 1
                    else:
                        summary["skipped_existing"] += 1

                    # Derive firm: part before "/" if present, else full raw string
                    _firm = _trustee_raw.partition("/")[0].strip() if "/" in _trustee_raw else _trustee_raw
                    if _firm and _is_printable_text(_firm):
                        _w = _write_prov(
                            con, lead_key, "notice_trustee_firm", _firm,
                            artifact_id, retrieved_at, run_id, _created_at,
                        )
                        if _w:
                            summary["trustee_fields_written"] += 1
                        else:
                            summary["skipped_existing"] += 1
                else:
                    _trustee_raw = None  # treat as not found for fallback gate

                _addr = _extract_labeled_value(_text, _ADDRESS_LABELS)
                if _addr and _is_printable_text(_addr):
                    _w = _write_prov(
                        con, lead_key, "notice_trustee_address", _addr,
                        artifact_id, retrieved_at, run_id, _created_at,
                    )
                    if _w:
                        summary["trustee_fields_written"] += 1
                    else:
                        summary["skipped_existing"] += 1
                else:
                    _addr = None  # treat as not found for fallback gate

                _email_label = _extract_labeled_value(_text, _EMAIL_LABEL_LABELS)
                if _email_label and _is_printable_text(_email_label):
                    _w = _write_prov(
                        con, lead_key, "notice_email", _email_label.lower(),
                        artifact_id, retrieved_at, run_id, _created_at,
                    )
                    if _w:
                        summary["trustee_fields_written"] += 1
                    else:
                        summary["skipped_existing"] += 1

                # --- Fallback: heuristic scan when label-based got no trustee ---
                if not _trustee_raw:
                    _lines = [ln.strip() for ln in _text.splitlines() if ln.strip()]
                    _fb_firm, _fb_addr = _fallback_trustee(_lines)

                    if _fb_firm and _is_printable_text(_fb_firm):
                        _w = _write_prov(
                            con, lead_key, "notice_trustee_name_raw", _fb_firm,
                            artifact_id, retrieved_at, run_id, _created_at,
                        )
                        if _w:
                            summary["trustee_fallback_written"] += 1
                        else:
                            summary["skipped_existing"] += 1

                        _w = _write_prov(
                            con, lead_key, "notice_trustee_firm", _fb_firm,
                            artifact_id, retrieved_at, run_id, _created_at,
                        )
                        if _w:
                            summary["trustee_fallback_written"] += 1
                        else:
                            summary["skipped_existing"] += 1

                    if _fb_addr and not _addr and _is_printable_text(_fb_addr):
                        _w = _write_prov(
                            con, lead_key, "notice_trustee_address", _fb_addr,
                            artifact_id, retrieved_at, run_id, _created_at,
                        )
                        if _w:
                            summary["trustee_fallback_written"] += 1
                        else:
                            summary["skipped_existing"] += 1

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
