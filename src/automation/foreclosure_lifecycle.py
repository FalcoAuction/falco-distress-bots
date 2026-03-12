import os
import re
import sqlite3
from datetime import UTC, datetime

from ..scoring.days_to_sale import days_to_sale


FORECLOSURE_SOURCES = {
    "ForeclosureTennessee",
    "TNForeclosureNotices",
    "PublicNotices",
}

UPSTREAM_SOURCES = {
    "LIS_PENDENS",
    "SUBSTITUTION_OF_TRUSTEE",
}


def _db_path() -> str:
    return os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")


def _now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _derive_status(
    current_sale_date: str | None,
    original_sale_date: str | None,
    dts_days: int | None,
    has_upstream: bool,
    has_foreclosure: bool,
) -> str:
    if current_sale_date:
        if dts_days is not None and dts_days < 0:
            return "expired"
        if original_sale_date and current_sale_date != original_sale_date:
            return "rescheduled"
        return "scheduled"

    if has_upstream:
        return "pre_foreclosure"

    if has_foreclosure:
        return "monitor"

    return "unknown"


def _norm_address(value: str | None) -> str:
    return " ".join(str(value or "").strip().lower().split())


def _clean_prefc_address(value: str | None) -> str | None:
    text = " ".join(str(value or "").strip().split())
    if not text:
        return None

    patterns = (
        r"(?i)^0+\s+",
        r"(?i)^0+\s+commonly\s+property\s+address:\s*",
        r"(?i)^0+\s+common\s+property\s+address:\s*",
        r"(?i)^common(?:ly)?\s+property\s+address:\s*",
        r"(?i)^common\s+address:\s*",
        r"(?i)^the\s+street\s+address\s+of\s+the\s+above-described\s+property\s+is\s+believed\s+to\s+be\s*",
        r"(?i)^the\s+street\s+address\s+of\s+the\s+property\s+is\s+believed\s+to\s+be\s*",
    )
    for pattern in patterns:
        text = re.sub(pattern, "", text).strip()

    text = re.sub(r"(?i)^\d{2,}\s+\d{3,}\s+", "", text).strip()
    text = re.sub(r"(?i)^common(?:ly)?\s+property\s+address:\s*", "", text).strip()
    text = re.sub(r"(?i)^common\s+address:\s*", "", text).strip()

    if " at " in text.lower() and "," in text:
        at_idx = text.lower().rfind(" at ")
        tail = text[at_idx + 4 :].strip()
        if re.search(r"\d", tail):
            text = tail

    if " is believed to be " in text.lower():
        text = text.split(" is believed to be ", 1)[1].strip()

    text = re.sub(r"(?i)^00\s+", "", text).strip(" ,.")
    text = re.sub(r"\b(\d{5})(\d{4})\b", r"\1-\2", text)
    text = re.sub(r"\s+,", ",", text)
    text = re.sub(r",\s*([A-Z][a-z]+)\.\s*([A-Z]{2}\b)", r", \1, \2", text)
    text = re.sub(r"\s{2,}", " ", text)

    lower = text.lower()
    if "city county building" in lower and "main street" in lower:
        return None
    if not re.search(r"\d", text):
        return None
    return text or None


def _merge_duplicate_leads(cur: sqlite3.Cursor) -> int:
    lead_rows = cur.execute(
        """
        SELECT lead_key, address, county, current_sale_date, sale_status, last_seen_at
        FROM leads
        WHERE COALESCE(current_sale_date, '') <> ''
           OR sale_status = 'pre_foreclosure'
        """
    ).fetchall()

    groups: dict[tuple[str, str, str], list[tuple[str, str]]] = {}
    for lead_key, address, county, current_sale_date, sale_status, last_seen_at in lead_rows:
        norm_address = _norm_address(address)
        if not norm_address or not county:
            continue

        if current_sale_date:
            group_key = (county, norm_address, current_sale_date)
        elif str(sale_status or "").strip().lower() == "pre_foreclosure":
            group_key = (county, norm_address, "pre_foreclosure")
        else:
            continue

        groups.setdefault(group_key, []).append(
            (lead_key, last_seen_at or "")
        )

    merged = 0
    for _, members in groups.items():
        if len(members) < 2:
            continue

        scored_members = []
        for lead_key, last_seen_at in members:
            ingest_count = cur.execute(
                "SELECT COUNT(*) FROM ingest_events WHERE lead_key=?",
                (lead_key,),
            ).fetchone()[0]
            scored_members.append((lead_key, ingest_count, last_seen_at))

        scored_members.sort(key=lambda row: (row[1], row[2], row[0]), reverse=True)
        canonical = scored_members[0][0]

        for duplicate, _, _ in scored_members[1:]:
            cur.execute("UPDATE ingest_events SET lead_key=? WHERE lead_key=?", (canonical, duplicate))
            cur.execute("UPDATE attom_enrichments SET lead_key=? WHERE lead_key=?", (canonical, duplicate))
            cur.execute("UPDATE raw_artifacts SET lead_key=? WHERE lead_key=?", (canonical, duplicate))
            cur.execute("UPDATE lead_field_provenance SET lead_key=? WHERE lead_key=?", (canonical, duplicate))
            cur.execute("DELETE FROM packets WHERE lead_key=?", (duplicate,))
            cur.execute("DELETE FROM leads WHERE lead_key=?", (duplicate,))
            merged += 1

    return merged


def run() -> dict[str, int]:
    con = sqlite3.connect(_db_path())
    cur = con.cursor()

    rows = cur.execute(
        """
        SELECT
            lead_key,
            source,
            sale_date,
            ingested_at
        FROM ingest_events
        ORDER BY lead_key ASC, ingested_at ASC, id ASC
        """
    ).fetchall()

    if not rows:
        con.close()
        return {
            "lead_rows_seen": 0,
            "leads_updated": 0,
            "scheduled": 0,
            "rescheduled": 0,
            "pre_foreclosure": 0,
            "expired": 0,
        }

    grouped: dict[str, list[tuple[str, str | None, str | None]]] = {}
    for lead_key, source, sale_date, ingested_at in rows:
        grouped.setdefault(lead_key, []).append((source or "", sale_date, ingested_at))

    summary = {
        "lead_rows_seen": len(grouped),
        "leads_updated": 0,
        "scheduled": 0,
        "rescheduled": 0,
        "pre_foreclosure": 0,
        "expired": 0,
        "invalid_prefc_removed": 0,
        "merged_duplicates": 0,
    }

    now = _now()

    for lead_key, events in grouped.items():
        sale_events = [(sale_date, ingested_at) for _, sale_date, ingested_at in events if sale_date]
        has_upstream = any(source in UPSTREAM_SOURCES for source, _, _ in events)
        has_foreclosure = any(source in FORECLOSURE_SOURCES for source, _, _ in events)

        original_sale_date = sale_events[0][0] if sale_events else None
        current_sale_date = sale_events[-1][0] if sale_events else None
        dts_days = days_to_sale(current_sale_date) if current_sale_date else None
        sale_status = _derive_status(
            current_sale_date=current_sale_date,
            original_sale_date=original_sale_date,
            dts_days=dts_days,
            has_upstream=has_upstream,
            has_foreclosure=has_foreclosure,
        )

        cur.execute(
            """
            UPDATE leads
            SET current_sale_date=?,
                original_sale_date=?,
                sale_status=?,
                dts_days=?,
                sale_date_updated_at=?
            WHERE lead_key=?
            """,
            (
                current_sale_date,
                original_sale_date,
                sale_status,
                dts_days,
                now,
                lead_key,
            ),
        )
        summary["leads_updated"] += cur.rowcount
        if sale_status in summary:
            summary[sale_status] += 1

        if sale_status == "pre_foreclosure":
            row = cur.execute("SELECT address FROM leads WHERE lead_key=?", (lead_key,)).fetchone()
            cleaned = _clean_prefc_address(row[0] if row else None)
            if cleaned and cleaned != (row[0] if row else None):
                cur.execute("UPDATE leads SET address=? WHERE lead_key=?", (cleaned, lead_key))
            elif not cleaned and row and row[0]:
                cur.execute("DELETE FROM ingest_events WHERE lead_key=?", (lead_key,))
                cur.execute("DELETE FROM attom_enrichments WHERE lead_key=?", (lead_key,))
                cur.execute("DELETE FROM raw_artifacts WHERE lead_key=?", (lead_key,))
                cur.execute("DELETE FROM lead_field_provenance WHERE lead_key=?", (lead_key,))
                cur.execute("DELETE FROM packets WHERE lead_key=?", (lead_key,))
                cur.execute("DELETE FROM leads WHERE lead_key=?", (lead_key,))
                summary["invalid_prefc_removed"] += 1

    summary["merged_duplicates"] = _merge_duplicate_leads(cur)
    con.commit()
    con.close()
    print(f"[ForeclosureLifecycle] summary {summary}")
    return summary
