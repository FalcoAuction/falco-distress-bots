"""Cross-reference AVM propagation enricher.

Audit found that some leads are missing property_value despite the same
property having an AVM in another lead (e.g., a memphis_codes row at
"123 Main St" with no AVM, while a memphis_daily_news row at the same
"123 Main St" has shelby_arcgis data with AVM filled).

Rule since 2026-11-24: a value belongs to a parcel, not to a street. An
unvalued row is filled only (1) from a county parcel record already on the
row itself, or (2) from another row that holds the same exact parcel number
in the same county, with the same unit, a stated basis and an observation
date (see _value_guard). A street address alone never moves a value: two
parcels can share one street (3911 Windyke, Shelby, 2026-11). Strictly
free, no external API calls.

Distress type: N/A (utility enricher).

Match key: county + exact parcel number.
"""
from __future__ import annotations

import re
import sys
import traceback as tb
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from .hamilton_assessor_bot import tax_roll_parcel
from ._field_confidence import deep_merge_dict
from ._provenance import record_field
from ._value_guard import (XREF_EXACT, XREF_KEY, XREF_SOURCE, copy_decision, norm_county, own_parcel_value,
                           row_parcels)
from ._slim_read import full_rows, paths, unpack

# Everything the parcel rule reads off a row (tax_roll_parcel and, in
# _value_guard, _doc_parcels and value_evidence). The scan selects these
# paths instead of the whole raw_payload and phone_metadata.
RAW_KEYS = ("parcel", "alt_parcel", "parcel_id", "county", "cumulative_owed", "hamilton_roll_valuation",
            "comptroller_parcel", "hamilton_assessor", "metro_parcel", "padctn", "rutherford_arcgis",
            "shelby_arcgis", "williamson_inigo", "trustee_detail", "preforeclosure_evidence", "code_case_parcel")
# One field of a larger record: (record, field).
RAW_FIELDS = (("extracted", "parcel"), ("nashville_codes", "Property_APN"), ("davidson_demolition_permit", "Parcel"))
META_KEYS = ("metro_parcel_checked", "shelby_trustee_value_review", XREF_KEY)
SCAN_COLUMNS = ("id, property_address, property_value, owner_name_records, county, property_value_source, "
                "property_value_as_of, " + paths("raw_payload", RAW_KEYS, "rp_") + ", "
                + ", ".join(f"rf_{rec}:raw_payload->{rec}->{field}" for rec, field in RAW_FIELDS) + ", "
                + paths("phone_metadata", META_KEYS, "pm_"))


def _light_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Put the selected paths back where the parcel rule looks for them."""
    raw = unpack(row, RAW_KEYS, "rp_")
    for rec, field in RAW_FIELDS:
        value = row.pop(f"rf_{rec}", None)
        if value is not None:
            raw[rec] = {field: value}
    row["raw_payload"] = raw
    row["phone_metadata"] = unpack(row, META_KEYS, "pm_")
    return row


def _normalize_address(addr: str) -> str:
    """Normalize for cross-referencing. Strips ZIP, normalizes street
    type abbreviations, uppercases, collapses whitespace.

    Examples:
      "1234 Main St, Nashville, TN 37206" -> "1234 MAIN STREET NASHVILLE TN"
      "1234 Main Street, Nashville, TN" -> "1234 MAIN STREET NASHVILLE TN"
    """
    if not addr or not isinstance(addr, str):
        return ""
    s = addr.upper().strip()
    # Drop ZIP
    s = re.sub(r"\s+\d{5}(-\d{4})?\s*,?\s*$", "", s)
    s = re.sub(r"\s+\d{5}(-\d{4})?\b", "", s)
    # Normalize street types — both directions go to canonical long form
    abbrev_map = (
        (r"\bST\.?\b", "STREET"), (r"\bRD\.?\b", "ROAD"),
        (r"\bAVE\.?\b", "AVENUE"), (r"\bDR\.?\b", "DRIVE"),
        (r"\bLN\.?\b", "LANE"), (r"\bBLVD\.?\b", "BOULEVARD"),
        (r"\bCT\.?\b", "COURT"), (r"\bCIR\.?\b", "CIRCLE"),
        (r"\bPL\.?\b", "PLACE"), (r"\bHWY\.?\b", "HIGHWAY"),
        (r"\bPKWY\.?\b", "PARKWAY"), (r"\bTRL\.?\b", "TRAIL"),
        (r"\bTER\.?\b", "TERRACE"), (r"\bCV\.?\b", "COVE"),
    )
    for pat, repl in abbrev_map:
        s = re.sub(pat, repl, s)
    # Preserve directional modifiers. Dropping them makes 123 E Main and
    # 123 W Main collapse to the same parcel, which is worse than a miss.
    directional_map = (
        (r"\bN\.?\b", "NORTH"), (r"\bS\.?\b", "SOUTH"),
        (r"\bE\.?\b", "EAST"), (r"\bW\.?\b", "WEST"),
    )
    for pat, repl in directional_map:
        s = re.sub(pat, repl, s)
    # Normalize commas + whitespace
    s = re.sub(r"[,.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _norm_county(county: Optional[str]) -> str:
    return norm_county(county)


def plan_fill(table: str, row: Dict[str, Any], rows: List[Tuple[str, Dict[str, Any]]],
              by_parcel: Dict[Tuple[str, str], List[int]]) -> Optional[Dict[str, Any]]:
    """What to write to one unvalued row, or None when there is nothing to
    consider. {"refused": reason} when a same-parcel candidate exists but the
    evidence is incomplete.

    Order: the row's own parcel record first (the value then keeps its real
    source, e.g. comptroller); otherwise a copy from another row that proves
    the same parcel, same county, same unit, with a basis and a date.
    """
    meta = row.get("phone_metadata") if isinstance(row.get("phone_metadata"), dict) else {}
    own = own_parcel_value(row)
    if own:
        return {"update": {"property_value": int(round(own["value"])), "property_value_source": own["source"],
                           "property_value_as_of": own["observed_on"]},
                "proof": {"match": "own_parcel_record", "parcel": own["parcel"], "county": own["county"],
                          "basis": own["basis"], "observed_on": own["observed_on"]}}
    county = norm_county(row.get("county"))
    if not county:
        return None
    seen = set()
    donors: List[Tuple[str, Dict[str, Any]]] = []
    for form in sorted(row_parcels(row)):
        for n in by_parcel.get((county, form), ()):
            d_table, donor = rows[n]
            if n in seen or donor.get("id") == row.get("id") or not donor.get("property_value"):
                continue
            seen.add(n)
            donors.append((d_table, donor))
    if not donors:
        return None
    reason = None
    best = None
    for d_table, donor in donors:
        decision = copy_decision(donor, row)
        if not decision["ok"]:
            reason = reason or decision["reason"]
            continue
        key = (decision["observed_on"], str(donor.get("id")))
        if best is None or key > best[0]:
            best = (key, d_table, donor, decision)
    if best is None:
        return {"refused": reason}
    _, d_table, donor, decision = best
    proof = {"source": XREF_SOURCE, "match": XREF_EXACT, "parcel": decision["parcel"],
             "county": decision["county"], "basis": decision["basis"], "basis_source": decision["basis_source"],
             "observed_on": decision["observed_on"], "confidence": 0.8,
             "donor_table": d_table, "donor_id": donor.get("id"),
             "note": "Same county parcel as the donor record; the value is that parcel's."}
    update: Dict[str, Any] = {
        "property_value": int(round(decision["value"])),
        # The source stays the county record the value was read from.
        "property_value_source": decision["basis_source"],
        "property_value_as_of": decision["observed_on"],
        "phone_metadata": deep_merge_dict(meta, {XREF_KEY: proof}),
    }
    if not row.get("owner_name_records") and donor.get("owner_name_records"):
        update["owner_name_records"] = donor["owner_name_records"]
    return {"update": update, "proof": proof}


class XrefAvmEnricherBot(BotBase):
    name = "xref_avm_enricher"
    description = "Fill a missing value from the same exact county parcel (never by street address)"
    throttle_seconds = 0.0
    expected_min_yield = 1
    max_leads_per_run = 5000

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "propagated": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        propagated = 0
        owner_propagated = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            # Index every row under each parcel number it holds. A street
            # address is never a key: two parcels can share one street.
            rows: List[Tuple[str, Dict[str, Any]]] = []
            by_parcel: Dict[Tuple[str, str], List[int]] = defaultdict(list)
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                fetched = self._fetch_all(client, table)
                self.logger.info(f"{table}: {len(fetched)} rows fetched")
                for r in fetched:
                    if tax_roll_parcel(r.get("raw_payload")):
                        continue  # Hamilton roll valuations use the assessor parcel ID.
                    rows.append((table, r))
                    county = norm_county(r.get("county"))
                    for form in (row_parcels(r) if county else ()):
                        by_parcel[(county, form)].append(len(rows) - 1)

            for rcpt_table, rcpt_row in rows:
                if rcpt_row.get("property_value"):
                    continue
                patch = plan_fill(rcpt_table, rcpt_row, rows, by_parcel)
                if patch is None:
                    continue
                if patch.get("refused"):
                    skipped += 1
                    continue
                update = patch["update"]
                if "owner_name_records" in update:
                    owner_propagated += 1
                try:
                    if "phone_metadata" in update:
                        # The scan held three phone_metadata keys. Merge the
                        # proof into the whole object as it stands now.
                        held = full_rows(client, rcpt_table, "id,phone_metadata", [rcpt_row["id"]])
                        if rcpt_row["id"] not in held:
                            skipped += 1
                            continue
                        update["phone_metadata"] = deep_merge_dict(
                            held[rcpt_row["id"]].get("phone_metadata"), {XREF_KEY: patch["proof"]})
                    q = client.table(rcpt_table).update(update).eq("id", rcpt_row["id"])
                    # Fill a gap only: refuse the write if a value landed meanwhile.
                    written = q.is_("property_value", "null").execute()
                    if not (getattr(written, "data", None) or []):
                        skipped += 1
                        continue
                    propagated += 1
                    if rcpt_table == "homeowner_requests":
                        record_field(
                            client, rcpt_row["id"], "property_value",
                            update["property_value"], update["property_value_source"],
                            confidence=0.8, metadata=patch["proof"],
                        )
                except Exception as e:
                    self.logger.warning(
                        f"  update failed id={rcpt_row['id']}: {e}"
                    )
                    skipped += 1

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif propagated == 0:
            status = "zero_yield"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=propagated + skipped,
            parsed_count=propagated + skipped,
            staged_count=propagated, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"propagated={propagated} owner_propagated={owner_propagated} "
            f"skipped={skipped}"
        )
        return {
            "name": self.name, "status": status,
            "propagated": propagated, "owner_propagated": owner_propagated,
            "skipped": skipped,
            "error": error_message,
            "staged": propagated, "duplicates": skipped,
            "fetched": propagated + skipped,
        }

    def _fetch_all(self, client, table: str) -> List[Dict[str, Any]]:
        out = []
        page = 0
        # Pages continue from the last id, not from an offset: at offset 42,000
        # the scan timed out on the small database (2026-11-27) and the run
        # worked from a partial table. One retry, then stop.
        PAGE_SIZE = 500
        last_id = None
        failures = 0
        while True:
            try:
                # Every status is read on purpose: a rejected row that holds
                # a verified parcel value can still supply it to a live lead.
                q = client.table(table).select(SCAN_COLUMNS)
                if last_id is not None:
                    q = q.gt("id", last_id)
                r = q.order("id").limit(PAGE_SIZE).execute()
                rows = getattr(r, "data", None) or []
                if not rows:
                    break
                last_id = rows[-1]["id"]
                out.extend(_light_row(x) for x in rows)
                if len(rows) < PAGE_SIZE:
                    break
                page += 1
                failures = 0
            except Exception as e:
                failures += 1
                self.logger.warning(f"fetch failed on {table} page {page}: {e}")
                if failures > 1:
                    break
        return out


def run() -> dict:
    bot = XrefAvmEnricherBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test mode: normalize input addresses
        for a in sys.argv[1:]:
            print(f"{a!r} -> {_normalize_address(a)!r}")
    else:
        print(run())
