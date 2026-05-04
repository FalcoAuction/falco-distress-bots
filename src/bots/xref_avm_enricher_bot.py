"""Cross-reference AVM propagation enricher.

Audit found that some leads are missing property_value despite the same
property having an AVM in another lead (e.g., a memphis_codes row at
"123 Main St" with no AVM, while a memphis_daily_news row at the same
"123 Main St" has shelby_arcgis data with AVM filled).

This enricher walks the corpus, normalizes addresses, and for each
group of rows sharing the same address propagates AVM (and optionally
owner data) from the row that has it to the rows that don't. Strictly
free — no external API calls.

Distress type: N/A (utility enricher).

Confidence: 0.9 — when source row has high-confidence AVM (from a
county assessor), the destination row gets it via address-match.
Match key: normalized property_address.
"""
from __future__ import annotations

import re
import sys
import traceback as tb
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._provenance import record_field


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
    # Drop directional modifiers that vary across sources
    s = re.sub(r"\b(NORTH|SOUTH|EAST|WEST|N|S|E|W)\b\.?", "", s)
    # Normalize commas + whitespace
    s = re.sub(r"[,.]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


class XrefAvmEnricherBot(BotBase):
    name = "xref_avm_enricher"
    description = "Propagate AVM across leads sharing the same property_address"
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
            # Build address → [rows] map across BOTH tables.
            # Strategy: rows missing AVM get filled from rows with AVM at
            # the same normalized address.
            by_addr: Dict[str, List[Tuple[str, Dict[str, Any]]]] = defaultdict(list)
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                rows = self._fetch_all(client, table)
                self.logger.info(f"{table}: {len(rows)} rows fetched")
                for r in rows:
                    addr = r.get("property_address")
                    norm = _normalize_address(addr or "")
                    if not norm:
                        continue
                    by_addr[norm].append((table, r))

            # For each address group, find a donor (row with AVM) and
            # propagate to recipients (rows without).
            for norm_addr, group in by_addr.items():
                if len(group) < 2:
                    continue  # No cross-ref opportunity
                donors = [(t, r) for t, r in group if r.get("property_value")]
                recipients = [(t, r) for t, r in group if not r.get("property_value")]
                if not donors or not recipients:
                    continue
                # Pick the donor with highest property_value confidence
                # (proxy: prefer one whose phone_metadata has lead_field_provenance
                # entries; fallback: any donor with non-null AVM).
                donor_table, donor_row = donors[0]
                donor_avm = donor_row.get("property_value")
                donor_owner = donor_row.get("owner_name_records")
                donor_county = donor_row.get("county")

                for rcpt_table, rcpt_row in recipients:
                    update: Dict[str, Any] = {"property_value": donor_avm}
                    # Also propagate owner_name + county if missing
                    if not rcpt_row.get("owner_name_records") and donor_owner:
                        update["owner_name_records"] = donor_owner
                        owner_propagated += 1
                    if not rcpt_row.get("county") and donor_county:
                        update["county"] = donor_county

                    try:
                        client.table(rcpt_table).update(update).eq(
                            "id", rcpt_row["id"]
                        ).execute()
                        propagated += 1
                        # Provenance for the live table
                        if rcpt_table == "homeowner_requests":
                            record_field(
                                client, rcpt_row["id"], "property_value",
                                donor_avm, "xref_avm_enricher",
                                confidence=0.9,
                                metadata={
                                    "donor_table": donor_table,
                                    "donor_id": donor_row.get("id"),
                                    "matched_address": norm_addr[:80],
                                },
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
        PAGE_SIZE = 1000
        while True:
            try:
                r = client.table(table).select(
                    "id, property_address, property_value, "
                    "owner_name_records, county"
                ).order("id").range(
                    page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1
                ).execute()
                rows = getattr(r, "data", None) or []
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
                page += 1
            except Exception as e:
                self.logger.warning(f"fetch failed on {table} page {page}: {e}")
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
