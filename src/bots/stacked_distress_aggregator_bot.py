"""
Stacked-distress aggregator — flags leads carrying MULTIPLE distress
signals.

A lead with one signal (just BANKRUPTCY, just PROBATE) is interesting
but not necessarily urgent. A lead with TWO OR MORE signals at the
same address (e.g., BANKRUPTCY filed by an owner whose property is
also TAX_LIEN delinquent and has a CODE_VIOLATION) is *severely*
distressed — those homeowners are out of options and the call gets
priority on Chris's queue.

The aggregator walks every lead and computes:
  - signal_count: number of distinct distress_type values observed
    for the same property_address (case-insensitive normalize)
    across BOTH live + staging tables
  - signal_types: sorted comma-list of those distress_types
  - is_stacked: signal_count >= 2

Output written to phone_metadata.distress_stack (existing JSONB
column, no schema change).

Distress type: N/A — utility enricher.
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from ._base import BotBase, _supabase


def _norm_address(address: Optional[str]) -> Optional[str]:
    """Normalize an address string to a deduplication key.

    Lowercase, collapse whitespace, drop punctuation, drop apartment/
    suite designators, drop trailing TN+zip.
    """
    if not address:
        return None
    s = address.lower()
    s = re.sub(r"\s+", " ", s).strip()
    # Drop common suffixes/punctuation
    s = re.sub(r"[,.]", " ", s)
    s = re.sub(r"\s+(apt|unit|suite|ste|#)\s*\S+", "", s)
    s = re.sub(r"\s+tn\s+\d{5}(-\d{4})?$", "", s)
    s = re.sub(r"\s+tn\s*$", "", s)
    s = re.sub(r"\s+\d{5}(-\d{4})?$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


class StackedDistressAggregatorBot(BotBase):
    name = "stacked_distress_aggregator"
    description = "Tag leads with the count + set of distress signals observed at their property address"
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
                    "stacked": 0, "tagged": 0, "skipped": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        tagged = 0
        stacked = 0
        skipped = 0
        error_message: Optional[str] = None

        try:
            # Step 1: build {addr_key: set(distress_types)} across both tables
            address_signals: Dict[str, Set[str]] = defaultdict(set)
            all_rows: List[Tuple[str, Dict[str, Any]]] = []

            for table in ("homeowner_requests", "homeowner_requests_staging"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} rows scanned")
                for r in rows:
                    addr_key = _norm_address(r.get("property_address"))
                    if not addr_key:
                        continue
                    dt = (r.get("distress_type") or "").strip().upper()
                    if dt:
                        address_signals[addr_key].add(dt)
                    all_rows.append((table, r))

            self.logger.info(
                f"observed {len(address_signals)} unique addresses; "
                f"{sum(1 for s in address_signals.values() if len(s) >= 2)} stacked"
            )

            # Step 2: write stack metadata back to every row
            for table, r in all_rows[: self.max_leads_per_run]:
                addr_key = _norm_address(r.get("property_address"))
                if not addr_key:
                    skipped += 1
                    continue
                signals = sorted(address_signals.get(addr_key, set()))
                stack_meta = {
                    "signal_count": len(signals),
                    "signal_types": signals,
                    "is_stacked": len(signals) >= 2,
                }

                existing_meta = r.get("phone_metadata") or {}
                if not isinstance(existing_meta, dict):
                    existing_meta = {}
                # Idempotent — skip if same
                if existing_meta.get("distress_stack") == stack_meta:
                    skipped += 1
                    continue
                existing_meta["distress_stack"] = stack_meta

                try:
                    client.table(table).update({
                        "phone_metadata": existing_meta,
                    }).eq("id", r["id"]).execute()
                    tagged += 1
                    if stack_meta["is_stacked"]:
                        stacked += 1
                except Exception as e:
                    self.logger.warning(f"  update failed id={r['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif tagged == 0 and skipped == 0:
            status = "zero_yield"
        elif tagged == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=tagged + skipped,
            parsed_count=tagged + skipped,
            staged_count=tagged, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(f"tagged={tagged} stacked={stacked} skipped={skipped}")
        return {
            "name": self.name, "status": status,
            "tagged": tagged, "stacked": stacked, "skipped": skipped,
            "error": error_message,
            "staged": tagged, "duplicates": skipped,
            "fetched": tagged + skipped,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        try:
            q = (
                client.table(table)
                .select("id, property_address, distress_type, phone_metadata")
                .not_.is_("property_address", "null")
                .limit(2500)
                .execute()
            )
            return getattr(q, "data", None) or []
        except Exception as e:
            self.logger.warning(f"candidate query on {table} failed: {e}")
            return []


def run() -> dict:
    bot = StackedDistressAggregatorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        for addr in sys.argv[1:]:
            print(f"{addr!r} -> norm: {_norm_address(addr)!r}")
    else:
        print(run())
