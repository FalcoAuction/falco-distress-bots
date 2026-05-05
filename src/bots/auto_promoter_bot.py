"""Auto-promoter — moves staging leads to live (homeowner_requests)
without manual /admin/staging review.

Eligibility (the "no human review needed" gate — every condition must
pass):
  - county in Middle TN focus
  - owner_name_records OR full_name set
  - property_address set
  - mortgage_balance set AND from a defensible source (ROD-verified,
    HMDA sale-anchored or year-anchored, nashville_ledger extracted,
    or amortized:* — written by mortgage_amortizer)
  - property_value (AVM) set
  - distress_type set (not null)
  - phone set OR will be backfilled tomorrow's 6am skip-trace pass
  - NOT a duplicate of an existing live lead (match by
    pipeline_lead_key OR property_address+owner_name)

For each promotable staging lead:
  1. INSERT into homeowner_requests with translated columns
     (bot_source → source; drop staging-only fields)
  2. UPDATE staging row: staging_status='promoted', reviewed_at=now,
     reviewed_by='auto_promoter'
  3. record_field provenance entry for mortgage_balance with the
     actual data source (rod_lookup, hmda_match, etc.)

Idempotent — re-running skips already-promoted leads.

Run via:
  python -m src.bots.auto_promoter_bot

Env:
  FALCO_AUTO_PROMOTE_SAMPLE  (=1 for dry-run, no writes)
  FALCO_AUTO_PROMOTE_MAX     (default 500)
"""
from __future__ import annotations

import os
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from ._base import BotBase, _supabase
from ._provenance import record_field


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _is_defensible(pm: Dict[str, Any]) -> tuple:
    """Return (defensible: bool, source_label: str) for a phone_metadata blob."""
    if not isinstance(pm, dict):
        return False, ""
    if pm.get("rod_lookup"):
        return True, "ustitlesearch_rod"
    sig = pm.get("mortgage_signal") or {}
    if not isinstance(sig, dict):
        return False, ""
    src = sig.get("source")
    if src == "ustitlesearch_rod":
        return True, "ustitlesearch_rod"
    if src == "nashville_ledger_extracted":
        return True, "nashville_ledger_extracted"
    if src == "hmda_match" and (sig.get("sale_anchored") or sig.get("year_anchored")):
        return True, "hmda_match"
    return False, ""


# Columns shared between staging + live. Translated when promoting.
SHARED_COLUMNS = (
    "pipeline_lead_key", "property_address", "county", "full_name",
    "owner_name_records", "email", "phone", "property_value",
    "mortgage_balance", "trustee_sale_date", "distress_type",
    "admin_notes", "raw_payload", "priority_score", "phone_metadata",
    "alternate_phones",
)


class AutoPromoterBot(BotBase):
    name = "auto_promoter"
    description = (
        "Promotes Middle TN staging leads to live homeowner_requests "
        "without manual review when defensible-mortgage gate passes."
    )
    throttle_seconds = 0
    expected_min_yield = 0
    max_leads_per_run = 500

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
            return self._fail(started, "no_supabase_client")

        sample = os.environ.get("FALCO_AUTO_PROMOTE_SAMPLE") == "1"
        max_per_run = int(
            os.environ.get("FALCO_AUTO_PROMOTE_MAX", self.max_leads_per_run)
        )

        # Build the set of pipeline_lead_keys + (address+owner) tuples
        # already in live, to skip duplicates.
        live_keys: Set[str] = set()
        live_addr_owner: Set[tuple] = set()
        try:
            page = 0
            while True:
                r = client.table("homeowner_requests").select(
                    "pipeline_lead_key, property_address, owner_name_records, full_name"
                ).range(page * 1000, (page + 1) * 1000 - 1).execute()
                rows = r.data or []
                if not rows:
                    break
                for row in rows:
                    if row.get("pipeline_lead_key"):
                        live_keys.add(row["pipeline_lead_key"])
                    addr = (row.get("property_address") or "").strip().lower()
                    name = ((row.get("owner_name_records") or row.get("full_name") or "")
                             .strip().lower())
                    if addr and name:
                        live_addr_owner.add((addr, name))
                if len(rows) < 1000:
                    break
                page += 1
        except Exception as e:
            self.logger.warning(f"could not load live key index: {e}")

        self.logger.info(
            f"live key index: {len(live_keys)} pipeline_lead_keys, "
            f"{len(live_addr_owner)} addr+owner pairs"
        )

        # Pull eligible staging candidates
        promoted = 0
        skipped_not_defensible = 0
        skipped_missing_field = 0
        skipped_dup = 0
        skipped_already_promoted = 0
        errors = 0
        attempted = 0

        page = 0
        while True:
            try:
                r = (
                    client.table("homeowner_requests_staging")
                    .select("*")
                    .eq("staging_status", "pending")
                    .range(page * 1000, (page + 1) * 1000 - 1)
                    .execute()
                )
            except Exception as e:
                self.logger.warning(f"staging query page {page}: {e}")
                break
            rows = getattr(r, "data", None) or []
            if not rows:
                break
            for row in rows:
                if attempted >= max_per_run:
                    break
                # Eligibility checks
                if _normalize_county(row.get("county")) not in FOCUS_COUNTIES:
                    continue
                if not (row.get("owner_name_records") or row.get("full_name")):
                    skipped_missing_field += 1
                    continue
                if not row.get("property_address"):
                    skipped_missing_field += 1
                    continue
                if not row.get("property_value"):
                    skipped_missing_field += 1
                    continue
                if not row.get("mortgage_balance"):
                    skipped_missing_field += 1
                    continue
                if not row.get("distress_type"):
                    skipped_missing_field += 1
                    continue
                pm = row.get("phone_metadata") or {}
                defensible, source_label = _is_defensible(pm)
                if not defensible:
                    skipped_not_defensible += 1
                    continue
                attempted += 1

                # Duplicate check
                key = row.get("pipeline_lead_key")
                if key and key in live_keys:
                    skipped_already_promoted += 1
                    continue
                addr = (row.get("property_address") or "").strip().lower()
                name = ((row.get("owner_name_records") or row.get("full_name") or "")
                         .strip().lower())
                if (addr, name) in live_addr_owner:
                    skipped_dup += 1
                    continue

                # Build live row. Live table types differ from staging
                # for monetary fields — coerce float → int.
                live_row: Dict[str, Any] = {}
                INT_FIELDS = {"property_value", "mortgage_balance"}
                for col in SHARED_COLUMNS:
                    val = row.get(col)
                    if val is None:
                        continue
                    if col in INT_FIELDS and isinstance(val, (int, float)):
                        live_row[col] = int(round(float(val)))
                    else:
                        live_row[col] = val
                # Live table's `source` column has a CHECK constraint
                # accepting 'bot' for all automated ingestion. The
                # specific scraper that found the lead is preserved in
                # staging.bot_source and in admin_notes.
                live_row["source"] = "bot"
                # Track origin scraper in admin_notes if not already there
                if row.get("bot_source"):
                    existing_notes = live_row.get("admin_notes") or ""
                    bot_tag = f"bot_source={row['bot_source']}"
                    if bot_tag not in existing_notes:
                        live_row["admin_notes"] = (
                            existing_notes + " · " if existing_notes else ""
                        ) + bot_tag

                if sample:
                    self.logger.info(
                        f"  SAMPLE would promote id={row['id'][:8]} "
                        f"county={row.get('county')} "
                        f"name={(row.get('owner_name_records') or row.get('full_name'))[:30]} "
                        f"src={source_label} mort=${row.get('mortgage_balance'):,.0f}"
                    )
                    promoted += 1
                    continue

                # Insert into live
                try:
                    client.table("homeowner_requests").insert(live_row).execute()
                except Exception as e:
                    self.logger.warning(
                        f"  insert failed id={row['id']}: {e}"
                    )
                    errors += 1
                    continue

                # Mark staging row promoted
                try:
                    client.table("homeowner_requests_staging").update({
                        "staging_status": "promoted",
                        "reviewed_at": datetime.now(timezone.utc).isoformat(),
                        "reviewed_by": "auto_promoter",
                    }).eq("id", row["id"]).execute()
                except Exception as e:
                    self.logger.warning(
                        f"  staging status update failed id={row['id']}: {e}"
                    )

                # Update local index so subsequent rows in same run dedupe
                if key:
                    live_keys.add(key)
                if addr and name:
                    live_addr_owner.add((addr, name))

                # Provenance for mortgage_balance on live row
                try:
                    record_field(
                        client, row["id"], "mortgage_balance",
                        int(row["mortgage_balance"]),
                        source_label,
                        confidence=0.85 if "rod" in source_label else 0.65,
                        metadata={"promoted_via": "auto_promoter"},
                    )
                except Exception:
                    pass

                promoted += 1

            if len(rows) < 1000 or attempted >= max_per_run:
                break
            page += 1

        self.logger.info(
            f"attempted={attempted} promoted={promoted} "
            f"not_defensible={skipped_not_defensible} "
            f"missing_field={skipped_missing_field} "
            f"already_in_live={skipped_already_promoted} "
            f"dup_addr={skipped_dup} errors={errors}"
        )
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="ok", started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=promoted,
            staged_count=promoted, duplicate_count=skipped_dup,
        )
        return {
            "name": self.name, "status": "ok",
            "attempted": attempted, "promoted": promoted,
            "not_defensible": skipped_not_defensible,
            "missing_field": skipped_missing_field,
            "already_in_live": skipped_already_promoted,
            "dup_addr_owner": skipped_dup, "errors": errors,
            "fetched": attempted, "staged": promoted,
            "duplicates": skipped_dup,
        }

    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="failed", started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {
            "name": self.name, "status": "failed", "error": msg,
            "fetched": 0, "staged": 0, "duplicates": 0,
        }


def run() -> dict:
    bot = AutoPromoterBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
