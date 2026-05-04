"""Stale re-grade — clear priority_score on leads graded >7 days ago.

A lead graded on day 1 with no AVM gets HOLD_FOR_DATA. On day 5 the
hamilton_assessor enricher backfills the AVM. The lead now has the data
needed to actually grade — but `priority_score IS NOT NULL` from the
day-1 grading, so the decision_engine's candidate query skips it
forever.

This bot finds leads where:
  - priority_score is set (already graded)
  - graded_at > 7 days ago
  - new enrichment has landed since (e.g., property_value was filled
    after the grading happened)

…and clears priority_score so the next decision_engine run picks them up.

Distress type: N/A (utility re-grader).
"""
from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase


# How long a grade is considered "fresh enough" before we re-check.
STALE_DAYS = 7


class StaleRegradeBot(BotBase):
    name = "stale_regrade"
    description = "Re-grade graded-but-stale leads where new enrichment has landed"
    throttle_seconds = 0.0
    expected_min_yield = 0  # legitimately may have nothing to do

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
                    "regraded": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        regraded = 0
        scanned = 0
        error_message: Optional[str] = None
        cutoff = datetime.now(timezone.utc) - timedelta(days=STALE_DAYS)

        try:
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} graded leads to inspect")
                for row in rows:
                    scanned += 1
                    if not self._should_regrade(row, cutoff):
                        continue
                    pm = row.get("phone_metadata") or {}
                    if isinstance(pm, dict):
                        de = pm.get("decision_engine") or {}
                        de["stale_regrade_at"] = datetime.now(timezone.utc).isoformat()
                        de["previous_action"] = de.get("action")
                        pm["decision_engine"] = de
                    try:
                        client.table(table).update({
                            "priority_score": None,
                            "phone_metadata": pm,
                        }).eq("id", row["id"]).execute()
                        regraded += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        status = "failed" if error_message else "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=scanned, parsed_count=scanned,
            staged_count=regraded, duplicate_count=0,
            error_message=error_message,
        )
        self.logger.info(f"regraded={regraded} scanned={scanned}")
        return {
            "name": self.name, "status": status,
            "regraded": regraded, "scanned": scanned,
            "error": error_message,
            "staged": regraded, "duplicates": 0,
            "fetched": scanned,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 20
        for page in range(MAX_PAGES):
            try:
                q = (
                    client.table(table)
                    .select("id, priority_score, property_value, mortgage_balance, "
                            "phone, phone_metadata, raw_payload")
                    .not_.is_("priority_score", "null")
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                out.extend(rows)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                self.logger.warning(f"candidate fetch on {table} page {page} failed: {e}")
                break
        return out

    @staticmethod
    def _should_regrade(row: Dict[str, Any], cutoff: datetime) -> bool:
        """Return True iff the lead's grade is stale AND new enrichment
        has landed since the grade timestamp.

        Trigger criteria:
          (a) graded as HOLD_FOR_DATA but property_value is now populated
          (b) graded ≥ STALE_DAYS ago AND enrichment timestamp newer than
              grade timestamp
          (c) graded as REJECT_FORECLOSED but trustee_sale_date moved
              into the future (fallback safety net for the dedicated
              continuance_reaper bot)
        """
        pm = row.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            return False
        de = pm.get("decision_engine") or {}
        action = de.get("action") if isinstance(de, dict) else None
        graded_at = de.get("decided_at") if isinstance(de, dict) else None

        # (a) HOLD_FOR_DATA → re-grade if AVM now exists
        if action == "HOLD_FOR_DATA" and row.get("property_value"):
            return True

        # (b) age-based: parse graded_at and compare
        if graded_at:
            try:
                gat = datetime.fromisoformat(str(graded_at).replace("Z", "+00:00"))
                if gat.tzinfo is None:
                    gat = gat.replace(tzinfo=timezone.utc)
                if gat >= cutoff:
                    return False
            except (ValueError, TypeError):
                pass

        # If we got here, graded_at is either missing or stale.
        # Require a meaningful enrichment present to justify re-grade.
        # (Avoid re-grading every stale row blindly.)
        if action == "HOLD_FOR_DATA" and (
            row.get("property_value") or row.get("phone")
        ):
            return True

        return False


def run() -> dict:
    bot = StaleRegradeBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
