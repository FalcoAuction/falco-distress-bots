"""Continuance reaper — re-activate REJECT_FORECLOSED leads when their
trustee_sale_date moves into the future.

TN trustee sales get postponed ("continued") often. The original notice
publishes with sale date X. The trustee then republishes with a new
date Y > X. Our ingestion picks up the new notice, updates the row's
trustee_sale_date to Y, but the row's priority_score is already 0 from
when it was REJECT_FORECLOSED on the past date X. Without intervention
the lead stays dead.

This bot walks REJECT_FORECLOSED leads, checks if trustee_sale_date is
now >= today, and clears priority_score so the decision_engine re-grades
on the next run.

Distress type: N/A (utility enricher).
"""
from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timezone, date
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase


class ContinuanceReaperBot(BotBase):
    name = "continuance_reaper"
    description = "Re-grade REJECT_FORECLOSED leads when trustee_sale_date returns to future"
    throttle_seconds = 0.0
    expected_min_yield = 0  # may legitimately have nothing to do

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
                    "reaped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        reaped = 0
        scanned = 0
        error_message: Optional[str] = None
        today_iso = date.today().isoformat()

        try:
            for table in ("homeowner_requests_staging", "homeowner_requests"):
                rows = self._candidates(client, table)
                self.logger.info(f"{table}: {len(rows)} REJECT_FORECLOSED candidates")
                for row in rows:
                    scanned += 1
                    ts = row.get("trustee_sale_date")
                    if not ts:
                        continue
                    try:
                        ts_d = datetime.fromisoformat(str(ts)[:10]).date()
                    except (ValueError, TypeError):
                        continue
                    if ts_d < date.today():
                        continue  # still past — leave rejected

                    # Clear priority_score so decision_engine re-grades.
                    # Don't clear phone_metadata.decision_engine — keep
                    # the audit trail that this was once REJECT_FORECLOSED.
                    pm = row.get("phone_metadata") or {}
                    if isinstance(pm, dict):
                        de = pm.get("decision_engine") or {}
                        de["continuance_reaped_at"] = datetime.now(timezone.utc).isoformat()
                        de["continuance_previous_action"] = de.get("action")
                        pm["decision_engine"] = de
                    try:
                        client.table(table).update({
                            "priority_score": None,
                            "phone_metadata": pm,
                        }).eq("id", row["id"]).execute()
                        reaped += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed id={row['id']}: {e}")

        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        else:
            status = "ok"  # zero reaps is fine — nothing to continue

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=scanned,
            parsed_count=scanned,
            staged_count=reaped, duplicate_count=0,
            error_message=error_message,
        )
        self.logger.info(f"reaped={reaped} scanned={scanned}")
        return {
            "name": self.name, "status": status,
            "reaped": reaped, "scanned": scanned,
            "error": error_message,
            "staged": reaped, "duplicates": 0,
            "fetched": scanned,
        }

    def _candidates(self, client, table: str) -> List[Dict[str, Any]]:
        # Pull REJECT_FORECLOSED leads (priority_score = 0 with that
        # specific decision_engine.action). Paginate around PostgREST cap.
        out = []
        PAGE_SIZE = 1000
        MAX_PAGES = 10
        for page in range(MAX_PAGES):
            try:
                # Filter by phone_metadata->decision_engine->>action via
                # PostgREST -> notation. Falls back to client-side filter
                # if the operator chain isn't supported.
                q = (
                    client.table(table)
                    .select("id, trustee_sale_date, priority_score, phone_metadata")
                    .eq("priority_score", 0)
                    .order("id")
                    .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                # Client-side filter for REJECT_FORECLOSED action
                for r in rows:
                    pm = r.get("phone_metadata") or {}
                    if not isinstance(pm, dict):
                        continue
                    de = pm.get("decision_engine") or {}
                    if isinstance(de, dict) and de.get("action") == "REJECT_FORECLOSED":
                        out.append(r)
                if len(rows) < PAGE_SIZE:
                    break
            except Exception as e:
                self.logger.warning(f"candidate query on {table} page {page} failed: {e}")
                break
        return out


def run() -> dict:
    bot = ContinuanceReaperBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
