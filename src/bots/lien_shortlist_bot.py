"""Queue today's best Middle Tennessee leads for a county lien check.

Reads the latest ranked list (lead_opportunities), applies the shortlist
screen (src/enrichment/shortlist.py) and marks each chosen staging row with

  raw_payload.lien_check = {status: "queued", queued_at, register_system,
                            priority, days_to_sale, tight}

The Register of Deeds lookups (bot or manual) read that queue and set
status "checked" with checked_at and the lien stack. Free: no external calls.

    python -m src.bots.lien_shortlist_bot            # write
    python -m src.bots.lien_shortlist_bot --dry-run  # counts only
"""

from __future__ import annotations

import argparse
import os
import sys
import traceback as tb
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any, Dict, List

from ._base import BotBase, _supabase
from ..enrichment.shortlist import MIDDLE_TN, county_key, screen

QUEUE_CAP = int(os.environ.get("FALCO_LIEN_QUEUE_CAP", "600") or 600)
ID_BATCH = 200


class LienShortlistBot(BotBase):
    name = "lien_shortlist"
    description = "Queues ranked Middle TN leads for a county Register of Deeds lien check"
    throttle_seconds = 0
    expected_min_yield = 0

    def __init__(self, write: bool = True):
        super().__init__()
        self.write = write

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_supabase_client")
            return {"name": self.name, "status": "no_supabase"}
        error_message = None
        reasons: Counter = Counter()
        by_system: Counter = Counter()
        chosen: List[Dict[str, Any]] = []
        written = 0
        opps: List[Dict[str, Any]] = []
        try:
            day = (client.table("lead_opportunities").select("captured_on")
                   .order("captured_on", desc=True).limit(1).execute().data) or []
            if day:
                opps = (client.table("lead_opportunities")
                        .select("staging_id,county,sale_date,expected_fee,property_value")
                        .eq("captured_on", day[0]["captured_on"]).limit(5000).execute().data) or []
            opps = [o for o in opps if county_key(o.get("county")) in MIDDLE_TN]
            raw_by_id: Dict[str, Dict[str, Any]] = {}
            ids = [o["staging_id"] for o in opps if o.get("staging_id")]
            for i in range(0, len(ids), ID_BATCH):
                rows = (client.table("homeowner_requests_staging").select("id,raw_payload")
                        .in_("id", ids[i:i + ID_BATCH]).execute().data) or []
                for r in rows:
                    raw_by_id[r["id"]] = r.get("raw_payload") or {}
            today = date.today()
            for o in opps:
                ok, detail = screen(o, raw_by_id.get(o["staging_id"]), today)
                if ok:
                    chosen.append({**detail, "staging_id": o["staging_id"]})
                else:
                    reasons[detail["reason"]] += 1
            chosen.sort(key=lambda c: -(c.get("expected_fee") or 0))
            chosen = chosen[:QUEUE_CAP]
            for rank, c in enumerate(chosen, 1):
                by_system[c["register_system"]] += 1
                if self.write:
                    written += self._queue(client, c, rank)
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error_message else "ok" if chosen else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=len(opps), parsed_count=len(chosen), staged_count=written,
                            duplicate_count=reasons.get("checked_recently", 0) + reasons.get("already_queued", 0),
                            error_message=error_message)
        self.logger.info(f"ranked middle TN {len(opps)}, queued {len(chosen)} (written {written}); "
                         f"by register {dict(by_system)}; left out {dict(reasons)}")
        return {"name": self.name, "status": status, "ranked": len(opps), "queued": len(chosen),
                "written": written, "by_register": dict(by_system), "left_out": dict(reasons),
                "error": error_message}

    def _queue(self, client, c: Dict[str, Any], rank: int) -> int:
        """Re-read raw_payload right before the write so a concurrent
        enricher's change is not clobbered."""
        try:
            cur = (client.table("homeowner_requests_staging").select("raw_payload")
                   .eq("id", c["staging_id"]).limit(1).execute().data) or []
            if not cur:
                return 0
            raw = cur[0].get("raw_payload") or {}
            if not isinstance(raw, dict):
                raw = {}
            prior = raw.get("lien_check") or {}
            raw["lien_check"] = {
                **({k: prior[k] for k in ("checked_at", "stack", "equity") if k in prior}),
                "status": "queued",
                "queued_at": datetime.now(timezone.utc).isoformat(),
                "register_system": c["register_system"],
                "priority": rank,
                "days_to_sale": c.get("days_to_sale"),
                "tight": c.get("tight"),
            }
            client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", c["staging_id"]).execute()
            return 1
        except Exception as e:
            self.logger.warning(f"  queue write failed id={c['staging_id']}: {e}")
            return 0


def run() -> dict:
    return LienShortlistBot(write=True).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    print(LienShortlistBot(write=not args.dry_run).run())
    sys.exit(0)
