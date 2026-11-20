"""Re-read stored public-notice excerpts with the trustee's sale parser.

tn_public_notice (and tn_lis_pendens, which reuses it) trusted the newspaper's category, so trustee's sales filed
under "Delinquent Taxes" (most of Sumner and Montgomery) were staged as
TAX_LIEN, and only one borrower pattern was read, so most foreclosures were
staged with no name and could never be matched to a house. The bot now
classifies by the text; this pass applies the same reading to rows already
staged:

  - a trustee's sale staged as TAX_LIEN becomes PRE_FORECLOSURE
    (raw_payload.filed_as keeps the old type)
  - borrower names fill full_name / owner_name_records when empty
  - the sale date fills trustee_sale_date when empty
  - the deed of trust book/page/instrument goes to raw_payload.deed_of_trust
  - a county-wide "taxes are due" notice is rejected (it names no property)

Free: reads and writes Supabase only. Idempotent: rows already reparsed
(raw_payload.notice_reparse) are skipped.

    python -m src.bots.notice_reparse_bot            # write
    python -m src.bots.notice_reparse_bot --dry-run  # counts only
"""

from __future__ import annotations

import argparse
import sys
import time
import traceback as tb
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from . import _trustee_notice as tn

LOOKBACK_DAYS = 240
PAGE = 1000
VERSION = 1


def reparse(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """The column/raw_payload update for one staged row, or None."""
    raw = row.get("raw_payload") or {}
    if not isinstance(raw, dict) or (raw.get("notice_reparse") or {}).get("v") == VERSION:
        return None
    excerpt = raw.get("excerpt") or ""
    kind = tn.classify(excerpt)
    upd: Dict[str, Any] = {}
    raw = dict(raw)
    raw["notice_reparse"] = {"v": VERSION, "at": datetime.now(timezone.utc).isoformat(), "kind": kind}
    distress = (row.get("distress_type") or "").upper()
    if kind == "general_tax":
        upd["staging_status"] = "rejected"
        upd["rejection_reason"] = "general_tax_notice"
    elif kind == "foreclosure" and distress not in ("PROBATE",):
        if distress != "PRE_FORECLOSURE":
            raw["filed_as"] = distress
            upd["distress_type"] = "PRE_FORECLOSURE"
        names = tn.borrowers(excerpt)
        # The old single pattern stored stretches of legal wording as names
        # ("Jason Swaw And Kelly Swaw Con- Veying Certain Real Property ...").
        # Replace those; clear them when nothing better is found.
        bad_name = bool(row.get("full_name")) and not tn.plausible_name(row["full_name"])
        bad_owner = bool(row.get("owner_name_records")) and not all(
            tn.plausible_name(n) for n in str(row["owner_name_records"]).split(";") if n.strip())
        if names:
            raw["borrowers"] = names
            if not row.get("full_name") or bad_name:
                upd["full_name"] = names[0]
            if not row.get("owner_name_records") or bad_owner:
                upd["owner_name_records"] = "; ".join(names)
        else:
            if bad_name:
                upd["full_name"] = None
            if bad_owner:
                upd["owner_name_records"] = None
        date = tn.sale_date(excerpt)
        if date and not row.get("trustee_sale_date"):
            upd["trustee_sale_date"] = date
        ref = tn.deed_ref(excerpt)
        if any(ref.values()):
            raw["deed_of_trust"] = ref
    upd["raw_payload"] = raw
    return upd


class NoticeReparseBot(BotBase):
    name = "notice_reparse"
    description = "Re-reads stored public-notice excerpts: trustee sales, borrower names, sale dates"
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
        stats: Counter = Counter()
        error_message = None
        since = (started - timedelta(days=LOOKBACK_DAYS)).isoformat()
        after = "00000000-0000-0000-0000-000000000000"
        try:
            while True:
                page = (client.table("homeowner_requests_staging")
                        .select("id,distress_type,full_name,owner_name_records,trustee_sale_date,raw_payload")
                        .in_("bot_source", ["tn_public_notice", "tn_lis_pendens"]).gte("staged_at", since).gt("id", after)
                        .order("id").limit(PAGE).execute().data) or []
                if not page:
                    break
                after = page[-1]["id"]
                for row in page:
                    stats["scanned"] += 1
                    upd = reparse(row)
                    if upd is None:
                        stats["already"] += 1
                        continue
                    kind = upd["raw_payload"]["notice_reparse"]["kind"] or "other"
                    stats[f"kind_{kind}"] += 1
                    if upd.get("distress_type"):
                        stats["reclassified_to_foreclosure"] += 1
                    if upd.get("full_name"):
                        stats["name_filled"] += 1
                    if upd.get("trustee_sale_date"):
                        stats["sale_date_filled"] += 1
                    if upd.get("staging_status") == "rejected":
                        stats["rejected_general_tax"] += 1
                    if self.write:
                        try:
                            client.table("homeowner_requests_staging").update(upd).eq("id", row["id"]).execute()
                            stats["written"] += 1
                        except Exception as e:
                            stats["write_failed"] += 1
                            self.logger.warning(f"  write failed id={row['id']}: {e}")
                if len(page) < PAGE:
                    break
                time.sleep(0.2)
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error_message else "ok" if stats.get("written") or not self.write else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=stats.get("scanned", 0), parsed_count=stats.get("scanned", 0) - stats.get("already", 0),
                            staged_count=stats.get("written", 0), duplicate_count=stats.get("already", 0),
                            error_message=error_message)
        self.logger.info(dict(stats))
        return {"name": self.name, "status": status, "stats": dict(stats), "error": error_message}


def run() -> dict:
    return NoticeReparseBot(write=True).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    print(NoticeReparseBot(write=not ap.parse_args().dry_run).run())
    sys.exit(0)
