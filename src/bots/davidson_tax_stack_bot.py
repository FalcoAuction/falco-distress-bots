"""Davidson tax-sale stack: mark Davidson leads whose parcel is on a recent
Clerk and Master delinquent tax-sale list (final revision, last ~15
months). A code-violation strip center that is also headed to tax sale is
a motivated seller; the lane classifier treats this as strong distress.

    python -m src.bots.davidson_tax_stack_bot --dry-run
    python -m src.bots.davidson_tax_stack_bot

Matches on the Metro parcel number already stored by davidson_parcels
(raw_payload.metro_parcel.parcel) or the PADCTN record. Writes
raw_payload.davidson_tax_sale = {sale_date, amount_due, list, version}.
PDFs are cached under data/davidson_tax_sale/ (gitignored).

Distress type: N/A (enricher; adds a stacked signal, never changes
distress_type).
"""
from __future__ import annotations

import argparse
import time
import traceback as tb
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List

from ._base import BotBase, _supabase
from ._davidson_tax_sale import load_tax_sale_parcels

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "data" / "davidson_tax_sale"
PAGE = 1000
VERSION = "davidson-tax-stack-2026-11-05"
FIELDS = ("id,mp:raw_payload->metro_parcel->>parcel,pp:raw_payload->padctn->>parcel,"
          "cur:raw_payload->davidson_tax_sale")


def _rows(client) -> Iterator[Dict[str, Any]]:
    """Davidson rows that carry a parcel number (keyset pages over the two keys)."""
    seen = set()
    for key in ("metro_parcel", "padctn"):
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            for attempt in range(4):
                try:
                    page = (client.table("homeowner_requests_staging").select(FIELDS).gt("id", after)
                            .not_.is_(f"raw_payload->{key}", "null").order("id").limit(PAGE).execute().data) or []
                    break
                except Exception:
                    if attempt == 3:
                        raise
                    time.sleep(3 * (attempt + 1))
            if not page:
                break
            after = page[-1]["id"]
            for r in page:
                if r["id"] not in seen:
                    seen.add(r["id"])
                    yield r
            if len(page) < PAGE:
                break


class DavidsonTaxStackBot(BotBase):
    name = "davidson_tax_stack"
    description = "Stack Davidson Clerk and Master tax-sale lists onto Davidson leads by parcel"

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
        scanned = matched = written = unchanged = 0
        listed = 0
        try:
            parcels = load_tax_sale_parcels(str(CACHE), session=self.session)
            listed = len(parcels)
            self.logger.info(f"{listed} parcels on recent tax-sale lists")
            for row in _rows(client):
                scanned += 1
                pid = (row.get("mp") or row.get("pp") or "").strip().upper()
                hit = parcels.get(pid)
                if not hit:
                    continue
                matched += 1
                tag = dict(hit, version=VERSION)
                if row.get("cur") == tag:
                    unchanged += 1
                    continue
                if not self.write:
                    continue
                try:
                    cur = (client.table("homeowner_requests_staging").select("raw_payload")
                           .eq("id", row["id"]).limit(1).execute().data) or []
                    raw = (cur[0].get("raw_payload") if cur else None) or {}
                    if not isinstance(raw, dict):
                        raw = {}
                    raw["davidson_tax_sale"] = tag
                    client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", row["id"]).execute()
                    written += 1
                except Exception as e:
                    self.logger.warning(f"  write failed id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error_message else "ok" if matched else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=matched, staged_count=written,
                            duplicate_count=unchanged, error_message=error_message)
        return {"name": self.name, "status": status, "parcels_listed": listed, "scanned": scanned,
                "matched": matched, "written": written, "unchanged": unchanged, "error": error_message}


def run() -> dict:
    return DavidsonTaxStackBot(write=True).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Stack Davidson tax-sale lists onto Davidson leads")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
    except ImportError:
        pass
    print(DavidsonTaxStackBot(write=not args.dry_run).run())
