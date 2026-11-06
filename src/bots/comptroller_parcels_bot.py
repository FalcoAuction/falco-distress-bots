"""Comptroller parcels: match staged leads in the 86 Comptroller-distributed
counties to their parcel record (owner, owner mailing, property class,
appraisal, acreage, last sale) from the local copy of the state's weekly
county files.

    python -m src.bots.comptroller_parcels_bot --build-index   # after each download
    python -m src.bots.comptroller_parcels_bot --dry-run       # match + report, no writes
    python -m src.bots.comptroller_parcels_bot                 # match + write

Data: data/comptroller/<County>/ASSESSMENT_DATA_##.dbf, downloaded from
comptroller.tn.gov (free, cleared for commercial use, refreshed weekly);
index at data/comptroller/index.sqlite. Without the files the bot reports
no_data and does nothing, so it is safe in CI.

Two matches, written to raw_payload.comptroller_parcel:
  address     leads with a street address; fills property_value only when empty
  owner_name  PROBATE / BANKRUPTCY leads with no address and a known county;
              strict (assessor owner starts with the last name, contains the
              first name, one distinct owner, at most 5 parcels); never sets
              property_value or the address

Distress type: N/A (enricher).
"""
from __future__ import annotations

import argparse
import os
import time
import traceback as tb
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ._base import BotBase, _supabase
from ._comptroller import ParcelIndex, build_index

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "comptroller"
DB_PATH = DATA_DIR / "index.sqlite"
PAGE = 1000
OWNER_MATCH_TYPES = {"PROBATE", "BANKRUPTCY"}
VERSION = "comptroller-parcels-2026-11-05"
FIELDS = ("id,county,property_address,owner_name_records,full_name,distress_type,property_value,"
          "done:raw_payload->comptroller_parcel->>parcelid")


def _index_stale() -> bool:
    if not DB_PATH.exists():
        return True
    newest = max((p.stat().st_mtime for p in DATA_DIR.glob("*/ASSESSMENT_DATA_*.dbf")), default=0)
    return newest > DB_PATH.stat().st_mtime


def _rows(client) -> Iterator[Dict[str, Any]]:
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        for attempt in range(4):
            try:
                page = (client.table("homeowner_requests_staging").select(FIELDS).gt("id", after)
                        .order("id").limit(PAGE).execute().data) or []
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(3 * (attempt + 1))
        if not page:
            return
        after = page[-1]["id"]
        yield from page
        if len(page) < PAGE:
            return
        time.sleep(0.2)


class ComptrollerParcelsBot(BotBase):
    name = "comptroller_parcels"
    description = "Match leads to TN Comptroller parcel records (86 counties): owner, mailing, class, value"

    def __init__(self, write: bool = True, refresh: bool = False):
        super().__init__()
        self.write = write
        self.refresh = refresh

    def scrape(self) -> List[Any]:
        return []

    def match_all(self, client, idx: ParcelIndex) -> Dict[str, Any]:
        stats: Counter = Counter()
        updates: List[Dict[str, Any]] = []
        for row in _rows(client):
            stats["scanned"] += 1
            if row.get("done") and not self.refresh:
                stats["already_matched"] += 1
                continue
            hit: Optional[Dict[str, Any]] = None
            addr = row.get("property_address")
            if addr:
                if idx.resolve_county(row.get("county"), addr):
                    stats["in_coverage_with_address"] += 1
                    hit = idx.match_address(addr, row.get("county"))
            elif (row.get("distress_type") or "").upper() in OWNER_MATCH_TYPES and row.get("county"):
                if idx.resolve_county(row.get("county"), None):
                    stats["in_coverage_owner_only"] += 1
                    hit = idx.match_owner(row.get("owner_name_records") or row.get("full_name"), row.get("county"))
            if not hit:
                continue
            hit["version"] = VERSION
            stats[f"matched_{hit['method']}"] += 1
            stats[f"class_{(hit.get('class') or '?')[:2]}"] += 1
            updates.append({"id": row["id"], "hit": hit,
                            "value": hit.get("appraisal") if hit["method"] == "address" and not row.get("property_value")
                            and (hit.get("appraisal") or 0) > 0 else None})
        return {"stats": stats, "updates": updates}

    def apply(self, client, updates: List[Dict[str, Any]]) -> int:
        """Re-read raw_payload right before each write so a concurrent enricher's
        write is not clobbered."""
        written = 0
        for u in updates:
            try:
                cur = (client.table("homeowner_requests_staging").select("raw_payload,property_value")
                       .eq("id", u["id"]).limit(1).execute().data) or []
                if not cur:
                    continue
                raw = cur[0].get("raw_payload") or {}
                if not isinstance(raw, dict):
                    raw = {}
                raw["comptroller_parcel"] = u["hit"]
                upd: Dict[str, Any] = {"raw_payload": raw}
                if u["value"] and not cur[0].get("property_value"):
                    upd["property_value"] = int(round(u["value"]))
                    upd["property_value_source"] = "comptroller"
                    upd["property_value_as_of"] = datetime.now(timezone.utc).isoformat()
                client.table("homeowner_requests_staging").update(upd).eq("id", u["id"]).execute()
                written += 1
            except Exception as e:
                self.logger.warning(f"  write failed id={u['id']}: {e}")
        return written

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        if not any(DATA_DIR.glob("*/ASSESSMENT_DATA_*.dbf")):
            self._report_health(status="zero_yield", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_data: download county files to data/comptroller/")
            return {"name": self.name, "status": "no_data"}
        client = _supabase()
        if client is None:
            self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_supabase_client")
            return {"name": self.name, "status": "no_supabase"}
        error_message = None
        result: Dict[str, Any] = {"stats": Counter(), "updates": []}
        written = 0
        try:
            if _index_stale():
                self.logger.info("building Comptroller index")
                build_index(str(DATA_DIR), str(DB_PATH))
            idx = ParcelIndex(str(DB_PATH))
            result = self.match_all(client, idx)
            if self.write:
                written = self.apply(client, result["updates"])
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        stats = result["stats"]
        matched = len(result["updates"])
        status = "failed" if error_message else "ok" if matched else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=stats.get("scanned", 0), parsed_count=matched, staged_count=written,
                            duplicate_count=stats.get("already_matched", 0), error_message=error_message)
        return {"name": self.name, "status": status, "matched": matched, "written": written,
                "stats": dict(stats), "error": error_message}


def run() -> dict:
    return ComptrollerParcelsBot(write=True).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Match leads to TN Comptroller parcel records")
    ap.add_argument("--build-index", action="store_true", help="rebuild data/comptroller/index.sqlite and exit")
    ap.add_argument("--dry-run", action="store_true", help="match and report without writing")
    ap.add_argument("--refresh", action="store_true", help="re-match rows that already have a parcel")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(ROOT / ".env", override=False)
    except ImportError:
        pass
    if args.build_index:
        t = time.time()
        counts = build_index(str(DATA_DIR), str(DB_PATH))
        print(f"indexed {sum(counts.values()):,} parcels in {len(counts)} counties in {time.time() - t:.0f}s")
    else:
        print(ComptrollerParcelsBot(write=not args.dry_run, refresh=args.refresh).run())
