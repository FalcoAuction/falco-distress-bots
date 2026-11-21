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
import json
import os
import re
import time
import traceback as tb
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ._base import BotBase, _supabase
from ._bk_match import district_counties, match_debtors, situs_address
from ._comptroller import ParcelIndex, build_index, _record

ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT / "data" / "comptroller"
DB_PATH = DATA_DIR / "index.sqlite"
PAGE = 1000
OWNER_MATCH_TYPES = {"PROBATE", "BANKRUPTCY"}
# Bankruptcy leads never carry a county or address; the court's district and
# the debtor names (spouses filing jointly confirm each other) find the home.
BK_TYPES = {"BANKRUPTCY", "BK_DISMISSED", "BK_DISMISSAL_MOTION", "BK_STAY_RELIEF", "BK_ABANDONED"}
# Debtor matches strong enough to set the lead's address and county.
BK_TRUSTED = {"joint", "middle_initial"}
# Debtor searches per run (~0.1s each live, more for common surnames), so
# the ~12k backlog clears over a few runs inside the bot's time cap.
MAX_DEBTOR_SEARCHES = 1500
# Bankruptcy events (dismissals, stay relief, abandonment) free the house
# and are the ones the CRM ranks; new filings are not ranked at all. Events
# are searched first so the filing backlog never crowds them out.
BK_FILING = "BANKRUPTCY"
# Foreclosure notices that name the borrowers but not the property (the
# tnpublicnotice excerpt cuts off before the address; the full text sits
# behind a captcha we do not get past). The notice names the county, so
# the same debtor matcher finds the home: both spouses on one deed
# ("joint") or a confirmed middle initial set the address and value; a
# bare name match is kept for review only.
NOTICE_TYPES = {"PRE_FORECLOSURE", "FORECLOSURE", "TRUSTEE_NOTICE"}
MAX_NOTICE_SEARCHES = 3000
# Leads already searched against this build of the index; cached with the
# index and cleared when the index changes, so new deeds get another look.
TRIED_PATH = DATA_DIR / "debtor_tried.json"


def _load_tried() -> set:
    try:
        d = json.loads(TRIED_PATH.read_text(encoding="utf-8"))
        if DB_PATH.exists() and d.get("index_mtime") == DB_PATH.stat().st_mtime:
            return set(d.get("ids") or [])
    except (OSError, ValueError):
        pass
    return set()


def _save_tried(ids: set) -> None:
    if not DB_PATH.exists():
        return
    tmp = TRIED_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps({"index_mtime": DB_PATH.stat().st_mtime, "ids": sorted(ids)}), encoding="utf-8")
    os.replace(tmp, TRIED_PATH)
VERSION = "comptroller-parcels-2026-11-05"
FIELDS = ("id,county,property_address,owner_name_records,full_name,distress_type,property_value,"
          "done:raw_payload->comptroller_parcel->>parcelid,court:raw_payload->>court_id,"
          "state:raw_payload->>state,borrowers:raw_payload->borrowers,bot_source,printed_parcel:raw_payload->>parcel,tax_name_context:raw_payload->>tax_name_context")


def dickson_tax_index(con):
    """Index Dickson roll numbers in the newspaper format, including special interests."""
    result = {}
    for parcel in con.execute("SELECT * FROM parcels WHERE county='DICKSON'"):
        m = re.fullmatch(r"022 (\d{3})([A-Z]?) +([A-Z]?) +(\d{3})(\d{2}) (\d{3}) \d{4}", parcel["parcelid"])
        if not m:
            continue
        sheet, block, group, whole, cents, interest = m.groups()
        key = "-".join(x for x in (str(int(sheet)), block + group, f"{int(whole)}.{cents}") if x)
        if interest != "000":
            key += " " + interest
        # A duplicate on the roll needs manual review.
        result[key] = None if key in result else parcel
    return result


def dickson_tax_hit(index, printed, name_context=""):
    parcel = index.get(printed)
    split = re.fullmatch(r"(\d+)([A-Z])/(\d+)([A-Z])-([A-Z])-(\d+\.\d{2})", printed or "")
    if split:
        # Both sheets can contain the printed lot. Require an adjacent owner name
        # from this notice and exactly one matching candidate across both sheets.
        a, b, c, d, group, lot = split.groups()
        matches = []
        context = re.sub(r"[^A-Z ]", " ", (name_context or "").upper()).split()
        while context and context[-1] in {"ETAL", "ET", "AL"}:
            context.pop()
        for key in {f"{a}-{b}{group}-{lot}", f"{c}-{d}{group}-{lot}"}:
            candidate = index.get(key)
            if candidate is None:
                continue
            words = re.findall(r"[A-Z]+", (candidate["owner"] or "").upper())
            if len(words) >= 2 and len(context) >= 2 and context[-1] == words[0] and (
                    context[-2] == words[1] or (len(context) >= 3 and len(words) >= 3
                    and context[-3] == words[1] and context[-2] == words[2])):
                matches.append(candidate)
        parcel = matches[0] if len(matches) == 1 else None
    if parcel is None:
        return None
    return {**_record(parcel), "method": "dickson_tax_parcel", "county": "DICKSON",
            "situs_address": situs_address(parcel), "printed_parcel": printed,
            "source_url": "https://comptroller.tn.gov/content/dam/cot/pa/documents/landuse/maps/parcel-maps-data/Dickson.zip"}


def wilson_tax_hit(con, printed):
    m = re.fullmatch(r"(\d{1,3})([A-Z]?)(?:-([A-Z]))?-(\d{1,3})\.(\d{2})", printed)
    if not m:
        return None
    sheet, block, group, lot, cents = m.groups()
    prefix = f"095 {int(sheet):03}{block or ' '} {group or ' '} {int(lot):03}{cents} "
    rows = con.execute("SELECT * FROM parcels WHERE county='WILSON' AND parcelid LIKE ? LIMIT 2",
                       (prefix + "%",)).fetchall()
    if len(rows) != 1 or not rows[0]["parcelid"].startswith(prefix + "000 "):
        return None
    return {**_record(rows[0]), "method": "wilson_tax_parcel", "county": "WILSON",
            "situs_address": situs_address(rows[0]), "printed_parcel": printed,
            "source_url": "https://comptroller.tn.gov/content/dam/cot/pa/documents/landuse/maps/parcel-maps-data/Wilson.zip"}


def dickson_tax_patch(row, hit):
    patch = {"raw_payload": {**row["raw_payload"], "comptroller_parcel": hit}}
    if not row.get("owner_name_records") and hit.get("owner"):
        patch["owner_name_records"] = hit["owner"]
    if not row.get("property_address") and hit.get("situs_address"):
        patch["property_address"] = hit["situs_address"]
    if not row.get("property_value") and (hit.get("appraisal") or 0) > 0:
        patch.update(property_value=hit["appraisal"], property_value_source="comptroller")
    return patch


def notice_names(row: Dict[str, Any]) -> List[str]:
    """Borrower names from a foreclosure notice row, spouses first-listed."""
    b = row.get("borrowers")
    if isinstance(b, list) and b:
        names = [str(x) for x in b]
    else:
        raw = row.get("owner_name_records") or row.get("full_name") or ""
        names = re.split(r";|\s+and\s+|\s*&\s*", raw, flags=re.IGNORECASE)
    return [n.strip() for n in names if n and n.strip()][:2]


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
        tried = set() if self.refresh else _load_tried()
        # Debtor searches wait until the scan is done, then run events first.
        bk_queue: List[tuple] = []
        notice_queue: List[tuple] = []
        dickson_index = None

        def record(row: Dict[str, Any], hit: Dict[str, Any]) -> None:
            hit["version"] = VERSION
            stats[f"matched_{hit['method']}"] += 1
            stats[f"class_{(hit.get('class') or '?')[:2]}"] += 1
            trusted_bk = hit["method"] == "debtor_name" and hit.get("confidence") in BK_TRUSTED
            updates.append({"id": row["id"], "hit": hit,
                            "value": hit.get("appraisal") if (hit["method"] == "address" or trusted_bk)
                            and not row.get("property_value") and (hit.get("appraisal") or 0) > 0 else None,
                            "address": hit.get("situs_address") if trusted_bk else None,
                            "county": hit["county"].title() if trusted_bk else None})

        for row in _rows(client):
            stats["scanned"] += 1
            if row.get("done") and not self.refresh:
                stats["already_matched"] += 1
                continue
            hit: Optional[Dict[str, Any]] = None
            addr = row.get("property_address")
            if (row.get("bot_source") == "main_street_media" and row.get("distress_type") == "TAX_LIEN"
                    and row.get("county") == "dickson" and row.get("printed_parcel")):
                if dickson_index is None:
                    dickson_index = dickson_tax_index(idx.con)
                hit = dickson_tax_hit(dickson_index, row["printed_parcel"], row.get("tax_name_context"))
                if hit:
                    record(row, hit)
                continue
            if (row.get("county") == "wilson" and row.get("distress_type") == "TAX_LIEN"
                    and row.get("bot_source") == "main_street_media" and row.get("printed_parcel")):
                hit = wilson_tax_hit(idx.con, row["printed_parcel"])
                if hit:
                    record(row, hit)
                continue
            if addr:
                if idx.resolve_county(row.get("county"), addr):
                    stats["in_coverage_with_address"] += 1
                    hit = idx.match_address(addr, row.get("county"))
            elif ((row.get("distress_type") or "").upper() in BK_TYPES and not row.get("county")
                  and row.get("court") and (row.get("state") or "TN") == "TN"):
                counties = district_counties(row["court"], idx.counties)
                names = [n.strip() for n in (row.get("owner_name_records") or row.get("full_name") or "").split(";")]
                if row["id"] in tried:
                    stats["bk_already_tried"] += 1
                elif counties and names:
                    is_filing = (row.get("distress_type") or "").upper() == BK_FILING
                    bk_queue.append((1 if is_filing else 0, row, counties, [n for n in names if n]))
            elif ((row.get("distress_type") or "").upper() in NOTICE_TYPES and row.get("county")
                  and (row.get("state") or "TN") == "TN"):
                ck = idx.resolve_county(row.get("county"), None)
                names = notice_names(row)
                if row["id"] in tried:
                    stats["notice_already_tried"] += 1
                elif ck and names:
                    notice_queue.append((row, [ck], names))
            elif (row.get("distress_type") or "").upper() in OWNER_MATCH_TYPES and row.get("county"):
                if idx.resolve_county(row.get("county"), None):
                    stats["in_coverage_owner_only"] += 1
                    hit = idx.match_owner(row.get("owner_name_records") or row.get("full_name"), row.get("county"))
            if hit:
                record(row, hit)

        # Events first (priority 0), then filings; stable sort keeps scan order.
        bk_queue.sort(key=lambda q: q[0])
        for n, (prio, row, counties, names) in enumerate(bk_queue):
            if n >= MAX_DEBTOR_SEARCHES:
                stats["bk_deferred"] += len(bk_queue) - n
                stats["bk_events_deferred"] += sum(1 for q in bk_queue[n:] if q[0] == 0)
                break
            stats["bk_debtor_search"] += 1
            if prio == 0:
                stats["bk_event_search"] += 1
            hit = match_debtors(idx.con, names, counties)
            if hit:
                stats[f"bk_{hit['confidence']}"] += 1
                if prio == 0:
                    stats["bk_event_matched"] += 1
                record(row, hit)
            else:
                tried.add(row["id"])
        # Many rows are the same notice published three times: search each
        # borrower set once per county.
        seen: Dict[tuple, Optional[Dict[str, Any]]] = {}
        for n, (row, counties, names) in enumerate(notice_queue):
            key = (counties[0], tuple(sorted(x.upper() for x in names)))
            if key not in seen:
                if len(seen) >= MAX_NOTICE_SEARCHES:
                    stats["notice_deferred"] += len(notice_queue) - n
                    break
                stats["notice_search"] += 1
                seen[key] = match_debtors(idx.con, names, counties)
            hit = seen[key]
            if hit:
                stats[f"notice_{hit['confidence']}"] += 1
                record(row, dict(hit))
            else:
                tried.add(row["id"])
        if self.write:
            _save_tried(tried)
        return {"stats": stats, "updates": updates}

    def apply(self, client, updates: List[Dict[str, Any]]) -> int:
        """Re-read raw_payload right before each write so a concurrent enricher's
        write is not clobbered."""
        written = 0
        for u in updates:
            try:
                if u["hit"].get("method") in ("dickson_tax_parcel", "wilson_tax_parcel"):
                    from .free_county_completion_bot import apply
                    rows = (client.table("homeowner_requests_staging")
                            .select("id,raw_payload,owner_name_records,property_address,property_value,property_value_source")
                            .eq("id", u["id"]).eq("bot_source", "main_street_media").eq("county", u["hit"]["county"].lower())
                            .eq("distress_type", "TAX_LIEN").limit(1).execute().data or [])
                    if rows and rows[0]["raw_payload"].get("parcel") == u["hit"]["printed_parcel"]:
                        patch = dickson_tax_patch(rows[0], u["hit"])
                        written += bool(apply(client, "homeowner_requests_staging", rows[0], patch))
                    continue
                cur = (client.table("homeowner_requests_staging").select("raw_payload,property_value,property_address")
                       .eq("id", u["id"]).limit(1).execute().data) or []
                if not cur:
                    continue
                raw = cur[0].get("raw_payload") or {}
                if not isinstance(raw, dict):
                    raw = {}
                raw["comptroller_parcel"] = u["hit"]
                upd: Dict[str, Any] = {"raw_payload": raw}
                if u.get("address") and not cur[0].get("property_address"):
                    upd["property_address"] = u["address"]
                    upd["county"] = u["county"]
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
        if not DB_PATH.exists() and not any(DATA_DIR.glob("*/ASSESSMENT_DATA_*.dbf")):
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
            if any(DATA_DIR.glob("*/ASSESSMENT_DATA_*.dbf")) and _index_stale():
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
