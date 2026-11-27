"""Slim placeholders for rejected staging rows.

A rejected row stays in homeowner_requests_staging (some bots read it to
avoid staging the same record twice) but the parts of raw_payload that
nothing reads on a rejected row are removed. The row keeps its id, source,
lead key, status, reason, dates and every plain column.

What may be dropped is decided per source in POLICIES, from a read of every
query in src/ and scripts/ that can return a rejected row. The default is
that a key survives. Today only one block qualifies:

  tn_public_notice  raw_payload.filing keeps version, type, county, key,
                    duplicate_of and republication_of_key. The derived
                    detail (signals, keys, parties, publications and the
                    rest of FILING_DETAIL) is dropped. The only reader of a
                    rejected row's filing is TnPublicNoticeBot._one_per_filing,
                    which selects filing.key and filing.duplicate_of. The
                    detail can be rebuilt from the stored excerpt by
                    _notice_filing.build.

Every other source is in EXCLUDED with the reason. In short: about twenty
enrichers page the whole staging table without a status filter and treat a
missing key as work to do (mortgage_estimator, skip_trace_enricher,
stale_regrade, decision_engine, owner_classifier, property_lane,
comptroller_parcels, free_lien_review, faa_aircraft and others), so removing
one of their keys from a rejected row makes them compute and write it again.
phone_metadata is never changed for the same reason.

Run by hand (dry run is the default, nothing is written):

  python -m src.bots.staging_slim_backfill
  python -m src.bots.staging_slim_backfill --sources tn_public_notice --limit 200
  python -m src.bots.staging_slim_backfill --sources tn_public_notice --write --expect N

A write run saves every affected row in full to a gzip JSON-lines file under
<archive root>/<date>-staging-slim/, reads the file back, and refuses to write
unless the row count and ids match. Each update applies only if the row is
still rejected and unchanged since it was read.

Scheduled: run() is registered in _run_new as "staging_slim". It handles at
most STAGING_SLIM_MAX_ROWS (default 200) rows per run and is a dry run unless
STAGING_SLIM_WRITE=1. On GitHub Actions it also needs
STAGING_SLIM_ARCHIVE_DIR, because the runner's disk is discarded and an
archive written there would be lost.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import quote

from ._base import BotBase, _supabase, guard_phone_metadata

VERSION = "slim-1"
TABLE = "homeowner_requests_staging"
DEFAULT_ARCHIVE_ROOT = "C:/code/falco-workspace/archive"

MAX_PAGE = 400          # rows per read page
MAX_BATCH = 100         # writes between pauses
HYDRATE = 50            # full rows per read
LOOKUP = 100            # keys per lookup in the other tables
PAGE_PAUSE = 1.5
BATCH_PAUSE = 3.0
WRITE_PAUSE = 0.15
# The compare-and-set sends the old raw_payload and phone_metadata in the
# request URL. Rows whose guards would not fit are skipped and counted.
MAX_GUARD_CHARS = 9000

DROP_ALL = "*"

# Derived by _notice_filing.build from the excerpt, which stays on the row.
FILING_DETAIL = ("type_basis", "signals", "filed_under", "county_basis", "county_from_text", "publication_county",
                 "identifiers", "parcels", "parties", "keys", "publications", "party_reason", "county_note")

# source -> column -> top-level key -> DROP_ALL or the sub-keys to drop.
# Anything not named here survives. "probe" is a JSON path that is non-null
# only on rows that still hold something to drop.
POLICIES: Dict[str, Dict[str, Any]] = {
    "tn_public_notice": {
        "raw_payload": {"filing": FILING_DETAIL},
        "phone_metadata": {},
        "probe": "raw_payload->filing->>version",
    },
}

EXCLUDED: Dict[str, str] = {
    "tn_lis_pendens": (
        "No filing block. Every other key is read by a whole-table enricher (excerpt by faa_aircraft and "
        "notice_reparse, lien_check by free_lien_review, comptroller_parcel by comptroller_parcels, and so on), "
        "so nothing can be shown safe to drop."),
    "nashville_codes": (
        "The bot refreshes every stored case whatever its status and reads raw_payload.nashville_codes and "
        "nashville_codes_review; phone_metadata holds paid lookup results that a paid bot would buy again if "
        "they were missing."),
    "hamilton_tax_delinquent": (
        "The payload is the scraped tax row plus enricher results (property_lane, hamilton_roll_valuation, "
        "mortgage_estimate, skip_trace, decision_engine) that whole-table enrichers would write again."),
    "chattanooga_codes": (
        "Rejections are CHATTANOOGA_CASE_INACTIVE or CHATTANOOGA_CASE_REVIEW, which the bot reverses to pending "
        "when the case is active again, and its refresh reads the full chattanooga_codes block."),
}

LIGHT = ("id,pipeline_lead_key,staging_status,reviewed_at,staged_at,phone,email,alternate_phones,"
         "pilot:raw_payload->pilot_review,pretrace:raw_payload->pretrace_check,slim:raw_payload->slim")


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


def _size(value: Any) -> int:
    return 0 if value is None else len(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode("utf-8"))


def _when(value: Any) -> Optional[datetime]:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _has_contact(row: Dict[str, Any]) -> bool:
    return bool(row.get("phone") or row.get("email") or row.get("alternate_phones"))


def skip_reason(row: Dict[str, Any], now: datetime, min_age_days: int) -> Optional[str]:
    """Why this row is left alone, or None. Works on a light row (pilot,
    pretrace and slim as projected columns) and on a full row."""
    if row.get("staging_status") != "rejected":
        return "not_rejected"
    if "raw_payload" in row:
        raw = row["raw_payload"]
        if not isinstance(raw, dict):
            return "payload_not_an_object"
        if "slim" in raw:
            return "already_slim"
        if "pilot_review" in raw or "pretrace_check" in raw:
            return "carded"
        pm = row.get("phone_metadata")
        if pm is not None and not isinstance(pm, dict):
            return "payload_not_an_object"
    else:
        if row.get("slim") is not None:
            return "already_slim"
        if row.get("pilot") is not None or row.get("pretrace") is not None:
            return "carded"
    if _has_contact(row):
        return "has_phone_or_email"
    rejected_at = _when(row.get("reviewed_at") or row.get("staged_at"))
    if rejected_at is None:
        return "no_rejection_date"
    if now - rejected_at <= timedelta(days=min_age_days):
        return "rejected_recently"
    return None


def slim_columns(source: str, raw: Dict[str, Any], pm: Optional[Dict[str, Any]]
                 ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]], List[str]]:
    """(raw_payload, phone_metadata, dropped paths) after the source's policy.
    The inputs are not modified. No policy, nothing dropped."""
    policy = POLICIES.get(source) or {}
    out = {"raw_payload": copy.deepcopy(raw), "phone_metadata": copy.deepcopy(pm)}
    dropped: List[str] = []
    for column in ("raw_payload", "phone_metadata"):
        value = out[column]
        if not isinstance(value, dict):
            continue
        for key, rule in (policy.get(column) or {}).items():
            if key not in value:
                continue
            if rule == DROP_ALL:
                del value[key]
                dropped.append(f"{column}.{key}")
            elif isinstance(value[key], dict):
                for sub in rule:
                    if sub in value[key]:
                        del value[key][sub]
                        dropped.append(f"{column}.{key}.{sub}")
    return out["raw_payload"], out["phone_metadata"], dropped


def plan_row(row: Dict[str, Any], now: datetime, min_age_days: int) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """(plan, None) for a full row that will be slimmed, else (None, reason)."""
    reason = skip_reason(row, now, min_age_days)
    if reason:
        return None, reason
    raw, pm, dropped = slim_columns(row.get("bot_source") or "", row["raw_payload"], row.get("phone_metadata"))
    if not dropped:
        return None, "nothing_to_drop"
    guard = len(quote(_json(row["raw_payload"]), safe="")) + len(quote(_json(row.get("phone_metadata")), safe=""))
    if guard > MAX_GUARD_CHARS:
        return None, "too_large_to_guard"
    before = _size(row["raw_payload"]) + _size(row.get("phone_metadata"))
    return {"id": row["id"], "bot_source": row.get("bot_source"), "pipeline_lead_key": row.get("pipeline_lead_key"),
            "dropped": dropped, "bytes_before": before, "bytes_after": _size(raw) + _size(pm),
            "raw_payload": raw, "phone_metadata": pm, "row": row}, None


# ── Reads ───────────────────────────────────────────────────────────────────


def _chunks(values: List[Any], size: int) -> Iterable[List[Any]]:
    for start in range(0, len(values), size):
        yield values[start:start + size]


def light_rows(client, source: str, cutoff: datetime, page: int, sleep: Callable[[float], None]):
    """Rejected rows of one source that still hold something to drop, light
    columns only, keyset pages by id."""
    page = max(1, min(int(page), MAX_PAGE))
    last = None
    while True:
        q = (client.table(TABLE).select(LIGHT).eq("bot_source", source).eq("staging_status", "rejected")
             .lt("staged_at", cutoff.isoformat()).is_("raw_payload->slim", "null")
             .not_.is_(POLICIES[source]["probe"], "null").order("id").limit(page))
        if last is not None:
            q = q.gt("id", last)
        rows = q.execute().data or []
        yield from rows
        if len(rows) < page:
            return
        last = rows[-1]["id"]
        sleep(PAGE_PAUSE)


def held_elsewhere(client, rows: List[Dict[str, Any]], sleep: Callable[[float], None]) -> Dict[str, str]:
    """id -> reason for rows that the live table, the ranked list or the CRM
    know about. A failed read raises: an unknown is never a clean bill."""
    held: Dict[str, str] = {}
    by_key: Dict[str, List[str]] = {}
    for row in rows:
        if row.get("pipeline_lead_key"):
            by_key.setdefault(row["pipeline_lead_key"], []).append(row["id"])
    keys = sorted(by_key)
    checks = (("homeowner_requests", "pipeline_lead_key", "on_live_table"),
              ("dialer_lead_workflow", "listing_slug", "crm_activity"),
              ("sms_outreach_state", "listing_slug", "crm_activity"),
              ("dialer_activities", "listing_slug", "crm_activity"))
    for table, column, reason in checks:
        for chunk in _chunks(keys, LOOKUP):
            for hit in client.table(table).select(column).in_(column, chunk).execute().data or []:
                for rid in by_key.get(hit.get(column), []):
                    held.setdefault(rid, reason)
            sleep(PAGE_PAUSE / 3)
    for chunk in _chunks([row["id"] for row in rows], LOOKUP):
        for hit in client.table("lead_opportunities").select("staging_id").in_("staging_id", chunk).execute().data or []:
            held.setdefault(hit.get("staging_id"), "ranked_before")
        sleep(PAGE_PAUSE / 3)
    return held


def full_rows(client, source: str, ids: List[str], sleep: Callable[[float], None]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for chunk in _chunks(ids, HYDRATE):
        out += (client.table(TABLE).select("*").eq("bot_source", source).eq("staging_status", "rejected")
                .in_("id", chunk).execute().data or [])
        sleep(PAGE_PAUSE / 3)
    return sorted(out, key=lambda r: str(r["id"]))


def plan_source(client, source: str, *, now: datetime, min_age_days: int, limit: Optional[int], page: int,
                sleep: Callable[[float], None]) -> Dict[str, Any]:
    skipped: Counter = Counter()
    scanned = 0
    candidates: List[Dict[str, Any]] = []
    if limit is not None:
        page = min(page, max(1, limit))     # a capped run never reads a page larger than its cap
    for row in light_rows(client, source, now - timedelta(days=min_age_days), page, sleep):
        scanned += 1
        reason = skip_reason(row, now, min_age_days)
        if reason:
            skipped[reason] += 1
            continue
        candidates.append(row)
        if limit is not None and len(candidates) >= limit:
            break
    held = held_elsewhere(client, candidates, sleep) if candidates else {}
    for reason in held.values():
        skipped[reason] += 1
    plans: List[Dict[str, Any]] = []
    wanted = [row["id"] for row in candidates if row["id"] not in held]
    read = full_rows(client, source, wanted, sleep) if wanted else []
    if len(read) < len(wanted):
        skipped["changed_before_full_read"] += len(wanted) - len(read)
    for row in read:
        plan, reason = plan_row(row, now, min_age_days)
        if plan is None:
            skipped[reason] += 1
        else:
            plans.append(plan)
    return {"source": source, "scanned": scanned, "plans": plans, "skipped": dict(+skipped)}


# ── Archive ─────────────────────────────────────────────────────────────────


def write_archive(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
    os.replace(tmp, path)


def verify_archive(path: Path, rows: List[Dict[str, Any]]) -> bool:
    """Read the file back: same count, same ids, same content."""
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            stored = [json.loads(line) for line in handle if line.strip()]
    except (OSError, ValueError, EOFError):
        return False
    if len(stored) != len(rows) or [r.get("id") for r in stored] != [r.get("id") for r in rows]:
        return False
    return stored == json.loads(json.dumps(rows, ensure_ascii=False))


# ── Writes ──────────────────────────────────────────────────────────────────


def apply_plan(client, plan: Dict[str, Any], marker: Dict[str, Any]) -> bool:
    """Compare-and-set one row. True when the update applied."""
    row = plan["row"]
    patch: Dict[str, Any] = {"raw_payload": {**plan["raw_payload"], "slim": {**marker, "dropped_keys": plan["dropped"]}}}
    if plan["phone_metadata"] != row.get("phone_metadata"):
        patch["phone_metadata"] = plan["phone_metadata"]
    update = (client.table(TABLE).update(patch).eq("id", row["id"]).eq("bot_source", row["bot_source"])
              .eq("staging_status", "rejected").eq("pipeline_lead_key", row["pipeline_lead_key"]))
    for column in ("reviewed_at", "rejection_reason", "phone", "email"):
        value = row.get(column)
        update = update.is_(column, "null") if value is None else update.eq(column, value)
    update = update.filter("raw_payload", "eq", _json(row["raw_payload"]))
    return bool(guard_phone_metadata(update, row.get("phone_metadata")).execute().data)


def execute(client, sources: Iterable[str], *, write: bool = False, expect: Optional[int] = None,
            require_expect: bool = True, min_age_days: int = 30, limit: Optional[int] = None,
            page: int = MAX_PAGE, batch: int = MAX_BATCH, archive_root: Optional[str] = None,
            report_path: Optional[str] = None, write_report: bool = True, now: Optional[datetime] = None,
            sleep: Optional[Callable[[float], None]] = None,
            out: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
    """Plan, report, and (with write) archive then slim. Returns the summary."""
    sleep = sleep or time.sleep
    out = out or print
    now = now or datetime.now(timezone.utc)
    stamp, day = now.strftime("%H%M%S"), now.date().isoformat()
    folder = Path(archive_root or os.environ.get("STAGING_SLIM_ARCHIVE_DIR") or DEFAULT_ARCHIVE_ROOT) / f"{day}-staging-slim"
    batch = max(1, min(int(batch), MAX_BATCH))
    summary: Dict[str, Any] = {"version": VERSION, "mode": "write" if write else "dry_run", "at": now.isoformat(),
                               "min_age_days": min_age_days, "limit": limit, "sources": {}, "excluded": {},
                               "planned": 0, "written": 0, "changed_since_read": 0, "write_failed": 0,
                               "write_blocked": None}
    planned: Dict[str, List[Dict[str, Any]]] = {}
    remaining = limit
    for source in sources:
        if source not in POLICIES:
            summary["excluded"][source] = EXCLUDED.get(source, "No reviewed policy for this source.")
            out(f"{source}: excluded. {summary['excluded'][source]}")
            continue
        if remaining is not None and remaining <= 0:
            break
        result = plan_source(client, source, now=now, min_age_days=min_age_days, limit=remaining, page=page, sleep=sleep)
        plans = result["plans"]
        planned[source] = plans
        keys = Counter(path for p in plans for path in p["dropped"])
        before, after = sum(p["bytes_before"] for p in plans), sum(p["bytes_after"] for p in plans)
        summary["sources"][source] = {"scanned": result["scanned"], "eligible": len(plans), "bytes_before": before,
                                      "bytes_after": after, "keys_dropped": dict(sorted(keys.items())),
                                      "skipped": result["skipped"]}
        summary["planned"] += len(plans)
        if remaining is not None:
            remaining -= len(plans)
        out(f"{source}: rows eligible {len(plans)} (scanned {result['scanned']}), payload bytes before {before:,}, "
            f"after {after:,}, saved {before - after:,}")
        out(f"  keys dropped: {dict(sorted(keys.items())) or 'none'}")
        out(f"  skipped: {result['skipped'] or 'none'}")

    def finish() -> Dict[str, Any]:
        if not write_report:
            return summary
        target = Path(report_path) if report_path else folder / f"plan-{stamp}-{summary['mode']}.json"
        try:
            target.parent.mkdir(parents=True, exist_ok=True)
            body = {**summary, "plan": [{k: p[k] for k in ("id", "bot_source", "pipeline_lead_key", "dropped",
                                                           "bytes_before", "bytes_after")}
                                        for plans in planned.values() for p in plans]}
            tmp = target.with_name(target.name + ".tmp")
            tmp.write_text(json.dumps(body, indent=1), encoding="utf-8")
            os.replace(tmp, target)
            summary["report"] = str(target)
            out(f"report: {target}")
        except OSError as exc:
            summary["report"] = None
            out(f"report not written ({type(exc).__name__})")
        return summary

    out(f"planned {summary['planned']} row(s); mode {summary['mode']}")
    if not write:
        return finish()
    if require_expect and expect != summary["planned"]:
        summary["write_blocked"] = f"--expect {expect} does not match the planned row count {summary['planned']}"
        out("NOT WRITING: " + summary["write_blocked"])
        return finish()
    if not summary["planned"]:
        return finish()

    # Every affected row is on disk, and read back, before the first update.
    archives: Dict[str, str] = {}
    for source, plans in planned.items():
        if not plans:
            continue
        path = folder / f"{source}-rejected-rows-{stamp}.jsonl.gz"
        rows = [p["row"] for p in plans]
        try:
            write_archive(path, rows)
            good = verify_archive(path, rows)
        except OSError:
            good = False
        if not good:
            summary["write_blocked"] = f"archive check failed for {source} ({path.name})"
            out("NOT WRITING: " + summary["write_blocked"])
            return finish()
        archives[source] = path.name
        summary["sources"][source]["archive"] = str(path)
        out(f"archive verified: {path} ({len(rows)} rows)")

    for source, plans in planned.items():
        counts: Counter = Counter()
        marker = {"version": VERSION, "on": day, "archive": archives.get(source)}
        for group in _chunks(plans, batch):
            for plan in group:
                try:
                    counts["written" if apply_plan(client, plan, marker) else "changed_since_read"] += 1
                except Exception as exc:  # noqa: BLE001 - one bad row never stops the batch
                    counts["write_failed"] += 1
                    out(f"  write failed id={plan['id']} ({type(exc).__name__})")
                sleep(WRITE_PAUSE)
            sleep(BATCH_PAUSE)
        for key in ("written", "changed_since_read", "write_failed"):
            summary["sources"][source][key] = counts[key]
            summary[key] += counts[key]
        if plans:
            out(f"{source}: written {counts['written']}, changed since read (skipped) {counts['changed_since_read']}, "
                f"failed {counts['write_failed']}")
    return finish()


# ── Scheduled step ──────────────────────────────────────────────────────────


class StagingSlimBot(BotBase):
    name = "staging_slim"
    description = "Slims rejected staging rows that became eligible (dry run unless STAGING_SLIM_WRITE=1)"
    expected_min_yield = 0

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None, fetched_count=0,
                            parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_supabase_client")
            return {"name": self.name, "status": "no_supabase"}
        write = os.environ.get("STAGING_SLIM_WRITE") == "1"
        notes: Dict[str, Any] = {"write_requested": write}
        # A runner's disk is discarded after the job. Without a directory the
        # owner chose, an archive written there would not survive the run.
        if write and os.environ.get("GITHUB_ACTIONS") and not os.environ.get("STAGING_SLIM_ARCHIVE_DIR"):
            write = False
            notes["write_refused"] = "no durable archive directory (set STAGING_SLIM_ARCHIVE_DIR)"
        try:
            cap = max(1, min(int(os.environ.get("STAGING_SLIM_MAX_ROWS") or 200), 1000))
        except ValueError:
            cap = 200
        error = None
        summary: Dict[str, Any] = {}
        try:
            summary = execute(client, sorted(POLICIES), write=write, require_expect=False, limit=cap,
                              write_report=write, out=self.logger.info)
        except Exception as exc:  # noqa: BLE001
            error = f"{type(exc).__name__}: {exc}"
            self.logger.error("staging slim failed (%s)", type(exc).__name__)
        counts = {key: summary.get(key, 0) for key in ("planned", "written", "changed_since_read", "write_failed")}
        notes.update(counts, mode=summary.get("mode"), write_blocked=summary.get("write_blocked"), cap=cap,
                     bytes_saved=sum(s["bytes_before"] - s["bytes_after"] for s in (summary.get("sources") or {}).values()))
        status = "failed" if error or summary.get("write_blocked") or counts["write_failed"] else "ok"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=counts["planned"], parsed_count=counts["planned"],
                            staged_count=counts["written"], duplicate_count=counts["changed_since_read"],
                            error_message=error or summary.get("write_blocked"), notes=notes)
        return {"name": self.name, "status": status, "fetched": counts["planned"], "staged": counts["written"],
                "duplicates": counts["changed_since_read"], "error": error, **counts}


def run() -> Dict[str, Any]:
    """Entry point for the scheduled runner."""
    return StagingSlimBot().run()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Slim rejected staging rows. Dry run unless --write.")
    parser.add_argument("--sources", default=",".join(sorted(set(POLICIES) | set(EXCLUDED))),
                        help="comma-separated bot_source values (default: every reviewed source)")
    parser.add_argument("--write", action="store_true", help="archive, then slim. Needs --expect")
    parser.add_argument("--expect", type=int, help="the planned row count printed by the dry run")
    parser.add_argument("--min-age-days", type=int, default=30, help="days since rejection (default 30)")
    parser.add_argument("--limit", type=int, help="plan at most this many rows")
    parser.add_argument("--page-size", type=int, default=MAX_PAGE, help=f"read page, at most {MAX_PAGE}")
    parser.add_argument("--batch-size", type=int, default=MAX_BATCH, help=f"writes per pause, at most {MAX_BATCH}")
    parser.add_argument("--archive-root", help=f"default {DEFAULT_ARCHIVE_ROOT}")
    parser.add_argument("--report", help="path of the JSON plan report")
    args = parser.parse_args(argv)
    if args.write and args.expect is None:
        parser.error("--write requires --expect N (the planned row count from the dry run)")
    if args.min_age_days < 0:
        parser.error("--min-age-days cannot be negative")
    try:
        from dotenv import load_dotenv
        here = Path(__file__).resolve()
        for parent in here.parents:
            if (parent / ".env").exists():
                load_dotenv(parent / ".env", override=False)
                break
    except ImportError:
        pass
    client = _supabase()
    if client is None:
        print("No Supabase credentials in the environment; nothing read.", file=sys.stderr)
        return 2
    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    summary = execute(client, sources, write=args.write, expect=args.expect, min_age_days=args.min_age_days,
                      limit=args.limit, page=args.page_size, batch=args.batch_size, archive_root=args.archive_root,
                      report_path=args.report)
    return 1 if summary.get("write_blocked") or summary.get("write_failed") else 0


if __name__ == "__main__":
    sys.exit(main())
