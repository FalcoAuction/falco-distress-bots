"""
Final check before paid trace: reads for the pilot worklist rows, and the
optional write of each result beside its card.

    python -m src.bots.pretrace_check_bot                  # dry run, prints the queue counts
    python -m src.bots.pretrace_check_bot --facts F.json   # add facts read by hand
    python -m src.bots.pretrace_check_bot --write          # store raw_payload.pretrace_check

It runs in the daily pipeline after the lane review bots and every time the
worklist is built (pilot_worklist calls collect()). The daily run is a dry
run unless PRETRACE_CHECK_WRITE=1. A write stores only raw_payload.pretrace_check
on rows that still carry the same first-trace card; it never touches
pilot_review, a route, or any other column, and it skips a row whose result
did not change. Read-only against every CRM table. No outside requests: the
check works from what the pipeline has already staged.

The facts file is JSON keyed by staging id, for things only a person has read:

    {"<staging id>": {"co_owner_prints": [{"name": "...", "source": "...", "date": "YYYY-MM-DD", "url": "..."}],
                      "listing": {"status": "active|pending|none", "source": "...", "date": "..."},
                      "payoff_document": {"amount": 0, "date": "...", "source": "..."},
                      "do_not_contact": {"reason": "...", "date": "..."},
                      "manual_hold": {"reason": "...", "date": "..."},
                      "contacted": {"reason": "...", "date": "..."}}}

Distress type: N/A (review of existing records).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import traceback as tb
from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

from ._base import BotBase, _supabase
from ._pilot_review import ROUTES, _owner_parts, rekey
from ._pretrace_check import KEY, QUEUES, check_all, without_run_flags

TABLE = "homeowner_requests_staging"
CRM = "https://falco.llc/crm/leads/"
ROW_FIELDS = ("id,bot_source,county,staging_status,rejection_reason,pipeline_lead_key,full_name,owner_name_records,"
              "property_address,phone,email,source_url,trustee_sale_date,property_value,property_value_source,"
              "property_value_as_of,raw_payload,phone_metadata")
WRITE_ENV = "PRETRACE_CHECK_WRITE"


def _chunks(items: List[Any], size: int) -> Iterable[List[Any]]:
    for i in range(0, len(items), size):
        yield items[i:i + size]


def fetch_cards(client) -> List[Dict[str, Any]]:
    """Every staging row with a pilot card, with its CRM key. Same indexed
    route filter as the worklist."""
    rows: List[Dict[str, Any]] = []
    for route in ROUTES:
        start = 0
        while True:
            page = (client.table(TABLE).select("id,county,pipeline_lead_key,pilot_review:raw_payload->pilot_review")
                    .eq("raw_payload->pilot_review->>route", route).order("id").range(start, start + 199).execute().data) or []
            rows.extend(page)
            if len(page) < 200:
                break
            start += 200
    return rows


def collect(client, cards: List[Dict[str, Any]], facts: Optional[Dict[str, Any]] = None,
            today: Optional[date] = None, fresh: bool = False, log=None) -> Dict[str, Any]:
    """Read what the check needs for the first-trace cards and run it.

    cards: [{"id", "county", "pilot_review", "pipeline_lead_key"?}] for every
    card (all routes), as the worklist reads them. Returns {"results": {id:
    result}, "rows": the full staging rows, "unread": tables that failed}.
    A table that cannot be read is reported on every row as an unknown hold.
    """
    facts = facts or {}
    first = [c for c in cards if isinstance(c.get("pilot_review"), dict) and c["pilot_review"].get("route") == "first_trace_review"]
    rows: List[Dict[str, Any]] = []
    for chunk in _chunks([c["id"] for c in first], 40):
        rows += client.table(TABLE).select(ROW_FIELDS).in_("id", chunk).execute().data or []
    rows = sorted((r for r in rows if isinstance((r.get("raw_payload") or {}).get("pilot_review"), dict)), key=lambda r: str(r["id"]))
    parts_of = {r["id"]: _owner_parts(rekey(r["raw_payload"]["pilot_review"], r.get("county")).get("owner_group_key")) for r in rows}
    wanted = {p for parts in parts_of.values() for p in parts}
    # Other records of the same owners (any lane, any route): their CRM rows count as holds too.
    others: Dict[str, Dict[str, Any]] = {}
    for c in cards:
        if c["id"] in parts_of or not isinstance(c.get("pilot_review"), dict):
            continue
        parts = _owner_parts(rekey(c["pilot_review"], c.get("county")).get("owner_group_key"))
        if wanted.intersection(parts):
            others[c["id"]] = {"owner_parts": parts, "slug": c.get("pipeline_lead_key")}
    slug_of = {r["id"]: r.get("pipeline_lead_key") for r in rows}
    slug_of.update({rid: o["slug"] for rid, o in others.items()})
    unread: List[str] = []

    def read(table: str, cols: str, key: str, values: List[str]) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        try:
            for chunk in _chunks(values, 80):
                out += client.table(table).select(cols).in_(key, chunk).execute().data or []
        except Exception as exc:        # a failed read is an unknown, never a clean bill
            unread.append(table)
            if log:
                log.warning("pretrace check could not read %s (%s)", table, type(exc).__name__)
            return []
        return out

    missing = sorted(rid for rid, o in others.items() if o["slug"] is None)     # cards read without the CRM key
    for r in read(TABLE, "id,pipeline_lead_key", "id", missing):
        slug_of[r["id"]] = r.get("pipeline_lead_key")
    slugs = sorted({s for s in slug_of.values() if s})
    workflow = {w["listing_slug"]: w for w in read(
        "dialer_lead_workflow", "listing_slug,status,next_action,last_contact_at,attempt_count,closed_lost_reason,updated_at",
        "listing_slug", slugs)}
    activities: Dict[str, List[Dict[str, Any]]] = {}
    for a in read("dialer_activities", "listing_slug,outcome,channel,occurred_at", "listing_slug", slugs):
        activities.setdefault(a["listing_slug"], []).append(a)
    sms = {s["listing_slug"]: s for s in read(
        "sms_outreach_state", "listing_slug,status,last_sent_at,enrolled_at,updated_at", "listing_slug", slugs)}
    live = {h["pipeline_lead_key"]: h for h in read(
        "homeowner_requests", "id,pipeline_lead_key,status,last_contacted_at,skiptrace_fetched_at,sale_status:phone_metadata->sale_status",
        "pipeline_lead_key", slugs)}
    emails = sorted({str(r["email"]).strip().lower() for r in rows if r.get("email")})
    suppressed = {s["email"]: s.get("reason") for s in read("outreach_suppressions", "email,reason", "email", emails)} if emails else {}

    contexts: Dict[str, Dict[str, Any]] = {}
    for rid, slug in slug_of.items():
        ctx = {"workflow": workflow.get(slug), "activities": sorted(activities.get(slug, []), key=lambda a: str(a.get("occurred_at"))),
               "sms": sms.get(slug), "live": live.get(slug), "suppressed_emails": suppressed, "unread": sorted(set(unread)),
               "facts": facts.get(rid) or {}}
        if rid in others:
            ctx["owner_parts"] = others[rid]["owner_parts"]
        contexts[rid] = ctx
    results = check_all(rows, contexts, cards, today, CRM, fresh)
    return {"results": results, "rows": rows, "unread": sorted(set(unread))}


def write_results(client, rows: List[Dict[str, Any]], results: Dict[str, Dict[str, Any]], log=None) -> Dict[str, List[str]]:
    """Store each changed result at raw_payload.pretrace_check. The row is read
    again first and the write is refused unless it still carries the same
    first-trace card, so a card that moved (or a manual card written since) is
    left alone and checked again on the next run."""
    out: Dict[str, List[str]] = {"written": [], "unchanged": [], "conflict": [], "failed": []}
    for row in rows:
        sid = row["id"]
        result = without_run_flags(results.get(sid))
        if not result:
            continue
        try:
            cur = client.table(TABLE).select("id,raw_payload").eq("id", sid).limit(1).execute().data or []
            raw = cur[0].get("raw_payload") if cur else None
            card = raw.get("pilot_review") if isinstance(raw, dict) else None
            reviewed = (row["raw_payload"].get("pilot_review") or {}).get("reviewed_at")
            if not isinstance(card, dict) or card.get("route") != "first_trace_review" or card.get("reviewed_at") != reviewed:
                out["conflict"].append(sid)
                continue
            if raw.get(KEY) == result:
                out["unchanged"].append(sid)
                continue
            update = (client.table(TABLE).update({"raw_payload": {**raw, KEY: result}}).eq("id", sid)
                      .eq("raw_payload->pilot_review->>route", "first_trace_review"))
            update = update.eq("raw_payload->pilot_review->>reviewed_at", reviewed) if reviewed else update
            encoded = json.dumps(raw, separators=(",", ":"))
            if len(quote(encoded, safe="")) <= 6000:   # full compare-and-swap when it fits in the request
                update = update.eq("raw_payload", encoded)
            out["written" if update.execute().data else "conflict"].append(sid)
        except Exception as exc:
            out["failed"].append(sid)
            if log:
                log.warning("pretrace check write failed id=%s type=%s", sid, type(exc).__name__)
    return out


def summarize(results: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    by_lane: Dict[str, Counter] = {}
    for r in results.values():
        by_lane.setdefault(r.get("lane") or "?", Counter())[r["queue"]] += 1
    return {"cards": len(results), "carried_forward": sum(1 for r in results.values() if r.get("carried_forward")),
            "by_lane": {lane: {q: c.get(q, 0) for q in QUEUES} for lane, c in sorted(by_lane.items())}}


class PretraceCheckBot(BotBase):
    name = "pretrace_check"
    description = "Final check before paid trace on pilot worklist rows (dry run unless PRETRACE_CHECK_WRITE=1)"

    def scrape(self) -> List[Any]:
        return []

    def run(self, write: Optional[bool] = None, facts: Optional[Dict[str, Any]] = None, fresh: bool = False) -> Dict[str, Any]:
        write = (os.environ.get(WRITE_ENV) == "1") if write is None else write
        started = datetime.now(timezone.utc)
        health = dict(fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            return {"name": self.name, "status": "no_supabase"}
        if write:   # a dry run leaves no trace in production, health rows included
            self._report_health(status="running", started_at=started, finished_at=None, **health)
        error, got, written = None, {"results": {}, "rows": [], "unread": []}, {}
        try:
            got = collect(client, fetch_cards(client), facts, fresh=fresh, log=self.logger)
            if write:
                written = write_results(client, got["rows"], got["results"], self.logger)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {exc}")
        stats = summarize(got["results"])
        status = "failed" if error else "ok" if got["results"] else "zero_yield"
        if write:
            self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=len(got["rows"]), parsed_count=len(got["results"]),
                                staged_count=len(written.get("written", [])), duplicate_count=len(written.get("unchanged", [])),
                                error_message=error)
        self.logger.info("pretrace check %s: %s", "written" if write else "dry run", json.dumps(stats))
        return {"name": self.name, "status": status, "dry_run": not write, "stats": stats, "unread": got["unread"],
                "written": {k: len(v) for k, v in written.items()}, "results": got["results"], "error": error}


def run() -> dict:
    result = PretraceCheckBot().run()
    return {k: v for k, v in result.items() if k != "results"}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--write", action="store_true", help="store raw_payload.pretrace_check (default: dry run)")
    ap.add_argument("--facts", help="JSON file of facts read by hand, keyed by staging id")
    ap.add_argument("--fresh", action="store_true", help="ignore stored results and evaluate every row")
    args = ap.parse_args(argv)
    facts = json.loads(Path(args.facts).read_text(encoding="utf-8")) if args.facts else None
    result = PretraceCheckBot().run(write=args.write, facts=facts, fresh=args.fresh)
    print(json.dumps({k: v for k, v in result.items() if k != "results"}, indent=1))
    return 0 if result.get("status") in ("ok", "zero_yield") else 1


if __name__ == "__main__":
    sys.exit(main())
