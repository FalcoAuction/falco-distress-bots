"""Commercial real estate review cards.

Runs after property_lane. For every commercial-lane staging row that is
graded, carries a sale date, or was excluded as a tenant record, it writes

  raw_payload.cre_review    parcel, owner entity, owner type, distress tie,
                            value basis, deadline, signer, listing status
  raw_payload.pilot_review  the shared pilot card (lane "cre")

Hamilton tax rows are checked against three free county files on each run:
the Trustee delinquent file (is the parcel still owed, which years, which
years were turned over for the tax suit), the assessor export (exact parcel,
current owner, last transfer, appraisal) and the latest Clerk and Master tax
sale list. No paid data, no Secretary of State scraping, no listing sites.

A Hamilton company owner held only because nobody can sign for it is looked
up once on the County Clerk's free business license search (_signer_lookup).
The answer is stored on the row (raw_payload.signer_lookup) and reused for
180 days; at most 15 companies are looked up per run, six seconds apart. A
lookup that fails stores nothing and changes no card. A license owner is a
named person or a mailing contact, never verified authority.

    python -m src.bots.cre_review_bot                  # dry run, prints the plan
    python -m src.bots.cre_review_bot --write
    python -m src.bots.cre_review_bot --ids ids.json --out plan.json [--write]

Distress type: N/A (reviewer). Writes go through apply_staging_enrichment,
which merges top-level raw_payload keys and never resends the notice text.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import time
import traceback as tb
import zipfile
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ._base import BotBase, _supabase
from ._cre_review import (COMPANY_HOLD, HAMILTON_ASSESSOR_URL, HAMILTON_SALE_LIST_URL, HAMILTON_TRUSTEE_URL, REVIEWER,
                          mailing_index_for, parse_sale_list, review_row, roll_index, roll_status)
from . import _signer_lookup as signer_lookup
from ._entity_signer import clean_owner_name, entity_key
from ._pilot_review import merge_review
from .hamilton_assessor_bot import HamiltonAssessorBot, _norm_parcel

TABLE = "homeowner_requests_staging"
TENANT_EXCLUDES = ("owner_mismatch", "personal_property_tax", "employment_notice")
RAW_KEYS = ("property_lane", "parcel", "owner_name_1", "owner_name_2", "structured", "extracted", "publication_date",
            "body", "comptroller_parcel", "metro_parcel", "hamilton_assessor", "tpad", "padctn", "williamson_inigo",
            "rutherford_arcgis", "shelby_arcgis", "warn", "warn_review", "source", "pilot_review", "cre_review",
            "signer_lookup")
SLIM = ("id,bot_source,distress_type,trustee_sale_date,staging_status,"
        "grade:raw_payload->property_lane->>grade,exclude:raw_payload->property_lane->>exclude_reason")
FULL = ("id,bot_source,distress_type,county,property_address,full_name,owner_name_records,property_value,"
        "property_value_source,trustee_sale_date,source_url,staging_status,"
        "notice_tracking:phone_metadata->notice_tracking,"
        + ",".join(f"rp_{k}:raw_payload->{k}" for k in RAW_KEYS))
_VOLATILE = ("reviewed_at",)


def _file_date(headers) -> Optional[str]:
    try:
        return parsedate_to_datetime(headers.get("Last-Modified")).astimezone(timezone.utc).date().isoformat()
    except (TypeError, ValueError, AttributeError):
        return None


def _csv_rows(content: bytes) -> Iterable[Dict[str, str]]:
    zf = zipfile.ZipFile(io.BytesIO(content))
    name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    with zf.open(name) as fh:
        yield from csv.DictReader(io.TextIOWrapper(fh, encoding="latin-1", newline=""))


def same_card(a: Any, b: Any) -> bool:
    """True when two cards say the same thing (ignoring the write timestamp)."""
    if not isinstance(a, dict) or not isinstance(b, dict):
        return False
    strip = lambda c: {k: v for k, v in c.items() if k not in _VOLATILE}
    return strip(a) == strip(b)


def in_scope(slim: Dict[str, Any]) -> bool:
    if slim.get("staging_status") == "rejected":
        return False
    return bool(slim.get("grade") or slim.get("trustee_sale_date") or slim.get("exclude") in TENANT_EXCLUDES)


def needs_signer_lookup(row: Dict[str, Any], item: Dict[str, Any]) -> bool:
    """A Hamilton company owner whose card is held on the signer alone."""
    return (str(row.get("county") or "").lower() == "hamilton" and item["facts"].get("owner_type") == "entity"
            and item["route"] == "research_hold" and item["route_reason"].startswith(COMPANY_HOLD))


def plan_signer_lookups(rows: List[Dict[str, Any]], plan: List[Dict[str, Any]], today: date,
                        limit: int = signer_lookup.MAX_PER_RUN) -> Dict[str, Any]:
    """Which companies to look up this run, and which rows can reuse an
    answer a sister row already holds. Returns {"fetch": {entity key:
    {"entity", "query", "ids"}}, "copy": {row id: stored lookup}}. A company
    is fetched once however many rows it owns."""
    by_id = {r["id"]: r for r in rows}
    groups: Dict[str, Dict[str, Any]] = {}
    for item in plan:
        row = by_id.get(item["id"])
        if not row or not needs_signer_lookup(row, item):
            continue
        name = clean_owner_name(item["facts"].get("owner_entity"))
        key = entity_key(name)
        if not key:
            continue
        g = groups.setdefault(key, {"entity": name, "ids": [], "fresh": None})
        stored = ((row.get("raw_payload") or {}).get("signer_lookup") or {}).get(signer_lookup.CLERK_KEY)
        if signer_lookup.is_fresh(stored, name, today):
            g["fresh"] = g["fresh"] or stored
        else:
            g["ids"].append(row["id"])
    out: Dict[str, Any] = {"fetch": {}, "copy": {}}
    for key in sorted(groups):
        g = groups[key]
        if not g["ids"]:
            continue
        if g["fresh"]:
            out["copy"].update({i: g["fresh"] for i in g["ids"]})
            continue
        query = signer_lookup.clerk_query(g["entity"])
        if query and len(out["fetch"]) < limit:
            out["fetch"][key] = {"entity": g["entity"], "query": query, "ids": g["ids"]}
    return out


def plan_rows(rows: List[Dict[str, Any]], evidence_for, today: date,
              new_lookups: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Cards to write. evidence_for(row) returns the Hamilton evidence dict or
    None. A row whose card is unchanged, or whose existing card wins the
    merge (manual, or a stronger card from another lane), is left alone.
    new_lookups: {row id: signer_lookup value} read this run; each is laid
    onto its row before the card is built and saved with it."""
    plan = []
    for row in rows:
        if new_lookups and row["id"] in new_lookups:
            row.setdefault("raw_payload", {})["signer_lookup"] = new_lookups[row["id"]]
    # Attention and care-of names per P.O. box, so a company with no name on its
    # own mailing line still gets the labeled path its sister company carries.
    boxes = mailing_index_for(rows, evidence_for)
    for row in rows:
        got = review_row(row, evidence_for(row), today, boxes)
        if not got:
            continue
        card, facts = got
        raw = row.get("raw_payload") or {}
        existing = raw.get("pilot_review")
        final = merge_review(existing, card)
        keep_existing = final is existing and existing is not None
        patch: Dict[str, Any] = {}
        if not keep_existing and not same_card(existing, final):
            patch["pilot_review"] = final
        if raw.get("cre_review") != facts:
            patch["cre_review"] = facts
        if new_lookups and row["id"] in new_lookups:
            patch["signer_lookup"] = new_lookups[row["id"]]
        plan.append({"id": row["id"], "route": card["route"], "route_reason": card["route_reason"],
                     "kept_existing_card": keep_existing, "card": card, "facts": facts, "patch": patch,
                     "before": {"pilot_review": existing, "cre_review": raw.get("cre_review")}})
    return plan


class CreReviewBot(BotBase):
    name = "cre_review"
    description = "Commercial real estate review cards: parcel, owner entity, signer path, distress tie, value, deadline"
    throttle_seconds = 0.5
    lookup_gap = signer_lookup.REQUEST_GAP

    def __init__(self, write: bool = True, ids: Optional[List[str]] = None, today: Optional[date] = None):
        super().__init__()
        self.write = write
        self.ids = ids
        self.today = today or date.today()
        self.source_notes: Dict[str, Any] = {}

    def scrape(self) -> List[Any]:
        return []

    # ── rows ──
    def _scope_ids(self, client) -> List[str]:
        if self.ids is not None:
            return list(self.ids)
        ids: List[str] = []
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            page = (client.table(TABLE).select(SLIM).not_.is_("raw_payload->property_lane", "null")
                    .eq("raw_payload->property_lane->>lane", "commercial").gt("id", after)
                    .order("id").limit(1000).execute().data) or []
            ids.extend(r["id"] for r in page if in_scope(r))
            if len(page) < 1000:
                return ids
            after = page[-1]["id"]

    def _rows(self, client, ids: List[str]) -> List[Dict[str, Any]]:
        out = []
        for i in range(0, len(ids), 40):
            page = client.table(TABLE).select(FULL).in_("id", ids[i:i + 40]).execute().data or []
            for r in page:
                r["raw_payload"] = {k: r.pop(f"rp_{k}") for k in RAW_KEYS if r.get(f"rp_{k}") is not None}
                for k in RAW_KEYS:
                    r.pop(f"rp_{k}", None)
                r["phone_metadata"] = {"notice_tracking": r.pop("notice_tracking", None)}
                out.append(r)
        return out

    # ── Hamilton county files ──
    def hamilton_evidence(self, parcels: set) -> Optional[Dict[str, Any]]:
        """County evidence for the wanted parcels, or None when the Trustee or
        assessor file is unavailable (existing cards are then left as they are)."""
        if not parcels:
            return None
        roll_res = self.fetch(HAMILTON_TRUSTEE_URL)
        asr_res = self.fetch(HAMILTON_ASSESSOR_URL)
        if not roll_res or roll_res.status_code != 200 or not asr_res or asr_res.status_code != 200:
            self.source_notes["hamilton"] = "trustee or assessor file unavailable"
            return None
        bills = roll_index(_csv_rows(roll_res.content), parcels)
        assessor: Dict[str, Dict[str, Any]] = {}
        duplicates = set()
        for r in _csv_rows(asr_res.content):
            key = _norm_parcel(r.get("MAP") or "", r.get("GROUP") or "", r.get("PARCEL") or "")
            if key in parcels:
                if key in assessor:
                    duplicates.add(key)
                assessor[key] = HamiltonAssessorBot._extract_record(r)
        sale_list, list_year, list_url = None, None, None
        for year in (self.today.year, self.today.year - 1):
            url = HAMILTON_SALE_LIST_URL.format(year=year)
            try:
                res = self.fetch(url)
                if res is None or res.status_code != 200:
                    continue
                import pdfplumber
                with pdfplumber.open(io.BytesIO(res.content)) as pdf:
                    text = "\n".join((p.extract_text() or "") for p in pdf.pages)
                parsed = parse_sale_list(text)
                if parsed:
                    sale_list, list_year, list_url = parsed, year, url
                    break
            except Exception as exc:  # a missing list is recorded, never fatal
                self.source_notes["hamilton_sale_list"] = f"{year}: {type(exc).__name__}"
        if sale_list is None:
            self.source_notes.setdefault("hamilton_sale_list", "no published list read")
        ev = {"roll_date": _file_date(roll_res.headers), "assessor_date": _file_date(asr_res.headers),
              "bills": bills, "assessor": assessor, "duplicates": duplicates,
              "sale_list": sale_list, "sale_list_year": list_year, "sale_list_url": list_url}
        self.source_notes["hamilton"] = {"roll_date": ev["roll_date"], "assessor_date": ev["assessor_date"],
                                         "sale_list_year": list_year, "sale_list_lines": len(sale_list or {})}
        return ev

    @staticmethod
    def evidence_lookup(ev: Optional[Dict[str, Any]]):
        def for_row(row):
            if ev is None or row.get("bot_source") != "hamilton_tax_delinquent":
                return None
            parcel = str((row.get("raw_payload") or {}).get("parcel") or "")
            return {"roll": roll_status(ev["bills"].get(parcel)), "roll_date": ev["roll_date"],
                    "assessor": ev["assessor"].get(parcel), "assessor_date": ev["assessor_date"],
                    "duplicate_parcel": parcel in ev["duplicates"], "sale_list": ev["sale_list"],
                    "sale_list_year": ev["sale_list_year"], "sale_list_url": ev["sale_list_url"]}
        return for_row

    # ── company signer lookups ──
    def signer_lookups(self, rows: List[Dict[str, Any]], plan: List[Dict[str, Any]]) -> Dict[str, Any]:
        """{row id: signer_lookup value} for rows that gained a County Clerk
        answer this run. Fails closed: a refused robots.txt, a failed
        request or an unreadable page adds nothing for that company."""
        todo = plan_signer_lookups(rows, plan, self.today)
        by_id = {r["id"]: r for r in rows}
        note: Dict[str, Any] = {"reused": len(todo["copy"]), "looked_up": 0, "failed": 0, "with_owner": 0}
        self.source_notes["signer_lookup"] = note
        found: Dict[str, Any] = dict(todo["copy"])
        if todo["fetch"]:
            robots = self.fetch(signer_lookup.CLERK_ROBOTS)
            if robots is None or robots.status_code != 200 or not signer_lookup.robots_allows(robots.text):
                note["skipped"] = "robots.txt unavailable or disallows the search"
                todo["fetch"] = {}
        for job in todo["fetch"].values():
            time.sleep(self.lookup_gap)
            res = self.fetch(signer_lookup.CLERK_URL, method="POST", data=signer_lookup.clerk_form(job["query"]))
            result = signer_lookup.clerk_result(job["entity"], job["query"], res.text, self.today) \
                if res is not None and res.status_code == 200 else None
            if result is None:
                note["failed"] += 1
                continue
            note["looked_up"] += 1
            note["with_owner"] += int(any(r.get("owner") for r in result["records"]))
            for row_id in job["ids"]:
                found[row_id] = result
        out = {}
        for row_id, result in found.items():
            held = dict((by_id[row_id].get("raw_payload") or {}).get("signer_lookup") or {})
            held[signer_lookup.CLERK_KEY] = result
            out[row_id] = held
        return out

    # ── write ──
    def apply(self, client, plan: List[Dict[str, Any]]) -> int:
        todo = [p for p in plan if p["patch"]]
        written = 0
        for i in range(0, len(todo), 50):
            batch = todo[i:i + 50]
            # Re-read the cards just before writing: another lane may have
            # written one since the plan was built.
            now = {r["id"]: r.get("pilot") for r in (client.table(TABLE).select("id,pilot:raw_payload->pilot_review")
                                                     .in_("id", [p["id"] for p in batch]).execute().data or [])}
            rows = []
            for p in batch:
                if p["id"] not in now:
                    continue
                patch = dict(p["patch"])
                if "pilot_review" in patch and now[p["id"]] != p["before"]["pilot_review"]:
                    final = merge_review(now[p["id"]], p["card"])
                    if final is now[p["id"]]:
                        patch.pop("pilot_review")
                    else:
                        patch["pilot_review"] = final
                if patch:
                    rows.append({"id": p["id"], "raw_patch": patch})
            if rows:
                written += int(client.rpc("apply_staging_enrichment", {"p_rows": rows}).execute().data or 0)
        return written

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
        plan: List[Dict[str, Any]] = []
        written, scoped, error = 0, 0, None
        try:
            ids = self._scope_ids(client)
            scoped = len(ids)
            rows = self._rows(client, ids)
            parcels = {str(r["raw_payload"].get("parcel")) for r in rows
                       if r.get("bot_source") == "hamilton_tax_delinquent" and r["raw_payload"].get("parcel")
                       and not str(r["raw_payload"]["parcel"]).upper().startswith("PER-")}
            ev = self.hamilton_evidence(parcels)
            plan = plan_rows(rows, self.evidence_lookup(ev), self.today)
            try:
                looked = self.signer_lookups(rows, plan)
            except Exception as exc:  # a failed lookup changes nothing
                looked = {}
                self.source_notes["signer_lookup"] = f"failed: {type(exc).__name__}"
            if looked:
                plan = plan_rows(rows, self.evidence_lookup(ev), self.today, looked)
            if self.write:
                written = self.apply(client, plan)
        except Exception as e:
            error = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        routes: Dict[str, int] = {}
        for p in plan:
            routes[p["route"]] = routes.get(p["route"], 0) + 1
        pending = sum(1 for p in plan if p["patch"])
        status = "failed" if error else "ok" if plan else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scoped, parsed_count=len(plan), staged_count=written,
                            duplicate_count=len(plan) - pending, error_message=error)
        return {"name": self.name, "status": status, "scoped": scoped, "carded": len(plan), "routes": routes,
                "pending_writes": pending, "written": written, "sources": self.source_notes, "error": error,
                "plan": plan, "reviewer": REVIEWER}


def run() -> dict:
    res = CreReviewBot(write=True).run()
    res.pop("plan", None)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--write", action="store_true", help="write cards (default: dry run)")
    ap.add_argument("--ids", help="JSON file with a list of staging ids to limit the run to")
    ap.add_argument("--out", help="save the full plan (before/after per id) to this JSON file")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    except ImportError:
        pass
    only = json.loads(Path(args.ids).read_text(encoding="utf-8")) if args.ids else None
    result = CreReviewBot(write=args.write, ids=only).run()
    full = result.pop("plan", [])
    if args.out:
        Path(args.out).write_text(json.dumps({"summary": result, "plan": full}, indent=1, default=str), encoding="utf-8")
    print(json.dumps(result, indent=1, default=str))
