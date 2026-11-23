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
import traceback as tb
import zipfile
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ._base import BotBase, _supabase
from ._cre_review import (HAMILTON_ASSESSOR_URL, HAMILTON_SALE_LIST_URL, HAMILTON_TRUSTEE_URL, REVIEWER,
                          mailing_index_for, parse_sale_list, review_row, roll_index, roll_status)
from ._pilot_review import merge_review
from .hamilton_assessor_bot import HamiltonAssessorBot, _norm_parcel

TABLE = "homeowner_requests_staging"
TENANT_EXCLUDES = ("owner_mismatch", "personal_property_tax", "employment_notice")
RAW_KEYS = ("property_lane", "parcel", "owner_name_1", "owner_name_2", "structured", "extracted", "publication_date",
            "body", "comptroller_parcel", "metro_parcel", "hamilton_assessor", "tpad", "padctn", "williamson_inigo",
            "rutherford_arcgis", "shelby_arcgis", "warn", "warn_review", "source", "pilot_review", "cre_review")
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


def plan_rows(rows: List[Dict[str, Any]], evidence_for, today: date) -> List[Dict[str, Any]]:
    """Cards to write. evidence_for(row) returns the Hamilton evidence dict or
    None. A row whose card is unchanged, or whose existing card wins the
    merge (manual, or a stronger card from another lane), is left alone."""
    plan = []
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
        plan.append({"id": row["id"], "route": card["route"], "route_reason": card["route_reason"],
                     "kept_existing_card": keep_existing, "card": card, "facts": facts, "patch": patch,
                     "before": {"pilot_review": existing, "cre_review": raw.get("cre_review")}})
    return plan


class CreReviewBot(BotBase):
    name = "cre_review"
    description = "Commercial real estate review cards: parcel, owner entity, signer path, distress tie, value, deadline"
    throttle_seconds = 0.5

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
