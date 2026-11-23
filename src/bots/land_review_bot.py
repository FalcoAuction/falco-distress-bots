"""Land lane second-event review (statewide, every scheduled run).

Cross-matches every land tract against current tax, foreclosure, partition
and estate events on the exact county parcel and writes the shared pilot
review card (raw_payload.pilot_review, lane "land"). Rules in _land_review.

Tracts reviewed:
  land_lane rows            heirs, family-group and absentee tracts (HEIRS_PROPERTY / ABSENTEE_LAND)
  graded land in tax rolls  rows the property lane graded as land in LAND_GRADE_SOURCES
                            (today: Hamilton County delinquent-roll lots)

Cards written: every tract with at least one candidate event on its parcel
(counted or not, with the reason), and every A-grade land row. A tract with
an ownership signal and nothing else is a prospect and gets no card: the
land lane already records why on the row, and a card per prospect would be
about 1,300 writes a run that say the same sentence.

    python -m src.bots.land_review_bot            # dry run: prints the plan, writes nothing
    python -m src.bots.land_review_bot --write

Free sources only: staged notices, the Hamilton County Trustee delinquent
file and the Clerk and Master tax sale list PDF.
"""

from __future__ import annotations

import argparse
import io
import json
import re
import traceback as tb
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

from ._base import BotBase, _supabase
from . import _hamilton_tax_sale as tax_sale
from ._land_review import EVENT_TYPES, REVIEWER, build_card, hamilton_control, obj, review, same_card, staged_events, tract
from ._pilot_review import merge_review
from .hamilton_tax_delinquent_bot import HAMILTON_ZIP_URL, HamiltonTaxDelinquentBot

TABLE = "homeowner_requests_staging"
ROW_FIELDS = ("id,bot_source,county,distress_type,staging_status,rejection_reason,full_name,owner_name_records,"
              "property_address,property_value,trustee_sale_date,source_url,raw_payload")
SUMMARY = ("id,county,distress_type,cp:raw_payload->comptroller_parcel->>parcelid,"
           "pc:raw_payload->comptroller_parcel->>parcel_count,apid:raw_payload->assessment->>parcel_id,"
           "printed:raw_payload->>parcel,ts:raw_payload->tax_sale->>parcel")
# Sources whose rows the property lane grades as land without a land_lane row.
LAND_GRADE_SOURCES = ("hamilton_tax_delinquent",)
EVENT_SOURCES = ("tn_public_notice", "court_notices", "tn_lis_pendens", "nashville_ledger", "memphis_daily_news",
                 "main_street_media", "hardeman_tax_notice", "davidson_tax_sale", "montgomery_tax_sale",
                 "hamilton_county_herald", "brock_scott_trustee", "mackie_wolf_trustee")
START = "00000000-0000-0000-0000-000000000000"
_YEAR = re.compile(r" 20[0-9]{2}$")


def _pages(client, fields: str, source: str, size: int = 500) -> Iterable[Dict[str, Any]]:
    """Keyset pages on the indexed (bot_source, id); never a JSON filter scan."""
    after = START
    while True:
        page = (client.table(TABLE).select(fields).eq("bot_source", source).gt("id", after)
                .order("id").limit(size).execute().data) or []
        yield from page
        if len(page) < size:
            return
        after = page[-1]["id"]


def _by_ids(client, ids: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for offset in range(0, len(ids), 80):
        rows += client.table(TABLE).select(ROW_FIELDS).in_("id", ids[offset:offset + 80]).execute().data or []
    return rows


def _pdf_text(content: bytes) -> str:
    import pdfplumber
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _file_date(response) -> Optional[str]:
    try:
        return parsedate_to_datetime(response.headers.get("Last-Modified")).date().isoformat()
    except (TypeError, ValueError, AttributeError):
        return None


class LandReviewBot(BotBase):
    name = "land_review"
    description = "Land tracts cross-matched to current tax, foreclosure, partition and estate events on the exact parcel"
    throttle_seconds = 2

    def scrape(self) -> List[Any]:
        return []

    # ── reads ──
    def load_tracts(self, client) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for row in _pages(client, ROW_FIELDS, "land_lane"):
            land = tract(row)
            if land:
                out[row["id"]] = {"row": row, "land": land}
        for source in LAND_GRADE_SOURCES:
            ids = [r["id"] for r in _pages(client, "id,lane:raw_payload->property_lane->>lane,"
                                           "grade:raw_payload->property_lane->>grade", source, 1000)
                   if r.get("lane") == "land" and r.get("grade")]
            for row in _by_ids(client, ids):
                land = tract(row)
                if land:
                    out[row["id"]] = {"row": row, "land": land}
        return out

    def load_events(self, client, identities: set, today: date) -> Dict[tuple, List[Dict[str, Any]]]:
        """Only rows whose stored parcel can equal a tract's control are read in full."""
        wanted = defaultdict(set)
        for _, county, control in identities:
            wanted[county].add(control)
        index: Dict[tuple, List[Dict[str, Any]]] = defaultdict(list)
        for source in EVENT_SOURCES:
            ids = []
            for s in _pages(client, SUMMARY, source):
                county = " ".join(str(s.get("county") or "").lower().split())
                if s.get("distress_type") not in EVENT_TYPES or county not in wanted:
                    continue
                printed = [str(v) for v in (s.get("cp"), s.get("apid"), s.get("printed"), s.get("ts")) if v]
                forms = {f for v in printed for f in (v, _YEAR.sub("", v), hamilton_control(v)) if f}
                if forms & wanted[county] or str(s.get("pc") or "1") != "1":
                    ids.append(s["id"])
            for row in _by_ids(client, ids):
                for event in staged_events(row, today):
                    if tuple(event["identity"]) in identities:
                        index[tuple(event["identity"])].append(event)
        return index

    def hamilton_sources(self, today: date) -> Optional[Dict[str, Any]]:
        """Trustee delinquent file and the Clerk and Master sale list. None
        when either is unreadable: Hamilton cards then wait for the next run
        instead of claiming a search that did not happen."""
        rows = HamiltonTaxDelinquentBot._fetch_csv_rows(self)
        if not rows:
            return None
        res = getattr(self, "_last_trustee_response", None)
        parcels = {}
        for key, row in HamiltonTaxDelinquentBot._latest_per_parcel(rows).items():
            control = hamilton_control(key)
            if control:
                parcels[control] = {"total": float(row.get("__cumulative_owed__") or 0),
                                    "years": int(row.get("__years_delinquent__") or 0),
                                    "latest_year": HamiltonTaxDelinquentBot._safe_int(row.get("Bill Year"))}
        events, used = [], None
        for year in (today.year, today.year - 1):
            url = tax_sale.list_url(year)
            got = self.fetch(url, timeout=120)
            if got is None:
                return None
            if got.status_code == 404:
                continue
            if got.status_code != 200:
                return None
            parsed = tax_sale.parse_list(_pdf_text(got.content))
            if not parsed["entries"] or parsed["unparsed"]:
                self.logger.warning("Hamilton tax sale list %s: %s entries, %s unparsed lines; not used",
                                    year, len(parsed["entries"]), parsed["unparsed"])
                return None
            info = self.fetch(tax_sale.info_url(year), timeout=120)
            sale_day = tax_sale.parse_sale_date(_pdf_text(info.content), year) if info is not None and info.status_code == 200 else None
            events = tax_sale.sale_events(parsed, year, url, sale_day, today)
            used = {"year": year, "url": url, "revised": parsed["revised"], "entries": len(parsed["entries"]), "sale_date": sale_day}
            break
        return {"roll": {"file_date": _file_date(res) if res is not None else None, "parcels": parcels},
                "events": events, "sale_list": used}

    def fetch(self, url, *args, **kwargs):
        res = super().fetch(url, *args, **kwargs)
        if url == HAMILTON_ZIP_URL:
            self._last_trustee_response = res
        return res

    # ── plan and write ──
    def plan(self, client, today: Optional[date] = None) -> Dict[str, Any]:
        today = today or date.today()
        tracts = self.load_tracts(client)
        identities = {tuple(t["land"]["identity"]) for t in tracts.values()}
        index = self.load_events(client, identities, today)
        hamilton = None
        if any(t["land"]["kind"] == "hamilton" for t in tracts.values()):
            hamilton = self.hamilton_sources(today)
            for event in (hamilton or {}).get("events", []):
                if tuple(event["identity"]) in identities:
                    index[tuple(event["identity"])].append(event)
        items, stats = [], Counter(tracts=len(tracts))
        for sid, item in sorted(tracts.items()):
            row, land = item["row"], item["land"]
            stats["tracts_" + land["kind"]] += 1
            if land["kind"] == "hamilton" and hamilton is None:
                stats["hamilton_sources_unavailable"] += 1
                continue
            result = review(row, land, index.get(tuple(land["identity"]), ()))
            grade = obj(obj(row.get("raw_payload")).get("property_lane")).get("grade")
            if not (result["matched"] or result["rejected"] or grade == "A"):
                stats["prospect_no_card"] += 1
                continue
            card = build_card(row, land, result, today, (hamilton or {}).get("roll") if land["kind"] == "hamilton" else None)
            stats["route_" + card["route"]] += 1
            stats["with_current_second_event"] += bool(result["matched"])
            stats["with_rejected_candidate"] += bool(result["rejected"])
            items.append({"id": sid, "card": card, "matched": result["matched"], "rejected": result["rejected"],
                          "identity": list(land["identity"]), "grade": grade})
        return {"items": items, "stats": dict(stats), "hamilton_sale_list": (hamilton or {}).get("sale_list"),
                "hamilton_roll_date": obj((hamilton or {}).get("roll")).get("file_date")}

    def write_cards(self, client, items: List[Dict[str, Any]], only_ids: Optional[set] = None) -> Dict[str, List[str]]:
        """Re-read each row, keep a manual or stronger card, and write only when
        the row is still the one that was reviewed."""
        out: Dict[str, List[str]] = {"written": [], "unchanged": [], "kept_existing": [], "conflict": [], "failed": []}
        for item in items:
            sid = item["id"]
            if only_ids is not None and sid not in only_ids:
                continue
            try:
                cur = (client.table(TABLE).select(ROW_FIELDS).eq("id", sid).limit(1).execute().data) or []
                if not cur or not isinstance(cur[0].get("raw_payload"), dict):
                    out["conflict"].append(sid)
                    continue
                row, raw = cur[0], cur[0]["raw_payload"]
                land = tract(row)
                if not land or list(land["identity"]) != item["identity"]:
                    out["conflict"].append(sid)      # the parcel changed under us: review it again next run
                    continue
                existing = raw.get("pilot_review")
                if same_card(existing, item["card"]):
                    out["unchanged"].append(sid)
                    continue
                merged = merge_review(existing, item["card"])
                if merged is not item["card"]:
                    out["kept_existing"].append(sid)
                    continue
                update = client.table(TABLE).update({"raw_payload": {**raw, "pilot_review": merged}}).eq("id", sid)
                for field in ("county", "owner_name_records", "staging_status"):
                    value = row.get(field)
                    update = update.eq(field, value) if value is not None else update.is_(field, "null")
                if isinstance(existing, dict) and existing.get("reviewed_at"):
                    update = update.eq("raw_payload->pilot_review->>reviewed_at", existing["reviewed_at"])
                else:
                    update = update.is_("raw_payload->pilot_review", "null")
                encoded = json.dumps(raw, separators=(",", ":"))
                if len(quote(encoded, safe="")) <= 6000:   # full compare-and-swap when it fits in the request
                    update = update.eq("raw_payload", encoded)
                out["written" if update.execute().data else "conflict"].append(sid)
            except Exception as exc:
                out["failed"].append(sid)
                self.logger.warning("land review write failed id=%s type=%s", sid, type(exc).__name__)
        return out

    def run(self, write: bool = True, only_ids: Optional[set] = None) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        health = dict(fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        if write:   # a dry run leaves no trace in production, health rows included
            self._report_health(status="running", started_at=started, finished_at=None, **health)
        client = _supabase()
        if client is None:
            if write:
                self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                    error_message="no_supabase_client", **health)
            return {"name": self.name, "status": "no_supabase"}
        error, planned, written = None, {"items": [], "stats": {}}, {}
        try:
            planned = self.plan(client)
            if write:
                written = self.write_cards(client, planned["items"], only_ids)
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {exc}")
        stats = planned["stats"]
        status = "failed" if error else "ok" if stats.get("tracts") else "zero_yield"
        if write:
            self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=stats.get("tracts", 0), parsed_count=len(planned["items"]),
                                staged_count=len(written.get("written", [])), duplicate_count=len(written.get("unchanged", [])),
                                error_message=error)
        return {"name": self.name, "status": status, "stats": stats, "planned": len(planned["items"]),
                "written": {k: len(v) for k, v in written.items()}, "written_ids": written,
                "hamilton_sale_list": planned.get("hamilton_sale_list"), "items": planned["items"], "error": error}


def run() -> dict:
    result = LandReviewBot().run(write=True)
    return {k: v for k, v in result.items() if k not in ("items", "written_ids")}


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--write", action="store_true", help="write the cards (default: dry run)")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    except ImportError:
        pass
    res = LandReviewBot().run(write=args.write)
    for it in res.get("items", []):
        print(it["id"], it["card"]["route"], "|", it["card"]["route_reason"])
    print({k: v for k, v in res.items() if k not in ("items", "written_ids")})
