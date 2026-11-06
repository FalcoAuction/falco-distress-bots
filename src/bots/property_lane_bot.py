"""Property lanes: split every lead into residential / commercial / land,
keep the commercial and land leads that have a real way to reach the
owner, and count what each pipeline produces.

Residential leads keep flowing through the decision engine and dialer as
before; this bot does not change their decisions. Commercial and land
leads (which the residential engine rejects or holds) get tagged in
raw_payload.property_lane so they can be worked as their own lanes:
commercial for auction/listing routing, land for buy-from-owner flips.

    python -m src.bots.property_lane_bot            # report only, no writes
    python -m src.bots.property_lane_bot --write    # also tag staging rows

The report prints per-pipeline counts and writes, under out/lanes/
(gitignored, contains owner PII):
    pipeline_counts.csv   one row per bot source
    commercial_leads.csv  graded commercial leads with a contact path
    land_leads.csv        graded land leads, sorted by land-flip score

Distress type: N/A (classifier only). Reads assessor data the enrichers
already stored; no network beyond Supabase.
"""
from __future__ import annotations

import argparse
import csv
import os
import time
import traceback as tb
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from ._base import BotBase, _supabase
from ._property_lane import property_lane

PAGE = 1000
OUT_DIR = Path(__file__).resolve().parents[2] / "out" / "lanes"
RAW_KEYS = ("padctn", "metro_parcel", "hamilton_assessor", "comptroller_parcel", "davidson_tax_sale", "tpad", "shelby_arcgis", "rutherford_arcgis", "williamson_inigo",
            "padctn_owner_match", "owner_match", "asset_profile", "land_use", "county", "mailing_address")
_BASE_FIELDS = ("id,county,distress_type,property_address,property_value,owner_name_records,full_name,"
                "phone,alternate_phones,email,trustee_sale_date,"
                "de_action:phone_metadata->decision_engine->>action,owner_class:phone_metadata->owner_class,"
                "pm_batch:phone_metadata->batchdata_skip_trace,pm_resolver:phone_metadata->phone_resolver,"
                "lane_tag:raw_payload->property_lane,"
                + ",".join(f"rp_{k}:raw_payload->{k}" for k in RAW_KEYS))
TABLES = {
    "homeowner_requests_staging": _BASE_FIELDS + ",bot_source,staging_status",
    "homeowner_requests": _BASE_FIELDS + ",bot_source:source",
}


def _rows(client, table: str, fields: str) -> Iterator[Dict[str, Any]]:
    """Keyset pages; a few retries because the project times out under load."""
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        for attempt in range(4):
            try:
                page = (client.table(table).select(fields).gt("id", after)
                        .order("id").limit(PAGE).execute().data) or []
                break
            except Exception:
                if attempt == 3:
                    raise
                time.sleep(3 * (attempt + 1))
        if not page:
            return
        after = page[-1]["id"]
        for r in page:
            r["raw_payload"] = {k: r.pop(f"rp_{k}") for k in RAW_KEYS if r.get(f"rp_{k}") is not None}
            for k in RAW_KEYS:
                r.pop(f"rp_{k}", None)
            r["phone_metadata"] = {"owner_class": r.pop("owner_class", None),
                                   "batchdata_skip_trace": r.pop("pm_batch", None),
                                   "phone_resolver": r.pop("pm_resolver", None)}
            r["__table__"] = table
            yield r
        if len(page) < PAGE:
            return
        time.sleep(0.2)


def _dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """One entry per property: the same parcel can sit in staging and in the
    live table, or come in from two sources. Keep the best grade."""
    best: Dict[str, Dict[str, Any]] = {}
    for it in items:
        addr = " ".join(str(it["row"].get("property_address") or it["row"]["id"]).upper().replace(",", " ").split())
        cur = best.get(addr)
        if cur is None or it["lane"]["grade"] < cur["lane"]["grade"]:
            best[addr] = it
    return list(best.values())


def _tag(lane: Dict[str, Any]) -> Dict[str, Any]:
    """What gets stored on the row: lane facts plus the contact path the grade
    rests on (owner mailing, and phones only when phone_trust ties them to the
    owner), so /admin/lanes can be worked without re-deriving it."""
    c = lane["contact"]
    tag = {k: lane[k] for k in ("lane", "subtype", "basis", "acres", "value", "owner_type",
                                "grade", "exclude_reason", "stacked_distress", "version")}
    tag["contact_tier"] = c["tier"]
    tag["out_of_state_owner"] = c["out_of_state"]
    tag["mailing"] = c["mailing"]
    tag["mailing_state"] = c["mailing_state"]
    tag["phones"] = c["phones"]
    tag["email"] = c["email"]
    if "land_flip" in lane:
        tag["land_flip"] = lane["land_flip"]
    return tag


class PropertyLaneBot(BotBase):
    name = "property_lane"
    description = "Split leads into residential / commercial / land lanes; grade commercial and land by contact path"

    def __init__(self, write: bool = True, out_dir: Optional[Path] = None):
        super().__init__()
        self.write = write
        self.out_dir = out_dir or OUT_DIR

    def scrape(self) -> List[Any]:
        return []

    # ── core pass ──
    def classify_all(self, client) -> Dict[str, Any]:
        counts: Dict[str, Counter] = defaultdict(Counter)
        commercial: List[Dict[str, Any]] = []
        land: List[Dict[str, Any]] = []
        to_tag: List[Dict[str, Any]] = []
        for table, fields in TABLES.items():
            for row in _rows(client, table, fields):
                src = row.get("bot_source") or "unknown"
                lane = property_lane(row)
                c = counts[src]
                c["total"] += 1
                c[f"lane_{lane['lane']}"] += 1
                if lane["lane"] in ("residential", "unknown") and str(row.get("de_action") or "").startswith("PROMOTE"):
                    c["dialer_promoted"] += 1
                if lane["owner_type"] in ("entity", "religious") and lane["lane"] == "unknown":
                    c["entity_owner_no_land_use"] += 1
                if lane["lane"] not in ("commercial", "land"):
                    continue
                key = lane["lane"]
                if lane["contact"]["unverified_phone"]:
                    c[f"{key}_phone_unverified"] += 1
                if lane["stacked_distress"]:
                    c[f"{key}_stacked_distress"] += 1
                if lane["grade"]:
                    c[f"{key}_qualified"] += 1
                    c[f"{key}_grade_{lane['grade']}"] += 1
                    (commercial if key == "commercial" else land).append({"row": row, "lane": lane})
                else:
                    c[f"{key}_excluded_{lane['exclude_reason']}"] += 1
                if key == "land" and lane["grade"] and lane["land_flip"]["score"] >= 4:
                    c["land_flip_strong"] += 1
                if table == "homeowner_requests_staging":
                    tag = _tag(lane)
                    if row.get("lane_tag") != tag:
                        to_tag.append({"id": row["id"], "tag": tag})
        return {"counts": counts, "commercial": _dedupe(commercial), "land": _dedupe(land), "to_tag": to_tag}

    def write_tags(self, client, to_tag: List[Dict[str, Any]]) -> int:
        """Merge raw_payload.property_lane into each row, re-reading the row first
        so a concurrent enricher's raw_payload write is not clobbered."""
        written = 0
        for item in to_tag:
            try:
                cur = (client.table("homeowner_requests_staging").select("raw_payload")
                       .eq("id", item["id"]).limit(1).execute().data) or []
                if not cur:
                    continue
                raw = cur[0].get("raw_payload") or {}
                if not isinstance(raw, dict):
                    raw = {}
                if raw.get("property_lane") == item["tag"]:
                    continue
                raw["property_lane"] = item["tag"]
                client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", item["id"]).execute()
                written += 1
            except Exception as e:
                self.logger.warning(f"  tag failed id={item['id']}: {e}")
        return written

    # ── reports ──
    def write_reports(self, result: Dict[str, Any]) -> Dict[str, str]:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        paths = {}
        counts = result["counts"]
        cols = sorted({k for c in counts.values() for k in c})
        lead_cols = ["total", "lane_residential", "dialer_promoted", "lane_commercial", "commercial_qualified",
                     "lane_land", "land_qualified", "land_flip_strong", "lane_unknown", "entity_owner_no_land_use"]
        cols = lead_cols + [c for c in cols if c not in lead_cols]
        p = self.out_dir / "pipeline_counts.csv"
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["bot_source"] + cols)
            for src, c in sorted(counts.items(), key=lambda kv: -kv[1]["total"]):
                w.writerow([src] + [c.get(k, 0) for k in cols])
        paths["pipeline_counts"] = str(p)

        def dump(name: str, items: List[Dict[str, Any]], sort_key) -> None:
            fp = self.out_dir / f"{name}.csv"
            with open(fp, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["grade", "land_flip_score", "land_flip_signals", "stacked_distress", "subtype", "basis", "bot_source",
                            "distress_type", "county", "property_address", "owner", "owner_type", "value", "acres",
                            "contact_tier", "phones", "email", "mailing", "out_of_state", "trustee_sale_date", "id"])
                for it in sorted(items, key=sort_key):
                    r, ln, c = it["row"], it["lane"], it["lane"]["contact"]
                    lf = ln.get("land_flip") or {}
                    w.writerow([ln["grade"], lf.get("score", ""), " ".join(lf.get("signals", [])),
                                " ".join(ln["stacked_distress"]), ln["subtype"] or "",
                                ln["basis"], r.get("bot_source"), r.get("distress_type"), r.get("county"),
                                r.get("property_address"), r.get("owner_name_records") or r.get("full_name"),
                                ln["owner_type"], ln["value"] or "", ln["acres"] or "", c["tier"], " ".join(c["phones"]),
                                c["email"] or "", c["mailing"] or "", c["out_of_state"], r.get("trustee_sale_date") or "",
                                r["id"]])
            paths[name] = str(fp)

        dump("commercial_leads", result["commercial"], lambda it: (it["lane"]["grade"], -(it["lane"]["value"] or 0)))
        dump("land_leads", result["land"], lambda it: (-it["lane"]["land_flip"]["score"], it["lane"]["grade"]))
        return paths

    @staticmethod
    def summary_table(counts: Dict[str, Counter]) -> str:
        hdr = ["bot_source", "total", "resid", "dialer_promoted", "comm", "comm_qual", "land", "land_qual",
               "land_flip_strong", "unknown"]
        keys = ["total", "lane_residential", "dialer_promoted", "lane_commercial", "commercial_qualified",
                "lane_land", "land_qualified", "land_flip_strong", "lane_unknown"]
        rows = [[src] + [c.get(k, 0) for k in keys] for src, c in sorted(counts.items(), key=lambda kv: -kv[1]["total"])]
        tot = ["TOTAL"] + [sum(c.get(k, 0) for c in counts.values()) for k in keys]
        widths = [max(len(str(x[i])) for x in [hdr] + rows + [tot]) for i in range(len(hdr))]
        fmt = lambda r: "  ".join(str(v).ljust(widths[i]) if i == 0 else str(v).rjust(widths[i]) for i, v in enumerate(r))
        return "\n".join([fmt(hdr)] + [fmt(r) for r in rows] + [fmt(tot)])

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
        result: Dict[str, Any] = {"counts": {}, "commercial": [], "land": [], "to_tag": []}
        written = 0
        paths: Dict[str, str] = {}
        try:
            result = self.classify_all(client)
            paths = self.write_reports(result)
            if self.write:
                written = self.write_tags(client, result["to_tag"])
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        scanned = sum(c["total"] for c in result["counts"].values())
        qualified = len(result["commercial"]) + len(result["land"])
        status = "failed" if error_message else "ok" if qualified else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=qualified, staged_count=written,
                            duplicate_count=0, error_message=error_message)
        return {"name": self.name, "status": status, "scanned": scanned,
                "commercial_qualified": len(result["commercial"]), "land_qualified": len(result["land"]),
                "pending_tags": len(result["to_tag"]), "tagged": written, "reports": paths,
                "counts": result["counts"], "error": error_message}


def run() -> dict:
    return PropertyLaneBot(write=True).run()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--write", action="store_true", help="tag commercial/land staging rows (default: report only)")
    args = ap.parse_args()
    try:
        from dotenv import load_dotenv
        load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
    except ImportError:
        pass
    res = PropertyLaneBot(write=args.write).run()
    print(PropertyLaneBot.summary_table(res.get("counts") or {}))
    print({k: v for k, v in res.items() if k != "counts"})
