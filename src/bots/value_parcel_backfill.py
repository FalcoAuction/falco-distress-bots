"""Repair staged values that were copied by street address.

Until 2026-11-24 xref_avm_enricher copied a value between staged rows that
shared a street address, without checking the parcel. This goes through every
staged row that still carries such a copy and settles it one of five ways:

  replace_with_own_parcel_record  the row holds a county record of its own
                                  parcel (parcel, county, basis, date): the
                                  value and source become that record's
  confirm_exact_parcel            the donor row is provably the same parcel,
                                  county and unit: the value stays, the source
                                  becomes the donor's county source, dated
  clear_cross_parcel              donor and recipient print different parcel
                                  numbers in the same format: the value is
                                  another parcel's and is removed
  label_unverified                nothing proves or disproves it: the value
                                  stays and is marked unverified (review cards
                                  say so)
  source_since_replaced           another enricher has since written the
                                  value under its own source: left alone

Read-only by default. Nothing is fetched from outside Supabase.

    python -m src.bots.value_parcel_backfill                    # dry run, summary
    python -m src.bots.value_parcel_backfill --out report.json  # dry run, every row
    python -m src.bots.value_parcel_backfill --write --expect N # N = "changes" from the dry run

--write refuses to start unless --expect equals the number of changes the
plan holds at that moment. Each write is conditional on the value and source
still being what the plan read, so a row another bot touched is skipped. A
second run plans zero changes.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import _supabase
from ._value_guard import (XREF_EXACT, XREF_KEY, XREF_SETTLED, XREF_SOURCE, copy_decision, own_parcel_value,
                           row_parcels, value_status, _d)

TABLE = "homeowner_requests_staging"
COLUMNS = ("id,bot_source,county,property_address,staging_status,property_value,property_value_source,"
           "property_value_as_of,owner_name_records,full_name,raw_payload,phone_metadata")
UNVERIFIED = "unverified_address_only"
CHANGES = ("replace_with_own_parcel_record", "confirm_exact_parcel", "clear_cross_parcel", "label_unverified")


def _cross_parcel(donor: Dict[str, Any], row: Dict[str, Any]) -> bool:
    """Both rows print a parcel number in the same format and they differ."""
    mine, theirs = row_parcels(row), row_parcels(donor)
    if not mine or not theirs or mine & theirs:
        return False
    return bool({len(f) for f in mine} & {len(f) for f in theirs})


def plan_row(row: Dict[str, Any], donor: Optional[Dict[str, Any]], now: str) -> Dict[str, Any]:
    """Decide what to do with one row. Pure: no reads, no writes."""
    meta = _d(_d(row.get("phone_metadata")).get(XREF_KEY))
    source = row.get("property_value_source")
    value = row.get("property_value")
    base = {"id": row["id"], "previous_value": value, "previous_source": source}
    if meta.get("match") == XREF_EXACT or meta.get("status") in XREF_SETTLED:
        return {**base, "action": "already_settled"}
    if value in (None, "") or float(value) <= 0:
        return {**base, "action": "no_value"}
    if source != XREF_SOURCE:
        return {**base, "action": "source_since_replaced", "status": value_status(row)["status"]}
    note = {"backfilled_at": now, "previous_value": float(value), "previous_source": source,
            "donor_id": meta.get("donor_id"), "donor_table": meta.get("donor_table")}
    own = own_parcel_value(row)
    if own:
        return {**base, "action": "replace_with_own_parcel_record",
                "columns": {"property_value": int(round(own["value"])), "property_value_source": own["source"],
                            "property_value_as_of": own["observed_on"]},
                "meta": {**note, "status": "replaced_by_own_parcel_record", "parcel": own["parcel"],
                         "county": own["county"], "basis": own["basis"], "observed_on": own["observed_on"]},
                "value_changes": abs(own["value"] - float(value)) > 1}
    reason = "donor_missing"
    if donor is not None:
        decision = copy_decision(donor, row)
        if decision["ok"] and abs((decision["value"] or 0) - float(value)) <= 1:
            return {**base, "action": "confirm_exact_parcel",
                    "columns": {"property_value_source": decision["basis_source"],
                                "property_value_as_of": decision["observed_on"]},
                    "meta": {**note, "source": XREF_SOURCE, "match": XREF_EXACT, "parcel": decision["parcel"],
                             "county": decision["county"], "basis": decision["basis"],
                             "basis_source": decision["basis_source"], "observed_on": decision["observed_on"],
                             "confidence": 0.8}}
        reason = "donor_value_changed" if decision["ok"] else decision["reason"]
        if (reason == "donor_value_unverified" and row_parcels(row) & row_parcels(donor)
                and value_status(donor)["reason"] == "the parcel record has no observation date"):
            # Not a cross-parcel copy: the parcel numbers agree. What is
            # missing is the date the donor's county record was read.
            reason = "same_parcel_donor_record_undated"
        if _cross_parcel(donor, row):
            return {**base, "action": "clear_cross_parcel",
                    "columns": {"property_value": None, "property_value_source": None, "property_value_as_of": None},
                    "meta": {**note, "status": "removed_cross_parcel",
                             "parcels": sorted(row_parcels(row)), "donor_parcels": sorted(row_parcels(donor)),
                             "note": "The value belonged to a different parcel at the same street."}}
    if meta.get("status") == UNVERIFIED:
        return {**base, "action": "already_labeled", "reason": meta.get("reason")}
    return {**base, "action": "label_unverified", "reason": reason,
            "meta": {**{k: v for k, v in meta.items() if k not in ("status", "reason")},
                     "status": UNVERIFIED, "reason": reason, "labeled_at": now}}


def fetch(client) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        page = (client.table(TABLE).select(COLUMNS)
                .or_(f"property_value_source.eq.{XREF_SOURCE},phone_metadata->{XREF_KEY}.not.is.null")
                .gt("id", after).order("id").limit(200).execute().data) or []
        rows.extend(page)
        if len(page) < 200:
            return rows
        after = page[-1]["id"]


def fetch_donors(client, rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    wanted: Dict[str, set] = {}
    for r in rows:
        meta = _d(_d(r.get("phone_metadata")).get(XREF_KEY))
        if meta.get("donor_id") and r.get("property_value_source") == XREF_SOURCE:
            wanted.setdefault(meta.get("donor_table") or TABLE, set()).add(meta["donor_id"])
    out: Dict[str, Dict[str, Any]] = {}
    for table, ids in wanted.items():
        cols = COLUMNS if table == TABLE else COLUMNS.replace("bot_source,", "").replace("staging_status,", "")
        ids = sorted(ids)
        for i in range(0, len(ids), 50):
            for d in (client.table(table).select(cols).in_("id", ids[i:i + 50]).execute().data) or []:
                out[d["id"]] = d
    return out


def apply_plan(client, row: Dict[str, Any], plan: Dict[str, Any]) -> bool:
    """One conditional write. The columns move only if the value and source
    are still what the plan read; the label is merged only after that."""
    columns = plan.get("columns")
    if columns:
        q = client.table(TABLE).update(columns).eq("id", row["id"]).eq("property_value_source", XREF_SOURCE)
        q = q.eq("property_value", row["property_value"])
        if not (q.execute().data or []):
            return False
    else:
        still = (client.table(TABLE).select("id").eq("id", row["id"]).eq("property_value_source", XREF_SOURCE)
                 .eq("property_value", row["property_value"]).limit(1).execute().data) or []
        if not still:
            return False
    client.rpc("apply_staging_enrichment", {"p_rows": [{"id": row["id"], "meta_patch": {XREF_KEY: plan["meta"]}}]}).execute()
    return True


def run(write: bool = False, expect: Optional[int] = None, out: Optional[str] = None) -> Dict[str, Any]:
    client = _supabase()
    if client is None:
        return {"status": "no_supabase"}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = fetch(client)
    donors = fetch_donors(client, rows)
    counts: Counter = Counter()
    reasons: Counter = Counter()
    by_source: Counter = Counter()
    detail: List[Dict[str, Any]] = []
    first_trace: List[Dict[str, Any]] = []
    plans = []
    for row in rows:
        meta = _d(_d(row.get("phone_metadata")).get(XREF_KEY))
        plan = plan_row(row, donors.get(meta.get("donor_id")), now)
        counts[plan["action"]] += 1
        if plan["action"] == "label_unverified":
            reasons[plan["reason"]] += 1
        if row.get("property_value_source") == XREF_SOURCE:
            by_source[row.get("bot_source")] += 1
        card = _d(_d(row.get("raw_payload")).get("pilot_review"))
        line = {"id": row["id"], "bot_source": row.get("bot_source"), "county": row.get("county"),
                "address": row.get("property_address"), "value": row.get("property_value"),
                "source": row.get("property_value_source"), "action": plan["action"],
                "reason": plan.get("reason"), "new": plan.get("columns"),
                "card": {"lane": card.get("lane"), "route": card.get("route")} if card else None}
        detail.append(line)
        if card.get("route") == "first_trace_review" and row.get("property_value_source") == XREF_SOURCE:
            first_trace.append({**line, "exact_parcel_value_available":
                                plan["action"] in ("replace_with_own_parcel_record", "confirm_exact_parcel")})
        if plan["action"] in CHANGES:
            plans.append((row, plan))
    result: Dict[str, Any] = {
        "status": "ok", "write": write, "rows_read": len(rows),
        "rows_with_address_copy_source": sum(by_source.values()),
        "by_bot_source": dict(by_source.most_common()), "actions": dict(counts),
        "unverified_reasons": dict(reasons), "changes": len(plans),
        "first_trace_cards": first_trace, "written": 0, "skipped_changed_since_read": 0}
    if out:
        with open(out, "w", encoding="utf-8") as fh:
            json.dump({**result, "rows": detail}, fh, indent=1, default=str)
    if not write:
        return result
    if expect != len(plans):
        return {**result, "status": "refused",
                "error": f"--expect {expect} does not match the {len(plans)} changes planned now; rerun the dry run"}
    for row, plan in plans:
        if apply_plan(client, row, plan):
            result["written"] += 1
        else:
            result["skipped_changed_since_read"] += 1
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--write", action="store_true", help="apply the plan (default: dry run)")
    parser.add_argument("--expect", type=int, help="required with --write: the 'changes' count from the dry run")
    parser.add_argument("--out", help="write the full per-row plan to this JSON file")
    args = parser.parse_args()
    print(json.dumps(run(write=args.write, expect=args.expect, out=args.out), indent=1, default=str))
