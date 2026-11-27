"""Fill missing county fields and repair specifically attributed legacy mistakes.

No external data calls, promotion, messaging or paid lookup. Dry run by default
when invoked directly; the scheduled runner explicitly requests writes.
"""
from __future__ import annotations

import argparse
import json
from urllib.parse import quote
from collections import Counter
from ._base import _supabase
from ._free_data import county_patch, hmda_principal_repair, notice_phone_repair, tracked_run
from ._slim_read import STAGING, full_rows, paths, unpack

COLUMNS = ("id,county,property_address,owner_name_records,property_value,"
           "property_value_source,year_built,mortgage_balance,phone")
# The keys changes() reads (county_patch, hmda_principal_repair and
# notice_phone_repair in _free_data). The staging scan selects these instead
# of the whole raw_payload and phone_metadata.
RAW_KEYS = ("comptroller_parcel", "source", "metro_parcel", "rutherford_arcgis", "hamilton_assessor",
            "williamson_inigo", "padctn", "montgomery_gis", "public_contact_candidates")
META_KEYS = ("mortgage_signal", "rod_lookup", "mortgage_estimate", "free_data_repair", "phone_resolver",
             "batchdata_skip_trace", "enformion", "enformion_skip_trace", "dialer_feedback")
SCAN_COLUMNS = (COLUMNS + ",bot_source,rp_permit_cost:raw_payload->permit->Const_Cost,"
                + paths("raw_payload", RAW_KEYS, "rp_") + "," + paths("phone_metadata", META_KEYS, "pm_"))


def _light_row(row):
    cost = row.pop("rp_permit_cost", None)
    row["raw_payload"] = unpack(row, RAW_KEYS, "rp_")
    if cost is not None:
        row["raw_payload"]["permit"] = {"Const_Cost": cost}
    row["phone_metadata"] = unpack(row, META_KEYS, "pm_")
    return row


def changes(row):
    patch = county_patch(row)
    patch.update(hmda_principal_repair(row))
    contact = notice_phone_repair({**row, **patch})
    patch.update(contact)
    return patch


def _may_change(light_row):
    """Does the light row show anything to change? A row the rules cannot
    read is sent on to the full pass, which fails on it as it always did."""
    try:
        return bool(changes(_light_row(light_row)))
    except Exception:
        return True


def apply(client, table, row, patch):
    patch = dict(patch)
    merged = 0
    if table == "homeowner_requests_staging":
        # The existing RPC merges JSON keys inside PostgreSQL. Large notice
        # bodies cannot safely travel in a URL-sized equality filter.
        payload = {"id": row["id"]}
        for key, dest in (("raw_payload", "raw_patch"), ("phone_metadata", "meta_patch")):
            if key in patch:
                before = row.get(key) or {}
                payload[dest] = {k: v for k, v in patch.pop(key).items() if before.get(k) != v}
        if payload.get("raw_patch") or payload.get("meta_patch"):
            merged = int(client.rpc("apply_staging_enrichment", {"p_rows": [payload]}).execute().data or 0)
        if not patch:
            return merged
    elif sum(len(quote(json.dumps(row.get(k) or {}, separators=(",", ":"))))
             for k in ("raw_payload", "phone_metadata")) > 3500:
        # No live-table merge RPC exists. Leave oversized live payloads for
        # transactional operator backfill rather than risk a lost update.
        return 0
    q = client.table(table).update(patch).eq("id", row["id"])
    # Compare the inputs as well as the output fields to avoid overwriting a
    # concurrent county/paid-provider update between the read and write.
    guards = set(patch)
    if table != "homeowner_requests_staging":
        guards |= {"raw_payload", "phone_metadata"}
    if "property_value_source" in patch:
        guards.add("property_value")
    for key in guards:
        value = row.get(key)
        if value is None:
            q = q.is_(key, "null")
        elif isinstance(value, (dict, list)):
            q = q.filter(key, "eq", json.dumps(value, separators=(",", ":")))
        else:
            q = q.eq(key, value)
    return max(merged, len(q.execute().data or []))


def _run(write=True):
    client = _supabase()
    if client is None:
        return {"status": "no_supabase"}
    stats = Counter()
    for table in ("homeowner_requests_staging", "homeowner_requests"):
        after = "00000000-0000-0000-0000-000000000000"
        while True:
            # Live has source, not bot_source. Other fields are shared.
            src = "bot_source" if table.endswith("staging") else "source"
            full = COLUMNS + ",phone_metadata,raw_payload," + src
            if table == STAGING:
                # Light scan, no rejected rows. A row is read in full only
                # when the light columns show there is something to change.
                size = 1000
                page = (client.table(table).select(SCAN_COLUMNS).neq("staging_status", "rejected")
                        .gt("id", after).order("id").limit(size).execute().data) or []
                due = [r["id"] for r in page if _may_change(r)]
                held = full_rows(client, table, full, due)
                rows = [held[i] for i in due if i in held]
            else:
                size = 250
                page = rows = (client.table(table).select(full)
                               .gt("id", after).order("id").limit(size).execute().data) or []
            if not page:
                break
            stats["scanned"] += len(page)
            for row in rows:
                patch = changes(row)
                if not patch:
                    continue
                for key in patch:
                    stats["candidate_" + key] += 1
                if write:
                    n = apply(client, table, row, patch)
                    stats["written" if n else "guarded_skip"] += n or 1
            after = page[-1]["id"]
            if len(page) < size:
                break
    return {"name": "free_county_completion", "status": "ok", "write": write,
            "staged": stats["written"], "fetched": stats["scanned"], **stats}


def run(write=True):
    return tracked_run("free_county_completion", lambda: _run(write), write)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    print(json.dumps(run(write=parser.parse_args().write)))
