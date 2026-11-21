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


def changes(row):
    patch = county_patch(row)
    patch.update(hmda_principal_repair(row))
    contact = notice_phone_repair({**row, **patch})
    patch.update(contact)
    return patch


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
            rows = (client.table(table).select("id,county,property_address,owner_name_records,property_value,"
                    "property_value_source,year_built,mortgage_balance,phone,phone_metadata,raw_payload," + src)
                    .gt("id", after).order("id").limit(250).execute().data) or []
            if not rows:
                break
            for row in rows:
                stats["scanned"] += 1
                patch = changes(row)
                if not patch:
                    continue
                for key in patch:
                    stats["candidate_" + key] += 1
                if write:
                    n = apply(client, table, row, patch)
                    stats["written" if n else "guarded_skip"] += n or 1
            after = rows[-1]["id"]
            if len(rows) < 250:
                break
    return {"name": "free_county_completion", "status": "ok", "write": write,
            "staged": stats["written"], "fetched": stats["scanned"], **stats}


def run(write=True):
    return tracked_run("free_county_completion", lambda: _run(write), write)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true")
    print(json.dumps(run(write=parser.parse_args().write)))
