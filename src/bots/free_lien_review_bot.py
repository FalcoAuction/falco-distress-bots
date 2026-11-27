"""Consume the lien queue with existing free evidence, never subscriptions."""
from __future__ import annotations

from datetime import datetime, timezone
from ._base import _supabase
from ._free_data import tracked_run
from ._slim_read import STAGING, full_rows
from .free_county_completion_bot import apply
from ..enrichment.shortlist import county_key


def review(row):
    raw = row.get("raw_payload") or {}
    prior = raw.get("lien_check") or {}
    if prior.get("status") != "queued":
        return {}
    shelby = county_key(row.get("county")) == "shelby"
    evidence = {k: raw[k] for k in ("loan", "deed_of_trust", "structured", "notice_reparse") if raw.get(k)}
    pm = row.get("phone_metadata") or {}
    if pm.get("mortgage_signal"):
        evidence["mortgage_signal"] = pm["mortgage_signal"]
    return {"raw_payload": {**raw, "lien_check": {**prior,
        "status": "pending_free_lookup" if shelby else "needs_free_records_review",
        "register_system": "shelby_register_free" if shelby else prior.get("register_system"),
        "spend_authorized": False, "available_evidence": evidence,
        "equity": None, "payoff_verified": False,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
        "next_action": "shelby_free_chain" if shelby else "Obtain existing county records or owner-authorized documents; no paid lookup"}}}


def _run(write=True):
    client = _supabase()
    if client is None:
        return {"status": "no_supabase"}
    scanned = written = 0
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        # Light scan: only a queued lien check is work, and a rejected row is
        # never worked. The payload is read for the queued rows alone.
        page = (client.table(STAGING).select("id,lien_status:raw_payload->lien_check->>status")
            .neq("staging_status", "rejected").gt("id", after).order("id")
            .limit(1000).execute().data) or []
        if not page:
            break
        queued = [r["id"] for r in page if r.get("lien_status") == "queued"]
        rows = full_rows(client, STAGING, "id,county,raw_payload,phone_metadata", queued)
        for row in (rows[i] for i in queued if i in rows):
            patch = review(row)
            scanned += bool(patch)
            if write and patch:
                written += apply(client, STAGING, row, patch)
        after = page[-1]["id"]
    return {"name": "free_lien_review", "status": "ok", "scanned": scanned, "written": written,
            "staged": written, "fetched": scanned}


def run(write=True):
    return tracked_run("free_lien_review", lambda: _run(write), write)
