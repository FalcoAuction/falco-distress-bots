"""Revisit the evidence on the exact WARN notice/county staging row."""
from collections import defaultdict
from datetime import date, timedelta
import logging
from uuid import NAMESPACE_URL, uuid5

from ._probate_staging import guard_raw
from ._warn_metro import worksite_parcel


HISTORY_LIMIT = 12
LOG = logging.getLogger(__name__)


def remember(history, event):
    # Repeated letter outages or alternating PDF revisions must not make the
    # optimistic-concurrency filter grow on every run. Keep the newest visit
    # to each distinct material state within a bounded history.
    def identity(snapshot):
        return {k: v for k, v in snapshot.items() if k != "checked_on"}

    return [x for x in history if identity(x) != identity(event)][-(HISTORY_LIMIT - 1):] + [event]


def attach_parcel(lead, old, fetch, today):
    rec = lead.raw_payload["warn"]
    review = lead.raw_payload["warn_review"]
    site = review.get("worksite")
    if (lead.county != "davidson" or review["verification"] != "verified"
            or review["event_type"] != "permanent_closure" or not site):
        return
    prior = old.get("warn") or {}
    cached = prior.get("worksite_parcel")
    if (isinstance(cached, dict) and cached.get("parcel_id") and cached.get("county") == "davidson"
            and cached.get("printed_worksite", prior.get("worksite")) == site):
        rec["worksite_parcel"] = cached
        try:
            age = (today - date.fromisoformat(cached.get("checked_on", ""))).days
            if 0 <= age < 30 and prior.get("worksite_parcel_status") != "previous_observation":
                return
        except (TypeError, ValueError):
            pass
    match = worksite_parcel(site, fetch)
    if match:
        rec["worksite_parcel"] = {**match, "printed_worksite": site, "checked_on": today.isoformat()}
    elif rec.get("worksite_parcel"):
        rec["worksite_parcel_status"] = "previous_observation"


def merge_observation(old, incoming):
    before = old.get("warn") or {}
    previous = old.get("warn_review") or {}
    rec = {**before, **incoming["warn"]}
    review = dict(incoming["warn_review"])
    # Unknown or conflicting evidence cannot inherit the old worksite as current.
    if (review["verification"] == "letter_unavailable" and before.get("worksite_parcel")
            and before.get("letter_url") == rec.get("letter_url")):
        rec["worksite_parcel"] = before["worksite_parcel"]
        rec["worksite_parcel_status"] = "previous_observation"
    elif "worksite_parcel" not in incoming["warn"]:
        rec.pop("worksite_parcel", None)
        rec.pop("worksite_parcel_status", None)
    elif "worksite_parcel_status" not in incoming["warn"]:
        rec.pop("worksite_parcel_status", None)
    if before.get("asset_disposition") not in (None, "", "unverified"):
        rec["asset_disposition"] = before["asset_disposition"]
        review["asset_disposition"] = before["asset_disposition"]
        if before.get("disposition_source"):
            review["disposition_source"] = before["disposition_source"]
    fields = ("verification", "event_type", "event_statement", "worksites", "employment_window",
              "source_url", "letter_sha256", "timing", "asset_disposition")
    history = list(old.get("warn_history") or [])[-HISTORY_LIMIT:]
    if previous and any(previous.get(k) != review.get(k) for k in fields):
        history = remember(history, {k: previous.get(k) for k in (*fields, "notice", "checked_on", "disposition_source")})
    elif not previous and before:
        history = remember(history, {"legacy_observation": before})
    if before.get("worksite_parcel") and before.get("worksite_parcel") != rec.get("worksite_parcel"):
        history = remember(history, {"prior_worksite": before.get("worksite"), "worksite_parcel": before["worksite_parcel"]})
    merged = {**old, "warn": rec, "warn_review": review}
    if history:
        merged["warn_history"] = history
    return merged


def refresh(client, leads, fetch, today, lookback, run_id):
    table = client.table("homeowner_requests_staging")
    existing = defaultdict(list)
    after = "00000000-0000-0000-0000-000000000000"
    while True:
        page = (table.select("id,pipeline_lead_key,county,raw_payload").eq("bot_source", "warn_notices")
                .gt("id", after).order("id").limit(100).execute().data)
        for row in page:
            existing[row["pipeline_lead_key"]].append(row)
        if len(page) < 100:
            break
        after = page[-1]["id"]
    inserted = duplicates = 0
    observed = set()

    def save(row, old, new):
        if new != old:
            update = (table.update({"raw_payload": new}).eq("id", row["id"]).eq("bot_source", "warn_notices")
                      .eq("pipeline_lead_key", row["pipeline_lead_key"]).eq("county", row["county"]))
            if not guard_raw(update, old).execute().data:
                raise RuntimeError("Concurrent WARN update; retry on next collection")

    for lead in leads:
        key = lead.pipeline_lead_key
        observed.add(key)
        matches = existing.get(key, [])
        if len(matches) > 1:
            LOG.warning("Ambiguous staged WARN key %s (%d rows)", key, len(matches))
            duplicates += 1
            continue
        old = matches[0]["raw_payload"] if matches else {}
        prior = old.get("warn") or {}
        rec = lead.raw_payload["warn"]
        if matches and (matches[0]["county"] != lead.county or prior.get("notice") != rec["notice"]):
            LOG.warning("WARN notice/county identity mismatch for %s", key)
            duplicates += 1
            continue
        if matches and prior.get("company") != rec["company"]:
            # A reused number or conflicting index row cannot refresh the old
            # employer's property. Keep both observations available for review.
            review = {**lead.raw_payload["warn_review"], "verification": "identity_review",
                      "event_type": "review", "worksite": None, "worksites": [], "timing": "unknown"}
            held = dict(prior)
            if held.get("worksite_parcel"):
                held["worksite_parcel_status"] = "previous_observation"
            save(matches[0], old, merge_observation(old, {"warn": held, "warn_review": review}))
            duplicates += 1
            continue
        if not matches and lead.raw_payload["warn_review"]["verification"] == "ambiguous_index":
            LOG.warning("Withholding conflicting new WARN notice %s", key)
            continue
        attach_parcel(lead, old, fetch, today)
        if matches:
            save(matches[0], old, merge_observation(old, lead.raw_payload))
            duplicates += 1
            continue
        # Historical promotions may have no remaining staging row.
        if (client.table("homeowner_requests").select("id").eq("source", "bot")
                .eq("pipeline_lead_key", key).limit(1).execute().data):
            duplicates += 1
            continue
        row = lead.as_db_row(run_id)
        row["id"] = str(uuid5(NAMESPACE_URL, "falco:warn_notices:" + key))
        result = table.upsert(row, on_conflict="id", ignore_duplicates=True).execute()
        inserted += bool(result.data)
        duplicates += not result.data

    # Loss of a listing is not a withdrawal. Preserve the original memo and
    # last positive review; clear the current verification in the review layer.
    for key in existing.keys() - observed:
        if len(existing[key]) != 1:
            continue
        row = existing[key][0]
        old = row["raw_payload"] or {}
        rec = old.get("warn") or {}
        if not rec.get("notice") or not rec.get("posted"):
            continue
        status = "outside_window" if rec["posted"] < (today - timedelta(days=lookback)).isoformat() else "not_on_index"
        previous = old.get("warn_review") or {}
        review = {**previous, "verification": status, "checked_on": today.isoformat(), "event_type": "review",
                  "timing": "unknown", "worksite": None, "worksites": [], "notice": rec["notice"],
                  "source_url": rec.get("letter_url"), "asset_disposition": rec.get("asset_disposition", "unverified")}
        if previous.get("verification") == status:
            continue
        history = remember(old.get("warn_history") or [], previous or {"legacy_observation": rec})
        held = dict(rec)
        if held.get("worksite_parcel"):
            held["worksite_parcel_status"] = "previous_observation"
        save(row, old, {**old, "warn": held, "warn_review": review, "warn_history": history})
    return inserted, duplicates
