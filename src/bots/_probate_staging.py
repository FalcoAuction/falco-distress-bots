"""Refresh probate evidence under historical pipeline keys on recurring runs."""
import json
from collections import defaultdict
from datetime import date, datetime

from ._base import BotBase, _supabase


def guard_raw(query, raw):
    if raw is None:
        return query.is_("raw_payload", "null")
    return query.filter("raw_payload", "eq", json.dumps(raw, separators=(",", ":"), ensure_ascii=False))


def _date(value):
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%B %d, %Y", "%A, %B %d, %Y"):
        try:
            return datetime.strptime(value or "", fmt).date()
        except (ValueError, TypeError):
            pass
    return None


def remember_notice(group, lead):
    """Keep a closure even if another edition repeats the creditor notice."""
    key = lead.pipeline_lead_key
    old = group.get(key)
    if old is None:
        group[key] = lead
        return
    if not (lead.raw_payload or {}).get("probate_review") or not (old.raw_payload or {}).get("probate_review"):
        return
    def rank(item):
        raw = item.raw_payload
        review = raw["probate_review"]
        return (review["stage"] == "estate_inactive_reported", review["stage"] == "sale_interrupted",
                _date(raw.get("publication_date")) or date.min, review["attorney_draft_status"] == "hold")
    if rank(lead) > rank(old):
        group[key] = lead


def refresh_patch(row, lead):
    """Keep the decision and enrichment when updating consistently identified evidence."""
    old = row.get("raw_payload") or {}
    new = lead.raw_payload
    if not isinstance(old, dict):
        return None
    before, after = old.get("court_notice") or {}, new["court_notice"]
    for field in ("decedent", "docket", "county"):
        if before.get(field) and str(before[field]).casefold() != str(after.get(field) or "").casefold():
            return None
    old_date, new_date = _date(old.get("publication_date")), _date(new.get("publication_date"))
    inactive = new["probate_review"]["stage"] == "estate_inactive_reported"
    if old_date and (not new_date or (new_date < old_date and not inactive)):
        return None
    same_document = old.get("notice_id") == new.get("notice_id") and old.get("publication") == new.get("publication")
    if not same_document and not (old_date and new_date and (new_date >= old_date or inactive)):
        return None
    previous = old.get("probate_review") or {}
    if (previous.get("stage") in {"estate_inactive_reported", "sale_interrupted"} and not inactive
            and previous.get("stage") != new["probate_review"]["stage"]):
        return None
    if before == after and previous == new["probate_review"] and all(
        old.get(k) == new.get(k) for k in ("text", "excerpt", "publication_date", "notice_id")
    ):
        return None
    merged = {**old, **new}
    # A failed identity lookup may be retried when the underlying evidence improves.
    if before != after:
        merged.pop("probate_match_attempt", None)
    patch = {"raw_payload": merged, "source_url": lead.source_url}
    if lead.distress_type == "PROBATE" and not row.get("full_name") and after.get("decedent"):
        patch["full_name"] = after["decedent"]
    return patch


class ProbateNoticeBot(BotBase):
    def _write_staging(self, leads):
        groups = defaultdict(dict)
        ordinary = []
        duplicates = 0
        for lead in leads:
            if not (lead.raw_payload or {}).get("probate_review"):
                ordinary.append(lead)
                continue
            group = groups[lead.bot_source]
            if lead.pipeline_lead_key in group:
                duplicates += 1
            remember_notice(group, lead)
        if not groups:
            return super()._write_staging(ordinary)
        client = _supabase()
        for source, group in groups.items():
            keys = list(group)
            for offset in range(0, len(keys), 50):
                batch = keys[offset:offset + 50]
                try:
                    staging = client.table("homeowner_requests_staging")
                    rows = (staging.select("id,pipeline_lead_key,staging_status,full_name,raw_payload")
                            .eq("bot_source", source).in_("pipeline_lead_key", batch).execute().data or [])
                    existing = defaultdict(list)
                    for row in rows:
                        existing[row["pipeline_lead_key"]].append(row)
                    live = (client.table("homeowner_requests").select("pipeline_lead_key").eq("source", "bot")
                            .in_("pipeline_lead_key", batch).execute().data or [])
                    live_keys = {row["pipeline_lead_key"] for row in live}
                except Exception as exc:
                    self.logger.warning("Probate identity lookup failed (%s)", type(exc).__name__)
                    continue
                for key in batch:
                    matches = existing[key]
                    if not matches and key not in live_keys:
                        ordinary.append(group[key])
                        continue
                    duplicates += 1
                    if len(matches) != 1 or matches[0]["staging_status"] not in {"pending", "promoted"}:
                        continue
                    row = matches[0]
                    patch = refresh_patch(row, group[key])
                    if patch:
                        try:
                            update = (staging.update(patch).eq("id", row["id"]).eq("bot_source", source)
                                      .eq("staging_status", row["staging_status"]).eq("pipeline_lead_key", key))
                            guard_raw(update, row.get("raw_payload")).execute()
                        except Exception as exc:
                            self.logger.warning("Probate refresh deferred for %s (%s)", row["id"], type(exc).__name__)
        staged, skipped = super()._write_staging(ordinary)
        return staged, skipped + duplicates
