"""Free, parcel-bound tax-assessment estimates for freshly seen Memphis notices."""
import json
import time
from datetime import date, datetime, timedelta, timezone
from urllib.parse import quote

from ._base import BotBase, _supabase, guard_phone_metadata
from ._value_guard import require_value_evidence, today_iso
from ._owner_lookup import LegacyTLSAdapter
from ._shelby_trustee_value import HOST, REVIEW, VALUE_SOURCE, lookup


class MemphisCountyValueBot(BotBase):
    name = "memphis_county_value"
    description = "Exact Shelby trustee parcel assessments for current Memphis Daily News notices"
    throttle_seconds = 1.5
    max_attempts = 150  # bounded by the 380 second budget in run()

    def __init__(self):
        super().__init__()
        self.session.mount(HOST + "/", LegacyTLSAdapter(max_retries=0))

    def _candidates(self, client):
        today = date.today()
        rows = (client.table("homeowner_requests_staging")
                .select("id,bot_source,pipeline_lead_key,property_address,county,staging_status,property_value,trustee_sale_date,raw_payload,phone_metadata")
                .eq("bot_source", "memphis_daily_news").eq("county", "shelby")
                .neq("staging_status", "rejected")
                .or_(f"trustee_sale_date.gte.{today.isoformat()},trustee_sale_date.is.null")
                .order("id").limit(500).execute().data or [])
        fresh = []
        for row in rows:
            pm = row.get("phone_metadata") or {}
            if not isinstance(pm, dict) or not isinstance(row.get("raw_payload"), dict):
                continue
            tracking, review = pm.get("notice_tracking") or {}, pm.get(REVIEW) or {}
            if not isinstance(tracking, dict) or not isinstance(review, dict):
                continue
            try:
                seen = datetime.fromisoformat(tracking["last_seen_at"].replace("Z", "+00:00")).date()
            except (ValueError, TypeError, KeyError):
                continue
            if tracking.get("last_seen_by") != "memphis_daily_news" or review.get("checked_on", "") >= today.isoformat():
                continue
            # The county bill is what ties a current owner and parcel to the
            # notice, so a row valued by another source still gets one check.
            # It is read once; only rows with no value are re-read daily.
            upcoming = (row.get("trustee_sale_date") or "") >= today.isoformat()
            if row.get("property_value") is not None:
                if review or not upcoming:
                    continue
            elif not (upcoming or today - timedelta(days=2) <= seen <= today):
                continue
            fresh.append(row)
        return sorted(fresh, key=lambda r: ((r.get("phone_metadata") or {}).get(REVIEW, {}).get("checked_on", ""), r["id"]))

    def _persist(self, client, row, evidence):
        original = row.get("phone_metadata")
        if len(quote(json.dumps(original), safe="")) > 6000:
            return False  # isolate oversized CAS payloads, never blindly overwrite
        patch = {"phone_metadata": {**(original or {}), REVIEW: evidence}}
        valued = row.get("property_value") is not None
        if evidence["status"] == "verified_assessment" and not valued:
            patch.update(property_value=evidence["estimated_appraisal"],
                         property_value_source=VALUE_SOURCE)
            # The bill is for one parcel in Shelby, read on checked_on: or no value.
            require_value_evidence(patch, row, parcel=evidence.get("parcel_id"), county="shelby",
                                   observed_on=evidence.get("checked_on") or today_iso(),
                                   record_address=evidence.get("location"))
        query = (client.table("homeowner_requests_staging").update(patch)
                 .eq("id", row["id"]).eq("bot_source", "memphis_daily_news")
                 .eq("pipeline_lead_key", row["pipeline_lead_key"])
                 .eq("property_address", row["property_address"]).eq("county", "shelby")
                 .eq("staging_status", row["staging_status"]))
        # A value another source already set is kept; the write is refused if it moved.
        query = query.eq("property_value", row["property_value"]) if valued else query.is_("property_value", "null")
        # Preserve all raw evidence; guard the original notice parcel independently.
        parcel = (row["raw_payload"].get("extracted") or {}).get("parcel")
        query = (query.is_("raw_payload->extracted->>parcel", "null") if parcel is None
                 else query.eq("raw_payload->extracted->>parcel", parcel))
        return bool(guard_phone_metadata(query, original).execute().data)

    def run(self):
        started = datetime.now(timezone.utc)
        counts = dict(attempted=0, valued=0, held=0, skipped=0, failed=0)
        error = None
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        try:
            client = _supabase()
            if client is None:
                raise ValueError("no_supabase_client")
            start = time.monotonic()
            for row in self._candidates(client)[:self.max_attempts]:
                if time.monotonic() - start >= 380:
                    break
                counts["attempted"] += 1
                try:
                    evidence = lookup(self, row)
                    if self._persist(client, row, evidence):
                        counts["valued" if evidence["status"] == "verified_assessment" else "held"] += 1
                    else:
                        counts["skipped"] += 1
                except Exception as exc:
                    counts["failed"] += 1
                    self.logger.warning("County lookup/write failed for %s (%s)", row["id"], type(exc).__name__)
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)
        status = "failed" if error else ("ok" if counts["valued"] else "all_dupes")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=counts["attempted"], parsed_count=counts["valued"],
                            staged_count=counts["valued"], duplicate_count=counts["skipped"],
                            error_message=error, notes=counts)
        return {"name": self.name, "status": status, "staged": counts["valued"],
                "fetched": counts["attempted"], "duplicates": counts["skipped"], "error": error, **counts}


def run():
    return MemphisCountyValueBot().run()
