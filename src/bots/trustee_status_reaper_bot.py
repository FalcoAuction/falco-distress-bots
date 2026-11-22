"""Track stale foreclosure notices for review, without inferring a sale outcome."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from ._base import BotBase, BotTimeout, _supabase, guard_phone_metadata


TRACKED_DISTRESS = (
    "PRE_FORECLOSURE", "PREFORECLOSURE", "TRUSTEE_NOTICE", "LIS_PENDENS",
    "FORECLOSURE", "SOT", "SUBSTITUTION_OF_TRUSTEE", "NOD", "NOTICE_OF_DEFAULT",
)
STALE_THRESHOLD_DAYS = 10
REAPER = "trustee_status_reaper"
REVIEW_KEY = "trustee_notice_review"


def review_metadata(row, now):
    """Return a replacement only when this bot's evidence or legacy flag changes."""
    before = row.get("phone_metadata")
    if not isinstance(before, dict):
        return None
    after = before.copy()
    status = before.get("sale_status")
    if (isinstance(status, dict) and status.get("updated_by") == REAPER
            and status.get("status") == "cancelled"):
        # The old writer inferred this from age alone. Retire it even if the
        # sale has passed or this lead has since been reclassified.
        del after["sale_status"]
    review = before.get(REVIEW_KEY)
    if REVIEW_KEY in before and not (isinstance(review, dict) and review.get("updated_by") == REAPER):
        return after if after != before else None

    try:
        sale = date.fromisoformat(row.get("trustee_sale_date") or "")
    except (TypeError, ValueError):
        sale = None
    tracking = before.get("notice_tracking")
    seen = tracking.get("last_seen_at") if isinstance(tracking, dict) else None
    applicable = (not after.get("sale_status") and row.get("distress_type") in TRACKED_DISTRESS
                  and sale is not None and sale >= now.date())
    if not applicable:
        after.pop(REVIEW_KEY, None)
    else:
        try:
            timestamp = datetime.fromisoformat(seen.replace("Z", "+00:00")) if isinstance(seen, str) else None
        except ValueError:
            timestamp = None
        if timestamp is not None and timestamp.tzinfo is not None and timestamp <= now:
            if timestamp < now - timedelta(days=STALE_THRESHOLD_DAYS):
                after[REVIEW_KEY] = dict(updated_by=REAPER, reason="stale_notice",
                                         last_seen_at=seen, sale_date=sale.isoformat())
            else:
                after.pop(REVIEW_KEY, None)
    return after if after != before else None


class TrusteeStatusReaperBot(BotBase):
    name = REAPER
    description = "Mark stale notices for review and retire legacy inferred cancellations."
    throttle_seconds = 0
    expected_min_yield = 0

    def scrape(self):
        return []

    def run(self):
        started = datetime.now(timezone.utc)
        scanned = updated = conflicts = failed_writes = 0
        error = None
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        try:
            client = _supabase()
            if client is None:
                raise RuntimeError("no_supabase_client")
            cursor = None
            while True:
                query = (client.table("homeowner_requests")
                         .select("id,distress_type,trustee_sale_date,phone_metadata")
                         .eq("source", "bot")
                         .or_(f"distress_type.in.({','.join(TRACKED_DISTRESS)}),"
                              f"phone_metadata->sale_status->>updated_by.eq.{REAPER},"
                              f"phone_metadata->{REVIEW_KEY}->>updated_by.eq.{REAPER}"))
                if cursor is not None:
                    query = query.gt("id", cursor)
                rows = query.order("id").limit(500).execute().data or []
                for row in rows:
                    scanned += 1
                    metadata = review_metadata(row, started)
                    if metadata is None:
                        continue
                    update = (client.table("homeowner_requests").update({"phone_metadata": metadata})
                              .eq("id", row["id"]).eq("source", "bot"))
                    for key in ("distress_type", "trustee_sale_date"):
                        update = update.is_(key, "null") if row[key] is None else update.eq(key, row[key])
                    try:
                        written = guard_phone_metadata(update, row["phone_metadata"]).execute().data
                    except BotTimeout:
                        raise
                    except Exception as exc:
                        # One oversized JSON guard or transient row error must not
                        # starve later pages. Never retry without the guard.
                        failed_writes += 1
                        self.logger.warning("Review write failed for %s (%s)", row["id"], type(exc).__name__)
                        continue
                    if written:
                        updated += 1
                    else:
                        conflicts += 1
                if len(rows) < 500:
                    break
                cursor = rows[-1]["id"]
        except BotTimeout:
            raise
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
            self.logger.exception("Trustee notice review failed")
        if failed_writes and not error:
            error = f"{failed_writes} guarded write(s) failed"
        status = "failed" if error else "ok"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=updated,
                            duplicate_count=0, error_message=error, notes={"cas_conflicts": conflicts})
        return dict(name=self.name, status=status, scanned=scanned, updated=updated,
                    cas_conflicts=conflicts, error=error, staged=updated, duplicates=0, fetched=scanned)


def run():
    return TrusteeStatusReaperBot().run()


if __name__ == "__main__":
    print(run())
