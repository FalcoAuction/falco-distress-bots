"""Trustee status reaper — detects withdrawn / cancelled / reinstated
foreclosure sales by docket disappearance.

The mechanic: TN law (TCA § 35-5-101) requires three consecutive weekly
publications of a substitute trustee sale notice. Our foreclosure
scrapers (nashville_ledger, tn_public_notice, hamilton_county_herald,
memphis_daily_news) re-pull these notices every day. When a notice is
present in a scrape, `_base._write_staging` touches the lead's
`phone_metadata.notice_tracking.last_seen_at`.

If a sale gets withdrawn — homeowner reinstates, loan modifies, or the
property goes off the docket for any reason — the notice stops being
republished. last_seen_at goes stale. This bot walks foreclosure-family
leads with future sale dates and stale last_seen_at, and auto-flags
them as `sale_status.status = 'cancelled'` so they fall out of the
dialer's urgency framing and we stop bugging homeowners who already
resolved their situation.

Threshold: 10 days since last seen, applied only to leads where:
  - distress_type IN ('PRE_FORECLOSURE','TRUSTEE_NOTICE','LIS_PENDENS')
  - trustee_sale_date >= today (still in the future)
  - phone_metadata.sale_status.status NOT already set (we don't
    overwrite manual flags)
  - phone_metadata.notice_tracking.last_seen_at older than 10 days

The 10-day window is wider than the 7-day publication cycle so we
don't false-positive on a delayed scrape or a one-day gap in source
availability.

Distress type: N/A (utility reaper).
"""
from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase


# Distress types that follow the publish-weekly-then-disappear-on-withdrawal
# pattern. CV / demo / probate / tax-lien follow different mechanics.
TRACKED_DISTRESS = (
    "PRE_FORECLOSURE",
    "PREFORECLOSURE",
    "TRUSTEE_NOTICE",
    "LIS_PENDENS",
    "FORECLOSURE",
    "SOT",
    "SUBSTITUTION_OF_TRUSTEE",
    "NOD",
    "NOTICE_OF_DEFAULT",
)

# Days since last_seen_at after which we consider the notice withdrawn.
# Wider than TN's 7-day publish cycle to absorb scrape jitter.
STALE_THRESHOLD_DAYS = 10


class TrusteeStatusReaperBot(BotBase):
    name = "trustee_status_reaper"
    description = (
        "Auto-flag foreclosure leads as cancelled when their notice "
        "disappears from the docket (Goliath-style docket diff)."
    )
    throttle_seconds = 0
    expected_min_yield = 0  # often nothing to do — that's fine

    def scrape(self) -> List[Any]:
        return []

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            return self._fail(started, "no_supabase_client")

        today = date.today()
        stale_cutoff = (datetime.now(timezone.utc) - timedelta(days=STALE_THRESHOLD_DAYS)).isoformat()

        scanned = 0
        flagged = 0
        skipped_no_tracking = 0
        skipped_manual_status = 0
        skipped_past_sale = 0
        skipped_fresh = 0
        error_message: Optional[str] = None

        try:
            # Walk live leads in foreclosure family with future sale dates.
            # Note: we walk live only (not staging) because withdrawn leads
            # already in staging will simply never get promoted; the dialer
            # only renders live rows.
            page = 0
            while True:
                r = (
                    client.table("homeowner_requests")
                    .select("id, distress_type, trustee_sale_date, phone_metadata, property_address")
                    .eq("source", "bot")
                    .in_("distress_type", list(TRACKED_DISTRESS))
                    .range(page * 500, (page + 1) * 500 - 1)
                    .execute()
                )
                rows = getattr(r, "data", None) or []
                if not rows:
                    break

                for row in rows:
                    scanned += 1
                    sale_date_str = row.get("trustee_sale_date")
                    if not sale_date_str:
                        # No sale date set → can't reason about "future sale,
                        # missing notice" so leave it.
                        continue
                    try:
                        sale_date = datetime.strptime(str(sale_date_str)[:10], "%Y-%m-%d").date()
                    except Exception:
                        continue
                    if sale_date < today:
                        skipped_past_sale += 1
                        continue

                    pm = row.get("phone_metadata") or {}
                    if not isinstance(pm, dict):
                        pm = {}

                    # Skip leads whose status is already manually set —
                    # the dialer caller knows more than we do.
                    ss = pm.get("sale_status")
                    if isinstance(ss, dict) and ss.get("status") in (
                        "cancelled", "postponed", "ran", "reinstated"
                    ):
                        skipped_manual_status += 1
                        continue

                    tracking = pm.get("notice_tracking")
                    if not isinstance(tracking, dict):
                        # No tracking timestamp yet — can't tell. Skip
                        # rather than false-positive. The base
                        # _write_staging will populate this on the next
                        # successful scrape match.
                        skipped_no_tracking += 1
                        continue
                    last_seen = tracking.get("last_seen_at")
                    if not last_seen or last_seen >= stale_cutoff:
                        skipped_fresh += 1
                        continue

                    # Flag as cancelled. Use the same status value the
                    # manual flow uses so all downstream consumers
                    # (math sheet, SMS opener, dialer urgency) treat it
                    # the same.
                    pm["sale_status"] = {
                        "status": "cancelled",
                        "note": (
                            f"auto-detected: notice not seen in last "
                            f"{STALE_THRESHOLD_DAYS}+ days "
                            f"(last_seen={last_seen[:10]}, sale_date={sale_date_str[:10]}). "
                            f"Likely withdrawn / reinstated / paid off."
                        ),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "updated_by": "trustee_status_reaper",
                    }
                    try:
                        client.table("homeowner_requests").update(
                            {"phone_metadata": pm}
                        ).eq("id", row["id"]).execute()
                        flagged += 1
                        self.logger.info(
                            f"flagged withdrawn: {row.get('property_address')} "
                            f"(sale={sale_date_str}, last_seen={last_seen[:10]})"
                        )
                    except Exception as e:
                        self.logger.warning(f"flag update failed for {row['id']}: {e}")

                if len(rows) < 500:
                    break
                page += 1
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"reaper failed: {e}")

        self.logger.info(
            f"scanned={scanned} flagged={flagged} "
            f"skipped: no_tracking={skipped_no_tracking} "
            f"manual_status={skipped_manual_status} "
            f"past_sale={skipped_past_sale} fresh={skipped_fresh}"
        )

        status = "failed" if error_message else "ok"
        self._report_health(
            status=status,
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            fetched_count=scanned,
            parsed_count=scanned,
            staged_count=flagged,
            duplicate_count=0,
            error_message=error_message,
        )

        return {
            "name": self.name,
            "status": status,
            "scanned": scanned,
            "flagged": flagged,
            "skipped_no_tracking": skipped_no_tracking,
            "skipped_manual_status": skipped_manual_status,
            "skipped_past_sale": skipped_past_sale,
            "skipped_fresh": skipped_fresh,
            "error": error_message,
            # Map to standard _run_new stat names
            "staged": flagged,
            "duplicates": 0,
            "fetched": scanned,
        }

    def _fail(self, started: datetime, msg: str) -> Dict[str, Any]:
        self._report_health(
            status="failed",
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            fetched_count=0,
            parsed_count=0,
            staged_count=0,
            duplicate_count=0,
            error_message=msg,
        )
        return {
            "name": self.name, "status": "failed", "scanned": 0, "flagged": 0,
            "error": msg, "staged": 0, "duplicates": 0, "fetched": 0,
        }


def run() -> dict:
    bot = TrusteeStatusReaperBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
