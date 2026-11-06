"""Asset profile backfill: tags every staged lead that has an assessor owner
match with what the owner holds (parcel count, acreage, value, land use)
and a tier (land / commercial / multi_parcel / single_residence).

The probate enricher writes the profile at match time; this catches rows
matched before that, and any row whose match changed. No network: it reads
raw_payload only. Writes raw_payload.asset_profile.

Distress type: N/A (enricher only).
"""
from __future__ import annotations

import traceback as tb
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List

from ._asset_profile import asset_profile
from ._base import BotBase, _supabase

PAGE = 200


class AssetProfileBot(BotBase):
    name = "asset_profile"
    description = "Tag matched leads with parcel count, acreage and land/multi-parcel tier"

    def scrape(self) -> List[Any]:
        return []

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

        scanned = updated = unchanged = 0
        tiers: Counter = Counter()
        error_message = None
        try:
            # Keyset pages over valued or addressed rows only (an offset scan
            # with a JSON filter over all of staging hits the statement timeout).
            seen = set()
            for key in ("owner_match", "padctn_owner_match"):
                after = "00000000-0000-0000-0000-000000000000"
                while True:
                    page = (client.table("homeowner_requests_staging").select("id,raw_payload")
                            .gt("id", after).not_.is_("property_address", "null")
                            .not_.is_(f"raw_payload->{key}", "null")
                            .order("id").limit(PAGE).execute().data) or []
                    if not page:
                        break
                    after = page[-1]["id"]
                    rows = [r for r in page if r["id"] not in seen]
                    seen.update(r["id"] for r in rows)
                    for row in rows:
                        scanned += 1
                        raw = row.get("raw_payload") or {}
                        profile = asset_profile(raw)
                        if not profile:
                            continue
                        tiers[profile["tier"]] += 1
                        if raw.get("asset_profile") == profile:
                            unchanged += 1
                            continue
                        raw["asset_profile"] = profile
                        try:
                            client.table("homeowner_requests_staging").update({"raw_payload": raw}).eq("id", row["id"]).execute()
                            updated += 1
                        except Exception as e:
                            self.logger.warning(f"  update failed id={row['id']}: {e}")
                    if len(page) < PAGE:
                        break
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        status = "failed" if error_message else "ok" if updated else "all_dupes" if unchanged else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=updated,
                            duplicate_count=unchanged, error_message=error_message)
        self.logger.info(f"scanned={scanned} updated={updated} unchanged={unchanged} tiers={dict(tiers)}")
        return {"name": self.name, "status": status, "scanned": scanned, "updated": updated,
                "unchanged": unchanged, "tiers": dict(tiers), "error": error_message}


def run() -> dict:
    return AssetProfileBot().run()


if __name__ == "__main__":
    print(run())
