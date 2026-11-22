"""
Personal-property lane — tags the few leads worth a high-value estate,
business liquidation, seizure, or owner-asset auction.

Rules live in `_personal_property.py` (pure, tested). This bot only reads
leads, classifies them, and writes `raw_payload.personal_property` on
homeowner_requests_staging and homeowner_requests. No schema change.
Leads that no longer qualify have the key removed, so the tag always
reflects today's rules and data.

Runs after the signal bots it reads: faa_aircraft (raw_payload.aircraft),
ffl_overlay (raw_payload.ffl), obituary_signals (raw_payload.obituary),
and whatever fills raw_payload.outbuildings.

Distress type: N/A — utility enricher.
"""

from __future__ import annotations

import sys
import json
import traceback as tb
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ._base import BotBase, _supabase
from ._personal_property import classify, dedupe_key

# Only the raw_payload keys the rules read (JSON-path selects keep pages small).
_RAW_TEXT = ("parcel", "cumulative_owed", "years_delinquent", "owner_name_1", "chapter", "case_name",
             "court_id", "docket_number", "courtlistener_docket_url", "case_number", "docket_url", "state",
             "source", "kind", "attorney", "petitioner", "petitioner_address", "filed", "debtor",
             "trustee", "sale_date")
_RAW_JSON = ("court_notice", "warn", "aircraft", "ffl", "outbuildings", "obituary", "vessel_case",
             "fl_tpp", "sunbiz_event", "vessel", "b2b_contacts", "personal_property_evidence")
_BASE_COLS = "id, distress_type, full_name, owner_name_records, property_value, trustee_sale_date, county"


class PersonalPropertyLaneBot(BotBase):
    name = "personal_property_lane"
    description = "Tag leads worth a high-value estate / liquidation / seizure / owner-asset auction"
    throttle_seconds = 0.0
    expected_min_yield = 0
    frozen_after_runs = 10_000      # enricher, not a source
    page_size = 1000
    max_pages = 200

    def scrape(self):
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
            return {"name": self.name, "status": "no_supabase", "tagged": 0, "cleared": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        scanned = tagged = cleared = unchanged = 0
        first_seen: Dict[str, str] = {}
        lanes: Counter = Counter()
        error_message: Optional[str] = None
        try:
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                for row in self._rows(client, table):
                    scanned += 1
                    raw = {k: row.get(k) for k in _RAW_TEXT + _RAW_JSON if row.get(k) is not None}
                    lead = {**row, "raw_payload": raw}
                    tag = classify(lead)
                    if tag:
                        key = dedupe_key(tag, lead, raw)
                        tag["dedupe_key"] = key
                        if key is not None and key in first_seen and first_seen[key] != row["id"]:
                            tag = {**tag, "duplicate_of": first_seen[key], "routable": False, "prospect": False,
                                   "not_routable_reason": "duplicate of another lead", "route_to": None}
                        elif key is not None:
                            first_seen[key] = row["id"]
                    if tag:
                        lanes[f"{tag['lane']}:{tag['grade']}"] += 1
                    if tag == row.get("_pp"):
                        unchanged += 1
                        continue
                    full = self._raw_payload(client, table, row["id"])
                    if full is None:
                        continue  # raw_payload holds a non-object; never clobber it
                    original = dict(full)
                    # The projection may have changed while this page was processed.
                    # Defer rather than attach a decision to different evidence.
                    if (any(full.get(k) != raw.get(k) for k in _RAW_JSON)
                            or any(str(full.get(k)) != str(raw.get(k)) for k in _RAW_TEXT)):
                        error_message = "personal_property_evidence_changed; retry next run"
                        continue
                    if tag:
                        full["personal_property"] = tag
                    else:
                        full.pop("personal_property", None)
                    try:
                        result = client.table(table).update({"raw_payload": full}).eq("id", row["id"]) \
                            .eq("raw_payload", json.dumps(original)).execute()
                        if not getattr(result, "data", None):
                            raise RuntimeError("concurrent change or missing row; retry next run")
                        if tag:
                            tagged += 1
                            self.logger.info(f"  {table} {row['id']} {tag['lane']}/{tag['grade']}: "
                                             f"{tag['signals'][0][:120]}")
                        else:
                            cleared += 1
                    except Exception as e:
                        error_message = "personal_property_update_failed"
                        self.logger.warning(f"  update failed {table} id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        if error_message:
            status = "failed"
        elif tagged or cleared:
            status = "ok"
        else:
            status = "all_dupes" if scanned else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=tagged + cleared,
                            duplicate_count=unchanged, error_message=error_message)
        self.logger.info(f"scanned={scanned} tagged={tagged} cleared={cleared} lanes={dict(lanes)}")
        return {"name": self.name, "status": status, "scanned": scanned, "tagged": tagged,
                "cleared": cleared, "lanes": dict(lanes), "error": error_message,
                "staged": tagged + cleared, "duplicates": unchanged, "fetched": scanned}

    def _rows(self, client, table: str):
        cols = ", ".join([_BASE_COLS]
                         + [f"{k}:raw_payload->>{k}" for k in _RAW_TEXT]
                         + [f"{k}:raw_payload->{k}" for k in _RAW_JSON]
                         + ["_pp:raw_payload->personal_property"])
        # Keyset paging on the primary key; OFFSET pages time out on staging.
        last_id = None
        for _ in range(self.max_pages):
            q = client.table(table).select(cols).order("id").limit(self.page_size)
            if last_id is not None:
                q = q.gt("id", last_id)
            rows = getattr(q.execute(), "data", None) or []
            yield from rows
            if len(rows) < self.page_size:
                return
            last_id = rows[-1]["id"]

    @staticmethod
    def _raw_payload(client, table: str, row_id: str) -> Optional[Dict[str, Any]]:
        q = client.table(table).select("raw_payload").eq("id", row_id).limit(1).execute()
        rows = getattr(q, "data", None) or []
        raw = rows[0].get("raw_payload") if rows else None
        if raw is None:
            return {}
        return dict(raw) if isinstance(raw, dict) else None


def run() -> dict:
    return PersonalPropertyLaneBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
