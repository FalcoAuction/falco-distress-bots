"""
Florida lead enricher — attaches the DOR statewide parcel roll to every
lead with raw_payload.state = 'FL'. Free (DOR data portal), one cached
SQLite index under data/fl_dor/, rebuilt only when DOR posts a new roll.

Per lead:
  raw_payload.fl_parcel   parcel id, just value, homestead, use code, year
                          built, living area, last sale, owner of record,
                          mailing address, absentee flag
  raw_payload.fl_tpp      business equipment accounts (NAP) for entity
                          owners: NAICS, equipment value, late-filing flag
  property_value          DOR just value, only when the column is empty
  property_address        parcel situs, only when the lead had none (estate
                          of an out-of-state decedent, absentee owner)

Matching: situs address first (unit-aware for condo towers); otherwise the
owner name inside the lead's county, accepted only when it resolves to at
most MAX_NAME_PARCELS parcels so common names never guess.

This is the Florida counterpart of tpad_enricher + the county assessor
bots; Tennessee leads are never touched.

Distress type: N/A — enricher.
"""

from __future__ import annotations

import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._fl_roll import RESIDENTIAL_UC, county_key, open_roll, parcel_tag, tpp_tag

MAX_NAME_PARCELS = 3
# Bots that stage FL leads (court_rss stages both states; the state filter
# below keeps only its FL rows).
FL_BOT_SOURCES = ("fl_hillsborough_clerk", "fl_code_enforcement", "fl_sunbiz", "court_rss")
TPP_MIN_VALUE = 25_000
_SELECT = "id, distress_type, full_name, owner_name_records, property_address, county, property_value"


class FlRollEnricherBot(BotBase):
    name = "fl_roll_enricher"
    description = "Attach the FL DOR parcel roll (value, homestead, mailing, business equipment) to FL leads"
    throttle_seconds = 0.0
    expected_min_yield = 0
    frozen_after_runs = 10_000
    page_size = 1000
    max_pages = 100

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
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}
        scanned = enriched = unmatched = 0
        error: Optional[str] = None
        try:
            roll = open_roll(self.session, self.logger, started.year, refresh=False)
            if roll is None:
                raise RuntimeError("fl_roll_unavailable")
            self.logger.info(f"FL roll: {roll.counts()}")
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                for lead in self._leads(client, table):
                    scanned += 1
                    update = self._enrich(roll, lead)
                    if update is None:
                        unmatched += 1
                        continue
                    if not update:
                        continue
                    try:
                        client.table(table).update(update).eq("id", lead["id"]).execute()
                        enriched += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed {table} id={lead['id']}: {e}")
        except Exception as e:
            error = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
        status = "failed" if error else "ok" if enriched else ("all_dupes" if scanned else "zero_yield")
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=enriched,
                            duplicate_count=scanned - enriched, error_message=error)
        self.logger.info(f"scanned={scanned} enriched={enriched} unmatched={unmatched}")
        return {"name": self.name, "status": status, "scanned": scanned, "enriched": enriched,
                "unmatched": unmatched, "error": error, "staged": enriched,
                "duplicates": scanned - enriched, "fetched": scanned}

    # ── matching ────────────────────────────────────────────────────────

    def _enrich(self, roll, lead: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """{} = already enriched, None = no parcel found, else the update."""
        if lead.get("_fl_parcel"):
            return {}
        county = lead.get("county") or lead.get("_county")
        if not county_key(county):
            return None
        names = [n.strip() for n in (lead.get("owner_name_records") or lead.get("full_name") or "").split(";") if n.strip()]
        parcel = roll.parcel_by_id(county, lead.get("_parcel_id"))
        how = "parcel_id"
        if parcel is None:
            parcel = roll.parcel_at(county, lead.get("property_address"))
            how = "address"
        if parcel is None:
            how = "owner_name"
            for n in names:
                hits = roll.parcels_of(n, county)
                res = [h for h in hits if h.get("dor_uc") in RESIDENTIAL_UC] or hits
                # One owner of record (same name and mailing address) only:
                # two 'HENRY JAMES' with different mailings are two people.
                people = {(h.get("own_name"), h.get("own_addr1")) for h in res}
                if 0 < len(res) <= MAX_NAME_PARCELS and len(people) == 1:
                    parcel = res[0]
                    break
        equipment = []
        for n in names[:2]:
            equipment += [tpp_tag(t) for t in roll.equipment_of(n) if (t.get("jv_total") or 0) >= TPP_MIN_VALUE][:3]
        if parcel is None and not equipment:
            return None
        if parcel is None and equipment == (lead.get("_fl_tpp") or []):
            return {}
        raw = self._raw_payload(lead)
        if raw is None:
            return {}
        update: Dict[str, Any] = {}
        if parcel is not None:
            tag = parcel_tag(parcel)
            tag["match"] = how
            raw["fl_parcel"] = tag
            if not lead.get("property_value") and tag.get("just_value"):
                update["property_value"] = tag["just_value"]
            if not lead.get("property_address") and tag.get("situs"):
                update["property_address"] = tag["situs"] + ", FL"
        if equipment:
            raw["fl_tpp"] = equipment
        update["raw_payload"] = raw
        return update

    # ── Supabase I/O ────────────────────────────────────────────────────

    def _leads(self, client, table: str):
        cols = (_SELECT + ", _county:raw_payload->>county, _parcel_id:raw_payload->>parcel_id, "
                "_fl_parcel:raw_payload->fl_parcel, "
                "_fl_tpp:raw_payload->fl_tpp")
        last_id = None
        for _ in range(self.max_pages):
            q = client.table(table).select(cols)
            # Staging is ~400 MB: a JSON-path filter seq-scans it into the
            # statement timeout, so filter on the FL bots' bot_source there.
            q = (q.in_("bot_source", list(FL_BOT_SOURCES)).eq("raw_payload->>state", "FL")
                 if table == "homeowner_requests_staging" else q.eq("raw_payload->>state", "FL"))
            q = q.order("id").limit(self.page_size)
            if last_id is not None:
                q = q.gt("id", last_id)
            rows = getattr(q.execute(), "data", None) or []
            for r in rows:
                r["_table"] = table
            yield from rows
            if len(rows) < self.page_size:
                return
            last_id = rows[-1]["id"]

    def _raw_payload(self, lead: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        client = _supabase()
        q = client.table(lead["_table"]).select("raw_payload").eq("id", lead["id"]).limit(1).execute()
        rows = getattr(q, "data", None) or []
        raw = rows[0].get("raw_payload") if rows else None
        if raw is None:
            return {}
        return dict(raw) if isinstance(raw, dict) else None


def run() -> dict:
    return FlRollEnricherBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
