"""Land lane: Tennessee farm and timber ownership research (statewide).

Reads the Comptroller parcel index (data/comptroller/index.sqlite, refreshed
weekly by comptroller_refresh, free and cleared for commercial use) and stages
every Tennessee tract of 20+ acres whose owner of record is an estate
or heirs, a long-held family group (ETAL), or an out-of-state owner holding
25+ years. Rules in _land.py.

Distress types:
  HEIRS_PROPERTY  heirs or family_etal
  ABSENTEE_LAND   absentee only

The Comptroller roll covers 86 of the 95 counties; Rutherford and
Davidson come from their county parcel map services (_land_gis). Williamson,
Montgomery, Hamilton, Knox, Shelby, Chester and Hickman are not in this lane yet.

    python -m src.bots.land_lane_bot
"""

from __future__ import annotations

import os
import json
from collections import Counter
import sqlite3
import sys
import time
from urllib.parse import quote
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ._base import BotBase, LeadPayload, _supabase
from ._comptroller import _record, county_key
from .comptroller_refresh import BASE as ROLL_BASE, COUNTIES as ROLL_COUNTIES
from ._land import lane_record, person_name, signals, street_address
from ._land_identity import parcel_control
from ._land_gis import SOURCES as GIS_COUNTIES, county_rows
from ._regions import TN_COUNTIES

ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / "data" / "comptroller" / "index.sqlite"
MAX_LEADS = int(os.environ.get("FALCO_LAND_MAX_LEADS", "4000") or 4000)
REFRESH_LIMIT = 200
REFRESH_SECONDS = 450


def row_identity(row):
    county = county_key(row.get("county"))
    source = "county_gis" if county.lower() in GIS_COUNTIES else "comptroller_roll"
    return parcel_control(source, county, row.get("parcelid"))


def stored_identity(row):
    raw = row.get("raw_payload") or {}
    if not isinstance(raw, dict):
        return None
    land = raw.get("land") if isinstance(raw.get("land"), dict) else {}
    parcel = raw.get("comptroller_parcel") if isinstance(raw.get("comptroller_parcel"), dict) else {}
    return parcel_control(raw.get("source"), land.get("county") or row.get("county"),
                          land.get("parcel_id") or parcel.get("parcelid"))


def refresh_order(existing, today):
    """A contiguous rotating window gives failures no permanent front position."""
    rows = sorted(((t, r) for t, records in existing.items() for r in records),
                  key=lambda pair: (pair[0], str(pair[1].get("id") or "")))
    if not rows:
        return rows
    start = today.toordinal() * REFRESH_LIMIT % len(rows)
    return rows[start:] + rows[:start]


def tract_leads(con: Optional[sqlite3.Connection], bot: "LandLaneBot", today: date,
                extra_rows: Iterable[Mapping[str, Any]] = ()) -> List[LeadPayload]:
    rows: List[Any] = []
    if con is not None:
        con.row_factory = sqlite3.Row
        counties = sorted({county_key(c) for c in TN_COUNTIES})   # the index keys counties as "VANBUREN"
        marks = ",".join("?" * len(counties))
        control = "CASE WHEN parcelid GLOB '* 20[0-9][0-9]' THEN substr(parcelid,1,length(parcelid)-5) ELSE parcelid END"
        rows = con.execute(f"SELECT * FROM parcels WHERE (county, {control}) IN "
                           f"(SELECT county, {control} FROM parcels WHERE county IN ({marks}) "
                           "AND calcac >= 20 AND appraisal >= 150000)",
                           counties).fetchall()
    # Group BEFORE signal filtering: an entity/new owner duplicate must not
    # disappear and leave an apparently unambiguous old estate owner.
    grouped = {}
    ambiguous = set()
    for original in list(rows) + list(extra_rows):
        row = dict(original)
        county, parcel = county_key(row.get("county")), str(row.get("parcelid") or "")
        if county not in {county_key(c) for c in TN_COUNTIES} or not parcel.strip() or parcel.strip().upper() in ("NONE", "NULL", "N/A", "UNKNOWN"):
            continue
        row.update(county=county, parcelid=parcel)
        key = row_identity(row)
        if key is None:
            continue
        if key in grouped and {k: v for k, v in grouped[key].items() if k != "parcelid"} != {k: v for k, v in row.items() if k != "parcelid"}:
            ambiguous.add(key)
        if key not in grouped or row["parcelid"] > grouped[key]["parcelid"]:
            grouped[key] = row
    bot._ambiguous = ambiguous
    bot._source_rows = {k: r for k, r in grouped.items() if k not in ambiguous}
    out: List[LeadPayload] = []
    for row in bot._source_rows.values():
        sigs = signals(row, today)
        if not sigs:
            continue
        rec = _record(row)
        rec.update({"method": "land_lane", "county": row["county"].lower()})
        street = street_address(row["address"])
        place = row["city"] or (row["county"].title() + " County")
        address = ", ".join(x for x in (street, place, "TN") if x)
        owner = rec["owner"]
        out.append(LeadPayload(
            bot_source=bot.name,
            pipeline_lead_key=bot.make_lead_key(bot.name, f"{row['county'].lower()}|{row['parcelid']}"),
            property_address=address if street else None,
            county=row["county"].lower(),
            full_name=person_name(row["owner"]),
            owner_name_records=owner,
            property_value=float(row["appraisal"] or 0) or None,
            distress_type="HEIRS_PROPERTY" if ("heirs" in sigs or "family_etal" in sigs) else "ABSENTEE_LAND",
            admin_notes=f"{rec['acres']:.0f} acres, ownership research: {', '.join(sigs)}; owner {owner}",
            raw_payload={"source": "county_gis" if row["county"].lower() in GIS_COUNTIES else "comptroller_roll", "state": "TN", "property_lane": "land",
                         "land": bot.land_evidence(row, sigs, today), "comptroller_parcel": rec},
        ))
    out.sort(key=lambda l: (0 if l.distress_type == "HEIRS_PROPERTY" else 1, -(l.property_value or 0)))
    return out  # Bound NEW inserts after excluding existing keys, not acquisition.


class LandLaneBot(BotBase):
    name = "land_lane"
    description = "Middle TN farm and timber tracts (20+ acres) owned by heirs, family groups or long-held absentees"
    throttle_seconds = 0
    expected_min_yield = 0

    def land_evidence(self, row, sigs, today):
        county = row["county"].lower()
        evidence = lane_record(row, sigs, county)
        evidence.update(status="current" if sigs else "no_longer_matches",
                        changed_fields=[],
                        intake_parcel_id=row["parcelid"],
                        parcel_identity=list(row_identity(row)) if row_identity(row) else None,
                        observed_on=today.isoformat(),
                        source_url=GIS_COUNTIES[county][0] if county in GIS_COUNTIES else
                        ROLL_BASE + next((c for c in ROLL_COUNTIES if county_key(c) == county_key(county)), county.title()) + ".zip",
                        source_verified_on=today.isoformat() if county in getattr(self, "_gis_complete", set()) else None,
                        source_refreshed_on=today.isoformat() if county in getattr(self, "_gis_complete", set()) else None,
                        source_stamp=getattr(self, "_source_stamps", {}).get(county_key(county)),
                        freshness_basis="live_county_gis" if county in getattr(self, "_gis_complete", set()) else "cached_roll_observation",
                        review_required=not bool(sigs))
        return evidence

    def scrape(self) -> List[LeadPayload]:
        self._acquisition_started = time.monotonic()
        self._gis_complete = set()
        self._covered = set()
        self._source_stamps = {}
        gis: List[Dict[str, Any]] = []
        for county in GIS_COUNTIES:
            try:
                rows = list(county_rows(county, lambda url, **kw: self.fetch(url, timeout=120, **kw)))
                self.logger.info(f"{county}: {len(rows)} large parcels from the county map service")
                gis += rows
                self._gis_complete.add(county)
                self._covered.add(county_key(county))
            except Exception as e:  # one county's service down must not cost the rest
                self.logger.warning(f"{county}: parcel service failed: {e}")
        # Use the producer's current src/data path; also accept the workflow's
        # repo-root cache when the producer path is absent. Never create a DB.
        path = DB_PATH if DB_PATH.exists() else Path(__file__).resolve().parents[3] / "data/comptroller/index.sqlite"
        con = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) if path.exists() else None
        self._index_path = path if con is not None else None
        if con is not None:
            self._covered.update(r[0] for r in con.execute("SELECT DISTINCT county FROM parcels"))
            try:
                state = json.loads(path.with_name("refresh_state.json").read_text())
                self._source_stamps = {county_key(k): v for k, v in state.items()}
            except (OSError, ValueError, AttributeError):
                pass
        if con is None:
            self.logger.warning("no Comptroller index; comptroller_refresh has not run (county GIS only)")
        try:
            leads = tract_leads(con, self, date.today(), gis)
        finally:
            if con is not None:
                con.close()
        counts: Dict[str, int] = {}
        for l in leads:
            counts[l.distress_type] = counts.get(l.distress_type, 0) + 1
        self.logger.info(f"staging {len(leads)} tract(s): {counts}")
        return leads

    def _existing(self, client, table):
        if table == "homeowner_requests":
            # Live rows have source='bot', not bot_source. The production
            # pipeline_lead_key persists promotion identity; scope to our
            # staging keys rather than scanning every bot's live payload.
            keys = getattr(self, "_live_keys", [])
            for offset in range(0, len(keys), 100):
                last = None
                while True:
                    q = client.table(table).select(
                        "id,pipeline_lead_key,county,owner_name_records,full_name,property_value,raw_payload"
                    ).eq("source", "bot").in_("pipeline_lead_key", keys[offset:offset + 100]).order("id").limit(500)
                    if last is not None:
                        q = q.gt("id", last)
                    try:
                        rows = getattr(q.execute(), "data", None) or []
                    except Exception as exc:
                        self.logger.warning("land live lookup failed batch=%s type=%s; staging remains available", offset // 100, type(exc).__name__)
                        break
                    yield from rows
                    if len(rows) < 500:
                        break
                    last = rows[-1]["id"]
            return
        last = None
        while True:
            q = client.table(table).select(
                "id,pipeline_lead_key,county,owner_name_records,full_name,property_value,raw_payload"
            ).eq("bot_source", self.name).order("id").limit(500)
            if last is not None:
                q = q.gt("id", last)
            rows = getattr(q.execute(), "data", None) or []
            yield from rows
            if len(rows) < 500:
                break
            last = rows[-1]["id"]

    def _write_staging(self, leads):
        started = getattr(self, "_acquisition_started", time.monotonic())
        client = _supabase()
        if client is None or not hasattr(self, "_source_rows"):
            return (0, 0)
        # Existing records refresh independently of the new-insert limit.
        staged_rows = list(self._existing(client, "homeowner_requests_staging"))
        self._live_keys = sorted({r["pipeline_lead_key"] for r in staged_rows if r.get("pipeline_lead_key")})
        existing = {"homeowner_requests_staging": staged_rows,
                    "homeowner_requests": list(self._existing(client, "homeowner_requests"))}
        seen = {r["pipeline_lead_key"] for r in existing["homeowner_requests_staging"]}
        controls = {stored_identity(r) for records in existing.values() for r in records} - {None}
        collisions = {table: Counter(stored_identity(r) for r in records) for table, records in existing.items()}
        fresh = sorted((l for l in leads if l.pipeline_lead_key not in seen
                        and parcel_control(l.raw_payload["source"], l.county, l.raw_payload["land"]["parcel_id"]) not in controls),
                       key=lambda l: l.pipeline_lead_key)
        if len(fresh) > MAX_LEADS:
            offset = date.today().toordinal() * MAX_LEADS % len(fresh)
            fresh = (fresh[offset:] + fresh[:offset])[:MAX_LEADS]
        staged, _ = super()._write_staging(fresh) if fresh else (0, 0)
        con = sqlite3.connect(self._index_path.as_uri() + "?mode=ro", uri=True) if getattr(self, "_index_path", None) else None
        if con is not None:
            con.row_factory = sqlite3.Row
        attempted = updated = unchanged = oversized = failed = 0
        try:
            for table, old in refresh_order(existing, date.today()):
                if attempted >= REFRESH_LIMIT or time.monotonic() - started >= REFRESH_SECONDS:
                    break
                raw = old.get("raw_payload")
                if not isinstance(raw, dict):
                    continue
                land = raw.get("land") if isinstance(raw.get("land"), dict) else {}
                parcel = raw.get("comptroller_parcel") if isinstance(raw.get("comptroller_parcel"), dict) else {}
                county = county_key(land.get("county") or old.get("county"))
                pid = str(land.get("parcel_id") or parcel.get("parcelid") or "")
                if not pid or county not in self._covered:
                    continue
                # Legacy keys must bind this record to exactly this parcel.
                intake_pid = land.get("intake_parcel_id") or pid
                expected = self.make_lead_key(self.name, f"{county.lower()}|{intake_pid}")
                if old.get("pipeline_lead_key") != expected:
                    continue
                key = stored_identity(old)
                if key is None:
                    continue
                source = self._source_rows.get(key)
                duplicate = collisions[table][key] > 1
                ambiguous = key in self._ambiguous or duplicate
                if ambiguous:
                    source = None
                if source is None and not ambiguous and con is not None and county.lower() not in GIS_COUNTIES:
                    matches = [dict(r) for r in con.execute(
                        "SELECT * FROM parcels WHERE county=? AND (parcelid=? OR substr(parcelid,1,length(parcelid)-5)=?)",
                        (county, pid, key[2])) if row_identity(dict(r)) == key]
                    if matches:
                        ambiguous = any(r != matches[0] for r in matches[1:])
                        source = matches[0] if not ambiguous else None
                if source is not None:
                    sigs = signals(source, date.today())
                    evidence = self.land_evidence(source, sigs, date.today())
                    evidence["intake_parcel_id"] = intake_pid
                    changed = [k for k in ("owner_of_record", "acres", "class", "mailing_state", "held_since", "ownership_signals")
                               if k in land and land[k] != evidence[k]]
                    if parcel.get("owner") and parcel["owner"] != evidence["owner_of_record"] and "owner_of_record" not in changed:
                        changed.append("owner_of_record")
                    # A repeated sighting is not an explanation/resolution of
                    # an owner change. Retain history for downstream review.
                    changed = sorted(set(changed) | set(land.get("changed_fields") or []))
                    evidence.update(changed_fields=changed, review_required=bool(changed) or not bool(sigs))
                else:
                    evidence = {**land, "county": county.lower(), "parcel_id": pid,
                                "parcel_identity": list(key), "intake_parcel_id": intake_pid,
                                "observed_on": date.today().isoformat(), "review_required": True,
                                "status": "ambiguous_existing_records" if duplicate else "ambiguous_parcel" if ambiguous else "not_in_current_filtered_source"}
                    if str(evidence.get("held_since") or "")[:4] == "1900":
                        evidence["held_since"] = None
                # Keep all independently acquired action/authority evidence.
                payload = {**raw, "land": evidence}
                patch = {"raw_payload": payload}
                if source is not None:
                    rec = _record(source)
                    rec.update(method="land_lane", county=county.lower())
                    if not parcel or parcel.get("method") == "land_lane":
                        payload["comptroller_parcel"] = rec
                    for field, previous, value in (
                        ("owner_name_records", parcel.get("owner"), rec["owner"]),
                        ("full_name", person_name(parcel.get("owner") or ""), person_name(source["owner"])),
                        ("property_value", parcel.get("appraisal"), rec["appraisal"]),
                    ):
                        if old.get(field) is None or old.get(field) == previous:
                            patch[field] = value
                if all(old.get(k) == v for k, v in patch.items()):
                    unchanged += 1
                    continue
                encoded = json.dumps(raw, separators=(",", ":"))
                # Full-payload CAS is safe but PostgREST puts its guard in
                # the URL. Do not send unbounded filters or clobber via an
                # unguarded fallback. A bounded RPC is a separate integration.
                if len(quote(encoded, safe="")) > 6000:
                    oversized += 1
                    continue
                q = client.table(table).update(patch).eq("id", old["id"]) \
                    .eq("raw_payload", encoded)
                for field in ("owner_name_records", "property_value", "full_name"):
                    q = q.is_(field, "null") if old.get(field) is None else q.eq(field, old[field])
                attempted += 1
                try:
                    if not getattr(q.execute(), "data", None):
                        failed += 1
                        self.logger.warning("land refresh conflict table=%s id=%s; retry next run", table, old["id"])
                    else:
                        updated += 1
                except Exception as exc:
                    failed += 1
                    self.logger.warning("land refresh failed table=%s id=%s type=%s; continuing", table, old["id"], type(exc).__name__)
        finally:
            if con is not None:
                con.close()
        self.logger.info("land refresh attempted=%s updated=%s unchanged=%s oversized=%s failed=%s existing=%s",
                         attempted, updated, unchanged, oversized, failed, sum(map(len, existing.values())))
        self.refresh_stats = dict(attempted=attempted, updated=updated, unchanged=unchanged,
                                  oversized=oversized, failed=failed, existing=sum(map(len, existing.values())))
        return staged, len(leads) - len(fresh)


def run() -> dict:
    return LandLaneBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
