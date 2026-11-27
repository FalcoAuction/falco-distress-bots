"""County value and owner of record for serious code cases outside Nashville.

The ranking rule (public.lead_code_severity, ported in _code_severity.py)
marks derelict and dangerous structure cases in Memphis, Knoxville,
Chattanooga and Johnson City as serious. Such a row cannot rank without a
property value and an owner. This step reads both from the county's own free
record, and only when the case matches exactly one parcel.

  memphis_codes      Shelby County Trustee current-year realty bill, by the
                     parcel the 311 request reports plus the street, or by a
                     street that has one parcel (_shelby_trustee_value.lookup)
  chattanooga_codes  Hamilton County Assessor weekly export, by street
  johnson_city_bdsr  Tennessee Comptroller county roll (local index), by street
  knoxville_poh      no live read. knox-tn.mygovonline.com/robots.txt closes
                     every path to crawlers, KGIS map services answer 401 and
                     its owner card redirects into a path its robots.txt
                     disallows. A county bill record already stored on the row
                     for its docket parcel is used; nothing else is.

Rules:
  * One parcel or nothing. Two parcels at one address, a street whose name
    differs, a different unit, a match by name: nothing is written except the
    reason, in raw_payload.code_case_parcel.
  * Every value passes _value_guard.require_value_evidence. The owner is
    written only when that same gate passes for the parcel.
  * An existing value or owner is never replaced.
  * A county read that fails or is refused changes nothing, and the row is
    read again on a later run.
  * A checked row is not read again for RECHECK_DAYS.

Scheduled run: at most MAX_PER_RUN county checks, newest cases first.

By hand (dry run unless --write):
    python -m src.bots.code_case_values_bot --ids-file ids.json --plan plan.json
    python -m src.bots.code_case_values_bot --apply-plan plan.json --write
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ._base import BotBase, _supabase
from ._code_severity import OTHER_CITY_RX, code_severity
from ._comptroller import _record, county_key, street_key_from_lead
from ._owner_lookup import LegacyTLSAdapter
from ._shelby_trustee_value import HOST as SHELBY_HOST, VALUE_SOURCE as SHELBY_SOURCE, lookup as shelby_lookup
from ._value_guard import BASIS, require_value_evidence

KEY = "code_case_parcel"
TABLE = "homeowner_requests_staging"
# bot_source -> county the cases are in.
SOURCES = {
    "memphis_codes": "shelby",
    "chattanooga_codes": "hamilton",
    "knoxville_poh": "knox",
    "johnson_city_bdsr": "washington",
}
MAX_PER_RUN = int(os.environ.get("CODE_CASE_VALUES_CAP", "40"))
KNOX_STORED_BILL_OK = os.environ.get("CODE_CASE_VALUES_KNOX_STORED", "0").strip().lower() in ("1", "true", "on", "yes")
RECHECK_DAYS = 30
PAGE = 100
MAX_PAGES = 3
BUDGET_SECONDS = 300
# Consecutive failed reads of one county before it is left alone for the run.
MAX_FAILED_READS = 3

ROOT = Path(__file__).resolve().parents[2]
COMPTROLLER_DB = ROOT / "data" / "comptroller" / "index.sqlite"
COMPTROLLER_STATE = ROOT / "data" / "comptroller" / "refresh_state.json"
COMPTROLLER_URL = "https://comptroller.tn.gov/content/dam/cot/pa/documents/landuse/maps/parcel-maps-data/"
COMPTROLLER_NAMES = {"washington": "Washington"}
HAMILTON_URL = "https://www.hamiltontn.gov/_downloadsAssessor/AssessorExportCSV.zip"

SELECT = ("id,bot_source,pipeline_lead_key,county,property_address,distress_type,staging_status,"
          "property_value,property_value_source,full_name,owner_name_records,"
          "m_parcel:raw_payload->memphis_311->>PARCEL_ID,m_type:raw_payload->memphis_311->>REQUEST_TYPE,"
          "c_desc:raw_payload->chattanooga_codes->descriptions,"
          "k_section:raw_payload->>poh_section,k_case:raw_payload->>case_number,"
          "k_parcel:raw_payload->>parcel_id,k_roll:raw_payload->knox_assessor,"
          "j_stage:raw_payload->johnson_city_bdsr->>stage,j_viol:raw_payload->johnson_city_bdsr->>violations,"
          f"prior:raw_payload->{KEY}")

_DIRECTIONS = {"N", "S", "E", "W"}
_DIRECTION_WORDS = _DIRECTIONS | {"NORTH", "SOUTH", "EAST", "WEST"}


def shape(row: Dict[str, Any]) -> Dict[str, Any]:
    """A selected row as the guard and the severity rule read it: only the
    payload fields this step needs, never the whole payload."""
    raw: Dict[str, Any] = {}
    if row.get("m_parcel") is not None or row.get("m_type") is not None:
        raw["memphis_311"] = {"PARCEL_ID": row.get("m_parcel"), "REQUEST_TYPE": row.get("m_type")}
    if row.get("c_desc") is not None:
        raw["chattanooga_codes"] = {"descriptions": row["c_desc"]}
    if row.get("k_section") is not None:
        raw["poh_section"] = row["k_section"]
    for key, col in (("case_number", "k_case"), ("parcel_id", "k_parcel"), ("knox_assessor", "k_roll")):
        if row.get(col) is not None:
            raw[key] = row[col]
    if row.get("j_stage") is not None or row.get("j_viol") is not None:
        raw["johnson_city_bdsr"] = {"stage": row.get("j_stage"), "violations": row.get("j_viol")}
    out = {k: row.get(k) for k in ("id", "bot_source", "pipeline_lead_key", "county", "property_address",
                                   "distress_type", "staging_status", "property_value",
                                   "property_value_source", "full_name", "owner_name_records")}
    out["raw_payload"] = raw
    out["prior"] = row.get("prior") if isinstance(row.get("prior"), dict) else None
    return out


def _fresh(prior: Optional[Dict[str, Any]], today: str) -> bool:
    try:
        return bool(prior) and date.fromisoformat(today) - date.fromisoformat(prior["checked_on"]) < timedelta(days=RECHECK_DAYS)
    except (KeyError, TypeError, ValueError):
        return False


def held(reason: str, **extra: Any) -> Dict[str, Any]:
    return {"status": "held", "reason": reason, **{k: v for k, v in extra.items() if v}}


def unavailable(reason: str) -> Dict[str, Any]:
    return {"status": "unavailable", "reason": reason}


# ── Hamilton: street match against the county export ────────────────────────

def hamilton_street(address: Any) -> Optional[Tuple[str, ...]]:
    """'2401 5TH ST, Chattanooga, TN' -> ('2401', '5TH', 'ST'). None without a
    house number and a street."""
    tokens = re.sub(r"[^A-Z0-9 ]", " ", str(address or "").split(",")[0].upper()).split()
    if len(tokens) < 2 or not tokens[0].isdigit():
        return None
    return tuple([tokens[0]] + [re.sub(r"^0+(\d+(?:ST|ND|RD|TH))$", r"\1", t) for t in tokens[1:]])


def _without_direction(street: Tuple[str, ...]) -> Tuple[str, ...]:
    if len(street) >= 4 and street[1] in _DIRECTION_WORDS:
        return street[:1] + street[2:]
    return street


def hamilton_streets(index: Dict[str, Dict[str, Any]]):
    exact, loose = defaultdict(list), defaultdict(list)
    for parcel, record in index.items():
        street = hamilton_street(record.get("property_address"))
        if street:
            exact[street].append(parcel)
            loose[_without_direction(street)].append(parcel)
    return exact, loose


def hamilton_match(address: Any, exact, loose, index) -> Dict[str, Any]:
    """The one county parcel at this street, or the reason there is none.

    The city prints "224 HICKORY ST" for the county's "224 N HICKORY ST". That
    is accepted only when no other parcel in the county sits at that number
    and street under any direction. A spelled-out direction is part of the
    street's name ("EAST BRAINERD RD" is not "BRAINERD RD"): held.
    """
    want = hamilton_street(address)
    if not want:
        return held("invalid_address")
    hits = exact.get(want, [])
    method = "exact_county_address"
    if len(hits) > 1:
        return held("ambiguous_address", candidate_parcels=sorted(hits))
    if not hits:
        hits = loose.get(_without_direction(want), [])
        if not hits:
            return held("no_county_address")
        if len(hits) > 1:
            return held("ambiguous_address", candidate_parcels=sorted(hits))
        theirs = hamilton_street(index[hits[0]].get("property_address"))
        if not (len(theirs) == len(want) + 1 and theirs[1] in _DIRECTIONS and theirs[:1] + theirs[2:] == want):
            return held("street_name_differs", candidate_parcels=hits,
                        candidate_address=index[hits[0]].get("property_address"))
        method = "unique_county_street_direction_prefix_omitted"
    return {"status": "matched", "parcel": hits[0], "match_method": method}


# ── the gate: what may be written for one row ───────────────────────────────

def decide(row: Dict[str, Any], found: Dict[str, Any], today: str) -> Dict[str, Any]:
    """Turn a county answer into the evidence to record and the columns to
    set. Pure. `write` is empty unless the parcel passes the value guard."""
    county = SOURCES[row["bot_source"]]
    entry = {"id": row["id"], "bot_source": row["bot_source"], "county": county,
             "property_address": row.get("property_address"), "status": found["status"],
             "reason": found.get("reason"), "evidence": None, "write": {}}
    if found["status"] == "unavailable":
        return entry  # nothing recorded: the row is read again next run
    evidence = {"status": found["status"], "checked_on": found.get("checked_on") or today, "county": county}
    for key in ("reason", "candidate_parcels", "candidate_address", "parcel", "record_address", "match_method",
                "source", "source_url", "owner", "owner_source", "tax_year", "assessed_value", "tax_class"):
        if found.get(key) not in (None, "", []):
            evidence[key] = found[key]
    if found["status"] == "matched":
        value = found.get("value")
        value = int(round(float(value))) if value and float(value) > 0 else None
        patch = {"property_value": value or 1, "property_value_source": found["source"]}
        reason = require_value_evidence(patch, row, parcel=found.get("parcel"), county=county,
                                        observed_on=found.get("observed_on"), basis=found.get("basis"),
                                        record_address=found.get("record_address"))
        if reason:
            evidence.update(status="held", reason=reason)
            evidence.pop("owner", None)
            evidence.pop("owner_source", None)
            entry.update(status="held", reason=reason)
        else:
            day = patch["property_value_as_of"]
            owner = " ".join(str(found.get("owner") or "").split())
            if owner:
                evidence.update(owner=owner, owner_as_of=day)
                if not row.get("owner_name_records"):
                    entry["write"]["owner_name_records"] = owner
            else:
                evidence.pop("owner", None)
                evidence.pop("owner_source", None)
            if value:
                evidence.update(value=value, value_basis=found.get("basis") or BASIS[found["source"]],
                                value_as_of=day)
                if not row.get("property_value"):
                    entry["write"].update(property_value=value, property_value_source=found["source"],
                                          property_value_as_of=day)
    entry["evidence"] = evidence
    return entry


def from_prior(prior: Dict[str, Any]) -> Dict[str, Any]:
    """A matched check already stored on the row, replayed without a county
    read so a column the earlier write missed can still be set."""
    found = dict(prior)
    found["observed_on"] = prior.get("value_as_of") or prior.get("owner_as_of") or prior.get("checked_on")
    found["basis"] = prior.get("value_basis")
    return found


class CodeCaseValuesBot(BotBase):
    name = "code_case_values"
    description = "County value and owner of record for serious code cases in Memphis, Chattanooga, Knoxville, Johnson City"
    throttle_seconds = 1.5
    expected_min_yield = 0

    def __init__(self):
        super().__init__()
        self.session.mount(SHELBY_HOST + "/", LegacyTLSAdapter(max_retries=0))
        self._hamilton = None
        self._failed_reads: Counter = Counter()

    def scrape(self) -> List[Any]:
        return []

    # ── county reads ────────────────────────────────────────────────────────

    def _shelby(self, row, today):
        parcel = str((row["raw_payload"].get("memphis_311") or {}).get("PARCEL_ID") or "").strip() or None
        shim = {"pipeline_lead_key": row["pipeline_lead_key"], "property_address": row.get("property_address") or "",
                "raw_payload": {"extracted": {"parcel": parcel}}}
        ev = shelby_lookup(self, shim, date.fromisoformat(today))
        status = ev["status"]
        if status == "source_unavailable":
            return unavailable("county_read_failed")
        if status not in ("verified_assessment", "assessment_unverified"):
            return held(status, candidate_parcels=ev.get("candidate_parcels"))
        return {"status": "matched", "parcel": ev["parcel_id"], "record_address": ev["location"],
                "owner": ev.get("owner"), "owner_source": "Shelby County Trustee current-year realty bill",
                "value": ev.get("estimated_appraisal") if status == "verified_assessment" else None,
                "source": SHELBY_SOURCE, "observed_on": ev["checked_on"], "source_url": ev["source_url"],
                "match_method": ev["match_method"], "tax_year": ev.get("tax_year"),
                "assessed_value": ev.get("assessed_value"), "tax_class": ev.get("tax_class")}

    def _hamilton_export(self):
        """(index, exact, loose, details, duplicates, file day) or None. One
        download per run, and only when a Chattanooga case needs it."""
        if self._hamilton is None:
            from .hamilton_assessor_bot import HamiltonAssessorBot, file_day
            county = HamiltonAssessorBot()
            index = county._build_parcel_index()
            if not index:
                self._hamilton = False
            else:
                exact, loose = hamilton_streets(index)
                self._hamilton = (index, exact, loose, county.herald_details, county.duplicate_parcels,
                                  file_day(getattr(county, "county_file_stamp", None)))
        return self._hamilton or None

    def _hamilton_county(self, row, today):
        export = self._hamilton_export()
        if not export:
            return unavailable("county_read_failed")
        index, exact, loose, details, duplicates, day = export
        match = hamilton_match(row.get("property_address"), exact, loose, index)
        if match["status"] != "matched":
            return match
        parcel = match["parcel"]
        if parcel in duplicates:
            return held("duplicate_county_parcel_key", candidate_parcels=[parcel])
        record = details.get(parcel) or index[parcel]
        unit = str(record.get("situs_unit") or "").strip()
        situs = record.get("property_address") or ""
        return {**match, "record_address": f"{situs} UNIT {unit}" if unit else situs,
                "owner": record.get("owner"), "owner_source": "Hamilton County Assessor weekly export",
                "value": record.get("appraised"), "source": "hamilton_assessor", "observed_on": day,
                "source_url": HAMILTON_URL}

    def _knox(self, row, today):
        raw = row["raw_payload"]
        roll = raw.get("knox_assessor")
        # The stored Knox bill was read from a host whose robots.txt closes all
        # crawling, so it is not used unless that source is cleared by the owner.
        if not KNOX_STORED_BILL_OK or not isinstance(roll, dict):
            return held("no_permitted_county_source")
        value = roll.get("appraised_value")
        if (not raw.get("parcel_id") or roll.get("parcel_id") != raw.get("parcel_id")
                or roll.get("case_number") != raw.get("case_number")
                or roll.get("property_address") != row.get("property_address")
                or not roll.get("owner") or not roll.get("checked_on")
                or type(value) not in (int, float) or value <= 0):
            return held("stored_county_record_does_not_match_case")
        return {"status": "matched", "parcel": roll["parcel_id"], "record_address": roll["property_address"],
                "owner": roll["owner"], "owner_source": "Knox County Trustee current-year real property bill",
                "value": value, "source": "knox_trustee_bill", "observed_on": roll["checked_on"],
                "source_url": roll.get("source_url"), "match_method": "stored_bill_for_docket_parcel_and_street",
                "tax_year": roll.get("tax_year")}

    def _comptroller(self, row, today):
        county = SOURCES[row["bot_source"]]
        ck = county_key(county)
        if not COMPTROLLER_DB.exists():
            return unavailable("county_roll_not_loaded")
        con = sqlite3.connect(str(COMPTROLLER_DB))
        con.row_factory = sqlite3.Row
        try:
            if not con.execute("SELECT 1 FROM parcels WHERE county=? LIMIT 1", (ck,)).fetchone():
                return unavailable("county_roll_not_loaded")
            key = street_key_from_lead(row.get("property_address"))
            if not key:
                return held("invalid_address")
            rows = con.execute("SELECT * FROM parcels WHERE county=? AND street_key=?", (ck, key)).fetchall()
        finally:
            con.close()
        if not rows:
            return held("no_county_address")
        if len(rows) > 1:
            return held("ambiguous_address", candidate_parcels=sorted(r["parcelid"] for r in rows))
        record = _record(rows[0])
        name = COMPTROLLER_NAMES[county]
        return {"status": "matched", "parcel": record["parcelid"], "record_address": record["address"],
                "owner": record["owner"], "owner_source": "Tennessee Comptroller county assessment roll",
                "value": record["appraisal"], "source": "comptroller", "observed_on": self._roll_day(name, today),
                "source_url": COMPTROLLER_URL + name + ".zip", "match_method": "unique_county_street"}

    @staticmethod
    def _roll_day(name, today):
        try:
            stamp = json.loads(COMPTROLLER_STATE.read_text(encoding="utf-8"))[name]
            return parsedate_to_datetime(stamp).date().isoformat()
        except (OSError, ValueError, KeyError, TypeError):
            try:
                return datetime.fromtimestamp(COMPTROLLER_DB.stat().st_mtime, timezone.utc).date().isoformat()
            except OSError:
                return today

    def resolve(self, row, today):
        county = SOURCES[row["bot_source"]]
        reader = {"shelby": self._shelby, "hamilton": self._hamilton_county, "knox": self._knox}.get(
            county, self._comptroller)
        return reader(row, today)

    # ── one row ─────────────────────────────────────────────────────────────

    def check(self, row, today) -> Dict[str, Any]:
        """Plan entry for one row. Reads the county only when the row needs
        it and has not been checked recently."""
        county = SOURCES.get(row.get("bot_source"))
        base = {"id": row.get("id"), "bot_source": row.get("bot_source"), "county": county,
                "property_address": row.get("property_address"), "reason": None, "evidence": None, "write": {}}
        if not county:
            return {**base, "status": "skipped", "reason": "source_not_covered"}
        if row.get("staging_status") == "rejected":
            return {**base, "status": "skipped", "reason": "rejected_row"}
        if code_severity(row) != "severe":
            return {**base, "status": "skipped", "reason": "not_a_serious_case"}
        if row.get("property_value") and row.get("owner_name_records"):
            return {**base, "status": "skipped", "reason": "value_and_owner_present"}
        prior = row.get("prior")
        if _fresh(prior, today):
            if prior.get("status") != "matched":
                return {**base, "status": "skipped", "reason": "checked_recently", "evidence": prior}
            entry = decide(row, from_prior(prior), today)
            if not entry["write"]:
                return {**base, "status": "skipped", "reason": "checked_recently", "evidence": prior}
            return entry
        if self._failed_reads[county] >= MAX_FAILED_READS:
            return {**base, "status": "unavailable", "reason": "county_left_alone_after_failed_reads"}
        try:
            found = self.resolve(row, today)
        except Exception as exc:  # a failed read changes nothing
            self.logger.warning("County read failed for %s (%s)", row.get("id"), type(exc).__name__)
            found = unavailable("county_read_failed")
        if found["status"] == "unavailable":
            self._failed_reads[county] += 1
        else:
            self._failed_reads[county] = 0
        return decide(row, found, today)

    @staticmethod
    def apply(client, row, entry) -> Dict[str, bool]:
        """Write one plan entry. The stored procedure merges the evidence key
        and sets the value only where the row has none; the owner update is
        refused if an owner arrived in the meantime."""
        done = {"evidence": False, "value": False, "owner": False}
        write = entry.get("write") or {}
        payload: Dict[str, Any] = {"id": row["id"]}
        if entry.get("evidence") and entry["evidence"] != row.get("prior"):
            payload["raw_patch"] = {KEY: entry["evidence"]}
        if write.get("property_value") and not row.get("property_value"):
            payload.update({k: write[k] for k in ("property_value", "property_value_source", "property_value_as_of")})
        if len(payload) > 1:
            if int(client.rpc("apply_staging_enrichment", {"p_rows": [payload]}).execute().data or 0) != 1:
                return done
            done["evidence"] = "raw_patch" in payload
            done["value"] = "property_value" in payload
        if write.get("owner_name_records") and not row.get("owner_name_records"):
            data = (client.table(TABLE).update({"owner_name_records": write["owner_name_records"]})
                    .eq("id", row["id"]).eq("bot_source", row["bot_source"])
                    .is_("owner_name_records", "null").execute().data)
            done["owner"] = bool(data)
        return done

    # ── selecting rows ──────────────────────────────────────────────────────

    def candidates(self, client, today, cap) -> List[Dict[str, Any]]:
        """Newest serious cases per source that lack a value or an owner and
        have no recent check. Small pages, a handful of short fields per row."""
        from .memphis_codes_bot import HIGH_SIGNAL_REQUEST_TYPES
        cutoff = (date.fromisoformat(today) - timedelta(days=RECHECK_DAYS)).isoformat()
        per_source: Dict[str, List[Dict[str, Any]]] = {}
        for source in SOURCES:
            picked: List[Dict[str, Any]] = []
            before = None
            for _ in range(MAX_PAGES):
                q = (client.table(TABLE).select(SELECT + ",staged_at").eq("bot_source", source)
                     .neq("staging_status", "rejected")
                     .or_("property_value.is.null,owner_name_records.is.null")
                     .or_(f"raw_payload->{KEY}->>checked_on.is.null,raw_payload->{KEY}->>checked_on.lt.{cutoff}"))
                if source == "memphis_codes":
                    # Only the request types the rule calls serious.
                    q = q.in_("raw_payload->memphis_311->>REQUEST_TYPE",
                              sorted(t for t in HIGH_SIGNAL_REQUEST_TYPES if OTHER_CITY_RX.search(t.upper())))
                elif source == "knoxville_poh":
                    q = q.not_.is_("raw_payload->>poh_section", "null")
                if before:
                    q = q.lt("staged_at", before)
                rows = q.order("staged_at", desc=True).limit(PAGE).execute().data or []
                for row in rows:
                    shaped = shape(row)
                    if code_severity(shaped) == "severe":
                        picked.append(shaped)
                if len(rows) < PAGE or len(picked) >= cap or not rows[-1].get("staged_at"):
                    break
                before = rows[-1]["staged_at"]
            per_source[source] = picked[:cap]
        # Take turns so one city's backlog cannot use the whole allowance.
        out: List[Dict[str, Any]] = []
        for i in range(cap):
            for source in SOURCES:
                if i < len(per_source[source]) and len(out) < cap:
                    out.append(per_source[source][i])
        return out

    @staticmethod
    def rows_by_id(client, ids: Iterable[str]) -> List[Dict[str, Any]]:
        ids = list(dict.fromkeys(ids))
        rows: List[Dict[str, Any]] = []
        for start in range(0, len(ids), PAGE):
            rows += client.table(TABLE).select(SELECT).in_("id", ids[start:start + PAGE]).execute().data or []
        by_id = {r["id"]: shape(r) for r in rows}
        return [by_id[i] for i in ids if i in by_id]

    # ── runs ────────────────────────────────────────────────────────────────

    def process(self, client, rows, *, write, cap=None, today=None, budget=None) -> Dict[str, Any]:
        today = today or datetime.now(timezone.utc).date().isoformat()
        counts: Counter = Counter()
        plan: List[Dict[str, Any]] = []
        started = time.monotonic()
        for row in rows:
            if cap is not None and counts["checked"] >= cap:
                break
            if budget is not None and time.monotonic() - started >= budget:
                counts["out_of_time"] += 1
                break
            entry = self.check(row, today)
            if entry["status"] != "skipped":
                counts["checked"] += 1
            counts[entry["status"]] += 1
            entry["written"] = {"evidence": False, "value": False, "owner": False}
            if write and entry["status"] in ("matched", "held") and (entry["write"] or entry["evidence"] != row.get("prior")):
                try:
                    entry["written"] = self.apply(client, row, entry)
                except Exception as exc:
                    counts["write_failed"] += 1
                    self.logger.warning("Write failed for %s (%s)", row.get("id"), type(exc).__name__)
            for key, did in entry["written"].items():
                counts["wrote_" + key] += bool(did)
            plan.append(entry)
        return {"counts": dict(counts), "plan": plan}

    def run(self) -> Dict[str, Any]:
        started = datetime.now(timezone.utc)
        write = os.environ.get("CODE_CASE_VALUES_WRITE", "1").strip().lower() not in ("0", "false", "off", "no")
        counts: Dict[str, Any] = {}
        error = None
        if write:
            self._report_health(status="running", started_at=started, finished_at=None,
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        try:
            client = _supabase()
            if client is None:
                raise ValueError("no_supabase_client")
            today = started.date().isoformat()
            rows = self.candidates(client, today, MAX_PER_RUN)
            counts = self.process(client, rows, write=write, cap=MAX_PER_RUN, today=today,
                                  budget=BUDGET_SECONDS)["counts"]
        except Exception as exc:
            error = type(exc).__name__ + ": " + str(exc)
        wrote = counts.get("wrote_value", 0) + counts.get("wrote_owner", 0)
        status = "failed" if error else ("ok" if wrote else "all_dupes")
        if write:
            self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=counts.get("checked", 0), parsed_count=counts.get("matched", 0),
                                staged_count=wrote, duplicate_count=counts.get("skipped", 0),
                                error_message=error, notes=counts)
        return {"name": self.name, "status": status, "staged": wrote, "fetched": counts.get("checked", 0),
                "duplicates": counts.get("skipped", 0), "error": error, **counts}


def run() -> dict:
    return CodeCaseValuesBot().run()


def plan_row(entry: Dict[str, Any]) -> Dict[str, Any]:
    """One line of the plan file: what was matched and what would be written."""
    ev = entry.get("evidence") or {}
    return {"id": entry["id"], "bot_source": entry["bot_source"], "county": entry["county"],
            "property_address": entry["property_address"], "status": entry["status"],
            "reason": entry.get("reason"), "parcel": ev.get("parcel") if entry["status"] == "matched" else None,
            "candidate_parcels": ev.get("candidate_parcels") or (
                [ev["parcel"]] if entry["status"] == "held" and ev.get("parcel") else None),
            "match_method": ev.get("match_method"), "record_address": ev.get("record_address"),
            "owner": ev.get("owner") if entry["status"] == "matched" else None,
            "owner_source": ev.get("owner_source") if entry["status"] == "matched" else None,
            "value": ev.get("value"), "basis": ev.get("value_basis"), "observed_on": ev.get("value_as_of") or ev.get("owner_as_of"),
            "checked_on": ev.get("checked_on"), "source_url": ev.get("source_url"),
            "write": entry.get("write") or {}, "evidence": entry.get("evidence"),
            "written": entry.get("written")}


def _ids(path: str) -> List[str]:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return [str(x["id"] if isinstance(x, dict) else x) for x in data]


def _save(path: str, payload: Any) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    os.replace(tmp, target)


def apply_plan(bot: CodeCaseValuesBot, client, entries: List[Dict[str, Any]], write: bool) -> Dict[str, Any]:
    """Write a reviewed plan exactly as it stands. No county is read. A row
    whose source or address changed since the plan was made is left alone."""
    rows = {r["id"]: r for r in bot.rows_by_id(client, [e["id"] for e in entries])}
    counts: Counter = Counter()
    out = []
    for entry in entries:
        row = rows.get(entry["id"])
        result = {"id": entry["id"], "written": {"evidence": False, "value": False, "owner": False}, "note": None}
        if entry.get("status") not in ("matched", "held") or not entry.get("evidence"):
            result["note"] = "nothing_to_write"
        elif (row is None or row["bot_source"] != entry["bot_source"]
              or row.get("property_address") != entry.get("property_address")
              or row.get("staging_status") == "rejected"):
            result["note"] = "row_changed_since_plan"
        elif write:
            try:
                result["written"] = bot.apply(client, row, entry)
            except Exception as exc:
                result["note"] = "write_failed_" + type(exc).__name__
        else:
            result["note"] = "dry_run"
        counts[result["note"] or "applied"] += 1
        for key, did in result["written"].items():
            counts["wrote_" + key] += bool(did)
        out.append(result)
    return {"counts": dict(counts), "results": out}


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="County value and owner for serious code cases. Dry run unless --write.")
    ap.add_argument("--ids-file", help="JSON list of staging ids (or of objects with an id): check only these rows")
    ap.add_argument("--apply-plan", help="plan file from an earlier run: write it as reviewed, read no county")
    ap.add_argument("--plan", help="where to save the plan (or the write receipt with --apply-plan)")
    ap.add_argument("--cap", type=int, help="most county checks in this run")
    ap.add_argument("--env-file", help="load credentials from this .env file")
    ap.add_argument("--write", action="store_true", help="write to staging; without it nothing is written")
    args = ap.parse_args(argv)
    try:
        from dotenv import load_dotenv
        candidates = [Path(args.env_file)] if args.env_file else [p / ".env" for p in Path(__file__).resolve().parents]
        for candidate in candidates:
            if candidate.exists():
                load_dotenv(candidate, override=False)
                break
    except ImportError:
        pass
    client = _supabase()
    if client is None:
        print("no database credentials")
        return 1
    bot = CodeCaseValuesBot()
    if args.apply_plan:
        entries = json.loads(Path(args.apply_plan).read_text(encoding="utf-8"))["rows"]
        result = apply_plan(bot, client, entries, args.write)
        if args.plan:
            _save(args.plan, result)
        print(json.dumps(result["counts"], sort_keys=True))
        return 0
    today = datetime.now(timezone.utc).date().isoformat()
    if args.ids_file:
        rows, cap = bot.rows_by_id(client, _ids(args.ids_file)), args.cap
    else:
        cap = args.cap or MAX_PER_RUN
        rows = bot.candidates(client, today, cap)
    result = bot.process(client, rows, write=args.write, cap=cap, today=today)
    summary: Dict[str, Counter] = defaultdict(Counter)
    for entry in result["plan"]:
        summary[entry["bot_source"]][entry["status"] + (":" + entry["reason"] if entry.get("reason") else "")] += 1
    payload = {"made_at": datetime.now(timezone.utc).isoformat(), "wrote": bool(args.write),
               "counts": result["counts"], "by_source": {k: dict(v) for k, v in summary.items()},
               "rows": [plan_row(e) for e in result["plan"]]}
    if args.plan:
        _save(args.plan, payload)
    print(json.dumps({"counts": payload["counts"], "by_source": payload["by_source"]}, indent=1, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
