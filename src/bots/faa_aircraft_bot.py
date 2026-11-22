"""
FAA aircraft overlay — tags leads whose owner holds a TN-registered
aircraft plausibly worth $100K+.

Why: a distressed owner (pre-foreclosure, tax delinquent, bankrupt,
deceased) who also owns a plane has a liquid asset that can be sold at
auction. That is an owner-direct outreach angle nobody else works, and
the source is free: the FAA releasable aircraft registry, rebuilt every
federal business day as one public zip.

  https://registry.faa.gov/database/ReleasableAircraft.zip

Matching rules and value bands live in `_aircraft_match.py` (pure, tested).

Output: `raw_payload.aircraft` on homeowner_requests_staging and
homeowner_requests — a list of match dicts (N-number, aircraft, value
band, registrant, match type, confidence). No schema change. Rows that
carried a tag but no longer match (plane sold, registration moved) have
the key removed, so the tag always reflects today's registry.

Distress type: N/A — utility enricher.
"""

from __future__ import annotations

import os
import json
import re
import sys
import tempfile
import traceback as tb
import zipfile
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from ._aircraft_match import AircraftIndex, build_index_from_zip
from ._base import BotBase, _supabase

FAA_ZIP_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"
# Registrant states indexed. Leads are TN unless raw_payload.state says
# otherwise (the court-feed bot stages FL leads with state=FL).
STATES = ("TN", "FL")
# One download per process: the court-feed bot and this bot share it.
_INDEX_CACHE: Dict[str, Any] = {}


def evidence_tags(index, lead):
    """Retain weak matches for research; never confuse shared address with identity."""
    def identity(name):
        return sorted(re.findall(r"[A-Z0-9]+", str(name or "").upper()))
    names = [identity(lead.get(k)) for k in
             ("owner_name_records", "full_name", "subject", "owner_name_1", "owner_name_2")]
    tags = index.match(lead)
    for tag in tags:
        registrants = [tag.get("registrant")] + (tag.get("co_owners") or [])
        tag.update(verified_on=date.today().isoformat(),
                   registry_date=getattr(index, "registry_date", None), source_url=FAA_ZIP_URL,
                   identity_verified=any(identity(n) in names for n in registrants if identity(n)),
                   title_status="not_established", availability="unknown")
        try:
            expiry = datetime.strptime(tag.get("registration_expires", ""), "%Y%m%d").date()
            if expiry <= date.today():
                tag["identity_verified"] = False
        except (ValueError, TypeError):
            tag["identity_verified"] = False
    return sorted(tags, key=lambda t: (t["confidence"] != "high", t["n_number"]))

# Flattened raw_payload fields the matcher reads. PostgREST JSON-path
# selects keep the page small; the full raw_payload is fetched only for
# rows that need a write.
_RAW_FIELDS = ("subject", "owner_name_1", "owner_name_2", "case_name", "mailing_address",
               "city", "court_id", "excerpt", "state")
_SELECT = {
    "homeowner_requests_staging": "id, distress_type, full_name, owner_name_records, property_address, county",
    "homeowner_requests": "id, distress_type, full_name, owner_name_records, property_address, county",
}


class FaaAircraftBot(BotBase):
    name = "faa_aircraft"
    description = "Tag leads whose owner is the registrant of a TN/FL aircraft worth ~$100K+ (FAA registry)"
    throttle_seconds = 0.0
    # registry.faa.gov (Akamai) 403s user agents that do not start with
    # "Mozilla/5.0", and also ones carrying a contact email or "+URL" (tested
    # 2026-11-15). This is the standard crawler form: it names FALCO and does
    # not pose as a browser; the contact rides in the HTTP From header.
    user_agent = "Mozilla/5.0 (compatible; FALCO-Lead-Research/1.0)"
    contact = "ops@falco.llc"
    expected_min_yield = 0          # matches are rare; zero is a normal day
    frozen_after_runs = 10_000      # enricher, not a source

    page_size = 1000
    max_pages = 200                 # ~200k rows per table; staging is ~50k today

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
            return {"name": self.name, "status": "no_supabase", "tagged": 0, "cleared": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        scanned = tagged = cleared = unchanged = matched = 0
        error_message: Optional[str] = None
        try:
            index = self._load_index()
            if index is None:
                raise RuntimeError("faa_registry_download_failed")
            self.logger.info(f"FAA index: {sum(len(v) for v in index.by_addr.values())} {'/'.join(STATES)} aircraft in value bands")

            for table in ("homeowner_requests", "homeowner_requests_staging"):
                for lead in self._leads(client, table):
                    scanned += 1
                    tags = evidence_tags(index, lead)
                    had = lead.get("_aircraft")
                    if tags:
                        matched += 1
                    if (tags or None) == (had or None):
                        unchanged += 1
                        continue
                    raw = self._raw_payload(client, table, lead["id"])
                    if raw is None:
                        continue  # raw_payload holds a non-object; never clobber it
                    original = dict(raw)
                    if tags:
                        raw["aircraft"] = tags
                    else:
                        raw.pop("aircraft", None)
                    try:
                        result = client.table(table).update({"raw_payload": raw}).eq("id", lead["id"]) \
                            .eq("raw_payload", json.dumps(original)).execute()
                        if not getattr(result, "data", None):
                            raise RuntimeError("concurrent change or missing row; retry next run")
                        if tags:
                            tagged += 1
                            self.logger.info(
                                f"  {table} {lead['id']} {lead.get('distress_type')}: "
                                + ", ".join(f"{t['n_number']} {t['aircraft']} ({t['match_type']})" for t in tags))
                        else:
                            cleared += 1
                    except Exception as e:
                        error_message = "aircraft_update_failed"
                        self.logger.warning(f"  update failed {table} id={lead['id']}: {e}")
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
        self.logger.info(f"scanned={scanned} matched={matched} tagged={tagged} cleared={cleared}")
        return {"name": self.name, "status": status, "scanned": scanned, "matched": matched,
                "tagged": tagged, "cleared": cleared, "error": error_message,
                "staged": tagged + cleared, "duplicates": unchanged, "fetched": scanned}

    # ── FAA registry ────────────────────────────────────────────────────

    def _load_index(self) -> Optional[AircraftIndex]:
        cache_key = (date.today().isoformat(), os.environ.get("FALCO_FAA_ZIP"))
        if _INDEX_CACHE.get("key") == cache_key:
            return _INDEX_CACHE["index"]
        index = self._build_index()
        if index is not None:
            _INDEX_CACHE["index"] = index
            _INDEX_CACHE["key"] = cache_key
        return index

    def _build_index(self) -> Optional[AircraftIndex]:
        # FALCO_FAA_ZIP lets a local run reuse an already-downloaded zip.
        local = os.environ.get("FALCO_FAA_ZIP")
        if local and os.path.exists(local):
            with zipfile.ZipFile(local) as zf:
                return self._validated_index(zf)
        fd, path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        try:
            res = self.session.get(FAA_ZIP_URL, timeout=300, stream=True, headers={"From": self.contact})
            if res.status_code != 200:
                self.logger.warning(f"FAA zip -> HTTP {res.status_code}")
                return None
            with open(path, "wb") as f:
                for chunk in res.iter_content(1 << 20):
                    f.write(chunk)
            with zipfile.ZipFile(path) as zf:
                return self._validated_index(zf)
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

    @staticmethod
    def _validated_index(zf):
        registry_date = date(*zf.getinfo("MASTER.txt").date_time[:3])
        if not 0 <= (date.today() - registry_date).days <= 7:
            raise ValueError("FAA snapshot stale or future-dated; retain existing evidence")
        index = build_index_from_zip(zf, STATES)
        if not index.by_addr:
            raise ValueError("FAA snapshot empty; refuse mass removal")
        index.registry_date = registry_date.isoformat()
        return index

    # ── Supabase I/O ────────────────────────────────────────────────────

    def _leads(self, client, table: str):
        cols = _SELECT[table] + ", " + ", ".join(f"{k}:raw_payload->>{k}" for k in _RAW_FIELDS) \
            + ", _aircraft:raw_payload->aircraft"
        # Keyset paging on the primary key: deep OFFSET pages plus JSON
        # extraction hit the statement timeout on staging (~50k rows).
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
    return FaaAircraftBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
