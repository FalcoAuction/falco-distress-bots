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
import sys
import tempfile
import traceback as tb
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._aircraft_match import AircraftIndex, build_index_from_zip
from ._base import BotBase, _supabase

FAA_ZIP_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"

# Flattened raw_payload fields the matcher reads. PostgREST JSON-path
# selects keep the page small; the full raw_payload is fetched only for
# rows that need a write.
_RAW_FIELDS = ("subject", "owner_name_1", "owner_name_2", "case_name", "mailing_address",
               "city", "court_id", "excerpt")
_SELECT = {
    "homeowner_requests_staging": "id, distress_type, full_name, owner_name_records, property_address, county",
    "homeowner_requests": "id, distress_type, full_name, owner_name_records, property_address, county",
}


class FaaAircraftBot(BotBase):
    name = "faa_aircraft"
    description = "Tag leads whose owner is the registrant of a TN aircraft worth ~$100K+ (FAA registry)"
    throttle_seconds = 0.0
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
            self.logger.info(f"FAA index: {sum(len(v) for v in index.by_addr.values())} TN aircraft in value bands")

            for table in ("homeowner_requests", "homeowner_requests_staging"):
                for lead in self._leads(client, table):
                    scanned += 1
                    tags = sorted(index.match(lead), key=lambda t: (t["confidence"] != "high", t["n_number"]))
                    had = lead.get("_aircraft")
                    if tags:
                        matched += 1
                    if (tags or None) == (had or None):
                        unchanged += 1
                        continue
                    raw = self._raw_payload(client, table, lead["id"])
                    if raw is None:
                        continue  # raw_payload holds a non-object; never clobber it
                    if tags:
                        raw["aircraft"] = tags
                    else:
                        raw.pop("aircraft", None)
                    try:
                        client.table(table).update({"raw_payload": raw}).eq("id", lead["id"]).execute()
                        if tags:
                            tagged += 1
                            self.logger.info(
                                f"  {table} {lead['id']} {lead.get('distress_type')}: "
                                + ", ".join(f"{t['n_number']} {t['aircraft']} ({t['match_type']})" for t in tags))
                        else:
                            cleared += 1
                    except Exception as e:
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
        # FALCO_FAA_ZIP lets a local run reuse an already-downloaded zip.
        local = os.environ.get("FALCO_FAA_ZIP")
        if local and os.path.exists(local):
            with zipfile.ZipFile(local) as zf:
                return build_index_from_zip(zf)
        fd, path = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        try:
            res = self.session.get(FAA_ZIP_URL, timeout=300, stream=True)
            if res.status_code != 200:
                self.logger.warning(f"FAA zip -> HTTP {res.status_code}")
                return None
            with open(path, "wb") as f:
                for chunk in res.iter_content(1 << 20):
                    f.write(chunk)
            with zipfile.ZipFile(path) as zf:
                return build_index_from_zip(zf)
        finally:
            try:
                os.remove(path)
            except OSError:
                pass

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
