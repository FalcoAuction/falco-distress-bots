"""
ATF firearms-licensee overlay — tags leads whose decedent, owner, or
business holds (or held) a federal firearms license.

Input: ATF monthly state listings saved by hand as CSVs in FALCO_FFL_DIR
(default data/atf). ATF's CDN refuses scripted downloads, so there is no
fetch step; with no files the bot reports `skipped_no_creds` and leaves
existing tags alone. Monthly file:
  https://www.atf.gov/sites/default/files/ffl/MMYY-ffl-list-tennessee.csv
Keep past months in the folder: a decedent drops off the list after death.

Output: raw_payload.ffl (list of licenses) on staging + live leads. Tags
that stop matching are removed. Matching rules live in `_ffl_match.py`.

Distress type: N/A — utility enricher.
"""

from __future__ import annotations

import os
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from ._base import BotBase, _supabase
from ._ffl_match import FflIndex, list_files, load_rows

DEFAULT_DIR = os.path.join("data", "atf")
_COLS = ("id, distress_type, full_name, owner_name_records, property_address, "
         "subject:raw_payload->>subject, owner_name_1:raw_payload->>owner_name_1, "
         "case_name:raw_payload->>case_name, decedent:raw_payload->court_notice->>decedent, "
         "warn_company:raw_payload->warn->>company, _ffl:raw_payload->ffl")


class FflOverlayBot(BotBase):
    name = "ffl_overlay"
    description = "Tag leads whose decedent/owner/business is an ATF firearms licensee"
    throttle_seconds = 0.0
    expected_min_yield = 0
    frozen_after_runs = 10_000
    page_size = 1000
    max_pages = 200

    def scrape(self):
        return []

    def run(self) -> Dict[str, Any]:
        files = list_files(os.environ.get("FALCO_FFL_DIR") or DEFAULT_DIR)
        if not files:
            self.logger.info("no ATF listing CSVs found; skipped (save monthly files to data/atf)")
            return {"name": self.name, "status": "skipped_no_creds", "staged": 0, "duplicates": 0, "fetched": 0}

        started = datetime.now(timezone.utc)
        self._report_health(status="running", started_at=started, finished_at=None,
                            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0)
        client = _supabase()
        if client is None:
            self._report_health(status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                                error_message="no_supabase_client")
            return {"name": self.name, "status": "no_supabase", "staged": 0, "duplicates": 0, "fetched": 0}

        scanned = tagged = cleared = unchanged = 0
        error_message: Optional[str] = None
        try:
            index = FflIndex(load_rows(files))
            self.logger.info(f"ATF index: {index.count} licenses from {len(files)} listing file(s)")
            for table in ("homeowner_requests", "homeowner_requests_staging"):
                for row in self._rows(client, table):
                    scanned += 1
                    tags = sorted(index.match(row), key=lambda t: (t["confidence"] != "high", t["license_name"] or ""))
                    if (tags or None) == (row.get("_ffl") or None):
                        unchanged += 1
                        continue
                    full = self._raw_payload(client, table, row["id"])
                    if full is None:
                        continue
                    if tags:
                        full["ffl"] = tags
                    else:
                        full.pop("ffl", None)
                    try:
                        client.table(table).update({"raw_payload": full}).eq("id", row["id"]).execute()
                        if tags:
                            tagged += 1
                            self.logger.info(f"  {table} {row['id']} {row.get('distress_type')}: "
                                             + ", ".join(f"{t['license_label']} {t['license_name']} ({t['match_type']})"
                                                         for t in tags))
                        else:
                            cleared += 1
                    except Exception as e:
                        self.logger.warning(f"  update failed {table} id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        status = "failed" if error_message else "ok" if (tagged or cleared) else "all_dupes" if scanned else "zero_yield"
        self._report_health(status=status, started_at=started, finished_at=datetime.now(timezone.utc),
                            fetched_count=scanned, parsed_count=scanned, staged_count=tagged + cleared,
                            duplicate_count=unchanged, error_message=error_message)
        self.logger.info(f"scanned={scanned} tagged={tagged} cleared={cleared}")
        return {"name": self.name, "status": status, "scanned": scanned, "tagged": tagged, "cleared": cleared,
                "error": error_message, "staged": tagged + cleared, "duplicates": unchanged, "fetched": scanned}

    def _rows(self, client, table: str):
        last_id = None
        for _ in range(self.max_pages):
            q = client.table(table).select(_COLS).order("id").limit(self.page_size)
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
    return FflOverlayBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
