"""
Davidson County (Nashville) Property Assessor enricher — fills the
TPAD external-county gap for Tennessee's largest county by population.

TPAD covers 86 of 95 TN counties; Davidson is one of the 9 EXTERNAL
counties that redirect to their own assessor sites. Davidson's portal
is the Catalis OFS system at portal.padctn.org — public, no auth, no
captcha.

Search endpoint: POST /OFS/WP/PropertySearch/QuickPropertySearchAsync
Returns: account_id, parcel ID, owner, mailing address, total
appraised value, land size, land-use code.

This bot is an ENRICHER — it walks existing Davidson leads (live +
staging) that lack `property_value` and fills it from the assessor.

Distress type: N/A (enricher, not a lead source)
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ._base import BotBase, LeadPayload, _supabase, make_session
from ._provenance import record_field


PADCTN_BASE = "https://portal.padctn.org"
PADCTN_HOME = PADCTN_BASE + "/OFS/WP/Home"
PADCTN_QUICKSEARCH = PADCTN_BASE + "/OFS/WP/PropertySearch/QuickSearch"
PADCTN_SEARCH = PADCTN_BASE + "/OFS/WP/PropertySearch/QuickPropertySearchAsync?Length=14"

ACCOUNT_RE = re.compile(r"OnSearchGridSelectAccount\((\d+)")
APPRAISED_RE = re.compile(r"Total Appraised:\s*\$([\d,]+)")
LAND_SIZE_RE = re.compile(r"Land Size:\s*([\d.]+)\s*acres", re.IGNORECASE)
LAND_USE_RE = re.compile(r"Land Use:\s*([A-Z0-9][A-Z0-9 \-]*)")
PARCEL_RE = re.compile(r"^\s*(\d{3}\s+\d{1,2}[A-Z0-9 ]+\.\d{2})", re.MULTILINE)
STREET_NUM_RE = re.compile(r"^\s*(\d+)\s+(.+?)$")


def _split_address(address: str) -> Optional[Tuple[str, str]]:
    """Split '4052 Windwood Ln, Nashville, TN 37214' → ('4052', 'Windwood').

    The PADCTN search needs street number + first significant token of
    the street name. Returns (number, street_keyword) or None.
    """
    if not address:
        return None
    head = address.split(",")[0].strip()
    m = STREET_NUM_RE.match(head)
    if not m:
        return None
    number = m.group(1)
    rest = m.group(2)
    # Strip directional prefix tokens that fragment search
    tokens = [t for t in rest.split() if t.upper() not in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}]
    if not tokens:
        return None
    # First non-directional token is the search anchor
    return (number, tokens[0])


class DavidsonAssessorBot(BotBase):
    name = "davidson_assessor"
    description = "Davidson County PADCTN assessor enricher (TPAD external-county fill)"
    throttle_seconds = 1.5
    expected_min_yield = 1

    # Cap per run — site is public but we should be polite.
    max_leads_per_run = 200

    def scrape(self) -> List[LeadPayload]:
        """Enricher: returns no NEW leads, only updates existing rows.
        We still implement scrape() to satisfy BotBase but keep the
        write side in run() so health reporting matches the actual work.
        """
        return []

    # Override run to do enrichment-style work + health reporting.
    def run(self) -> Dict[str, Any]:
        from datetime import datetime, timezone
        import traceback as tb

        started = datetime.now(timezone.utc)
        self._report_health(
            status="running", started_at=started, finished_at=None,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
        )

        client = _supabase()
        if client is None:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "enriched": 0, "skipped": 0}

        # Establish session cookies up front
        self.fetch(PADCTN_HOME)
        self.fetch(PADCTN_QUICKSEARCH)

        enriched = 0
        skipped = 0
        not_found = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidate_leads(client)
            self.logger.info(f"{len(candidates)} Davidson candidates lacking property_value")

            for row in candidates[:self.max_leads_per_run]:
                addr = row.get("property_address") or ""
                hit = self._lookup(addr)
                if hit is None:
                    not_found += 1
                    continue
                if not hit.get("appraised") and not hit.get("parcel"):
                    skipped += 1
                    continue
                update: Dict[str, Any] = {}
                # Assessor data is AUTHORITATIVE for property_value —
                # overrides any prior HMDA-anchored phantom value. The
                # property_value_source column is set so audits can
                # distinguish defensible county-record values from the
                # loose mortgage-anchor estimates that used to leak in.
                if hit.get("appraised"):
                    update["property_value"] = int(round(float(hit["appraised"])))
                    update["property_value_source"] = "davidson_assessor"
                if hit.get("owner") and not row.get("owner_name_records"):
                    update["owner_name_records"] = hit["owner"]
                # Always merge the assessor record into raw_payload for audit
                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["padctn"] = {
                    "account_id": hit.get("account_id"),
                    "parcel": hit.get("parcel"),
                    "land_acres": hit.get("acres"),
                    "land_use": hit.get("land_use"),
                    "appraised": hit.get("appraised"),
                    "owner": hit.get("owner"),
                    "mailing_address": hit.get("mailing_address"),
                }
                update["raw_payload"] = existing_raw

                if not update:
                    skipped += 1
                    continue

                table = row["__table__"]
                try:
                    client.table(table).update(update).eq("id", row["id"]).execute()
                    enriched += 1
                    # Record per-field provenance (live table only — staging
                    # rows get rewritten when promoted; provenance follows
                    # promotion via the _promote_staged_lead flow)
                    if table == "homeowner_requests":
                        meta = {
                            "account_id": hit.get("account_id"),
                            "parcel": hit.get("parcel"),
                        }
                        if "property_value" in update:
                            record_field(client, row["id"], "property_value",
                                          update["property_value"], "davidson_assessor",
                                          confidence=1.0, metadata=meta)
                        if "owner_name_records" in update:
                            record_field(client, row["id"], "owner_name_records",
                                          update["owner_name_records"], "davidson_assessor",
                                          confidence=1.0, metadata=meta)
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif enriched == 0 and not_found == 0:
            status = "zero_yield"
        elif enriched == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=enriched + skipped + not_found,
            parsed_count=enriched + skipped,
            staged_count=enriched, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(
            f"enriched={enriched} skipped={skipped} not_found={not_found}"
        )
        return {
            "name": self.name, "status": status, "enriched": enriched,
            "skipped": skipped, "not_found": not_found, "error": error_message,
            # Map to the stat names _run_new prints
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + skipped + not_found,
        }

    # ── Internal: query Supabase for Davidson candidates ────────────────────

    def _candidate_leads(self, client) -> List[Dict[str, Any]]:
        """Return dicts of leads from BOTH live + staging that look like
        Davidson County and lack property_value. Each row tagged with its
        source table so we update the right one."""
        out: List[Dict[str, Any]] = []
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                q = (
                    client.table(table)
                    .select("id, property_address, county, owner_name_records, property_value, raw_payload")
                    .or_("county.eq.davidson,property_address.ilike.%nashville%,property_address.ilike.%antioch%,property_address.ilike.%hermitage%,property_address.ilike.%madison%,property_address.ilike.%goodlettsville%")
                    .is_("property_value", "null")
                    .limit(500)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                for r in rows:
                    r["__table__"] = table
                    out.append(r)
            except Exception as e:
                self.logger.warning(f"candidates query on {table} failed: {e}")
        return out

    # ── Lookup ──────────────────────────────────────────────────────────────

    def _lookup(self, address: str) -> Optional[Dict[str, Any]]:
        parts = _split_address(address)
        if not parts:
            return None
        number, street = parts
        res = self.fetch(
            PADCTN_SEARCH,
            method="POST",
            data={
                "RealEstate": "true",
                "SelectedSearch": "2",   # 2 = Address
                "StreetNumber": number,
                "SingleSearchCriteria": street,
                "AlterCriteria": "False",
            },
            headers={"X-Requested-With": "XMLHttpRequest"},
        )
        if res is None or res.status_code != 200:
            return None
        return self._parse_first_result(res.text)

    @staticmethod
    def _parse_first_result(html: str) -> Optional[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        items = soup.select("td.dxdvItem")
        if not items:
            return None
        first = items[0]
        text = first.get_text("\n", strip=True)

        out: Dict[str, Any] = {}
        m = ACCOUNT_RE.search(str(first))
        if m:
            out["account_id"] = int(m.group(1))
        m = APPRAISED_RE.search(text)
        if m:
            out["appraised"] = float(m.group(1).replace(",", ""))
        m = LAND_SIZE_RE.search(text)
        if m:
            out["acres"] = float(m.group(1))
        m = LAND_USE_RE.search(text)
        if m:
            out["land_use"] = m.group(1).strip()
        m = PARCEL_RE.search(text)
        if m:
            out["parcel"] = m.group(1).strip()

        # Owner + mailing address: typically two lines after the parcel line.
        # Format observed:
        #   108 12 0B 138.00
        #   BARNYASHEV, MIROSLAV
        #   4052 WINDWOOD LN
        #   NASHVILLE
        #   37214
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        for i, ln in enumerate(lines):
            if PARCEL_RE.match(ln):
                if i + 1 < len(lines):
                    out["owner"] = lines[i + 1]
                if i + 2 < len(lines):
                    addr_lines = []
                    for j in range(i + 2, min(i + 5, len(lines))):
                        if APPRAISED_RE.match(lines[j]) or LAND_SIZE_RE.match(lines[j]):
                            break
                        addr_lines.append(lines[j])
                    if addr_lines:
                        out["mailing_address"] = ", ".join(addr_lines)
                break
        return out


def run() -> dict:
    bot = DavidsonAssessorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Test mode: padctn lookup CLI
        bot = DavidsonAssessorBot()
        bot.fetch(PADCTN_HOME)
        bot.fetch(PADCTN_QUICKSEARCH)
        for addr in sys.argv[1:]:
            print(f"{addr}: {bot._lookup(addr)}")
    else:
        print(run())
