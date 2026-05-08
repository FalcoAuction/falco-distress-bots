"""
Hamilton County (Chattanooga) Property Assessor enricher.

Hamilton publishes the FULL assessor dataset as a downloadable
21MB ZIP/CSV at hamiltontn.gov, refreshed weekly. Same pattern as
the tax-delinquent CSV (hamilton_tax_delinquent_bot) but for the
APPRAISED VALUES — closes Hamilton's AVM gap.

Source: https://www.hamiltontn.gov/_downloadsAssessor/AssessorExportCSV.zip
Refresh: weekly

CSV schema (relevant fields):
  - GISLINK, MAP, GROUP, PARCEL  (composite parcel ID matching the
    hamilton_tax_delinquent CSV format "MAP+GROUP+PARCEL")
  - OWNER_NAME_1, OWNER_NAME_2, OWNER_NAME_3
  - ST_NUM, ST_DIR_PFX, ST_NAME, ST_TYPE_SFX, ST_ADDR_UNIT, ST_ADDRESS
  - MAIL_ST_NAME, MAIL_UNIT, MAIL_LINE_2, MAIL_CITY, MAIL_STATE, MAIL_ZIP
  - LEGAL_DESC, CALC_ACRES
  - LAND_USE_CODE, LAND_USE_CODE_DESC, NEIGHBORHOOD_CODE
  - LAND_VALUE, BUILD_VALUE, YARDITEMS_VALUE
  - APPRAISED_VALUE   ← THE AVM
  - ASSESSED_VALUE
  - DISTRICT, DISTRICT_DESC, ZONING, ZONING_DESC
  - PROPERTY_TYPE_CODE_DESC, EXEMPT_CODE_DESC
  - SALE_1_DATE, SALE_1_CONSIDERATION (price), SALE_1_BOOK, SALE_1_PAGE
  - SALE_2/3/4 history
  - SUBDIVISION_NAME, CURRENT_USE_CODE_DESC

Two-mode workflow:
  1. ENRICH MODE: walk staged + live Hamilton leads, match by parcel
     ID (the hamilton_tax_delinquent CSV provides Map+Group+Parcel),
     fill APPRAISED_VALUE + last_sale_date/price + owner mailing.
  2. SOURCE MODE (optional, disabled by default): scan ALL ~150K
     Hamilton parcels for owner-occupancy mismatch (mailing != property)
     as standalone absentee-owner leads. Generates noise; only enable
     when explicitly testing.

Distress type: N/A (enricher).
"""

from __future__ import annotations

import csv
import io
import sys
import traceback as tb
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ._base import BotBase, _supabase
from ._provenance import record_field


HAMILTON_ASSESSOR_ZIP = (
    "https://www.hamiltontn.gov/_downloadsAssessor/AssessorExportCSV.zip"
)


def _norm_parcel(map_part: str, group_part: str, parcel_part: str) -> Optional[str]:
    """Normalize MAP/GROUP/PARCEL into the same key the
    hamilton_tax_delinquent_bot uses: 'MAP-GROUP-PARCEL' with whitespace
    stripped."""
    map_p = (map_part or "").strip()
    group_p = (group_part or "").strip()
    parcel_p = (parcel_part or "").strip()
    if not (map_p and parcel_p):
        return None
    return f"{map_p}-{group_p}-{parcel_p}"


def _parse_money(raw: Optional[str]) -> Optional[float]:
    """Parse the assessor CSV's whitespace-padded number columns."""
    if raw is None:
        return None
    s = raw.strip().replace(",", "")
    if not s or s == "0":
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def _parse_sale_date(raw: Optional[str]) -> Optional[str]:
    """Parse 'MM-DD-YYYY' to ISO YYYY-MM-DD."""
    if not raw:
        return None
    s = raw.strip()
    if not s:
        return None
    for fmt in ("%m-%d-%Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _build_owner_mailing(row: Dict[str, str]) -> Optional[str]:
    parts = [
        (row.get("MAIL_ST_NAME") or "").strip(),
        (row.get("MAIL_UNIT") or "").strip(),
        (row.get("MAIL_LINE_2") or "").strip(),
    ]
    parts = [p for p in parts if p]
    city = (row.get("MAIL_CITY") or "").strip()
    state = (row.get("MAIL_STATE") or "").strip()
    zip_code = (row.get("MAIL_ZIP") or "").strip()
    line1 = ", ".join(parts)
    line2_parts = [p for p in (city, state, zip_code) if p]
    line2 = " ".join(line2_parts)
    full = ", ".join([p for p in (line1, line2) if p])
    return full or None


class HamiltonAssessorBot(BotBase):
    name = "hamilton_assessor"
    description = "Hamilton County assessor (Chattanooga) — AVM + sales + owner from weekly CSV"
    throttle_seconds = 0.5
    expected_min_yield = 50

    max_leads_per_run = 5000

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
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="no_supabase_client",
            )
            return {"name": self.name, "status": "no_supabase",
                    "enriched": 0, "skipped": 0,
                    "staged": 0, "duplicates": 0, "fetched": 0}

        # Step 1: download + parse the CSV into a parcel→record dict
        self.logger.info("downloading Hamilton assessor CSV (~21MB)")
        index = self._build_parcel_index()
        if not index:
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="csv_download_or_parse_failed",
            )
            return {"name": self.name, "status": "csv_failed",
                    "enriched": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}
        self.logger.info(f"loaded {len(index)} Hamilton parcel records")

        # Also build an address-keyed secondary index for leads that
        # don't have parcel ID yet
        addr_index = self._build_address_index(index)

        # Step 2: walk Hamilton leads needing AVM
        enriched = 0
        skipped = 0
        no_match = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} Hamilton candidates to enrich")
            for row in candidates[:self.max_leads_per_run]:
                hit = self._lookup(row, index, addr_index)
                if hit is None:
                    no_match += 1
                    continue

                update: Dict[str, Any] = {}
                # Authoritative — override any prior HMDA-anchored phantom.
                if hit.get("appraised"):
                    update["property_value"] = hit["appraised"]
                    update["property_value_source"] = "hamilton_assessor"
                if hit.get("owner") and not row.get("owner_name_records"):
                    update["owner_name_records"] = hit["owner"]
                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["hamilton_assessor"] = hit
                update["raw_payload"] = existing_raw
                if not update:
                    skipped += 1
                    continue

                table = row["__table__"]
                try:
                    client.table(table).update(update).eq("id", row["id"]).execute()
                    enriched += 1
                    if table == "homeowner_requests" and "property_value" in update:
                        record_field(client, row["id"], "property_value",
                                      update["property_value"], "hamilton_assessor",
                                      confidence=1.0,
                                      metadata={"parcel": hit.get("parcel"),
                                                "land_use": hit.get("land_use")})
                except Exception as e:
                    self.logger.warning(f"  update failed id={row['id']}: {e}")
        except Exception as e:
            error_message = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")

        finished = datetime.now(timezone.utc)
        if error_message:
            status = "failed"
        elif enriched == 0 and no_match == 0:
            status = "zero_yield"
        elif enriched == 0:
            status = "all_dupes"
        else:
            status = "ok"

        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=enriched + skipped + no_match,
            parsed_count=enriched + skipped,
            staged_count=enriched, duplicate_count=skipped,
            error_message=error_message,
        )
        self.logger.info(f"enriched={enriched} skipped={skipped} no_match={no_match}")
        return {
            "name": self.name, "status": status,
            "enriched": enriched, "skipped": skipped, "no_match": no_match,
            "error": error_message,
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + skipped + no_match,
        }

    # ── CSV download + index ────────────────────────────────────────────────

    def _build_parcel_index(self) -> Dict[str, Dict[str, Any]]:
        res = self.fetch(HAMILTON_ASSESSOR_ZIP)
        if res is None or res.status_code != 200:
            self.logger.warning(f"Hamilton ZIP fetch failed: "
                                  f"{res.status_code if res else 'no-response'}")
            return {}
        try:
            zf = zipfile.ZipFile(io.BytesIO(res.content))
        except zipfile.BadZipFile as e:
            self.logger.error(f"corrupt ZIP: {e}")
            return {}
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            self.logger.error("no CSV in Hamilton assessor ZIP")
            return {}
        index: Dict[str, Dict[str, Any]] = {}
        with zf.open(names[0]) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
            reader = csv.DictReader(text)
            for row in reader:
                key = _norm_parcel(row.get("MAP", ""), row.get("GROUP", ""),
                                     row.get("PARCEL", ""))
                if not key:
                    continue
                index[key] = self._extract_record(row)
        return index

    @staticmethod
    def _extract_record(row: Dict[str, str]) -> Dict[str, Any]:
        owner = (row.get("OWNER_NAME_1") or "").strip()
        owner2 = (row.get("OWNER_NAME_2") or "").strip()
        if owner2:
            owner = f"{owner} & {owner2}"
        return {
            "parcel": _norm_parcel(row.get("MAP", ""), row.get("GROUP", ""),
                                     row.get("PARCEL", "")),
            "owner": owner or None,
            "property_address": (row.get("ST_ADDRESS") or "").strip() or None,
            "owner_mailing": _build_owner_mailing(row),
            "owner_city": (row.get("MAIL_CITY") or "").strip() or None,
            "owner_state": (row.get("MAIL_STATE") or "").strip() or None,
            "owner_zip": (row.get("MAIL_ZIP") or "").strip() or None,
            "land_value": _parse_money(row.get("LAND_VALUE")),
            "building_value": _parse_money(row.get("BUILD_VALUE")),
            "appraised": _parse_money(row.get("APPRAISED_VALUE")),
            "assessed_value": _parse_money(row.get("ASSESSED_VALUE")),
            "land_use": (row.get("LAND_USE_CODE_DESC") or "").strip() or None,
            "neighborhood": (row.get("NEIGHBORHOOD_CODE") or "").strip() or None,
            "zoning": (row.get("ZONING") or "").strip() or None,
            "property_type": (row.get("PROPERTY_TYPE_CODE_DESC") or "").strip() or None,
            "subdivision": (row.get("SUBDIVISION_NAME") or "").strip() or None,
            "calc_acres": _parse_money(row.get("CALC_ACRES")),
            "last_sale_date": _parse_sale_date(row.get("SALE_1_DATE")),
            "last_sale_price": _parse_money(row.get("SALE_1_CONSIDERATION")),
            "sale_1_book": (row.get("SALE_1_BOOK") or "").strip() or None,
            "sale_1_page": (row.get("SALE_1_PAGE") or "").strip() or None,
            "sale_history": [
                {
                    "date": _parse_sale_date(row.get(f"SALE_{i}_DATE")),
                    "price": _parse_money(row.get(f"SALE_{i}_CONSIDERATION")),
                    "book": (row.get(f"SALE_{i}_BOOK") or "").strip() or None,
                    "page": (row.get(f"SALE_{i}_PAGE") or "").strip() or None,
                }
                for i in (1, 2, 3, 4)
                if (row.get(f"SALE_{i}_DATE") or "").strip()
            ],
        }

    @staticmethod
    def _build_address_index(parcel_index: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
        """Build an address→parcel_key lookup for leads that don't yet
        have parcel ID. Only stores leading-portion of normalized addr."""
        idx: Dict[str, str] = {}
        for parcel_key, rec in parcel_index.items():
            addr = (rec.get("property_address") or "").strip().upper()
            if not addr:
                continue
            idx[addr] = parcel_key
        return idx

    # ── Candidate fetch + lookup ────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        # Paginate via .range() — PostgREST silently caps at 1000 rows per
        # query, so .limit(2500) was leaving 100+ Hamilton leads untouched
        # (Hamilton tax-delinquent corpus has 2000+ rows, half lack AVM).
        out = []
        OR_FILTER = (
            "county.eq.hamilton,property_address.ilike.%chattanooga%,"
            "property_address.ilike.%hixson%,"
            "property_address.ilike.%signal mountain%,"
            "property_address.ilike.%lookout mountain%,"
            "property_address.ilike.%collegedale%,"
            "property_address.ilike.%east ridge%,"
            "property_address.ilike.%red bank%,"
            "property_address.ilike.%soddy daisy%,"
            "property_address.ilike.%ooltewah%,"
            "property_address.ilike.%harrison%,"
            "property_address.ilike.%apison%,"
            "property_address.ilike.%sale creek%"
        )
        PAGE_SIZE = 1000
        MAX_PAGES = 10  # 10K Hamilton leads cap (corpus is ~2K, ample)
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            for page in range(MAX_PAGES):
                try:
                    q = (
                        client.table(table)
                        .select("id, property_address, county, owner_name_records, "
                                "property_value, raw_payload")
                        .or_(OR_FILTER)
                        .is_("property_value", "null")
                        .order("id")
                        .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1)
                        .execute()
                    )
                    rows = getattr(q, "data", None) or []
                    if not rows:
                        break
                    for r in rows:
                        r["__table__"] = table
                        out.append(r)
                    if len(rows) < PAGE_SIZE:
                        break
                except Exception as e:
                    self.logger.warning(
                        f"candidates query on {table} page {page} failed: {e}"
                    )
                    break
        return out

    def _lookup(self, row: Dict[str, Any],
                  parcel_index: Dict[str, Dict[str, Any]],
                  addr_index: Dict[str, str]) -> Optional[Dict[str, Any]]:
        # Try parcel ID first (most reliable)
        raw = row.get("raw_payload") or {}
        if isinstance(raw, dict):
            tax_data = raw.get("hamilton_tax_delinquent") or {}
            parcel = tax_data.get("parcel") if isinstance(tax_data, dict) else None
            if parcel and parcel in parcel_index:
                return parcel_index[parcel]
            # Some leads come from notice_enricher with a different
            # parcel format
            tpad = raw.get("tpad") or {}
            if isinstance(tpad, dict):
                tpad_parcel = tpad.get("parcel")
                if tpad_parcel and tpad_parcel in parcel_index:
                    return parcel_index[tpad_parcel]

        # Fallback: address match
        addr = (row.get("property_address") or "").strip().upper()
        if not addr:
            return None
        # Exact match
        if addr in addr_index:
            return parcel_index[addr_index[addr]]
        # Address-prefix match (drop ", Chattanooga, TN" suffix)
        head = addr.split(",")[0].strip()
        if head in addr_index:
            return parcel_index[addr_index[head]]
        return None


def run() -> dict:
    bot = HamiltonAssessorBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
