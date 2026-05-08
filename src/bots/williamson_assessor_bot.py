"""
Williamson County (Franklin) Property Assessor enricher.

Williamson is one of the 9 EXTERNAL TPAD counties — TPAD doesn't cover
it; the county runs its own free Inigo-based property search at
inigo.williamson-tn.org. Public, no auth, returns clean JSON arrays.

Two-step lookup:
  1. GET /property_search/json/search with property_address keyword →
     returns JSON array of matches with parcel ID, owner, address,
     last sale price/date, legal description, mailing address.
  2. GET /property_search/parcel/{lrsn}?csrf={token} → HTML detail page
     with Land Market Value + Improvement Value + Total Market
     Appraisal (the AVM-equivalent), plus Year Built, Legal Acreage,
     Property Class, sale history.

Together they give us a stronger property picture than even ATTOM
provides — current market appraisal + complete sale history + legal
acreage + improvement breakdown — for free.

Williamson is the second-largest EXTERNAL TPAD county after Davidson
and is Patrick's #2 pilot-geography target.

Distress type: N/A (enricher only)
"""

from __future__ import annotations

import re
import sys
import traceback as tb
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup

from ._base import BotBase, _supabase


INIGO_BASE = "https://inigo.williamson-tn.org/property_search"
INIGO_HOME = INIGO_BASE + "/"
INIGO_SEARCH = INIGO_BASE + "/json/search"
INIGO_DETAIL = INIGO_BASE + "/parcel/{lrsn}"

CSRF_FORM_RE = re.compile(r'name="csrf_token"[^>]+value="([^"]+)"')


def _split_address(address: str) -> Optional[Tuple[str, str]]:
    """Extract street number + first significant street keyword.
    Returns (number, search_keyword) or None.
    """
    if not address:
        return None
    head = address.split(",")[0].strip()
    m = re.match(r"(\d+)\s+(.+)$", head)
    if not m:
        return None
    number = m.group(1)
    rest = m.group(2)
    tokens = [t for t in rest.split() if t.upper() not in {"N", "S", "E", "W", "NE", "NW", "SE", "SW"}]
    if not tokens:
        return None
    return (number, tokens[0])


class WilliamsonAssessorBot(BotBase):
    name = "williamson_assessor"
    description = "Williamson County (Franklin) Inigo property-search enricher"
    throttle_seconds = 1.5
    expected_min_yield = 1

    max_leads_per_run = 200

    def __init__(self):
        super().__init__()
        self._csrf_token: Optional[str] = None

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
                    "enriched": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        if not self._init_session():
            self._report_health(
                status="failed", started_at=started, finished_at=datetime.now(timezone.utc),
                fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
                error_message="failed_to_init_inigo_session",
            )
            return {"name": self.name, "status": "session_init_failed",
                    "enriched": 0, "skipped": 0, "staged": 0, "duplicates": 0, "fetched": 0}

        enriched = 0
        skipped = 0
        not_found = 0
        error_message: Optional[str] = None

        try:
            candidates = self._candidates(client)
            self.logger.info(f"{len(candidates)} Williamson candidates lacking property_value")

            for row in candidates[:self.max_leads_per_run]:
                addr = row.get("property_address") or ""
                hit = self._lookup(addr)
                if hit is None:
                    not_found += 1
                    continue

                update: Dict[str, Any] = {}
                # Authoritative — override any prior HMDA-anchored phantom.
                if hit.get("appraised"):
                    update["property_value"] = hit["appraised"]
                    update["property_value_source"] = "williamson_assessor"
                if hit.get("owner") and not row.get("owner_name_records"):
                    update["owner_name_records"] = hit["owner"]
                existing_raw = row.get("raw_payload") or {}
                if not isinstance(existing_raw, dict):
                    existing_raw = {}
                existing_raw["williamson_inigo"] = {
                    "lrsn": hit.get("lrsn"),
                    "parcel_id": hit.get("parcel_id"),
                    "owner": hit.get("owner"),
                    "owner_address": hit.get("owner_address"),
                    "property_address": hit.get("property_address"),
                    "property_city": hit.get("property_city"),
                    "land_market": hit.get("land_market"),
                    "improvement": hit.get("improvement"),
                    "appraised": hit.get("appraised"),
                    "last_price": hit.get("last_price"),
                    "last_transfer_date": hit.get("last_transfer_date"),
                    "year_built": hit.get("year_built"),
                    "acreage": hit.get("acreage"),
                    "property_class": hit.get("property_class"),
                }
                update["raw_payload"] = existing_raw

                if not update:
                    skipped += 1
                    continue

                table = row["__table__"]
                try:
                    client.table(table).update(update).eq("id", row["id"]).execute()
                    enriched += 1
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
        self.logger.info(f"enriched={enriched} skipped={skipped} not_found={not_found}")
        return {
            "name": self.name, "status": status,
            "enriched": enriched, "skipped": skipped, "not_found": not_found,
            "error": error_message,
            "staged": enriched, "duplicates": skipped,
            "fetched": enriched + skipped + not_found,
        }

    # ── Session bootstrap ──────────────────────────────────────────────────

    def _init_session(self) -> bool:
        res = self.fetch(INIGO_HOME)
        if res is None or res.status_code != 200:
            return False
        m = CSRF_FORM_RE.search(res.text)
        if not m:
            return False
        self._csrf_token = m.group(1)
        return True

    # ── Candidate query ─────────────────────────────────────────────────────

    def _candidates(self, client) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for table in ("homeowner_requests", "homeowner_requests_staging"):
            try:
                q = (
                    client.table(table)
                    .select("id, property_address, county, owner_name_records, property_value, raw_payload")
                    .or_("county.eq.williamson,property_address.ilike.%franklin%,property_address.ilike.%brentwood%,property_address.ilike.%nolensville%,property_address.ilike.%spring hill%,property_address.ilike.%fairview%")
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
        # Inigo does exact-substring matching, and Williamson stores
        # directionals between the number and street (e.g. "1284 W MAIN ST").
        # So searching "1284 Main" misses. Search by street name alone, then
        # filter results to ones starting with the target street number.
        res = self.fetch(INIGO_SEARCH, params={
            "csrf_token": self._csrf_token,
            "owner_name": "",
            "property_address": street,
            "parcel": "",
            "map_number": "",
            "lot": "",
            "subdivision": "",
            "city": "",
            "sales_date_start": "",
            "sales_date_end": "",
        })
        if res is None or res.status_code != 200:
            return None
        try:
            data = res.json()
        except Exception:
            return None
        items = data.get("data") or []
        if not items:
            return None
        # Filter to results whose property address STARTS with our number.
        target_prefix = f"{number} "
        target_street_upper = street.upper()
        candidates = [
            it for it in items
            if (it.get("Property Address") or "").startswith(target_prefix)
            and target_street_upper in (it.get("Property Address") or "").upper()
        ]
        if not candidates:
            return None  # no parcel matches both the number AND street
        if len(candidates) > 1:
            # Ambiguous — multiple parcels with same number on same street
            # (could be subunits). Pick the active one if possible.
            active = [c for c in candidates if c.get("Status") == "A"]
            if active:
                candidates = active
        chosen = candidates[0]

        out: Dict[str, Any] = {
            "lrsn": chosen.get("DT_RowId") or chosen.get("lrsn"),
            "parcel_id": chosen.get("Parcel ID"),
            "owner": chosen.get("Owner"),
            "owner_address": chosen.get("Owner Address"),
            "property_address": chosen.get("Property Address"),
            "property_city": chosen.get("Property City"),
            "last_price": chosen.get("Last Price"),
            "last_transfer_date": chosen.get("Last Transfer Date"),
        }

        # Step 2: detail page for Total Market Appraisal + Year Built + Acreage
        if out["lrsn"]:
            detail = self._fetch_detail(out["lrsn"])
            if detail:
                out.update(detail)
        return out

    def _fetch_detail(self, lrsn: int) -> Optional[Dict[str, Any]]:
        url = INIGO_DETAIL.format(lrsn=lrsn)
        res = self.fetch(url, params={"csrf": self._csrf_token})
        if res is None or res.status_code != 200:
            return None
        return self._parse_detail(res.text)

    @staticmethod
    def _parse_detail(html: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        soup = BeautifulSoup(html, "html.parser")

        # Find the values table (Land Market Value / Improvement Value / Total Market Appraisal)
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                th = row.find("th")
                td = row.find("td")
                if not th or not td:
                    continue
                label = th.get_text(strip=True)
                value = td.get_text(strip=True)
                m = re.search(r"\$([\d,]+)", value)
                amount = float(m.group(1).replace(",", "")) if m else None
                if label.startswith("Land Market Value"):
                    out["land_market"] = amount
                elif label.startswith("Improvement Value"):
                    out["improvement"] = amount
                elif label.startswith("Total Market Appraisal"):
                    out["appraised"] = amount

        # Year Built, Legal Acreage, Property Class via dt/dd pairs
        for dt in soup.find_all("dt"):
            dd = dt.find_next_sibling("dd")
            if not dd:
                continue
            label = dt.get_text(strip=True)
            value = dd.get_text(strip=True)
            if label == "Year Built" and value:
                try:
                    out["year_built"] = int(value)
                except ValueError:
                    pass
            elif label == "Legal Acreage" and value:
                try:
                    out["acreage"] = float(value)
                except ValueError:
                    pass
            elif label == "Property Class" and value:
                out["property_class"] = value
        return out


def run() -> dict:
    bot = WilliamsonAssessorBot()
    return bot.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        bot = WilliamsonAssessorBot()
        bot._init_session()
        for addr in sys.argv[1:]:
            print(f"{addr}: {bot._lookup(addr)}")
    else:
        print(run())
