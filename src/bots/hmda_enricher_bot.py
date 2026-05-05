"""HMDA enricher — verified mortgage data via CFPB Home Mortgage Disclosure
Act loan-level data. Free, public, covers every US county uniformly.

How it works:
  1. For each focus lead with a property address, geocode the address
     via the free Census Geocoder to get a 11-digit FIPS census tract.
  2. Use lead's known property_value (AVM/appraised) as the amount
     anchor for matching candidate loans.
  3. Use lead's known sale year if available (from raw_payload.structured
     trust_date or assessor sale_date); otherwise query a 5-year window.
  4. Download HMDA loan-level data for that county + year combo
     (cached on disk).
  5. Filter HMDA rows to: census_tract == lead's tract, action_taken=1
     (originated), lien_status=1 (first lien), loan_purpose in
     {1=purchase, 31=refi, 32=cash-out refi}.
  6. Match by loan_amount ∈ [property_value × 0.50, property_value × 1.05].
  7. If 1 candidate → high confidence. If 2+ → pick closest to
     property_value × 0.80 (typical LTV) → medium confidence.
  8. LEI → lender name via GLEIF (cached).
  9. Write mortgage_balance + phone_metadata.mortgage_signal +
     lead_field_provenance(source='hmda_match', confidence=...).

Critical caveat: HMDA matches are PROBABILISTIC. Action date is
year-only (privacy-redacted). Multiple originations may match.
We mark confidence accordingly. ROD-verified leads stay at 0.85+;
HMDA-matched leads at 0.65 (single candidate) or 0.45 (multi-candidate).

Run via:
  python -m src.bots.hmda_enricher_bot

Env:
  FALCO_HMDA_MAX_PER_RUN  (default 100)
  FALCO_HMDA_SAMPLE        (=1 for dry-run)
  FALCO_HMDA_YEAR_WINDOW   (default 6 — search this many years back)
"""
from __future__ import annotations

import csv
import io
import os
import re
import sys
import time
import traceback as tb
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from ._base import BotBase, _supabase
from ._provenance import record_field
from . import _assessor_sale_data


CORE_COUNTIES = {"davidson", "williamson", "sumner", "rutherford", "wilson"}
STRETCH_COUNTIES = {"maury", "montgomery"}
FOCUS_COUNTIES = CORE_COUNTIES | STRETCH_COUNTIES

# TN county FIPS codes — only counties we operate in
TN_COUNTY_FIPS = {
    "davidson": "47037",
    "williamson": "47187",
    "sumner": "47165",
    "rutherford": "47149",
    "wilson": "47189",
    "maury": "47119",
    "montgomery": "47125",
}

# HMDA + Census + GLEIF endpoints
CENSUS_GEOCODER = (
    "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
)
HMDA_CSV = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"
GLEIF_LEI = "https://api.gleif.org/api/v1/lei-records/"

# Years to consider when sale year is unknown. HMDA 2024 = latest available
# as of mid-2025. Earlier years available going back to 2018.
DEFAULT_YEAR_WINDOW = 6  # 2019-2024 inclusive
HMDA_LATEST_YEAR = 2024
HMDA_EARLIEST_YEAR = 2018

DEFAULT_MAX_PER_RUN = 100
REQUEST_TIMEOUT = 60

# Local on-disk cache for HMDA county-year CSVs
CACHE_DIR = Path(os.environ.get("FALCO_HMDA_CACHE_DIR", "data/hmda_cache"))


def _normalize_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).replace(",", ""))
    except (ValueError, TypeError):
        return None


def _to_int(s: Any) -> Optional[int]:
    f = _to_float(s)
    return int(f) if f is not None else None


class HmdaEnricherBot(BotBase):
    name = "hmda_enricher"
    description = (
        "HMDA loan-level matching — verifies mortgage_balance + lender via "
        "CFPB Home Mortgage Disclosure Act data. Free, all US counties."
    )
    throttle_seconds = 0.5
    expected_min_yield = 0
    max_leads_per_run = DEFAULT_MAX_PER_RUN

    def __init__(self):
        super().__init__()
        self._hmda_cache: Dict[Tuple[str, int], List[Dict[str, str]]] = {}
        self._gleif_cache: Dict[str, Optional[str]] = {}
        self._tract_cache: Dict[str, Optional[str]] = {}
        self._sale_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._session = requests.Session()
        # CFPB ffiec.cfpb.gov has an Akamai WAF that 403s "Mozilla/5.0".
        # `curl/8.0` and absent UA both pass. Use curl-style UA.
        self._session.headers.update({"User-Agent": "curl/8.0"})
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _sale_data(
        self, address: str, county: str, owner: str = ""
    ) -> Dict[str, Any]:
        """Fetch sale_date + sale_price from the county's free assessor."""
        key = (address, county, owner or "")
        if key in self._sale_cache:
            return self._sale_cache[key]
        try:
            data = _assessor_sale_data.resolve(address, county, owner=owner)
        except Exception as e:
            self.logger.warning(f"sale-data fetch failed {address!r}: {e}")
            data = {}
        self._sale_cache[key] = data
        return data

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
            return self._fail(started, "no_supabase_client")

        max_per_run = int(
            os.environ.get("FALCO_HMDA_MAX_PER_RUN", DEFAULT_MAX_PER_RUN)
        )
        sample = os.environ.get("FALCO_HMDA_SAMPLE") == "1"
        year_window = int(
            os.environ.get("FALCO_HMDA_YEAR_WINDOW", DEFAULT_YEAR_WINDOW)
        )

        candidates = self._candidates(client, max_per_run)
        self.logger.info(
            f"{len(candidates)} Middle TN focus leads to HMDA-match"
        )

        attempted = 0
        single_match = 0
        multi_match = 0
        no_tract = 0
        no_anchor = 0
        no_match = 0
        errors = 0

        try:
            for lead in candidates:
                if attempted >= max_per_run:
                    break
                attempted += 1

                addr = lead.get("property_address")
                county = _normalize_county(lead.get("county"))
                if not addr or county not in TN_COUNTY_FIPS:
                    continue

                # 1) Geocode → census tract
                tract = self._geocode(addr)
                if not tract:
                    no_tract += 1
                    continue

                # 2) Get exact sale data (sale_date + sale_price + deed
                # ref) from the county's free assessor portal. Lifts
                # match accuracy by narrowing the HMDA candidate pool.
                #
                # Three scenarios:
                #   (a) sale_date AND sale_price (real arms-length sale)
                #       → 1-year window, ±5% amount filter, very tight
                #   (b) sale_date only (price was $0 — quitclaim, gift,
                #       related-party transfer; or pre-2018)
                #       → 1-year window if 2018+, ±10% amount filter
                #         using property_value as anchor
                #   (c) no sale data at all
                #       → 6-year window, ±10%
                owner = lead.get("owner_name_records") or lead.get("full_name") or ""
                sale = self._sale_data(addr, county, owner)
                sale_year = None
                sale_price = None
                if sale.get("sale_date"):
                    m = re.search(r"^(\d{4})", str(sale["sale_date"]))
                    if m:
                        sale_year = int(m.group(1))
                if sale.get("sale_price"):
                    sale_price = _to_float(sale["sale_price"])

                # 3) Determine match anchor — sale_price > property_value
                anchor_value = sale_price or _to_float(lead.get("property_value"))
                if not anchor_value:
                    no_anchor += 1
                    continue

                # 4) Determine candidate years
                if (sale_year and HMDA_EARLIEST_YEAR <= sale_year <= HMDA_LATEST_YEAR):
                    # Recent real sale within HMDA range — 1-year window
                    year_range = [sale_year]
                    target_year = sale_year
                elif sale_year and sale_year > HMDA_LATEST_YEAR:
                    # Sale POST-HMDA-range (2025+) — HMDA hasn't published
                    # this year yet. Drop year anchor; widen window.
                    year_range = list(range(HMDA_LATEST_YEAR, HMDA_EARLIEST_YEAR - 1, -1))
                    sale_year = None  # downgrade to wide-match
                    target_year = None
                elif sale_year and sale_year < HMDA_EARLIEST_YEAR:
                    # Pre-HMDA sale — search 2018+ for refis. Drop
                    # sale_price anchor (refi amount may differ from
                    # original purchase price).
                    year_range = list(range(HMDA_LATEST_YEAR, HMDA_EARLIEST_YEAR - 1, -1))
                    sale_price = None
                    sale_year = None  # downgrade
                    target_year = None
                else:
                    target_year = self._pick_target_year(lead)
                    year_range = self._year_search_range(target_year, year_window)

                # 5) Pull HMDA + filter by tract + amount + property type.
                #
                # Window depends on anchor quality:
                #   sale_price (exact, year-exact)   → 60-100% (tight)
                #   property_value (current AVM)     → 50-105% (loose)
                if sale_price:
                    amt_min = sale_price * 0.60
                    amt_max = sale_price * 1.05
                    purpose_filter = ("1",)  # purchase only when sale-anchored
                else:
                    amt_min = anchor_value * 0.50
                    amt_max = anchor_value * 1.10
                    purpose_filter = ("1", "31", "32")  # purchase + refi
                county_fips = TN_COUNTY_FIPS[county]
                candidates_loans: List[Dict[str, str]] = []
                for yr in year_range:
                    rows = self._hmda(county_fips, yr)
                    for row in rows:
                        if row.get("census_tract") != tract:
                            continue
                        if row.get("action_taken") != "1":
                            continue
                        if row.get("lien_status") != "1":
                            continue
                        if row.get("loan_purpose") not in purpose_filter:
                            continue
                        # Filter to single-family residential (1-4 units),
                        # primary residence — drops investor + commercial
                        # + multifamily noise. Roughly halves candidate
                        # count in dense tracts.
                        ddc = row.get("derived_dwelling_category", "")
                        if "Single Family (1-4 Units)" not in ddc:
                            continue
                        # occupancy_type 1 = principal residence
                        # (1=primary, 2=second home, 3=investment)
                        if row.get("occupancy_type") not in ("1", ""):
                            continue
                        amt = _to_float(row.get("loan_amount"))
                        if amt is None:
                            continue
                        if amt < amt_min or amt > amt_max:
                            continue
                        row["__match_year__"] = str(yr)
                        candidates_loans.append(row)

                if not candidates_loans:
                    no_match += 1
                    continue

                # 5) Pick best match
                target_amt = anchor_value * 0.80
                best = min(
                    candidates_loans,
                    key=lambda r: abs(_to_float(r.get("loan_amount")) - target_amt),
                )
                multi = len(candidates_loans) > 1
                if multi:
                    multi_match += 1
                else:
                    single_match += 1

                # 6) Resolve lender name from LEI
                lei = best.get("lei", "")
                lender_name = self._gleif(lei)

                principal = _to_float(best.get("loan_amount"))
                match_year = best.get("__match_year__")
                interest_rate = _to_float(best.get("interest_rate"))

                if sample:
                    n_cand = len(candidates_loans)
                    sale_tag = (
                        f"[sale={sale_year}/${sale_price:,.0f}]"
                        if sale_price else "[no-sale]"
                    )
                    self.logger.info(
                        f"  SAMPLE id={lead['id'][:8]} addr={addr[:45]} "
                        f"{sale_tag} -> {n_cand} cands, "
                        f"picked ${principal:,.0f} ({match_year}) "
                        f"lender={lender_name!r}"
                    )
                    continue

                # Year-anchored = we know the exact origination year (1-yr window)
                # Sale-anchored = we know exact year AND price (1-yr + ±5%)
                year_anchored = bool(
                    sale_year and sale_year >= HMDA_EARLIEST_YEAR
                )
                fully_sale_anchored = year_anchored and bool(sale_price)

                self._write(client, lead, {
                    "principal": principal,
                    "lender_name": lender_name,
                    "lei": lei,
                    "match_year": match_year,
                    "interest_rate": interest_rate,
                    "loan_purpose": best.get("loan_purpose"),
                    "loan_term": best.get("loan_term"),
                    "tract": tract,
                    "candidate_count": len(candidates_loans),
                    "anchor_value": anchor_value,
                    "sale_anchored": fully_sale_anchored,
                    "year_anchored": year_anchored,
                    "sale_date": sale.get("sale_date"),
                    "sale_price": sale_price,
                    "deed_reference": sale.get("deed_reference"),
                })

        except Exception as e:
            err = f"{type(e).__name__}: {e}\n{tb.format_exc()}"
            self.logger.error(f"FAILED: {e}")
            return self._wrap(
                started, attempted, single_match, multi_match,
                no_tract, no_anchor, no_match, errors,
                status="failed", error=err,
            )

        self.logger.info(
            f"attempted={attempted} single={single_match} multi={multi_match} "
            f"no_tract={no_tract} no_anchor={no_anchor} no_match={no_match} "
            f"errors={errors}"
        )
        return self._wrap(
            started, attempted, single_match, multi_match,
            no_tract, no_anchor, no_match, errors,
        )

    # ── candidates ────────────────────────────────────────────────────────
    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        out = []
        for table, src in (("homeowner_requests_staging", "bot_source"),
                            ("homeowner_requests", "source")):
            page = 0
            while True:
                try:
                    cols = (
                        "id, county, property_address, property_value, "
                        "raw_payload, mortgage_balance, phone_metadata, "
                        "priority_score, owner_name_records, full_name"
                    )
                    r = (
                        client.table(table)
                        .select(cols)
                        .not_.is_("property_address", "null")
                        .not_.is_("property_value", "null")
                        .order("priority_score", desc=True)
                        .range(page * 1000, (page + 1) * 1000 - 1)
                        .execute()
                    )
                    rows = getattr(r, "data", None) or []
                    if not rows:
                        break
                    for row in rows:
                        if _normalize_county(row.get("county")) not in FOCUS_COUNTIES:
                            continue
                        pm = row.get("phone_metadata") or {}
                        # Skip already-ROD-verified — don't overwrite better data
                        if isinstance(pm, dict) and pm.get("rod_lookup"):
                            continue
                        # Allow re-match on existing hmda_match if it
                        # was wide-window (low confidence). Skip if
                        # already year-anchored or fully sale-anchored.
                        if isinstance(pm, dict):
                            sig = pm.get("mortgage_signal") or {}
                            if isinstance(sig, dict) and sig.get("source") == "hmda_match":
                                if sig.get("sale_anchored") or sig.get("year_anchored"):
                                    continue
                        row["__table__"] = table
                        out.append(row)
                    if len(rows) < 1000:
                        break
                    page += 1
                except Exception as e:
                    self.logger.warning(
                        f"candidate query on {table}: {e}"
                    )
                    break
        return out[:max_per_run]

    # ── target year picker ───────────────────────────────────────────────
    @staticmethod
    def _pick_target_year(lead: Dict[str, Any]) -> Optional[int]:
        raw = lead.get("raw_payload") or {}
        if not isinstance(raw, dict):
            return None
        s = raw.get("structured") or {}
        if isinstance(s, dict):
            td = s.get("trust_date")
            if td:
                # Format: m/d/yyyy
                m = re.search(r"/(\d{4})\s*$", str(td))
                if m:
                    return int(m.group(1))
        # Try assessor sale_date
        for key in ("padctn", "padctn_owner_match", "assessor_owner_match"):
            obj = raw.get(key)
            if isinstance(obj, dict):
                for sd_key in ("last_sale_date", "most_recent_sale_date", "sale_date"):
                    sd = obj.get(sd_key)
                    if sd:
                        m = re.search(r"(\d{4})", str(sd))
                        if m:
                            return int(m.group(1))
        return None

    @staticmethod
    def _year_search_range(target_year: Optional[int], window: int) -> List[int]:
        if target_year is None:
            return list(range(HMDA_LATEST_YEAR, HMDA_LATEST_YEAR - window, -1))
        # Clamp to HMDA published range (2018-2024 as of 2026-Q2)
        target = max(HMDA_EARLIEST_YEAR, min(HMDA_LATEST_YEAR, target_year))
        lo = max(HMDA_EARLIEST_YEAR, target - 1)
        hi = min(HMDA_LATEST_YEAR, target + 1)
        return list(range(hi, lo - 1, -1))

    # ── Census geocoder ──────────────────────────────────────────────────
    def _geocode(self, address: str) -> Optional[str]:
        if address in self._tract_cache:
            return self._tract_cache[address]
        try:
            r = self._session.get(
                CENSUS_GEOCODER,
                params={
                    "address": address,
                    "benchmark": "Public_AR_Current",
                    "vintage": "Current_Current",
                    "format": "json",
                },
                timeout=15,
            )
            data = r.json()
            matches = data.get("result", {}).get("addressMatches", [])
            if not matches:
                self._tract_cache[address] = None
                return None
            geos = matches[0].get("geographies", {})
            tracts = geos.get("Census Tracts", [])
            if not tracts:
                self._tract_cache[address] = None
                return None
            geoid = tracts[0].get("GEOID")
            self._tract_cache[address] = geoid
            return geoid
        except Exception as e:
            self.logger.warning(f"geocode failed for {address!r}: {e}")
            self._tract_cache[address] = None
            return None

    # ── HMDA fetch + cache ────────────────────────────────────────────────
    def _hmda(self, county_fips: str, year: int) -> List[Dict[str, str]]:
        cache_key = (county_fips, year)
        if cache_key in self._hmda_cache:
            return self._hmda_cache[cache_key]
        # Disk cache
        cache_path = CACHE_DIR / f"hmda_{county_fips}_{year}.csv"
        if cache_path.exists():
            try:
                with open(cache_path, "r", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
                self._hmda_cache[cache_key] = rows
                return rows
            except Exception as e:
                self.logger.warning(f"cache read fail {cache_path}: {e}")
        # Live fetch
        try:
            self.logger.info(f"  HMDA download: {county_fips} {year}")
            r = self._session.get(
                HMDA_CSV,
                params={
                    "years": str(year),
                    "counties": county_fips,
                    "actions_taken": "1",
                },
                timeout=REQUEST_TIMEOUT,
            )
            if r.status_code != 200:
                self.logger.warning(
                    f"HMDA fetch {county_fips}/{year}: HTTP {r.status_code}"
                )
                self._hmda_cache[cache_key] = []
                return []
            text = r.text
            # Save to disk + parse
            try:
                with open(cache_path, "w", encoding="utf-8") as f:
                    f.write(text)
            except Exception as e:
                self.logger.warning(f"cache write fail {cache_path}: {e}")
            rows = list(csv.DictReader(io.StringIO(text)))
            self._hmda_cache[cache_key] = rows
            time.sleep(self.throttle_seconds)
            return rows
        except Exception as e:
            self.logger.warning(f"HMDA fetch error {county_fips}/{year}: {e}")
            self._hmda_cache[cache_key] = []
            return []

    # ── GLEIF (LEI → lender name) ────────────────────────────────────────
    def _gleif(self, lei: str) -> Optional[str]:
        if not lei:
            return None
        if lei in self._gleif_cache:
            return self._gleif_cache[lei]
        try:
            r = self._session.get(GLEIF_LEI + lei, timeout=10)
            if r.status_code != 200:
                self._gleif_cache[lei] = None
                return None
            attrs = r.json().get("data", {}).get("attributes", {})
            name = attrs.get("entity", {}).get("legalName", {}).get("name")
            self._gleif_cache[lei] = name
            return name
        except Exception:
            self._gleif_cache[lei] = None
            return None

    # ── DB write ──────────────────────────────────────────────────────────
    def _write(self, client, lead: Dict[str, Any], match: Dict[str, Any]) -> None:
        table = lead["__table__"]
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}

        # Confidence ladder:
        #   0.85 fully sale-anchored single-candidate (tract+year+amount tight)
        #   0.75 fully sale-anchored multi-candidate (closest-to-LTV pick)
        #   0.70 year-anchored single-candidate (1-yr window, no price anchor)
        #   0.60 year-anchored multi-candidate
        #   0.65 wide-window single-candidate
        #   0.45 wide-window multi-candidate
        if match.get("sale_anchored"):
            confidence = 0.85 if match["candidate_count"] == 1 else 0.75
        elif match.get("year_anchored"):
            confidence = 0.70 if match["candidate_count"] == 1 else 0.60
        else:
            confidence = 0.65 if match["candidate_count"] == 1 else 0.45
        pm["mortgage_signal"] = {
            "kind": "hmda_origination",
            "source": "hmda_match",
            "amount": match["principal"],
            "confidence": confidence,
            "lender": match["lender_name"],
            "lei": match["lei"],
            "match_year": match["match_year"],
            "interest_rate": match["interest_rate"],
            "loan_purpose": match["loan_purpose"],
            "loan_term": match["loan_term"],
            "census_tract": match["tract"],
            "anchor_property_value": match["anchor_value"],
            "candidate_count": match["candidate_count"],
            "sale_anchored": match.get("sale_anchored", False),
            "year_anchored": match.get("year_anchored", False),
            "sale_date": match.get("sale_date"),
            "sale_price": match.get("sale_price"),
            "deed_reference": match.get("deed_reference"),
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }
        update: Dict[str, Any] = {"phone_metadata": pm}
        # Promote to mortgage_balance if confidence >= 0.75 (sale-anchored)
        existing = lead.get("mortgage_balance")
        if confidence >= 0.75 and not existing:
            update["mortgage_balance"] = int(match["principal"])

        try:
            client.table(table).update(update).eq("id", lead["id"]).execute()
        except Exception as e:
            self.logger.warning(f"  update failed id={lead['id']}: {e}")
            return

        # Provenance for live table only
        if (
            update.get("mortgage_balance")
            and table == "homeowner_requests"
        ):
            try:
                record_field(
                    client, lead["id"], "mortgage_balance",
                    int(match["principal"]),
                    "hmda_match",
                    confidence=confidence,
                    metadata={
                        "lei": match["lei"],
                        "lender": match["lender_name"],
                        "match_year": match["match_year"],
                        "candidate_count": match["candidate_count"],
                        "tract": match["tract"],
                    },
                )
            except Exception as e:
                self.logger.warning(f"  provenance fail id={lead['id']}: {e}")

    # ── status helpers ────────────────────────────────────────────────────
    def _fail(self, started, msg: str) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        self._report_health(
            status="failed", started_at=started, finished_at=finished,
            fetched_count=0, parsed_count=0, staged_count=0, duplicate_count=0,
            error_message=msg,
        )
        return {"name": self.name, "status": "failed", "error": msg,
                "matched": 0, "staged": 0, "duplicates": 0, "fetched": 0}

    def _wrap(self, started, attempted, single, multi,
                no_tract, no_anchor, no_match, errors,
                status: str = "ok", error: Optional[str] = None) -> Dict[str, Any]:
        finished = datetime.now(timezone.utc)
        matched = single + multi
        self._report_health(
            status=status, started_at=started, finished_at=finished,
            fetched_count=attempted, parsed_count=matched,
            staged_count=matched, duplicate_count=0,
            error_message=error,
        )
        return {
            "name": self.name, "status": status,
            "attempted": attempted,
            "single_match": single, "multi_match": multi,
            "no_tract": no_tract, "no_anchor": no_anchor,
            "no_match": no_match, "errors": errors,
            "error": error,
            "staged": matched, "duplicates": 0, "fetched": attempted,
        }


def run() -> dict:
    bot = HmdaEnricherBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
