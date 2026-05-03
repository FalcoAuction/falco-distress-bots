"""
Hamilton County (Chattanooga) delinquent property tax scraper.

Hamilton County publishes the FULL delinquent property tax dataset as
a downloadable ZIP/CSV at hamiltontn.gov, refreshed weekly. The CSV
contains every delinquent tax record going back to 1999 — ~60k rows
representing ~10-15k unique properties (one row per delinquent year
per parcel).

Source: https://www.hamiltontn.gov/_downloadsTrusteeDelinquent/CTRUDELQCSV.zip
Refresh: weekly (last_modified header from the CDN)

This is a PURE statewide-grade dataset — no scraping fragility. Hamilton
is the 4th-largest TN county by population.

Filtering strategy:
  - Group by parcel (Map+Group+Parcel composite key)
  - Take latest record per parcel (most recent bill year)
  - Keep only parcels with Grand Totals > $500 (filter out trinket bills)
  - Keep only parcels with a real Property Address (skip institutional)
  - Keep only parcels with bill year >= current year - 5 (recent
    delinquencies are actionable; very old ones likely already foreclosed)
  - Cap at 2000 leads per run to avoid flooding the staging table

Distress type: TAX_LIEN
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections import defaultdict
from datetime import date
from typing import Dict, List, Optional

from ._base import BotBase, LeadPayload


HAMILTON_ZIP_URL = "https://www.hamiltontn.gov/_downloadsTrusteeDelinquent/CTRUDELQCSV.zip"

MIN_BILL_YEAR_LOOKBACK = 5      # only delinquencies from the last 5 years
MIN_GRAND_TOTAL_DOLLARS = 500   # skip parcels owing less than $500 total
MAX_LEADS = 2000                # safety cap per run

# Owner-name patterns that indicate a business/institution rather than a
# homeowner. We're targeting residential distress, so filter these out.
_BUSINESS_OWNER_PATTERNS = (
    " LLC", " LLP", " INC", " INC.", " CORP", " CORP.", " CO ", " CO.",
    " COMPANY", " PARTNERSHIP", " TRUST", " CHURCH", " MINISTRIES", " FOUNDATION",
    " ASSOCIATION", " ASSOC", " FUND", " HOLDINGS", " GROUP", " ENTERPRISES",
    " PROPERTIES", " INVESTMENTS", " REALTY", " DEVELOPMENT", " CITY OF",
    " HOSPITAL", " CHURCH", " MOSQUE", " TEMPLE", " SCHOOL", " UNIVERSITY",
    " HOMES", " AUTHORITY", " DISTRICT", " GOVERNMENT", " HEIRS OF", " ESTATE OF",
    " LIMITED", " LP ", " L P", " PLC", " S CORPORATION", " GENERAL PARTNERSHIP",
)

# Common street keywords used to validate the Property Address column —
# the CSV occasionally has a legal-description hint instead of a real address.
_ADDRESS_KEYWORDS = (
    "RD", "ROAD", "ST", "STREET", "AVE", "AVENUE", "DR", "DRIVE", "LN",
    "LANE", "BLVD", "BOULEVARD", "CT", "COURT", "CIR", "CIRCLE", "PL",
    "PLACE", "WAY", "HWY", "PKWY", "PARKWAY", "TRL", "TRAIL", "TER",
    "TERRACE", "PIKE", "ALY", "ALLEY",
)


class HamiltonTaxDelinquentBot(BotBase):
    name = "hamilton_tax_delinquent"
    description = "Hamilton County (Chattanooga) delinquent property tax CSV — weekly statewide-grade refresh"
    throttle_seconds = 0.5
    expected_min_yield = 100  # 60k rows / dedup → realistically 5k+ unique parcels

    def scrape(self) -> List[LeadPayload]:
        rows = self._fetch_csv_rows()
        if not rows:
            return []
        self.logger.info(f"loaded {len(rows)} delinquent-tax rows from Hamilton CSV")

        latest_by_parcel = self._latest_per_parcel(rows)
        self.logger.info(f"{len(latest_by_parcel)} unique parcels after dedup")

        cutoff_year = date.today().year - MIN_BILL_YEAR_LOOKBACK
        leads: List[LeadPayload] = []
        skipped_old = 0
        skipped_small = 0
        skipped_noaddr = 0

        for parcel_key, row in latest_by_parcel.items():
            year = self._safe_int(row.get("Bill Year"))
            if year is None or year < cutoff_year:
                skipped_old += 1
                continue

            grand_total = float(row.get("__cumulative_owed__") or 0.0)
            years_delinquent = int(row.get("__years_delinquent__") or 0)
            if grand_total < MIN_GRAND_TOTAL_DOLLARS:
                skipped_small += 1
                continue

            address = self._clean_address(row.get("Property Address") or "")
            if not address:
                skipped_noaddr += 1
                continue

            owner = self._compose_owner(row)
            if owner is None or self._looks_like_business(owner):
                skipped_small += 1  # bucket with small for telemetry
                continue
            mailing_address = self._compose_mailing(row)
            full_address = f"{address}, Chattanooga, TN"

            leads.append(LeadPayload(
                bot_source=self.name,
                pipeline_lead_key=self.make_lead_key(self.name, f"hamilton-{parcel_key}"),
                property_address=full_address,
                county="hamilton",
                full_name=owner,
                owner_name_records=owner,
                distress_type="TAX_LIEN",
                admin_notes=(
                    f"Hamilton Co tax delinquent · parcel {parcel_key} · "
                    f"${grand_total:,.2f} cumulative owed · {years_delinquent} delinquent years · "
                    f"last bill year {year}"
                ),
                source_url=HAMILTON_ZIP_URL,
                raw_payload={
                    "county": "hamilton",
                    "parcel": parcel_key,
                    "cumulative_owed": grand_total,
                    "years_delinquent": years_delinquent,
                    "last_bill_year": year,
                    "owner_name_1": row.get("Owner Name 1"),
                    "owner_name_2": row.get("Owner Name 2"),
                    "mailing_address": mailing_address,
                    "land_use": (row.get("Land Use") or "").strip() or None,
                    "property_type": (row.get("Property Type") or "").strip() or None,
                    "back_tax_indicator": (row.get("Back Tax Indicator") or "").strip() or None,
                    "tax_relief_indicator": (row.get("Tax Relief Indicator") or "").strip() or None,
                },
            ))

            if len(leads) >= MAX_LEADS:
                self.logger.info(f"hit max_leads cap of {MAX_LEADS}; truncating")
                break

        self.logger.info(
            f"yielded={len(leads)} skipped_old={skipped_old} "
            f"skipped_small={skipped_small} skipped_noaddr={skipped_noaddr}"
        )
        return leads

    # ── CSV download + parse ────────────────────────────────────────────────

    def _fetch_csv_rows(self) -> List[Dict[str, str]]:
        res = self.fetch(HAMILTON_ZIP_URL)
        if res is None or res.status_code != 200:
            self.logger.warning(f"Hamilton ZIP fetch failed: {res.status_code if res else 'none'}")
            return []

        try:
            zf = zipfile.ZipFile(io.BytesIO(res.content))
        except zipfile.BadZipFile as e:
            self.logger.error(f"corrupt ZIP: {e}")
            return []

        # Single CSV inside the archive
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            self.logger.error("no CSV inside Hamilton ZIP")
            return []

        with zf.open(names[0]) as fh:
            text = io.TextIOWrapper(fh, encoding="latin-1", newline="")
            reader = csv.DictReader(text)
            return list(reader)

    @staticmethod
    def _latest_per_parcel(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
        """Group by Map+Group+Parcel; keep the row with the highest Bill Year.

        The "Grand Totals" CSV column is always zero in this dataset. The
        actual outstanding amount is the sum of Current County Owed +
        Current Mun Owed + Current Stw Owed + E and R Pickup Owed for each
        row, then summed across years to get cumulative outstanding for
        the parcel.
        """
        latest: Dict[str, Dict[str, str]] = {}
        latest_year: Dict[str, int] = defaultdict(int)
        totals: Dict[str, float] = defaultdict(float)
        years_count: Dict[str, int] = defaultdict(int)

        for row in rows:
            mp = (row.get("Map") or "").strip()
            gp = (row.get("Group") or "").strip()
            pc = (row.get("Parcel") or "").strip()
            if not (mp and pc):
                continue
            key = f"{mp}-{gp}-{pc}"
            year = HamiltonTaxDelinquentBot._safe_int(row.get("Bill Year")) or 0

            row_total = (
                HamiltonTaxDelinquentBot._parse_total(row.get("Current County Owed"))
                + HamiltonTaxDelinquentBot._parse_total(row.get("Current Mun Owed"))
                + HamiltonTaxDelinquentBot._parse_total(row.get("Current Stw Owed"))
                + HamiltonTaxDelinquentBot._parse_total(row.get("E and R Pickup Owed"))
            )
            totals[key] += row_total
            years_count[key] += 1

            if year > latest_year[key]:
                latest_year[key] = year
                latest[key] = row

        # Stash cumulative outstanding + years count back into the latest row
        # so the caller can read them via the same row dict.
        for key, total in totals.items():
            if key in latest:
                latest[key] = dict(latest[key])  # copy before mutating
                latest[key]["__cumulative_owed__"] = f"{total:.2f}"
                latest[key]["__years_delinquent__"] = str(years_count[key])
        return latest

    # ── Cleanup / formatting ────────────────────────────────────────────────

    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            return None

    @staticmethod
    def _parse_total(value: Optional[str]) -> float:
        """Grand Totals appears to be cent-encoded with leading zeros, e.g.
        '000046906' → $469.06. Strip and divide by 100."""
        if not value:
            return 0.0
        s = value.strip().lstrip("0")
        if not s:
            return 0.0
        try:
            return int(s) / 100.0
        except ValueError:
            return 0.0

    @staticmethod
    def _clean_address(address: str) -> Optional[str]:
        """Validate + normalize the Property Address column."""
        s = " ".join(address.split())  # collapse whitespace
        if not s or len(s) < 6:
            return None
        upper = s.upper()
        # Must contain a street keyword (filters out empty / legal-description-only rows)
        if not any(f" {kw}" in f" {upper} " or f" {kw} " in f" {upper} " for kw in _ADDRESS_KEYWORDS):
            return None
        # Must start with a digit (street number)
        if not s[0].isdigit():
            return None
        return s.title()

    @staticmethod
    def _looks_like_business(owner: str) -> bool:
        upper = f" {owner.upper()} "
        return any(pat in upper for pat in _BUSINESS_OWNER_PATTERNS)

    @staticmethod
    def _compose_owner(row: Dict[str, str]) -> Optional[str]:
        n1 = (row.get("Owner Name 1") or "").strip()
        n2 = (row.get("Owner Name 2") or "").strip()
        if n1 and n2:
            return f"{n1} & {n2}"
        return n1 or None

    @staticmethod
    def _compose_mailing(row: Dict[str, str]) -> Optional[str]:
        parts = [
            (row.get("Mail Addr 1") or "").strip(),
            (row.get("Mail Addr 2") or "").strip(),
            (row.get("Mail Addr 3 ") or "").strip(),
        ]
        joined = ", ".join(p for p in parts if p)
        return joined or None


def run() -> dict:
    bot = HamiltonTaxDelinquentBot()
    return bot.run()


if __name__ == "__main__":
    print(run())
