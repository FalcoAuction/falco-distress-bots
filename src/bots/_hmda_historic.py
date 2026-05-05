"""Download + filter pre-2018 HMDA archives from CFPB historic data.

CFPB Data Browser API only exposes 2018-current. Older years (2007-2017)
are in nationwide ZIP archives at:
  files.consumerfinance.gov/hmda-historic-loan-data/

Each is ~150-350MB compressed, single CSV inside, "labels" version
(text values not numeric codes).

This module downloads each year, filters to TN (state_code=47) + our
target counties, and saves slim CSVs that match the schema expected
by hmda_enricher_bot's _hmda() cache.

Run via:
  python -m src.bots._hmda_historic --year 2017
  python -m src.bots._hmda_historic --year 2010 --year 2011 ...
  python -m src.bots._hmda_historic --all  (downloads 2010-2017)
"""
from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import zipfile
from pathlib import Path
from typing import Dict, List, Optional

import requests


CFPB_HISTORIC_BASE = (
    "https://files.consumerfinance.gov/hmda-historic-loan-data/"
)

# TN county FIPS — same set as hmda_enricher_bot.TN_COUNTY_FIPS
TN_FOCUS_FIPS = {
    "47037": "davidson",
    "47187": "williamson",
    "47165": "sumner",
    "47149": "rutherford",
    "47189": "wilson",
    "47119": "maury",
    "47125": "montgomery",
}

CACHE_DIR = Path(os.environ.get("FALCO_HMDA_CACHE_DIR", "data/hmda_cache"))


def archive_url(year: int) -> str:
    return (
        CFPB_HISTORIC_BASE
        + f"hmda_{year}_nationwide_first-lien-owner-occupied-1-4-family-records_labels.zip"
    )


def _column_map(header: List[str]) -> Dict[str, str]:
    """Map historic-LAR columns to Data Browser column names so the
    same hmda_enricher_bot match logic works."""
    rename = {
        # historic_name : data_browser_name
        "as_of_year":              "activity_year",
        "respondent_id":           "respondent_id",       # no LEI pre-2018
        "agency_code":             "agency_code",
        "loan_type":               "loan_type",
        "property_type":           "property_type",
        "loan_purpose":            "loan_purpose",
        "owner_occupancy":         "occupancy_type",
        "loan_amount_000s":        "loan_amount_000s",    # in $thousands
        "preapproval":             "preapproval",
        "action_taken":            "action_taken",
        "msa_md":                  "derived_msa-md",
        "state_code":              "state_code",
        "county_code":             "county_code",
        "census_tract_number":     "census_tract",
        "applicant_ethnicity":     "applicant_ethnicity-1",
        "applicant_race_1":        "applicant_race-1",
        "applicant_sex":           "applicant_sex",
        "applicant_income_000s":   "income",
        "purchaser_type":          "purchaser_type",
        "denial_reason_1":         "denial_reason-1",
        "rate_spread":             "rate_spread",
        "hoepa_status":            "hoepa_status",
        "lien_status":             "lien_status",
        "edit_status":             "edit_status",
        "sequence_number":         "sequence_number",
        "population":              "tract_population",
        "minority_population":     "tract_minority_population_percent",
        "hud_median_family_income": "ffiec_msa_md_median_family_income",
        "tract_to_msa_md_income_pct": "tract_to_msa_income_percentage",
        "number_of_owner_occupied_units": "tract_owner_occupied_units",
        "number_of_1_to_4_family_units": "tract_one_to_four_family_homes",
    }
    return {old: rename.get(old, old) for old in header}


def _normalize_loan_purpose(historic_value: str) -> str:
    """Historic uses 1=Home purchase, 2=Home improvement, 3=Refinance.
    Data Browser uses 1=Home purchase, 2=Home improvement, 31=Refinance,
    32=Cash-out refi, 4=Other purpose. Map historic → browser."""
    if not historic_value:
        return ""
    v = historic_value.strip()
    return {"3": "31"}.get(v, v)


def _normalize_action_taken(historic_value: str) -> str:
    """Historic and browser both use 1=Loan originated. Match."""
    return historic_value.strip()


def _normalize_lien_status(historic_value: str) -> str:
    """Historic 1=First lien, 2=Subordinate. Browser same. The
    historic-archive zip is filtered to first-lien-owner-occupied
    already, so all rows should be 1."""
    return historic_value.strip()


def _normalize_loan_amount(historic_value: str) -> str:
    """Historic stores loan amount in thousands. Convert to dollars."""
    if not historic_value:
        return ""
    try:
        return str(int(float(historic_value) * 1000))
    except (ValueError, TypeError):
        return ""


def _normalize_state_code(historic_value: str) -> str:
    """Historic stores 2-digit FIPS. Browser uses 2-letter state code.
    For TN 47=TN. Keep as 47 since our match logic uses county_code only."""
    return historic_value.strip()


def _normalize_county_code(state: str, county: str) -> str:
    """Historic stores 3-digit county. Combine with state to make 5-digit
    Data Browser format (e.g. 47 + 037 = 47037)."""
    if not state or not county:
        return ""
    s = state.strip().zfill(2)
    c = county.strip().zfill(3)
    return s + c


def _normalize_census_tract(state: str, county: str, tract_raw: str) -> str:
    """Historic uses xxxx.xx format (e.g. '105.20'). Browser uses
    11-digit FIPS (e.g. '47037010520'). Build it."""
    if not tract_raw:
        return ""
    sc = _normalize_county_code(state, county)
    if not sc:
        return ""
    # Strip decimal and zero-pad to 6 digits
    digits = tract_raw.replace(".", "").strip()
    return sc + digits.zfill(6)


def filter_archive_to_focus_counties(
    year: int, target_dir: Path = CACHE_DIR,
) -> Dict[str, int]:
    """Download year ZIP, stream-filter to TN focus counties, write
    one CSV per (county_fips, year) into the cache dir.
    Returns row counts per county."""
    target_dir.mkdir(parents=True, exist_ok=True)
    counts = {fips: 0 for fips in TN_FOCUS_FIPS}

    url = archive_url(year)
    print(f"  downloading {year} archive ({url})", file=sys.stderr)
    r = requests.get(url, timeout=600, headers={"User-Agent": "curl/8.0"},
                      stream=True)
    if r.status_code != 200:
        print(f"  HTTP {r.status_code} on {url}", file=sys.stderr)
        return counts

    # Stream into a temp file then unzip (safer than holding in memory)
    tmp_zip = target_dir / f"_dl_{year}.zip"
    with open(tmp_zip, "wb") as f:
        for chunk in r.iter_content(chunk_size=2_000_000):
            if chunk:
                f.write(chunk)
    print(f"  downloaded {tmp_zip.stat().st_size//1024//1024}MB", file=sys.stderr)

    # Open ZIP, find inner CSV, stream-filter
    file_handles = {}  # fips -> open csv writer
    csv_writers = {}

    try:
        with zipfile.ZipFile(tmp_zip, "r") as zf:
            inner_name = None
            for name in zf.namelist():
                if name.lower().endswith(".csv"):
                    inner_name = name
                    break
            if not inner_name:
                print(f"  no CSV in archive", file=sys.stderr)
                return counts
            with zf.open(inner_name) as csvf:
                reader = csv.DictReader(io.TextIOWrapper(csvf, encoding="latin-1"))
                # Define output columns in Data Browser format
                output_cols = [
                    "activity_year", "lei", "state_code", "county_code",
                    "census_tract", "loan_type", "loan_purpose",
                    "lien_status", "action_taken", "loan_amount",
                    "interest_rate", "loan_term", "occupancy_type",
                    "derived_dwelling_category", "property_value",
                ]
                for row in reader:
                    # Skip non-TN
                    state = (row.get("state_code") or "").strip()
                    if state != "47":
                        continue
                    county = (row.get("county_code") or "").strip()
                    fips = _normalize_county_code(state, county)
                    if fips not in TN_FOCUS_FIPS:
                        continue
                    # Open writer for this county on first row
                    if fips not in csv_writers:
                        out_path = target_dir / f"hmda_{fips}_{year}.csv"
                        fh = open(out_path, "w", newline="", encoding="utf-8")
                        wr = csv.DictWriter(fh, fieldnames=output_cols)
                        wr.writeheader()
                        file_handles[fips] = fh
                        csv_writers[fips] = wr
                    # Build output row in Data Browser shape
                    out = {
                        "activity_year": str(year),
                        "lei": row.get("respondent_id") or "",  # placeholder
                        "state_code": "TN",
                        "county_code": fips,
                        "census_tract": _normalize_census_tract(
                            state, county, row.get("census_tract_number") or ""
                        ),
                        "loan_type": row.get("loan_type") or "",
                        "loan_purpose": _normalize_loan_purpose(
                            row.get("loan_purpose") or ""
                        ),
                        "lien_status": row.get("lien_status") or "1",
                        "action_taken": row.get("action_taken") or "",
                        "loan_amount": _normalize_loan_amount(
                            row.get("loan_amount_000s") or ""
                        ),
                        "interest_rate": row.get("rate_spread") or "",
                        "loan_term": "",
                        "occupancy_type": row.get("owner_occupancy") or "1",
                        "derived_dwelling_category": "Single Family (1-4 Units):Site-Built",
                        "property_value": "",
                    }
                    csv_writers[fips].writerow(out)
                    counts[fips] += 1
    finally:
        for fh in file_handles.values():
            fh.close()
        try:
            tmp_zip.unlink()
        except Exception:
            pass

    return counts


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, action="append", default=[])
    ap.add_argument("--all", action="store_true",
                     help="Download 2010-2017 (8 years)")
    args = ap.parse_args()

    if args.all:
        years = list(range(2010, 2018))
    else:
        years = args.year

    if not years:
        print("no years specified — use --year YYYY or --all")
        return

    for year in years:
        print(f"\n=== {year} ===")
        counts = filter_archive_to_focus_counties(year)
        for fips, n in counts.items():
            county = TN_FOCUS_FIPS[fips]
            print(f"  {fips} ({county}): {n} rows")


if __name__ == "__main__":
    main()
