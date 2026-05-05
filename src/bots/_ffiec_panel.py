"""FFIEC HMDA Institution Panel — maps respondent_id → bank name.

Pre-2018 HMDA data uses respondent_id (e.g. '0002736291') instead of
the LEI introduced with HMDA modernization in 2018. The CFPB historic
institution panel files map respondent_id → bank name + parent.

Free download from files.consumerfinance.gov/hmda-historic-institution-data/.

Build a cache once per year, then look up by (activity_year, respondent_id).
"""
from __future__ import annotations

import csv
import io
import os
import zipfile
from pathlib import Path
from typing import Dict, Optional

import requests


PANEL_URL_TEMPLATE = (
    "https://files.consumerfinance.gov/hmda-historic-institution-data/"
    "hmda_{year}_panel.zip"
)

CACHE_DIR = Path(os.environ.get("FALCO_HMDA_CACHE_DIR", "data/hmda_cache"))


# (year, respondent_id) -> bank name. Built lazily.
_PANEL_CACHE: Dict[tuple, str] = {}
_LOADED_YEARS: set = set()


def _load_year(year: int) -> None:
    """Download + parse one year's panel into _PANEL_CACHE."""
    if year in _LOADED_YEARS:
        return
    cache_path = CACHE_DIR / f"hmda_panel_{year}.csv"
    rows: list = []

    if cache_path.exists():
        try:
            with open(cache_path, "r", encoding="utf-8") as f:
                rows = list(csv.DictReader(f))
        except Exception:
            rows = []

    if not rows:
        try:
            r = requests.get(
                PANEL_URL_TEMPLATE.format(year=year),
                timeout=60,
                headers={"User-Agent": "curl/8.0"},
            )
            if r.status_code != 200:
                _LOADED_YEARS.add(year)
                return
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            inner = next((n for n in zf.namelist() if n.endswith(".csv")), None)
            if not inner:
                _LOADED_YEARS.add(year)
                return
            with zf.open(inner) as f:
                text = io.TextIOWrapper(f, encoding="latin-1")
                rows = list(csv.DictReader(text))
            # Save to disk cache (slim form: just the columns we need)
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with open(cache_path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(
                    f, fieldnames=["respondent_id", "name", "city", "state"]
                )
                w.writeheader()
                for row in rows:
                    w.writerow({
                        "respondent_id": row.get("Respondent ID", "").strip(),
                        "name": row.get("Respondent Name (Panel)", "").strip(),
                        "city": row.get("Respondent City (Panel)", "").strip(),
                        "state": row.get("Respondent State (Panel)", "").strip(),
                    })
        except Exception:
            _LOADED_YEARS.add(year)
            return

    for row in rows:
        rid = (row.get("Respondent ID") or row.get("respondent_id") or "").strip()
        name = (row.get("Respondent Name (Panel)") or row.get("name") or "").strip()
        if rid and name:
            # Normalize: pad to 10 digits, also store unpadded
            _PANEL_CACHE[(year, rid)] = name
            _PANEL_CACHE[(year, rid.lstrip("0"))] = name
            _PANEL_CACHE[(year, rid.zfill(10))] = name

    _LOADED_YEARS.add(year)


def lookup(activity_year: int, respondent_id: str) -> Optional[str]:
    """Return bank name for a given (year, respondent_id), or None."""
    if not respondent_id:
        return None
    rid = str(respondent_id).strip()
    if rid in ("", "nan", "None"):
        return None
    _load_year(activity_year)
    # Try several format variations
    for key in (
        (activity_year, rid),
        (activity_year, rid.lstrip("0")),
        (activity_year, rid.zfill(10)),
        # Some LARs have format "AGENCY-RID" — strip the prefix
        (activity_year, rid.split("-", 1)[-1] if "-" in rid else rid),
    ):
        if key in _PANEL_CACHE:
            return _PANEL_CACHE[key]
    return None
