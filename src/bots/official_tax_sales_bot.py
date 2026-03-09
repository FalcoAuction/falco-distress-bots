from __future__ import annotations

import json
import os
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..gating.convertibility import apply_convertibility_gate
from ..scoring.days_to_sale import days_to_sale
from ..settings import get_dts_window, is_allowed_county, within_target_counties
from ..storage import sqlite_store as _store
from ..utils import make_lead_key
from .record_seed_utils import (
    default_seed_path,
    extract_address_candidates,
    extract_date_candidates,
    iter_normalized_rows,
    load_seed_rows,
)


_DTS_MIN, _DTS_MAX = get_dts_window("OFFICIAL_TAX_SALE")
_HEADERS = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}
_LIVE_SOURCES = [
    ("Davidson County", "https://chanceryclerkandmaster.nashville.gov/fees/property-tax-schedule/"),
    ("Davidson County", "https://chanceryclerkandmaster.nashville.gov/fees/delinquent-tax-sales/"),
    ("Rutherford County", "https://rutherfordcountytn.gov/delinquent-taxes"),
]


def _fetch_live_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()

    for county, url in _LIVE_SOURCES:
        try:
            html = requests.get(url, headers=_HEADERS, timeout=30).text
        except Exception:
            continue

        soup = BeautifulSoup(html, "html.parser")
        page_text = soup.get_text("\n")
        date_candidates = extract_date_candidates(page_text)
        if not date_candidates:
            continue

        for address in extract_address_candidates(page_text):
            sale_date = date_candidates[0]
            key = (county.lower(), sale_date, address.lower())
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "address": address,
                    "county": county,
                    "sale_date": sale_date,
                    "source_url": url,
                    "notes": "official tax sale live scrape",
                }
            )

        for link in soup.find_all("a", href=True):
            href = urljoin(url, link["href"])
            text = " ".join(link.get_text(" ", strip=True).split())
            if "tax sale" not in text.lower() and "delinquent" not in text.lower():
                continue
            for sale_date in extract_date_candidates(text):
                key = (county.lower(), sale_date, href.lower())
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "address": "",
                        "county": county,
                        "sale_date": sale_date,
                        "source_url": href,
                        "notes": text or "official tax sale schedule link",
                    }
                )

    return rows


def run():
    seed_file = os.environ.get("FALCO_TAX_SALE_SEED_FILE") or default_seed_path("official_tax_sales.csv")
    seed_rows = load_seed_rows(seed_file) if os.path.isfile(seed_file) else []
    live_rows = _fetch_live_rows()
    rows = [*live_rows, *seed_rows]
    seed_count = len(seed_rows)
    live_count = len(live_rows)
    valid_rows = 0
    dts_skipped = 0
    geo_skipped = 0
    gate_skipped = 0
    dedupe_skipped = 0
    stored_leads = 0
    stored_ingests = 0
    seen_in_run: set[str] = set()

    for row in iter_normalized_rows(rows):
        address = row["address"]
        county = row["county"]
        sale_date = row["sale_date"]
        if not address or not county or not sale_date:
            continue

        valid_rows += 1
        if not is_allowed_county(county) or not within_target_counties(county):
            geo_skipped += 1
            continue

        dts = days_to_sale(sale_date)
        if dts is None or dts < _DTS_MIN or dts > _DTS_MAX:
            dts_skipped += 1
            continue

        payload = {
            "address": address,
            "county": county,
            "state": "TN",
            "distress_type": "TAX_SALE",
            "source": "OfficialTaxSale",
            "raw": row,
        }

        decision = apply_convertibility_gate(payload)
        if isinstance(decision, tuple):
            keep = bool(decision[0])
        elif isinstance(decision, dict) and "keep" in decision:
            keep = bool(decision["keep"])
        else:
            keep = bool(decision) if isinstance(decision, bool) else True
        if not keep:
            gate_skipped += 1
            continue

        lead_key = make_lead_key("OFFICIAL_TAX_SALE", county, sale_date, address)
        if lead_key in seen_in_run:
            dedupe_skipped += 1
            continue
        seen_in_run.add(lead_key)

        ingest_payload = {
            "address": address,
            "county": county,
            "sale_date": sale_date,
            "source_url": row["source_url"],
            "notes": row["notes"],
            "channel": "OFFICIAL_TAX_SALE",
        }

        if _store.upsert_lead(lead_key, {"address": address, "state": "TN"}, county, distress_type="TAX_SALE"):
            stored_leads += 1
        if _store.insert_ingest_event(
            lead_key,
            "OFFICIAL_TAX_SALE",
            row["source_url"],
            sale_date,
            json.dumps(ingest_payload),
        ):
            stored_ingests += 1

    summary = {
        "seed_rows": seed_count,
        "live_rows": live_count,
        "valid_rows": valid_rows,
        "dts_skipped": dts_skipped,
        "geo_skipped": geo_skipped,
        "gate_skipped": gate_skipped,
        "dedupe_skipped": dedupe_skipped,
        "stored_leads": stored_leads,
        "stored_ingests": stored_ingests,
    }
    print(f"[OfficialTaxSalesBot] summary {summary}")
    return summary
