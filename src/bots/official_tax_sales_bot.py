from __future__ import annotations

import json
import os

from ..gating.convertibility import apply_convertibility_gate
from ..scoring.days_to_sale import days_to_sale
from ..settings import get_dts_window, is_allowed_county, within_target_counties
from ..storage import sqlite_store as _store
from ..utils import make_lead_key
from .record_seed_utils import iter_normalized_rows, load_seed_rows


_DTS_MIN, _DTS_MAX = get_dts_window("OFFICIAL_TAX_SALE")


def run():
    seed_file = os.environ.get("FALCO_TAX_SALE_SEED_FILE")
    if not seed_file:
        print("[OfficialTaxSalesBot] No seed file configured - skipping.")
        return {}
    if not os.path.isfile(seed_file):
        print("[OfficialTaxSalesBot] Seed file not found.")
        return {}

    rows = load_seed_rows(seed_file)
    seed_rows = len(rows)
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
        "seed_rows": seed_rows,
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
