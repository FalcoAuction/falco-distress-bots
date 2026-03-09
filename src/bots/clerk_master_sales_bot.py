from __future__ import annotations

import json
import os
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from ..gating.convertibility import apply_convertibility_gate
from ..scoring.days_to_sale import days_to_sale
from ..settings import get_dts_window, is_allowed_county, normalize_county_full, within_target_counties
from ..storage import sqlite_store as _store
from ..utils import make_lead_key
from .record_seed_utils import (
    default_seed_path,
    extract_address_candidates,
    extract_date_candidates,
    iter_normalized_rows,
    load_seed_rows,
    parse_date_flex,
)


_DTS_MIN, _DTS_MAX = get_dts_window("CLERK_MASTER_SALE")
_HEADERS = {"User-Agent": "Mozilla/5.0 (Falco Distress Bot)"}
_LIVE_SOURCES = [
    ("Montgomery County", "https://montgomerytn.gov/chancery/upcoming-clerk-and-master-sales"),
]


def _iter_event_blobs(html: str) -> list[dict[str, object]]:
    events: list[dict[str, object]] = []
    seen: set[str] = set()
    for raw_line in (html or "").splitlines():
        line = raw_line.strip()
        if not line.startswith("{") or '"subject"' not in line:
            continue
        try:
            blob = json.loads(line)
        except Exception:
            continue
        event_id = str(blob.get("id") or "")
        if event_id and event_id in seen:
            continue
        if event_id:
            seen.add(event_id)
        events.append(blob)
    return events


def _event_text(blob: dict[str, object]) -> str:
    parts: list[str] = []
    for key in ("subject", "bodyPreview", "dateDisplay"):
        value = blob.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    body = blob.get("body")
    if isinstance(body, dict):
        body_content = body.get("content")
        if isinstance(body_content, str) and body_content.strip():
            parts.append(body_content.strip())
    location = blob.get("location")
    if isinstance(location, dict):
        display = location.get("displayName")
        if isinstance(display, str) and display.strip():
            parts.append(display.strip())
    return "\n".join(parts)


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
        for blob in _iter_event_blobs(html):
            event_text = _event_text(blob)
            text = "\n".join(part for part in (event_text, page_text) if part)
            start = blob.get("start")
            sale_date = None
            if isinstance(start, dict):
                sale_date = parse_date_flex(str(start.get("dateTime") or ""))
            if not sale_date:
                dates = extract_date_candidates(text)
                sale_date = dates[0] if dates else None
            if not sale_date:
                continue

            addresses = extract_address_candidates(text)
            for address in addresses:
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
                        "notes": str(blob.get("subject") or "clerk and master sale"),
                    }
                )

        for link in soup.find_all("a", href=True):
            href = urljoin(url, link["href"])
            text = " ".join(link.get_text(" ", strip=True).split())
            if not text:
                continue
            lowered = text.lower()
            if "sale" not in lowered and "commissioner" not in lowered and "special master" not in lowered:
                continue
            sale_dates = extract_date_candidates(text)
            if not sale_dates:
                continue
            addresses = extract_address_candidates(text)
            for address in addresses:
                sale_date = sale_dates[0]
                key = (county.lower(), sale_date, address.lower())
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "address": address,
                        "county": county,
                        "sale_date": sale_date,
                        "source_url": href,
                        "notes": text or "clerk and master sale link",
                    }
                )

    return rows


def run():
    seed_file = os.environ.get("FALCO_CLERK_MASTER_SALE_SEED_FILE") or default_seed_path("clerk_master_sales.csv")
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
        county = normalize_county_full(row["county"])
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
            "distress_type": "COURT_SALE",
            "source": "ClerkMasterSale",
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

        lead_key = make_lead_key("CLERK_MASTER_SALE", county, sale_date, address)
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
            "channel": "CLERK_MASTER_SALE",
        }

        if _store.upsert_lead(lead_key, {"address": address, "state": "TN"}, county, distress_type="COURT_SALE"):
            stored_leads += 1
        if _store.insert_ingest_event(
            lead_key,
            "CLERK_MASTER_SALE",
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
    print(f"[ClerkMasterSalesBot] summary {summary}")
    return summary
