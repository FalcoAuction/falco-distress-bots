from __future__ import annotations

import json
import os
from datetime import date
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
_SRI_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Falco Distress Bot)",
}
_SRI_API_URL = "https://sriservicesusermgmtprod.azurewebsites.net/api"
_SRI_PUBLIC_API_KEY = "9f8fd9fe5160294175e1c737567030f495d838a7922a678bc06e0a093910"
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


def _sri_headers() -> dict[str, str]:
    headers = dict(_SRI_HEADERS)
    headers["x-api-key"] = os.environ.get("FALCO_SRI_API_KEY") or _SRI_PUBLIC_API_KEY
    return headers


def _map_sale_type(code: str, description: str) -> str:
    normalized = (code or "").strip().upper()
    desc = (description or "").lower()
    if normalized == "F" or "foreclosure" in desc:
        return "FORECLOSURE"
    if normalized in {"A", "C", "J", "O"} or "tax" in desc or "certificate" in desc or "redemption" in desc:
        return "TAX_SALE"
    return "COURT_SALE"


def _fetch_sri_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    headers = _sri_headers()
    allowed_counties = sorted(
        {
            normalize_county_full(county)
            for county in (
                "Davidson County",
                "Williamson County",
                "Rutherford County",
                "Sumner County",
                "Wilson County",
                "Maury County",
                "Montgomery County",
            )
            if is_allowed_county(normalize_county_full(county))
        }
    )

    for county in allowed_counties:
        payload = {
            "searchText": "",
            "state": "TN",
            "county": county,
            "propertySaleType": "",
            "auctionStyle": "",
            "saleStatus": "",
            "auctionDateRange": {
                "startDate": date.today().isoformat(),
                "endDate": "",
                "compareOperator": ">",
            },
            "recordCount": 25,
            "startIndex": 0,
        }
        try:
            auctions = requests.post(
                f"{_SRI_API_URL}/auction/listall",
                headers=headers,
                json=payload,
                timeout=30,
            ).json()
        except Exception:
            continue
        if not isinstance(auctions, list):
            continue

        for auction in auctions:
            sale_id = auction.get("saleId")
            if not sale_id:
                continue
            try:
                detail = requests.post(
                    f"{_SRI_API_URL}/property/carddetail",
                    headers=headers,
                    json={"saleId": sale_id, "state": "TN", "county": county},
                    timeout=45,
                ).json()
            except Exception:
                continue
            properties = detail.get("properties") if isinstance(detail, dict) else None
            if not isinstance(properties, list):
                continue

            for prop in properties:
                address_parts = [
                    str(prop.get("address1") or "").strip(),
                    str(prop.get("city") or "").strip(),
                    "TN",
                    str(prop.get("zip") or "").strip(),
                ]
                address = ", ".join(part for part in address_parts if part)
                sale_date = parse_date_flex(str(prop.get("date") or prop.get("auctionDate") or ""))
                if not address or not sale_date:
                    continue
                key = (county.lower(), sale_date, address.lower())
                if key in seen:
                    continue
                seen.add(key)
                sale_type_code = str(prop.get("saleType") or "")
                sale_type_description = str(prop.get("saleTypeDescription") or "")
                rows.append(
                    {
                        "address": address,
                        "county": county,
                        "sale_date": sale_date,
                        "source_url": "https://www.sriservices.com/properties",
                        "notes": sale_type_description or "clerk and master sale",
                        "distress_type": _map_sale_type(sale_type_code, sale_type_description),
                        "sale_type": sale_type_description,
                        "sale_location": str(prop.get("location") or ""),
                        "sale_time": str(prop.get("time") or ""),
                        "owner_name": " ".join(
                            part for part in [str(prop.get("ownerName1") or "").strip(), str(prop.get("ownerName2") or "").strip()] if part
                        ),
                        "property_id": str(prop.get("propertyId") or prop.get("altPropertyId") or "").strip(),
                    }
                )

    return rows


def _fetch_html_rows() -> list[dict[str, str]]:
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


def _fetch_live_rows() -> list[dict[str, str]]:
    sri_rows = _fetch_sri_rows()
    html_rows = _fetch_html_rows()
    merged: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for row in [*sri_rows, *html_rows]:
        key = (row["county"].lower(), row["sale_date"], row["address"].lower())
        if key in seen:
            continue
        seen.add(key)
        merged.append(row)
    return merged


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
            "distress_type": row.get("distress_type") or "COURT_SALE",
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
            "sale_type": row.get("sale_type"),
            "sale_location": row.get("sale_location"),
            "sale_time": row.get("sale_time"),
            "owner_name": row.get("owner_name"),
            "property_id": row.get("property_id"),
        }

        lead_attrs = {
            "address": address,
            "state": "TN",
        }
        if row.get("owner_name"):
            lead_attrs["owner_name"] = row["owner_name"]
        if row.get("property_id"):
            lead_attrs["property_identifier"] = row["property_id"]
        if _store.upsert_lead(lead_key, lead_attrs, county, distress_type=row.get("distress_type") or "COURT_SALE"):
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
