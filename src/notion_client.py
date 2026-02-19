# src/notion_client.py

import os
import time
import random
from typing import Any, Dict, Optional, Tuple

import requests


NOTION_TOKEN = os.environ.get("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.environ.get("NOTION_DATABASE_ID", "").strip()

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.environ.get("NOTION_VERSION", "2022-06-28")

# Network hardening (fixes ReadTimeout in Actions)
REQUEST_TIMEOUT_SECS = int(os.environ.get("NOTION_TIMEOUT_SECS", "90"))
MAX_RETRIES = int(os.environ.get("NOTION_MAX_RETRIES", "7"))

_session = requests.Session()


def _headers() -> Dict[str, str]:
    if not NOTION_TOKEN:
        raise RuntimeError("Missing NOTION_TOKEN env var.")
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _sleep_backoff(attempt: int) -> None:
    base = 0.7 * (2 ** max(0, attempt - 1))  # 0.7, 1.4, 2.8, 5.6...
    jitter = random.uniform(0.0, 0.5)
    time.sleep(min(15.0, base + jitter))


def _request(method: str, path: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = f"{NOTION_API_BASE}{path}"
    last_err: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = _session.request(
                method=method,
                url=url,
                headers=_headers(),
                json=json,
                timeout=REQUEST_TIMEOUT_SECS,
            )

            if resp.status_code == 429:
                retry_after = resp.headers.get("retry-after")
                if retry_after:
                    try:
                        time.sleep(float(retry_after))
                    except Exception:
                        _sleep_backoff(attempt)
                else:
                    _sleep_backoff(attempt)
                continue

            if 500 <= resp.status_code <= 599:
                _sleep_backoff(attempt)
                continue

            if resp.status_code >= 400:
                raise RuntimeError(f"Notion API {resp.status_code}: {resp.text}")

            return resp.json()

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout, requests.exceptions.ConnectionError) as e:
            last_err = e
            _sleep_backoff(attempt)
            continue
        except Exception as e:
            last_err = e
            break

    raise RuntimeError(f"Notion request failed after {MAX_RETRIES} attempts: {method} {path} :: {last_err}")


# ----------------------------
# Property helpers
# ----------------------------

def _title_prop(text: str) -> Dict[str, Any]:
    return {"title": [{"text": {"content": (text or "").strip()}}]}


def _rich_text_prop(text: Optional[str]) -> Dict[str, Any]:
    if text is None:
        return {"rich_text": []}
    s = str(text)
    if not s.strip():
        return {"rich_text": []}
    if len(s) > 1900:
        s = s[:1900] + "…"
    return {"rich_text": [{"text": {"content": s}}]}


def _select_prop(name: Optional[str]) -> Dict[str, Any]:
    if not name:
        return {"select": None}
    return {"select": {"name": str(name)}}


def _number_prop(n: Optional[Any]) -> Dict[str, Any]:
    if n is None:
        return {"number": None}
    try:
        return {"number": float(n)}
    except Exception:
        return {"number": None}


def _date_prop(iso_date: Optional[str]) -> Dict[str, Any]:
    if not iso_date:
        return {"date": None}
    return {"date": {"start": iso_date}}


def _url_prop(url: Optional[str]) -> Dict[str, Any]:
    if not url:
        return {"url": None}
    return {"url": str(url)}


# ----------------------------
# build_properties (FIXES Days-to-Sale publishing)
# ----------------------------

def build_properties(*args, **kwargs) -> Dict[str, Any]:
    """
    Flexible builder. Supports both:
      - positional required fields: (sale_date_iso, trustee_attorney, score, contact_info, ...)
      - dict/kwargs payloads from bots

    Canonical keys this function understands:
      title, source, county, distress_type, address,
      sale_date_iso, trustee_attorney, contact_info,
      status, score, raw_snippet, url, lead_key, days_to_sale
    """
    data: Dict[str, Any] = {}

    if len(args) == 1 and isinstance(args[0], dict) and not kwargs:
        data.update(args[0])
    else:
        data.update(kwargs)
        if len(args) >= 1 and "sale_date_iso" not in data:
            data["sale_date_iso"] = args[0]
        if len(args) >= 2 and "trustee_attorney" not in data:
            data["trustee_attorney"] = args[1]
        if len(args) >= 3 and "score" not in data:
            data["score"] = args[2]
        if len(args) >= 4 and "contact_info" not in data:
            data["contact_info"] = args[3]

    # Aliases (from other bots / earlier versions)
    if "sale_date_iso" not in data and "sale_date" in data:
        data["sale_date_iso"] = data.get("sale_date")
    if "trustee_attorney" not in data and "trustee" in data:
        data["trustee_attorney"] = data.get("trustee")
    if "score" not in data and "falco_score" in data:
        data["score"] = data.get("falco_score")
    if "days_to_sale" not in data and "dts" in data:
        data["days_to_sale"] = data.get("dts")

    title = data.get("title") or data.get("property_name") or data.get("name") or data.get("address") or "Unknown"
    contact_info = data.get("contact_info")
    if contact_info is None:
        contact_info = data.get("trustee_attorney") or ""

    props: Dict[str, Any] = {
        "Property Name": _title_prop(str(title)),
        "Source": _select_prop(data.get("source")),
        "County": _select_prop(data.get("county")),
        "Distress Type": _select_prop(data.get("distress_type")),
        "Address": _rich_text_prop(data.get("address")),
        "Sale Date": _date_prop(data.get("sale_date_iso")),
        "Trustee/Attorney": _rich_text_prop(data.get("trustee_attorney")),
        "Contact Info": _rich_text_prop(contact_info),
        "Status": _select_prop(data.get("status")),
        "Falco Score": _number_prop(data.get("score")),
        "Raw Snippet": _rich_text_prop(data.get("raw_snippet")),
        "URL": _url_prop(data.get("url")),
        "Lead Key": _rich_text_prop(data.get("lead_key")),
        # ✅ THIS is what fixes Days-to-Sale not publishing:
        "Days to Sale": _number_prop(data.get("days_to_sale")),
    }

    return props


# ----------------------------
# CRUD / UPSERT
# ----------------------------

def find_existing_by_lead_key(lead_key: str) -> Optional[str]:
    if not NOTION_DATABASE_ID:
        raise RuntimeError("Missing NOTION_DATABASE_ID env var.")
    if not lead_key:
        return None

    body = {
        "filter": {"property": "Lead Key", "rich_text": {"equals": lead_key}},
        "page_size": 1,
    }
    res = _request("POST", f"/databases/{NOTION_DATABASE_ID}/query", json=body)
    results = res.get("results") or []
    if not results:
        return None
    return results[0].get("id")


def create_lead(properties: Dict[str, Any]) -> str:
    if not NOTION_DATABASE_ID:
        raise RuntimeError("Missing NOTION_DATABASE_ID env var.")
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    res = _request("POST", "/pages", json=body)
    return res.get("id")


def update_lead(page_id: str, properties: Dict[str, Any]) -> str:
    body = {"properties": properties}
    res = _request("PATCH", f"/pages/{page_id}", json=body)
    return res.get("id")


def upsert_lead(lead_key: str, properties: Dict[str, Any]) -> Tuple[str, str]:
    existing_id = find_existing_by_lead_key(lead_key)
    if existing_id:
        update_lead(existing_id, properties)
        return existing_id, "updated"
    new_id = create_lead(properties)
    return new_id, "created"
