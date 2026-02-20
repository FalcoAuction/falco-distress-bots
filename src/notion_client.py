import os
import json
import requests
from typing import Dict, Any, Optional

NOTION_API_KEY = os.getenv("NOTION_API_KEY")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")

NOTION_VERSION = "2022-06-28"

BASE_URL = "https://api.notion.com/v1"

HEADERS = {
    "Authorization": f"Bearer {NOTION_API_KEY}",
    "Notion-Version": NOTION_VERSION,
    "Content-Type": "application/json",
}


# =========================================================
# LOW-LEVEL SAFE HELPERS
# =========================================================

def _safe_get_rich_text(prop: Dict[str, Any]) -> str:
    try:
        if not prop:
            return ""
        rich = prop.get("rich_text", [])
        if not rich:
            return ""
        return "".join(t.get("plain_text", "") for t in rich)
    except Exception:
        return ""


def _safe_get_number(prop: Dict[str, Any]) -> Optional[float]:
    try:
        if not prop:
            return None
        return prop.get("number")
    except Exception:
        return None


# =========================================================
# READ
# =========================================================

def extract_page_fields(page: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts normalized fields from a Notion page.
    Stage 1 + Stage 2 compatible.
    """

    props = page.get("properties", {})
    fields: Dict[str, Any] = {}

    # ---- Stage 1 fields ----
    fields["lead_key"] = _safe_get_rich_text(props.get("Lead Key"))
    fields["address"] = _safe_get_rich_text(props.get("Address"))
    fields["county"] = _safe_get_rich_text(props.get("County"))
    fields["state"] = _safe_get_rich_text(props.get("State"))
    fields["event_date"] = _safe_get_rich_text(props.get("Event Date"))

    # ---- Stage 2 enrichment ----
    enrichment_raw = _safe_get_rich_text(props.get("Enrichment JSON"))
    fields["enrichment_json"] = enrichment_raw

    if enrichment_raw:
        try:
            fields["enrichment_json_parsed"] = json.loads(enrichment_raw)
        except Exception:
            fields["enrichment_json_parsed"] = None
    else:
        fields["enrichment_json_parsed"] = None

    fields["estimated_value"] = _safe_get_number(props.get("Estimated Value"))
    fields["value_band_low"] = _safe_get_number(props.get("Value Band Low"))
    fields["value_band_high"] = _safe_get_number(props.get("Value Band High"))

    return fields


# =========================================================
# WRITE — STAGE 1 COMPATIBILITY
# =========================================================

def build_properties(fields: Dict[str, Any]) -> Dict[str, Any]:
    props: Dict[str, Any] = {}

    if "lead_key" in fields:
        props["Lead Key"] = {
            "rich_text": [{"type": "text", "text": {"content": fields["lead_key"] or ""}}]
        }

    if "address" in fields:
        props["Address"] = {
            "rich_text": [{"type": "text", "text": {"content": fields["address"] or ""}}]
        }

    if "county" in fields:
        props["County"] = {
            "rich_text": [{"type": "text", "text": {"content": fields["county"] or ""}}]
        }

    if "state" in fields:
        props["State"] = {
            "rich_text": [{"type": "text", "text": {"content": fields["state"] or ""}}]
        }

    if "event_date" in fields:
        props["Event Date"] = {
            "rich_text": [{"type": "text", "text": {"content": fields["event_date"] or ""}}]
        }

    return props


def build_extra_properties(extra_fields: Dict[str, Any]) -> Dict[str, Any]:
    properties: Dict[str, Any] = {}

    if "enrichment_json" in extra_fields and extra_fields["enrichment_json"]:
        properties["Enrichment JSON"] = {
            "rich_text": [{"type": "text", "text": {"content": extra_fields["enrichment_json"]}}]
        }

    if "estimated_value" in extra_fields and extra_fields["estimated_value"] is not None:
        properties["Estimated Value"] = {"number": extra_fields["estimated_value"]}

    if "value_band_low" in extra_fields and extra_fields["value_band_low"] is not None:
        properties["Value Band Low"] = {"number": extra_fields["value_band_low"]}

    if "value_band_high" in extra_fields and extra_fields["value_band_high"] is not None:
        properties["Value Band High"] = {"number": extra_fields["value_band_high"]}

    return properties


# =========================================================
# CREATE / UPDATE
# =========================================================

def create_lead(properties: Dict[str, Any]) -> None:
    url = f"{BASE_URL}/pages"

    payload = {
        "parent": {"database_id": NOTION_DATABASE_ID},
        "properties": properties
    }

    r = requests.post(url, headers=HEADERS, json=payload)

    if r.status_code >= 300:
        print("[NOTION] create error:", r.status_code, r.text)


def update_lead(page_id: str, properties: Dict[str, Any]) -> None:
    url = f"{BASE_URL}/pages/{page_id}"

    payload = {"properties": properties}

    r = requests.patch(url, headers=HEADERS, json=payload)

    if r.status_code >= 300:
        print("[NOTION] update error:", r.status_code, r.text)


# =========================================================
# FIND EXISTING
# =========================================================

def find_existing_by_lead_key(lead_key: str) -> Optional[Dict[str, Any]]:
    payload = {
        "filter": {
            "property": "Lead Key",
            "rich_text": {"equals": lead_key}
        }
    }

    results = query_database(filter_payload=payload)
    pages = results.get("results", [])

    if pages:
        return pages[0]

    return None


# =========================================================
# QUERY — FULL STAGE 2 COMPATIBILITY
# =========================================================

def query_database(
    filter_payload: Optional[Dict[str, Any]] = None,
    page_size: int = 100,
    start_cursor: Optional[str] = None
) -> Dict[str, Any]:
    """
    Supports:
    - Stage 1 bots
    - Stage 2 enrichment
    - Pagination
    """

    url = f"{BASE_URL}/databases/{NOTION_DATABASE_ID}/query"

    payload: Dict[str, Any] = {
        "page_size": page_size
    }

    if filter_payload:
        payload.update(filter_payload)

    if start_cursor:
        payload["start_cursor"] = start_cursor

    r = requests.post(url, headers=HEADERS, json=payload)

    if r.status_code >= 300:
        print("[NOTION] query error:", r.status_code, r.text)
        return {}

    return r.json()
