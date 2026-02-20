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


# ---------------------------------------------------------
# Core Helpers
# ---------------------------------------------------------

def _safe_get_rich_text(prop: Dict[str, Any]) -> str:
    """
    Safely extracts plain text from a rich_text Notion property.
    Returns empty string if missing.
    """
    try:
        if not prop:
            return ""
        rich = prop.get("rich_text", [])
        if not rich:
            return ""
        return "".join([t.get("plain_text", "") for t in rich])
    except Exception:
        return ""


def _safe_get_number(prop: Dict[str, Any]) -> Optional[float]:
    try:
        if not prop:
            return None
        return prop.get("number")
    except Exception:
        return None


def _safe_get_select(prop: Dict[str, Any]) -> Optional[str]:
    try:
        if not prop:
            return None
        sel = prop.get("select")
        if not sel:
            return None
        return sel.get("name")
    except Exception:
        return None


# ---------------------------------------------------------
# READ
# ---------------------------------------------------------

def extract_page_fields(page: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extracts normalized fields from a Notion page.

    CRITICAL FIX:
    We now explicitly map "Enrichment JSON" from rich_text into
    fields["enrichment_json"].

    This is what Stage 2 expects.
    """

    props = page.get("properties", {})

    fields = {}

    # --- Core Stage 1 fields ---
    fields["lead_key"] = _safe_get_rich_text(props.get("Lead Key"))
    fields["address"] = _safe_get_rich_text(props.get("Address"))
    fields["county"] = _safe_get_rich_text(props.get("County"))
    fields["state"] = _safe_get_rich_text(props.get("State"))
    fields["event_date"] = _safe_get_rich_text(props.get("Event Date"))

    # --- Stage 2 critical field ---
    enrichment_raw = _safe_get_rich_text(props.get("Enrichment JSON"))
    fields["enrichment_json"] = enrichment_raw

    # Optional parsed version
    if enrichment_raw:
        try:
            fields["enrichment_json_parsed"] = json.loads(enrichment_raw)
        except Exception:
            fields["enrichment_json_parsed"] = None
    else:
        fields["enrichment_json_parsed"] = None

    # --- Optional numeric values (if present in DB) ---
    fields["estimated_value"] = _safe_get_number(props.get("Estimated Value"))
    fields["value_band_low"] = _safe_get_number(props.get("Value Band Low"))
    fields["value_band_high"] = _safe_get_number(props.get("Value Band High"))

    return fields


# ---------------------------------------------------------
# WRITE
# ---------------------------------------------------------

def build_extra_properties(extra_fields: Dict[str, Any]) -> Dict[str, Any]:
    """
    Converts internal field dict into Notion property payload.

    NON-DESTRUCTIVE:
    Only includes fields explicitly provided.
    """

    properties = {}

    # Enrichment JSON
    if "enrichment_json" in extra_fields and extra_fields["enrichment_json"]:
        properties["Enrichment JSON"] = {
            "rich_text": [
                {
                    "type": "text",
                    "text": {"content": extra_fields["enrichment_json"]},
                }
            ]
        }

    # Estimated Value
    if "estimated_value" in extra_fields and extra_fields["estimated_value"] is not None:
        properties["Estimated Value"] = {
            "number": extra_fields["estimated_value"]
        }

    # Value Bands
    if "value_band_low" in extra_fields and extra_fields["value_band_low"] is not None:
        properties["Value Band Low"] = {
            "number": extra_fields["value_band_low"]
        }

    if "value_band_high" in extra_fields and extra_fields["value_band_high"] is not None:
        properties["Value Band High"] = {
            "number": extra_fields["value_band_high"]
        }

    return properties


def update_page(page_id: str, properties: Dict[str, Any]) -> None:
    """
    Non-destructive update.
    """

    url = f"{BASE_URL}/pages/{page_id}"

    payload = {
        "properties": properties
    }

    r = requests.patch(url, headers=HEADERS, json=payload)

    if r.status_code >= 300:
        print("[NOTION] update error:", r.status_code, r.text)


# ---------------------------------------------------------
# QUERY
# ---------------------------------------------------------

def query_database(filter_payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BASE_URL}/databases/{NOTION_DATABASE_ID}/query"

    r = requests.post(url, headers=HEADERS, json=filter_payload)

    if r.status_code >= 300:
        print("[NOTION] query error:", r.status_code, r.text)
        return {}

    return r.json()
