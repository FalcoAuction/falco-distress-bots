# src/notion_client.py

import os
import requests
from typing import Dict, Any, Optional

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
DB_ID = os.environ["NOTION_DATABASE_ID"]


def headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def rich(text: str, limit=2000):
    return [{
        "type": "text",
        "text": {"content": (text or "")[:limit]}
    }]


def build_properties(
    title: str,
    source: str,
    distress_type: str,
    county: str,
    address: str,
    sale_date_iso: str,
    trustee_attorney: str,
    contact_info: str,
    raw_snippet: str,
    url: str,
    score: int,
    status: str,
    lead_key: str,
    days_to_sale_num: Optional[int] = None,
    priority: Optional[str] = None,
) -> Dict[str, Any]:

    props = {
        "Property Name": {"title": rich(title, 200)},
        "Source": {"select": {"name": source}},
        "County": {"select": {"name": county}} if county else None,
        "Distress Type": {"select": {"name": distress_type}},
        "Address": {"rich_text": rich(address, 200)},
        "Sale Date": {"date": {"start": sale_date_iso}} if sale_date_iso else None,
        "Trustee/Attorney": {"rich_text": rich(trustee_attorney, 200)},
        "Contact Info": {"rich_text": rich(contact_info, 200)},
        "Status": {"select": {"name": status}},
        "Falco Score": {"number": score},
        "Raw Snippet": {"rich_text": rich(raw_snippet)},
        "URL": {"url": url},
        "Lead Key": {"rich_text": rich(lead_key, 80)},

        # ✅ New (optional)
        "Days to Sale": {"number": days_to_sale_num} if days_to_sale_num is not None else None,
        "Priority": {"select": {"name": priority}} if priority else None,
    }

    return {k: v for k, v in props.items() if v is not None}


def create_lead(properties: Dict[str, Any]):
    payload = {"parent": {"database_id": DB_ID}, "properties": properties}
    r = requests.post("https://api.notion.com/v1/pages", headers=headers(), json=payload, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion create failed: {r.status_code} {r.text}")


def update_lead(page_id: str, properties: Dict[str, Any]):
    r = requests.patch(
        f"https://api.notion.com/v1/pages/{page_id}",
        headers=headers(),
        json={"properties": properties},
        timeout=30
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Notion update failed: {r.status_code} {r.text}")


def find_existing_by_lead_key(lead_key: str):
    if not lead_key:
        return None

    payload = {
        "filter": {
            "property": "Lead Key",
            "rich_text": {"equals": lead_key}
        }
    }

    r = requests.post(
        f"https://api.notion.com/v1/databases/{DB_ID}/query",
        headers=headers(),
        json=payload,
        timeout=30
    )

    if r.status_code >= 300:
        return None

    results = r.json().get("results", [])
    return results[0]["id"] if results else None
