import os
import requests
from typing import Dict, Any

NOTION_TOKEN = os.environ["NOTION_TOKEN"].strip()
DB_ID = os.environ["NOTION_DATABASE_ID"].strip()
NOTION_VERSION = "2022-06-28"

def headers():
    return {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }

def rich(text: str, limit: int = 1800):
    text = (text or "").strip()
    if len(text) > limit:
        text = text[:limit] + "…"
    return [{"type": "text", "text": {"content": text}}] if text else []

def create_lead(props: Dict[str, Any]):
    payload = {"parent": {"database_id": DB_ID}, "properties": props}
    r = requests.post(
        "https://api.notion.com/v1/pages",
        headers=headers(),
        json=payload,
        timeout=30
    )
    if r.status_code >= 300:
        raise RuntimeError(f"Notion create failed: {r.status_code} {r.text}")

def build_properties(
    title: str,
    source: str,
    distress_type: str,
    county: str,
    sale_date_iso: str,
    contact_info: str,
    url: str,
    score: int,
    status: str,
) -> Dict[str, Any]:
    props: Dict[str, Any] = {
        "Property Name": {"title": [{"type": "text", "text": {"content": title[:180]}}]},
        "Source": {"select": {"name": source}},
        "Distress Type": {"select": {"name": distress_type}},
        "Falco Score": {"number": int(score)},
        "Status": {"select": {"name": status}},
    }

    if county:
        props["County"] = {"select": {"name": county}}

    if sale_date_iso:
        props["Sale Date"] = {"date": {"start": sale_date_iso}}

    if contact_info:
        props["Contact Info"] = {"rich_text": rich(contact_info)}

    if url:
        props["URL"] = {"url": url}

    return props
