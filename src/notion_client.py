import os
import requests
from typing import Any, Dict, Optional, Tuple

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

_WARNED_MISSING_CREDS = False


def _have_creds() -> bool:
    return bool(NOTION_API_KEY and NOTION_DATABASE_ID)


def _warn_missing_creds_once():
    global _WARNED_MISSING_CREDS
    if _WARNED_MISSING_CREDS:
        return
    _WARNED_MISSING_CREDS = True
    print("[NotionClient] WARNING: Missing NOTION_API_KEY or NOTION_DATABASE_ID. Notion writes are disabled for this run.")


def _headers() -> Dict[str, str]:
    # DO NOT raise — keep system runnable even without Notion env vars
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, json: Optional[dict] = None) -> dict:
    if not _have_creds():
        _warn_missing_creds_once()
        return {"results": []}  # safe default for query; create/update callers will no-op
    url = NOTION_API_BASE + path
    r = requests.request(method, url, headers=_headers(), json=json, timeout=30)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion API error {r.status_code}: {r.text}")
    return r.json()


# ----------------------------
# PROPERTY BUILDERS
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


def _number_prop(n: Any) -> Dict[str, Any]:
    try:
        if n is None:
            return {"number": None}
        return {"number": float(n)}
    except Exception:
        return {"number": None}


def _date_prop(iso: Optional[str]) -> Dict[str, Any]:
    if not iso:
        return {"date": None}
    return {"date": {"start": str(iso)}}


def _url_prop(url: Optional[str]) -> Dict[str, Any]:
    if not url:
        return {"url": None}
    return {"url": str(url)}


def build_properties(*args, **kwargs) -> Dict[str, Any]:
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
        "Days to Sale": _number_prop(data.get("days_to_sale")),
    }
    return props


def _is_empty_prop(prop: Dict[str, Any]) -> bool:
    if prop is None:
        return True
    if "rich_text" in prop:
        return not prop.get("rich_text")
    if "title" in prop:
        t = prop.get("title") or []
        if not t:
            return True
        try:
            content = (t[0].get("text") or {}).get("content", "")
        except Exception:
            content = ""
        return not str(content).strip()
    if "select" in prop:
        return prop.get("select") is None
    if "date" in prop:
        return prop.get("date") is None
    if "url" in prop:
        return prop.get("url") in (None, "")
    if "number" in prop:
        return prop.get("number") is None
    return False


def prune_empty_properties_for_update(properties: Dict[str, Any]) -> Dict[str, Any]:
    if not properties:
        return {}
    out: Dict[str, Any] = {}
    for k, v in properties.items():
        if _is_empty_prop(v):
            continue
        if k == "Property Name":
            try:
                content = ((v.get("title") or [])[0].get("text") or {}).get("content", "")
            except Exception:
                content = ""
            if not str(content).strip() or str(content).strip().lower() in {"unknown", "foreclosure notice"}:
                continue
        out[k] = v
    return out


# ----------------------------
# CRUD / UPSERT
# ----------------------------

def find_existing_by_lead_key(lead_key: str) -> Optional[str]:
    if not _have_creds():
        _warn_missing_creds_once()
        return None
    if not lead_key:
        return None

    body = {"filter": {"property": "Lead Key", "rich_text": {"contains": lead_key}}}
    res = _request("POST", f"/databases/{NOTION_DATABASE_ID}/query", json=body)
    results = res.get("results", [])
    if not results:
        return None
    return results[0].get("id")


def create_lead(properties: Dict[str, Any]) -> str:
    if not _have_creds():
        _warn_missing_creds_once()
        return ""
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    res = _request("POST", "/pages", json=body)
    return res.get("id", "")


def update_lead(page_id: str, properties: Dict[str, Any]) -> str:
    if not _have_creds():
        _warn_missing_creds_once()
        return page_id
    safe_props = prune_empty_properties_for_update(properties)
    if not safe_props:
        return page_id
    body = {"properties": safe_props}
    res = _request("PATCH", f"/pages/{page_id}", json=body)
    return res.get("id", page_id)


def upsert_lead(lead_key: str, properties: Dict[str, Any]) -> Tuple[str, str]:
    if not _have_creds():
        _warn_missing_creds_once()
        return "", "disabled"
    existing_id = find_existing_by_lead_key(lead_key)
    if existing_id:
        update_lead(existing_id, properties)
        return existing_id, "updated"
    new_id = create_lead(properties)
    return new_id, "created"
