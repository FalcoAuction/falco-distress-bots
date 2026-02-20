import os
import json as _json
import time
import requests
from typing import Any, Dict, Optional, Tuple, List

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

_WARNED_MISSING_CREDS = False

# Stage 2-3 schema safety
_DB_SCHEMA_CACHE: Optional[Dict[str, Any]] = None
_DB_SCHEMA_FETCHED_AT: float = 0.0


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


def _request(method: str, path: str, json: Optional[dict] = None, *, timeout: int = 30) -> dict:
    if not _have_creds():
        _warn_missing_creds_once()
        return {"results": []}  # safe default for query; create/update callers will no-op
    url = NOTION_API_BASE + path
    r = requests.request(method, url, headers=_headers(), json=json, timeout=timeout)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion API error {r.status_code}: {r.text}")
    return r.json()


# ----------------------------
# PROPERTY BUILDERS (Stage 1)
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


def _checkbox_prop(val: Any) -> Dict[str, Any]:
    # For checkbox, False is still meaningful and should not be pruned.
    return {"checkbox": bool(val)}


def build_properties(*args, **kwargs) -> Dict[str, Any]:
    """
    Stage 1 build_properties used by existing bots.
    MUST remain backwards-compatible.
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
    if "checkbox" in prop:
        # checkbox is always meaningful (False is valid)
        return False
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
# DB SCHEMA HELPERS (Stage 2-3 safety)
# ----------------------------

def get_database_schema(force_refresh: bool = False) -> Optional[Dict[str, Any]]:
    """
    Returns database schema JSON or None. Cached.
    """
    global _DB_SCHEMA_CACHE, _DB_SCHEMA_FETCHED_AT
    if not _have_creds():
        _warn_missing_creds_once()
        return None

    ttl_seconds = int(os.getenv("FALCO_NOTION_SCHEMA_TTL_SECONDS", "3600"))
    now = time.time()
    if (not force_refresh) and _DB_SCHEMA_CACHE is not None and (now - _DB_SCHEMA_FETCHED_AT) < ttl_seconds:
        return _DB_SCHEMA_CACHE

    try:
        res = _request("GET", f"/databases/{NOTION_DATABASE_ID}", json=None, timeout=30)
        _DB_SCHEMA_CACHE = res
        _DB_SCHEMA_FETCHED_AT = now
        return _DB_SCHEMA_CACHE
    except Exception as e:
        print(f"[NotionClient] WARNING: failed to fetch database schema ({type(e).__name__}: {e}). Proceeding without schema filter.")
        _DB_SCHEMA_CACHE = None
        _DB_SCHEMA_FETCHED_AT = now
        return None


def database_property_names() -> Optional[set]:
    schema = get_database_schema()
    if not schema:
        return None
    props = schema.get("properties") or {}
    if not isinstance(props, dict):
        return None
    return set(props.keys())


def filter_properties_to_database(properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes properties not present in DB. If schema not available, returns as-is.
    """
    if not properties:
        return {}
    names = database_property_names()
    if not names:
        return properties
    return {k: v for (k, v) in properties.items() if k in names}


# ----------------------------
# Stage 2-3 PROPERTY BUILDER
# ----------------------------

def build_extra_properties(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2-3 properties. Returns Notion property objects.
    IMPORTANT: filter_properties_to_database() is applied in update/create.
    """
    props: Dict[str, Any] = {}

    def rt(name: str, val: Any):
        props[name] = _rich_text_prop(None if val is None else str(val))

    def num(name: str, val: Any):
        props[name] = _number_prop(val)

    def datep(name: str, val: Any):
        props[name] = _date_prop(None if not val else str(val))

    def urlp(name: str, val: Any):
        props[name] = _url_prop(None if not val else str(val))

    def chk(name: str, val: Any):
        props[name] = _checkbox_prop(val)

    # Enrichment
    if "owner_name" in data:
        rt("Owner Name", data.get("owner_name"))
    if "mailing_address" in data:
        rt("Mailing Address", data.get("mailing_address"))
    if "absentee_flag" in data:
        chk("Absentee Flag", data.get("absentee_flag"))
    if "beds" in data:
        num("Beds", data.get("beds"))
    if "baths" in data:
        num("Baths", data.get("baths"))
    if "sqft" in data:
        num("Sqft", data.get("sqft"))
    if "year_built" in data:
        num("Year Built", data.get("year_built"))
    if "estimated_value_low" in data:
        num("Estimated Value Low", data.get("estimated_value_low"))
    if "estimated_value_high" in data:
        num("Estimated Value High", data.get("estimated_value_high"))
    if "loan_indicators" in data:
        rt("Loan Indicators", data.get("loan_indicators"))
    if "last_sale_date" in data:
        datep("Last Sale Date", data.get("last_sale_date"))
    if "tax_assessed_value" in data:
        num("Tax Assessed Value", data.get("tax_assessed_value"))
    if "enrichment_confidence" in data:
        num("Enrichment Confidence", data.get("enrichment_confidence"))
    if "enrichment_json" in data:
        rt("Enrichment JSON", data.get("enrichment_json"))

    # Comps
    if "comps_json" in data:
        rt("Comps JSON", data.get("comps_json"))
    if "comps_summary" in data:
        rt("Comps Summary", data.get("comps_summary"))
    if "value_band_low" in data:
        num("Value Band Low", data.get("value_band_low"))
    if "value_band_high" in data:
        num("Value Band High", data.get("value_band_high"))
    if "liquidity_score" in data:
        num("Liquidity Score", data.get("liquidity_score"))

    # Grading
    if "grade_score" in data:
        num("Grade Score", data.get("grade_score"))
    if "grade" in data:
        rt("Grade", data.get("grade"))
    if "grade_reasons" in data:
        rt("Grade Reasons", data.get("grade_reasons"))
    if "status_flag" in data:
        rt("Status Flag", data.get("status_flag"))
    if "time_score" in data:
        num("Time Score", data.get("time_score"))
    if "equity_score" in data:
        num("Equity Score", data.get("equity_score"))
    if "complexity_penalty" in data:
        num("Complexity Penalty", data.get("complexity_penalty"))

    # Packet
    if "packet_pdf_url" in data:
        urlp("Packet PDF URL", data.get("packet_pdf_url"))
    if "packet_built_at" in data:
        datep("Packet Built At", data.get("packet_built_at"))

    return props


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
    properties = filter_properties_to_database(properties)
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    res = _request("POST", "/pages", json=body)
    return res.get("id", "")


def update_lead(page_id: str, properties: Dict[str, Any]) -> str:
    if not _have_creds():
        _warn_missing_creds_once()
        return page_id
    safe_props = prune_empty_properties_for_update(properties)
    safe_props = filter_properties_to_database(safe_props)
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


# ----------------------------
# QUERY HELPERS (Stage 2-3)
# ----------------------------

def query_database(
    filter_obj: Optional[dict] = None,
    *,
    page_size: int = 50,
    sorts: Optional[list] = None,
    max_pages: int = 10,
) -> List[dict]:
    """
    Database query with pagination. Returns list of page objects.
    """
    if not _have_creds():
        _warn_missing_creds_once()
        return []

    url = f"/databases/{NOTION_DATABASE_ID}/query"
    out: List[dict] = []
    start_cursor: Optional[str] = None

    for _ in range(max_pages):
        body: Dict[str, Any] = {"page_size": page_size}
        if filter_obj:
            body["filter"] = filter_obj
        if sorts:
            body["sorts"] = sorts
        if start_cursor:
            body["start_cursor"] = start_cursor

        res = _request("POST", url, json=body)
        out.extend(res.get("results", []) or [])
        if not res.get("has_more"):
            break
        start_cursor = res.get("next_cursor")

    return out


def _rt_plain(prop: dict) -> str:
    try:
        parts = prop.get("rich_text") or []
        return "".join(((p.get("plain_text") or "") for p in parts)).strip()
    except Exception:
        return ""


def _title_plain(prop: dict) -> str:
    try:
        parts = prop.get("title") or []
        return "".join(((p.get("plain_text") or "") for p in parts)).strip()
    except Exception:
        return ""


def _select_plain(prop: dict) -> str:
    try:
        sel = prop.get("select")
        return (sel or {}).get("name", "") if sel else ""
    except Exception:
        return ""


def _date_plain(prop: dict) -> str:
    try:
        d = prop.get("date")
        return (d or {}).get("start", "") if d else ""
    except Exception:
        return ""


def _number_plain(prop: dict) -> Optional[float]:
    try:
        return prop.get("number")
    except Exception:
        return None


def _url_plain(prop: dict) -> str:
    try:
        return prop.get("url") or ""
    except Exception:
        return ""


def _checkbox_plain(prop: dict) -> bool:
    try:
        return bool(prop.get("checkbox"))
    except Exception:
        return False


def extract_page_fields(page: dict) -> Dict[str, Any]:
    """
    Extracts a stable subset of fields using the repo's known property names,
    plus optional Stage 2-3 fields if they exist.
    """
    props = (page or {}).get("properties") or {}

    def gp(name: str) -> dict:
        return props.get(name) or {}

    out: Dict[str, Any] = {
        "page_id": (page or {}).get("id", ""),
        "property_name": _title_plain(gp("Property Name")),
        "source": _select_plain(gp("Source")),
        "county": _select_plain(gp("County")),
        "distress_type": _select_plain(gp("Distress Type")),
        "address": _rt_plain(gp("Address")),
        "sale_date": _date_plain(gp("Sale Date")),
        "trustee_attorney": _rt_plain(gp("Trustee/Attorney")),
        "contact_info": _rt_plain(gp("Contact Info")),
        "status": _select_plain(gp("Status")),
        "falco_score": _number_plain(gp("Falco Score")),
        "raw_snippet": _rt_plain(gp("Raw Snippet")),
        "url": _url_plain(gp("URL")),
        "lead_key": _rt_plain(gp("Lead Key")),
        "days_to_sale": _number_plain(gp("Days to Sale")),
        # Optional Stage fields (may not exist)
        "owner_name": _rt_plain(gp("Owner Name")),
        "mailing_address": _rt_plain(gp("Mailing Address")),
        "absentee_flag": _checkbox_plain(gp("Absentee Flag")),
        "beds": _number_plain(gp("Beds")),
        "baths": _number_plain(gp("Baths")),
        "sqft": _number_plain(gp("Sqft")),
        "year_built": _number_plain(gp("Year Built")),
        "estimated_value_low": _number_plain(gp("Estimated Value Low")),
        "estimated_value_high": _number_plain(gp("Estimated Value High")),
        "loan_indicators": _rt_plain(gp("Loan Indicators")),
        "last_sale_date": _date_plain(gp("Last Sale Date")),
        "tax_assessed_value": _number_plain(gp("Tax Assessed Value")),
        "enrichment_confidence": _number_plain(gp("Enrichment Confidence")),
        "enrichment_json": _rt_plain(gp("Enrichment JSON")),
        "comps_json": _rt_plain(gp("Comps JSON")),
        "comps_summary": _rt_plain(gp("Comps Summary")),
        "value_band_low": _number_plain(gp("Value Band Low")),
        "value_band_high": _number_plain(gp("Value Band High")),
        "liquidity_score": _number_plain(gp("Liquidity Score")),
        "grade_score": _number_plain(gp("Grade Score")),
        "grade": _rt_plain(gp("Grade")),
        "grade_reasons": _rt_plain(gp("Grade Reasons")),
        "status_flag": _rt_plain(gp("Status Flag")),
        "packet_pdf_url": _url_plain(gp("Packet PDF URL")),
        "packet_built_at": _date_plain(gp("Packet Built At")),
    }
    return out
