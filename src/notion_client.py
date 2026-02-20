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

    props: Dict[str, Any] = {
        "Property Name": _title_prop(data.get("property_name", "") or ""),
        "Source": _select_prop(data.get("source")),
        "County": _select_prop(data.get("county")),
        "Distress Type": _select_prop(data.get("distress_type")),
        "Address": _rich_text_prop(data.get("address")),
        "Sale Date": _date_prop(data.get("sale_date_iso")),
        "Trustee/Attorney": _rich_text_prop(data.get("trustee_attorney")),
        "Contact Info": _rich_text_prop(data.get("contact_info")),
        "Status": _select_prop(data.get("status")),
        "Falco Score": _number_prop(data.get("score")),
        "Raw Snippet": _rich_text_prop(data.get("raw_snippet")),
        "URL": _url_prop(data.get("url")),
        "Lead Key": _rich_text_prop(data.get("lead_key")),
        "Days to Sale": _number_prop(data.get("days_to_sale")),
    }

    return props


# ----------------------------
# STAGE 2-3 PROPERTY BUILDERS
# ----------------------------

def build_extra_properties(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build optional Stage 2-3 properties. Caller should pass only keys it wants to set.
    Non-destructive: do NOT overwrite existing non-empty fields with empty.
    """
    props: Dict[str, Any] = {}

    def put_rich(name: str, val: Optional[str]):
        if val is None:
            return
        s = str(val)
        if not s.strip():
            return
        props[name] = _rich_text_prop(s)

    def put_num(name: str, val: Any):
        if val is None:
            return
        try:
            f = float(val)
        except Exception:
            return
        props[name] = _number_prop(f)

    def put_url(name: str, val: Optional[str]):
        if not val:
            return
        props[name] = _url_prop(val)

    def put_chk(name: str, val: Any):
        if val is None:
            return
        props[name] = _checkbox_prop(val)

    def put_date(name: str, val: Optional[str]):
        if not val:
            return
        props[name] = _date_prop(val)

    # Common enrichment fields
    put_rich("Owner Name", data.get("owner_name"))
    put_rich("Mailing Address", data.get("mailing_address"))
    put_chk("Absentee Flag", data.get("absentee_flag"))
    put_num("Beds", data.get("beds"))
    put_num("Baths", data.get("baths"))
    put_num("Sqft", data.get("sqft"))
    put_num("Year Built", data.get("year_built"))
    put_num("Estimated Value Low", data.get("estimated_value_low"))
    put_num("Estimated Value High", data.get("estimated_value_high"))
    put_rich("Loan Indicators", data.get("loan_indicators"))
    put_date("Last Sale Date", data.get("last_sale_date"))
    put_num("Tax Assessed Value", data.get("tax_assessed_value"))
    put_num("Enrichment Confidence", data.get("enrichment_confidence"))

    # Pipeline JSON / computed / grades
    put_rich("Enrichment JSON", data.get("enrichment_json"))
    put_rich("Comps JSON", data.get("comps_json"))
    put_rich("Comps Summary", data.get("comps_summary"))
    put_num("Value Band Low", data.get("value_band_low"))
    put_num("Value Band High", data.get("value_band_high"))
    put_num("Liquidity Score", data.get("liquidity_score"))
    put_num("Grade Score", data.get("grade_score"))
    put_rich("Grade", data.get("grade"))
    put_rich("Grade Reasons", data.get("grade_reasons"))
    put_rich("Status Flag", data.get("status_flag"))
    put_url("Packet PDF URL", data.get("packet_pdf_url"))

    return props


# ----------------------------
# DB schema utilities (safe)
# ----------------------------

def _fetch_db_schema(force: bool = False) -> Optional[Dict[str, Any]]:
    global _DB_SCHEMA_CACHE, _DB_SCHEMA_FETCHED_AT

    if not _have_creds():
        return None

    now = time.time()
    if _DB_SCHEMA_CACHE and not force and (now - _DB_SCHEMA_FETCHED_AT) < 300:
        return _DB_SCHEMA_CACHE

    try:
        db = _request("GET", f"/databases/{NOTION_DATABASE_ID}")
        _DB_SCHEMA_CACHE = db.get("properties") or {}
        _DB_SCHEMA_FETCHED_AT = now
        return _DB_SCHEMA_CACHE
    except Exception:
        return _DB_SCHEMA_CACHE


def database_property_names() -> List[str]:
    schema = _fetch_db_schema()
    if not schema:
        return []
    return list(schema.keys())


def filter_properties_to_database(props: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only properties that exist in the current Notion DB schema.
    This prevents Notion errors if your schema differs between environments.
    """
    schema = _fetch_db_schema()
    if not schema:
        return props
    out = {}
    for k, v in props.items():
        if k in schema:
            out[k] = v
    return out


# ----------------------------
# CREATE / UPDATE / UPSERT
# ----------------------------

def create_lead(properties: Dict[str, Any]) -> Optional[str]:
    if not _have_creds():
        _warn_missing_creds_once()
        return None
    body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
    body["properties"] = filter_properties_to_database(body["properties"])
    res = _request("POST", "/pages", json=body)
    return res.get("id")


def update_lead(page_id: str, properties: Dict[str, Any]) -> None:
    if not _have_creds():
        _warn_missing_creds_once()
        return
    body = {"properties": filter_properties_to_database(properties)}
    _request("PATCH", f"/pages/{page_id}", json=body)


def find_existing_by_lead_key(lead_key: str) -> Optional[dict]:
    if not lead_key:
        return None
    filter_obj = {"property": "Lead Key", "rich_text": {"equals": lead_key}}
    pages = query_database(filter_obj, page_size=1, max_pages=1)
    return pages[0] if pages else None


# ----------------------------
# QUERY
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


# ----------------------------
# FIELD EXTRACTORS
# ----------------------------

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
        s = prop.get("select") or None
        return (s.get("name") if s else "") or ""
    except Exception:
        return ""


def _number_plain(prop: dict) -> Optional[float]:
    try:
        return prop.get("number")
    except Exception:
        return None


def _date_plain(prop: dict) -> str:
    try:
        d = prop.get("date") or None
        return (d.get("start") if d else "") or ""
    except Exception:
        return ""


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
    }
    return out
