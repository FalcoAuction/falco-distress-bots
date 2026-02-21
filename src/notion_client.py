import os
import json as _json
import time
import re
import datetime as _dt
import requests
from typing import Any, Dict, Optional, Tuple, List

NOTION_API_KEY = os.getenv("NOTION_API_KEY", "")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "")

NOTION_API_BASE = "https://api.notion.com/v1"
NOTION_VERSION = os.getenv("NOTION_VERSION", "2022-06-28")

# If true (default), we will attempt to add missing Stage2/3 properties to the DB schema.
# This is Notion-only (no ATTOM), and is required if your DB currently lacks Enrichment/Comps/Grade fields.
AUTO_SCHEMA = os.getenv("FALCO_NOTION_AUTO_SCHEMA", "1").strip().lower() not in ("0", "false", "no", "")

_WARNED_MISSING_CREDS = False

# Stage 2-3 schema cache
_DB_SCHEMA_CACHE: Optional[Dict[str, Any]] = None
_DB_SCHEMA_FETCHED_AT: float = 0.0

# =========================================================
# WRITE KILL SWITCH  (FALCO_NOTION_WRITE=1)
# Default is SAFE (disabled). Set FALCO_NOTION_WRITE=1 to
# allow create/update calls to reach Notion.
# =========================================================
_NOTION_WRITE: bool = os.getenv("FALCO_NOTION_WRITE", "0").strip() == "1"
if not _NOTION_WRITE:
    print("[NOTION] Write kill switch active — FALCO_NOTION_WRITE != '1'. create/update are no-ops.")

# =========================================================
# DRY-RUN MODE  (FALCO_DRY_RUN=1)
# No Notion API calls are made. Every lead is written to
# out/leads_<timestamp>.jsonl instead.
# =========================================================
_DRY_RUN: bool = os.getenv("FALCO_DRY_RUN", "0").strip().lower() not in ("", "0", "false", "no")
_DRY_RUN_FILE: Optional[str] = None
_DRY_RUN_COUNT: int = 0


def _get_dry_run_file() -> str:
    global _DRY_RUN_FILE
    if _DRY_RUN_FILE is None:
        os.makedirs("out", exist_ok=True)
        ts = _dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        _DRY_RUN_FILE = f"out/leads_{ts}.jsonl"
        print(f"[DRY_RUN] Output file: {_DRY_RUN_FILE}")
    return _DRY_RUN_FILE


def _dry_run_append(action: str, properties: Dict[str, Any]) -> None:
    global _DRY_RUN_COUNT
    try:
        lk = properties["Lead Key"]["rich_text"][0]["text"]["content"]
    except Exception:
        lk = ""
    record = {"_action": action, "_lead_key": lk, "properties": properties}
    path = _get_dry_run_file()
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(_json.dumps(record) + "\n")
    _DRY_RUN_COUNT += 1
    print(f"[DRY_RUN] {action} lead_key={lk or '[missing]'} total={_DRY_RUN_COUNT}")


# =========================================================
# CORE UTILITIES
# =========================================================

def _have_creds() -> bool:
    return bool(NOTION_API_KEY and NOTION_DATABASE_ID)


def _warn_missing_creds_once():
    global _WARNED_MISSING_CREDS
    if _WARNED_MISSING_CREDS:
        return
    _WARNED_MISSING_CREDS = True
    print("[NotionClient] WARNING: Missing NOTION_API_KEY or NOTION_DATABASE_ID. Notion operations disabled for this run.")


def _headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, json: Optional[dict] = None, *, timeout: int = 30) -> dict:
    """
    Low-level Notion request.
    NOTE: This function raises RuntimeError on API errors (>=300).
    Higher-level wrappers catch and log so Stage 1 doesn't crash.
    """
    if not _have_creds():
        _warn_missing_creds_once()
        return {}
    url = NOTION_API_BASE + path
    r = requests.request(method, url, headers=_headers(), json=json, timeout=timeout)
    if r.status_code >= 300:
        raise RuntimeError(f"Notion API error {r.status_code}: {r.text}")
    return r.json()


def normalize_page_id(page_id: str) -> str:
    """
    Notion page IDs should be a UUID (with or without dashes).
    If a full URL sneaks in, extract the last UUID-like token.
    """
    if not page_id:
        return ""

    s = str(page_id).strip()

    # If it's a dict or something odd, just string-coerce and try to extract uuid.
    # Example: "{'id': '...'}" is not valid, but we can still recover the UUID.
    # Find a 32-hex or UUID pattern anywhere.
    m = re.search(r"([0-9a-fA-F]{32})", s)
    if m:
        return m.group(1)

    m = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", s)
    if m:
        return m.group(1)

    # If it contains notion.so URL, try last path segment
    if "notion.so" in s:
        tail = s.split("/")[-1]
        tail = tail.split("?")[0]
        m = re.search(r"([0-9a-fA-F]{32})", tail)
        if m:
            return m.group(1)
        m = re.search(r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})", tail)
        if m:
            return m.group(1)

    # Fall back: keep only safe URL/path chars
    return s


# =========================================================
# SCHEMA MANAGEMENT (UNBLOCK STAGE 2/3)
# =========================================================

def _required_stage23_properties() -> Dict[str, Any]:
    """
    Properties that Stage 2/3 expects to exist.
    We only add missing ones. This is non-destructive.
    """
    return {
        # Enrichment
        "Enrichment JSON": {"rich_text": {}},
        "Enrichment Confidence": {"number": {"format": "number"}},
        "Estimated Value Low": {"number": {"format": "number"}},
        "Estimated Value High": {"number": {"format": "number"}},

        # Comps / value band
        "Comps JSON": {"rich_text": {}},
        "Comps Summary": {"rich_text": {}},
        "Value Band Low": {"number": {"format": "number"}},
        "Value Band High": {"number": {"format": "number"}},
        "Liquidity Score": {"number": {"format": "number"}},

        # Grading
        "Grade": {"rich_text": {}},
        "Grade Reasons": {"rich_text": {}},
        "Grade Score": {"number": {"format": "number"}},
        "Status Flag": {"rich_text": {}},

        # Packaging
        "Packet PDF URL": {"url": {}},
    }


def _fetch_db_schema(force: bool = False) -> Optional[Dict[str, Any]]:
    global _DB_SCHEMA_CACHE, _DB_SCHEMA_FETCHED_AT

    if not _have_creds():
        return None

    now = time.time()
    if _DB_SCHEMA_CACHE and not force and (now - _DB_SCHEMA_FETCHED_AT) < 300:
        return _DB_SCHEMA_CACHE

    try:
        db = _request("GET", f"/databases/{NOTION_DATABASE_ID}")
        schema = db.get("properties") or {}
        _DB_SCHEMA_CACHE = schema
        _DB_SCHEMA_FETCHED_AT = now

        # Attempt auto-schema extension once schema is known.
        if AUTO_SCHEMA:
            _ensure_required_schema(schema)

        return _DB_SCHEMA_CACHE
    except Exception as e:
        print(f"[NotionClient] schema fetch error: {type(e).__name__}: {e}")
        return _DB_SCHEMA_CACHE


def _ensure_required_schema(schema: Dict[str, Any]) -> None:
    """
    Add missing Stage 2/3 properties to the database schema.
    This fixes the root cause where enrichment/comps fields are silently dropped.
    """
    try:
        required = _required_stage23_properties()
        missing = {k: v for k, v in required.items() if k not in (schema or {})}

        if not missing:
            return

        body = {"properties": missing}
        _request("PATCH", f"/databases/{NOTION_DATABASE_ID}", json=body)
        print(f"[NotionClient] Added missing Notion DB properties: {sorted(list(missing.keys()))}")

        # Refresh cache after mutation
        db = _request("GET", f"/databases/{NOTION_DATABASE_ID}")
        _DB_SCHEMA_CACHE = db.get("properties") or {}
        _DB_SCHEMA_FETCHED_AT = time.time()
    except Exception as e:
        # Do NOT crash pipeline. Just log.
        print(f"[NotionClient] auto-schema error (non-fatal): {type(e).__name__}: {e}")


def database_property_names() -> List[str]:
    schema = _fetch_db_schema()
    if not schema:
        return []
    return list(schema.keys())


def filter_properties_to_database(props: Dict[str, Any]) -> Dict[str, Any]:
    schema = _fetch_db_schema()
    if not schema:
        return props
    return {k: v for k, v in props.items() if k in schema}


def prune_empty_properties_for_update(properties: Dict[str, Any]) -> Dict[str, Any]:
    """
    Non-destructive update rule:
    - Never overwrite non-empty fields with empty fields.
    - We can safely drop "empty" update payloads.
    """
    out: Dict[str, Any] = {}
    for k, v in (properties or {}).items():
        if not isinstance(v, dict):
            continue

        if "rich_text" in v:
            rt = v.get("rich_text") or []
            if rt:
                out[k] = v
            continue

        if "title" in v:
            tt = v.get("title") or []
            if tt:
                out[k] = v
            continue

        if "number" in v:
            if v.get("number") is not None:
                out[k] = v
            continue

        if "date" in v:
            if v.get("date") is not None:
                out[k] = v
            continue

        if "select" in v:
            if v.get("select") is not None:
                out[k] = v
            continue

        if "url" in v:
            if v.get("url") is not None:
                out[k] = v
            continue

        if "checkbox" in v:
            # checkbox should always pass (False is meaningful)
            out[k] = v
            continue

    return out


# =========================================================
# PROPERTY BUILDERS
# =========================================================

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
    return {"checkbox": bool(val)}


def build_properties(*args, **kwargs) -> Dict[str, Any]:
    """
    Stage 1 build_properties used by existing bots.
    Backwards-compatible.
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


def build_extra_properties(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Stage 2/3 property builder. Writes only non-empty.
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

    # Stage 2/3 fields
    put_rich("Enrichment JSON", data.get("enrichment_json"))
    put_num("Enrichment Confidence", data.get("enrichment_confidence"))
    put_num("Estimated Value Low", data.get("estimated_value_low"))
    put_num("Estimated Value High", data.get("estimated_value_high"))

    put_rich("Comps JSON", data.get("comps_json"))
    put_rich("Comps Summary", data.get("comps_summary"))
    put_num("Value Band Low", data.get("value_band_low"))
    put_num("Value Band High", data.get("value_band_high"))
    put_num("Liquidity Score", data.get("liquidity_score"))

    put_rich("Grade", data.get("grade"))
    put_rich("Grade Reasons", data.get("grade_reasons"))
    put_num("Grade Score", data.get("grade_score"))
    put_rich("Status Flag", data.get("status_flag"))

    put_url("Packet PDF URL", data.get("packet_pdf_url"))

    return props


# =========================================================
# CRUD
# =========================================================

def find_existing_by_lead_key(lead_key: str) -> Optional[str]:
    """
    MUST return page_id string (Stage 1 expects this).
    """
    if _DRY_RUN:
        return None
    if not _have_creds():
        _warn_missing_creds_once()
        return None
    if not lead_key:
        return None

    try:
        body = {"filter": {"property": "Lead Key", "rich_text": {"equals": lead_key}}}
        res = _request("POST", f"/databases/{NOTION_DATABASE_ID}/query", json=body)
        results = res.get("results", [])
        if not results:
            return None
        if len(results) > 1:
            raise ValueError(f"[NOTION] Duplicate lead_key detected: '{lead_key}' matched {len(results)} pages")
        return results[0].get("id")
    except Exception as e:
        print(f"[NOTION] find_existing error (non-fatal): {type(e).__name__}: {e}")
        return None


def create_lead(properties: Dict[str, Any]) -> str:
    if _DRY_RUN:
        _dry_run_append("create", properties)
        return "[dry-run]"
    if not _NOTION_WRITE:
        return ""
    if not _have_creds():
        _warn_missing_creds_once()
        return ""

    try:
        properties = filter_properties_to_database(properties)
        body = {"parent": {"database_id": NOTION_DATABASE_ID}, "properties": properties}
        res = _request("POST", "/pages", json=body)
        return res.get("id", "")
    except Exception as e:
        print(f"[NOTION] create error (non-fatal): {type(e).__name__}: {e}")
        return ""


def update_lead(page_id: str, properties: Dict[str, Any]) -> str:
    """
    Stage 1 MUST NOT crash if Notion rejects an update.
    We log and continue.
    """
    if _DRY_RUN:
        _dry_run_append("update", properties)
        return page_id
    if not _NOTION_WRITE:
        return page_id
    if not _have_creds():
        _warn_missing_creds_once()
        return page_id

    pid = normalize_page_id(page_id)
    if not pid:
        print("[NOTION] update error (non-fatal): empty page_id")
        return page_id

    try:
        safe_props = prune_empty_properties_for_update(properties)
        safe_props = filter_properties_to_database(safe_props)
        if not safe_props:
            return pid
        body = {"properties": safe_props}
        res = _request("PATCH", f"/pages/{pid}", json=body)
        return res.get("id", pid)
    except Exception as e:
        print(f"[NOTION] update error (non-fatal): {type(e).__name__}: {e}")
        return pid


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


# =========================================================
# QUERY HELPERS (Stage 2-3)
# =========================================================

def query_database(
    filter_obj: Optional[dict] = None,
    *,
    page_size: int = 50,
    sorts: Optional[list] = None,
    max_pages: int = 10,
) -> List[dict]:
    """
    Database query with pagination. Returns list of page objects.
    IMPORTANT: filter_obj must be the Notion filter object itself (e.g. {"and":[...]}),
    NOT wrapped in {"filter": ...}. We wrap it here.
    """
    if not _have_creds():
        _warn_missing_creds_once()
        return []

    _fetch_db_schema()  # ensures schema cached + auto-schema runs (if enabled)

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

        try:
            res = _request("POST", f"/databases/{NOTION_DATABASE_ID}/query", json=body)
        except Exception as e:
            print(f"[NOTION] query error (non-fatal): {type(e).__name__}: {e}")
            return out

        out.extend(res.get("results", []) or [])
        if not res.get("has_more"):
            break
        start_cursor = res.get("next_cursor")

    return out


# =========================================================
# FIELD EXTRACTORS
# =========================================================

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
    Extracts known Stage 1 fields + Stage 2/3 fields when present.
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

        # Stage 2/3 fields (may not exist)
        "enrichment_confidence": _number_plain(gp("Enrichment Confidence")),
        "estimated_value_low": _number_plain(gp("Estimated Value Low")),
        "estimated_value_high": _number_plain(gp("Estimated Value High")),
        "enrichment_json": _rt_plain(gp("Enrichment JSON")),

        "comps_json": _rt_plain(gp("Comps JSON")),
        "comps_summary": _rt_plain(gp("Comps Summary")),
        "value_band_low": _number_plain(gp("Value Band Low")),
        "value_band_high": _number_plain(gp("Value Band High")),
        "liquidity_score": _number_plain(gp("Liquidity Score")),

        "grade": _rt_plain(gp("Grade")),
        "grade_reasons": _rt_plain(gp("Grade Reasons")),
        "grade_score": _number_plain(gp("Grade Score")),
        "status_flag": _rt_plain(gp("Status Flag")),

        "packet_pdf_url": _url_plain(gp("Packet PDF URL")),
    }

    return out
