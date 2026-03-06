# src/enrichment/attom_enricher.py

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..notion_client import build_extra_properties, extract_page_fields, query_database, update_lead
from ..settings import get_dts_window, is_allowed_county, within_target_counties
from .attom_client import AttomClient, AttomError
from ..gating.convertibility import is_institutional
from ..storage import sqlite_store as _store
from ..telemetry.stage2_gating import write_gating_event

DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


def _write_gate(lead_key: str, result: str, skip_reason: Optional[str] = None, meta: Optional[Dict[str, Any]] = None) -> None:
    try:
        write_gating_event(lead_key, result, skip_reason, meta)
    except Exception:
        pass


# =========================================================
# SMALL UTILS
# =========================================================

def _clip_json(obj: Any, max_chars: int = 1800) -> str:
    """Keep Notion rich_text payloads small-ish (and safe to store)."""
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


def _parse_raw_json_safe(raw: Any, lead_key: str = "") -> Optional[Dict[str, Any]]:
    """
    Defensive parser for attom_raw_json read back from DB.
    - If already a dict (caller pre-parsed): return as-is.
    - If string: attempt json.loads.
    - On failure: print warning and return None.
    Never raises.
    """
    if isinstance(raw, dict):
        return raw
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        print(f"[ATTOM][WARN] raw_json not valid JSON for lead_key={lead_key!r}")
        return None


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_zip(s: str) -> str:
    return re.sub(r"\b\d{5}(?:-\d{4})?\b", "", s or "").strip(" ,")


def _normalize_state(st: str) -> str:
    st = (st or "").strip().upper()
    if len(st) == 2 and st.isalpha():
        return st
    return "TN"


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).replace("$", "").replace(",", "").strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _status_msg(payload: Dict[str, Any]) -> str:
    try:
        s = payload.get("status") or {}
        if isinstance(s, dict):
            return str(s.get("msg") or s.get("message") or "")
    except Exception:
        pass
    return ""


def _is_success_without_result(payload: Dict[str, Any]) -> bool:
    msg = (_status_msg(payload) or "").lower()
    if "successwithoutresult" in msg:
        return True
    prop = payload.get("property")
    if isinstance(prop, list) and len(prop) == 0:
        return True
    return False


def _has_property(payload: Dict[str, Any]) -> bool:
    prop = payload.get("property")
    return isinstance(prop, list) and len(prop) > 0 and isinstance(prop[0], dict)


def _get_p0(payload: Dict[str, Any]) -> Dict[str, Any]:
    prop = payload.get("property")
    if isinstance(prop, list) and prop and isinstance(prop[0], dict):
        return prop[0]
    return {}


def _is_legacy_avm_raw(raw_json: Any) -> bool:
    """
    Returns True if attom_raw_json is a legacy AVM-only blob (pre-merged format).
    Legacy blobs have 'amount' but not 'avm'/'detail' keys.
    Merged blobs always have both 'avm' and 'detail' keys.
    """
    if not isinstance(raw_json, dict):
        return False
    return "avm" not in raw_json and "detail" not in raw_json and "amount" in raw_json


def _call_safe(
    client_method: Any,
    **kwargs: Any,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Calls client_method(**kwargs) and returns:
      (payload, None)      — call succeeded and has property results
      (None, "no_result")  — SuccessWithoutResult / empty property list
      (None, error_msg)    — AttomError or other exception
    Caller inspects error_msg: "no_result" vs anything else (real error).
    """
    try:
        payload = client_method(**kwargs)
    except AttomError as e:
        return None, str(e)
    except Exception as e:
        return None, f"{type(e).__name__}: {e}"
    if _is_success_without_result(payload) or not _has_property(payload):
        return None, "no_result"
    return payload, None


def _extract_value_from_attom_avm(avm_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Observed shape:
      property[0].avm.amount = {"scr":95,"value":529582,"high":..., "low":..., "fsd":...}
    Returns (value, low, high).
    """
    p0 = _get_p0(avm_payload)
    avm = p0.get("avm") if isinstance(p0, dict) else None
    if not isinstance(avm, dict):
        return None, None, None
    amt = avm.get("amount")
    if not isinstance(amt, dict):
        v = _safe_float(avm.get("amount"))
        return v, None, None
    v = _safe_float(amt.get("value"))
    lo = _safe_float(amt.get("low"))
    hi = _safe_float(amt.get("high"))
    return v, lo, hi


def _parse_address(addr: str) -> Tuple[str, str]:
    """Best-effort split into ATTOM address1/address2."""
    raw = _clean_spaces(str(addr or ""))
    if not raw:
        return "", ""

    raw = raw.replace("\n", " ").replace("\r", " ")
    raw = _clean_spaces(raw)

    parts = [p.strip() for p in raw.split(",") if p.strip()]

    def fix_state_token(token: str) -> str:
        t = (token or "").strip()
        if t.lower() == "tennessee":
            return "TN"
        return t

    if len(parts) >= 3:
        street = parts[0]
        city = _strip_zip(parts[1])
        city = re.sub(r"\bTN\b", "", city, flags=re.I).strip(" ,")
        city = _clean_spaces(city)

        st_part = _strip_zip(parts[2])
        st_tokens = [fix_state_token(t) for t in st_part.replace(",", " ").split() if t.strip()]
        st = _normalize_state(st_tokens[0] if st_tokens else "TN")

        if street and city:
            return street, f"{city}, {st}"

    if len(parts) == 2:
        street = parts[0]
        tail = _strip_zip(parts[1])
        tail_tokens = [fix_state_token(t) for t in tail.replace(",", " ").split() if t.strip()]

        st = "TN"
        city_tokens = tail_tokens[:]
        for i, t in enumerate(tail_tokens):
            if len(t) == 2 and t.isalpha():
                st = _normalize_state(t)
                city_tokens = tail_tokens[:i]
                break

        city = _clean_spaces(" ".join(city_tokens))
        city = re.sub(r"\bTN\b", "", city, flags=re.I).strip(" ,")
        if not city:
            city = "Nashville"
        return street, f"{city}, {st}"

    tokens = raw.split()
    st_idx = None
    for i, t in enumerate(tokens):
        tt = fix_state_token(t).upper()
        if tt in ("TN", "KY", "AL", "MS", "GA", "NC", "SC", "VA", "AR"):
            st_idx = i
            break
    if st_idx is not None and st_idx >= 1:
        st = _normalize_state(tokens[st_idx])
        city = tokens[st_idx - 1]
        street = " ".join(tokens[: st_idx - 1]).strip()
        if street and city:
            return street, f"{city}, {st}"

    if len(tokens) >= 3 and tokens[-1].isalpha():
        city = tokens[-1]
        street = " ".join(tokens[:-1]).strip()
        return street, f"{city}, TN"

    return raw, "Nashville, TN"


def _read_no_result_marker(enrichment_json: str) -> Optional[Dict[str, Any]]:
    """
    Prefer parsing full JSON. Fallback to regex fragment extraction if the stored
    string is clipped / contains extra text.
    """
    if not enrichment_json:
        return None

    # 1) full JSON parse
    try:
        obj = json.loads(enrichment_json)
        if isinstance(obj, dict):
            falco = obj.get("falco") or {}
            if isinstance(falco, dict):
                attom = falco.get("attom")
                if isinstance(attom, dict):
                    return attom
    except Exception:
        pass

    # 2) fragment parse fallback
    try:
        m = re.search(r'("falco"\s*:\s*\{.*?\})', enrichment_json)
        if not m:
            return None
        frag = "{" + m.group(1) + "}"
        obj2 = json.loads(frag)
        return (obj2.get("falco") or {}).get("attom")
    except Exception:
        return None


# ============================================================
# ATTOM DEDUPE (TTL)
# ============================================================

def _parse_iso_z(ts: str | None) -> datetime | None:
    if not ts:
        return None
    s = str(ts).strip()
    if not s:
        return None
    # stored like: 2026-02-24T03:58:03.906869Z
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _attom_ttl_hours() -> int:
    try:
        return int(os.getenv("FALCO_ATTOM_TTL_HOURS", "720"))  # 30d default
    except Exception:
        return 720


def _already_enriched_recently(cur, lead_key: str) -> tuple[bool, str | None]:
    """
    Returns (skip, reason). Skip if we have an attom_enrichments row within TTL,
    regardless of status (enriched / no_result_detail / etc).
    """
    try:
        row = cur.execute(
            "SELECT status, enriched_at FROM attom_enrichments WHERE lead_key=? ORDER BY enriched_at DESC LIMIT 1",
            (lead_key,),
        ).fetchone()
        if not row:
            return (False, None)
        last_dt = _parse_iso_z(row[1])
        if not last_dt:
            return (False, None)
        ttl = timedelta(hours=_attom_ttl_hours())
        if datetime.now(timezone.utc) - last_dt <= ttl:
            return (True, f"ttl_hit status={row[0]} enriched_at={row[1]}")
        return (False, None)
    except Exception:
        return (False, None)


# =========================================================
# INSTITUTIONAL / LOW-PROBABILITY FILTER (TN-native)
# =========================================================

_DEFAULT_INSTITUTIONAL_TRUSTEE_KEYWORDS: List[str] = [
    "mackie wolf zientz & mann",
    "mackie, wolf, zientz & mann",
    "western progressive",
    "winchester sellers foster & steele",
    "kizer bonds hughes & bowen",
    "kizer, bonds, hughes & bowen",
    "crawford & von keller",
    "henry, henry & underwood",
    "wilson & associates",
    "mccalla raymer",
    "shapiro",
]

_DEFAULT_INSTITUTIONAL_CONTEXT_KEYWORDS: List[str] = []


def _load_keyword_list(env_var: str, default_list: List[str]) -> List[str]:
    raw = os.getenv(env_var, "").strip()
    if not raw:
        return default_list[:]
    parts: List[str] = []
    for p in raw.split(","):
        p = _clean_spaces(p).lower()
        if p:
            parts.append(p)
    return parts or default_list[:]


def _detect_institutional(fields: Dict[str, Any]) -> Optional[Dict[str, str]]:
    trustee = _clean_spaces(str(fields.get("trustee_attorney") or "")).lower()
    contact = _clean_spaces(str(fields.get("contact_info") or "")).lower()
    raw_snip = _clean_spaces(str(fields.get("raw_snippet") or "")).lower()
    url = _clean_spaces(str(fields.get("url") or "")).lower()

    trustee_kws = _load_keyword_list("FALCO_INSTITUTIONAL_TRUSTEE_KEYWORDS", _DEFAULT_INSTITUTIONAL_TRUSTEE_KEYWORDS)
    ctx_kws = _load_keyword_list("FALCO_INSTITUTIONAL_CONTEXT_KEYWORDS", _DEFAULT_INSTITUTIONAL_CONTEXT_KEYWORDS)

    def has_any(text: str, kws: List[str]) -> Optional[str]:
        for kw in kws:
            if kw and kw in text:
                return kw
        return None

    m = has_any(trustee, trustee_kws)
    if m:
        return {"matched_in": "Trustee/Attorney", "keyword": m}

    m = has_any(contact, trustee_kws)
    if m:
        return {"matched_in": "Contact Info", "keyword": m}

    m = has_any(raw_snip, trustee_kws)
    if m:
        return {"matched_in": "Raw Snippet", "keyword": m}

    m = has_any(url, trustee_kws)
    if m:
        return {"matched_in": "URL", "keyword": m}

    if ctx_kws:
        m = has_any(raw_snip, ctx_kws)
        if m:
            return {"matched_in": "Raw Snippet", "keyword": m}

    return None


def _mark_institutional_skip(page_id: str, *, reason: Dict[str, str], now: datetime) -> None:
    try:
        write_obj = {
            "status_flag": "INSTITUTIONAL_SKIP",
            "enrichment_json": _clip_json(
                {
                    "falco": {
                        "attom": {
                            "skipped": True,
                            "skip_reason": "institutional_trustee",
                            "matched_in": reason.get("matched_in"),
                            "keyword": reason.get("keyword"),
                            "ts": now.isoformat().replace("+00:00", "Z"),
                        }
                    }
                }
            ),
        }
        update_lead(page_id, build_extra_properties(write_obj))
    except Exception as e:
        if DEBUG:
            print(f"[ATTOM][DEBUG] failed to mark institutional skip page_id={page_id}: {type(e).__name__}: {e}")


# =========================================================
# SQLITE CANDIDATE LOADER
# =========================================================

def _default_ttl_by_status() -> Dict[str, int]:
    # days
    return {
        "enriched": 30,
        "no_result_detail": 30,
        "no_result_avm": 14,
        "partial_detail_failed": 7,
        "partial_avm_failed": 7,
    }

def _load_sqlite_candidates(dts_min: int, dts_max: int) -> List[Dict[str, Any]]:
    """
    Load enrichment candidates from SQLite (FALCO_STAGE2_SOURCE=sqlite).
    Returns a list of fields-compatible dicts, filtered to the DTS window in Python.
    Excludes leads already present in attom_enrichments with a terminal status.
    """
    import sqlite3

    db_path = os.environ.get("FALCO_SQLITE_PATH", "data/falco.db")
    print(f"[ATTOM][sqlite] db_path={db_path} cwd={os.getcwd()}")
    if not os.path.isfile(db_path):
        print(f"[ATTOM][sqlite] DB not found at {db_path} — no candidates.")
        return []

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    try:
        rows = con.execute(
            """
            SELECT l.lead_key, l.address, l.county, l.state,
                   ie.sale_date, ie.source_url, ie.source,
                   latest_ae.status AS ae_status,
                   latest_ae.enriched_at AS ae_enriched_at,
                   latest_ae.avm_value AS ae_avm_value,
                   latest_ae.confidence AS ae_confidence
            FROM leads l
            INNER JOIN (
                SELECT lead_key, MAX(id) AS max_id
                FROM ingest_events
                WHERE sale_date IS NOT NULL
                GROUP BY lead_key
            ) latest ON latest.lead_key = l.lead_key
            INNER JOIN ingest_events ie ON ie.id = latest.max_id
            LEFT JOIN (
                SELECT ae1.lead_key, ae1.status, ae1.enriched_at, ae1.avm_value, ae1.confidence
                FROM attom_enrichments ae1
                INNER JOIN (
                    SELECT lead_key, MAX(id) AS max_id
                    FROM attom_enrichments
                    GROUP BY lead_key
                ) m ON m.lead_key = ae1.lead_key AND m.max_id = ae1.id
            ) latest_ae ON latest_ae.lead_key = l.lead_key
            WHERE l.address IS NOT NULL AND l.address != ''
            """,
        ).fetchall()
    finally:
        con.close()

    print(f"[ATTOM][sqlite] sqlite_rows_with_sale_date={len(rows)}")

    ttl_days_default = int(os.environ.get("FALCO_ATTOM_TTL_DAYS", "30"))
    ttl_days = ttl_days_default  # kept for backward-compat reference in print line
    _TERMINAL = frozenset({"enriched", "no_result_detail", "no_result_avm", "partial_detail_failed", "partial_avm_failed"})
    _ttl_by_status_raw = os.environ.get(
        "FALCO_ATTOM_TTL_BY_STATUS",
        "enriched=30,no_result_detail=30,no_result_avm=14,partial_detail_failed=7,partial_avm_failed=7",
    ).strip()
    ttl_by_status: Dict[str, int] = {}
    for _entry in _ttl_by_status_raw.split(",") if _ttl_by_status_raw else []:
        _entry = _entry.strip()
        if "=" not in _entry:
            continue
        _k, _, _v = _entry.partition("=")
        _k = _k.strip()
        if _k not in _TERMINAL:
            continue
        try:
            _n = int(_v.strip())
            # Prevent accidental "always refresh" behavior when env sets 0
            # unless explicitly allowed.
            allow_zero = os.environ.get("FALCO_ATTOM_ALLOW_ZERO_TTL", "0").strip() in ("1", "true", "True")
            if _n <= 0 and not allow_zero:
                continue
            ttl_by_status[_k] = _n
        except ValueError:
            pass
    if not ttl_by_status:
        ttl_by_status = _default_ttl_by_status()
    _ttl_mode_raw = os.environ.get("FALCO_ATTOM_TTL_MODE", "terminal_only").strip().lower()
    ttl_mode = _ttl_mode_raw if _ttl_mode_raw in ("terminal_only", "all") else "terminal_only"
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    candidates: List[Dict[str, Any]] = []
    ttl_skipped_fresh = 0
    ttl_expired_refreshable = 0
    parse_or_missing_skipped = 0
    no_prior_enrichment = 0
    _status_fresh: Dict[str, int] = {}
    _status_expired: Dict[str, int] = {}
    _row_keys_cache: Optional[List[str]] = None
    for row in rows:
        if _row_keys_cache is None:
            _row_keys_cache = list(row.keys())
        _is_ttl_expired = False
        ae_status = row["ae_status"] if "ae_status" in _row_keys_cache else None
        _has_prior = ae_status is not None
        if ttl_mode == "all":
            _ttl_applies = ae_status is not None and ae_status != ""
        else:
            _ttl_applies = ae_status in _TERMINAL
        if _ttl_applies:
            ttl_days_effective = ttl_by_status.get(ae_status, ttl_days_default)
            ae_enriched_at = (row["ae_enriched_at"] if "ae_enriched_at" in _row_keys_cache else None) or ""
            if not ae_enriched_at:
                # Missing enriched_at: conservative — do not refresh
                parse_or_missing_skipped += 1
                _write_gate(row["lead_key"], "skipped", "TERMINAL_MISSING_TS", {
                    "status": ae_status,
                    "ttl_days_effective": ttl_days_effective,
                })
                continue
            try:
                enriched_dt = datetime.fromisoformat(ae_enriched_at.replace("Z", "+00:00"))
                if enriched_dt.tzinfo is None:
                    enriched_dt = enriched_dt.replace(tzinfo=timezone.utc)
                else:
                    enriched_dt = enriched_dt.astimezone(timezone.utc)
                age_days = int((now_utc - enriched_dt).total_seconds() // 86400)
                if age_days < 0:
                    age_days = 0
            except Exception:
                # Unparseable enriched_at: conservative — do not refresh
                parse_or_missing_skipped += 1
                _write_gate(row["lead_key"], "skipped", "TERMINAL_PARSE_ERR_TS", {
                    "status": ae_status,
                    "ttl_days_effective": ttl_days_effective,
                    "ae_enriched_at": ae_enriched_at,
                })
                continue
            if age_days < ttl_days_effective:
                ttl_skipped_fresh += 1
                _status_fresh[ae_status] = _status_fresh.get(ae_status, 0) + 1
                _write_gate(row["lead_key"], "skipped", "TERMINAL_FRESH_TTL", {
                    "status": ae_status,
                    "ttl_days_effective": ttl_days_effective,
                    "age_days": age_days,
                })
                continue
            else:
                ttl_expired_refreshable += 1
                _status_expired[ae_status] = _status_expired.get(ae_status, 0) + 1
                _is_ttl_expired = True
                _write_gate(row["lead_key"], "eligible", "TERMINAL_TTL_EXPIRED", {
                    "status": ae_status,
                    "ttl_days_effective": ttl_days_effective,
                    "age_days": age_days,
                })
        else:
            no_prior_enrichment += 1
            if ae_status is None:
                _write_gate(row["lead_key"], "eligible", "NO_PRIOR_ENRICHMENT", {})
        if row["source"] == "API_TAX":
            _write_gate(row["lead_key"], "skipped", "TAX_SOURCE", {"source": row["source"]})
            continue
        try:
            sale_date = datetime.fromisoformat(row["sale_date"]).date()
        except (ValueError, TypeError):
            _write_gate(row["lead_key"], "skipped", "INVALID_SALE_DATE", {"sale_date": row["sale_date"]})
            continue
        dts = (sale_date - today).days
        if not (dts_min <= dts <= dts_max):
            _write_gate(row["lead_key"], "skipped", "OUTSIDE_DTS_WINDOW", {"dts": dts, "dts_min": dts_min, "dts_max": dts_max})
            continue
        county = row["county"] or ""
        if county and (not is_allowed_county(county) or not within_target_counties(county)):
            _write_gate(row["lead_key"], "skipped", "OUT_OF_GEO", {"county": county})
            continue
        lk = row["lead_key"]
        candidates.append({
            "lead_key": lk,
            "page_id": lk,          # no Notion page; reuse lead_key so page_id check passes
            "address": row["address"],
            "county": county,
            "state": row["state"] or "TN",
            "sale_date_iso": row["sale_date"],
            "url": row["source_url"],
            # Pass-through TTL fields for budget ordering
            "ae_status": ae_status,
            "ae_enriched_at": (row["ae_enriched_at"] if "ae_enriched_at" in _row_keys_cache else None),
            "ae_avm_value": (row["ae_avm_value"] if "ae_avm_value" in _row_keys_cache else None),
            "ae_confidence": (row["ae_confidence"] if "ae_confidence" in _row_keys_cache else None),
            # Fields not available from SQLite — set to falsy so existing checks are no-ops
            "status_flag": None,
            "estimated_value_low": None,
            "estimated_value_high": None,
            "enrichment_json": None,
            "trustee_attorney": None,
            "contact_info": None,
            "raw_snippet": None,
            **({"_ttl_expired_refreshable": True} if _is_ttl_expired else {}),
        })
    _status_breakdown = " ".join(
        f"{s}(fresh={_status_fresh.get(s,0)},exp={_status_expired.get(s,0)})"
        for s in ("enriched", "no_result_detail", "no_result_avm")
        if _status_fresh.get(s, 0) + _status_expired.get(s, 0) > 0
    )
    _tbs_keys = ",".join(sorted(ttl_by_status.keys()))
    _tbs_vals = ",".join(f"{k}={ttl_by_status[k]}" for k in sorted(ttl_by_status.keys()))
    print(f"[ATTOM][ttl] ttl_mode={ttl_mode} ttl_default={ttl_days_default} ttl_by_status_raw=\"{_ttl_by_status_raw}\" ttl_by_status_keys={_tbs_keys} ttl_by_status_vals={_tbs_vals} rows={len(rows)} fresh_skips={ttl_skipped_fresh} expired_refreshable={ttl_expired_refreshable} parse_or_missing_skipped={parse_or_missing_skipped} no_prior={no_prior_enrichment} breakdown=[{_status_breakdown}]")
    return candidates


# =========================================================
# MAIN
# =========================================================

def run() -> Dict[str, int]:
    _run_id = os.getenv("FALCO_RUN_ID", "").strip()
    if _run_id:
        try:
            from ..core.run_metadata import store_run_metadata as _srm
            _srm(_run_id)
        except Exception:
            pass

    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0, "skipped_enrich_missing_key": 1}

    dts_min, dts_max = get_dts_window("ENRICH")
    print(f"[ATTOM][DEBUG] DTS_WINDOW dts_min={dts_min} dts_max={dts_max}")
    # FALCO_ATTOM_MAX_ENRICH overrides FALCO_MAX_ENRICH_PER_RUN for targeted backfill runs
    _max_enrich_raw = os.getenv("FALCO_ATTOM_MAX_ENRICH", "").strip()
    max_enrich = int(_max_enrich_raw) if _max_enrich_raw else int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "10"))
    cooldown_hours = int(os.getenv("FALCO_ENRICH_NO_RESULT_COOLDOWN_HOURS", "72"))
    # FALCO_ATTOM_COUNTY_FILTER: if set, only enrich leads whose county contains this string (case-insensitive)
    county_filter = os.getenv("FALCO_ATTOM_COUNTY_FILTER", "").strip().lower()

    # cost controls
    skip_institutional = os.getenv("FALCO_SKIP_INSTITUTIONAL_ENRICH", "1").strip() not in ("", "0", "false", "False")
    mark_institutional = os.getenv("FALCO_MARK_INSTITUTIONAL_SKIP", "1").strip() not in ("", "0", "false", "False")

    client = AttomClient(api_key=api_key)

    stage2_source = os.getenv("FALCO_STAGE2_SOURCE", "notion").strip().lower()

    if stage2_source == "sqlite":
        candidates = _load_sqlite_candidates(dts_min, dts_max)
        print(f"[ATTOM] loaded_candidates={len(candidates)} source=sqlite")
    else:
        filter_obj = {
            "and": [
                {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
                {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
                {"property": "Address", "rich_text": {"is_not_empty": True}},
            ]
        }
        candidates = [extract_page_fields(p) for p in query_database(filter_obj, page_size=50, max_pages=10)]

    _refresh_limit_raw = os.getenv("FALCO_ATTOM_REFRESH_LIMIT", "0").strip()
    _refresh_limit = 0
    if _refresh_limit_raw:
        try:
            _v = int(_refresh_limit_raw)
            if _v >= 0:
                _refresh_limit = _v
        except ValueError:
            pass
    if _refresh_limit > 0:
        _ttl_cands = [c for c in candidates if c.get("_ttl_expired_refreshable") is True]
        _other_cands = [c for c in candidates if c.get("_ttl_expired_refreshable") is not True]
        _before_ttl = len(_ttl_cands)
        if _before_ttl > _refresh_limit:
            _ttl_cands = sorted(_ttl_cands, key=lambda c: (
                c.get("sale_date_iso") or "\xff",
                -(c.get("ae_avm_value") or -1),
                c.get("lead_key") or "",
            ))[:_refresh_limit]
            applied = 1
        else:
            applied = 0
        candidates = _other_cands + _ttl_cands
        print(f"[ATTOM][budget] refresh_limit={_refresh_limit} before={_before_ttl} after={len(_ttl_cands)} applied={applied}")
        for _i, _c in enumerate(_ttl_cands[:5]):
            print(f"[ATTOM][budget_sample] idx={_i} lead_key={_c.get('lead_key')} prior=1 sale_date={_c.get('sale_date_iso')} avm={_c.get('ae_avm_value') or ''} conf={_c.get('ae_confidence') or ''}")
    else:
        print(f"[ATTOM][budget] refresh_limit={_refresh_limit} before={len(candidates)} after={len(candidates)} applied=0")

    enriched = 0
    enriched_with_value = 0
    skipped_missing_address = 0
    skipped_already_enriched = 0
    skipped_no_match = 0
    skipped_cooldown = 0
    skipped_out_of_geo = 0
    skipped_institutional_count = 0
    skipped_dup_in_run = 0
    errors = 0
    stored_attom = 0

    max_attom_calls_env = os.getenv("FALCO_MAX_ATTOM_CALLS_PER_RUN")
    max_attom_calls = int(max_attom_calls_env) if max_attom_calls_env else None
    print(f"[ATTOM][DEBUG] HARD_CAP max_attom_calls={max_attom_calls}")

    now = datetime.now(timezone.utc)
    cooldown = timedelta(hours=cooldown_hours)

    logged_sample = False
    seen_addr_keys: set[str] = set()

    for fields in candidates:
        if enriched >= max_enrich:
            break

        page_id = fields.get("page_id") or ""
        lk = fields.get("lead_key") or page_id
        address = fields.get("address") or ""
        county = fields.get("county") or ""

        if not page_id:
            continue

        if is_institutional(fields):
            skipped_institutional_count += 1
            _write_gate(lk, "skipped", "INSTITUTIONAL")
            continue

        if county and (not is_allowed_county(county) or not within_target_counties(county)):
            skipped_out_of_geo += 1
            _write_gate(lk, "skipped", "OUT_OF_GEO", {"county": county})
            continue

        # Targeted county filter for backfill runs (FALCO_ATTOM_COUNTY_FILTER)
        if county_filter and county_filter not in (county or "").lower():
            continue

        if not str(address).strip():
            skipped_missing_address += 1
            _write_gate(lk, "skipped", "MISSING_ADDRESS")
            continue

        if skip_institutional:
            reason = _detect_institutional(fields)
            if reason:
                skipped_institutional_count += 1
                _write_gate(lk, "skipped", "INSTITUTIONAL_DETECTED", reason)
                if mark_institutional and stage2_source == "notion":
                    _mark_institutional_skip(page_id, reason=reason, now=now)
                if DEBUG:
                    print(f"[ATTOM][DEBUG] institutional skip page_id={page_id} matched_in={reason.get('matched_in')} kw={reason.get('keyword')}")
                continue

        if fields.get("estimated_value_low") is not None or fields.get("estimated_value_high") is not None:
            skipped_already_enriched += 1
            _write_gate(lk, "skipped", "ALREADY_ENRICHED")
            continue

        ej = str(fields.get("enrichment_json") or "").strip()
        marker = _read_no_result_marker(ej)
        if marker and (marker.get("no_result") is True) and marker.get("ts"):
            try:
                ts = datetime.fromisoformat(str(marker["ts"]).replace("Z", "+00:00"))
                if now - ts < cooldown:
                    skipped_cooldown += 1
                    _write_gate(lk, "skipped", "COOLDOWN", {"ts": str(marker.get("ts"))})
                    continue
            except Exception:
                pass

        address1, address2 = _parse_address(address)
        if not address1 or not address2:
            skipped_missing_address += 1
            _write_gate(lk, "skipped", "MISSING_ADDRESS_PARSE", {"address": address})
            continue

        addr_key = _clean_spaces(f"{address1}|{address2}").lower()
        if addr_key in seen_addr_keys:
            skipped_dup_in_run += 1
            _write_gate(lk, "skipped", "DUP_IN_RUN", {"addr_key": addr_key})
            continue
        seen_addr_keys.add(addr_key)

        print(f"[ATTOM][DEBUG] client.call_count_before_cap={client.call_count}")
        if max_attom_calls is not None and client.call_count >= max_attom_calls:
            print(f"[ATTOM][HARD_CAP] Max ATTOM calls reached ({max_attom_calls}). Stopping enrichment.")
            break

        _write_gate(lk, "eligible", meta={"address1": address1, "address2": address2})
        try:
            # Call both endpoints independently — no short-circuit
            avm_result, avm_err = _call_safe(client.avm_detail, address1=address1, address2=address2)
            detail_result, detail_err = _call_safe(client.property_detail, address1=address1, address2=address2)
            owner_result, owner_err = _call_safe(client.property_detail_owner, address1=address1, address2=address2)
            mortgage_result, mortgage_err = _call_safe(client.property_detail_mortgage, address1=address1, address2=address2)

            avm_ok = avm_result is not None
            detail_ok = detail_result is not None
            avm_is_exc = bool(avm_err and avm_err != "no_result")
            detail_is_exc = bool(detail_err and detail_err != "no_result")

            if not avm_ok and not detail_ok:
                # Both failed
                if avm_is_exc or detail_is_exc:
                    errors += 1
                    _store.insert_attom_enrichment(lk, "error_attom", None, None, None, None, None)
                    stored_attom += 1
                    if DEBUG:
                        print(f"[ATTOM][DEBUG] error {address1} | {address2}: avm_err={avm_err} detail_err={detail_err}")
                else:
                    # Both returned no_result
                    skipped_no_match += 1
                    write_obj = {
                        "enrichment_json": _clip_json({
                            "falco": {"attom": {"no_result": True, "ts": now.isoformat().replace("+00:00", "Z"), "reason": "both_no_result"}},
                            "meta": {"address1": address1, "address2": address2},
                        })
                    }
                    if stage2_source == "notion":
                        update_lead(page_id, build_extra_properties(write_obj))
                    if _store.insert_attom_enrichment(lk, "no_result_detail", None, None, None, None, None):
                        stored_attom += 1
                    if DEBUG:
                        print(f"[ATTOM][DEBUG] no-result both {address1} | {address2}")
                continue

            # At least one call succeeded — determine status
            if avm_ok and detail_ok:
                status = "enriched"
            elif avm_ok:
                status = "partial_detail_failed"   # AVM ok, DETAIL failed/no-result
            else:
                status = "partial_avm_failed"      # DETAIL ok, AVM failed/no-result

            # Extract AVM blob
            avm_blob = None
            if avm_ok:
                p0a = _get_p0(avm_result)
                avm_blob = p0a.get("avm") if isinstance(p0a, dict) else None

            # Extract detail blob (full property[0] dict)
            detail_blob = _get_p0(detail_result) if detail_ok else None

            # Merged raw json — replaces legacy AVM-only storage
            raw_merged = {
                "avm": avm_blob,
                "detail": detail_blob,
                "owner": _get_p0(owner_result) if owner_result else None,
                "mortgage": _get_p0(mortgage_result) if mortgage_result else None,
            }

            v, lo, hi = _extract_value_from_attom_avm(avm_result) if avm_ok else (None, None, None)

            if DEBUG and not logged_sample:
                logged_sample = True
                print(f"[ATTOM][DEBUG] sample avm.avm={_clip_json(avm_blob)} status={status}")

            enrichment_payload = {
                "falco": {"attom": {"no_result": False, "ts": now.isoformat().replace("+00:00", "Z")}},
                "meta": {"address1": address1, "address2": address2, "value_source": "avm.amount", "enrichment_status": status},
                "attom_avm": avm_blob,
            }

            write_obj: Dict[str, Any] = {
                "enrichment_json": _clip_json(enrichment_payload),
                "enrichment_confidence": None,
            }

            if v is not None:
                write_obj["estimated_value_low"] = float(lo if lo is not None else v)
                write_obj["estimated_value_high"] = float(hi if hi is not None else v)
                enriched_with_value += 1

            # Notion write only when AVM data is present (value fields require AVM)
            if stage2_source == "notion" and avm_ok:
                update_lead(page_id, build_extra_properties(write_obj))

            # Strict JSON serialization — never str(dict), never truncate with …
            print(f"[ATTOM][DEBUG] raw_merged_keys={list(raw_merged.keys())}")
            raw_json_str = json.dumps(raw_merged, ensure_ascii=False)
            print(f"[ATTOM][DEBUG] storing_raw_json_type= {type(raw_json_str)}")
            if _store.insert_attom_enrichment(lk, status, raw_json_str, v, lo, hi, None):
                stored_attom += 1
            enriched += 1
            if DEBUG:
                print(f"[ATTOM] {status} {address1} | {address2} value={v} low={lo} high={hi} avm_err={avm_err} detail_err={detail_err}")

        except Exception as e:
            errors += 1
            _store.insert_attom_enrichment(lk, "error", None, None, None, None, None)
            stored_attom += 1
            print(f"[ATTOM] ERROR {address1} | {address2}: {type(e).__name__}: {e}")

    summary = {
        "enriched_count": enriched,
        "enriched_with_value_count": enriched_with_value,
        "skipped_enrich_missing_address": skipped_missing_address,
        "skipped_enrich_already_enriched": skipped_already_enriched,
        "skipped_enrich_no_match": skipped_no_match,
        "skipped_enrich_cooldown": skipped_cooldown,
        "skipped_enrich_out_of_geo": skipped_out_of_geo,
        "skipped_enrich_institutional": skipped_institutional_count,
        "skipped_enrich_dup_in_run": skipped_dup_in_run,
        "errors": errors,
        "attom_call_count": client.call_count,
        "attom_call_count_by_path": client.call_count_by_path,
        "stored_attom": stored_attom,
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
