# src/enrichment/attom_enricher.py

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..notion_client import build_extra_properties, extract_page_fields, query_database, update_lead
from ..settings import get_dts_window, is_allowed_county
from .attom_client import AttomClient, AttomError

DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


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
# MAIN
# =========================================================

def run() -> Dict[str, int]:
    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0, "skipped_enrich_missing_key": 1}

    dts_min, dts_max = get_dts_window("ENRICH")
    max_enrich = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "10"))
    cooldown_hours = int(os.getenv("FALCO_ENRICH_NO_RESULT_COOLDOWN_HOURS", "72"))

    # cost controls
    skip_institutional = os.getenv("FALCO_SKIP_INSTITUTIONAL_ENRICH", "1").strip() not in ("", "0", "false", "False")
    mark_institutional = os.getenv("FALCO_MARK_INSTITUTIONAL_SKIP", "1").strip() not in ("", "0", "false", "False")

    client = AttomClient(api_key=api_key)

    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
            {"property": "Address", "rich_text": {"is_not_empty": True}},
        ]
    }

    pages = query_database(filter_obj, page_size=50, max_pages=10)

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

    now = datetime.now(timezone.utc)
    cooldown = timedelta(hours=cooldown_hours)

    logged_sample = False
    seen_addr_keys: set[str] = set()

    for page in pages:
        if enriched >= max_enrich:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        address = fields.get("address") or ""
        county = fields.get("county") or ""

        if not page_id:
            continue

        if county and not is_allowed_county(county):
            skipped_out_of_geo += 1
            continue

        if not str(address).strip():
            skipped_missing_address += 1
            continue

        if skip_institutional:
            reason = _detect_institutional(fields)
            if reason:
                skipped_institutional_count += 1
                if mark_institutional:
                    _mark_institutional_skip(page_id, reason=reason, now=now)
                if DEBUG:
                    print(f"[ATTOM][DEBUG] institutional skip page_id={page_id} matched_in={reason.get('matched_in')} kw={reason.get('keyword')}")
                continue

        if fields.get("estimated_value_low") is not None or fields.get("estimated_value_high") is not None:
            skipped_already_enriched += 1
            continue

        ej = str(fields.get("enrichment_json") or "").strip()
        marker = _read_no_result_marker(ej)
        if marker and (marker.get("no_result") is True) and marker.get("ts"):
            try:
                ts = datetime.fromisoformat(str(marker["ts"]).replace("Z", "+00:00"))
                if now - ts < cooldown:
                    skipped_cooldown += 1
                    continue
            except Exception:
                pass

        address1, address2 = _parse_address(address)
        if not address1 or not address2:
            skipped_missing_address += 1
            continue

        addr_key = _clean_spaces(f"{address1}|{address2}").lower()
        if addr_key in seen_addr_keys:
            skipped_dup_in_run += 1
            continue
        seen_addr_keys.add(addr_key)

        try:
            detail = client.property_detail(address1=address1, address2=address2)
            if _is_success_without_result(detail) or not _has_property(detail):
                skipped_no_match += 1
                write_obj = {
                    "enrichment_json": _clip_json(
                        {
                            "falco": {
                                "attom": {
                                    "no_result": True,
                                    "ts": now.isoformat().replace("+00:00", "Z"),
                                    "reason": "detail_no_result",
                                }
                            },
                            "meta": {"address1": address1, "address2": address2},
                        }
                    )
                }
                update_lead(page_id, build_extra_properties(write_obj))
                if DEBUG:
                    print(f"[ATTOM][DEBUG] no-result detail {address1} | {address2} msg={_status_msg(detail)}")
                continue

            avm = client.avm_detail(address1=address1, address2=address2)
            if _is_success_without_result(avm) or not _has_property(avm):
                skipped_no_match += 1
                write_obj = {
                    "enrichment_json": _clip_json(
                        {
                            "falco": {
                                "attom": {
                                    "no_result": True,
                                    "ts": now.isoformat().replace("+00:00", "Z"),
                                    "reason": "avm_no_result",
                                }
                            },
                            "meta": {"address1": address1, "address2": address2},
                        }
                    )
                }
                update_lead(page_id, build_extra_properties(write_obj))
                if DEBUG:
                    print(f"[ATTOM][DEBUG] no-result avm {address1} | {address2} msg={_status_msg(avm)}")
                continue

            v, lo, hi = _extract_value_from_attom_avm(avm)

            p0a = _get_p0(avm)
            avm_blob = p0a.get("avm") if isinstance(p0a, dict) else None

            if DEBUG and not logged_sample:
                logged_sample = True
                print(f"[ATTOM][DEBUG] sample avm.avm={_clip_json(avm_blob)}")

            enrichment_payload = {
                "falco": {"attom": {"no_result": False, "ts": now.isoformat().replace("+00:00", "Z")}},
                "meta": {"address1": address1, "address2": address2, "value_source": "avm.amount"},
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

            update_lead(page_id, build_extra_properties(write_obj))

            enriched += 1
            if DEBUG:
                print(f"[ATTOM] enriched {address1} | {address2} value={v} low={lo} high={hi}")

        except AttomError as e:
            skipped_no_match += 1
            if DEBUG:
                print(f"[ATTOM][DEBUG] error {address1} | {address2}: {e}")
        except Exception as e:
            errors += 1
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
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
