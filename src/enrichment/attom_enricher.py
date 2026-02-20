# src/enrichment/attom_enricher.py

from __future__ import annotations

import os
import json
import re
from typing import Any, Dict, Optional, Tuple

from ..notion_client import (
    query_database,
    extract_page_fields,
    build_extra_properties,
    update_lead,
)
from ..settings import get_dts_window
from .attom_client import AttomClient, AttomError

DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


def _clip_json(obj: Any, max_chars: int = 1800) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False)
    except Exception:
        s = str(obj)
    if len(s) <= max_chars:
        return s
    return s[: max_chars - 1] + "…"


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


def _looks_like_no_result(payload: Dict[str, Any]) -> bool:
    msg = (_status_msg(payload) or "").lower()
    if "successwithoutresult" in msg:
        return True
    # some responses return empty property[]
    prop = payload.get("property")
    if isinstance(prop, list) and len(prop) == 0:
        return True
    return False


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_zip(s: str) -> str:
    return re.sub(r"\b\d{5}(?:-\d{4})?\b", "", s or "").strip(" ,")


def _normalize_state(st: str) -> str:
    st = (st or "").strip().upper()
    if len(st) == 2 and st.isalpha():
        return st
    # fall back (we're TN-only right now)
    return "TN"


def _parse_address(addr: str) -> Tuple[str, str]:
    """
    Returns (address1, address2='City, ST') for ATTOM tenant.

    Handles messy inputs like:
      "409-A Eastboro Drive, Nashville, Tennessee 37209, Nashville, TN 37209"
      "98 Randy Road,\r\nMadison, TN, Madison, TN 37115"
      "2654 Fizer Road Memphis"  (tries to infer city if last token is a known city word)
    """
    raw = _clean_spaces(str(addr or ""))
    if not raw:
        return "", ""

    # Normalize commas and remove duplicate whitespace/newlines
    raw = raw.replace("\n", " ").replace("\r", " ")
    raw = _clean_spaces(raw)

    # Split by commas
    parts = [p.strip() for p in raw.split(",") if p.strip()]

    # Helper: if we see "Tennessee" convert to TN
    def fix_state_token(token: str) -> str:
        t = (token or "").strip()
        if t.lower() == "tennessee":
            return "TN"
        return t

    # Try canonical: street, city, state/zip...
    if len(parts) >= 3:
        street = parts[0]
        city = parts[1]
        st_part = _strip_zip(parts[2])
        st_tokens = [fix_state_token(t) for t in st_part.replace(",", " ").split() if t.strip()]
        st = _normalize_state(st_tokens[0] if st_tokens else "TN")

        # If city itself contains "TN" etc, clean it
        city = _strip_zip(city)
        city = re.sub(r"\bTN\b", "", city, flags=re.I).strip(" ,")
        city = _clean_spaces(city)

        if street and city:
            return street, f"{city}, {st}"

    # If two parts: street + (city/state/zip)
    if len(parts) == 2:
        street = parts[0]
        tail = _strip_zip(parts[1])
        tail_tokens = [fix_state_token(t) for t in tail.replace(",", " ").split() if t.strip()]

        st = "TN"
        city_tokens = tail_tokens[:]
        # find state token position if present
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

    # No commas: heuristic
    tokens = raw.split()
    # look for state token
    st_idx = None
    for i, t in enumerate(tokens):
        tt = fix_state_token(t).upper()
        if tt in ("TN", "KY", "AL", "MS", "GA", "NC", "SC", "VA", "AR"):
            st_idx = i
            break
    if st_idx is not None and st_idx >= 1:
        st = _normalize_state(tokens[st_idx])
        # assume token before state is city
        city = tokens[st_idx - 1]
        street = " ".join(tokens[: st_idx - 1]).strip()
        if street and city:
            return street, f"{city}, {st}"

    # Last-resort: if last token is alphabetic, treat it as city (common bad input)
    if len(tokens) >= 3 and tokens[-1].isalpha():
        city = tokens[-1]
        street = " ".join(tokens[:-1]).strip()
        return street, f"{city}, TN"

    # fallback
    return raw, "Nashville, TN"


def _extract_avm_low_high_conf(avm_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    try:
        prop_list = avm_payload.get("property")
        if isinstance(prop_list, list) and prop_list:
            prop = prop_list[0]
        else:
            prop = {}
        avm = prop.get("avm") if isinstance(prop, dict) else {}
        if not isinstance(avm, dict):
            avm = {}
        low = _safe_float(avm.get("valueRangeLow"))
        high = _safe_float(avm.get("valueRangeHigh"))
        conf = _safe_float(avm.get("confidenceScore"))
        return low, high, conf
    except Exception:
        return None, None, None


def run() -> Dict[str, int]:
    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0, "skipped_enrich_missing_key": 1}

    dts_min, dts_max = get_dts_window("ENRICH")
    max_enrich = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "20"))

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
    errors = 0

    for page in pages:
        if enriched >= max_enrich:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        address = fields.get("address") or ""

        if not page_id:
            continue

        if not str(address).strip():
            skipped_missing_address += 1
            continue

        # skip if already enriched WITH VALUE
        if fields.get("estimated_value_low") or fields.get("estimated_value_high"):
            skipped_already_enriched += 1
            continue

        address1, address2 = _parse_address(address)
        if not address1 or not address2:
            skipped_missing_address += 1
            continue

        try:
            detail = client.property_detail(address1=address1, address2=address2)
            if _looks_like_no_result(detail):
                skipped_no_match += 1
                if DEBUG:
                    print(f"[ATTOM][DEBUG] no-result detail {address1} | {address2} msg={_status_msg(detail)}")
                continue

            avm = client.avm_detail(address1=address1, address2=address2)
            if _looks_like_no_result(avm):
                skipped_no_match += 1
                if DEBUG:
                    print(f"[ATTOM][DEBUG] no-result avm {address1} | {address2} msg={_status_msg(avm)}")
                continue

            avm_low, avm_high, avm_conf = _extract_avm_low_high_conf(avm)

            bundle = {
                "attom_detail": detail,
                "attom_avm": avm,
                "meta": {"address1": address1, "address2": address2},
            }

            write_obj: Dict[str, Any] = {
                "enrichment_json": _clip_json(bundle),
                "enrichment_confidence": avm_conf,
            }

            # Only write value fields if we actually got them
            if avm_low is not None or avm_high is not None:
                write_obj["estimated_value_low"] = avm_low
                write_obj["estimated_value_high"] = avm_high
                enriched_with_value += 1

            props = build_extra_properties(write_obj)
            update_lead(page_id, props)

            enriched += 1
            if DEBUG:
                print(f"[ATTOM] enriched {address1} | {address2} avm_low={avm_low} conf={avm_conf}")

        except AttomError as e:
            skipped_no_match += 1
            if DEBUG:
                print(f"[ATTOM][DEBUG] no-match {address1} | {address2}: {e}")
        except Exception as e:
            errors += 1
            print(f"[ATTOM] ERROR {address1} | {address2}: {type(e).__name__}: {e}")

    summary = {
        "enriched_count": enriched,
        "enriched_with_value_count": enriched_with_value,
        "skipped_enrich_missing_address": skipped_missing_address,
        "skipped_enrich_already_enriched": skipped_already_enriched,
        "skipped_enrich_no_match": skipped_no_match,
        "errors": errors,
        "attom_call_count": client.call_count,
        "attom_call_count_by_path": client.call_count_by_path,
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
