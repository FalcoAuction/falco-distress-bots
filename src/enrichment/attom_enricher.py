# src/enrichment/attom_enricher.py

from __future__ import annotations

import os
import json
import re
from datetime import datetime, timezone, timedelta
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


def _clip_json(obj: Any, max_chars: int = 1400) -> str:
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


def _clean_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _strip_zip(s: str) -> str:
    return re.sub(r"\b\d{5}(?:-\d{4})?\b", "", s or "").strip(" ,")


def _normalize_state(st: str) -> str:
    st = (st or "").strip().upper()
    if len(st) == 2 and st.isalpha():
        return st
    return "TN"


def _parse_address(addr: str) -> Tuple[str, str]:
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


def _extract_avm_amount(avm_payload: Dict[str, Any]) -> Optional[float]:
    """
    Your tenant shape:
      property[0].avm.amount
    """
    try:
        prop_list = avm_payload.get("property")
        if not (isinstance(prop_list, list) and prop_list):
            return None
        p0 = prop_list[0]
        if not isinstance(p0, dict):
            return None
        avm = p0.get("avm") or {}
        if not isinstance(avm, dict):
            return None
        return _safe_float(avm.get("amount"))
    except Exception:
        return None


def _read_no_result_marker(enrichment_json: str) -> Optional[Dict[str, Any]]:
    if not enrichment_json:
        return None
    try:
        m = re.search(r'("falco"\s*:\s*\{.*?\})', enrichment_json)
        if not m:
            return None
        frag = "{" + m.group(1) + "}"
        obj = json.loads(frag)
        return obj.get("falco", {}).get("attom")
    except Exception:
        return None


def run() -> Dict[str, int]:
    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0, "skipped_enrich_missing_key": 1}

    dts_min, dts_max = get_dts_window("ENRICH")

    # cost controls (keep tight while building)
    max_enrich = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "10"))
    cooldown_hours = int(os.getenv("FALCO_ENRICH_NO_RESULT_COOLDOWN_HOURS", "72"))

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
    errors = 0

    now = datetime.now(timezone.utc)
    cooldown = timedelta(hours=cooldown_hours)

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

        # Already enriched with value?
        if fields.get("estimated_value_low") or fields.get("estimated_value_high"):
            skipped_already_enriched += 1
            continue

        # cooldown skip if prior no-result
        ej = str(fields.get("enrichment_json") or "").strip()
        marker = _read_no_result_marker(ej)
        if marker and marker.get("no_result") is True and marker.get("ts"):
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

        try:
            detail = client.property_detail(address1=address1, address2=address2)
            if _is_success_without_result(detail) or not _has_property(detail):
                skipped_no_match += 1
                write_obj = {
                    "enrichment_json": _clip_json(
                        {
                            "falco": {"attom": {"no_result": True, "ts": now.isoformat().replace("+00:00", "Z"), "reason": "detail_no_result"}},
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
                            "falco": {"attom": {"no_result": True, "ts": now.isoformat().replace("+00:00", "Z"), "reason": "avm_no_result"}},
                            "meta": {"address1": address1, "address2": address2},
                        }
                    )
                }
                update_lead(page_id, build_extra_properties(write_obj))
                if DEBUG:
                    print(f"[ATTOM][DEBUG] no-result avm {address1} | {address2} msg={_status_msg(avm)}")
                continue

            amount = _extract_avm_amount(avm)

            write_obj: Dict[str, Any] = {
                "enrichment_json": _clip_json(
                    {
                        "falco": {"attom": {"no_result": False, "ts": now.isoformat().replace("+00:00", "Z")}},
                        "meta": {"address1": address1, "address2": address2},
                    }
                ),
                # tenant doesn't provide confidenceScore here
                "enrichment_confidence": None,
            }

            if amount is not None:
                write_obj["estimated_value_low"] = amount
                write_obj["estimated_value_high"] = amount
                enriched_with_value += 1

            update_lead(page_id, build_extra_properties(write_obj))

            enriched += 1
            if DEBUG:
                print(f"[ATTOM] enriched {address1} | {address2} avm_amount={amount}")

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
        "errors": errors,
        "attom_call_count": client.call_count,
        "attom_call_count_by_path": client.call_count_by_path,
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
