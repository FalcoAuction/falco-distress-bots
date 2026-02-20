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


def _parse_address(addr: str) -> Tuple[str, str]:
    """
    Returns (address1, address2) where:
      address1 = street line
      address2 = "City, ST" (ATTOM tenant requires both)

    Input examples:
      "1409 Scarcroft Lane, Nashville, TN 37221"
      "323 WITHAM COURT, GOODLETTSVILLE, TN 37072"
      "98 Randy Road, Madison, TN 37115"
    """
    addr = str(addr or "").strip()
    if not addr:
        return "", ""

    # Split by commas first (best case)
    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 3:
        street = parts[0]
        city = parts[1]
        # state part might include zip
        st_tokens = parts[2].split()
        st = st_tokens[0].upper() if st_tokens else "TN"
        if len(st) != 2:
            st = "TN"
        return street, f"{city}, {st}"

    if len(parts) == 2:
        street = parts[0]
        # second part might be "Nashville, TN 37209" collapsed or "Nashville TN 37209"
        tail = parts[1]
        tokens = tail.replace(",", " ").split()
        st = "TN"
        if len(tokens) >= 2 and len(tokens[-2]) == 2 and tokens[-2].isalpha():
            st = tokens[-2].upper()
            city = " ".join(tokens[:-2]).strip()
        elif len(tokens) >= 1:
            # sometimes it's just city
            city = " ".join([t for t in tokens if not (t.isdigit() and len(t) == 5)]).strip()
        else:
            city = tail
        city = city.strip()
        if not city:
            city = "Nashville"
        return street, f"{city}, {st}"

    # No commas: try heuristic "street ... city state zip"
    tokens = addr.split()
    # find state token
    st_idx = None
    for i, t in enumerate(tokens):
        if len(t) == 2 and t.isalpha() and t.upper() in ("TN", "KY", "AL", "MS", "GA", "NC", "SC", "VA", "AR"):
            st_idx = i
            break
    if st_idx is not None and st_idx >= 1:
        st = tokens[st_idx].upper()
        city = tokens[st_idx - 1]
        street = " ".join(tokens[: st_idx - 1]).strip()
        if street:
            return street, f"{city}, {st}"

    # Fallback: assume Nashville, TN (keeps system moving)
    return addr, "Nashville, TN"


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

        # skip if already enriched
        if (fields.get("estimated_value_low") or fields.get("estimated_value_high")) or str(fields.get("enrichment_json") or "").strip():
            skipped_already_enriched += 1
            continue

        address1, address2 = _parse_address(address)
        if not address1 or not address2:
            skipped_missing_address += 1
            continue

        try:
            detail = client.property_detail(address1=address1, address2=address2)
            avm = client.avm_detail(address1=address1, address2=address2)

            avm_low = None
            avm_high = None
            avm_conf = None

            try:
                prop = (avm.get("property") or [{}])[0]
                avm_obj = prop.get("avm") or {}
                avm_low = _safe_float(avm_obj.get("valueRangeLow"))
                avm_high = _safe_float(avm_obj.get("valueRangeHigh"))
                avm_conf = _safe_float(avm_obj.get("confidenceScore"))
            except Exception:
                pass

            bundle = {"attom_detail": detail, "attom_avm": avm, "meta": {"address1": address1, "address2": address2}}

            write_obj = {
                "estimated_value_low": avm_low,
                "estimated_value_high": avm_high,
                "enrichment_confidence": avm_conf,
                "enrichment_json": _clip_json(bundle),
            }

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
        "skipped_enrich_missing_address": skipped_missing_address,
        "skipped_enrich_already_enriched": skipped_already_enriched,
        "skipped_enrich_no_match": skipped_no_match,
        "errors": errors,
        "attom_call_count": client.call_count,
        "attom_call_count_by_path": client.call_count_by_path,
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
