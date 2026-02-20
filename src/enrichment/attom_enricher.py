# src/enrichment/attom_enricher.py

from __future__ import annotations

import os
import json
import re
from typing import Any, Dict, Optional, Tuple, List

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


def _parse_address(addr: str) -> Tuple[str, str, str]:
    addr = str(addr or "").strip()
    if not addr:
        return "", "", ""

    zip_code = ""
    m = re.search(r"\b(\d{5})\b", addr)
    if m:
        zip_code = m.group(1)

    parts = [p.strip() for p in addr.split(",") if p.strip()]
    if len(parts) >= 2:
        address1 = parts[0]
        address2 = ", ".join(parts[1:]).replace(zip_code, "").strip()
        return address1, address2, zip_code

    return addr, "TN", zip_code


def run() -> Dict[str, int]:
    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0}

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
    skipped_no_match = 0

    for page in pages:
        if enriched >= max_enrich:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id")
        address = fields.get("address")

        if not address:
            continue

        address1, address2, postalcode = _parse_address(address)

        try:
            detail = client.property_detail(
                address1=address1,
                address2=address2,
                postalcode=postalcode,
            )

            if not detail or not detail.get("property"):
                skipped_no_match += 1
                continue

            avm = client.avm_detail(
                address1=address1,
                address2=address2,
                postalcode=postalcode,
            )

            avm_low = None
            avm_high = None

            try:
                prop = avm.get("property", [{}])[0]
                avm_obj = prop.get("avm", {})
                avm_low = _safe_float(avm_obj.get("valueRangeLow"))
                avm_high = _safe_float(avm_obj.get("valueRangeHigh"))
            except Exception:
                pass

            bundle = {
                "attom_detail": detail,
                "attom_avm": avm,
            }

            write_obj = {
                "estimated_value_low": avm_low,
                "estimated_value_high": avm_high,
                "enrichment_json": _clip_json(bundle),
            }

            props = build_extra_properties(write_obj)
            update_lead(page_id, props)

            enriched += 1

            if DEBUG:
                print(f"[ATTOM] enriched {address1} avm_low={avm_low}")

        except AttomError as e:
            if DEBUG:
                print(f"[ATTOM][DEBUG] error {address1}: {e}")
            skipped_no_match += 1

    summary = {
        "enriched_count": enriched,
        "skipped_enrich_no_match": skipped_no_match,
        "attom_call_count": client.call_count,
    }

    print(f"[ATTOM] summary {json.dumps(summary)}")
    return summary
