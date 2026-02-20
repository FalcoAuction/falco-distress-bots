# src/enrichment/attom_enricher.py
"""
Stage 2 enrichment (ATTOM-first, Premium tier).

Behavior:
- Safe no-op if FALCO_ATTOM_API_KEY missing
- Only enrich pre-filtered, eligible leads
- Non-destructive Notion updates (handled by notion_client.update_lead)
- Strict cost controls:
    - FALCO_MAX_ENRICH_PER_RUN (default 20)
    - FALCO_MAX_COMPS_PER_RUN (default 8)  [Tier-2 calls]
- Logs:
    enriched_count
    skipped_enrich_missing_address
    skipped_enrich_already_enriched
    skipped_enrich_already_graded
    skipped_enrich_no_match
    attom_call_count
"""

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
from .attom_client import AttomClient, AttomError, _clip_json


DEBUG = os.getenv("FALCO_ENRICH_DEBUG", "").strip() not in ("", "0", "false", "False")


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)):
            return float(x)
        s = str(x).strip().replace("$", "").replace(",", "")
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def _safe_int(x: Any) -> Optional[int]:
    f = _safe_float(x)
    if f is None:
        return None
    try:
        return int(round(f))
    except Exception:
        return None


def _rt_clean(s: Any) -> str:
    return str(s or "").strip()


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _parse_address_components(addr: str) -> Tuple[str, str, str]:
    """
    Returns (address1, address2, postalcode)

    Examples:
      "123 Main St, Nashville, TN 37209" -> ("123 Main St", "Nashville, TN", "37209")
      "123 Main St Nashville TN 37209"   -> heuristic
    """
    a = _norm_ws(addr)
    if not a:
        return ("", "", "")

    zip_code = ""
    m = re.search(r"\b(\d{5})(?:-\d{4})?\b", a)
    if m:
        zip_code = m.group(1)

    parts = [p.strip() for p in a.split(",") if p.strip()]
    if len(parts) >= 2:
        address1 = parts[0]
        tail = ", ".join(parts[1:]).strip()
        tail2 = re.sub(r"\b\d{5}(?:-\d{4})?\b", "", tail).strip()
        tail2 = _norm_ws(tail2).strip(", ")
        address2 = tail2 or "TN"
        return (address1, address2, zip_code)

    tokens = a.split(" ")
    if len(tokens) >= 2:
        if re.fullmatch(r"\d{5}", tokens[-1]):
            zip_code = tokens[-1]
            if tokens[-2].upper() in ("TN", "TENN", "TENNESSEE"):
                return (" ".join(tokens[:-2]).strip(), "TN", zip_code)
            return (" ".join(tokens[:-1]).strip(), "TN", zip_code)

    return (a, "TN", zip_code)


def _extract_owner_and_mailing(prop_owner: Optional[Dict[str, Any]], prop_detail: Optional[Dict[str, Any]]) -> Tuple[str, str]:
    owner_name = ""
    mailing = ""

    if isinstance(prop_owner, dict):
        owner = prop_owner.get("owner")
        if isinstance(owner, dict):
            owner_name = _rt_clean(owner.get("name") or owner.get("owner1FullName") or owner.get("ownerName"))
            mail = owner.get("mailingAddress") or owner.get("mailing") or owner.get("mailingaddress")
            if isinstance(mail, dict):
                mailing = _norm_ws(
                    " ".join(
                        [
                            str(mail.get(k) or "").strip()
                            for k in ("line1", "line2", "city", "state", "postal1")
                        ]
                    ).strip()
                )
            elif isinstance(mail, str):
                mailing = _norm_ws(mail)
        if not owner_name:
            owner_name = _rt_clean(prop_owner.get("ownerName") or prop_owner.get("owner_name"))

    if not owner_name and isinstance(prop_detail, dict):
        o = prop_detail.get("owner") or prop_detail.get("ownerName")
        if isinstance(o, dict):
            owner_name = _rt_clean(o.get("name") or o.get("owner1FullName") or o.get("ownerName"))
        elif isinstance(o, str):
            owner_name = _rt_clean(o)

    return (owner_name, mailing)


def _extract_facts(prop_detail: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if not isinstance(prop_detail, dict):
        return out

    b = prop_detail.get("building") or {}
    if isinstance(b, dict):
        out["beds"] = _safe_int(b.get("rooms") or b.get("beds") or b.get("bedrooms"))
        out["baths"] = _safe_float(b.get("bathsTotal") or b.get("baths") or b.get("bathrooms"))
        out["sqft"] = _safe_int(b.get("size") or b.get("livingSize") or b.get("livingSqFt") or b.get("sqft"))
        out["year_built"] = _safe_int(b.get("yearBuilt") or b.get("yearbuilt"))

    a = prop_detail.get("assessment") or prop_detail.get("tax") or {}
    if isinstance(a, dict):
        out["tax_assessed_value"] = _safe_float(
            a.get("assessed") or a.get("assdTtlValue") or a.get("totalAssessedValue")
        )

    s = prop_detail.get("sale") or prop_detail.get("sales") or {}
    if isinstance(s, dict):
        dt = s.get("saleDate") or s.get("lastSaleDate") or s.get("date")
        if isinstance(dt, str):
            if "T" in dt:
                dt = dt.split("T")[0]
            if re.match(r"^\d{4}-\d{2}-\d{2}$", dt):
                out["last_sale_date"] = dt

    return out


def _extract_mortgage_summary(prop_mort: Optional[Dict[str, Any]]) -> Tuple[str, Optional[float]]:
    if not isinstance(prop_mort, dict):
        return ("", None)

    mtg = prop_mort.get("mortgage") or prop_mort.get("mortgages") or prop_mort.get("loan") or None
    balances: List[float] = []
    indicators: List[str] = []

    if isinstance(mtg, list):
        for m in mtg:
            if not isinstance(m, dict):
                continue
            amt = _safe_float(m.get("amount") or m.get("loanAmount") or m.get("originalBalance") or m.get("balance"))
            if amt:
                balances.append(float(amt))
            lt = _rt_clean(m.get("loanType") or m.get("type") or "")
            if lt:
                indicators.append(lt)
    elif isinstance(mtg, dict):
        amt = _safe_float(mtg.get("amount") or mtg.get("loanAmount") or mtg.get("originalBalance") or mtg.get("balance"))
        if amt:
            balances.append(float(amt))
        lt = _rt_clean(mtg.get("loanType") or mtg.get("type") or "")
        if lt:
            indicators.append(lt)

    total = sum(balances) if balances else None
    text = ", ".join([x for x in indicators if x])[:500]
    return (text, total)


def _extract_avm(avm_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not isinstance(avm_payload, dict):
        return (None, None, None)

    p = AttomClient.first_property(avm_payload) or avm_payload
    avm = None
    if isinstance(p, dict):
        avm = p.get("avm") or p.get("attomAvm") or p.get("valuation") or p.get("avmDetail")
    if not isinstance(avm, dict):
        avm = avm_payload.get("avm") if isinstance(avm_payload.get("avm"), dict) else None
    if not isinstance(avm, dict):
        return (None, None, None)

    low = _safe_float(avm.get("valueRangeLow") or avm.get("avmValueRangeLow") or avm.get("low"))
    high = _safe_float(avm.get("valueRangeHigh") or avm.get("avmValueRangeHigh") or avm.get("high"))
    conf = _safe_float(avm.get("confidenceScore") or avm.get("confidence") or avm.get("conf"))
    return (low, high, conf)


def _extract_comps(payload: Dict[str, Any]) -> List[dict]:
    comps: List[dict] = []
    if not isinstance(payload, dict):
        return comps

    arr = payload.get("property") or payload.get("comparables") or payload.get("comps") or []
    if isinstance(arr, dict):
        arr = [arr]
    if not isinstance(arr, list):
        return comps

    for it in arr:
        if not isinstance(it, dict):
            continue

        addr = ""
        ad = it.get("address")
        if isinstance(ad, dict):
            addr = _norm_ws(
                " ".join([str(ad.get(k) or "").strip() for k in ("line1", "line2", "city", "state", "postal1")]).strip()
            )
        elif isinstance(ad, str):
            addr = _norm_ws(ad)

        sale = it.get("sale") if isinstance(it.get("sale"), dict) else {}
        bld = it.get("building") if isinstance(it.get("building"), dict) else {}

        sale_price = _safe_float(sale.get("saleAmt") or sale.get("saleAmount") or sale.get("price") or it.get("sale_price"))
        sale_date = sale.get("saleDate") or sale.get("date") or it.get("sale_date")
        if isinstance(sale_date, str) and "T" in sale_date:
            sale_date = sale_date.split("T")[0]
        if not isinstance(sale_date, str):
            sale_date = ""

        sqft = _safe_float(bld.get("size") or bld.get("livingSize") or bld.get("sqft") or it.get("sqft"))
        dist = _safe_float(it.get("distance") or it.get("distanceMiles") or it.get("distance_miles"))

        comps.append(
            {
                "sale_price": sale_price,
                "sqft": sqft,
                "sale_date": sale_date,
                "distance_miles": dist,
                "address": addr,
            }
        )

    return comps


def _should_do_tier2(dts: Optional[int], avm_low: Optional[float], mortgage_total: Optional[float], avm_conf: Optional[float]) -> bool:
    if dts is None:
        return False
    if dts < 0 or dts > int(os.getenv("FALCO_TIER2_MAX_DTS", "60")):
        return False

    conf_floor = _safe_float(os.getenv("FALCO_TIER2_MIN_AVM_CONF", "60"))
    if avm_conf is not None and conf_floor is not None and avm_conf < conf_floor:
        return False

    if avm_low and mortgage_total and avm_low > 0:
        equity_pct = (avm_low - mortgage_total) / avm_low
        if equity_pct >= float(os.getenv("FALCO_TIER2_MIN_EQUITY_PCT", "0.15")):
            return True
        return False

    if dts <= 35:
        return True
    return False


def run() -> Dict[str, int]:
    api_key = os.getenv("FALCO_ATTOM_API_KEY", "").strip()
    if not api_key:
        print("[ATTOM] No FALCO_ATTOM_API_KEY set. Skipping ATTOM enrichment (safe no-op).")
        return {"enriched_count": 0, "attom_call_count": 0, "skipped_enrich_missing_key": 1}

    dts_min, dts_max = get_dts_window("ENRICH")
    max_enrich = int(os.getenv("FALCO_MAX_ENRICH_PER_RUN", "20"))
    max_tier2 = int(os.getenv("FALCO_MAX_COMPS_PER_RUN", "8"))

    radius = float(os.getenv("FALCO_COMPS_RADIUS_MILES", "1.0"))
    lookback = int(os.getenv("FALCO_COMPS_LOOKBACK_DAYS", "180"))
    comps_pagesize = int(os.getenv("FALCO_COMPS_PAGESIZE", "10"))

    client = AttomClient(api_key=api_key)

    filter_obj = {
        "and": [
            {"property": "Days to Sale", "number": {"greater_than_or_equal_to": dts_min}},
            {"property": "Days to Sale", "number": {"less_than_or_equal_to": dts_max}},
            {"property": "Address", "rich_text": {"is_not_empty": True}},
        ]
    }
    pages = query_database(
        filter_obj,
        page_size=50,
        sorts=[{"property": "Sale Date", "direction": "ascending"}],
        max_pages=10,
    )

    enriched = 0
    tier2_done = 0
    skipped_missing_address = 0
    skipped_already_enriched = 0
    skipped_already_graded = 0
    skipped_no_match = 0
    errors = 0

    for page in pages:
        if enriched >= max_enrich:
            break

        fields = extract_page_fields(page)
        page_id = fields.get("page_id") or ""
        if not page_id:
            continue

        addr = _rt_clean(fields.get("address"))
        if not addr:
            skipped_missing_address += 1
            continue

        if _rt_clean(fields.get("enrichment_json")) or fields.get("estimated_value_low") or fields.get("estimated_value_high"):
            skipped_already_enriched += 1
            continue

        if _rt_clean(fields.get("grade")) or (fields.get("grade_score") or 0) > 0:
            skipped_already_graded += 1
            continue

        address1, address2, postalcode = _parse_address_components(addr)
        if not address1:
            skipped_missing_address += 1
            continue

        dts = fields.get("days_to_sale")
        try:
            dts_int = int(round(float(dts))) if dts is not None else None
        except Exception:
            dts_int = None

        try:
            detail = client.property_detail(address1=address1, address2=address2, postalcode=postalcode)
            if not AttomClient.status_ok(detail) and not AttomClient.first_property(detail):
                skipped_no_match += 1
                continue
            prop_detail = AttomClient.first_property(detail) or {}

            owner_payload = {}
            try:
                owner_payload = client.property_detail_owner(address1=address1, address2=address2, postalcode=postalcode)
            except Exception:
                owner_payload = {}
            prop_owner = AttomClient.first_property(owner_payload) or {}

            mort_payload = {}
            try:
                mort_payload = client.property_detail_mortgage(address1=address1, address2=address2, postalcode=postalcode)
            except Exception:
                mort_payload = {}
            prop_mort = AttomClient.first_property(mort_payload) or {}

            avm_payload = {}
            try:
                avm_payload = client.avm_detail(address1=address1, address2=address2, postalcode=postalcode)
            except Exception:
                avm_payload = {}

            avm_low, avm_high, avm_conf = _extract_avm(avm_payload)

            owner_name, mailing = _extract_owner_and_mailing(prop_owner, prop_detail)
            facts = _extract_facts(prop_detail)
            loan_indicators, mortgage_total = _extract_mortgage_summary(prop_mort)

            absentee_flag = None
            if mailing:
                mlow = mailing.lower()
                if " tn" not in mlow and "tennessee" not in mlow:
                    absentee_flag = True
                else:
                    absentee_flag = False

            enrich_conf = avm_conf if avm_conf is not None else 55.0

            payload_bundle: Dict[str, Any] = {
                "attom": {
                    "property_detail": detail,
                    "property_owner": owner_payload,
                    "property_mortgage": mort_payload,
                    "avm": avm_payload,
                },
                "meta": {
                    "address1": address1,
                    "address2": address2,
                    "postalcode": postalcode,
                    "dts": dts_int,
                },
            }

            if tier2_done < max_tier2 and _should_do_tier2(dts_int, avm_low, mortgage_total, avm_conf):
                home_eq_payload = None
                comps_payload = None
                comps_list: List[dict] = []

                try:
                    home_eq_payload = client.valuation_home_equity(address1=address1, address2=address2, postalcode=postalcode)
                except Exception:
                    home_eq_payload = None

                try:
                    comps_payload = client.sales_comparables(
                        address1=address1,
                        address2=address2,
                        postalcode=postalcode,
                        radius_miles=radius,
                        days_back=lookback,
                        pagesize=comps_pagesize,
                    )
                except Exception:
                    comps_payload = None

                if comps_payload:
                    comps_list = _extract_comps(comps_payload)

                payload_bundle["comps"] = comps_list[:20]
                if home_eq_payload is not None:
                    payload_bundle["attom"]["homeequity"] = home_eq_payload
                if comps_payload is not None:
                    payload_bundle["attom"]["salescomparables"] = comps_payload

                tier2_done += 1

            enrichment_json = _clip_json(payload_bundle, max_chars=1800)

            write_obj: Dict[str, Any] = {
                "owner_name": owner_name,
                "mailing_address": mailing,
                "absentee_flag": absentee_flag,
                "beds": facts.get("beds"),
                "baths": facts.get("baths"),
                "sqft": facts.get("sqft"),
                "year_built": facts.get("year_built"),
                "estimated_value_low": avm_low,   # baseline for scoring uses AVM low
                "estimated_value_high": avm_high,
                "loan_indicators": loan_indicators,
                "last_sale_date": facts.get("last_sale_date"),
                "tax_assessed_value": facts.get("tax_assessed_value"),
                "enrichment_confidence": enrich_conf,
                "enrichment_json": enrichment_json,
            }

            props = build_extra_properties(write_obj)
            update_lead(page_id, props)
            enriched += 1

            if DEBUG:
                print(
                    f"[ATTOM] enriched page_id={page_id} avm_low={avm_low} avm_high={avm_high} conf={avm_conf} tier2={'yes' if 'comps' in payload_bundle else 'no'}"
                )

        except AttomError:
            skipped_no_match += 1
        except Exception as e:
            errors += 1
            print(f"[ATTOM] ERROR enriching page_id={page_id} addr='{addr}': {type(e).__name__}: {e}")

    summary = {
        "enriched_count": enriched,
        "tier2_count": tier2_done,
        "skipped_enrich_missing_address": skipped_missing_address,
        "skipped_enrich_already_enriched": skipped_already_enriched,
        "skipped_enrich_already_graded": skipped_already_graded,
        "skipped_enrich_no_match": skipped_no_match,
        "errors": errors,
        "attom_call_count": client.call_count,
        "attom_call_count_by_path": client.call_count_by_path,
    }
    print(f"[ATTOM] summary {json.dumps(summary, ensure_ascii=False)}")
    return summary
