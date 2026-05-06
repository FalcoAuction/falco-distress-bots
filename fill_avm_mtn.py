"""
One-shot AVM bulk filler for Middle TN leads missing property_value.

Uses the working resolver in src/bots/_assessor_sale_data.py:
  - Davidson  → padctn (with 5+ street-name variations)
  - Williamson → Inigo JSON
  - Rutherford → ArcGIS FeatureServer
  - Sumner/Wilson/Maury → TPAD
  - Montgomery → not covered (skipped for now)

Writes:
  - property_value (int, from `appraised`)
  - phone_metadata.assessor_lookup = {sale_date, sale_price, parcel, source, resolved_at}
    (so HMDA enricher can year-anchor / sale-anchor on these)

Run:
  python fill_avm_mtn.py [--dry-run] [--limit N]
"""
import argparse
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.bots._base import _supabase
from src.bots._assessor_sale_data import resolve

FOCUS_COUNTIES = {
    "davidson", "williamson", "sumner", "rutherford", "wilson",
    "maury", "montgomery",
}


def _norm_county(c: Optional[str]) -> str:
    if not c:
        return ""
    return c.lower().strip().replace(" county", "").strip()


def _candidates(client) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for table in ("homeowner_requests", "homeowner_requests_staging"):
        page = 0
        while True:
            try:
                q = (
                    client.table(table)
                    .select(
                        "id, property_address, county, owner_name_records, "
                        "full_name, property_value, phone_metadata"
                    )
                    .is_("property_value", "null")
                    .not_.is_("property_address", "null")
                    .range(page * 1000, (page + 1) * 1000 - 1)
                    .execute()
                )
                rows = getattr(q, "data", None) or []
                if not rows:
                    break
                for r in rows:
                    if _norm_county(r.get("county")) not in FOCUS_COUNTIES:
                        continue
                    r["__table__"] = table
                    out.append(r)
                if len(rows) < 1000:
                    break
                page += 1
            except Exception as e:
                print(f"[warn] candidate fetch {table} page {page}: {e}")
                break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=500)
    args = ap.parse_args()

    client = _supabase()
    if client is None:
        print("[fatal] no supabase client")
        sys.exit(1)

    candidates = _candidates(client)
    print(f"[info] {len(candidates)} MTN leads missing property_value")

    by_county: Dict[str, int] = {}
    for r in candidates:
        c = _norm_county(r.get("county"))
        by_county[c] = by_county.get(c, 0) + 1
    for c, n in sorted(by_county.items(), key=lambda x: -x[1]):
        print(f"  {c:14s} {n}")

    enriched = 0
    not_found = 0
    skipped_no_resolver = 0
    update_failed = 0

    for i, lead in enumerate(candidates[: args.limit]):
        addr = lead["property_address"]
        cty = _norm_county(lead.get("county"))
        owner = lead.get("owner_name_records") or lead.get("full_name") or ""

        # Resolve
        try:
            data = resolve(addr, cty, owner)
        except Exception as e:
            print(f"  [{i+1}/{args.limit}] {addr[:55]:58s} ERROR: {e}")
            not_found += 1
            continue

        appraised = data.get("appraised")
        # _resolve_tpad sometimes returns string; coerce
        if isinstance(appraised, str):
            try:
                appraised = float(
                    appraised.replace("$", "").replace(",", "").strip()
                )
            except Exception:
                appraised = None

        if not appraised or appraised <= 0:
            note = data.get("source", "?")
            print(f"  [{i+1}/{args.limit}] {addr[:55]:58s} no AVM ({note})")
            if note == "no_resolver":
                skipped_no_resolver += 1
            else:
                not_found += 1
            time.sleep(0.4)
            continue

        # Build update
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["assessor_lookup"] = {
            "source": data.get("source"),
            "appraised": appraised,
            "sale_date": data.get("sale_date"),
            "sale_price": data.get("sale_price"),
            "parcel": data.get("parcel") or data.get("account_id"),
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }

        update = {
            "property_value": int(round(appraised)),
            "phone_metadata": pm,
        }

        # Live table is int, staging is float — both accept int
        if args.dry_run:
            print(
                f"  [{i+1}/{args.limit}] {addr[:55]:58s} "
                f"AVM=${int(appraised):,} (DRY)"
            )
            enriched += 1
            time.sleep(0.4)
            continue

        try:
            client.table(lead["__table__"]).update(update).eq(
                "id", lead["id"]
            ).execute()
            enriched += 1
            extras = []
            if data.get("sale_date"):
                extras.append(f"sold {data['sale_date']}")
            if data.get("sale_price"):
                extras.append(f"${int(data['sale_price']):,}")
            extra_str = (" · " + ", ".join(extras)) if extras else ""
            print(
                f"  [{i+1}/{args.limit}] {addr[:55]:58s} "
                f"AVM=${int(appraised):,}{extra_str}"
            )
        except Exception as e:
            update_failed += 1
            print(
                f"  [{i+1}/{args.limit}] {addr[:55]:58s} "
                f"update_failed: {e}"
            )
        time.sleep(0.4)

    print()
    print(f"enriched={enriched} not_found={not_found} "
          f"no_resolver={skipped_no_resolver} update_failed={update_failed}")


if __name__ == "__main__":
    main()
