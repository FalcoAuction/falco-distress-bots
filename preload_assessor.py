"""
Pre-load assessor sale_date / sale_price / appraised into
phone_metadata.assessor_lookup for every Middle TN lead currently
showing wide-window HMDA confidence (sale_anchored=false AND
year_anchored=false).

Why: HMDA enricher's inline assessor call sometimes fails silently
(timeouts, transient padctn 5xx). Persisting assessor data first lets
the next HMDA run year-anchor cleanly, upgrading wide_multi (0.45)
leads to year_anchored (0.60-0.70) — which is what the dialer needs
to mark them mortgageDefensible.

Run:
  python preload_assessor.py [--dry-run] [--limit N]
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
    """Wide-multi MTN leads (HMDA hit but no anchor) OR leads missing
    any assessor data entirely."""
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
                    pm = r.get("phone_metadata") or {}
                    if not isinstance(pm, dict):
                        pm = {}
                    # Already have assessor data with a sale_date?
                    al = pm.get("assessor_lookup") or {}
                    if isinstance(al, dict) and al.get("sale_date"):
                        continue
                    # Eligible if:
                    #   (a) HMDA wide (no anchor) — would benefit from sale_date
                    #   (b) Missing AVM
                    sig = pm.get("mortgage_signal") or {}
                    is_wide_hmda = (
                        isinstance(sig, dict)
                        and sig.get("source") == "hmda_match"
                        and not sig.get("sale_anchored")
                        and not sig.get("year_anchored")
                    )
                    needs_avm = not r.get("property_value")
                    if not (is_wide_hmda or needs_avm):
                        continue
                    r["__table__"] = table
                    out.append(r)
                if len(rows) < 1000:
                    break
                page += 1
            except Exception as e:
                print(f"[warn] candidate fetch {table}: {e}")
                break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--limit", type=int, default=200)
    args = ap.parse_args()

    client = _supabase()
    if client is None:
        print("[fatal] no supabase client")
        sys.exit(1)

    candidates = _candidates(client)
    print(f"[info] {len(candidates)} candidates needing assessor data")

    by_county: Dict[str, int] = {}
    for r in candidates:
        c = _norm_county(r.get("county"))
        by_county[c] = by_county.get(c, 0) + 1
    for c, n in sorted(by_county.items(), key=lambda x: -x[1]):
        print(f"  {c:14s} {n}")

    enriched = 0
    not_found = 0
    update_failed = 0
    new_avm = 0
    new_sale_date = 0

    for i, lead in enumerate(candidates[: args.limit]):
        addr = lead["property_address"]
        cty = _norm_county(lead.get("county"))
        owner = lead.get("owner_name_records") or lead.get("full_name") or ""

        try:
            data = resolve(addr, cty, owner)
        except Exception as e:
            print(f"  [{i+1}] {addr[:55]:58s} ERROR: {e}")
            not_found += 1
            time.sleep(0.6)
            continue

        appraised = data.get("appraised")
        if isinstance(appraised, str):
            try:
                appraised = float(
                    appraised.replace("$", "").replace(",", "").strip()
                )
            except Exception:
                appraised = None

        sale_date = data.get("sale_date")
        sale_price = data.get("sale_price")
        if not appraised and not sale_date:
            print(f"  [{i+1}] {addr[:55]:58s} no data (src={data.get('source')})")
            not_found += 1
            time.sleep(0.6)
            continue

        # Build update
        pm = lead.get("phone_metadata") or {}
        if not isinstance(pm, dict):
            pm = {}
        pm["assessor_lookup"] = {
            "source": data.get("source"),
            "appraised": appraised,
            "sale_date": sale_date,
            "sale_price": sale_price,
            "deed_reference": data.get("deed_reference"),
            "parcel": data.get("parcel") or data.get("account_id"),
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        }

        update: Dict[str, Any] = {"phone_metadata": pm}

        # Backfill property_value if missing
        if not lead.get("property_value") and appraised:
            update["property_value"] = int(round(appraised))
            new_avm += 1

        if sale_date:
            new_sale_date += 1

        if args.dry_run:
            tags = []
            if appraised and not lead.get("property_value"):
                tags.append(f"AVM=${int(appraised):,}")
            if sale_date:
                tags.append(f"sold {sale_date}")
            if sale_price:
                tags.append(f"${int(sale_price):,}")
            extras = " · ".join(tags) if tags else "no_change"
            print(f"  [{i+1}] {addr[:55]:58s} {extras} (DRY)")
            enriched += 1
            time.sleep(0.6)
            continue

        try:
            client.table(lead["__table__"]).update(update).eq(
                "id", lead["id"]
            ).execute()
            enriched += 1
            tags = []
            if "property_value" in update:
                tags.append(f"AVM=${update['property_value']:,}")
            if sale_date:
                tags.append(f"sold {sale_date}")
            if sale_price:
                tags.append(f"${int(sale_price):,}")
            extras = " · ".join(tags) if tags else "(meta only)"
            print(f"  [{i+1}] {addr[:55]:58s} {extras}")
        except Exception as e:
            update_failed += 1
            print(f"  [{i+1}] {addr[:55]:58s} update_failed: {e}")
        time.sleep(0.6)

    print()
    print(
        f"enriched={enriched} not_found={not_found} "
        f"new_avm={new_avm} new_sale_date={new_sale_date} "
        f"update_failed={update_failed}"
    )


if __name__ == "__main__":
    main()
