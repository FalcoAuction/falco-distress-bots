"""
Boat-owner skip trace: phone numbers for the owners named in marina-lien
notices (tn_boat_notice), so the boat lane is owner-callable.

PAID and PARKED. This is an Enformion Contact Enrich lookup (charged on a
match only) and sits in _paid.PAID_BOTS, so the runner skips it until
FALCO_PAID_DATA_ENABLED=1 and the Enformion credentials are set. It turns on
with the rest of paid data, not on its own.

The notices give the owner's name and the marina's town, not the owner's
home address, so the lookup sends name + area only. Every number it writes
is tagged match_basis = "name_and_marina_area" so the caller confirms the
person before pitching. Only high-value boats (lane grade set) with a
person-named owner and no phone yet are looked up.

Distress type: N/A (utility skip-tracer).
"""

from __future__ import annotations

import os
import re
import sys
from typing import Any, Dict, List

from .enformion_skip_trace_bot import BUSINESS_RE, EnformionSkipTraceBot

DEFAULT_MAX_PER_RUN = 25
AREA_RE = re.compile(r"(?:^|,|\bin)\s*([A-Z][A-Za-z.'\-]+(?:\s+[A-Z][A-Za-z.'\-]+){0,2})\s*,?\s*(TN|FL)\b\s*,?\s*(\d{5})?")


def area_of(marina_address: str) -> Dict[str, str]:
    """'9120 Saundersville Road in Mount Juliet, TN 37122' -> {'addressLine2': 'Mount Juliet, TN 37122'}."""
    hits = list(AREA_RE.finditer(re.sub(r"\s+", " ", marina_address or "")))
    if not hits:
        return {}
    m = hits[-1]
    return {"addressLine1": "", "addressLine2": f"{m.group(1)}, {m.group(2)} {m.group(3) or ''}".strip()}


class BoatOwnerSkiptraceBot(EnformionSkipTraceBot):
    name = "boat_owner_skiptrace"
    description = "Enformion phone lookup for owners named in marina-lien notices (paid, parked)"
    match_basis = "name_and_marina_area"

    @staticmethod
    def _address_parts(row: Dict[str, Any]) -> Dict[str, str]:
        return area_of(row.get("_marina_address") or "")

    def _candidates(self, client, max_per_run: int) -> List[Dict[str, Any]]:
        cap = int(os.environ.get("FALCO_MAX_BOAT_SKIPTRACE_PER_RUN") or DEFAULT_MAX_PER_RUN)
        q = (client.table("homeowner_requests_staging")
             .select("id, owner_name_records, full_name, phone, alternate_phones, phone_metadata, "
                     "_marina_address:raw_payload->vessel->>marina_address, _grade:raw_payload->vessel->>grade")
             .eq("distress_type", "MARINA_LIEN").is_("phone", "null")
             .not_.is_("owner_name_records", "null").limit(500).execute())
        out = []
        for r in getattr(q, "data", None) or []:
            pm = r.get("phone_metadata") if isinstance(r.get("phone_metadata"), dict) else {}
            if not r.get("_grade") or pm.get("enformion_skip_trace") or BUSINESS_RE.search(r["owner_name_records"]):
                continue
            if not area_of(r.get("_marina_address") or ""):
                continue
            r["__table__"] = "homeowner_requests_staging"
            out.append(r)
        return out[:min(cap, max_per_run)]


def run() -> dict:
    return BoatOwnerSkiptraceBot().run()


if __name__ == "__main__":
    print(run())
    sys.exit(0)
